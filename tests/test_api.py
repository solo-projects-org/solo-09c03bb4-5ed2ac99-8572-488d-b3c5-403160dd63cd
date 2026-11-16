"""HTTP-surface tests for the collector API (worker disabled)."""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from aq import db
from aq.app import create_app
from aq.settings import Settings
from aq.timeutil import normalize_source


@pytest.fixture()
def client(tmp_path):
    settings = Settings(db_path=str(tmp_path / "api.sqlite3"), instance_id="api-test")
    app = create_app(settings, start_collector=False)
    with TestClient(app) as test_client:
        yield test_client, settings


def create_job(client: TestClient, **body):
    return client.post("/api/v1/jobs", json=body)


def test_job_creation_and_progress(client):
    test_client, _ = client
    response = create_job(
        test_client,
        source="http://station-a:9001",
        station_id="A001",
        hours=["2026-11-15T01:00:00Z", "2026-11-15T00:30:00Z", "2026-11-15T01:00:00Z"],
    )
    assert response.status_code == 201, response.text
    data = response.json()
    assert data["source"] == {
        "protocol": "http",
        "host": "station-a",
        "port": 9001,
        "base_url": "http://station-a:9001",
    }
    assert data["progress"]["total"] == 2  # duplicate hour collapsed
    assert data["progress"]["queued"] == 2
    assert data["hour_from"] == "2026-11-15T00:00:00Z"
    assert data["hour_to"] == "2026-11-15T01:00:00Z"

    job_id = data["job_id"]
    fetched = test_client.get(f"/api/v1/jobs/{job_id}")
    assert fetched.status_code == 200
    assert fetched.json()["job_id"] == job_id

    tasks = test_client.get(f"/api/v1/jobs/{job_id}/tasks").json()
    assert [t["hour"] for t in tasks] == [
        "2026-11-15T00:00:00Z",
        "2026-11-15T01:00:00Z",
    ]


def test_validation_errors(client):
    test_client, _ = client
    bad_cases = [
        {"source": "http://a:1/path", "station_id": "S", "hours": ["2026-11-15T00:00:00Z"]},
        {"source": "ftp://a:1", "station_id": "S", "hours": ["2026-11-15T00:00:00Z"]},
        {"source": "http://a:99999", "station_id": "S", "hours": ["2026-11-15T00:00:00Z"]},
        {"source": "http://a:1", "station_id": "  ", "hours": ["2026-11-15T00:00:00Z"]},
        {"source": "http://a:1", "station_id": "S", "hours": []},
        {"source": "http://a:1", "station_id": "S", "hours": ["garbage"]},
        {"source": "http://a:1", "station_id": "S", "hours": [True]},
        {"source": "http://user:pass@a:1", "station_id": "S", "hours": ["2026-11-15T00:00:00Z"]},
    ]
    for body in bad_cases:
        response = create_job(test_client, **body)
        assert response.status_code == 422, (body, response.text)


def test_observation_queries(client):
    test_client, settings = client
    job = create_job(
        test_client,
        source={"protocol": "http", "host": "station-a", "port": 9001},
        station_id="A001",
        hours=["2026-11-15T00:00:00Z", "2026-11-15T01:00:00Z"],
    ).json()
    # Directly lease and seed observations (worker disabled).
    db.claim_tasks(settings.db_path, "seeder", time.time(), 60.0, 10)
    tasks = db.list_tasks(settings.db_path, job["job_id"], None, 10, 0)
    now = time.time()
    for task, value in zip(tasks, (11.5, 22.5)):
        db.record_success(settings.db_path, task["task_id"], "seeder", value, "µg/m³", {"pm25": value}, now)
    progress = test_client.get(f"/api/v1/jobs/{job['job_id']}").json()
    assert progress["progress"]["succeeded"] == 2
    assert progress["progress"]["observation_count"] == 2
    assert progress["progress"]["percent"] == 100.0
    assert progress["progress"]["status"] == "completed"

    observations = test_client.get(
        "/api/v1/observations",
        params={"source": "http://station-a:9001", "station_id": "A001"},
    ).json()
    assert [o["pm25"] for o in observations] == [11.5, 22.5]
    assert observations[0]["unit"] == "µg/m³"

    filtered = test_client.get(
        "/api/v1/observations",
        params={"from_hour": "2026-11-15T01:00:00Z", "to_hour": "2026-11-15T01:00:00Z"},
    ).json()
    assert len(filtered) == 1 and filtered[0]["pm25"] == 22.5

    by_job = test_client.get(f"/api/v1/jobs/{job['job_id']}/observations").json()
    assert len(by_job) == 2

    # Different source port is a different source, even with the same station.
    other = test_client.get(
        "/api/v1/observations", params={"source": "http://station-a:9002"}
    ).json()
    assert other == []

    assert test_client.get("/api/v1/observations", params={"from_hour": "bad"}).status_code == 422
    assert (
        test_client.get(
            "/api/v1/observations",
            params={"from_hour": "2026-11-16T00:00:00Z", "to_hour": "2026-11-15T00:00:00Z"},
        ).status_code
        == 422
    )


def test_404s_and_source_status(client):
    test_client, settings = client
    assert test_client.get("/api/v1/jobs/nope").status_code == 404
    assert test_client.get("/api/v1/jobs/nope/tasks").status_code == 404
    assert test_client.get("/api/v1/jobs/nope/observations").status_code == 404

    job = create_job(
        test_client,
        source="http://station-b:9002",
        station_id="B",
        hours=[1_700_000_000],
    ).json()
    db.update_source_cooldown(
        settings.db_path,
        normalize_source("http://station-b:9002"),
        time.time() + 30,
        time.time(),
    )
    sources = test_client.get("/api/v1/sources").json()
    assert len(sources) == 1
    assert sources[0]["source"]["base_url"] == "http://station-b:9002"
    assert 25 <= sources[0]["cooldown_remaining_seconds"] <= 31
    assert test_client.get(f"/api/v1/jobs/{job['job_id']}").json()["source_cooldown_until"] is not None


def test_workerless_health_is_degraded(client):
    test_client, _ = client
    health = test_client.get("/healthz")
    assert health.status_code == 200
    data = health.json()
    assert data["database"] == "ok"
    assert data["worker"]["running"] is False


def test_openapi_and_swagger_available(client):
    test_client, _ = client
    assert test_client.get("/openapi.json").status_code == 200
    swagger = test_client.get("/docs")
    assert swagger.status_code == 200
    assert "swagger-ui" in swagger.text.lower()
