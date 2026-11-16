from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from aq import db
from aq.collector import extract_pm25, parse_retry_after, retry_delay
from aq.settings import Settings
from aq.timeutil import normalize_source, parse_utc_hour


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-11-15T03:00:00Z", "2026-11-15T03:00:00+00:00"),
        ("2026-11-15T03:45:12+08:00", "2026-11-14T19:00:00+00:00"),
        ("2026-11-15T03:45:12", "2026-11-15T03:00:00+00:00"),
        ("2026-11-15T03:00:00+00:00", "2026-11-15T03:00:00+00:00"),
    ],
)
def test_parse_utc_hour(value, expected):
    assert parse_utc_hour(value).isoformat() == expected


def test_parse_utc_hour_truncates_and_rejects_garbage():
    assert parse_utc_hour("2026-11-15T03:59:59Z").minute == 0
    with pytest.raises(ValueError):
        parse_utc_hour("not-a-time")
    with pytest.raises(ValueError):
        parse_utc_hour(True)


def test_source_identity_ignores_path_but_rejects_it():
    a = normalize_source("http://Station-A:9001")
    b = normalize_source({"protocol": "http", "host": "station-a", "port": 9001})
    assert a == b
    with pytest.raises(ValueError):
        normalize_source("http://station-a:9001/v1/pm25")
    with pytest.raises(ValueError):
        normalize_source({"protocol": "http", "host": "station-a", "port": 0})


def test_source_defaults_ports():
    assert normalize_source("https://station-a").port == 443
    assert normalize_source("station-a").port == 80


@pytest.mark.parametrize(
    "header, expected",
    [(None, 1), ("3", 3), ("0", 0), (" 12 ", 12), ("-2", 1), ("abc", 1), (
        "Wed, 21 Oct 2015 07:28:00 GMT", 1,
    )],
)
def test_parse_retry_after_integer_seconds(header, expected):
    assert parse_retry_after(header) == expected


def test_retry_delay_is_bounded():
    assert retry_delay(1) == 0.5
    assert retry_delay(2) == 1.0
    assert retry_delay(1000) == 30.0


def test_extract_pm25_variants():
    assert extract_pm25({"pm25": 12.5})[0] == 12.5
    assert extract_pm25({"data": {"value": "7"}})[0] == 7.0
    with pytest.raises(ValueError):
        extract_pm25({"pm25": -1})
    with pytest.raises(ValueError):
        extract_pm25({"pm25": "NaN"})
    with pytest.raises(ValueError):
        extract_pm25({"nope": 1})


def test_claim_respects_two_slots_per_source_across_instances(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    now = time.time()
    for source in ("http://origin-a:8001", "http://origin-b:8002"):
        db.create_job(
            path,
            normalize_source(source),
            "S1",
            [1_700_000_000 + 3600 * i for i in range(6)],
            now,
        )
    first = db.claim_tasks(path, "i1", now, 10.0, 100)
    assert len(first) == 4
    per_source: dict[int, int] = {}
    for task in first:
        per_source[task["source_id"]] = per_source.get(task["source_id"], 0) + 1
    assert sorted(per_source.values()) == [2, 2]
    # Second instance sharing the DB sees no free slots.
    second = db.claim_tasks(path, "i2", now, 10.0, 100)
    assert second == []


def test_lease_expiry_requeues_to_peer(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    now = time.time()
    db.create_job(path, normalize_source("http://origin-a:8001"), "S1", [1_700_000_000], now)
    assert len(db.claim_tasks(path, "i1", now, 0.5, 10)) == 1
    assert db.claim_tasks(path, "i2", now + 0.1, 0.5, 10) == []
    reclaimed = db.claim_tasks(path, "i2", now + 1.0, 0.5, 10)
    assert len(reclaimed) == 1
    assert reclaimed[0]["attempt"] == 2


def test_cooldown_is_monotonic_and_shared(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    source = normalize_source("http://origin-a:8001")
    now = time.time()
    first = db.update_source_cooldown(path, source, now + 5, now)
    second = db.update_source_cooldown(path, source, now + 1, now)
    assert second == pytest.approx(first)
    third = db.update_source_cooldown(path, source, now + 9, now)
    assert third == pytest.approx(now + 9)


def test_observation_unique_per_source_station_hour(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    now = time.time()
    db.create_job(path, normalize_source("http://origin-a:8001"), "S1", [1_700_000_000], now)
    task = db.claim_tasks(path, "i1", now, 10.0, 1)[0]
    first = db.record_success(path, task["task_id"], "i1", 1.0, "µg/m³", {}, now)
    assert first == {"observation_inserted": True, "task_completed": True}
    second = db.record_success(path, task["task_id"], "i1", 99.0, "µg/m³", {}, now)
    assert second["observation_inserted"] is False
    rows = db.query_observations(path, None, "S1", None, None, 10, 0)
    assert len(rows) == 1
    assert rows[0]["pm25"] == 1.0


def test_success_does_not_clear_cooldown(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    now = time.time()
    source = normalize_source("http://origin-a:8001")
    db.create_job(path, source, "S1", [1_700_000_000, 1_700_003_600], now)
    tasks = db.claim_tasks(path, "i1", now, 10.0, 2)
    db.record_429(path, tasks[0]["task_id"], "i1", 5, now)
    cooldown = db.get_source_cooldown(path, source)
    assert cooldown is not None and cooldown >= now + 4.9
    db.record_success(path, tasks[1]["task_id"], "i1", 10, "µg/m³", {}, now + 0.1)
    assert db.get_source_cooldown(path, source) == cooldown


def test_confirm_dispatch_blocks_during_cooldown(tmp_path):
    path = str(tmp_path / "aq.sqlite3")
    db.init_db(path)
    now = time.time()
    source = normalize_source("http://origin-a:8001")
    db.create_job(path, source, "S1", [1_700_000_000, 1_700_003_600], now)
    tasks = db.claim_tasks(path, "i1", now, 10.0, 2)
    db.record_429(path, tasks[0]["task_id"], "i1", 5, now)
    # The second task was claimed before the 429; the dispatch gate must stop it.
    allowed, cooldown = db.confirm_dispatch(path, tasks[1]["task_id"], "i1", now)
    assert allowed is False
    assert cooldown is not None and cooldown >= now + 4.9
    parked = db.list_tasks(path, db.list_jobs(path, 1, 0)[0]["job_id"], "retrying", 10, 0)
    assert any(t["task_id"] == tasks[1]["task_id"] for t in parked)


def test_settings_reject_bad_values(tmp_path, monkeypatch):
    monkeypatch.setenv("AQ_LEASE_SECONDS", "nope")
    with pytest.raises(RuntimeError):
        Settings(db_path=str(tmp_path / "x.sqlite3"))
    monkeypatch.setenv("AQ_LEASE_SECONDS", "2")
    settings = Settings(db_path=str(tmp_path / "x.sqlite3"), http_timeout_seconds=20.0)
    assert settings.effective_lease_seconds >= 22.0


def test_iso_from_epoch_roundtrip():
    from aq.timeutil import epoch_to_hour

    dt = datetime(2026, 1, 2, 3, tzinfo=timezone.utc)
    assert epoch_to_hour(int(dt.timestamp())) == "2026-01-02T03:00:00Z"
