from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest

from aq import db
from aq.collector import Collector
from aq.settings import Settings
from aq.timeutil import SourceAddress, normalize_source


class FakeOrigin:
    """In-process origin shared by two collectors; records concurrency."""

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.active: dict[str, int] = {}
        self.max_active: dict[str, int] = {}
        self.requests: list[dict[str, Any]] = []
        self.fail_first: dict[tuple[str, str], int] = {}
        self.rate_limit_every: dict[str, int] = {}
        self.counts: dict[str, int] = {}
        self.retry_after: dict[str, int] = {}

    def transport(self) -> httpx.MockTransport:
        async def handler(request: httpx.Request) -> httpx.Response:
            key = f"{request.url.scheme}://{request.url.host}:{request.url.port}"
            async with self.lock:
                self.counts[key] = self.counts.get(key, 0) + 1
                seq = self.counts[key]
                self.active[key] = self.active.get(key, 0) + 1
                self.max_active[key] = max(self.max_active.get(key, 0), self.active[key])
            record = {
                "source": key,
                "seq": seq,
                "station": request.url.params.get("station_id"),
                "hour": request.url.params.get("hour"),
                "started": time.time(),
            }
            try:
                await asyncio.sleep(0.05)
                station = request.url.params.get("station_id") or ""
                pair = (key, station)
                if self.fail_first.get(pair, 0) > 0:
                    self.fail_first[pair] -= 1
                    record["status"] = 503
                    return httpx.Response(503, json={"error": "unavailable"})
                every = self.rate_limit_every.get(key, 0)
                if every and seq % every == 0:
                    record["status"] = 429
                    return httpx.Response(
                        429,
                        headers={"Retry-After": str(self.retry_after.get(key, 1))},
                        json={"error": "too_many_requests"},
                    )
                record["status"] = 200
                return httpx.Response(
                    200,
                    json={
                        "station_id": station,
                        "hour": request.url.params.get("hour"),
                        "pm25": 42.5,
                        "unit": "µg/m³",
                    },
                )
            finally:
                async with self.lock:
                    self.active[key] -= 1
                    record["ended"] = time.time()
                self.requests.append(record)

        return httpx.MockTransport(handler)


def make_settings(tmp_path, instance_id: str, **overrides) -> Settings:
    values = dict(
        db_path=str(tmp_path / "aq.sqlite3"),
        instance_id=instance_id,
        http_timeout_seconds=1.5,
        lease_seconds=1.0,
        poll_seconds=0.02,
        max_concurrency=16,
    )
    values.update(overrides)
    return Settings(**values)


async def wait_for(predicate, timeout: float = 20.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


def submit(settings: Settings, source: str, station: str, hours: list[int]) -> str:
    job = db.create_job(
        settings.db_path,
        normalize_source(source),
        station,
        hours,
        time.time(),
    )
    return job["job_id"]


def succeeded(settings: Settings, job_id: str) -> int:
    progress = db.get_job_progress(settings.db_path, job_id, time.time())
    assert progress is not None
    return progress["progress"]["succeeded"]


@pytest.mark.asyncio
async def test_two_collectors_share_two_slots_per_source(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    origin = FakeOrigin()
    source_a = "http://origin-a:8001"
    source_b = "http://origin-b:8002"

    s1 = make_settings(tmp_path, "inst-1")
    s2 = make_settings(tmp_path, "inst-2")
    hours = [1_700_000_000 + 3600 * i for i in range(4)]
    job_a = submit(s1, source_a, "A", hours)
    job_b = submit(s2, source_b, "B", hours)

    c1 = Collector(s1, client=httpx.AsyncClient(transport=origin.transport()))
    c2 = Collector(s2, client=httpx.AsyncClient(transport=origin.transport()))
    await c1.start()
    await c2.start()
    try:
        assert await wait_for(lambda: succeeded(s1, job_a) == 4 and succeeded(s2, job_b) == 4)
    finally:
        await c1.stop()
        await c2.stop()

    assert origin.max_active["http://origin-a:8001"] <= 2
    assert origin.max_active["http://origin-b:8002"] <= 2
    # Both collectors really participated rather than one draining everything.
    assert origin.max_active["http://origin-a:8001"] == 2

    observations = db.query_observations(s1.db_path, None, "A", None, None, 100, 0)
    assert len({o["hour"] for o in observations}) == 4


@pytest.mark.asyncio
async def test_429_pauses_source_and_cooldown_never_shortens(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    origin = FakeOrigin()
    origin.rate_limit_every["http://origin-a:8001"] = 2
    origin.retry_after["http://origin-a:8001"] = 2

    settings = make_settings(tmp_path, "inst-1")
    hours = [1_700_000_000 + 3600 * i for i in range(3)]
    job = submit(settings, "http://origin-a:8001", "A", hours)
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    try:
        assert await wait_for(
            lambda: db.get_source_cooldown(settings.db_path, SourceAddress("http", "origin-a", 8001))
            or 0
        )
        deadline = db.get_source_cooldown(settings.db_path, SourceAddress("http", "origin-a", 8001))
        assert deadline is not None and deadline >= time.time() + 1.0

        # A later, shorter deadline must not shorten the existing cooldown.
        result = db.update_source_cooldown(
            settings.db_path,
            SourceAddress("http", "origin-a", 8001),
            time.time() + 0.1,
            time.time(),
        )
        assert result >= deadline

        # No new request may start for that source before the cooldown expires.
        blocked_until = result
        start_count = origin.counts.get("http://origin-a:8001", 0)
        while time.time() < blocked_until - 0.05:
            assert origin.counts.get("http://origin-a:8001", 0) == start_count
            await asyncio.sleep(0.02)
        assert await wait_for(lambda: succeeded(settings, job) == 3, timeout=20)
    finally:
        await collector.stop()


@pytest.mark.asyncio
async def test_cooldown_survives_instance_restart(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    source = SourceAddress("http", "origin-a", 8001)
    deadline = time.time() + 1.2
    db.update_source_cooldown(str(tmp_path / "aq.sqlite3"), source, deadline, time.time())

    origin = FakeOrigin()
    settings = make_settings(tmp_path, "restarted-instance")
    job = submit(settings, "http://origin-a:8001", "A", [1_700_000_000])
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    try:
        # A freshly started process must honour the persisted cooldown.
        while time.time() < deadline - 0.1:
            assert origin.counts.get("http://origin-a:8001", 0) == 0
            await asyncio.sleep(0.02)
        assert await wait_for(lambda: succeeded(settings, job) == 1)
    finally:
        await collector.stop()


@pytest.mark.asyncio
async def test_failed_hour_retries_and_observation_only_after_success(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    origin = FakeOrigin()
    origin.fail_first[("http://origin-a:8001", "A")] = 1
    settings = make_settings(tmp_path, "inst-1")
    job = submit(settings, "http://origin-a:8001", "A", [1_700_000_000])
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    try:
        assert await wait_for(lambda: origin.counts.get("http://origin-a:8001", 0) >= 1)
        # First attempt failed: no observation and progress still pending/retrying.
        await asyncio.sleep(0.1)
        rows = db.query_observations(settings.db_path, None, "A", None, None, 10, 0)
        assert rows == []
        progress = db.get_job_progress(settings.db_path, job, time.time())
        assert progress["progress"]["succeeded"] == 0
        assert await wait_for(lambda: succeeded(settings, job) == 1, timeout=15)
        rows = db.query_observations(settings.db_path, None, "A", None, None, 10, 0)
        assert len(rows) == 1
        progress = db.get_job_progress(settings.db_path, job, time.time())
        assert progress["progress"]["attempts"] >= 2
    finally:
        await collector.stop()


@pytest.mark.asyncio
async def test_duplicate_submission_stores_one_observation(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    origin = FakeOrigin()
    settings = make_settings(tmp_path, "inst-1")
    hours = [1_700_000_000 + 3600 * i for i in range(3)]
    job1 = submit(settings, "http://origin-a:8001", "A", hours)
    job2 = submit(settings, "http://origin-a:8001", "A", hours)
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    try:
        assert await wait_for(
            lambda: succeeded(settings, job1) == 3 and succeeded(settings, job2) == 3
        )
    finally:
        await collector.stop()
    rows = db.query_observations(settings.db_path, None, "A", None, None, 100, 0)
    assert len(rows) == 3
    assert len({(r["station_id"], r["hour"]) for r in rows}) == 3
    # Both jobs reached completion even though they shared the same task rows.
    assert db.get_job_progress(settings.db_path, job1, time.time())["progress"]["status"] == "completed"
    assert db.get_job_progress(settings.db_path, job2, time.time())["progress"]["status"] == "completed"


@pytest.mark.asyncio
async def test_takeover_after_instance_exit_waits_for_lease(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))
    s1 = make_settings(tmp_path, "dying-instance")
    s2 = make_settings(tmp_path, "surviving-instance")
    job = submit(s1, "http://origin-a:8001", "A", [1_700_000_000])

    # Simulate a hard crash: claim the task but never finish it.
    claimed = db.claim_tasks(s1.db_path, s1.instance_id, time.time(), s1.effective_lease_seconds, 1)
    assert len(claimed) == 1
    lease_until = claimed[0]["lease_until"]

    origin = FakeOrigin()
    c2 = Collector(s2, client=httpx.AsyncClient(transport=origin.transport()))
    await c2.start()
    try:
        await asyncio.sleep(0.1)
        # The peer must not steal a slot that may still be processing at origin.
        assert origin.counts.get("http://origin-a:8001", 0) == 0
        assert await wait_for(
            lambda: origin.counts.get("http://origin-a:8001", 0) > 0,
            timeout=s2.effective_lease_seconds + 5,
        )
        assert time.time() >= lease_until - 0.05
        assert await wait_for(lambda: succeeded(s2, job) == 1)
        assert db.get_job_progress(s2.db_path, job, time.time())["progress"]["status"] == "completed"
    finally:
        await c2.stop()


@pytest.mark.asyncio
async def test_graceful_stop_does_not_release_unfinished_lease(tmp_path):
    db.init_db(str(tmp_path / "aq.sqlite3"))

    class HangingOrigin(FakeOrigin):
        def transport(self):
            async def handler(request):
                await asyncio.sleep(10)
                return httpx.Response(200, json={"pm25": 1})

            return httpx.MockTransport(handler)

    origin = HangingOrigin()
    settings = make_settings(tmp_path, "inst-1", http_timeout_seconds=2.0, shutdown_grace_seconds=0.2)
    submit(settings, "http://origin-a:8001", "A", [1_700_000_000])
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    assert await wait_for(
        lambda: db.list_tasks(settings.db_path, db.list_jobs(settings.db_path, 1, 0)[0]["job_id"], None, 10, 0)[0]["state"] == "in_flight"
    )
    started = time.time()
    await collector.stop()
    assert time.time() - started < 2.0
    progress = db.get_job_progress(settings.db_path, db.list_jobs(settings.db_path, 1, 0)[0]["job_id"], time.time())
    assert progress["progress"]["in_flight"] == 1
    # The lease is still held, so another instance cannot immediately retry.
    claimed = db.claim_tasks(settings.db_path, "other", time.time(), 1.0, 10)
    assert claimed == []


@pytest.mark.asyncio
async def test_timeout_keeps_slot_until_lease(tmp_path):
    """An origin that outlives the client timeout still counts as in-flight."""
    db.init_db(str(tmp_path / "aq.sqlite3"))
    release = asyncio.Event()

    class LateOrigin(FakeOrigin):
        def transport(self):
            async def handler(request):
                await release.wait()
                return httpx.Response(200, json={"pm25": 1})

            return httpx.MockTransport(handler)

    origin = LateOrigin()
    settings = make_settings(tmp_path, "inst-1", http_timeout_seconds=0.4, lease_seconds=1.5)
    job = submit(settings, "http://origin-a:8001", "A", [1_700_000_000])
    collector = Collector(settings, client=httpx.AsyncClient(transport=origin.transport()))
    await collector.start()
    try:
        assert await wait_for(
            lambda: db.get_job_progress(settings.db_path, job, time.time())["progress"]["attempts"] == 1
        )
        await asyncio.sleep(0.6)  # client timeout has fired
        state = db.list_tasks(settings.db_path, job, "in_flight", 10, 0)
        assert len(state) == 1
        assert state[0]["last_error"] and "timeout" in state[0]["last_error"]
        # No peer may start another request while the origin is still working.
        assert db.claim_tasks(settings.db_path, "peer", time.time(), 5.0, 10) == []
    finally:
        release.set()
        await collector.stop()
