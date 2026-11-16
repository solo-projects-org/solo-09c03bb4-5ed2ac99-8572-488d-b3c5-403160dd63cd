#!/usr/bin/env python3
"""End-to-end acceptance check against a running docker-compose stack.

Run after ``docker compose up -d --build``::

    python scripts/e2e_check.py

It checks, from the outside:
  1. Swagger / OpenAPI availability on both collector instances.
  2. A job against the fast station completes, with progress readable while
     the slow station is still working.
  3. The slow station sees at most 2 concurrent requests (per-source cap).
  4. A 429 from station-a produces a persistent cooldown, and the collector
     issues no new requests to that source until the cooldown expires.
  5. The same job resubmitted yields the same number of observations.
  6. Killing collector-1 leaves collector-2 to complete the job.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

API1 = "http://127.0.0.1:8001"
API2 = "http://127.0.0.1:8002"
# Host-side URLs for inspecting the simulators.
STATION_A = "http://127.0.0.1:9001"
STATION_B = "http://127.0.0.1:9002"
# URLs the *collector containers* must use (Compose network DNS).  Override for
# a local, non-Docker run where collectors resolve loopback instead.
SOURCE_A = os.getenv("AQ_E2E_SOURCE_A", "http://station-a:9001")
SOURCE_B = os.getenv("AQ_E2E_SOURCE_B", "http://station-b:9002")


def get(url: str, timeout: float = 10.0):
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read())


def request(method: str, url: str, body: dict | None = None, timeout: float = 10.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        raw = response.read()
        return response.status, json.loads(raw) if raw else None


def wait_http(url: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            get(url, timeout=3)
            return
        except Exception:
            time.sleep(1)
    raise RuntimeError(f"{url} did not become ready")


def hours(count: int) -> list[str]:
    base = datetime(2026, 11, 15, tzinfo=timezone.utc)
    return [(base + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M:%SZ") for i in range(count)]


def progress(job_id: str, api: str = API1) -> dict:
    return get(f"{api}/api/v1/jobs/{job_id}")["progress"]


def wait_completed(job_id: str, api: str = API1, timeout: float = 180.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = progress(job_id, api)
        if current["status"] == "completed":
            return current
        time.sleep(0.5)
    raise AssertionError(f"job {job_id} did not complete: {progress(job_id, api)}")


def station_requests(base: str) -> list[dict]:
    return get(f"{base}/_station/requests")["requests"]


def compute(name: str, ok: bool, detail: str) -> None:
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
    if not ok:
        FAILURES.append(name)


FAILURES: list[str] = []


def main() -> int:
    wait_http(f"{API1}/healthz")
    wait_http(f"{API2}/healthz")
    wait_http(f"{STATION_A}/_station/config")
    wait_http(f"{STATION_B}/_station/config")

    swagger1 = get(f"{API1}/openapi.json")
    swagger2 = get(f"{API2}/openapi.json")
    compute("swagger", bool(swagger1["paths"]) and bool(swagger2["paths"]), "OpenAPI served by both instances")

    # Every run gets a fresh hour window.  Base + minutes-since-2020 keeps the
    # hours unique per run (so dedup checks are meaningful) and in the past.
    base = datetime(2020, 1, 1, tzinfo=timezone.utc)
    run_index = int(time.time() // 60)
    job_hours = [
        (base + timedelta(hours=run_index + i)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for i in range(6)
    ]

    # Reset stations deterministically through the control API.
    for base, delay, every in ((STATION_A, 2.0, 4), (STATION_B, 0.2, 0)):
        request(
            "PATCH",
            f"{base}/_station/config",
            {"delay_seconds": delay, "rate_limit_every": every, "retry_after_seconds": 3},
        )

    station_a_before = len(station_requests(STATION_A))

    status, job_a = request(
        "POST",
        f"{API1}/api/v1/jobs",
        {"source": SOURCE_A, "station_id": "A-SLOW", "hours": job_hours},
    )
    assert status == 201, job_a
    status, job_b = request(
        "POST",
        f"{API2}/api/v1/jobs",
        {"source": SOURCE_B, "station_id": "B-FAST", "hours": job_hours},
    )
    assert status == 201, job_b

    b = wait_completed(job_b["job_id"], API2, timeout=90)
    a_mid = progress(job_a["job_id"], API1)
    compute(
        "fast-source-isolated",
        b["succeeded"] == 6 and a_mid["status"] != "completed",
        f"station-b completed {b['succeeded']}/6 while station-a was at {a_mid['succeeded']}/6",
    )

    a = wait_completed(job_a["job_id"], API1, timeout=240)
    compute("slow-source-completes", a["succeeded"] == 6, json.dumps(a, ensure_ascii=False))

    slow_records = station_requests(STATION_A)[station_a_before:]
    max_active = max((r["max_concurrent_observed"] for r in slow_records), default=0)
    compute("two-request-cap", max_active <= 2, f"station-a observed max concurrency {max_active}")
    compute(
        "max-processing-5s",
        all(r["duration_seconds"] <= 5.05 for r in slow_records),
        f"{len(slow_records)} requests, longest {max(r['duration_seconds'] for r in slow_records):.2f}s",
    )
    limited = [r for r in slow_records if r["rate_limited"]]
    compute("429-exercised", bool(limited), f"{len(limited)} of {len(slow_records)} requests were 429")

    if limited:
        # A request that was already in flight when the 429 arrived is allowed
        # to finish (it already occupies a slot at the origin).  What must be
        # true is that no request is *dispatched* after the 429 until the
        # integer Retry-After window has elapsed.
        retry_after = 3.0
        violations = []
        starts = sorted(slow_records, key=lambda r: r["started_at"])
        for record in limited:
            cutoff = record["ended_at"] + 0.05
            window_end = record["ended_at"] + retry_after - 0.2
            offending = [
                r
                for r in starts
                if cutoff <= r["started_at"] < window_end and r["seq"] != record["seq"]
            ]
            if offending:
                violations.append(
                    (record["seq"], offending[0]["seq"], offending[0]["started_at"] - record["ended_at"])
                )
        if violations:
            detail = ", ".join(
                f"seq {a}->{b} after {gap:.2f}s" for a, b, gap in violations
            )
        else:
            detail = f"no new requests within Retry-After={retry_after}s after {len(limited)} 429(s)"
        compute("retry-after-honoured", not violations, detail)

    _, observations = request(
        "GET", f"{API1}/api/v1/observations?station_id=A-SLOW&limit=1000"
    )
    compute("observations-stored", len(observations) >= 6, f"{len(observations)} observations for A-SLOW")

    # Resubmit the same window; must not create duplicates.
    _, duplicate = request(
        "POST",
        f"{API1}/api/v1/jobs",
        {"source": SOURCE_A, "station_id": "A-SLOW", "hours": job_hours},
    )
    wait_completed(duplicate["job_id"], API2, timeout=120)
    _, observations_after = request(
        "GET", f"{API1}/api/v1/observations?station_id=A-SLOW&limit=1000"
    )
    compute(
        "deduplicated",
        len(observations_after) == len(observations),
        f"{len(observations)} -> {len(observations_after)} observations after resubmission",
    )

    # Failover: stop collector-1; collector-2 must finish a new job.
    _, failover_job = request(
        "POST",
        f"{API2}/api/v1/jobs",
        {"source": SOURCE_A, "station_id": "A-FAILOVER", "hours": hours(4)},
    )
    stop_cmd = os.getenv(
        "AQ_E2E_STOP_CMD", "docker compose stop collector-1"
    )
    start_cmd = os.getenv(
        "AQ_E2E_START_CMD", "docker compose start collector-1"
    )
    subprocess.run(stop_cmd, shell=True, check=False, stdout=subprocess.DEVNULL)
    try:
        result = wait_completed(failover_job["job_id"], API2, timeout=180)
        compute("failover", result["succeeded"] == 4, json.dumps(result, ensure_ascii=False))
    finally:
        subprocess.run(start_cmd, shell=True, check=False, stdout=subprocess.DEVNULL)

    if FAILURES:
        print(f"\nFAILED: {', '.join(FAILURES)}")
        return 1
    print("\nAll acceptance checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
