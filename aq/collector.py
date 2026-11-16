"""Asynchronous collector worker shared by every API instance."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from typing import Any

import httpx

from . import db
from .settings import Settings
from .timeutil import SourceAddress, epoch_to_hour, iso_from_epoch

log = logging.getLogger("aq.collector")

RETRY_BASE_SECONDS = 0.5
RETRY_CAP_SECONDS = 30.0
DEFAULT_RETRY_AFTER_SECONDS = 1


def parse_retry_after(value: str | None) -> int:
    """Return an integer-second Retry-After.

    The protocol used by the provincial stations specifies integer seconds.
    Anything else is treated defensively as one second; HTTP-date values are
    not accepted because an incorrect clock must never enable early retries.
    """
    if value is None:
        return DEFAULT_RETRY_AFTER_SECONDS
    text = value.strip()
    if not text or not text.isdigit():
        return DEFAULT_RETRY_AFTER_SECONDS
    try:
        return max(0, int(text))
    except ValueError:
        return DEFAULT_RETRY_AFTER_SECONDS


def retry_delay(attempt: int) -> float:
    return min(RETRY_CAP_SECONDS, RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)))


def extract_pm25(payload: Any) -> tuple[float, str]:
    """Extract a finite, non-negative PM2.5 value from a few common shapes."""
    candidates: list[Any] = []
    if isinstance(payload, dict):
        candidates.append(payload)
        for key in ("data", "observation", "result"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
    elif isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        candidates.append(payload[0])

    for obj in candidates:
        for key in ("pm25", "pm2_5", "pm2.5", "PM2.5", "value"):
            if key not in obj:
                continue
            raw = obj[key]
            if isinstance(raw, bool):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"PM2.5 out of range: {raw!r}")
            unit = str(obj.get("unit") or "µg/m³")
            return value, unit
    raise ValueError("response does not contain a numeric pm25 value")


class Collector:
    def __init__(self, settings: Settings, client: httpx.AsyncClient | None = None):
        self.settings = settings
        self._client = client
        self._owns_client = client is None
        self._stop = asyncio.Event()
        self._active: dict[asyncio.Task, dict] = {}
        self._loop_task: asyncio.Task | None = None
        self._last_claim_at: float | None = None
        self._started_at: float | None = None
        self._error: str | None = None

    async def start(self) -> None:
        if self._loop_task is not None:
            return
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.http_timeout_seconds),
                limits=httpx.Limits(
                    max_connections=max(10, self.settings.max_concurrency + 5),
                    max_keepalive_connections=max(10, self.settings.max_concurrency),
                ),
                headers={"User-Agent": f"aq-collector/{self.settings.instance_id}"},
            )
        self._started_at = time.time()
        self._stop.clear()
        self._loop_task = asyncio.create_task(self._run_loop(), name=f"collector-{self.settings.instance_id}")
        log.info("collector started instance=%s", self.settings.instance_id)

    async def stop(self) -> None:
        """Stop claiming, let in-flight work finish, then leave any stragglers' leases."""
        self._stop.set()
        if self._loop_task is not None:
            try:
                await asyncio.wait_for(self._loop_task, timeout=2.0)
            except asyncio.TimeoutError:
                self._loop_task.cancel()
                await asyncio.gather(self._loop_task, return_exceptions=True)
            self._loop_task = None

        pending = [t for t in self._active if not t.done()]
        if pending:
            log.info("waiting up to %.1fs for %d in-flight request(s)", self.settings.shutdown_grace_seconds, len(pending))
            done, still_pending = await asyncio.wait(pending, timeout=self.settings.shutdown_grace_seconds)
            for task in still_pending:
                # Do not release the DB lease: the origin may still be processing
                # this request.  The peer instance resumes only after it expires.
                task.cancel()
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)
        self._active.clear()
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None
        log.info("collector stopped instance=%s", self.settings.instance_id)

    @property
    def status(self) -> dict:
        return {
            "instance_id": self.settings.instance_id,
            "running": self._loop_task is not None and not self._loop_task.done(),
            "active_requests": sum(1 for t in self._active if not t.done()),
            "started_at": self._started_at,
            "last_claim_at": self._last_claim_at,
            "error": self._error,
            "http_timeout_seconds": self.settings.http_timeout_seconds,
            "lease_seconds": self.settings.effective_lease_seconds,
            "max_concurrency": self.settings.max_concurrency,
        }

    async def _run_loop(self) -> None:
        assert self._client is not None
        while not self._stop.is_set():
            try:
                self._active = {t: m for t, m in self._active.items() if not t.done()}
                available = self.settings.max_concurrency - len(self._active)
                if available > 0:
                    now = time.time()
                    claimed = await asyncio.to_thread(
                        db.claim_tasks,
                        self.settings.db_path,
                        self.settings.instance_id,
                        now,
                        self.settings.effective_lease_seconds,
                        available,
                    )
                    if claimed:
                        self._last_claim_at = now
                        self._error = None
                        for task in claimed:
                            worker = asyncio.create_task(self._process(task), name=f"task-{task['task_id']}")
                            worker.add_done_callback(self._worker_done)
                            self._active[worker] = task
                        continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # transient DB hiccup must not kill the worker
                self._error = f"{type(exc).__name__}: {exc}"
                log.exception("claim loop error; retrying")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.poll_seconds)
            except asyncio.TimeoutError:
                pass

    @staticmethod
    def _worker_done(worker: asyncio.Task) -> None:
        """Never let an unexpected worker exception disappear silently.

        The task stays in flight, so its lease will expire and a peer retries;
        logging here makes that visible instead of an asyncio warning.
        """
        if worker.cancelled():
            return
        error = worker.exception()
        if error is not None:
            log.error("task worker %s crashed: %r", worker.get_name(), error, exc_info=error)

    async def _process(self, task: dict) -> None:
        assert self._client is not None
        source: SourceAddress = task["source"]
        hour = epoch_to_hour(task["hour_epoch"])
        url = f"{source.base_url}{self.settings.station_path}"
        params = {"station_id": task["station_id"], "hour": hour}
        started = time.time()
        try:
            allowed, cooldown = await asyncio.to_thread(
                db.confirm_dispatch,
                self.settings.db_path,
                task["task_id"],
                self.settings.instance_id,
                started,
            )
        except Exception:
            log.exception("could not confirm dispatch for task=%s", task["task_id"])
            return
        if not allowed:
            log.info(
                "request_skipped task=%s source=%s (source cooling down or task re-queued; cooldown_until=%s)",
                task["task_id"], source.base_url, iso_from_epoch(cooldown),
            )
            return
        log.info(
            "request_start task=%s source=%s station=%s hour=%s attempt=%s",
            task["task_id"], source.base_url, task["station_id"], hour, task["attempt"],
        )
        try:
            # A total deadline, not just per-operation timeouts: the lease that
            # keeps the origin slot occupied is sized from this upper bound.
            async with asyncio.timeout(self.settings.http_timeout_seconds):
                response = await self._client.get(url, params=params)
        except TimeoutError as exc:
            # The origin may still be processing this request.  Do not free the
            # slot: leave the task in flight and let its lease expire, so no
            # peer can start another request for this source while the source
            # might still be busy.
            try:
                await asyncio.to_thread(
                    db.annotate_in_flight_error,
                    self.settings.db_path,
                    task["task_id"],
                    self.settings.instance_id,
                    f"timeout after {self.settings.http_timeout_seconds}s",
                    time.time(),
                )
            except KeyError:
                pass
            log.warning(
                "request_end task=%s status=timeout error=%r slot_held_until_lease duration=%.3fs",
                task["task_id"], exc, time.time() - started,
            )
            return
        except asyncio.CancelledError:
            log.warning("request_cancelled task=%s (lease retained)", task["task_id"])
            raise
        except httpx.HTTPError as exc:
            await self._fail(task, None, f"transport error: {exc!r}", started)
            return

        try:
            if response.status_code == 429:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                now = time.time()
                result = await asyncio.to_thread(
                    db.record_429,
                    self.settings.db_path,
                    task["task_id"],
                    self.settings.instance_id,
                    retry_after,
                    now,
                )
                log.warning(
                    "request_end task=%s status=429 retry_after=%ss source_cooldown_until=%.3f duration=%.3fs",
                    task["task_id"], retry_after, result["cooldown_until"], now - started,
                )
                return
            if 200 <= response.status_code < 300:
                try:
                    payload = response.json()
                except ValueError as exc:
                    await self._fail(task, response.status_code, f"invalid JSON: {exc}", started)
                    return
                try:
                    pm25, unit = extract_pm25(payload)
                except ValueError as exc:
                    await self._fail(task, response.status_code, str(exc), started)
                    return
                now = time.time()
                result = await asyncio.to_thread(
                    db.record_success,
                    self.settings.db_path,
                    task["task_id"],
                    self.settings.instance_id,
                    pm25,
                    unit,
                    payload,
                    now,
                )
                log.info(
                    "request_end task=%s status=%s pm25=%s inserted=%s duration=%.3fs",
                    task["task_id"], response.status_code, pm25,
                    result["observation_inserted"], now - started,
                )
                return
            await self._fail(
                task,
                response.status_code,
                f"HTTP {response.status_code}: {response.text[:300]}",
                started,
            )
        except KeyError:
            # The task vanished (operator deleted the DB row); nothing to update.
            log.warning("request_end task=%s disappeared from store", task["task_id"])
        except Exception as exc:  # pragma: no cover - defensive
            log.exception("request_end task=%s failed while recording result", task["task_id"])
            try:
                await asyncio.to_thread(
                    db.record_failure,
                    self.settings.db_path,
                    task["task_id"],
                    self.settings.instance_id,
                    response.status_code,
                    f"internal: {type(exc).__name__}: {exc}",
                    time.time() + retry_delay(task["attempt"]),
                    time.time(),
                )
            except Exception:
                log.exception("could not re-queue task %s", task["task_id"])

    async def _fail(self, task: dict, status: int | None, error: str, started: float) -> None:
        now = time.time()
        delay = retry_delay(task["attempt"])
        try:
            await asyncio.to_thread(
                db.record_failure,
                self.settings.db_path,
                task["task_id"],
                self.settings.instance_id,
                status,
                error,
                now + delay,
                now,
            )
        except KeyError:
            pass
        log.warning(
            "request_end task=%s status=%s error=%s retry_in=%.1fs duration=%.3fs",
            task["task_id"], status, error[:200], delay, now - started,
        )
