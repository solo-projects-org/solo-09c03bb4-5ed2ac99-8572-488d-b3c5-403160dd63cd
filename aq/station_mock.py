"""Local provincial-station simulator (two separately configurable stations).

Each station:
  * accepts ``GET /v1/pm25?station_id=...&hour=...`` (path configurable),
  * pauses its own responses to simulate slow origins up to 5 seconds,
  * returns HTTP 429 with an integer ``Retry-After`` on a configurable cadence,
  * records the start and end of every request (plus its configured behaviour),
  * ends processing of every accepted request within 5 seconds.

It is deliberately its own tiny ASGI app so it can be started twice with
different environment variables and no external dependencies beyond FastAPI.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

MAX_PROCESSING_SECONDS = 5.0


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class StationConfig:
    name: str = field(default_factory=lambda: os.getenv("STATION_NAME", "provincial-station"))
    path: str = field(default_factory=lambda: os.getenv("STATION_PATH", "/v1/pm25"))
    delay_seconds: float = field(default_factory=lambda: env_float("STATION_DELAY_SECONDS", 0.3))
    jitter_seconds: float = field(default_factory=lambda: env_float("STATION_JITTER_SECONDS", 0.0))
    rate_limit_every: int = field(default_factory=lambda: int(os.getenv("STATION_429_EVERY", "0")))
    retry_after_seconds: int = field(
        default_factory=lambda: int(os.getenv("STATION_RETRY_AFTER_SECONDS", "2"))
    )
    fail_every: int = field(default_factory=lambda: int(os.getenv("STATION_FAIL_EVERY", "0")))
    log_path: str = field(
        default_factory=lambda: os.getenv("STATION_LOG_PATH", "./station-data/requests.jsonl")
    )

    def __post_init__(self) -> None:
        self.delay_seconds = max(0.0, min(MAX_PROCESSING_SECONDS, self.delay_seconds))
        self.jitter_seconds = max(0.0, min(MAX_PROCESSING_SECONDS, self.jitter_seconds))
        self.rate_limit_every = max(0, self.rate_limit_every)
        self.retry_after_seconds = max(0, self.retry_after_seconds)
        self.fail_every = max(0, self.fail_every)


class ReconfigureIn(BaseModel):
    delay_seconds: float | None = Field(None, ge=0, le=MAX_PROCESSING_SECONDS)
    jitter_seconds: float | None = Field(None, ge=0, le=MAX_PROCESSING_SECONDS)
    rate_limit_every: int | None = Field(None, ge=0, description="每 N 个请求返回一次 429；0 关闭")
    retry_after_seconds: int | None = Field(None, ge=0)
    fail_every: int | None = Field(None, ge=0, description="每 N 个请求返回一次 503；0 关闭")


class StationState:
    def __init__(self, config: StationConfig):
        self.config = config
        self.lock = asyncio.Lock()
        self.seq = 0
        self.active = 0
        self.max_active = 0
        self.records: list[dict] = []
        self.lock_dir = os.path.dirname(os.path.abspath(config.log_path))
        try:
            os.makedirs(self.lock_dir, exist_ok=True)
        except OSError:
            # Read-only mounts are acceptable until the first request log write.
            pass

    def update(self, **changes: object) -> None:
        """Live-adjust behaviour without restarting the station.

        ``None`` means "leave unchanged".  Slow response is capped at 5 s so the
        station can never violate its processing ceiling.
        """
        if changes.get("delay_seconds") is not None:
            self.config.delay_seconds = max(0.0, min(MAX_PROCESSING_SECONDS, float(changes["delay_seconds"])))
        if changes.get("jitter_seconds") is not None:
            self.config.jitter_seconds = max(0.0, min(MAX_PROCESSING_SECONDS, float(changes["jitter_seconds"])))
        if changes.get("rate_limit_every") is not None:
            self.config.rate_limit_every = max(0, int(changes["rate_limit_every"]))
        if changes.get("retry_after_seconds") is not None:
            self.config.retry_after_seconds = max(0, int(changes["retry_after_seconds"]))
        if changes.get("fail_every") is not None:
            self.config.fail_every = max(0, int(changes["fail_every"]))
        self._log(
            {
                "event": "reconfigured",
                "station": self.config.name,
                "delay_seconds": self.config.delay_seconds,
                "jitter_seconds": self.config.jitter_seconds,
                "rate_limit_every": self.config.rate_limit_every,
                "retry_after_seconds": self.config.retry_after_seconds,
                "fail_every": self.config.fail_every,
                "at": time.time(),
            }
        )

    def _log(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        with open(self.config.log_path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
            handle.flush()
        if record.get("event") == "request":
            self.records.append(record)
            if len(self.records) > 10000:
                del self.records[:5000]

    def snapshot_records(self) -> list[dict]:
        return list(self.records)

    async def begin(self) -> int:
        async with self.lock:
            self.seq += 1
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            return self.seq

    async def end(self) -> None:
        async with self.lock:
            self.active -= 1


def pm25_for(station: str, station_id: str, hour: str) -> float:
    """Deterministic value so retries and duplicate submissions are comparable."""
    seed = f"{station}|{station_id}|{hour}"
    rng = random.Random(seed)
    return round(8 + rng.random() * 90, 2)


def create_station_app(config: StationConfig | None = None) -> FastAPI:
    config = config or StationConfig()
    state = StationState(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        state._log(
            {
                "event": "station_start",
                "station": config.name,
                "path": config.path,
                "delay_seconds": config.delay_seconds,
                "jitter_seconds": config.jitter_seconds,
                "rate_limit_every": config.rate_limit_every,
                "retry_after_seconds": config.retry_after_seconds,
                "fail_every": config.fail_every,
                "pid": os.getpid(),
                "at": time.time(),
            }
        )
        yield
        state._log({"event": "station_stop", "station": config.name, "at": time.time()})

    app = FastAPI(
        title=f"省站模拟器 · {config.name}",
        version="1.0.0",
        description=(
            "本地省站模拟：可设置慢响应（合计不超过 5 秒）、按请求序号返回 429，"
            "并记录每个请求的开始/结束时间与行为。"
        ),
        lifespan=lifespan,
    )

    async def handle(request: Request, station_id: str, hour: str) -> JSONResponse:
        seq = await state.begin()
        started = time.time()
        slow = False
        rate_limited = False
        failed = False
        plan = "normal"
        delay = max(
            0.0,
            min(
                MAX_PROCESSING_SECONDS,
                config.delay_seconds + (random.uniform(0, config.jitter_seconds) if config.jitter_seconds else 0.0),
            ),
        )
        try:
            if config.rate_limit_every and seq % config.rate_limit_every == 0:
                rate_limited = True
                plan = "429"
            elif config.fail_every and seq % config.fail_every == 0:
                failed = True
                plan = "503"
            else:
                plan = "ok"
            slow = plan == "ok" and delay >= 1.0
            if delay > 0:
                await asyncio.sleep(delay)
            if rate_limited:
                return JSONResponse(
                    {"error": "too_many_requests", "station": config.name, "retry_after": config.retry_after_seconds},
                    status_code=429,
                    headers={
                        "Retry-After": str(config.retry_after_seconds),
                        "X-Station": config.name,
                        "X-Request-Seq": str(seq),
                    },
                )
            if failed:
                return JSONResponse(
                    {"error": "station_unavailable", "station": config.name},
                    status_code=503,
                    headers={"X-Station": config.name, "X-Request-Seq": str(seq)},
                )
            return JSONResponse(
                {
                    "station_id": station_id,
                    "hour": hour,
                    "pm25": pm25_for(config.name, station_id, hour),
                    "unit": "µg/m³",
                    "station": config.name,
                    "served_at": time.time(),
                },
                headers={"X-Station": config.name, "X-Request-Seq": str(seq)},
            )
        finally:
            ended = time.time()
            try:
                await state.end()
                async with state.lock:
                    concurrent = state.max_active
                record = {
                    "event": "request",
                    "station": config.name,
                    "seq": seq,
                    "started_at": started,
                    "ended_at": ended,
                    "duration_seconds": round(ended - started, 4),
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.url.query),
                    "planned_delay_seconds": round(delay, 4),
                    "plan": plan,
                    "slow": slow,
                    "rate_limited": rate_limited,
                    "failed": failed,
                    "max_concurrent_observed": concurrent,
                    "client": request.client.host if request.client else None,
                }
                state._log(record)
            except Exception:  # pragma: no cover - logging must never break serving
                pass

    @app.get(config.path, summary="读取某测站某 UTC 小时 PM2.5")
    async def get_pm25(
        request: Request,
        station_id: str = Query(..., min_length=1),
        hour: str = Query(..., min_length=1),
    ):
        return await handle(request, station_id, hour)

    @app.get("/_station/requests", summary="查看本实例记录的请求日志")
    def list_requests(limit: int = Query(100, ge=1, le=10000)):
        snapshot = state.snapshot_records()
        return {
            "station": config.name,
            "count": len(snapshot),
            "max_active": state.max_active,
            "requests": snapshot[-limit:],
        }

    @app.get("/_station/config", summary="查看本实例配置")
    def get_config():
        return _config_payload()

    def _config_payload() -> dict:
        return {
            "station": config.name,
            "path": config.path,
            "delay_seconds": config.delay_seconds,
            "jitter_seconds": config.jitter_seconds,
            "rate_limit_every": config.rate_limit_every,
            "retry_after_seconds": config.retry_after_seconds,
            "fail_every": config.fail_every,
            "max_processing_seconds": MAX_PROCESSING_SECONDS,
        }

    @app.patch("/_station/config", summary="运行时调整慢响应/429/失败行为")
    def patch_config(body: ReconfigureIn):
        state.update(**body.model_dump())
        return _config_payload()

    @app.get("/_station/log.txt", response_class=PlainTextResponse, summary="原始请求日志")
    def raw_log():
        if not os.path.exists(config.log_path):
            return ""
        with open(config.log_path, "r", encoding="utf-8") as handle:
            return handle.read()

    @app.get("/healthz", include_in_schema=False)
    def healthz():
        return {"status": "ok", "station": config.name}

    return app


app = create_station_app()


def main() -> None:
    import argparse

    import uvicorn

    parser = argparse.ArgumentParser(description="Run a local provincial-station simulator")
    parser.add_argument("--host", default=os.getenv("STATION_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.getenv("STATION_PORT", "9001")))
    parser.add_argument("--log-level", default=os.getenv("STATION_LOG_LEVEL", "info"))
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level, workers=1)


if __name__ == "__main__":
    main()

