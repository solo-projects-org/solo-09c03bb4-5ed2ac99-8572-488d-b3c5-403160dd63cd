"""FastAPI surface for the hourly PM2.5 collector.

Both API instances run the same worker; progress is derived live from the shared
SQLite database, so it is not tied to the instance that accepted the job.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator

from . import __version__, db
from .collector import Collector
from .settings import Settings
from .timeutil import (
    SourceAddress,
    hour_epoch,
    iso_from_epoch,
    normalize_source,
    parse_utc_hour,
)

log = logging.getLogger("aq.api")

MAX_HOURS_PER_JOB = 10000
MAX_PAGE_SIZE = 1000


class SourceIn(BaseModel):
    protocol: str = Field(default="http", pattern="^https?$")
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(ge=1, le=65535)


class SourceOut(BaseModel):
    protocol: str
    host: str
    port: int
    base_url: str


class JobCreate(BaseModel):
    source: str | SourceIn = Field(
        description="Origin of the provincial station: a base URL such as "
        "`http://station-a:9001` or `{protocol, host, port}`. "
        "Identity is protocol + host + port; paths are not allowed."
    )
    station_id: str = Field(min_length=1, max_length=128, description="Station identifier at the source.")
    hours: list[str | int | float] = Field(
        min_length=1,
        max_length=MAX_HOURS_PER_JOB,
        description="UTC hours to collect. ISO-8601 (with Z/offset) or Unix seconds; minute/second are truncated.",
        examples=[["2026-11-15T00:00:00Z", "2026-11-15T01:00:00Z"]],
    )

    @field_validator("source")
    @classmethod
    def _validate_source(cls, value: object) -> SourceAddress:
        if isinstance(value, SourceIn):
            value = value.model_dump()
        return normalize_source(value)

    @field_validator("station_id")
    @classmethod
    def _validate_station(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("station_id must not be blank")
        if any(ch.isspace() or ord(ch) < 32 for ch in text):
            raise ValueError("station_id must not contain whitespace or control characters")
        return text

    @field_validator("hours", mode="before")
    @classmethod
    def _reject_bools(cls, values: Any) -> Any:
        if isinstance(values, (list, tuple)):
            for index, value in enumerate(values):
                if isinstance(value, bool):
                    raise ValueError(f"hours[{index}]: hour must not be a boolean")
        return values

    @field_validator("hours")
    @classmethod
    def _validate_hours(cls, values: list[Any]) -> list[int]:
        parsed: list[int] = []
        seen: set[int] = set()
        for index, value in enumerate(values):
            try:
                epoch = hour_epoch(parse_utc_hour(value))
            except ValueError as exc:
                raise ValueError(f"hours[{index}]: {exc}") from exc
            if epoch not in seen:
                seen.add(epoch)
                parsed.append(epoch)
        parsed.sort()
        return parsed


class ProgressOut(BaseModel):
    total: int
    succeeded: int
    in_flight: int
    queued: int
    retrying: int
    attempts: int
    percent: float
    status: str
    observation_count: int


class JobOut(BaseModel):
    job_id: str
    station_id: str
    source: SourceOut
    created_at: str
    hour_from: str | None = None
    hour_to: str | None = None
    progress: ProgressOut
    source_cooldown_until: str | None
    source_cooldown_remaining_seconds: float


class JobCreatedOut(JobOut):
    pass


class TaskOut(BaseModel):
    task_id: int
    station_id: str
    hour: str
    hour_epoch: int
    state: str
    attempts: int
    owner: str | None
    lease_until: str | None
    last_http_status: int | None
    last_error: str | None
    next_attempt_at: str | None
    updated_at: str | None


class ObservationOut(BaseModel):
    id: int
    source: SourceOut
    station_id: str
    hour: str
    hour_epoch: int
    pm25: float
    unit: str
    fetched_at: str | None
    instance_id: str | None


class SourceStatusOut(BaseModel):
    source: SourceOut
    cooldown_until: str | None
    cooldown_remaining_seconds: float
    cooldown_reason: str | None
    in_flight: int
    pending: int
    succeeded: int
    created_at: str | None
    updated_at: str | None


class WorkerStatusOut(BaseModel):
    instance_id: str
    running: bool
    active_requests: int
    started_at: str | None
    last_claim_at: str | None
    error: str | None
    http_timeout_seconds: float
    lease_seconds: float
    max_concurrency: int


class HealthOut(BaseModel):
    status: str
    version: str
    database: str
    worker: WorkerStatusOut


def _src(source: SourceAddress) -> SourceOut:
    return SourceOut(
        protocol=source.protocol,
        host=source.host,
        port=source.port,
        base_url=source.base_url,
    )


def _job_out(raw: dict, now: float) -> JobOut:
    progress = dict(raw["progress"])
    cooldown = raw["source_cooldown_until"]
    return JobOut(
        job_id=raw["job_id"],
        station_id=raw["station_id"],
        source=_src(raw["source"]),
        created_at=iso_from_epoch(raw["created_at"]) or "",
        hour_from=raw.get("hour_from"),
        hour_to=raw.get("hour_to"),
        progress=ProgressOut(**progress),
        source_cooldown_until=iso_from_epoch(cooldown) if cooldown and cooldown > 0 else None,
        source_cooldown_remaining_seconds=max(0.0, round(cooldown - now, 3)) if cooldown else 0.0,
    )


def _parse_hour_query(value: str | None, name: str) -> int | None:
    if value is None:
        return None
    try:
        return hour_epoch(parse_utc_hour(value))
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"{name}: {exc}") from exc


def _parse_source_query(value: str | None) -> SourceAddress | None:
    if value is None:
        return None
    try:
        return normalize_source(value)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"source: {exc}") from exc


def create_app(
    settings: Settings | None = None,
    *,
    client: httpx.AsyncClient | None = None,
    start_collector: bool = True,
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        db.init_db(settings.db_path)
        collector = Collector(settings, client=client)
        app.state.collector = collector
        app.state.settings = settings
        if start_collector:
            await collector.start()
        else:
            app.state.collector = None
        try:
            yield
        finally:
            if app.state.collector is not None:
                await app.state.collector.stop()

    app = FastAPI(
        title="空气质量小时 PM2.5 采集 API",
        version=__version__,
        description=(
            "提交省级测站源地址、测站号与 UTC 小时清单，异步采集小时 PM2.5。\n\n"
            "两个采集实例共享同一 SQLite（WAL）协调库：每个源最多 2 个在途请求、"
            "429 Retry-After 冷却持久化且只延长不缩短、同源同站同小时只入库一次、"
            "失败小时自动重试。\n\n"
            "源地址按 `protocol + host + port` 区分。Compose 内请使用 "
            "`http://station-a:9001` 或 `http://station-b:9002`。"
        ),
        lifespan=lifespan,
    )

    def collector_of(request: Request) -> Collector | None:
        return getattr(request.app.state, "collector", None)

    @app.get("/healthz", response_model=HealthOut, tags=["运维"], summary="健康与实例状态")
    def healthz(request: Request) -> HealthOut:
        try:
            db.ping(settings.db_path)
            database = "ok"
        except Exception as exc:  # pragma: no cover - defensive
            database = f"error: {exc}"
        collector: Collector | None = collector_of(request)
        worker = collector.status if collector is not None else {
            "instance_id": settings.instance_id,
            "running": False,
            "active_requests": 0,
            "started_at": None,
            "last_claim_at": None,
            "error": "collector disabled",
            "http_timeout_seconds": settings.http_timeout_seconds,
            "lease_seconds": settings.effective_lease_seconds,
            "max_concurrency": settings.max_concurrency,
        }
        worker = dict(worker)
        worker["started_at"] = iso_from_epoch(worker["started_at"])
        worker["last_claim_at"] = iso_from_epoch(worker["last_claim_at"])
        return HealthOut(
            status="ok" if database == "ok" and worker["running"] else "degraded",
            version=__version__,
            database=database,
            worker=WorkerStatusOut(**worker),
        )

    @app.post(
        "/api/v1/jobs",
        response_model=JobCreatedOut,
        status_code=status.HTTP_201_CREATED,
        tags=["采集任务"],
        summary="提交采集任务",
    )
    def create_job(body: JobCreate) -> JobCreatedOut:
        now = time.time()
        try:
            raw = db.create_job(
                settings.db_path,
                body.source,
                body.station_id,
                body.hours,
                now,
            )
        except Exception as exc:
            log.exception("could not create job")
            raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"could not create job: {exc}") from exc
        return _job_out(db.get_job_progress(settings.db_path, raw["job_id"], now), now)  # type: ignore[arg-type]

    @app.get("/api/v1/jobs", response_model=list[JobOut], tags=["采集任务"], summary="任务列表")
    def list_jobs(
        limit: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> list[JobOut]:
        now = time.time()
        return [_job_out(raw, now) for raw in db.list_jobs(settings.db_path, limit, offset)]

    @app.get(
        "/api/v1/jobs/{job_id}",
        response_model=JobOut,
        tags=["采集任务"],
        summary="查看任务进度",
    )
    def get_job(job_id: str) -> JobOut:
        now = time.time()
        raw = db.get_job_progress(settings.db_path, job_id, now)
        if raw is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="job not found")
        return _job_out(raw, now)

    @app.get(
        "/api/v1/jobs/{job_id}/tasks",
        response_model=list[TaskOut],
        tags=["采集任务"],
        summary="查看任务内每个小时的采集状态",
    )
    def list_job_tasks(
        job_id: str,
        task_status: str | None = Query(
            None,
            alias="status",
            pattern="^(queued|retrying|in_flight|succeeded)$",
            description="按状态过滤",
        ),
        limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> list[TaskOut]:
        if not db.job_exists(settings.db_path, job_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="job not found")
        return [TaskOut(**row) for row in db.list_tasks(settings.db_path, job_id, task_status, limit, offset)]

    @app.get(
        "/api/v1/jobs/{job_id}/observations",
        response_model=list[ObservationOut],
        tags=["观测数据"],
        summary="查看任务已入库观测",
    )
    def list_job_observations(
        job_id: str,
        limit: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> list[ObservationOut]:
        if not db.job_exists(settings.db_path, job_id):
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail="job not found")
        rows = db.job_observations(settings.db_path, job_id, limit, offset)
        return [
            ObservationOut(
                **{**row, "source": _src(row["source"])}
            )
            for row in rows
        ]

    @app.get(
        "/api/v1/observations",
        response_model=list[ObservationOut],
        tags=["观测数据"],
        summary="查询已入库观测",
    )
    def list_observations(
        source: str | None = Query(None, description="源地址，如 http://station-a:9001"),
        station_id: str | None = Query(None, min_length=1, max_length=128),
        from_hour: str | None = Query(None, description="起始 UTC 小时（含）"),
        to_hour: str | None = Query(None, description="结束 UTC 小时（含）"),
        limit: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ) -> list[ObservationOut]:
        from_epoch = _parse_hour_query(from_hour, "from_hour")
        to_epoch = _parse_hour_query(to_hour, "to_hour")
        if from_epoch is not None and to_epoch is not None and from_epoch > to_epoch:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY, detail="from_hour must not be after to_hour"
            )
        rows = db.query_observations(
            settings.db_path,
            _parse_source_query(source),
            station_id,
            from_epoch,
            to_epoch,
            limit,
            offset,
        )
        return [ObservationOut(**{**row, "source": _src(row["source"])}) for row in rows]

    @app.get(
        "/api/v1/sources",
        response_model=list[SourceStatusOut],
        tags=["运维"],
        summary="源及其冷却/队列状态",
    )
    def list_sources() -> list[SourceStatusOut]:
        now = time.time()
        rows = db.list_sources(settings.db_path, now)
        return [
            SourceStatusOut(
                source=_src(row["source"]),
                cooldown_until=(
                    iso_from_epoch(row["cooldown_until"]) if row["cooldown_until"] > 0 else None
                ),
                cooldown_remaining_seconds=row["cooldown_remaining_seconds"],
                cooldown_reason=row["cooldown_reason"],
                in_flight=row["in_flight"],
                pending=row["pending"],
                succeeded=row["succeeded"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
            for row in rows
        ]

    return app


app = create_app()


def _configure_logging() -> None:
    if os.getenv("AQ_LOG_LEVEL"):
        logging.basicConfig(
            level=os.getenv("AQ_LOG_LEVEL", "INFO").upper(),
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )


_configure_logging()
