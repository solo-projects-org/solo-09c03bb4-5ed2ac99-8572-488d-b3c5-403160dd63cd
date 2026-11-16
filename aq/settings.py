"""Configuration for one collector instance."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number, got {raw!r}") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc


def _default_instance_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


@dataclass(frozen=True)
class Settings:
    db_path: str = field(default_factory=lambda: os.getenv("AQ_DB_PATH", "./aq-data/aq.sqlite3"))
    instance_id: str = field(default_factory=lambda: os.getenv("AQ_INSTANCE_ID") or _default_instance_id())

    # The provincial stations must finish processing within 5 s.  Client timeout
    # is a total deadline; the crash-safe lease outlives it so that an aborted-
    # but-still-running origin request keeps occupying its slot.
    http_timeout_seconds: float = field(default_factory=lambda: _env_float("AQ_HTTP_TIMEOUT_SECONDS", 6.0))
    lease_seconds: float = field(default_factory=lambda: _env_float("AQ_LEASE_SECONDS", 8.0))
    poll_seconds: float = field(default_factory=lambda: _env_float("AQ_POLL_SECONDS", 0.2))
    max_concurrency: int = field(default_factory=lambda: _env_int("AQ_MAX_CONCURRENCY", 64))
    station_path: str = field(default_factory=lambda: os.getenv("AQ_STATION_PATH", "/v1/pm25"))
    shutdown_grace_seconds: float = field(
        default_factory=lambda: _env_float("AQ_SHUTDOWN_GRACE_SECONDS", 3.0)
    )

    def __post_init__(self) -> None:
        if self.http_timeout_seconds <= 0:
            raise RuntimeError("AQ_HTTP_TIMEOUT_SECONDS must be positive")
        if self.lease_seconds <= 0:
            raise RuntimeError("AQ_LEASE_SECONDS must be positive")
        if self.poll_seconds <= 0:
            raise RuntimeError("AQ_POLL_SECONDS must be positive")
        if self.shutdown_grace_seconds < 0:
            raise RuntimeError("AQ_SHUTDOWN_GRACE_SECONDS must not be negative")
        if self.max_concurrency < 1:
            raise RuntimeError("AQ_MAX_CONCURRENCY must be at least 1")
        if not self.station_path.startswith("/"):
            raise RuntimeError("AQ_STATION_PATH must start with '/'")

    @property
    def effective_lease_seconds(self) -> float:
        """Never let a slot lease expire before the client-side request does."""
        return max(self.lease_seconds, self.http_timeout_seconds + 2.0)
