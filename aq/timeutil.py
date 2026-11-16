"""UTC hour and source-address parsing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlsplit


def parse_utc_hour(value: object) -> datetime:
    """Parse a UTC hour, normalising sub-hour components away.

    ISO-8601 strings with ``Z``/an offset are accepted.  Naive ISO strings are
    interpreted as UTC because the API contract is explicitly UTC.  Integer or
    numeric-string Unix timestamps (seconds) are also accepted.
    """
    if isinstance(value, bool):
        raise ValueError("hour must not be a boolean")
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc).replace(
                minute=0, second=0, microsecond=0
            )
        except (OverflowError, OSError, ValueError) as exc:
            raise ValueError(f"invalid Unix timestamp: {value!r}") from exc
    if not isinstance(value, str) or not value.strip():
        raise ValueError("hour must be a non-empty ISO-8601 string or Unix timestamp")
    text = value.strip()
    # Be forgiving about a numeric string, which is common in CSV exports.
    try:
        return datetime.fromtimestamp(float(text), tz=timezone.utc).replace(
            minute=0, second=0, microsecond=0
        )
    except (ValueError, OverflowError, OSError):
        pass
    iso = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise ValueError(f"invalid ISO-8601 hour: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def hour_epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def epoch_to_hour(value: int) -> str:
    return (
        datetime.fromtimestamp(int(value), tz=timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def iso_from_epoch(value: float | int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SourceAddress:
    protocol: str
    host: str
    port: int

    @property
    def base_url(self) -> str:
        host = f"[{self.host}]" if ":" in self.host and not self.host.startswith("[") else self.host
        return f"{self.protocol}://{host}:{self.port}"

    def as_dict(self) -> dict:
        return {"protocol": self.protocol, "host": self.host, "port": self.port}


def normalize_source(value: object) -> SourceAddress:
    """Accept a URL string or a {protocol, host, port} object.

    Source identity is exactly (protocol, host, port); paths are not part of the
    identity, so a base URL path is rejected rather than silently ignored.
    """
    if isinstance(value, SourceAddress):
        protocol, host, port = value.protocol, value.host, value.port
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("source must not be empty")
        if "://" not in text:
            text = "http://" + text
        parts = urlsplit(text)
        if parts.scheme.lower() not in {"http", "https"}:
            raise ValueError("source protocol must be http or https")
        if parts.username or parts.password:
            raise ValueError("source URL must not contain credentials")
        if parts.path not in {"", "/"} or parts.query or parts.fragment:
            raise ValueError("source must be an origin/base URL without path, query or fragment")
        if not parts.hostname:
            raise ValueError("source host is missing")
        protocol = parts.scheme.lower()
        host = parts.hostname.lower()
        port = parts.port
        if port is None:
            port = 443 if protocol == "https" else 80
    elif isinstance(value, dict):
        try:
            protocol = str(value.get("protocol") or "http").lower()
            host = str(value.get("host") or "").strip().lower()
            raw_port = value.get("port")
            port = int(raw_port) if raw_port is not None else (443 if protocol == "https" else 80)
        except (TypeError, ValueError) as exc:
            raise ValueError("source port must be an integer") from exc
        if protocol not in {"http", "https"}:
            raise ValueError("source protocol must be http or https")
        if not host or any(ch in host for ch in "/?#@"):
            raise ValueError("source host is invalid")
    else:
        raise ValueError("source must be a URL string or an object with protocol/host/port")

    if not (1 <= int(port) <= 65535):
        raise ValueError("source port must be between 1 and 65535")
    if not host:
        raise ValueError("source host is missing")
    return SourceAddress(protocol=protocol, host=host, port=int(port))
