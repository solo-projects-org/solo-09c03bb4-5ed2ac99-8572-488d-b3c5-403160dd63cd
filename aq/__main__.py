"""Combined launcher: API + collector worker with a shared uvicorn lifespan."""

from __future__ import annotations

import logging
import os
import sys

import uvicorn

from .app import create_app
from .settings import Settings


def main() -> None:
    # Settings() validates all environment variables up front so a typo fails
    # fast at container start instead of after the first request.
    settings = Settings()
    log_level = os.getenv("AQ_LOG_LEVEL", "info").lower()
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("aq").info(
        "starting instance=%s db=%s lease=%.1fs timeout=%.1fs",
        settings.instance_id,
        settings.db_path,
        settings.effective_lease_seconds,
        settings.http_timeout_seconds,
    )
    # One uvicorn worker process per instance.  Horizontal scale-out is the
    # second container, not uvicorn subprocesses: each process would run its own
    # claim loop (the DB slot constraint would still hold, but there is no need
    # for the extra moving parts here).
    uvicorn.run(
        create_app(settings),
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_level=log_level,
        proxy_headers=False,
        access_log=True,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"startup failed: {exc}", file=sys.stderr)
        raise
