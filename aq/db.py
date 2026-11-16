"""SQLite coordination store shared by both collector instances.

The database is the single source of truth for:
  * per-source request slots (at most two leased in-flight requests per source),
  * the persisted 429 cooldown (``cooldown_until`` is only ever increased),
  * task progress, and
  * de-duplicated observations.

SQLite is opened in WAL mode with a generous busy timeout.  Every mutation that
changes a slot, cooldown, task state or observation uses ``BEGIN IMMEDIATE`` so
two collector processes cannot both consume the second free slot.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Iterable, Iterator, Sequence

from .timeutil import SourceAddress, epoch_to_hour, iso_from_epoch

SCHEMA = """
CREATE TABLE IF NOT EXISTS sources (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    protocol        TEXT    NOT NULL,
    host            TEXT    NOT NULL,
    port            INTEGER NOT NULL,
    cooldown_until  REAL    NOT NULL DEFAULT 0,
    cooldown_reason TEXT,
    created_at      REAL    NOT NULL,
    updated_at      REAL    NOT NULL,
    UNIQUE (protocol, host, port)
);

CREATE TABLE IF NOT EXISTS jobs (
    id           TEXT    PRIMARY KEY,
    source_id    INTEGER NOT NULL REFERENCES sources(id),
    station_id   TEXT    NOT NULL,
    created_at   REAL    NOT NULL,
    total_tasks  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       INTEGER NOT NULL REFERENCES sources(id),
    station_id      TEXT    NOT NULL,
    hour_epoch      INTEGER NOT NULL,
    state           TEXT    NOT NULL DEFAULT 'pending'
                    CHECK (state IN ('pending', 'in_flight', 'succeeded')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL    NOT NULL DEFAULT 0,
    owner           TEXT,
    lease_until     REAL,
    last_http_status INTEGER,
    last_error      TEXT,
    created_at      REAL    NOT NULL,
    updated_at      REAL    NOT NULL,
    UNIQUE (source_id, station_id, hour_epoch)
);

CREATE INDEX IF NOT EXISTS tasks_claim_idx
    ON tasks (state, next_attempt_at, source_id);
CREATE INDEX IF NOT EXISTS tasks_lease_idx
    ON tasks (state, lease_until);

CREATE TABLE IF NOT EXISTS job_tasks (
    job_id  TEXT    NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, task_id)
);

CREATE INDEX IF NOT EXISTS job_tasks_task_idx ON job_tasks (task_id);

CREATE TABLE IF NOT EXISTS observations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id   INTEGER NOT NULL REFERENCES sources(id),
    station_id  TEXT    NOT NULL,
    hour_epoch  INTEGER NOT NULL,
    pm25        REAL    NOT NULL,
    unit        TEXT    NOT NULL,
    raw_json    TEXT,
    fetched_at  REAL    NOT NULL,
    instance_id TEXT,
    UNIQUE (source_id, station_id, hour_epoch)
);

CREATE INDEX IF NOT EXISTS observations_hour_idx
    ON observations (hour_epoch);
CREATE INDEX IF NOT EXISTS observations_station_idx
    ON observations (station_id, hour_epoch);
"""

MAX_IN_FLIGHT_PER_SOURCE = 2


def _ensure_parent(path: str) -> None:
    if path == ":memory:":
        return
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def connect(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ping(path: str) -> None:
    """Cheap health probe: opens the DB and runs one trivial query."""
    conn = connect(path)
    try:
        conn.execute("SELECT 1 FROM sources LIMIT 1").fetchall()
    finally:
        conn.close()


def init_db(path: str) -> None:
    """Create schema; safe when both instances start at the same moment.

    ``PRAGMA journal_mode = WAL`` needs a brief exclusive lock, so a second
    instance booting concurrently can see ``database is locked``.  Retry a few
    times rather than failing container startup.
    """
    _ensure_parent(path)
    last_error: Exception | None = None
    for attempt in range(20):
        conn = connect(path)
        try:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.executescript(SCHEMA)
            return
        except sqlite3.OperationalError as exc:
            last_error = exc
            if "locked" not in str(exc).lower() and "busy" not in str(exc).lower():
                raise
        finally:
            conn.close()
        time.sleep(min(0.5, 0.02 * (attempt + 1)))
    raise RuntimeError(f"could not initialise database {path}: {last_error}")


@contextmanager
def _write_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def _read_tx(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _json_or_none(value: object) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)
    except TypeError:
        return str(value)


def _get_or_create_source(conn: sqlite3.Connection, source: SourceAddress, now: float) -> int:
    conn.execute(
        """
        INSERT INTO sources (protocol, host, port, cooldown_until, created_at, updated_at)
        VALUES (?, ?, ?, 0, ?, ?)
        ON CONFLICT (protocol, host, port) DO NOTHING
        """,
        (source.protocol, source.host, source.port, now, now),
    )
    row = conn.execute(
        "SELECT id FROM sources WHERE protocol = ? AND host = ? AND port = ?",
        (source.protocol, source.host, source.port),
    ).fetchone()
    assert row is not None
    return int(row["id"])


def create_job(
    path: str,
    source: SourceAddress,
    station_id: str,
    hours_epoch: Sequence[int],
    now: float,
) -> dict:
    """Create a job and its (globally de-duplicated) tasks in one transaction."""
    job_id = uuid.uuid4().hex
    conn = connect(path)
    try:
        with _write_tx(conn):
            source_id = _get_or_create_source(conn, source, now)
            conn.execute(
                "INSERT INTO jobs (id, source_id, station_id, created_at, total_tasks) VALUES (?, ?, ?, ?, ?)",
                (job_id, source_id, station_id, now, len(hours_epoch)),
            )
            conn.execute("CREATE TEMP TABLE IF NOT EXISTS submitted_hours (hour_epoch INTEGER PRIMARY KEY)")
            conn.execute("DELETE FROM submitted_hours")
            conn.executemany(
                "INSERT OR IGNORE INTO submitted_hours (hour_epoch) VALUES (?)",
                ((h,) for h in hours_epoch),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO tasks (source_id, station_id, hour_epoch, state, attempts,
                                             next_attempt_at, created_at, updated_at)
                SELECT ?, ?, hour_epoch, 'pending', 0, ?, ?, ?
                FROM submitted_hours
                """,
                (source_id, station_id, now, now, now),
            )
            conn.execute(
                """
                INSERT INTO job_tasks (job_id, task_id)
                SELECT ?, t.id
                FROM tasks t
                JOIN submitted_hours h ON h.hour_epoch = t.hour_epoch
                WHERE t.source_id = ? AND t.station_id = ?
                """,
                (job_id, source_id, station_id),
            )
            total = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM job_tasks WHERE job_id = ?", (job_id,)
                ).fetchone()["n"]
            )
            conn.execute("UPDATE jobs SET total_tasks = ? WHERE id = ?", (total, job_id))
        return {
            "job_id": job_id,
            "source": source,
            "station_id": station_id,
            "total_tasks": total,
            "created_at": now,
        }
    finally:
        conn.close()


def claim_tasks(
    path: str,
    instance_id: str,
    now: float,
    lease_seconds: float,
    limit: int,
) -> list[dict]:
    """Lease up to ``limit`` eligible tasks respecting two slots per source.

    All eligible claims happen in one ``BEGIN IMMEDIATE`` transaction.  Pending
    rows are ranked per source, and only rows that fit into the source's free
    slots are visible, so concurrent instances cannot oversubscribe a source.
    """
    if limit <= 0:
        return []
    conn = connect(path)
    try:
        with _write_tx(conn):
            # A vanished instance leaves leases behind.  Once the lease expires
            # the origin request has certainly finished (lease > client timeout
            # > origin's 5 s processing ceiling), so the task can be re-queued.
            conn.execute(
                """
                UPDATE tasks
                   SET state = 'pending', owner = NULL, lease_until = NULL,
                       next_attempt_at = ?, updated_at = ?,
                       last_error = COALESCE(last_error, 'lease expired; resumed by another instance')
                 WHERE state = 'in_flight' AND lease_until IS NOT NULL AND lease_until <= ?
                """,
                (now, now, now),
            )
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT t.id, t.source_id, t.station_id, t.hour_epoch, t.attempts,
                           t.next_attempt_at, s.cooldown_until,
                           ROW_NUMBER() OVER (
                               PARTITION BY t.source_id
                               ORDER BY t.next_attempt_at, t.id
                           ) AS source_rank
                      FROM tasks t
                      JOIN sources s ON s.id = t.source_id
                     WHERE t.state = 'pending'
                       AND t.next_attempt_at <= ?
                       AND s.cooldown_until <= ?
                ),
                active AS (
                    SELECT source_id, COUNT(*) AS n
                      FROM tasks
                     WHERE state = 'in_flight' AND lease_until > ?
                     GROUP BY source_id
                )
                SELECT r.id, r.source_id, r.station_id, r.hour_epoch, r.attempts,
                       s.protocol, s.host, s.port
                  FROM ranked r
                  JOIN sources s ON s.id = r.source_id
                  LEFT JOIN active a ON a.source_id = r.source_id
                 WHERE r.source_rank <= (? - COALESCE(a.n, 0))
                 ORDER BY r.next_attempt_at, r.source_id, r.source_rank
                 LIMIT ?
                """,
                (now, now, now, MAX_IN_FLIGHT_PER_SOURCE, limit),
            ).fetchall()
            claimed: list[dict] = []
            lease_until = now + lease_seconds
            for row in rows:
                updated = conn.execute(
                    """
                    UPDATE tasks
                       SET state = 'in_flight', owner = ?, lease_until = ?,
                           attempts = attempts + 1, updated_at = ?
                     WHERE id = ? AND state = 'pending'
                    """,
                    (instance_id, lease_until, now, row["id"]),
                )
                if updated.rowcount != 1:
                    continue
                claimed.append(
                    {
                        "task_id": int(row["id"]),
                        "source_id": int(row["source_id"]),
                        "source": SourceAddress(
                            protocol=row["protocol"], host=row["host"], port=int(row["port"])
                        ),
                        "station_id": row["station_id"],
                        "hour_epoch": int(row["hour_epoch"]),
                        "attempt": int(row["attempts"]) + 1,
                        "lease_until": lease_until,
                    }
                )
        return claimed
    finally:
        conn.close()


def confirm_dispatch(
    path: str,
    task_id: int,
    instance_id: str,
    now: float,
) -> tuple[bool, float | None]:
    """Final, atomic gate immediately before an HTTP request is sent.

    Claiming a lease and sending the request are separated by scheduling time.
    A 429 may be persisted in that gap; re-checking here linearises "cooldown
    recorded" and "new request dispatched" on the same SQLite write lock.  If
    the source is now cooling down (or the task moved on), the task is parked
    and no request is issued.

    Returns ``(allowed, cooldown_until)``.
    """
    conn = connect(path)
    try:
        with _write_tx(conn):
            row = conn.execute(
                """
                SELECT t.id, t.source_id, t.state, t.owner, s.cooldown_until
                  FROM tasks t JOIN sources s ON s.id = t.source_id
                 WHERE t.id = ?
                """,
                (task_id,),
            ).fetchone()
            if row is None or row["state"] != "in_flight" or row["owner"] != instance_id:
                return False, None
            active = conn.execute(
                """
                SELECT COUNT(*) AS n FROM tasks
                 WHERE source_id = ? AND state = 'in_flight' AND lease_until > ?
                """,
                (row["source_id"], now),
            ).fetchone()["n"]
            cooldown = float(row["cooldown_until"])
            if cooldown > now or int(active) > MAX_IN_FLIGHT_PER_SOURCE:
                retry_at = max(cooldown, now + 0.05)
                conn.execute(
                    """
                    UPDATE tasks
                       SET state = 'pending', owner = NULL, lease_until = NULL,
                           next_attempt_at = MAX(next_attempt_at, ?), updated_at = ?
                     WHERE id = ? AND owner = ? AND state = 'in_flight'
                    """,
                    (retry_at, now, task_id, instance_id),
                )
                return False, cooldown
            return True, None
    finally:
        conn.close()


def record_success(
    path: str,
    task_id: int,
    instance_id: str,
    pm25: float,
    unit: str,
    raw_json: object,
    fetched_at: float,
) -> dict:
    """Insert the observation and complete the task in the same transaction.

    The observation unique key makes "same source + station + hour only stored
    once" true even if two stale tasks somehow race.  A success never touches
    the source cooldown.
    """
    conn = connect(path)
    try:
        with _write_tx(conn):
            task = conn.execute(
                "SELECT id, source_id, station_id, hour_epoch, owner, state FROM tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if task is None:
                raise KeyError(f"task {task_id} not found")
            inserted = conn.execute(
                """
                INSERT INTO observations
                    (source_id, station_id, hour_epoch, pm25, unit, raw_json, fetched_at, instance_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (source_id, station_id, hour_epoch) DO NOTHING
                """,
                (
                    task["source_id"],
                    task["station_id"],
                    task["hour_epoch"],
                    float(pm25),
                    unit,
                    _json_or_none(raw_json),
                    fetched_at,
                    instance_id,
                ),
            )
            completed = conn.execute(
                """
                UPDATE tasks
                   SET state = 'succeeded', owner = NULL, lease_until = NULL,
                       last_http_status = 200, last_error = NULL, updated_at = ?
                 WHERE id = ? AND owner = ? AND state = 'in_flight'
                """,
                (fetched_at, task_id, instance_id),
            )
            return {
                "observation_inserted": inserted.rowcount == 1,
                "task_completed": completed.rowcount == 1,
            }
    finally:
        conn.close()


def record_429(
    path: str,
    task_id: int,
    instance_id: str,
    retry_after_seconds: int,
    now: float,
    raw_json: object = None,
) -> dict:
    """Persist a source cooldown and re-queue the affected task.

    ``MAX(cooldown_until, new_deadline)`` guarantees that a later short
    Retry-After can never shorten an existing cooldown.  The source cooldown is
    intentionally not cleared by any other request's success/failure, nor is it
    tied to the lifetime of this process.
    """
    conn = connect(path)
    try:
        with _write_tx(conn):
            task = conn.execute(
                "SELECT source_id, owner, state FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
            if task is None:
                raise KeyError(f"task {task_id} not found")
            deadline = now + max(0, int(retry_after_seconds))
            conn.execute(
                """
                UPDATE sources
                   SET cooldown_until = MAX(cooldown_until, ?),
                       cooldown_reason = ?,
                       updated_at = ?
                 WHERE id = ?
                """,
                (deadline, f"HTTP 429 Retry-After={int(retry_after_seconds)}s", now, task["source_id"]),
            )
            conn.execute(
                """
                UPDATE tasks
                   SET state = 'pending', owner = NULL, lease_until = NULL,
                       last_http_status = 429,
                       last_error = ?, next_attempt_at = MAX(?, ?), updated_at = ?
                 WHERE id = ? AND owner = ? AND state = 'in_flight'
                """,
                (
                    f"HTTP 429; source paused until {iso_from_epoch(deadline)}",
                    deadline,
                    now,
                    now,
                    task_id,
                    instance_id,
                ),
            )
            cooldown_until = float(
                conn.execute(
                    "SELECT cooldown_until FROM sources WHERE id = ?", (task["source_id"],)
                ).fetchone()["cooldown_until"]
            )
            return {"cooldown_until": cooldown_until, "retry_after": int(retry_after_seconds)}
    finally:
        conn.close()


def annotate_in_flight_error(
    path: str,
    task_id: int,
    instance_id: str,
    error: str,
    now: float,
) -> None:
    """Record an error without releasing the in-flight slot.

    Used when the origin may still be processing (client timeout): the slot must
    keep counting until the crash-safe lease expires.
    """
    conn = connect(path)
    try:
        with _write_tx(conn):
            conn.execute(
                """
                UPDATE tasks SET last_error = ?, updated_at = ?
                 WHERE id = ? AND owner = ? AND state = 'in_flight'
                """,
                (error[:2000], now, task_id, instance_id),
            )
    finally:
        conn.close()


def record_failure(
    path: str,
    task_id: int,
    instance_id: str,
    status_code: int | None,
    error: str,
    next_attempt_at: float,
    now: float,
) -> bool:
    """Return a task to the retry queue without altering the source cooldown."""
    conn = connect(path)
    try:
        with _write_tx(conn):
            updated = conn.execute(
                """
                UPDATE tasks
                   SET state = 'pending', owner = NULL, lease_until = NULL,
                       last_http_status = ?, last_error = ?,
                       next_attempt_at = MAX(next_attempt_at, ?), updated_at = ?
                 WHERE id = ? AND owner = ? AND state = 'in_flight'
                """,
                (status_code, error[:2000], next_attempt_at, now, task_id, instance_id),
            )
            return updated.rowcount == 1
    finally:
        conn.close()


def update_source_cooldown(path: str, source: SourceAddress, deadline: float, now: float) -> float:
    """Test/administration helper implementing the monotonic MAX rule."""
    conn = connect(path)
    try:
        with _write_tx(conn):
            source_id = _get_or_create_source(conn, source, now)
            conn.execute(
                """
                UPDATE sources
                   SET cooldown_until = MAX(cooldown_until, ?), updated_at = ?
                 WHERE id = ?
                """,
                (deadline, now, source_id),
            )
            return float(
                conn.execute("SELECT cooldown_until FROM sources WHERE id = ?", (source_id,))
                .fetchone()["cooldown_until"]
            )
    finally:
        conn.close()


def get_source_cooldown(path: str, source: SourceAddress) -> float | None:
    conn = connect(path)
    try:
        row = conn.execute(
            "SELECT cooldown_until FROM sources WHERE protocol = ? AND host = ? AND port = ?",
            (source.protocol, source.host, source.port),
        ).fetchone()
        return float(row["cooldown_until"]) if row else None
    finally:
        conn.close()


def _progress_from_conn(conn: sqlite3.Connection, job_id: str, now: float) -> dict | None:
    job = conn.execute(
        """
        SELECT j.id, j.station_id, j.created_at, j.total_tasks,
               s.id AS source_id, s.protocol, s.host, s.port, s.cooldown_until
          FROM jobs j JOIN sources s ON s.id = j.source_id
         WHERE j.id = ?
        """,
        (job_id,),
    ).fetchone()
    if job is None:
        return None
    counts = conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COALESCE(SUM(CASE WHEN t.state = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded,
            COALESCE(SUM(CASE WHEN t.state = 'in_flight' THEN 1 ELSE 0 END), 0) AS in_flight,
            COALESCE(SUM(CASE WHEN t.state = 'pending' AND t.attempts = 0 THEN 1 ELSE 0 END), 0) AS queued,
            COALESCE(SUM(CASE WHEN t.state = 'pending' AND t.attempts > 0 THEN 1 ELSE 0 END), 0) AS retrying,
            COALESCE(SUM(t.attempts), 0) AS attempts
          FROM job_tasks jt JOIN tasks t ON t.id = jt.task_id
         WHERE jt.job_id = ?
        """,
        (job_id,),
    ).fetchone()
    hour_range = conn.execute(
        """
        SELECT MIN(t.hour_epoch) AS lo, MAX(t.hour_epoch) AS hi
          FROM job_tasks jt JOIN tasks t ON t.id = jt.task_id
         WHERE jt.job_id = ?
        """,
        (job_id,),
    ).fetchone()
    observations = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM job_tasks jt
          JOIN tasks t ON t.id = jt.task_id
          JOIN observations o
            ON o.source_id = t.source_id AND o.station_id = t.station_id AND o.hour_epoch = t.hour_epoch
         WHERE jt.job_id = ?
        """,
        (job_id,),
    ).fetchone()["n"]
    total = int(counts["total"])
    succeeded = int(counts["succeeded"])
    percent = round((succeeded / total * 100.0), 3) if total else 100.0
    return {
        "job_id": job["id"],
        "station_id": job["station_id"],
        "source": SourceAddress(job["protocol"], job["host"], int(job["port"])),
        "created_at": float(job["created_at"]),
        "hour_from": epoch_to_hour(hour_range["lo"]) if hour_range and hour_range["lo"] is not None else None,
        "hour_to": epoch_to_hour(hour_range["hi"]) if hour_range and hour_range["hi"] is not None else None,
        "progress": {
            "total": total,
            "succeeded": succeeded,
            "in_flight": int(counts["in_flight"]),
            "queued": int(counts["queued"]),
            "retrying": int(counts["retrying"]),
            "attempts": int(counts["attempts"]),
            "percent": percent,
            "status": "completed" if total > 0 and succeeded == total else "in_progress",
            "observation_count": int(observations),
        },
        "source_cooldown_until": float(job["cooldown_until"]),
    }


def get_job_progress(path: str, job_id: str, now: float) -> dict | None:
    conn = connect(path)
    try:
        with _read_tx(conn):
            return _progress_from_conn(conn, job_id, now)
    finally:
        conn.close()


def list_jobs(path: str, limit: int, offset: int) -> list[dict]:
    conn = connect(path)
    try:
        with _read_tx(conn):
            rows = conn.execute(
                """
                SELECT id FROM jobs ORDER BY created_at DESC, rowid DESC LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            return [
                _progress_from_conn(conn, row["id"], 0)  # type: ignore[misc]
                for row in rows
            ]
    finally:
        conn.close()


def list_tasks(
    path: str,
    job_id: str,
    status: str | None,
    limit: int,
    offset: int,
) -> list[dict]:
    clauses = ["jt.job_id = ?"]
    params: list[object] = [job_id]
    if status == "succeeded":
        clauses.append("t.state = 'succeeded'")
    elif status == "in_flight":
        clauses.append("t.state = 'in_flight'")
    elif status == "queued":
        clauses.append("t.state = 'pending' AND t.attempts = 0")
    elif status == "retrying":
        clauses.append("t.state = 'pending' AND t.attempts > 0")
    elif status is not None:
        raise ValueError("status must be queued, retrying, in_flight or succeeded")
    sql = f"""
        SELECT t.id, t.station_id, t.hour_epoch, t.state, t.attempts,
               t.owner, t.lease_until, t.last_http_status, t.last_error,
               t.next_attempt_at, t.updated_at
          FROM job_tasks jt JOIN tasks t ON t.id = jt.task_id
         WHERE {' AND '.join(clauses)}
         ORDER BY t.hour_epoch, t.id LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    conn = connect(path)
    try:
        with _read_tx(conn):
            rows = conn.execute(sql, params).fetchall()
            return [
                {
                    "task_id": int(r["id"]),
                    "station_id": r["station_id"],
                    "hour": epoch_to_hour(r["hour_epoch"]),
                    "hour_epoch": int(r["hour_epoch"]),
                    "state": r["state"],
                    "attempts": int(r["attempts"]),
                    "owner": r["owner"],
                    "lease_until": iso_from_epoch(r["lease_until"]),
                    "last_http_status": r["last_http_status"],
                    "last_error": r["last_error"],
                    "next_attempt_at": iso_from_epoch(r["next_attempt_at"]),
                    "updated_at": iso_from_epoch(r["updated_at"]),
                }
                for r in rows
            ]
    finally:
        conn.close()


def _observation_rows(
    conn: sqlite3.Connection, where: str, params: Iterable[object], limit: int, offset: int
) -> list[dict]:
    sql = f"""
        SELECT o.id, o.station_id, o.hour_epoch, o.pm25, o.unit, o.fetched_at,
               o.instance_id, s.protocol, s.host, s.port
          FROM observations o JOIN sources s ON s.id = o.source_id
         WHERE {where}
         ORDER BY o.hour_epoch, o.id LIMIT ? OFFSET ?
    """
    rows = conn.execute(sql, (*params, limit, offset)).fetchall()
    return [
        {
            "id": int(r["id"]),
            "source": SourceAddress(r["protocol"], r["host"], int(r["port"])),
            "station_id": r["station_id"],
            "hour": epoch_to_hour(r["hour_epoch"]),
            "hour_epoch": int(r["hour_epoch"]),
            "pm25": float(r["pm25"]),
            "unit": r["unit"],
            "fetched_at": iso_from_epoch(r["fetched_at"]),
            "instance_id": r["instance_id"],
        }
        for r in rows
    ]


def query_observations(
    path: str,
    source: SourceAddress | None,
    station_id: str | None,
    from_epoch: int | None,
    to_epoch: int | None,
    limit: int,
    offset: int,
) -> list[dict]:
    clauses = ["1 = 1"]
    params: list[object] = []
    if source is not None:
        clauses.extend(
            ["s.protocol = ?", "s.host = ?", "s.port = ?"]
        )
        params.extend([source.protocol, source.host, source.port])
    if station_id is not None:
        clauses.append("o.station_id = ?")
        params.append(station_id)
    if from_epoch is not None:
        clauses.append("o.hour_epoch >= ?")
        params.append(from_epoch)
    if to_epoch is not None:
        clauses.append("o.hour_epoch <= ?")
        params.append(to_epoch)
    conn = connect(path)
    try:
        with _read_tx(conn):
            return _observation_rows(conn, " AND ".join(clauses), params, limit, offset)
    finally:
        conn.close()


def job_observations(path: str, job_id: str, limit: int, offset: int) -> list[dict]:
    conn = connect(path)
    try:
        with _read_tx(conn):
            return _observation_rows(
                conn,
                """EXISTS (
                       SELECT 1
                         FROM job_tasks jt JOIN tasks t ON t.id = jt.task_id
                        WHERE jt.job_id = ?
                          AND t.source_id = o.source_id
                          AND t.station_id = o.station_id
                          AND t.hour_epoch = o.hour_epoch
                   )""",
                (job_id,),
                limit,
                offset,
            )
    finally:
        conn.close()


def job_exists(path: str, job_id: str) -> bool:
    conn = connect(path)
    try:
        return conn.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,)).fetchone() is not None
    finally:
        conn.close()


def list_sources(path: str, now: float) -> list[dict]:
    conn = connect(path)
    try:
        with _read_tx(conn):
            rows = conn.execute(
                """
                SELECT s.id, s.protocol, s.host, s.port, s.cooldown_until, s.cooldown_reason,
                       s.created_at, s.updated_at,
                       COALESCE(SUM(CASE WHEN t.state = 'in_flight' THEN 1 ELSE 0 END), 0) AS in_flight,
                       COALESCE(SUM(CASE WHEN t.state = 'pending' THEN 1 ELSE 0 END), 0) AS pending,
                       COALESCE(SUM(CASE WHEN t.state = 'succeeded' THEN 1 ELSE 0 END), 0) AS succeeded
                  FROM sources s LEFT JOIN tasks t ON t.source_id = s.id
                 GROUP BY s.id
                 ORDER BY s.protocol, s.host, s.port
                """
            ).fetchall()
            return [
                {
                    "source": SourceAddress(r["protocol"], r["host"], int(r["port"])),
                    "cooldown_until": float(r["cooldown_until"]),
                    "cooldown_remaining_seconds": max(0.0, round(float(r["cooldown_until"]) - now, 3)),
                    "cooldown_reason": r["cooldown_reason"],
                    "in_flight": int(r["in_flight"]),
                    "pending": int(r["pending"]),
                    "succeeded": int(r["succeeded"]),
                    "created_at": iso_from_epoch(r["created_at"]),
                    "updated_at": iso_from_epoch(r["updated_at"]),
                }
                for r in rows
            ]
    finally:
        conn.close()
