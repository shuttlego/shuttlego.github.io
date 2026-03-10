"""TMAP collection schema and DB helpers."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_SQL = """
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS tmap_target (
    target_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    route_type TEXT NOT NULL,
    route_uuid TEXT NOT NULL,
    route_id INTEGER NOT NULL,
    route_name TEXT NOT NULL,
    day_type TEXT NOT NULL,
    departure_time TEXT NOT NULL,
    stop_count INTEGER NOT NULL,
    api_call_cost INTEGER NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    last_collected_at TEXT,
    last_run_status TEXT,
    last_run_http_code INTEGER,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tmap_target_unique
ON tmap_target(route_uuid, day_type, departure_time);

CREATE INDEX IF NOT EXISTS idx_tmap_target_active_collect
ON tmap_target(active, last_collected_at, target_id);

CREATE TABLE IF NOT EXISTS tmap_run (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_id TEXT NOT NULL REFERENCES tmap_target(target_id) ON DELETE CASCADE,
    requested_start_time TEXT NOT NULL,
    called_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL,
    http_code INTEGER,
    api_calls_used INTEGER NOT NULL DEFAULT 0,
    error_code TEXT,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_tmap_run_target_called
ON tmap_run(target_id, called_at_utc DESC, run_id DESC);

CREATE TABLE IF NOT EXISTS tmap_stop_observation (
    run_id INTEGER NOT NULL REFERENCES tmap_run(run_id) ON DELETE CASCADE,
    stop_seq INTEGER NOT NULL,
    stop_uuid TEXT,
    stop_id INTEGER,
    stop_name TEXT,
    point_type TEXT,
    arrive_time TEXT,
    segment_distance_m INTEGER,
    cumulative_distance_m INTEGER,
    PRIMARY KEY(run_id, stop_seq)
);

CREATE TABLE IF NOT EXISTS tmap_segment_observation (
    run_id INTEGER NOT NULL REFERENCES tmap_run(run_id) ON DELETE CASCADE,
    from_seq INTEGER NOT NULL,
    to_seq INTEGER NOT NULL,
    from_stop_uuid TEXT,
    to_stop_uuid TEXT,
    distance_m INTEGER,
    encoded_polyline TEXT,
    PRIMARY KEY(run_id, from_seq, to_seq)
);

CREATE TABLE IF NOT EXISTS tmap_quota_state (
    quota_date_kst TEXT PRIMARY KEY,
    attempts INTEGER NOT NULL DEFAULT 0,
    successes INTEGER NOT NULL DEFAULT 0,
    blocked_until_utc TEXT,
    last_http_code INTEGER,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tmap_collector_state (
    state_key TEXT PRIMARY KEY,
    state_value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tmap_holiday_cache (
    holiday_date TEXT PRIMARY KEY,
    is_holiday INTEGER NOT NULL,
    source TEXT NOT NULL,
    fetched_at_utc TEXT NOT NULL
);
"""

TMAP_TABLES = (
    "tmap_target",
    "tmap_run",
    "tmap_stop_observation",
    "tmap_segment_observation",
    "tmap_quota_state",
    "tmap_collector_state",
    "tmap_holiday_cache",
)


@dataclass(frozen=True)
class TargetRow:
    target_id: str
    site_id: str
    route_type: str
    route_uuid: str
    route_id: int
    route_name: str
    day_type: str
    departure_time: str
    stop_count: int
    api_call_cost: int
    last_collected_at: str | None


@dataclass(frozen=True)
class StopRow:
    seq: int
    stop_id: int
    stop_uuid: str
    stop_name: str
    lat: float
    lon: float
    estimated_elapsed_min: int | None
    cumulative_distance_m: int | None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def _target_id(route_uuid: str, day_type: str, departure_time: str) -> str:
    src = f"{route_uuid}|{day_type}|{departure_time}".encode("utf-8")
    return hashlib.sha1(src).hexdigest()


def _row_get(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def refresh_targets(conn: sqlite3.Connection) -> tuple[int, int]:
    now = utc_now_iso()
    conn.execute("UPDATE tmap_target SET active=0, updated_at=?", (now,))

    rows = conn.execute(
        """
        WITH grouped AS (
            SELECT
                r.site_id,
                r.route_type,
                r.route_uuid,
                r.route_id,
                r.route_name,
                sv.day_type,
                sv.departure_time,
                MIN(sv.variant_id) AS variant_id
            FROM route r
            JOIN service_variant sv ON sv.route_id = r.route_id
            GROUP BY
                r.site_id,
                r.route_type,
                r.route_uuid,
                r.route_id,
                r.route_name,
                sv.day_type,
                sv.departure_time
        )
        SELECT
            g.site_id,
            g.route_type,
            g.route_uuid,
            g.route_id,
            g.route_name,
            g.day_type,
            g.departure_time,
            COUNT(vs.stop_id) AS stop_count
        FROM grouped g
        JOIN variant_stop vs ON vs.variant_id = g.variant_id
        GROUP BY
            g.site_id,
            g.route_type,
            g.route_uuid,
            g.route_id,
            g.route_name,
            g.day_type,
            g.departure_time
        """
    ).fetchall()

    upserted = 0
    for row in rows:
        route_uuid = str(_row_get(row, "route_uuid", "") or "").strip()
        day_type = str(_row_get(row, "day_type", "") or "").strip()
        departure_time = str(_row_get(row, "departure_time", "") or "").strip()
        stop_count = int(_row_get(row, "stop_count", 0) or 0)
        if not route_uuid or not day_type or not departure_time or stop_count <= 1:
            continue
        target_id = _target_id(route_uuid, day_type, departure_time)
        api_call_cost = max(1, (stop_count + 31) // 32)
        conn.execute(
            """
            INSERT INTO tmap_target (
                target_id, site_id, route_type, route_uuid, route_id, route_name,
                day_type, departure_time, stop_count, api_call_cost,
                active, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(target_id) DO UPDATE SET
                site_id=excluded.site_id,
                route_type=excluded.route_type,
                route_uuid=excluded.route_uuid,
                route_id=excluded.route_id,
                route_name=excluded.route_name,
                day_type=excluded.day_type,
                departure_time=excluded.departure_time,
                stop_count=excluded.stop_count,
                api_call_cost=excluded.api_call_cost,
                active=1,
                updated_at=excluded.updated_at
            """,
            (
                target_id,
                str(_row_get(row, "site_id", "") or ""),
                str(_row_get(row, "route_type", "") or ""),
                route_uuid,
                int(_row_get(row, "route_id", 0) or 0),
                str(_row_get(row, "route_name", "") or ""),
                day_type,
                departure_time,
                stop_count,
                api_call_cost,
                now,
            ),
        )
        upserted += 1

    inactive = int(
        conn.execute("SELECT COUNT(*) FROM tmap_target WHERE active=0").fetchone()[0]
    )
    return upserted, inactive


def fetch_next_target(conn: sqlite3.Connection) -> TargetRow | None:
    row = conn.execute(
        """
        SELECT
            t.target_id,
            t.site_id,
            t.route_type,
            t.route_uuid,
            t.route_id,
            t.route_name,
            t.day_type,
            t.departure_time,
            t.stop_count,
            t.api_call_cost,
            t.last_collected_at
        FROM tmap_target t
        LEFT JOIN (
            SELECT target_id, COUNT(*) AS run_count
            FROM tmap_run
            GROUP BY target_id
        ) rc ON rc.target_id = t.target_id
        WHERE t.active=1
        ORDER BY
            COALESCE(rc.run_count, 0) ASC,
            t.stop_count DESC,
            CASE WHEN t.last_collected_at IS NULL THEN 0 ELSE 1 END,
            t.last_collected_at,
            t.target_id
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    return TargetRow(
        target_id=str(_row_get(row, "target_id", "") or ""),
        site_id=str(_row_get(row, "site_id", "") or ""),
        route_type=str(_row_get(row, "route_type", "") or ""),
        route_uuid=str(_row_get(row, "route_uuid", "") or ""),
        route_id=int(_row_get(row, "route_id", 0) or 0),
        route_name=str(_row_get(row, "route_name", "") or ""),
        day_type=str(_row_get(row, "day_type", "") or ""),
        departure_time=str(_row_get(row, "departure_time", "") or ""),
        stop_count=int(_row_get(row, "stop_count", 0) or 0),
        api_call_cost=int(_row_get(row, "api_call_cost", 1) or 1),
        last_collected_at=str(_row_get(row, "last_collected_at", "") or "") or None,
    )


def fetch_target_stops(conn: sqlite3.Connection, target: TargetRow) -> list[StopRow]:
    row = conn.execute(
        """
        SELECT MIN(variant_id) AS variant_id
        FROM service_variant
        WHERE route_id = ? AND day_type = ? AND departure_time = ?
        """,
        (target.route_id, target.day_type, target.departure_time),
    ).fetchone()
    if row is None or row[0] is None:
        return []
    variant_id = int(row[0])
    rows = conn.execute(
        """
        SELECT
            vs.seq AS seq,
            s.stop_id AS stop_id,
            COALESCE(s.stop_uuid, '') AS stop_uuid,
            COALESCE(s.name, '') AS stop_name,
            s.lat AS lat,
            s.lon AS lon,
            vs.estimated_elapsed_min AS estimated_elapsed_min,
            vs.cumulative_distance_m AS cumulative_distance_m
        FROM variant_stop vs
        JOIN stop s ON s.stop_id = vs.stop_id
        WHERE vs.variant_id = ?
        ORDER BY vs.seq
        """,
        (variant_id,),
    ).fetchall()
    stops: list[StopRow] = []
    for r in rows:
        try:
            seq = int(_row_get(r, "seq", 0) or 0)
            stop_id = int(_row_get(r, "stop_id", 0) or 0)
            lat = float(_row_get(r, "lat", 0.0) or 0.0)
            lon = float(_row_get(r, "lon", 0.0) or 0.0)
        except (TypeError, ValueError):
            continue
        if seq <= 0 or stop_id <= 0:
            continue
        est = _row_get(r, "estimated_elapsed_min", None)
        cum = _row_get(r, "cumulative_distance_m", None)
        try:
            est_i = int(est) if est is not None else None
        except (TypeError, ValueError):
            est_i = None
        try:
            cum_i = int(round(float(cum))) if cum is not None else None
        except (TypeError, ValueError):
            cum_i = None
        stops.append(
            StopRow(
                seq=seq,
                stop_id=stop_id,
                stop_uuid=str(_row_get(r, "stop_uuid", "") or ""),
                stop_name=str(_row_get(r, "stop_name", "") or ""),
                lat=lat,
                lon=lon,
                estimated_elapsed_min=est_i,
                cumulative_distance_m=cum_i,
            )
        )
    return stops


def trim_runs(conn: sqlite3.Connection, target_id: str, keep: int = 30) -> None:
    keep = max(1, int(keep))
    conn.execute(
        """
        DELETE FROM tmap_run
        WHERE target_id = ?
          AND run_id NOT IN (
              SELECT run_id
              FROM tmap_run
              WHERE target_id = ?
              ORDER BY called_at_utc DESC, run_id DESC
              LIMIT ?
          )
        """,
        (target_id, target_id, keep),
    )


def mark_target_result(
    conn: sqlite3.Connection,
    target_id: str,
    *,
    status: str,
    http_code: int | None,
) -> None:
    conn.execute(
        """
        UPDATE tmap_target
        SET
            last_collected_at = ?,
            last_run_status = ?,
            last_run_http_code = ?,
            updated_at = ?
        WHERE target_id = ?
        """,
        (utc_now_iso(), status, http_code, utc_now_iso(), target_id),
    )


def merge_tmap_tables(source_db: Path, target_db: Path) -> None:
    if not source_db.exists() or not target_db.exists():
        return
    conn = sqlite3.connect(str(target_db))
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        ensure_schema(conn)
        conn.execute("ATTACH DATABASE ? AS src", (str(source_db),))
        for table in TMAP_TABLES:
            exists = conn.execute(
                "SELECT 1 FROM src.sqlite_master WHERE type='table' AND name = ? LIMIT 1",
                (table,),
            ).fetchone()
            if not exists:
                continue
            conn.execute(f"INSERT OR REPLACE INTO {table} SELECT * FROM src.{table}")
        conn.execute("DETACH DATABASE src")
        conn.commit()
    finally:
        conn.close()
