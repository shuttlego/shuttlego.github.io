#!/usr/bin/env python3
"""TMAP collector using routeSequential30 + routes APIs.

Collection strategy:
1) Prefer routeSequential30 (32-stop window, step 31 overlap).
2) If seq30 is quota-blocked (429 or quota error), switch to routes API.
3) routes API uses 7-stop window (start + 5 via + end), step 6 overlap.
4) Keep collecting on current API phase until that phase is blocked.
5) Keep the latest N runs per target (source-integrated rolling window).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from k8s_lease import LeaseLock, LeaseSettings
from s3_db_sync import S3DbConfig, S3DbStore
from tmap_collection import (
    StopRow,
    TargetRow,
    ensure_schema,
    fetch_next_target,
    fetch_target_stops,
    mark_target_result,
    merge_tmap_tables,
    refresh_targets,
    trim_runs,
    utc_now_iso,
)

LOG = logging.getLogger("tmap-collector")

KST = timezone(timedelta(hours=9))
DEFAULT_TMAP_SEQ30_URL = "https://apis.openapi.sk.com/tmap/routes/routeSequential30"
DEFAULT_TMAP_ROUTE_URL = "https://apis.openapi.sk.com/tmap/routes"
API_SEQ30 = "seq30"
API_ROUTE = "route"
POINT_B_PATTERN = re.compile(r"^B(\d+)$")


def _env_int(name: str, default: int, minimum: int = 0) -> int:
    raw = str(os.environ.get(name, str(default)) or str(default)).strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = str(os.environ.get(name, str(default)) or str(default)).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        value = default
    return max(minimum, value)


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None


def _to_kst_yyyymmddhhmm(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y%m%d%H%M")


def _to_kst_hhmmss(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%H%M%S")


def _parse_hhmm(value: str) -> tuple[int, int]:
    token = str(value or "").strip()
    parts = token.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid departure_time: {value}")
    hh = int(parts[0])
    mm = int(parts[1])
    if hh < 0 or hh > 23 or mm < 0 or mm > 59:
        raise ValueError(f"Invalid departure_time: {value}")
    return hh, mm


def _safe_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except Exception:
        return None


def _chunks(stops: list[StopRow], *, window: int, step: int) -> list[tuple[int, list[StopRow]]]:
    window = max(2, int(window))
    step = max(1, int(step))
    n = len(stops)
    chunks: list[tuple[int, list[StopRow]]] = []
    start = 0
    while start < n - 1:
        end = min(n, start + window)
        chunk = stops[start:end]
        if len(chunk) >= 2:
            chunks.append((start, chunk))
        if end >= n:
            break
        start += step
    return chunks


def _chunks_seq30(stops: list[StopRow]) -> list[tuple[int, list[StopRow]]]:
    # 1 call: start + via30 + end = 32 points. overlap 1 point between windows.
    return _chunks(stops, window=32, step=31)


def _chunks_route(stops: list[StopRow]) -> list[tuple[int, list[StopRow]]]:
    # 1 call: start + via5 + end = 7 points. overlap 1 point between windows.
    return _chunks(stops, window=7, step=6)


def _point_name(stop: StopRow) -> str:
    name = str(stop.stop_name or "").strip()
    return name if name else f"stop-{stop.seq}"


def _build_seq30_payload(points: list[StopRow], start_time: str) -> dict[str, Any]:
    start = points[0]
    end = points[-1]
    via = points[1:-1]
    return {
        "reqCoordType": "WGS84GEO",
        "resCoordType": "WGS84GEO",
        "startName": _point_name(start),
        "startX": f"{start.lon:.7f}",
        "startY": f"{start.lat:.7f}",
        "startTime": start_time,
        "endName": _point_name(end),
        "endX": f"{end.lon:.7f}",
        "endY": f"{end.lat:.7f}",
        "searchOption": "0",
        "carType": "1",
        "viaPoints": [
            {
                "viaPointId": f"vp{idx:02d}",
                "viaPointName": _point_name(stop),
                "viaX": f"{stop.lon:.7f}",
                "viaY": f"{stop.lat:.7f}",
            }
            for idx, stop in enumerate(via, start=1)
        ],
    }


def _build_route_payload(points: list[StopRow], start_time: str) -> dict[str, Any]:
    start = points[0]
    end = points[-1]
    via = points[1:-1]
    payload: dict[str, Any] = {
        "startName": _point_name(start),
        "startX": f"{start.lon:.7f}",
        "startY": f"{start.lat:.7f}",
        "endName": _point_name(end),
        "endX": f"{end.lon:.7f}",
        "endY": f"{end.lat:.7f}",
        "startTime": start_time,
        "reqCoordType": "WGS84GEO",
        "resCoordType": "WGS84GEO",
        "searchOption": "0",
        "trafficInfo": "Y",
    }
    if via:
        payload["passList"] = "_".join([f"{s.lon:.7f},{s.lat:.7f}" for s in via])
    return payload


def _encode_polyline_coordinates(feature: dict[str, Any]) -> str:
    geometry = feature.get("geometry") if isinstance(feature, dict) else None
    if not isinstance(geometry, dict):
        return ""
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        return ""
    try:
        return json.dumps(coordinates, ensure_ascii=True, separators=(",", ":"))
    except Exception:
        return ""


def _encode_coordinate_list(coords: list[list[float]]) -> str:
    if len(coords) < 2:
        return ""
    try:
        return json.dumps(coords, ensure_ascii=True, separators=(",", ":"))
    except Exception:
        return ""


def _append_line_coords(dst: list[list[float]], raw_coords: Any) -> None:
    if not isinstance(raw_coords, list):
        return
    for item in raw_coords:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            lon = float(item[0])
            lat = float(item[1])
        except (TypeError, ValueError):
            continue
        point = [lon, lat]
        if dst and dst[-1] == point:
            continue
        dst.append(point)


def _fill_monotonic(values: list[int | None]) -> list[int]:
    n = len(values)
    if n <= 0:
        return []
    known = [(i, int(v)) for i, v in enumerate(values) if v is not None]
    if not known:
        return [0] * n

    out = [0] * n
    first_i, first_v = known[0]
    for i in range(0, first_i + 1):
        out[i] = max(0, first_v)

    for idx in range(len(known) - 1):
        left_i, left_v = known[idx]
        right_i, right_v = known[idx + 1]
        out[left_i] = max(0, left_v)
        span = max(1, right_i - left_i)
        for i in range(left_i + 1, right_i):
            frac = float(i - left_i) / float(span)
            out[i] = max(0, int(round(left_v + (right_v - left_v) * frac)))
        out[right_i] = max(0, right_v)

    last_i, last_v = known[-1]
    for i in range(last_i, n):
        out[i] = max(0, last_v)

    prev = 0
    for i in range(n):
        cur = int(out[i])
        if cur < prev:
            cur = prev
        out[i] = cur
        prev = cur
    return out


def _route_point_to_local_idx(point_type: str, stop_count: int) -> int | None:
    token = str(point_type or "").strip().upper()
    if token == "S":
        return 0
    if token == "E":
        return stop_count - 1
    m = POINT_B_PATTERN.match(token)
    if not m:
        return None
    try:
        via_idx = int(m.group(1))
    except Exception:
        return None
    if via_idx <= 0 or via_idx >= stop_count - 1:
        return None
    return via_idx


def _route_local_point_type(local_idx: int, stop_count: int) -> str:
    if local_idx <= 0:
        return "S"
    if local_idx >= stop_count - 1:
        return "E"
    return f"B{local_idx}"


def _derive_route_chunk_observations(
    *,
    features: list[dict[str, Any]],
    chunk_points: list[StopRow],
    global_start_idx: int,
    chunk_start_dt: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stop_count = len(chunk_points)
    cum_time_sec = 0
    cum_dist_m = 0
    waypoint_metrics: dict[int, tuple[int, int]] = {0: (0, 0)}
    segment_coords: list[list[list[float]]] = [[] for _ in range(max(0, stop_count - 1))]
    active_segment_idx = 0 if stop_count > 1 else None

    for feature in features:
        if not isinstance(feature, dict):
            continue
        geometry = feature.get("geometry")
        props = feature.get("properties")
        if not isinstance(geometry, dict):
            continue
        if not isinstance(props, dict):
            props = {}
        gtype = str(geometry.get("type") or "")

        if gtype == "LineString":
            dist = _safe_int(props.get("distance")) or 0
            seg_time = _safe_int(props.get("time")) or 0
            if dist < 0:
                dist = 0
            if seg_time < 0:
                seg_time = 0
            if active_segment_idx is not None and 0 <= active_segment_idx < len(segment_coords):
                _append_line_coords(segment_coords[active_segment_idx], geometry.get("coordinates"))
            cum_dist_m += dist
            cum_time_sec += seg_time
            continue

        if gtype != "Point":
            continue

        nav_index = _safe_int(props.get("index"))
        if nav_index is None:
            # routes API appends synthetic B1..Bn points with null index at the tail.
            continue
        local_idx = _route_point_to_local_idx(str(props.get("pointType") or ""), stop_count)
        if local_idx is None:
            continue
        if local_idx not in waypoint_metrics:
            waypoint_metrics[local_idx] = (cum_time_sec, cum_dist_m)
        if local_idx >= stop_count - 1:
            active_segment_idx = None
        else:
            active_segment_idx = local_idx

    if stop_count - 1 not in waypoint_metrics:
        waypoint_metrics[stop_count - 1] = (cum_time_sec, cum_dist_m)

    known_time: list[int | None] = [None] * stop_count
    known_dist: list[int | None] = [None] * stop_count
    for idx, (t_sec, d_m) in waypoint_metrics.items():
        if 0 <= idx < stop_count:
            known_time[idx] = max(0, int(t_sec))
            known_dist[idx] = max(0, int(d_m))

    base_est = chunk_points[0].estimated_elapsed_min
    base_cum = chunk_points[0].cumulative_distance_m
    for i, stop in enumerate(chunk_points):
        if known_time[i] is None and base_est is not None and stop.estimated_elapsed_min is not None:
            known_time[i] = max(0, int(stop.estimated_elapsed_min - base_est) * 60)
        if known_dist[i] is None and base_cum is not None and stop.cumulative_distance_m is not None:
            known_dist[i] = max(0, int(stop.cumulative_distance_m - base_cum))

    cum_times = _fill_monotonic(known_time)
    cum_dists = _fill_monotonic(known_dist)

    stop_rows: list[dict[str, Any]] = []
    seg_rows: list[dict[str, Any]] = []

    for i, stop in enumerate(chunk_points):
        arrive_dt = chunk_start_dt + timedelta(seconds=max(0, cum_times[i]))
        seg_dist = None
        if i > 0:
            seg_dist = max(0, int(cum_dists[i] - cum_dists[i - 1]))
        if base_cum is not None:
            cumulative_dist = max(0, int(base_cum + cum_dists[i]))
        else:
            cumulative_dist = stop.cumulative_distance_m
        stop_rows.append(
            {
                "stop_seq": global_start_idx + i + 1,
                "stop_uuid": stop.stop_uuid,
                "stop_id": stop.stop_id,
                "stop_name": stop.stop_name,
                "point_type": _route_local_point_type(i, stop_count),
                "arrive_time": _to_kst_hhmmss(arrive_dt),
                "segment_distance_m": seg_dist,
                "cumulative_distance_m": cumulative_dist,
            }
        )

    for i in range(max(0, stop_count - 1)):
        seg_dist = max(0, int(cum_dists[i + 1] - cum_dists[i]))
        seg_rows.append(
            {
                "from_seq": global_start_idx + i + 1,
                "to_seq": global_start_idx + i + 2,
                "from_stop_uuid": chunk_points[i].stop_uuid,
                "to_stop_uuid": chunk_points[i + 1].stop_uuid,
                "distance_m": seg_dist,
                "encoded_polyline": _encode_coordinate_list(segment_coords[i]),
            }
        )

    return stop_rows, seg_rows


def _is_quota_block(http_code: int | None, error_code: str | None) -> bool:
    code = int(http_code or 0)
    if code == 429:
        return True
    token = str(error_code or "").upper()
    return "QUOTA" in token or "LIMIT_EXCEEDED" in token


@dataclass
class CollectorConfig:
    local_db_path: Path
    tmap_app_key: str
    seq30_url: str
    route_url: str
    request_timeout_sec: float
    cooldown_sec: int
    publish_every_calls: int
    keep_runs_per_target: int
    idle_sleep_sec: int
    run_once: bool


class HolidayResolver:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._years: dict[int, set[str]] = {}

    def _fetch_year(self, year: int) -> set[str]:
        cached = self._years.get(year)
        if cached is not None:
            return cached

        rows = self.conn.execute(
            """
            SELECT holiday_date
            FROM tmap_holiday_cache
            WHERE holiday_date LIKE ? AND is_holiday = 1
            """,
            (f"{year:04d}-%",),
        ).fetchall()
        holidays = {str(r[0]) for r in rows if r and r[0]}
        if holidays:
            self._years[year] = holidays
            return holidays

        url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/KR"
        now_iso = utc_now_iso()
        fetched: set[str] = set()
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, list):
                    for item in payload:
                        if not isinstance(item, dict):
                            continue
                        token = str(item.get("date") or "").strip()
                        if token:
                            fetched.add(token)
                for token in fetched:
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO tmap_holiday_cache (
                            holiday_date, is_holiday, source, fetched_at_utc
                        ) VALUES (?, 1, 'nager', ?)
                        """,
                        (token, now_iso),
                    )
                self.conn.commit()
        except Exception:
            LOG.exception("Failed to fetch holidays year=%s", year)

        self._years[year] = fetched
        return fetched

    def is_holiday(self, d: date) -> bool:
        year_holidays = self._fetch_year(d.year)
        return d.isoformat() in year_holidays


def _matches_day_type(day_type: str, d: date, holiday_resolver: HolidayResolver) -> bool:
    code = str(day_type or "").strip().lower()
    weekday = d.weekday()  # Mon=0
    is_holiday = holiday_resolver.is_holiday(d)
    if code == "monday":
        return weekday == 0
    if code == "weekday":
        return weekday < 5 and not is_holiday
    if code == "saturday":
        return weekday == 5
    if code == "holiday":
        return weekday == 6 or is_holiday
    if code == "familyday":
        return weekday == 4
    return weekday < 5


def _resolve_start_datetime_kst(
    *,
    day_type: str,
    departure_time: str,
    now_kst: datetime,
    holiday_resolver: HolidayResolver,
) -> datetime:
    hh, mm = _parse_hhmm(departure_time)
    for offset in range(0, 400):
        candidate_date = (now_kst + timedelta(days=offset)).date()
        candidate_dt = datetime(
            year=candidate_date.year,
            month=candidate_date.month,
            day=candidate_date.day,
            hour=hh,
            minute=mm,
            tzinfo=KST,
        )
        if candidate_dt <= now_kst:
            continue
        if _matches_day_type(day_type, candidate_date, holiday_resolver):
            return candidate_dt
    raise RuntimeError(f"No valid future start date for day_type={day_type} departure={departure_time}")


def _state_key_for_api(api_name: str) -> str:
    return f"collector.api_state.{api_name}"


def _load_api_state(conn: sqlite3.Connection, api_name: str) -> dict[str, Any]:
    key = _state_key_for_api(api_name)
    row = conn.execute(
        "SELECT state_value FROM tmap_collector_state WHERE state_key = ?",
        (key,),
    ).fetchone()
    if row is None or row[0] is None:
        return {"blocked_until_utc": None, "last_http_code": None, "last_error_code": None}
    raw = str(row[0] or "").strip()
    if not raw:
        return {"blocked_until_utc": None, "last_http_code": None, "last_error_code": None}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return {
                "blocked_until_utc": parsed.get("blocked_until_utc"),
                "last_http_code": parsed.get("last_http_code"),
                "last_error_code": parsed.get("last_error_code"),
            }
    except Exception:
        pass
    return {"blocked_until_utc": None, "last_http_code": None, "last_error_code": None}


def _save_api_state(conn: sqlite3.Connection, api_name: str, state: dict[str, Any]) -> None:
    key = _state_key_for_api(api_name)
    value = json.dumps(
        {
            "blocked_until_utc": state.get("blocked_until_utc"),
            "last_http_code": state.get("last_http_code"),
            "last_error_code": state.get("last_error_code"),
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO tmap_collector_state (state_key, state_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at
        """,
        (key, value, now),
    )


def _get_blocked_until(conn: sqlite3.Connection, api_name: str) -> datetime | None:
    state = _load_api_state(conn, api_name)
    return _parse_iso_datetime(state.get("blocked_until_utc"))


def _clear_block_if_expired(conn: sqlite3.Connection, api_name: str, now_utc: datetime) -> None:
    state = _load_api_state(conn, api_name)
    blocked_until = _parse_iso_datetime(state.get("blocked_until_utc"))
    if blocked_until is None:
        return
    if blocked_until.astimezone(timezone.utc) > now_utc:
        return
    state["blocked_until_utc"] = None
    _save_api_state(conn, api_name, state)


def _set_blocked(
    conn: sqlite3.Connection,
    *,
    api_name: str,
    blocked_until_utc: datetime,
    http_code: int | None,
    error_code: str | None,
) -> None:
    state = _load_api_state(conn, api_name)
    state["blocked_until_utc"] = blocked_until_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    state["last_http_code"] = int(http_code) if http_code is not None else None
    state["last_error_code"] = str(error_code or "") or None
    _save_api_state(conn, api_name, state)


class TmapCollector:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.s3_config = S3DbConfig.from_env(default_prefix="db/dev")
        self.s3_store = S3DbStore(self.s3_config) if self.s3_config.enabled else None
        self.current_version_key = ""

    def _open_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.config.local_db_path))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _initial_sync_from_s3(self) -> None:
        if self.s3_store is None:
            return
        result = self.s3_store.sync_latest_to_local(self.config.local_db_path, current_version_key="")
        if result.version_key:
            self.current_version_key = result.version_key
        LOG.info("Initial S3 sync result=%s version=%s", result.reason, result.version_key)

    def _publish_db(self, conn: sqlite3.Connection, *, reason: str) -> bool:
        if self.s3_store is None:
            return False
        conn.commit()

        latest = self.s3_store.load_manifest()
        if latest is not None and self.current_version_key and latest.db_key != self.current_version_key:
            LOG.info("Detected newer base in S3. Rebase local DB before publish.")
            with tempfile_db_copy(self.config.local_db_path) as old_local_snapshot:
                self.s3_store.download_manifest_db(latest, self.config.local_db_path)
                merge_tmap_tables(old_local_snapshot, self.config.local_db_path)
            self.current_version_key = latest.db_key

        lease_settings = LeaseSettings.from_env()
        lease = LeaseLock(lease_settings)
        acquired = lease.acquire()
        if lease_settings.enabled and not acquired:
            LOG.warning("Skipping publish: failed to acquire lease")
            return False
        try:
            manifest = self.s3_store.publish_local_db(
                self.config.local_db_path,
                updated_by="tmap-collector",
                metadata={"reason": reason},
            )
            self.current_version_key = manifest.db_key
            LOG.info("Published DB version=%s", manifest.db_key)
            return True
        finally:
            lease.release()

    def _call_tmap(
        self,
        *,
        url: str,
        payload: dict[str, Any],
    ) -> tuple[int, dict[str, Any] | None, str | None, str | None]:
        headers = {
            "Content-Type": "application/json",
            "appKey": self.config.tmap_app_key,
        }
        try:
            resp = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=self.config.request_timeout_sec,
            )
        except Exception as exc:
            return 0, None, "request_error", str(exc)

        if resp.status_code != 200:
            err_code = str(resp.status_code)
            err_message = (resp.text or "")[:1000]
            try:
                body = resp.json()
                if isinstance(body, dict):
                    err = body.get("error")
                    if isinstance(err, dict):
                        err_code = str(err.get("code") or err.get("id") or err_code)
                    err_message = json.dumps(body, ensure_ascii=True)[:1000]
            except Exception:
                pass
            return int(resp.status_code), None, err_code, err_message

        try:
            body = resp.json()
        except Exception as exc:
            return int(resp.status_code), None, "invalid_json", f"invalid_json:{exc}"
        return int(resp.status_code), body, None, None

    def _collect_target_seq30(
        self,
        conn: sqlite3.Connection,
        target: TargetRow,
        holiday_resolver: HolidayResolver,
    ) -> tuple[int, str, int | None, str | None]:
        stops = fetch_target_stops(conn, target)
        if len(stops) < 2:
            mark_target_result(conn, target.target_id, status="no_stops", http_code=None)
            conn.commit()
            return 0, "no_stops", None, None

        now_kst = datetime.now(timezone.utc).astimezone(KST)
        start_dt = _resolve_start_datetime_kst(
            day_type=target.day_type,
            departure_time=target.departure_time,
            now_kst=now_kst,
            holiday_resolver=holiday_resolver,
        )

        chunks = _chunks_seq30(stops)
        called_at = utc_now_iso()
        run_status = "success"
        run_http_code: int | None = 200
        error_code = ""
        error_message = ""
        api_calls_used = 0

        stop_rows: list[dict[str, Any]] = []
        seg_rows: list[dict[str, Any]] = []

        for chunk_idx, (global_start_idx, chunk_points) in enumerate(chunks):
            chunk_start_dt = start_dt
            if chunk_idx > 0:
                first_stop = chunk_points[0]
                elapsed_min = int(first_stop.estimated_elapsed_min or 0)
                chunk_start_dt = start_dt + timedelta(minutes=max(0, elapsed_min))

            payload = _build_seq30_payload(chunk_points, _to_kst_yyyymmddhhmm(chunk_start_dt))
            code, body, err_code, err = self._call_tmap(
                url=f"{self.config.seq30_url}?version=1&format=json",
                payload=payload,
            )
            api_calls_used += 1
            run_http_code = code if code > 0 else None

            if code != 200 or body is None:
                run_status = "http_error" if code > 0 else "request_error"
                error_code = str(err_code or (code if code > 0 else "request_error"))
                error_message = str(err or "request failed")[:1000]
                break

            features = body.get("features") if isinstance(body, dict) else None
            if not isinstance(features, list):
                run_status = "invalid_payload"
                error_code = "invalid_payload"
                error_message = "features is missing"
                break

            point_features = [
                f
                for f in features
                if isinstance(f, dict)
                and isinstance(f.get("geometry"), dict)
                and str(f["geometry"].get("type") or "") == "Point"
            ]
            line_features = [
                f
                for f in features
                if isinstance(f, dict)
                and isinstance(f.get("geometry"), dict)
                and str(f["geometry"].get("type") or "") == "LineString"
            ]

            for i, stop in enumerate(chunk_points):
                pf = point_features[i] if i < len(point_features) else {}
                pp = pf.get("properties") if isinstance(pf, dict) else {}
                if not isinstance(pp, dict):
                    pp = {}
                stop_rows.append(
                    {
                        "stop_seq": global_start_idx + i + 1,
                        "stop_uuid": stop.stop_uuid,
                        "stop_id": stop.stop_id,
                        "stop_name": stop.stop_name,
                        "point_type": str(pp.get("pointType") or ""),
                        "arrive_time": str(pp.get("arriveTime") or ""),
                        "segment_distance_m": _safe_int(pp.get("distance")),
                        "cumulative_distance_m": stop.cumulative_distance_m,
                    }
                )

            for i in range(len(chunk_points) - 1):
                lf = line_features[i] if i < len(line_features) else {}
                lp = lf.get("properties") if isinstance(lf, dict) else {}
                if not isinstance(lp, dict):
                    lp = {}
                seg_rows.append(
                    {
                        "from_seq": global_start_idx + i + 1,
                        "to_seq": global_start_idx + i + 2,
                        "from_stop_uuid": chunk_points[i].stop_uuid,
                        "to_stop_uuid": chunk_points[i + 1].stop_uuid,
                        "distance_m": _safe_int(lp.get("distance")),
                        "encoded_polyline": _encode_polyline_coordinates(lf),
                    }
                )

        finished_at = utc_now_iso()
        if run_status == "success" and api_calls_used <= 0:
            run_status = "skipped"

        run_cursor = conn.execute(
            """
            INSERT INTO tmap_run (
                target_id, requested_start_time, called_at_utc, finished_at_utc,
                status, http_code, api_calls_used, error_code, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target.target_id,
                _to_kst_yyyymmddhhmm(start_dt),
                called_at,
                finished_at,
                run_status,
                run_http_code,
                api_calls_used,
                error_code or None,
                error_message or None,
            ),
        )
        run_id = int(run_cursor.lastrowid)

        for row in stop_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO tmap_stop_observation (
                    run_id, stop_seq, stop_uuid, stop_id, stop_name, point_type,
                    arrive_time, segment_distance_m, cumulative_distance_m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row["stop_seq"],
                    row["stop_uuid"],
                    row["stop_id"],
                    row["stop_name"],
                    row["point_type"],
                    row["arrive_time"],
                    row["segment_distance_m"],
                    row["cumulative_distance_m"],
                ),
            )

        for row in seg_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO tmap_segment_observation (
                    run_id, from_seq, to_seq, from_stop_uuid, to_stop_uuid,
                    distance_m, encoded_polyline
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row["from_seq"],
                    row["to_seq"],
                    row["from_stop_uuid"],
                    row["to_stop_uuid"],
                    row["distance_m"],
                    row["encoded_polyline"],
                ),
            )

        mark_target_result(conn, target.target_id, status=run_status, http_code=run_http_code)
        trim_runs(conn, target.target_id, keep=self.config.keep_runs_per_target)
        conn.commit()
        return api_calls_used, run_status, run_http_code, (error_code or None)

    def _collect_target_route(
        self,
        conn: sqlite3.Connection,
        target: TargetRow,
        holiday_resolver: HolidayResolver,
    ) -> tuple[int, str, int | None, str | None]:
        stops = fetch_target_stops(conn, target)
        if len(stops) < 2:
            mark_target_result(conn, target.target_id, status="no_stops", http_code=None)
            conn.commit()
            return 0, "no_stops", None, None

        now_kst = datetime.now(timezone.utc).astimezone(KST)
        start_dt = _resolve_start_datetime_kst(
            day_type=target.day_type,
            departure_time=target.departure_time,
            now_kst=now_kst,
            holiday_resolver=holiday_resolver,
        )

        chunks = _chunks_route(stops)
        called_at = utc_now_iso()
        run_status = "success"
        run_http_code: int | None = 200
        error_code = ""
        error_message = ""
        api_calls_used = 0

        stop_rows: list[dict[str, Any]] = []
        seg_rows: list[dict[str, Any]] = []

        for chunk_idx, (global_start_idx, chunk_points) in enumerate(chunks):
            chunk_start_dt = start_dt
            if chunk_idx > 0:
                first_stop = chunk_points[0]
                elapsed_min = int(first_stop.estimated_elapsed_min or 0)
                chunk_start_dt = start_dt + timedelta(minutes=max(0, elapsed_min))

            payload = _build_route_payload(chunk_points, _to_kst_yyyymmddhhmm(chunk_start_dt))
            code, body, err_code, err = self._call_tmap(
                url=f"{self.config.route_url}?version=1&format=json",
                payload=payload,
            )
            api_calls_used += 1
            run_http_code = code if code > 0 else None

            if code != 200 or body is None:
                run_status = "http_error" if code > 0 else "request_error"
                error_code = str(err_code or (code if code > 0 else "request_error"))
                error_message = str(err or "request failed")[:1000]
                break

            features = body.get("features") if isinstance(body, dict) else None
            if not isinstance(features, list):
                run_status = "invalid_payload"
                error_code = "invalid_payload"
                error_message = "features is missing"
                break

            chunk_stop_rows, chunk_seg_rows = _derive_route_chunk_observations(
                features=[f for f in features if isinstance(f, dict)],
                chunk_points=chunk_points,
                global_start_idx=global_start_idx,
                chunk_start_dt=chunk_start_dt,
            )
            stop_rows.extend(chunk_stop_rows)
            seg_rows.extend(chunk_seg_rows)

        finished_at = utc_now_iso()
        if run_status == "success" and api_calls_used <= 0:
            run_status = "skipped"

        run_cursor = conn.execute(
            """
            INSERT INTO tmap_run (
                target_id, requested_start_time, called_at_utc, finished_at_utc,
                status, http_code, api_calls_used, error_code, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                target.target_id,
                _to_kst_yyyymmddhhmm(start_dt),
                called_at,
                finished_at,
                run_status,
                run_http_code,
                api_calls_used,
                error_code or None,
                error_message or None,
            ),
        )
        run_id = int(run_cursor.lastrowid)

        for row in stop_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO tmap_stop_observation (
                    run_id, stop_seq, stop_uuid, stop_id, stop_name, point_type,
                    arrive_time, segment_distance_m, cumulative_distance_m
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row["stop_seq"],
                    row["stop_uuid"],
                    row["stop_id"],
                    row["stop_name"],
                    row["point_type"],
                    row["arrive_time"],
                    row["segment_distance_m"],
                    row["cumulative_distance_m"],
                ),
            )

        for row in seg_rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO tmap_segment_observation (
                    run_id, from_seq, to_seq, from_stop_uuid, to_stop_uuid,
                    distance_m, encoded_polyline
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    row["from_seq"],
                    row["to_seq"],
                    row["from_stop_uuid"],
                    row["to_stop_uuid"],
                    row["distance_m"],
                    row["encoded_polyline"],
                ),
            )

        mark_target_result(conn, target.target_id, status=run_status, http_code=run_http_code)
        trim_runs(conn, target.target_id, keep=self.config.keep_runs_per_target)
        conn.commit()
        return api_calls_used, run_status, run_http_code, (error_code or None)

    def run(self) -> int:
        self.config.local_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initial_sync_from_s3()

        conn = self._open_conn()
        try:
            ensure_schema(conn)
            upserted, inactive = refresh_targets(conn)
            conn.commit()
            LOG.info("Targets refreshed upserted=%s inactive=%s", upserted, inactive)

            holiday_resolver = HolidayResolver(conn)
            total_calls = 0
            calls_since_publish = 0
            changed = False
            phase = API_SEQ30

            while True:
                now_utc = datetime.now(timezone.utc)
                _clear_block_if_expired(conn, API_SEQ30, now_utc)
                _clear_block_if_expired(conn, API_ROUTE, now_utc)
                conn.commit()

                seq30_blocked_until = _get_blocked_until(conn, API_SEQ30)
                route_blocked_until = _get_blocked_until(conn, API_ROUTE)
                seq30_blocked = bool(
                    seq30_blocked_until is not None and now_utc < seq30_blocked_until.astimezone(timezone.utc)
                )
                route_blocked = bool(
                    route_blocked_until is not None and now_utc < route_blocked_until.astimezone(timezone.utc)
                )

                if phase == API_SEQ30 and seq30_blocked:
                    phase = API_ROUTE

                if phase == API_ROUTE and route_blocked:
                    if not seq30_blocked:
                        phase = API_SEQ30
                    else:
                        if changed and calls_since_publish > 0 and self.s3_store is not None:
                            if self._publish_db(conn, reason="all_blocked"):
                                calls_since_publish = 0
                        if self.config.run_once:
                            LOG.warning("Both APIs blocked in run_once mode")
                            break
                        wake_list = [seq30_blocked_until, route_blocked_until]
                        wake_ts = min([d for d in wake_list if d is not None], default=None)
                        if wake_ts is None:
                            sleep_sec = max(1, self.config.idle_sleep_sec)
                        else:
                            sleep_sec = max(1, int((wake_ts.astimezone(timezone.utc) - now_utc).total_seconds()))
                        LOG.warning(
                            "Both APIs blocked seq30_until=%s route_until=%s sleep_sec=%s",
                            seq30_blocked_until.isoformat() if seq30_blocked_until else None,
                            route_blocked_until.isoformat() if route_blocked_until else None,
                            sleep_sec,
                        )
                        time.sleep(sleep_sec)
                        continue

                target = fetch_next_target(conn)
                if target is None:
                    if changed and calls_since_publish > 0 and self.s3_store is not None:
                        if self._publish_db(conn, reason="idle_flush"):
                            calls_since_publish = 0
                    if self.config.run_once:
                        LOG.info("No active targets")
                        break
                    upserted, inactive = refresh_targets(conn)
                    conn.commit()
                    sleep_sec = max(1, self.config.idle_sleep_sec)
                    LOG.info(
                        "No active targets. Refreshed targets upserted=%s inactive=%s sleep_sec=%s",
                        upserted,
                        inactive,
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)
                    continue

                if phase == API_ROUTE and route_blocked:
                    if not seq30_blocked:
                        phase = API_SEQ30
                    else:
                        sleep_sec = max(1, self.config.idle_sleep_sec)
                        time.sleep(sleep_sec)
                        continue

                if phase == API_SEQ30 and seq30_blocked:
                    phase = API_ROUTE

                if phase == API_SEQ30:
                    calls, status, code, err_code = self._collect_target_seq30(conn, target, holiday_resolver)
                else:
                    calls, status, code, err_code = self._collect_target_route(conn, target, holiday_resolver)

                total_calls += calls
                calls_since_publish += calls
                changed = changed or calls > 0
                LOG.info(
                    "Collected api=%s target=%s route=%s day=%s dep=%s calls=%s status=%s code=%s err=%s",
                    phase,
                    target.target_id,
                    target.route_name,
                    target.day_type,
                    target.departure_time,
                    calls,
                    status,
                    code,
                    err_code,
                )

                if changed and self.s3_store is not None and calls_since_publish >= self.config.publish_every_calls:
                    published = self._publish_db(conn, reason=f"{phase}_periodic")
                    if published:
                        calls_since_publish = 0

                if _is_quota_block(code, err_code):
                    blocked_until = datetime.now(timezone.utc) + timedelta(seconds=self.config.cooldown_sec)
                    _set_blocked(
                        conn,
                        api_name=phase,
                        blocked_until_utc=blocked_until,
                        http_code=code,
                        error_code=err_code,
                    )
                    conn.commit()
                    LOG.warning(
                        "API blocked api=%s until=%s http_code=%s error_code=%s",
                        phase,
                        blocked_until.replace(microsecond=0).isoformat(),
                        code,
                        err_code,
                    )
                    if changed and calls_since_publish > 0 and self.s3_store is not None:
                        published = self._publish_db(conn, reason=f"{phase}_blocked")
                        if published:
                            calls_since_publish = 0
                    if phase == API_SEQ30:
                        phase = API_ROUTE
                    else:
                        # route cycle ended by block; switch back to seq30 next.
                        phase = API_SEQ30

                if self.config.run_once:
                    break

            if changed and self.s3_store is not None:
                self._publish_db(conn, reason="final")
            LOG.info("Collector loop finished total_calls=%s", total_calls)
            return 0
        finally:
            conn.close()


class tempfile_db_copy:
    def __init__(self, src: Path):
        self.src = src
        self.tmp_path: Path | None = None

    def __enter__(self) -> Path:
        self.src.parent.mkdir(parents=True, exist_ok=True)
        fd, raw = tempfile.mkstemp(prefix=f".{self.src.name}.", suffix=".snapshot", dir=str(self.src.parent))
        os.close(fd)
        self.tmp_path = Path(raw)
        with self.src.open("rb") as src_f, self.tmp_path.open("wb") as dst_f:
            while True:
                chunk = src_f.read(1024 * 1024)
                if not chunk:
                    break
                dst_f.write(chunk)
        return self.tmp_path

    def __exit__(self, exc_type, exc, tb):
        if self.tmp_path and self.tmp_path.exists():
            try:
                self.tmp_path.unlink()
            except OSError:
                pass
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TMAP collector")
    parser.add_argument("--once", action="store_true", help="Collect at most one target and exit")
    parser.add_argument(
        "--db-path",
        default=str(Path(os.environ.get("COLLECTOR_DB_PATH", "/app/data/data.db"))),
        help="SQLite DB path",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    args = parse_args()
    app_key = str(os.environ.get("TMAP_APP_KEY", "") or "").strip()
    if not app_key:
        LOG.error("TMAP_APP_KEY is required")
        return 2

    config = CollectorConfig(
        local_db_path=Path(str(args.db_path)).resolve(),
        tmap_app_key=app_key,
        seq30_url=str(os.environ.get("TMAP_ROUTE_SEQUENTIAL30_URL", DEFAULT_TMAP_SEQ30_URL)).strip(),
        route_url=str(os.environ.get("TMAP_ROUTE_URL", DEFAULT_TMAP_ROUTE_URL)).strip(),
        request_timeout_sec=_env_float("TMAP_HTTP_TIMEOUT_SEC", 20.0, minimum=1.0),
        cooldown_sec=_env_int("TMAP_QUOTA_COOLDOWN_SEC", 3600, minimum=60),
        publish_every_calls=_env_int("TMAP_PUBLISH_EVERY_CALLS", 50, minimum=1),
        keep_runs_per_target=_env_int("TMAP_KEEP_RUNS_PER_TARGET", 30, minimum=1),
        idle_sleep_sec=_env_int("TMAP_IDLE_SLEEP_SEC", 60, minimum=1),
        run_once=bool(args.once),
    )

    collector = TmapCollector(config)
    return collector.run()


if __name__ == "__main__":
    raise SystemExit(main())
