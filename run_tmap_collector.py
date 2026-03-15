#!/usr/bin/env python3
"""TMAP collector using routeSequential30 + routes APIs.

Collection strategy:
1) Prefer routeSequential30 (32-stop window, step 31 overlap).
2) If seq30 is quota-blocked (429 or quota error), switch to routes API.
3) routes API uses 7-stop window (start + 5 via + end), step 6 overlap.
4) Rotate across multiple app keys; if a key is quota-blocked, mark it unavailable.
5) Keep collecting on current API phase until that phase is blocked.
6) Keep the latest N runs per target (source-integrated rolling window).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import random
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
WEEKDAY_BALANCE_LOOKAHEAD_MATCHES = 14
RETRYABLE_ROUTE_ERROR_CODES = {"1100", "1101"}
DEFAULT_SEQ30_DAILY_LIMIT_PER_KEY = 100
DEFAULT_ROUTE_DAILY_LIMIT_PER_KEY = 1000
DEFAULT_CALL_WINDOW_START_HOUR_KST = 8
DEFAULT_CALL_WINDOW_END_HOUR_KST = 17
PAIR_STATUS_PENDING = "pending"
PAIR_STATUS_EXHAUSTED = "exhausted_confirmed"
PAIR_STATUS_BROKEN = "broken"
PAIR_STATUS_SET = {PAIR_STATUS_PENDING, PAIR_STATUS_EXHAUSTED, PAIR_STATUS_BROKEN}


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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _clamp_int(value: int, *, minimum: int, maximum: int) -> int:
    return min(maximum, max(minimum, int(value)))


def _parse_app_keys_from_env() -> list[str]:
    parsed: list[str] = []
    raw_multi = str(os.environ.get("TMAP_APP_KEYS", "") or "").strip()
    if raw_multi:
        parsed.extend([token.strip() for token in re.split(r"[\s,;]+", raw_multi) if token.strip()])
    raw_single = str(os.environ.get("TMAP_APP_KEY", "") or "").strip()
    if raw_single:
        parsed.append(raw_single)
    deduped: list[str] = []
    seen: set[str] = set()
    for key in parsed:
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _parse_iso_datetime(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except Exception:
        return None


def _next_kst_midnight_utc(now_utc: datetime) -> datetime:
    now_kst = now_utc.astimezone(KST)
    next_day = now_kst.date() + timedelta(days=1)
    next_midnight_kst = datetime(
        year=next_day.year,
        month=next_day.month,
        day=next_day.day,
        tzinfo=KST,
    )
    return next_midnight_kst.astimezone(timezone.utc)


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


@dataclass(frozen=True)
class RoutePayloadPlan:
    points: list[StopRow]
    dropped_consecutive: int
    dropped_via_equals_end: int
    start_equals_end: bool

    @property
    def changed(self) -> bool:
        return self.dropped_consecutive > 0 or self.dropped_via_equals_end > 0


def _coord_token(stop: StopRow) -> str:
    return f"{stop.lon:.7f},{stop.lat:.7f}"


def _prepare_route_payload_points(points: list[StopRow]) -> RoutePayloadPlan:
    if len(points) < 2:
        return RoutePayloadPlan(
            points=list(points),
            dropped_consecutive=0,
            dropped_via_equals_end=0,
            start_equals_end=True,
        )

    original_end_token = _coord_token(points[-1])
    filtered: list[StopRow] = [points[0]]
    dropped_consecutive = 0
    dropped_via_equals_end = 0

    for stop in points[1:-1]:
        token = _coord_token(stop)
        if token == original_end_token:
            dropped_via_equals_end += 1
            continue
        if token == _coord_token(filtered[-1]):
            dropped_consecutive += 1
            continue
        filtered.append(stop)

    end_stop = points[-1]
    if _coord_token(filtered[-1]) == _coord_token(end_stop):
        dropped_consecutive += 1
    else:
        filtered.append(end_stop)

    if len(filtered) <= 1:
        start_equals_end = True
    else:
        start_equals_end = _coord_token(filtered[0]) == _coord_token(filtered[-1])

    return RoutePayloadPlan(
        points=filtered,
        dropped_consecutive=dropped_consecutive,
        dropped_via_equals_end=dropped_via_equals_end,
        start_equals_end=start_equals_end,
    )


def _is_retryable_route_failure(http_code: int | None, error_code: str | None, error_message: str | None) -> bool:
    token = str(error_code or "").strip()
    if token in RETRYABLE_ROUTE_ERROR_CODES:
        return True
    if int(http_code or 0) == 400 and token == "400":
        return True
    msg = str(error_message or "")
    if "WR312" in msg:
        return True
    return any(code in msg for code in RETRYABLE_ROUTE_ERROR_CODES)


def _build_seq30_payload(points: list[StopRow], start_time: str) -> dict[str, Any]:
    start = points[0]
    end = points[-1]
    via = points[1:-1]
    payload: dict[str, Any] = {
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
    }
    if via:
        payload["viaPoints"] = [
            {
                "viaPointId": f"vp{idx:02d}",
                "viaPointName": _point_name(stop),
                "viaX": f"{stop.lon:.7f}",
                "viaY": f"{stop.lat:.7f}",
            }
            for idx, stop in enumerate(via, start=1)
        ]
    return payload


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


def _mask_app_key(app_key: str) -> str:
    token = str(app_key or "").strip()
    if len(token) <= 8:
        if len(token) <= 2:
            return "***"
        return f"{token[:1]}***{token[-1:]}"
    return f"{token[:4]}...{token[-4:]}"


@dataclass
class CollectorConfig:
    local_db_path: Path
    tmap_app_keys: tuple[str, ...]
    seq30_url: str
    route_url: str
    request_timeout_sec: float
    cooldown_sec: int
    publish_every_calls: int
    keep_runs_per_target: int
    idle_sleep_sec: int
    seq30_daily_limit_per_key: int
    route_daily_limit_per_key: int
    call_window_start_hour_kst: int
    call_window_end_hour_kst: int
    pacing_jitter_min: float
    pacing_jitter_max: float
    pacing_min_sleep_sec: int
    drain_jitter_min_sec: int
    drain_jitter_max_sec: int
    require_all_pairs_exhausted: bool
    drain_pair_failure_threshold: int
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
    weekday_usage: dict[int, int] | None = None,
) -> datetime:
    code = str(day_type or "").strip().lower()
    hh, mm = _parse_hhmm(departure_time)
    candidates: list[tuple[int, datetime]] = []
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
            if code != "weekday":
                return candidate_dt
            candidates.append((offset, candidate_dt))
    if not candidates:
        raise RuntimeError(f"No valid future start date for day_type={day_type} departure={departure_time}")

    usage = weekday_usage or {}
    shortlist = candidates[:WEEKDAY_BALANCE_LOOKAHEAD_MATCHES]
    selected = min(
        shortlist,
        key=lambda item: (int(usage.get(item[1].weekday(), 0)), int(item[0])),
    )
    if selected[0] != shortlist[0][0]:
        LOG.debug(
            "Weekday-balanced start date selected nearest=%s selected=%s usage=%s",
            shortlist[0][1].date().isoformat(),
            selected[1].date().isoformat(),
            usage,
        )
    return selected[1]


def _load_target_weekday_usage(conn: sqlite3.Connection, *, target_id: str, limit: int = 200) -> dict[int, int]:
    rows = conn.execute(
        """
        SELECT requested_start_time
        FROM tmap_run
        WHERE target_id = ?
          AND status = 'success'
          AND requested_start_time IS NOT NULL
        ORDER BY called_at_utc DESC, run_id DESC
        LIMIT ?
        """,
        (target_id, max(1, int(limit))),
    ).fetchall()
    usage: dict[int, int] = {}
    for row in rows:
        token = str(row[0] or "").strip()
        if len(token) < 8:
            continue
        date_token = token[:8]
        try:
            kst_date = datetime.strptime(date_token, "%Y%m%d").date()
        except ValueError:
            continue
        wd = int(kst_date.weekday())
        usage[wd] = int(usage.get(wd, 0)) + 1
    return usage


def _state_key_for_api(api_name: str) -> str:
    return f"collector.api_state.{api_name}"


def _app_key_digest(app_key: str) -> str:
    token = str(app_key or "").strip()
    return hashlib.sha1(token.encode("utf-8")).hexdigest()[:16]


def _state_key_for_app_key(api_name: str, app_key: str) -> str:
    digest = _app_key_digest(app_key)
    api_token = str(api_name or "").strip().lower() or "unknown"
    return f"collector.app_key_state.{api_token}.{digest}"


def _state_key_for_api_pacing(api_name: str, kst_day: date) -> str:
    api_token = str(api_name or "").strip().lower() or "unknown"
    return f"collector.api_pacing.{api_token}.{kst_day.strftime('%Y%m%d')}"


def _state_key_for_quota_matrix(kst_day: date) -> str:
    return f"collector.quota_matrix.{kst_day.strftime('%Y%m%d')}"


def _load_state_json(conn: sqlite3.Connection, state_key: str) -> dict[str, Any]:
    row = conn.execute(
        "SELECT state_value FROM tmap_collector_state WHERE state_key = ?",
        (state_key,),
    ).fetchone()
    if row is None or row[0] is None:
        return {}
    raw = str(row[0] or "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _save_state_json(conn: sqlite3.Connection, state_key: str, state: dict[str, Any]) -> None:
    value = json.dumps(state, ensure_ascii=True, separators=(",", ":"))
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO tmap_collector_state (state_key, state_value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            state_value = excluded.state_value,
            updated_at = excluded.updated_at
        """,
        (state_key, value, now),
    )


def _load_collector_state(conn: sqlite3.Connection, state_key: str) -> dict[str, Any]:
    parsed = _load_state_json(conn, state_key)
    return {
        "blocked_until_utc": parsed.get("blocked_until_utc"),
        "last_http_code": parsed.get("last_http_code"),
        "last_error_code": parsed.get("last_error_code"),
    }


def _save_collector_state(conn: sqlite3.Connection, state_key: str, state: dict[str, Any]) -> None:
    _save_state_json(
        conn,
        state_key,
        {
            "blocked_until_utc": state.get("blocked_until_utc"),
            "last_http_code": state.get("last_http_code"),
            "last_error_code": state.get("last_error_code"),
        },
    )


def _get_blocked_until(conn: sqlite3.Connection, *, state_key: str) -> datetime | None:
    state = _load_collector_state(conn, state_key)
    return _parse_iso_datetime(state.get("blocked_until_utc"))


def _clear_block_if_expired(
    conn: sqlite3.Connection,
    *,
    state_key: str,
    now_utc: datetime,
) -> bool:
    state = _load_collector_state(conn, state_key)
    blocked_until = _parse_iso_datetime(state.get("blocked_until_utc"))
    if blocked_until is None:
        return False
    if blocked_until.astimezone(timezone.utc) > now_utc:
        return False
    state["blocked_until_utc"] = None
    _save_collector_state(conn, state_key, state)
    return True


def _set_blocked(
    conn: sqlite3.Connection,
    *,
    state_key: str,
    blocked_until_utc: datetime,
    http_code: int | None,
    error_code: str | None,
) -> None:
    state = _load_collector_state(conn, state_key)
    state["blocked_until_utc"] = blocked_until_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    state["last_http_code"] = int(http_code) if http_code is not None else None
    state["last_error_code"] = str(error_code or "") or None
    _save_collector_state(conn, state_key, state)


class TmapCollector:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.s3_config = S3DbConfig.from_env(default_prefix="db/dev")
        self.s3_store = S3DbStore(self.s3_config) if self.s3_config.enabled else None
        self.current_version_key = ""
        self._app_keys: list[str] = list(config.tmap_app_keys)
        self._app_key_labels: list[str] = [_mask_app_key(k) for k in self._app_keys]
        self._app_key_digests: list[str] = [_app_key_digest(k) for k in self._app_keys]
        self._app_key_cursor = 0
        self._quota_pair_cursor = 0
        self._rng = random.Random()

    def _app_key_state_key(self, *, api_name: str, key_idx: int) -> str:
        app_key = self._app_keys[int(key_idx)]
        return _state_key_for_app_key(api_name, app_key)

    def _window_bounds_for_kst_day(self, kst_day: date) -> tuple[datetime, datetime]:
        start_dt = datetime(
            year=kst_day.year,
            month=kst_day.month,
            day=kst_day.day,
            hour=self.config.call_window_start_hour_kst,
            minute=0,
            tzinfo=KST,
        )
        end_dt = datetime(
            year=kst_day.year,
            month=kst_day.month,
            day=kst_day.day,
            hour=self.config.call_window_end_hour_kst,
            minute=0,
            tzinfo=KST,
        )
        return start_dt, end_dt

    def _is_within_call_window(self, now_kst: datetime) -> bool:
        start_dt, end_dt = self._window_bounds_for_kst_day(now_kst.date())
        return start_dt <= now_kst < end_dt

    def _next_window_start_kst(self, now_kst: datetime) -> datetime:
        start_dt, end_dt = self._window_bounds_for_kst_day(now_kst.date())
        if now_kst < start_dt:
            return start_dt
        if now_kst < end_dt:
            return now_kst
        next_day = now_kst.date() + timedelta(days=1)
        return self._window_bounds_for_kst_day(next_day)[0]

    def _daily_budget_for_api(self, api_name: str) -> int:
        key_count = max(0, len(self._app_keys))
        if api_name == API_SEQ30:
            return max(0, self.config.seq30_daily_limit_per_key * key_count)
        return max(0, self.config.route_daily_limit_per_key * key_count)

    def _load_pacing_calls_used(self, conn: sqlite3.Connection, *, api_name: str, kst_day: date) -> int:
        state = _load_state_json(conn, _state_key_for_api_pacing(api_name, kst_day))
        try:
            used = int(state.get("calls_used") or 0)
        except Exception:
            used = 0
        return max(0, used)

    def _record_pacing_call(self, conn: sqlite3.Connection, *, api_name: str, now_utc: datetime) -> int:
        now_kst = now_utc.astimezone(KST)
        state_key = _state_key_for_api_pacing(api_name, now_kst.date())
        state = _load_state_json(conn, state_key)
        try:
            used = int(state.get("calls_used") or 0)
        except Exception:
            used = 0
        used = max(0, used) + 1
        state["calls_used"] = used
        state["updated_at"] = utc_now_iso()
        _save_state_json(conn, state_key, state)
        conn.commit()
        return used

    def _compute_pacing_sleep(
        self,
        conn: sqlite3.Connection,
        *,
        api_name: str,
        now_utc: datetime,
    ) -> tuple[int, str, int, int, int]:
        now_kst = now_utc.astimezone(KST)
        window_start, window_end = self._window_bounds_for_kst_day(now_kst.date())
        if now_kst < window_start:
            sleep_sec = max(1, int((window_start - now_kst).total_seconds()))
            return sleep_sec, "before_window", 0, 0, 0
        if now_kst >= window_end:
            next_start = self._window_bounds_for_kst_day(now_kst.date() + timedelta(days=1))[0]
            sleep_sec = max(1, int((next_start - now_kst).total_seconds()))
            return sleep_sec, "after_window", 0, 0, 0

        budget = self._daily_budget_for_api(api_name)
        used = self._load_pacing_calls_used(conn, api_name=api_name, kst_day=now_kst.date())
        remaining_calls = max(0, budget - used)
        remaining_window_sec = max(1, int((window_end - now_kst).total_seconds()))
        if remaining_calls <= 0:
            jitter = self._rng.uniform(self.config.pacing_jitter_min, self.config.pacing_jitter_max)
            sleep_sec = max(
                self.config.pacing_min_sleep_sec,
                int(round(float(self.config.pacing_min_sleep_sec) * jitter)),
            )
            sleep_sec = min(max(1, sleep_sec), remaining_window_sec)
            return sleep_sec, "daily_budget_exhausted_soft", 0, remaining_window_sec, budget

        base_interval = float(remaining_window_sec) / float(remaining_calls)
        jitter = self._rng.uniform(self.config.pacing_jitter_min, self.config.pacing_jitter_max)
        sleep_sec = max(self.config.pacing_min_sleep_sec, int(round(base_interval * jitter)))
        sleep_sec = min(max(1, sleep_sec), remaining_window_sec)
        return sleep_sec, "paced", remaining_calls, remaining_window_sec, budget

    def _quota_matrix_state_key(self, kst_day: date) -> str:
        return _state_key_for_quota_matrix(kst_day)

    def _load_quota_matrix_state(
        self,
        conn: sqlite3.Connection,
        *,
        kst_day: date,
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        now = (now_utc or datetime.now(timezone.utc)).replace(microsecond=0)
        now_token = now.isoformat().replace("+00:00", "Z")
        state_key = self._quota_matrix_state_key(kst_day)
        state = _load_state_json(conn, state_key)
        if not isinstance(state, dict):
            state = {}
        changed = False
        day_token = kst_day.strftime("%Y%m%d")
        if str(state.get("kst_day") or "") != day_token:
            state = {"kst_day": day_token, "apis": {}}
            changed = True

        apis = state.get("apis")
        if not isinstance(apis, dict):
            apis = {}
            state["apis"] = apis
            changed = True

        for api_name in (API_SEQ30, API_ROUTE):
            api_state = apis.get(api_name)
            if not isinstance(api_state, dict):
                api_state = {}
                apis[api_name] = api_state
                changed = True
            for idx, digest in enumerate(self._app_key_digests):
                entry = api_state.get(digest)
                if not isinstance(entry, dict):
                    entry = {
                        "status": PAIR_STATUS_PENDING,
                        "attempts": 0,
                        "consecutive_failures": 0,
                        "key_label": self._app_key_labels[idx],
                        "updated_at_utc": now_token,
                    }
                    api_state[digest] = entry
                    changed = True
                    continue
                status = str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()
                if status not in PAIR_STATUS_SET:
                    entry["status"] = PAIR_STATUS_PENDING
                    changed = True
                if "attempts" not in entry:
                    entry["attempts"] = 0
                    changed = True
                if "consecutive_failures" not in entry:
                    entry["consecutive_failures"] = 0
                    changed = True
                if str(entry.get("key_label") or "") != self._app_key_labels[idx]:
                    entry["key_label"] = self._app_key_labels[idx]
                    changed = True
                if "updated_at_utc" not in entry:
                    entry["updated_at_utc"] = now_token
                    changed = True

        if self._sync_quota_matrix_from_blocks(conn, state=state, now_utc=now):
            changed = True

        if changed:
            _save_state_json(conn, state_key, state)
            conn.commit()
        return state

    def _save_quota_matrix_state(self, conn: sqlite3.Connection, *, kst_day: date, state: dict[str, Any]) -> None:
        _save_state_json(conn, self._quota_matrix_state_key(kst_day), state)
        conn.commit()

    def _quota_pair_entry(
        self,
        *,
        state: dict[str, Any],
        api_name: str,
        key_idx: int,
    ) -> dict[str, Any]:
        digest = self._app_key_digests[int(key_idx)]
        apis = state.get("apis")
        if not isinstance(apis, dict):
            apis = {}
            state["apis"] = apis
        api_state = apis.get(api_name)
        if not isinstance(api_state, dict):
            api_state = {}
            apis[api_name] = api_state
        entry = api_state.get(digest)
        if not isinstance(entry, dict):
            entry = {
                "status": PAIR_STATUS_PENDING,
                "attempts": 0,
                "consecutive_failures": 0,
                "key_label": self._app_key_labels[int(key_idx)],
                "updated_at_utc": utc_now_iso(),
            }
            api_state[digest] = entry
        return entry

    def _sync_quota_matrix_from_blocks(
        self,
        conn: sqlite3.Connection,
        *,
        state: dict[str, Any],
        now_utc: datetime,
    ) -> bool:
        changed = False
        for api_name in (API_SEQ30, API_ROUTE):
            for key_idx in range(len(self._app_keys)):
                entry = self._quota_pair_entry(state=state, api_name=api_name, key_idx=key_idx)
                status = str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()
                if status != PAIR_STATUS_PENDING:
                    continue
                state_key = self._app_key_state_key(api_name=api_name, key_idx=key_idx)
                blocked_until = _get_blocked_until(conn, state_key=state_key)
                if blocked_until is None:
                    continue
                blocked_until_utc = blocked_until.astimezone(timezone.utc)
                if blocked_until_utc <= now_utc:
                    continue
                app_state = _load_collector_state(conn, state_key)
                if not _is_quota_block(app_state.get("last_http_code"), app_state.get("last_error_code")):
                    continue
                entry["status"] = PAIR_STATUS_EXHAUSTED
                entry["confirmed_at_utc"] = now_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                entry["last_http_code"] = app_state.get("last_http_code")
                entry["last_error_code"] = app_state.get("last_error_code")
                entry["updated_at_utc"] = entry["confirmed_at_utc"]
                entry["consecutive_failures"] = 0
                changed = True
        return changed

    def _record_quota_pair_attempt(
        self,
        conn: sqlite3.Connection,
        *,
        api_name: str,
        key_idx: int,
        http_code: int | None,
        error_code: str | None,
        error_message: str | None,
        quota_confirmed: bool,
        allow_broken: bool,
        now_utc: datetime | None = None,
    ) -> str:
        now = (now_utc or datetime.now(timezone.utc)).replace(microsecond=0)
        kst_day = now.astimezone(KST).date()
        state = self._load_quota_matrix_state(conn, kst_day=kst_day, now_utc=now)
        entry = self._quota_pair_entry(state=state, api_name=api_name, key_idx=key_idx)
        attempts = 0
        try:
            attempts = int(entry.get("attempts") or 0)
        except Exception:
            attempts = 0
        entry["attempts"] = max(0, attempts) + 1
        entry["last_http_code"] = int(http_code) if http_code is not None else None
        entry["last_error_code"] = str(error_code or "") or None
        entry["last_error_message"] = str(error_message or "")[:1000] or None
        now_token = now.isoformat().replace("+00:00", "Z")
        entry["updated_at_utc"] = now_token

        status = str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()
        if quota_confirmed:
            status = PAIR_STATUS_EXHAUSTED
            entry["status"] = status
            entry["confirmed_at_utc"] = now_token
            entry["consecutive_failures"] = 0
        else:
            if http_code == 200 and not error_code:
                entry["consecutive_failures"] = 0
            else:
                failures = 0
                try:
                    failures = int(entry.get("consecutive_failures") or 0)
                except Exception:
                    failures = 0
                failures = max(0, failures) + 1
                entry["consecutive_failures"] = failures
                if (
                    allow_broken
                    and status == PAIR_STATUS_PENDING
                    and failures >= max(1, int(self.config.drain_pair_failure_threshold))
                ):
                    status = PAIR_STATUS_BROKEN
                    entry["status"] = status
                    entry["broken_at_utc"] = now_token

        self._save_quota_matrix_state(conn, kst_day=kst_day, state=state)
        return str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()

    def _pending_quota_pairs(
        self,
        *,
        state: dict[str, Any],
        include_broken: bool,
    ) -> list[tuple[str, int]]:
        apis = state.get("apis")
        if not isinstance(apis, dict):
            return []
        pairs: list[tuple[str, int]] = []
        for api_name in (API_SEQ30, API_ROUTE):
            api_state = apis.get(api_name)
            if not isinstance(api_state, dict):
                continue
            for key_idx, digest in enumerate(self._app_key_digests):
                entry = api_state.get(digest)
                if not isinstance(entry, dict):
                    pairs.append((api_name, key_idx))
                    continue
                status = str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()
                if status == PAIR_STATUS_PENDING:
                    pairs.append((api_name, key_idx))
                    continue
                if include_broken and status == PAIR_STATUS_BROKEN:
                    pairs.append((api_name, key_idx))
        return pairs

    def _quota_matrix_counts(self, *, state: dict[str, Any]) -> dict[str, int]:
        apis = state.get("apis")
        if not isinstance(apis, dict):
            return {"total": 0, "pending": 0, "exhausted": 0, "broken": 0}
        total = 0
        pending = 0
        exhausted = 0
        broken = 0
        for api_name in (API_SEQ30, API_ROUTE):
            api_state = apis.get(api_name)
            if not isinstance(api_state, dict):
                continue
            for digest in self._app_key_digests:
                entry = api_state.get(digest)
                if not isinstance(entry, dict):
                    total += 1
                    pending += 1
                    continue
                total += 1
                status = str(entry.get("status") or PAIR_STATUS_PENDING).strip().lower()
                if status == PAIR_STATUS_EXHAUSTED:
                    exhausted += 1
                elif status == PAIR_STATUS_BROKEN:
                    broken += 1
                else:
                    pending += 1
        return {"total": total, "pending": pending, "exhausted": exhausted, "broken": broken}

    def _select_next_drain_pair(
        self,
        conn: sqlite3.Connection,
        *,
        pending_pairs: list[tuple[str, int]],
        now_utc: datetime,
    ) -> tuple[tuple[str, int] | None, datetime | None]:
        if not pending_pairs:
            return None, None
        wake_ts: datetime | None = None
        for offset in range(len(pending_pairs)):
            pair = pending_pairs[(self._quota_pair_cursor + offset) % len(pending_pairs)]
            api_name, key_idx = pair
            available, key_wake = self._available_app_key_indexes(
                conn,
                api_name=api_name,
                now_utc=now_utc,
                ordered_indexes=[int(key_idx)],
            )
            if available:
                self._quota_pair_cursor = (self._quota_pair_cursor + offset + 1) % max(1, len(pending_pairs))
                return pair, wake_ts
            if key_wake is not None and (wake_ts is None or key_wake < wake_ts):
                wake_ts = key_wake
        self._quota_pair_cursor = (self._quota_pair_cursor + 1) % max(1, len(pending_pairs))
        return None, wake_ts

    def _compute_drain_sleep_sec(self) -> int:
        low = max(1, int(self.config.drain_jitter_min_sec))
        high = max(low, int(self.config.drain_jitter_max_sec))
        return max(1, int(round(self._rng.uniform(float(low), float(high)))))

    def _build_probe_request(
        self,
        conn: sqlite3.Connection,
        *,
        api_name: str,
        holiday_resolver: HolidayResolver,
    ) -> tuple[str, dict[str, Any], TargetRow] | None:
        target = fetch_next_target(conn)
        if target is None:
            return None
        stops = fetch_target_stops(conn, target)
        if len(stops) < 2:
            return None
        now_kst = datetime.now(timezone.utc).astimezone(KST)
        weekday_usage = None
        if str(target.day_type or "").strip().lower() == "weekday":
            weekday_usage = _load_target_weekday_usage(conn, target_id=target.target_id)
        start_dt = _resolve_start_datetime_kst(
            day_type=target.day_type,
            departure_time=target.departure_time,
            now_kst=now_kst,
            holiday_resolver=holiday_resolver,
            weekday_usage=weekday_usage,
        )
        start_token = _to_kst_yyyymmddhhmm(start_dt)

        if api_name == API_SEQ30:
            chunks = _chunks_seq30(stops)
            if not chunks:
                return None
            _, chunk_points = chunks[0]
            payload = _build_seq30_payload(chunk_points, start_token)
            return f"{self.config.seq30_url}?version=1&format=json", payload, target

        chunks = _chunks_route(stops)
        for _, chunk_points in chunks:
            payload_plan = _prepare_route_payload_points(chunk_points)
            points = payload_plan.points
            if len(points) < 2:
                continue
            if len(points) == 2 and payload_plan.start_equals_end:
                continue
            payload = _build_route_payload(points, start_token)
            return f"{self.config.route_url}?version=1&format=json", payload, target
        return None

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

    def _iter_app_key_indexes(self) -> list[int]:
        total = len(self._app_keys)
        if total <= 0:
            return []
        start = self._app_key_cursor % total
        return [((start + offset) % total) for offset in range(total)]

    def _advance_app_key_cursor(self, used_idx: int) -> None:
        total = len(self._app_keys)
        if total <= 0:
            return
        self._app_key_cursor = (int(used_idx) + 1) % total

    def _available_app_key_indexes(
        self,
        conn: sqlite3.Connection,
        *,
        api_name: str,
        now_utc: datetime,
        ordered_indexes: list[int] | None = None,
    ) -> tuple[list[int], datetime | None]:
        indexes = ordered_indexes if ordered_indexes is not None else list(range(len(self._app_keys)))
        available: list[int] = []
        wake_ts: datetime | None = None
        changed = False
        for idx in indexes:
            state_key = self._app_key_state_key(api_name=api_name, key_idx=idx)
            if _clear_block_if_expired(conn, state_key=state_key, now_utc=now_utc):
                changed = True
            blocked_until = _get_blocked_until(conn, state_key=state_key)
            if blocked_until is None:
                available.append(idx)
                continue
            blocked_until_utc = blocked_until.astimezone(timezone.utc)
            if blocked_until_utc <= now_utc:
                available.append(idx)
                continue
            if wake_ts is None or blocked_until_utc < wake_ts:
                wake_ts = blocked_until_utc
        if changed:
            conn.commit()
        return available, wake_ts

    def _block_app_key_for_quota(
        self,
        conn: sqlite3.Connection,
        *,
        key_idx: int,
        http_code: int | None,
        error_code: str | None,
        api_name: str,
    ) -> datetime:
        now_utc = datetime.now(timezone.utc)
        # Free-tier quota is daily; wait until next KST midnight before reusing this key.
        blocked_until = _next_kst_midnight_utc(now_utc)
        _set_blocked(
            conn,
            state_key=self._app_key_state_key(api_name=api_name, key_idx=key_idx),
            blocked_until_utc=blocked_until,
            http_code=http_code,
            error_code=error_code,
        )
        conn.commit()
        LOG.warning(
            "App key blocked key=%s api=%s until=%s code=%s err=%s",
            self._app_key_labels[key_idx],
            api_name,
            blocked_until.replace(microsecond=0).isoformat(),
            http_code,
            error_code,
        )
        return blocked_until

    def _call_tmap(
        self,
        conn: sqlite3.Connection,
        *,
        api_name: str,
        url: str,
        payload: dict[str, Any],
        forced_key_idx: int | None = None,
        allow_fallback: bool = True,
        skip_internal_pacing: bool = False,
        allow_broken_pair: bool = False,
    ) -> tuple[int, Any | None, str | None, str | None, int]:
        now_utc = datetime.now(timezone.utc)
        ordered_indexes = (
            [int(forced_key_idx)]
            if forced_key_idx is not None
            else self._iter_app_key_indexes()
        )
        candidate_indexes, wake_ts = self._available_app_key_indexes(
            conn,
            api_name=api_name,
            now_utc=now_utc,
            ordered_indexes=ordered_indexes,
        )
        if not candidate_indexes:
            wake_token = (
                wake_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                if wake_ts is not None
                else None
            )
            return 429, None, "all_keys_blocked", wake_token, 0

        attempts = 0
        for key_idx in candidate_indexes:
            attempts += 1
            app_key = self._app_keys[key_idx]
            headers = {
                "Content-Type": "application/json",
                "appKey": app_key,
            }
            if not skip_internal_pacing:
                pace_sleep_sec, pace_reason, remaining_calls, remaining_window_sec, budget = self._compute_pacing_sleep(
                    conn,
                    api_name=api_name,
                    now_utc=datetime.now(timezone.utc),
                )
                LOG.info(
                    "Pacing sleep api=%s key=%s sec=%s reason=%s remaining_calls=%s window_remaining_sec=%s budget=%s",
                    api_name,
                    self._app_key_labels[key_idx],
                    pace_sleep_sec,
                    pace_reason,
                    remaining_calls,
                    remaining_window_sec,
                    budget,
                )
                time.sleep(max(1, int(pace_sleep_sec)))
            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=self.config.request_timeout_sec,
                )
            except Exception as exc:
                self._record_quota_pair_attempt(
                    conn,
                    api_name=api_name,
                    key_idx=key_idx,
                    http_code=None,
                    error_code="request_error",
                    error_message=str(exc),
                    quota_confirmed=False,
                    allow_broken=allow_broken_pair,
                )
                self._advance_app_key_cursor(key_idx)
                return 0, None, "request_error", str(exc), attempts
            self._record_pacing_call(conn, api_name=api_name, now_utc=datetime.now(timezone.utc))

            err_code: str | None = None
            err_message: str | None = None
            body: Any | None = None
            if resp.status_code != 200:
                err_code = str(resp.status_code)
                err_message = (resp.text or "")[:1000]
                try:
                    parsed = resp.json()
                    if isinstance(parsed, dict):
                        err = parsed.get("error")
                        if isinstance(err, dict):
                            err_code = str(err.get("code") or err.get("id") or err_code)
                        err_message = json.dumps(parsed, ensure_ascii=True)[:1000]
                except Exception:
                    pass
                quota_hit = _is_quota_block(resp.status_code, err_code)
                status = self._record_quota_pair_attempt(
                    conn,
                    api_name=api_name,
                    key_idx=key_idx,
                    http_code=int(resp.status_code),
                    error_code=err_code,
                    error_message=err_message,
                    quota_confirmed=quota_hit,
                    allow_broken=allow_broken_pair,
                )
                if status == PAIR_STATUS_BROKEN:
                    LOG.error(
                        "Quota pair marked broken api=%s key=%s code=%s err=%s",
                        api_name,
                        self._app_key_labels[key_idx],
                        resp.status_code,
                        err_code,
                    )
                if quota_hit:
                    self._block_app_key_for_quota(
                        conn,
                        key_idx=key_idx,
                        http_code=int(resp.status_code),
                        error_code=err_code,
                        api_name=api_name,
                    )
                    self._advance_app_key_cursor(key_idx)
                    if allow_fallback and forced_key_idx is None:
                        continue
                    return int(resp.status_code), None, err_code, err_message, attempts
                self._advance_app_key_cursor(key_idx)
                return int(resp.status_code), None, err_code, err_message, attempts

            try:
                parsed_ok = resp.json()
                body = parsed_ok
            except Exception as exc:
                self._record_quota_pair_attempt(
                    conn,
                    api_name=api_name,
                    key_idx=key_idx,
                    http_code=int(resp.status_code),
                    error_code="invalid_json",
                    error_message=f"invalid_json:{exc}",
                    quota_confirmed=False,
                    allow_broken=allow_broken_pair,
                )
                self._advance_app_key_cursor(key_idx)
                return int(resp.status_code), None, "invalid_json", f"invalid_json:{exc}", attempts
            self._record_quota_pair_attempt(
                conn,
                api_name=api_name,
                key_idx=key_idx,
                http_code=int(resp.status_code),
                error_code=None,
                error_message=None,
                quota_confirmed=False,
                allow_broken=False,
            )
            self._advance_app_key_cursor(key_idx)
            return int(resp.status_code), body, None, None, attempts

        # every currently available key was quota-blocked by this call.
        wake_ts_token = None
        now_utc = datetime.now(timezone.utc)
        _, wake_ts = self._available_app_key_indexes(
            conn,
            api_name=api_name,
            now_utc=now_utc,
            ordered_indexes=ordered_indexes if forced_key_idx is not None else list(range(len(self._app_keys))),
        )
        if wake_ts is not None:
            wake_ts_token = wake_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return 429, None, "all_keys_blocked", wake_ts_token, attempts

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
        weekday_usage = None
        if str(target.day_type or "").strip().lower() == "weekday":
            weekday_usage = _load_target_weekday_usage(conn, target_id=target.target_id)
        start_dt = _resolve_start_datetime_kst(
            day_type=target.day_type,
            departure_time=target.departure_time,
            now_kst=now_kst,
            holiday_resolver=holiday_resolver,
            weekday_usage=weekday_usage,
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
            code, body, err_code, err, used_calls = self._call_tmap(
                conn,
                api_name=API_SEQ30,
                url=f"{self.config.seq30_url}?version=1&format=json",
                payload=payload,
            )
            api_calls_used += used_calls
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
        weekday_usage = None
        if str(target.day_type or "").strip().lower() == "weekday":
            weekday_usage = _load_target_weekday_usage(conn, target_id=target.target_id)
        start_dt = _resolve_start_datetime_kst(
            day_type=target.day_type,
            departure_time=target.departure_time,
            now_kst=now_kst,
            holiday_resolver=holiday_resolver,
            weekday_usage=weekday_usage,
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
        normalized_chunks = 0
        retried_chunks = 0
        synthetic_chunks = 0

        for chunk_idx, (global_start_idx, chunk_points) in enumerate(chunks):
            chunk_start_dt = start_dt
            if chunk_idx > 0:
                first_stop = chunk_points[0]
                elapsed_min = int(first_stop.estimated_elapsed_min or 0)
                chunk_start_dt = start_dt + timedelta(minutes=max(0, elapsed_min))

            payload_plan = _prepare_route_payload_points(chunk_points)
            payload_points = payload_plan.points
            if payload_plan.changed:
                normalized_chunks += 1
                LOG.info(
                    "Route chunk normalized target=%s chunk=%s start_seq=%s points=%s->%s drop_consecutive=%s drop_via_end=%s",
                    target.target_id,
                    chunk_idx + 1,
                    global_start_idx + 1,
                    len(chunk_points),
                    len(payload_points),
                    payload_plan.dropped_consecutive,
                    payload_plan.dropped_via_equals_end,
                )

            invalid_payload = len(payload_points) < 2 or (
                len(payload_points) == 2 and payload_plan.start_equals_end
            )
            if invalid_payload:
                synthetic_chunks += 1
                LOG.warning(
                    "Route chunk skipped API due to invalid payload target=%s chunk=%s start_seq=%s points=%s payload_points=%s start_equals_end=%s",
                    target.target_id,
                    chunk_idx + 1,
                    global_start_idx + 1,
                    len(chunk_points),
                    len(payload_points),
                    payload_plan.start_equals_end,
                )
                chunk_stop_rows, chunk_seg_rows = _derive_route_chunk_observations(
                    features=[],
                    chunk_points=chunk_points,
                    global_start_idx=global_start_idx,
                    chunk_start_dt=chunk_start_dt,
                )
                stop_rows.extend(chunk_stop_rows)
                seg_rows.extend(chunk_seg_rows)
                continue

            payload = _build_route_payload(payload_points, _to_kst_yyyymmddhhmm(chunk_start_dt))
            code, body, err_code, err, used_calls = self._call_tmap(
                conn,
                api_name=API_ROUTE,
                url=f"{self.config.route_url}?version=1&format=json",
                payload=payload,
            )
            api_calls_used += used_calls
            run_http_code = code if code > 0 else None

            if code != 200 or body is None:
                retryable = _is_retryable_route_failure(code, err_code, err)
                start_end_only = [chunk_points[0], chunk_points[-1]]
                start_end_only_valid = len(start_end_only) == 2 and (
                    _coord_token(start_end_only[0]) != _coord_token(start_end_only[1])
                )
                using_start_end_only = (
                    len(payload_points) == 2
                    and _coord_token(payload_points[0]) == _coord_token(start_end_only[0])
                    and _coord_token(payload_points[1]) == _coord_token(start_end_only[1])
                )
                if retryable and start_end_only_valid and not using_start_end_only:
                    retried_chunks += 1
                    LOG.warning(
                        "Route chunk retrying with start/end only target=%s chunk=%s start_seq=%s code=%s err=%s",
                        target.target_id,
                        chunk_idx + 1,
                        global_start_idx + 1,
                        code,
                        err_code,
                    )
                    retry_payload = _build_route_payload(start_end_only, _to_kst_yyyymmddhhmm(chunk_start_dt))
                    code2, body2, err_code2, err2, used_calls2 = self._call_tmap(
                        conn,
                        api_name=API_ROUTE,
                        url=f"{self.config.route_url}?version=1&format=json",
                        payload=retry_payload,
                    )
                    api_calls_used += used_calls2
                    run_http_code = code2 if code2 > 0 else run_http_code
                    code, body, err_code, err = code2, body2, err_code2, err2

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

        LOG.info(
            "Route collection chunk summary target=%s normalized=%s retried=%s synthetic=%s",
            target.target_id,
            normalized_chunks,
            retried_chunks,
            synthetic_chunks,
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

    def run(self) -> int:
        self.config.local_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initial_sync_from_s3()

        conn = self._open_conn()
        try:
            ensure_schema(conn)
            upserted, inactive = refresh_targets(conn)
            conn.commit()
            LOG.info("Targets refreshed upserted=%s inactive=%s", upserted, inactive)
            LOG.info(
                "TMAP app keys configured count=%s labels=%s",
                len(self._app_keys),
                ",".join(self._app_key_labels),
            )
            LOG.info(
                "TMAP pacing config window=%02d:00-%02d:00(KST) seq30_limit_per_key=%s route_limit_per_key=%s jitter=[%s,%s] min_sleep_sec=%s",
                self.config.call_window_start_hour_kst,
                self.config.call_window_end_hour_kst,
                self.config.seq30_daily_limit_per_key,
                self.config.route_daily_limit_per_key,
                self.config.pacing_jitter_min,
                self.config.pacing_jitter_max,
                self.config.pacing_min_sleep_sec,
            )
            LOG.info(
                "TMAP drain config jitter_sec=[%s,%s] require_all_pairs_exhausted=%s pair_failure_threshold=%s",
                self.config.drain_jitter_min_sec,
                self.config.drain_jitter_max_sec,
                self.config.require_all_pairs_exhausted,
                self.config.drain_pair_failure_threshold,
            )

            holiday_resolver = HolidayResolver(conn)
            total_calls = 0
            calls_since_publish = 0
            changed = False
            phase = API_SEQ30

            while True:
                now_utc = datetime.now(timezone.utc)
                now_kst = now_utc.astimezone(KST)
                window_start, window_end = self._window_bounds_for_kst_day(now_kst.date())
                within_window = self._is_within_call_window(now_kst)
                quota_state = self._load_quota_matrix_state(conn, kst_day=now_kst.date(), now_utc=now_utc)
                counts = self._quota_matrix_counts(state=quota_state)
                pending_pairs = self._pending_quota_pairs(
                    state=quota_state,
                    include_broken=False,
                )
                pending_exists = bool(pending_pairs)
                mode = "normal" if within_window else ("drain" if pending_exists else "idle")
                LOG.info(
                    "Quota progress day=%s mode=%s pending=%s exhausted=%s broken=%s total=%s",
                    now_kst.strftime("%Y%m%d"),
                    mode,
                    counts["pending"],
                    counts["exhausted"],
                    counts["broken"],
                    counts["total"],
                )

                if now_kst < window_start:
                    if self.config.run_once:
                        LOG.warning(
                            "Before call window in run_once mode now_kst=%s window=%02d:00-%02d:00",
                            now_kst.replace(microsecond=0).isoformat(),
                            self.config.call_window_start_hour_kst,
                            self.config.call_window_end_hour_kst,
                        )
                        break
                    sleep_sec = max(1, int((window_start - now_kst).total_seconds()))
                    LOG.info(
                        "Before call window now_kst=%s window=%02d:00-%02d:00 sleep_sec=%s",
                        now_kst.replace(microsecond=0).isoformat(),
                        self.config.call_window_start_hour_kst,
                        self.config.call_window_end_hour_kst,
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)
                    continue

                if not within_window and not pending_exists:
                    if self.config.require_all_pairs_exhausted and counts["broken"] > 0:
                        LOG.error(
                            "Drain incomplete: broken quota pairs remain day=%s broken=%s",
                            now_kst.strftime("%Y%m%d"),
                            counts["broken"],
                        )
                        return 1
                    if self.config.run_once:
                        LOG.info(
                            "Outside call window and no pending quota pairs in run_once mode now_kst=%s",
                            now_kst.replace(microsecond=0).isoformat(),
                        )
                        break
                    if changed and calls_since_publish > 0 and self.s3_store is not None:
                        if self._publish_db(conn, reason="outside_call_window"):
                            calls_since_publish = 0
                    next_window_start = self._next_window_start_kst(now_kst)
                    sleep_sec = max(1, int((next_window_start - now_kst).total_seconds()))
                    LOG.info(
                        "Outside call window with no pending quota pairs now_kst=%s sleep_sec=%s",
                        now_kst.replace(microsecond=0).isoformat(),
                        sleep_sec,
                    )
                    time.sleep(sleep_sec)
                    continue

                if not within_window and pending_exists:
                    pair, wake_ts = self._select_next_drain_pair(
                        conn,
                        pending_pairs=pending_pairs,
                        now_utc=now_utc,
                    )
                    if pair is None:
                        if self.config.require_all_pairs_exhausted and counts["broken"] > 0:
                            LOG.error(
                                "Unable to continue drain due to broken quota pairs day=%s broken=%s",
                                now_kst.strftime("%Y%m%d"),
                                counts["broken"],
                            )
                            return 1
                        if self.config.run_once:
                            LOG.warning("No available key for pending drain pair in run_once mode")
                            break
                        if changed and calls_since_publish > 0 and self.s3_store is not None:
                            if self._publish_db(conn, reason="drain_wait"):
                                calls_since_publish = 0
                        sleep_sec = (
                            max(1, int((wake_ts - now_utc).total_seconds()))
                            if wake_ts is not None
                            else max(1, self.config.idle_sleep_sec)
                        )
                        LOG.warning(
                            "Drain pending but all selected keys unavailable pending=%s sleep_sec=%s",
                            len(pending_pairs),
                            sleep_sec,
                        )
                        time.sleep(sleep_sec)
                        continue

                    api_name, key_idx = pair
                    probe = self._build_probe_request(conn, api_name=api_name, holiday_resolver=holiday_resolver)
                    if probe is None:
                        if self.config.run_once:
                            LOG.warning("No probe target available in run_once mode")
                            break
                        upserted, inactive = refresh_targets(conn)
                        conn.commit()
                        sleep_sec = max(1, self.config.idle_sleep_sec)
                        LOG.warning(
                            "Drain pending but probe target unavailable. Refreshed targets upserted=%s inactive=%s sleep_sec=%s",
                            upserted,
                            inactive,
                            sleep_sec,
                        )
                        time.sleep(sleep_sec)
                        continue

                    url, payload, probe_target = probe
                    drain_sleep_sec = self._compute_drain_sleep_sec()
                    LOG.info(
                        "Drain pacing sleep api=%s key=%s sec=%s pending_pairs=%s target=%s",
                        api_name,
                        self._app_key_labels[key_idx],
                        drain_sleep_sec,
                        len(pending_pairs),
                        probe_target.target_id,
                    )
                    time.sleep(drain_sleep_sec)
                    code, _body, err_code, err, used_calls = self._call_tmap(
                        conn,
                        api_name=api_name,
                        url=url,
                        payload=payload,
                        forced_key_idx=key_idx,
                        allow_fallback=False,
                        skip_internal_pacing=True,
                        allow_broken_pair=True,
                    )
                    total_calls += used_calls
                    calls_since_publish += used_calls
                    changed = changed or used_calls > 0
                    LOG.info(
                        "Drain result api=%s key=%s target=%s code=%s err=%s detail=%s attempts=%s",
                        api_name,
                        self._app_key_labels[key_idx],
                        probe_target.target_id,
                        code,
                        err_code,
                        str(err or "")[:240],
                        used_calls,
                    )
                    if changed and self.s3_store is not None and calls_since_publish >= self.config.publish_every_calls:
                        published = self._publish_db(conn, reason="drain_periodic")
                        if published:
                            calls_since_publish = 0
                    if self.config.run_once:
                        break
                    continue

                seq30_keys, seq30_key_wake_ts = self._available_app_key_indexes(
                    conn,
                    api_name=API_SEQ30,
                    now_utc=now_utc,
                )
                route_keys, route_key_wake_ts = self._available_app_key_indexes(
                    conn,
                    api_name=API_ROUTE,
                    now_utc=now_utc,
                )
                seq30_ready = bool(seq30_keys)
                route_ready = bool(route_keys)

                if phase == API_SEQ30 and not seq30_ready and route_ready:
                    phase = API_ROUTE
                elif phase == API_ROUTE and not route_ready and seq30_ready:
                    phase = API_SEQ30

                if (phase == API_SEQ30 and not seq30_ready) or (phase == API_ROUTE and not route_ready):
                    if self.config.run_once:
                        LOG.warning("No available API/key combination in run_once mode")
                        break
                    if changed and calls_since_publish > 0 and self.s3_store is not None:
                        if self._publish_db(conn, reason="all_key_blocked"):
                            calls_since_publish = 0
                    wake_candidates: list[datetime] = []
                    if seq30_key_wake_ts is not None:
                        wake_candidates.append(seq30_key_wake_ts)
                    if route_key_wake_ts is not None:
                        wake_candidates.append(route_key_wake_ts)
                    wake_ts = min(wake_candidates) if wake_candidates else None
                    sleep_sec = (
                        max(1, int((wake_ts - now_utc).total_seconds()))
                        if wake_ts is not None
                        else max(1, self.config.idle_sleep_sec)
                    )
                    LOG.warning(
                        "No available key in normal mode phase=%s seq30_keys=%s route_keys=%s sleep_sec=%s",
                        phase,
                        len(seq30_keys),
                        len(route_keys),
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

                all_keys_blocked = str(err_code or "") == "all_keys_blocked"
                if _is_quota_block(code, err_code) and not all_keys_blocked:
                    LOG.warning("Quota observed api=%s code=%s err=%s", phase, code, err_code)
                    if phase == API_SEQ30:
                        phase = API_ROUTE
                    else:
                        phase = API_SEQ30
                elif all_keys_blocked:
                    if changed and calls_since_publish > 0 and self.s3_store is not None:
                        published = self._publish_db(conn, reason=f"{phase}_all_keys_blocked")
                        if published:
                            calls_since_publish = 0
                    if phase == API_SEQ30 and route_ready:
                        phase = API_ROUTE
                    elif phase == API_ROUTE and seq30_ready:
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
    app_keys = _parse_app_keys_from_env()
    if not app_keys:
        LOG.error("TMAP_APP_KEY or TMAP_APP_KEYS is required")
        return 2

    window_start_hour = _clamp_int(
        _env_int("TMAP_CALL_WINDOW_START_HOUR_KST", DEFAULT_CALL_WINDOW_START_HOUR_KST, minimum=0),
        minimum=0,
        maximum=23,
    )
    window_end_hour = _clamp_int(
        _env_int("TMAP_CALL_WINDOW_END_HOUR_KST", DEFAULT_CALL_WINDOW_END_HOUR_KST, minimum=0),
        minimum=1,
        maximum=23,
    )
    if window_end_hour <= window_start_hour:
        LOG.error(
            "Invalid call window start=%s end=%s. Expect start < end within 0-23.",
            window_start_hour,
            window_end_hour,
        )
        return 2
    pacing_jitter_min = _env_float("TMAP_PACING_JITTER_MIN", 0.75, minimum=0.01)
    pacing_jitter_max = _env_float("TMAP_PACING_JITTER_MAX", 1.35, minimum=0.01)
    if pacing_jitter_max < pacing_jitter_min:
        LOG.warning(
            "TMAP_PACING_JITTER_MAX(%s) < MIN(%s); using MIN for both",
            pacing_jitter_max,
            pacing_jitter_min,
        )
        pacing_jitter_max = pacing_jitter_min
    drain_jitter_min_sec = _env_int("TMAP_DRAIN_JITTER_MIN_SEC", 2, minimum=1)
    drain_jitter_max_sec = _env_int("TMAP_DRAIN_JITTER_MAX_SEC", 12, minimum=1)
    if drain_jitter_max_sec < drain_jitter_min_sec:
        LOG.warning(
            "TMAP_DRAIN_JITTER_MAX_SEC(%s) < MIN_SEC(%s); using MIN for both",
            drain_jitter_max_sec,
            drain_jitter_min_sec,
        )
        drain_jitter_max_sec = drain_jitter_min_sec

    config = CollectorConfig(
        local_db_path=Path(str(args.db_path)).resolve(),
        tmap_app_keys=tuple(app_keys),
        seq30_url=str(os.environ.get("TMAP_ROUTE_SEQUENTIAL30_URL", DEFAULT_TMAP_SEQ30_URL)).strip(),
        route_url=str(os.environ.get("TMAP_ROUTE_URL", DEFAULT_TMAP_ROUTE_URL)).strip(),
        request_timeout_sec=_env_float("TMAP_HTTP_TIMEOUT_SEC", 20.0, minimum=1.0),
        cooldown_sec=_env_int("TMAP_QUOTA_COOLDOWN_SEC", 3600, minimum=60),
        publish_every_calls=_env_int("TMAP_PUBLISH_EVERY_CALLS", 50, minimum=1),
        keep_runs_per_target=_env_int("TMAP_KEEP_RUNS_PER_TARGET", 30, minimum=1),
        idle_sleep_sec=_env_int("TMAP_IDLE_SLEEP_SEC", 60, minimum=1),
        seq30_daily_limit_per_key=_env_int(
            "TMAP_SEQ30_DAILY_LIMIT_PER_KEY",
            DEFAULT_SEQ30_DAILY_LIMIT_PER_KEY,
            minimum=1,
        ),
        route_daily_limit_per_key=_env_int(
            "TMAP_ROUTE_DAILY_LIMIT_PER_KEY",
            DEFAULT_ROUTE_DAILY_LIMIT_PER_KEY,
            minimum=1,
        ),
        call_window_start_hour_kst=window_start_hour,
        call_window_end_hour_kst=window_end_hour,
        pacing_jitter_min=pacing_jitter_min,
        pacing_jitter_max=pacing_jitter_max,
        pacing_min_sleep_sec=_env_int("TMAP_PACING_MIN_SLEEP_SEC", 1, minimum=1),
        drain_jitter_min_sec=drain_jitter_min_sec,
        drain_jitter_max_sec=drain_jitter_max_sec,
        require_all_pairs_exhausted=_env_bool("TMAP_REQUIRE_ALL_PAIRS_EXHAUSTED", default=False),
        drain_pair_failure_threshold=_env_int("TMAP_DRAIN_PAIR_FAILURE_THRESHOLD", 8, minimum=1),
        run_once=bool(args.once),
    )

    collector = TmapCollector(config)
    return collector.run()


if __name__ == "__main__":
    raise SystemExit(main())
