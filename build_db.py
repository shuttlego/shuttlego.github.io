#!/usr/bin/env python3
"""
Raw HTML 파일(data/raw/)을 파싱하여 SQLite DB(data/data.db)를 생성하는 ETL 스크립트.

Usage:
    python build_db.py --mode prepare
    python build_db.py --mode release
"""

import argparse
import csv
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import requests

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "data.db"
IDENTITY_DIR = DATA_DIR / "identity"
ROUTE_ALIAS_OVERRIDE_CSV = IDENTITY_DIR / "route_alias_override.csv"
STOP_ALIAS_OVERRIDE_CSV = IDENTITY_DIR / "stop_alias_override.csv"
AUTO_APPLIED_JSONL = IDENTITY_DIR / "identity-auto-applied.jsonl"
REVIEW_QUEUE_JSONL = IDENTITY_DIR / "identity-review-queue.jsonl"
CONFLICTS_JSONL = IDENTITY_DIR / "identity-conflicts.jsonl"
SITES_CSV = DATA_DIR / "sites.csv"
ENV_PATH = Path(__file__).resolve().parent / ".env"
REPORT_ESTIMATED_MINUTES_PER_KM = 2.0

# ── 파일명 매핑 ───────────────────────────────────────────────
ROUTE_TYPE_MAP = {"1": "commute_in", "2": "commute_out", "5": "shuttle"}
DAY_TYPE_MAP = {
    "123001": "weekday",
    "123002": "saturday",
    "123003": "holiday",
    "123004": "monday",
    "123007": "familyday",
}

# ── SQLite 스키마 ─────────────────────────────────────────────
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS site (
    site_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route (
    route_id INTEGER PRIMARY KEY,
    route_uuid TEXT NOT NULL,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    source_route_id TEXT NOT NULL,
    route_name TEXT NOT NULL,
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    UNIQUE(site_id, route_type, source_route_id),
    UNIQUE(route_uuid)
);

CREATE TABLE IF NOT EXISTS service_variant (
    variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_id INTEGER NOT NULL REFERENCES route(route_id),
    day_type TEXT NOT NULL CHECK(day_type IN ('weekday','saturday','holiday','monday','familyday')),
    departure_time TEXT NOT NULL,
    company TEXT,
    bus_count INTEGER,
    UNIQUE(route_id, day_type, departure_time, company)
);

CREATE TABLE IF NOT EXISTS stop (
    stop_id INTEGER PRIMARY KEY,
    stop_uuid TEXT NOT NULL,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    stop_code TEXT NOT NULL,
    name TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    UNIQUE(site_id, stop_code),
    UNIQUE(stop_uuid)
);

CREATE TABLE IF NOT EXISTS variant_stop (
    variant_id INTEGER NOT NULL REFERENCES service_variant(variant_id),
    seq INTEGER NOT NULL,
    stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    cumulative_distance_m REAL,
    estimated_elapsed_min INTEGER,
    PRIMARY KEY(variant_id, seq)
);

CREATE TABLE IF NOT EXISTS stop_scope (
    stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    site_id TEXT NOT NULL REFERENCES site(site_id),
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    day_type TEXT NOT NULL CHECK(day_type IN ('weekday','saturday','holiday','monday','familyday')),
    PRIMARY KEY(stop_id, site_id, route_type, day_type)
);

CREATE TABLE IF NOT EXISTS stop_segment_polyline (
    from_stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    to_stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    encoded_polyline TEXT NOT NULL,
    distance_m REAL,
    PRIMARY KEY(from_stop_id, to_stop_id)
);

CREATE TABLE IF NOT EXISTS stop_segment_no_route (
    from_stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    to_stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    reason TEXT NOT NULL,
    detail TEXT,
    PRIMARY KEY(from_stop_id, to_stop_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS stop_rtree USING rtree(
    stop_id, min_lat, max_lat, min_lon, max_lon
);

CREATE TABLE IF NOT EXISTS route_identity (
    route_uuid TEXT PRIMARY KEY,
    route_id INTEGER NOT NULL UNIQUE,
    site_id TEXT NOT NULL,
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    canonical_name TEXT NOT NULL,
    first_seen_build TEXT NOT NULL,
    last_seen_build TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS route_source (
    site_id TEXT NOT NULL,
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    source_route_id TEXT NOT NULL,
    route_uuid TEXT NOT NULL REFERENCES route_identity(route_uuid),
    first_seen_build TEXT NOT NULL,
    last_seen_build TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(site_id, route_type, source_route_id)
);

CREATE TABLE IF NOT EXISTS stop_identity (
    stop_uuid TEXT PRIMARY KEY,
    stop_id INTEGER NOT NULL UNIQUE,
    site_id TEXT NOT NULL,
    canonical_name TEXT NOT NULL,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    first_seen_build TEXT NOT NULL,
    last_seen_build TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS stop_source (
    site_id TEXT NOT NULL,
    stop_code TEXT NOT NULL,
    stop_uuid TEXT NOT NULL REFERENCES stop_identity(stop_uuid),
    first_seen_build TEXT NOT NULL,
    last_seen_build TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(site_id, stop_code)
);

CREATE TABLE IF NOT EXISTS build_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_route_site_type ON route(site_id, route_type);
CREATE INDEX IF NOT EXISTS idx_variant_route_day ON service_variant(route_id, day_type);
CREATE INDEX IF NOT EXISTS idx_variant_stop_stop ON variant_stop(stop_id);
CREATE INDEX IF NOT EXISTS idx_route_source_uuid ON route_source(route_uuid, active);
CREATE INDEX IF NOT EXISTS idx_stop_source_uuid ON stop_source(stop_uuid, active);
CREATE INDEX IF NOT EXISTS idx_stop_site_code ON stop(site_id, stop_code);
CREATE INDEX IF NOT EXISTS idx_stop_scope_site_type_day ON stop_scope(site_id, route_type, day_type, stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_scope_stop ON stop_scope(stop_id);
CREATE INDEX IF NOT EXISTS idx_segment_polyline_to_stop ON stop_segment_polyline(to_stop_id);
CREATE INDEX IF NOT EXISTS idx_segment_no_route_to_stop ON stop_segment_no_route(to_stop_id);
"""


# ── HTML 파싱 ─────────────────────────────────────────────────
def parse_filename(filename: str) -> tuple[str, str, str] | None:
    """파일명에서 (site_id, route_type, day_type) 추출. 실패 시 None."""
    base = filename.rsplit(".", 1)[0]  # 확장자 제거
    parts = base.split("_")
    if len(parts) != 3:
        return None
    site_id, rt_code, dt_code = parts
    route_type = ROUTE_TYPE_MAP.get(rt_code)
    day_type = DAY_TYPE_MAP.get(dt_code)
    if not route_type or not day_type:
        return None
    return site_id, route_type, day_type


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6371000.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return radius_m * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _format_time(hhmm: str) -> str:
    """'0612' → '06:12'"""
    hhmm = hhmm.strip()
    if len(hhmm) == 4 and hhmm.isdigit():
        return f"{hhmm[:2]}:{hhmm[2:]}"
    return hhmm


def _load_env_file(path: Path) -> dict[str, str]:
    env_map: dict[str, str] = {}
    if not path.exists():
        return env_map
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        env_map[key] = value
    return env_map


def _env_int(
    env_map: dict[str, str],
    key: str,
    default: int,
    *,
    minimum: int = 1,
) -> int:
    raw = (os.environ.get(key) or env_map.get(key) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    if value < minimum:
        return minimum
    return value


SegmentTask = tuple[int, int, tuple[float, float], tuple[float, float]]


def _resolve_otp_graphql_url(env_map: dict[str, str]) -> str:
    base_url = (
        os.environ.get("OTP_HTTP_BASE_URL")
        or env_map.get("OTP_HTTP_BASE_URL")
        or "http://localhost:8888"
    ).strip()
    if not base_url:
        base_url = "http://localhost:8888"
    base_url = base_url.rstrip("/")
    path = (
        os.environ.get("OTP_HTTP_PLAN_PATH")
        or env_map.get("OTP_HTTP_PLAN_PATH")
        or "/otp/gtfs/v1"
    ).strip()
    if not path:
        path = "/otp/gtfs/v1"
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base_url}{path}"


def _segment_coord_key(
    from_coord: tuple[float, float],
    to_coord: tuple[float, float],
) -> tuple[float, float, float, float]:
    return (
        round(from_coord[0], 7),
        round(from_coord[1], 7),
        round(to_coord[0], 7),
        round(to_coord[1], 7),
    )


def _load_existing_segment_cache(
    db_path: Path,
) -> dict[tuple[float, float, float, float], tuple[str, float | None]]:
    if not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        has_segment_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_segment_polyline' LIMIT 1"
        ).fetchone()
        if not has_segment_table:
            conn.close()
            return {}
        rows = conn.execute(
            """
            SELECT
                sf.lat AS from_lat,
                sf.lon AS from_lon,
                st.lat AS to_lat,
                st.lon AS to_lon,
                sp.encoded_polyline,
                sp.distance_m
            FROM stop_segment_polyline sp
            JOIN stop sf ON sf.stop_id = sp.from_stop_id
            JOIN stop st ON st.stop_id = sp.to_stop_id
            """
        ).fetchall()
        conn.close()
    except sqlite3.Error:
        return {}

    cache: dict[tuple[float, float, float, float], tuple[str, float | None]] = {}
    for from_lat, from_lon, to_lat, to_lon, encoded, distance_m in rows:
        encoded_s = str(encoded or "").strip()
        if not encoded_s:
            continue
        key = _segment_coord_key((float(from_lat), float(from_lon)), (float(to_lat), float(to_lon)))
        distance_val: float | None = None
        if distance_m is not None:
            try:
                distance_val = float(distance_m)
            except (TypeError, ValueError):
                distance_val = None
        cache[key] = (encoded_s, distance_val)
    return cache


def _load_existing_no_route_cache(
    db_path: Path,
) -> dict[tuple[float, float, float, float], tuple[str, str | None]]:
    if not db_path.exists():
        return {}
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        has_table = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_segment_no_route' LIMIT 1"
        ).fetchone()
        if not has_table:
            conn.close()
            return {}
        rows = conn.execute(
            """
            SELECT
                sf.lat AS from_lat,
                sf.lon AS from_lon,
                st.lat AS to_lat,
                st.lon AS to_lon,
                nr.reason,
                nr.detail
            FROM stop_segment_no_route nr
            JOIN stop sf ON sf.stop_id = nr.from_stop_id
            JOIN stop st ON st.stop_id = nr.to_stop_id
            """
        ).fetchall()
        conn.close()
    except sqlite3.Error:
        return {}

    cache: dict[tuple[float, float, float, float], tuple[str, str | None]] = {}
    for from_lat, from_lon, to_lat, to_lon, reason, detail in rows:
        reason_s = str(reason or "").strip()
        if not reason_s:
            continue
        key = _segment_coord_key((float(from_lat), float(from_lon)), (float(to_lat), float(to_lon)))
        detail_s = str(detail).strip() if detail is not None else None
        cache[key] = (reason_s, detail_s)
    return cache


class RateLimiter:
    """Simple global rate limiter for OTP calls."""

    def __init__(self, rate_per_sec: int):
        self.rate_per_sec = max(1, int(rate_per_sec))
        self._interval = 1.0 / self.rate_per_sec
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        wait_sec = 0.0
        with self._lock:
            now = time.monotonic()
            if now < self._next_allowed:
                wait_sec = self._next_allowed - now
                self._next_allowed += self._interval
            else:
                self._next_allowed = now + self._interval
        if wait_sec > 0:
            time.sleep(wait_sec)


class OTPPolylineClient:
    def __init__(
        self,
        graphql_url: str,
        timeout_sec: float = 10.0,
        max_retries: int = 2,
        rate_limiter: RateLimiter | None = None,
    ):
        self.graphql_url = graphql_url
        self.timeout_sec = timeout_sec
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter
        self._thread_local = threading.local()

    def _session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session

    def _build_query(self, from_lat: float, from_lon: float, to_lat: float, to_lon: float) -> str:
        return (
            "query { "
            f"plan(from:{{lat:{from_lat:.7f},lon:{from_lon:.7f}}} "
            f"to:{{lat:{to_lat:.7f},lon:{to_lon:.7f}}} "
            "transportModes:[{mode:CAR}]) { "
            "itineraries { legs { distance legGeometry { points } } } "
            "} }"
        )

    @staticmethod
    def _extract_polyline(payload: dict) -> tuple[str, str | None, float | None, str | None]:
        if isinstance(payload, dict) and payload.get("errors"):
            return "graphql_errors", None, None, str(payload.get("errors"))

        try:
            data = payload.get("data") if isinstance(payload, dict) else None
            plan = data.get("plan") if isinstance(data, dict) else None
            itineraries = plan.get("itineraries") if isinstance(plan, dict) else None
        except Exception:  # noqa: BLE001
            itineraries = None

        if itineraries is None:
            return "invalid_payload", None, None, "missing data.plan.itineraries"
        if not isinstance(itineraries, list):
            return "invalid_payload", None, None, "itineraries is not a list"
        if len(itineraries) == 0:
            return "empty_itineraries", None, None, None
        for itinerary in itineraries:
            legs = itinerary.get("legs") if isinstance(itinerary, dict) else None
            if not isinstance(legs, list):
                continue
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                geom = leg.get("legGeometry")
                if not isinstance(geom, dict):
                    continue
                points = str(geom.get("points") or "").strip()
                if not points:
                    continue
                distance_raw = leg.get("distance")
                try:
                    distance_m = float(distance_raw) if distance_raw is not None else None
                except (TypeError, ValueError):
                    distance_m = None
                return "ok", points, distance_m, None
        return "no_points", None, None, None

    def get_segment_polyline(
        self,
        from_lat: float,
        from_lon: float,
        to_lat: float,
        to_lon: float,
    ) -> tuple[str, str | None, float | None, str | None]:
        if from_lat == to_lat and from_lon == to_lon:
            return "same_point", None, 0.0, None
        query = self._build_query(from_lat, from_lon, to_lat, to_lon)
        payload = {"query": query}
        last_status = "unknown_error"
        last_detail: str | None = None
        for _ in range(self.max_retries + 1):
            try:
                if self.rate_limiter is not None:
                    self.rate_limiter.acquire()
                resp = self._session().post(
                    self.graphql_url,
                    json=payload,
                    timeout=self.timeout_sec,
                    headers={"Content-Type": "application/json"},
                )
                resp.raise_for_status()
                data = resp.json()
                status, points, distance_m, detail = self._extract_polyline(data)
                if status == "ok" and points:
                    return status, points, distance_m, detail
                if status == "empty_itineraries":
                    # 경로 없음은 재시도해도 동일할 가능성이 높다.
                    return status, None, None, detail
                last_status = status
                last_detail = detail
            except requests.exceptions.Timeout as exc:
                last_status = "timeout"
                last_detail = str(exc)
            except requests.exceptions.RequestException as exc:
                last_status = "request_error"
                last_detail = str(exc)
            except ValueError as exc:
                last_status = "invalid_json"
                last_detail = str(exc)
            except Exception as exc:  # noqa: BLE001
                last_status = "unknown_error"
                last_detail = str(exc)
        return last_status, None, None, last_detail


def _run_otp_batch(
    conn: sqlite3.Connection,
    otp_client: OTPPolylineClient,
    tasks: list[SegmentTask],
    *,
    max_parallel: int,
    progress_label: str,
) -> tuple[int, int, list[tuple[SegmentTask, str]], Counter[str]]:
    if not tasks:
        return 0, 0, [], Counter()

    future_map = {}
    success_count = 0
    no_route_count = 0
    retry_tasks: list[tuple[SegmentTask, str]] = []
    status_counts: Counter[str] = Counter()
    total_tasks = len(tasks)

    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        for task in tasks:
            from_stop_id, to_stop_id, from_coord, to_coord = task
            future = executor.submit(
                otp_client.get_segment_polyline,
                from_lat=from_coord[0],
                from_lon=from_coord[1],
                to_lat=to_coord[0],
                to_lon=to_coord[1],
            )
            future_map[future] = (from_stop_id, to_stop_id, from_coord, to_coord)

        for idx, future in enumerate(as_completed(future_map), start=1):
            task = future_map[future]
            from_stop_id, to_stop_id, _from_coord, _to_coord = task
            status = "unknown_error"
            points: str | None = None
            distance_m: float | None = None
            detail: str | None = None
            try:
                status, points, distance_m, detail = future.result()
            except Exception:
                status, points, distance_m, detail = "unknown_error", None, None, None

            if status == "ok" and points:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO stop_segment_polyline (
                        from_stop_id, to_stop_id, encoded_polyline, distance_m
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (from_stop_id, to_stop_id, points, distance_m),
                )
                success_count += 1
            elif status == "empty_itineraries":
                conn.execute(
                    """
                    INSERT OR REPLACE INTO stop_segment_no_route (
                        from_stop_id, to_stop_id, reason, detail
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (from_stop_id, to_stop_id, "empty_itineraries", detail),
                )
                no_route_count += 1
                status_counts["empty_itineraries"] += 1
            else:
                retry_tasks.append((task, status))
                status_counts[status] += 1

            if idx % 200 == 0 or idx == total_tasks:
                conn.commit()
                print(
                    f"    {progress_label}: {idx}/{total_tasks} "
                    f"(ok={success_count}, empty_itineraries={status_counts['empty_itineraries']}, "
                    f"timeout={status_counts['timeout']}, retryable={len(retry_tasks)})"
                )

    conn.commit()
    return success_count, no_route_count, retry_tasks, status_counts


def _split_showmap_args(raw: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quote = False
    for ch in raw:
        if ch == "'":
            in_quote = not in_quote
            continue
        if ch == "," and not in_quote:
            token = "".join(current).strip()
            if token:
                parts.append(token)
            else:
                parts.append("")
            current = []
            continue
        current.append(ch)
    tail = "".join(current).strip()
    if tail or raw.endswith(","):
        parts.append(tail)
    return [part.strip().strip("'").strip('"') for part in parts]


def parse_html_file(content: str) -> list[dict]:
    """
    HTML 내용을 파싱하여 노선 블록 리스트를 반환.
    각 블록: {
      source_route_id, route_name, departure_time, company, bus_count,
      route_showmap_tail, stops:[{name, lat, lon, stop_code, showmap_tail}, ...]
    }
    """
    if "등록된 게시글이 없습니다" in content:
        return []

    blocks_raw = re.split(r'<th scope="row">노선명</th>', content)[1:]
    if not blocks_raw:
        return []

    results: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()  # (source_route_id, departure_time)

    for block in blocks_raw:
        td_match = re.search(r"<td[^>]*>(.*?)</td>", block, re.DOTALL)
        if not td_match:
            continue
        td_html = td_match.group(1)
        td_clean = _strip_html(td_html)

        sm_match = re.search(r"showMap\(([^)]+)\)", td_clean)
        if not sm_match:
            continue
        route_args = _split_showmap_args(sm_match.group(1))
        if len(route_args) < 7:
            continue
        source_route_id = str(route_args[0] or "").strip()
        dep_time_raw = str(route_args[1] or "").strip()
        if not source_route_id or not dep_time_raw:
            continue

        dedup_key = (source_route_id, dep_time_raw)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        name_match = re.search(r">(.+?)/a>$", td_clean)
        if not name_match:
            parts = td_clean.rsplit(">", 1)
            route_name = parts[-1].strip() if len(parts) > 1 else td_clean
        else:
            route_name = name_match.group(1).strip()
        if not route_name:
            continue

        company_match = re.search(r"업체명.*?<td[^>]*>([^<]+)</td>", block, re.DOTALL)
        company = company_match.group(1).strip() if company_match else ""

        time_match = re.search(r"출발시간.*?<td>.*?>(\d{4})", block, re.DOTALL)
        departure_time = _format_time(time_match.group(1)) if time_match else _format_time(dep_time_raw)

        bus_match = re.search(r"운행대수.*?<td[^>]*>(\d+)</td>", block, re.DOTALL)
        bus_count = int(bus_match.group(1)) if bus_match else None

        stop_section = re.search(r"경유지.*?<td[^>]*>(.*?)</td>", block, re.DOTALL)
        stops: list[dict] = []
        if stop_section:
            stop_html = stop_section.group(1)
            for match in re.finditer(r"showMap\(([^)]+)\)[^>]*>([^<]+)<", stop_html):
                stop_args = _split_showmap_args(match.group(1))
                stop_name = match.group(2).strip()
                if len(stop_args) < 7 or not stop_name:
                    continue
                try:
                    lat = float(stop_args[2])
                    lon = float(stop_args[3])
                except (TypeError, ValueError):
                    continue
                stop_code = str(stop_args[4] or "").strip()
                if not stop_code:
                    continue
                stops.append(
                    {
                        "name": stop_name,
                        "lat": lat,
                        "lon": lon,
                        "stop_code": stop_code,
                        "showmap_tail": str(stop_args[6] or "").strip(),
                    }
                )

        if not stops:
            continue

        results.append(
            {
                "source_route_id": source_route_id,
                "route_name": route_name,
                "departure_time": departure_time,
                "company": company,
                "bus_count": bus_count,
                "route_showmap_tail": str(route_args[6] or "").strip(),
                "stops": stops,
            }
        )

    return results


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "").strip()).casefold()


def _name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, _normalize_name(a), _normalize_name(b)).ratio()


def _jaccard(values_a: set[tuple[str, str]] | set[str], values_b: set[tuple[str, str]] | set[str]) -> float:
    if not values_a and not values_b:
        return 1.0
    union = values_a | values_b
    if not union:
        return 1.0
    return len(values_a & values_b) / len(union)


def _ensure_override_files(identity_dir: Path) -> None:
    identity_dir.mkdir(parents=True, exist_ok=True)
    if not ROUTE_ALIAS_OVERRIDE_CSV.exists():
        with ROUTE_ALIAS_OVERRIDE_CSV.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "site_id",
                    "route_type",
                    "old_source_route_id",
                    "new_source_route_id",
                    "reason",
                    "approved_by",
                    "approved_at",
                ]
            )
    if not STOP_ALIAS_OVERRIDE_CSV.exists():
        with STOP_ALIAS_OVERRIDE_CSV.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "site_id",
                    "old_stop_code",
                    "new_stop_code",
                    "reason",
                    "approved_by",
                    "approved_at",
                ]
            )


def _load_route_alias_overrides() -> dict[tuple[str, str, str], str]:
    mapping: dict[tuple[str, str, str], str] = {}
    if not ROUTE_ALIAS_OVERRIDE_CSV.exists():
        return mapping
    with ROUTE_ALIAS_OVERRIDE_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            site_id = str(row.get("site_id") or "").strip()
            route_type = str(row.get("route_type") or "").strip()
            old_source = str(row.get("old_source_route_id") or "").strip()
            new_source = str(row.get("new_source_route_id") or "").strip()
            if not site_id or not route_type or not old_source or not new_source:
                continue
            mapping[(site_id, route_type, new_source)] = old_source
    return mapping


def _load_stop_alias_overrides() -> dict[tuple[str, str], str]:
    mapping: dict[tuple[str, str], str] = {}
    if not STOP_ALIAS_OVERRIDE_CSV.exists():
        return mapping
    with STOP_ALIAS_OVERRIDE_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            site_id = str(row.get("site_id") or "").strip()
            old_code = str(row.get("old_stop_code") or "").strip()
            new_code = str(row.get("new_stop_code") or "").strip()
            if not site_id or not old_code or not new_code:
                continue
            mapping[(site_id, new_code)] = old_code
    return mapping


def _write_jsonl(path: Path, items: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False, sort_keys=True))
            f.write("\n")


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_has_columns(conn: sqlite3.Connection, table_name: str, required: set[str]) -> bool:
    if not _table_exists(conn, table_name):
        return False
    cols = {
        str(row[1] or "")
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    return required.issubset(cols)


def _load_existing_identity_state(db_path: Path) -> dict:
    state = {
        "route_identity_by_uuid": {},
        "route_source_map": {},
        "stop_identity_by_uuid": {},
        "stop_source_map": {},
        "route_obs_by_source": {},
        "route_signatures_by_uuid": {},
        "stop_obs_by_key": {},
    }
    if not db_path.exists():
        return state
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error:
        return state

    try:
        has_identity_tables = all(
            _table_exists(conn, table_name)
            for table_name in ("route_identity", "route_source", "stop_identity", "stop_source")
        )
        if not has_identity_tables:
            conn.close()
            return state

        for row in conn.execute("SELECT * FROM route_identity").fetchall():
            route_uuid = str(row["route_uuid"] or "").strip()
            if not route_uuid:
                continue
            state["route_identity_by_uuid"][route_uuid] = {
                "route_id": int(row["route_id"]),
                "site_id": str(row["site_id"] or "").strip(),
                "route_type": str(row["route_type"] or "").strip(),
                "canonical_name": str(row["canonical_name"] or "").strip(),
                "first_seen_build": str(row["first_seen_build"] or "").strip(),
                "last_seen_build": str(row["last_seen_build"] or "").strip(),
                "active": int(row["active"] or 0),
            }

        for row in conn.execute("SELECT * FROM route_source").fetchall():
            key = (
                str(row["site_id"] or "").strip(),
                str(row["route_type"] or "").strip(),
                str(row["source_route_id"] or "").strip(),
            )
            if not key[0] or not key[1] or not key[2]:
                continue
            state["route_source_map"][key] = {
                "route_uuid": str(row["route_uuid"] or "").strip(),
                "first_seen_build": str(row["first_seen_build"] or "").strip(),
                "last_seen_build": str(row["last_seen_build"] or "").strip(),
                "active": int(row["active"] or 0),
            }

        for row in conn.execute("SELECT * FROM stop_identity").fetchall():
            stop_uuid = str(row["stop_uuid"] or "").strip()
            if not stop_uuid:
                continue
            state["stop_identity_by_uuid"][stop_uuid] = {
                "stop_id": int(row["stop_id"]),
                "site_id": str(row["site_id"] or "").strip(),
                "canonical_name": str(row["canonical_name"] or "").strip(),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "first_seen_build": str(row["first_seen_build"] or "").strip(),
                "last_seen_build": str(row["last_seen_build"] or "").strip(),
                "active": int(row["active"] or 0),
            }

        for row in conn.execute("SELECT * FROM stop_source").fetchall():
            key = (
                str(row["site_id"] or "").strip(),
                str(row["stop_code"] or "").strip(),
            )
            if not key[0] or not key[1]:
                continue
            state["stop_source_map"][key] = {
                "stop_uuid": str(row["stop_uuid"] or "").strip(),
                "first_seen_build": str(row["first_seen_build"] or "").strip(),
                "last_seen_build": str(row["last_seen_build"] or "").strip(),
                "active": int(row["active"] or 0),
            }

        can_load_observation = (
            _table_has_columns(conn, "route", {"route_uuid", "source_route_id", "site_id", "route_type", "route_name"})
            and _table_has_columns(conn, "stop", {"stop_uuid", "stop_code", "site_id", "lat", "lon", "name"})
            and _table_exists(conn, "service_variant")
            and _table_exists(conn, "variant_stop")
        )
        if can_load_observation:
            rows = conn.execute(
                """
                SELECT
                    r.route_uuid,
                    r.site_id,
                    r.route_type,
                    r.source_route_id,
                    r.route_name,
                    sv.day_type,
                    sv.departure_time,
                    vs.seq,
                    s.stop_code,
                    s.lat,
                    s.lon,
                    s.name AS stop_name
                FROM route r
                JOIN service_variant sv ON sv.route_id = r.route_id
                JOIN variant_stop vs ON vs.variant_id = sv.variant_id
                JOIN stop s ON s.stop_id = vs.stop_id
                ORDER BY r.route_uuid, sv.day_type, sv.departure_time, vs.seq
                """
            ).fetchall()

            route_obs_by_source: dict[tuple[str, str, str], dict] = {}
            route_signatures_by_uuid: dict[str, dict[tuple[str, str], list[str]]] = {}
            for row in rows:
                source_key = (
                    str(row["site_id"] or "").strip(),
                    str(row["route_type"] or "").strip(),
                    str(row["source_route_id"] or "").strip(),
                )
                route_uuid = str(row["route_uuid"] or "").strip()
                if not source_key[0] or not source_key[1] or not source_key[2] or not route_uuid:
                    continue
                departure_key = (
                    str(row["day_type"] or "").strip(),
                    str(row["departure_time"] or "").strip(),
                )
                stop_code = str(row["stop_code"] or "").strip()
                route_obs = route_obs_by_source.setdefault(
                    source_key,
                    {
                        "route_name": str(row["route_name"] or "").strip(),
                        "departures": set(),
                        "stop_codes": set(),
                    },
                )
                route_obs["departures"].add(departure_key)
                if stop_code:
                    route_obs["stop_codes"].add(stop_code)
                dep_map = route_signatures_by_uuid.setdefault(route_uuid, {})
                dep_codes = dep_map.setdefault(departure_key, [])
                if stop_code:
                    dep_codes.append(stop_code)

            state["route_obs_by_source"] = route_obs_by_source
            state["route_signatures_by_uuid"] = route_signatures_by_uuid

        stop_obs_by_key: dict[tuple[str, str], dict] = {}
        for key, source_row in state["stop_source_map"].items():
            if int(source_row.get("active") or 0) != 1:
                continue
            stop_uuid = str(source_row.get("stop_uuid") or "").strip()
            identity_row = state["stop_identity_by_uuid"].get(stop_uuid)
            if not identity_row:
                continue
            stop_obs_by_key[key] = {
                "name": str(identity_row["canonical_name"] or "").strip(),
                "lat": float(identity_row["lat"]),
                "lon": float(identity_row["lon"]),
            }
        state["stop_obs_by_key"] = stop_obs_by_key
        conn.close()
    except Exception:
        conn.close()
        return state
    return state


def _build_route_observations(blocks: list[dict]) -> dict[tuple[str, str, str], dict]:
    observations: dict[tuple[str, str, str], dict] = {}
    for block in blocks:
        key = (
            str(block["site_id"]),
            str(block["route_type"]),
            str(block["source_route_id"]),
        )
        departure_key = (
            str(block["day_type"]),
            str(block["departure_time"]),
        )
        obs = observations.setdefault(
            key,
            {"route_name_counter": Counter(), "departures": set(), "stop_codes": set()},
        )
        obs["route_name_counter"][str(block["route_name"])] += 1
        obs["departures"].add(departure_key)
        for stop in block["stops"]:
            stop_code = str(stop["stop_code"])
            if stop_code:
                obs["stop_codes"].add(stop_code)
    finalized: dict[tuple[str, str, str], dict] = {}
    for key, obs in observations.items():
        route_name = obs["route_name_counter"].most_common(1)[0][0]
        finalized[key] = {
            "route_name": route_name,
            "departures": set(obs["departures"]),
            "stop_codes": set(obs["stop_codes"]),
        }
    return finalized


def _build_stop_observations(blocks: list[dict]) -> dict[tuple[str, str], dict]:
    observations: dict[tuple[str, str], dict] = {}
    for block in blocks:
        site_id = str(block["site_id"])
        for stop in block["stops"]:
            stop_code = str(stop["stop_code"])
            key = (site_id, stop_code)
            obs = observations.setdefault(
                key,
                {"name_counter": Counter(), "coord_counter": Counter()},
            )
            obs["name_counter"][str(stop["name"])] += 1
            coord_key = (round(float(stop["lat"]), 7), round(float(stop["lon"]), 7))
            obs["coord_counter"][coord_key] += 1

    finalized: dict[tuple[str, str], dict] = {}
    for key, obs in observations.items():
        top_name = obs["name_counter"].most_common(1)[0][0]
        (lat, lon), _count = obs["coord_counter"].most_common(1)[0]
        finalized[key] = {"name": top_name, "lat": float(lat), "lon": float(lon)}
    return finalized


def _score_route_observation(new_obs: dict, old_obs: dict) -> tuple[float, dict]:
    stop_jaccard = _jaccard(set(new_obs["stop_codes"]), set(old_obs["stop_codes"]))
    dep_jaccard = _jaccard(set(new_obs["departures"]), set(old_obs["departures"]))
    name_sim = _name_similarity(str(new_obs["route_name"]), str(old_obs["route_name"]))
    score = (0.55 * stop_jaccard) + (0.35 * dep_jaccard) + (0.10 * name_sim)
    evidence = {
        "stop_jaccard": round(stop_jaccard, 6),
        "departure_jaccard": round(dep_jaccard, 6),
        "name_similarity": round(name_sim, 6),
    }
    return round(score, 6), evidence


def _assign_route_mapping(
    route_obs_new: dict[tuple[str, str, str], dict],
    existing_state: dict,
    route_alias_overrides: dict[tuple[str, str, str], str],
) -> tuple[dict[tuple[str, str, str], str], list[dict], list[dict], list[dict]]:
    route_map: dict[tuple[str, str, str], str] = {}
    auto_items: list[dict] = []
    review_items: list[dict] = []
    conflict_items: list[dict] = []

    prev_route_source_map: dict = existing_state["route_source_map"]
    prev_route_obs_by_source: dict = existing_state["route_obs_by_source"]
    prev_active_keys = {
        key for key, value in prev_route_source_map.items() if int(value.get("active") or 0) == 1
    }

    current_keys = set(route_obs_new.keys())

    for key in sorted(current_keys):
        if key in prev_route_source_map:
            route_map[key] = str(prev_route_source_map[key]["route_uuid"])
            continue
        override_old_source = route_alias_overrides.get(key)
        if override_old_source:
            old_key = (key[0], key[1], override_old_source)
            old_source_row = prev_route_source_map.get(old_key)
            if old_source_row and str(old_source_row.get("route_uuid") or "").strip():
                route_map[key] = str(old_source_row["route_uuid"])
                auto_items.append(
                    {
                        "entity": "route",
                        "decision": "override_applied",
                        "site_id": key[0],
                        "route_type": key[1],
                        "old_source_route_id": override_old_source,
                        "new_source_route_id": key[2],
                        "route_uuid": route_map[key],
                    }
                )

    retired_old_keys = {
        key for key in prev_active_keys if key not in current_keys
    }

    for key in sorted(current_keys):
        if key in route_map:
            continue
        site_id, route_type, source_route_id = key
        new_obs = route_obs_new[key]
        candidates: list[tuple[float, dict, tuple[str, str, str]]] = []
        for old_key in sorted(retired_old_keys):
            if old_key[0] != site_id or old_key[1] != route_type:
                continue
            old_obs = prev_route_obs_by_source.get(old_key)
            if not old_obs:
                continue
            score, evidence = _score_route_observation(new_obs, old_obs)
            if score < 0.75:
                continue
            candidates.append((score, evidence, old_key))
        if not candidates:
            continue
        candidates.sort(key=lambda item: item[0], reverse=True)
        best_score, best_evidence, best_old_key = candidates[0]
        margin = best_score - candidates[1][0] if len(candidates) > 1 else best_score
        review_items.append(
            {
                "entity": "route",
                "site_id": site_id,
                "route_type": route_type,
                "new_source_route_id": source_route_id,
                "recommended_old_source_route_id": best_old_key[2],
                "score": round(best_score, 6),
                "margin": round(margin, 6),
                "recommended_action": "REVIEW",
                "evidence": best_evidence,
            }
        )
        if len(candidates) > 1 and margin < 0.05:
            conflict_items.append(
                {
                    "entity": "route",
                    "site_id": site_id,
                    "route_type": route_type,
                    "new_source_route_id": source_route_id,
                    "recommended_action": "CONFLICT",
                    "reason": "multiple_high_similarity_candidates",
                    "top_candidates": [
                        {
                            "old_source_route_id": candidate_key[2],
                            "score": round(score, 6),
                            "evidence": evidence,
                        }
                        for score, evidence, candidate_key in candidates[:3]
                    ],
                }
            )

    for key in sorted(current_keys):
        if key in route_map:
            continue
        route_map[key] = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"route:{key[0]}:{key[1]}:{key[2]}",
            )
        )
    return route_map, auto_items, review_items, conflict_items


def _build_route_signatures_from_blocks(
    blocks: list[dict],
    route_map: dict[tuple[str, str, str], str],
) -> tuple[dict[str, dict[tuple[str, str], list[str]]], dict[str, str]]:
    signatures: dict[str, dict[tuple[str, str], list[str]]] = {}
    route_site_by_uuid: dict[str, str] = {}
    for block in blocks:
        key = (
            str(block["site_id"]),
            str(block["route_type"]),
            str(block["source_route_id"]),
        )
        route_uuid = route_map.get(key)
        if not route_uuid:
            continue
        route_site_by_uuid[route_uuid] = key[0]
        dep_key = (str(block["day_type"]), str(block["departure_time"]))
        dep_codes = signatures.setdefault(route_uuid, {}).setdefault(dep_key, [])
        dep_codes.extend(str(stop["stop_code"]) for stop in block["stops"] if str(stop["stop_code"]))
    return signatures, route_site_by_uuid


def _collect_stop_alias_reviews(
    route_signatures_old: dict[str, dict[tuple[str, str], list[str]]],
    route_signatures_new: dict[str, dict[tuple[str, str], list[str]]],
    route_site_by_uuid_new: dict[str, str],
    stop_obs_old: dict[tuple[str, str], dict],
    stop_obs_new: dict[tuple[str, str], dict],
    stop_map: dict[tuple[str, str], str],
    existing_state: dict,
) -> tuple[list[dict], list[dict]]:
    review_items: list[dict] = []
    conflict_items: list[dict] = []
    pair_counter: Counter[tuple[str, str, str]] = Counter()
    pair_distance: dict[tuple[str, str, str], float] = {}
    current_stop_keys = set(stop_obs_new.keys())
    prev_stop_source_map: dict = existing_state["stop_source_map"]
    route_identity_by_uuid: dict = existing_state["route_identity_by_uuid"]

    for route_uuid in sorted(set(route_signatures_old) & set(route_signatures_new)):
        old_dep_map = route_signatures_old[route_uuid]
        new_dep_map = route_signatures_new[route_uuid]
        site_id = str(route_site_by_uuid_new.get(route_uuid) or "").strip()
        if not site_id:
            route_identity = route_identity_by_uuid.get(route_uuid) or {}
            site_id = str(route_identity.get("site_id") or "").strip()
        if not site_id:
            continue
        for dep_key in sorted(set(old_dep_map) & set(new_dep_map)):
            old_codes = set(old_dep_map[dep_key])
            new_codes = set(new_dep_map[dep_key])
            removed = sorted(code for code in (old_codes - new_codes) if code)
            added = sorted(code for code in (new_codes - old_codes) if code)
            if not removed or not added:
                continue
            for old_code in removed:
                old_key = (site_id, old_code)
                if old_key in current_stop_keys:
                    continue
                old_obs = stop_obs_old.get(old_key)
                if not old_obs:
                    continue
                for new_code in added:
                    new_key = (site_id, new_code)
                    if new_key in prev_stop_source_map:
                        continue
                    new_obs = stop_obs_new.get(new_key)
                    if not new_obs:
                        continue
                    distance_m = _haversine_m(
                        float(old_obs["lat"]),
                        float(old_obs["lon"]),
                        float(new_obs["lat"]),
                        float(new_obs["lon"]),
                    )
                    pair_key = (site_id, old_code, new_code)
                    pair_counter[pair_key] += 1
                    current_distance = pair_distance.get(pair_key)
                    if current_distance is None or distance_m < current_distance:
                        pair_distance[pair_key] = distance_m

    by_new: dict[tuple[str, str], list[tuple[tuple[str, str, str], int]]] = {}
    by_old: dict[tuple[str, str], list[tuple[tuple[str, str, str], int]]] = {}
    for pair_key, count in pair_counter.items():
        by_new.setdefault((pair_key[0], pair_key[2]), []).append((pair_key, count))
        by_old.setdefault((pair_key[0], pair_key[1]), []).append((pair_key, count))

    for pair_key, count in sorted(pair_counter.items(), key=lambda item: item[1], reverse=True):
        site_id, old_code, new_code = pair_key
        if count < 3:
            continue
        distance_m = float(pair_distance.get(pair_key) or 999999.0)
        if distance_m > 120.0:
            continue
        old_options = sorted(by_old.get((site_id, old_code), []), key=lambda item: item[1], reverse=True)
        new_options = sorted(by_new.get((site_id, new_code), []), key=lambda item: item[1], reverse=True)
        old_margin = old_options[0][1] - old_options[1][1] if len(old_options) > 1 else old_options[0][1]
        new_margin = new_options[0][1] - new_options[1][1] if len(new_options) > 1 else new_options[0][1]
        suggested = {
            "entity": "stop",
            "site_id": site_id,
            "old_stop_code": old_code,
            "new_stop_code": new_code,
            "support_count": int(count),
            "distance_m": round(distance_m, 2),
            "recommended_action": "REVIEW",
            "old_margin": int(old_margin),
            "new_margin": int(new_margin),
        }
        review_items.append(suggested)
        if old_margin <= 1 or new_margin <= 1:
            conflict_items.append(
                {
                    "entity": "stop",
                    "site_id": site_id,
                    "old_stop_code": old_code,
                    "new_stop_code": new_code,
                    "recommended_action": "CONFLICT",
                    "reason": "ambiguous_stop_alias_candidate",
                    "support_count": int(count),
                    "distance_m": round(distance_m, 2),
                }
            )

    dedup_review: list[dict] = []
    seen_review: set[tuple[str, str, str]] = set()
    for item in review_items:
        key = (str(item["site_id"]), str(item["old_stop_code"]), str(item["new_stop_code"]))
        if key in seen_review:
            continue
        seen_review.add(key)
        dedup_review.append(item)

    dedup_conflicts: list[dict] = []
    seen_conflict: set[tuple[str, str, str]] = set()
    for item in conflict_items:
        key = (str(item["site_id"]), str(item["old_stop_code"]), str(item["new_stop_code"]))
        if key in seen_conflict:
            continue
        seen_conflict.add(key)
        dedup_conflicts.append(item)

    return dedup_review, dedup_conflicts


def _assign_stop_mapping(
    stop_obs_new: dict[tuple[str, str], dict],
    existing_state: dict,
    stop_alias_overrides: dict[tuple[str, str], str],
    blocks: list[dict],
    route_map: dict[tuple[str, str, str], str],
) -> tuple[dict[tuple[str, str], str], list[dict], list[dict], list[dict]]:
    stop_map: dict[tuple[str, str], str] = {}
    auto_items: list[dict] = []
    review_items: list[dict] = []
    conflict_items: list[dict] = []
    prev_stop_source_map: dict = existing_state["stop_source_map"]
    prev_stop_obs_by_key: dict = existing_state["stop_obs_by_key"]

    for key in sorted(stop_obs_new.keys()):
        if key in prev_stop_source_map:
            stop_map[key] = str(prev_stop_source_map[key]["stop_uuid"])
            continue
        override_old_code = stop_alias_overrides.get(key)
        if override_old_code:
            old_key = (key[0], override_old_code)
            old_source_row = prev_stop_source_map.get(old_key)
            if old_source_row and str(old_source_row.get("stop_uuid") or "").strip():
                stop_map[key] = str(old_source_row["stop_uuid"])
                auto_items.append(
                    {
                        "entity": "stop",
                        "decision": "override_applied",
                        "site_id": key[0],
                        "old_stop_code": override_old_code,
                        "new_stop_code": key[1],
                        "stop_uuid": stop_map[key],
                    }
                )

    route_signatures_old: dict = existing_state["route_signatures_by_uuid"]
    route_signatures_new, route_site_by_uuid_new = _build_route_signatures_from_blocks(blocks, route_map)
    stop_review, stop_conflicts = _collect_stop_alias_reviews(
        route_signatures_old,
        route_signatures_new,
        route_site_by_uuid_new,
        prev_stop_obs_by_key,
        stop_obs_new,
        stop_map,
        existing_state,
    )
    review_items.extend(stop_review)
    conflict_items.extend(stop_conflicts)

    for key in sorted(stop_obs_new.keys()):
        if key in stop_map:
            continue
        stop_map[key] = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"stop:{key[0]}:{key[1]}",
            )
        )
    return stop_map, auto_items, review_items, conflict_items


def _materialize_identity_state(
    build_id: str,
    existing_state: dict,
    route_map: dict[tuple[str, str, str], str],
    route_obs_new: dict[tuple[str, str, str], dict],
    stop_map: dict[tuple[str, str], str],
    stop_obs_new: dict[tuple[str, str], dict],
) -> dict:
    prev_route_identity = existing_state["route_identity_by_uuid"]
    prev_route_source = existing_state["route_source_map"]
    prev_stop_identity = existing_state["stop_identity_by_uuid"]
    prev_stop_source = existing_state["stop_source_map"]

    route_uuid_to_route_id: dict[str, int] = {
        route_uuid: int(row["route_id"])
        for route_uuid, row in prev_route_identity.items()
    }
    next_route_id = (max(route_uuid_to_route_id.values()) if route_uuid_to_route_id else 0) + 1
    for route_uuid in sorted(set(route_map.values())):
        if route_uuid not in route_uuid_to_route_id:
            route_uuid_to_route_id[route_uuid] = next_route_id
            next_route_id += 1

    stop_uuid_to_stop_id: dict[str, int] = {
        stop_uuid: int(row["stop_id"])
        for stop_uuid, row in prev_stop_identity.items()
    }
    next_stop_id = (max(stop_uuid_to_stop_id.values()) if stop_uuid_to_stop_id else 0) + 1
    for stop_uuid in sorted(set(stop_map.values())):
        if stop_uuid not in stop_uuid_to_stop_id:
            stop_uuid_to_stop_id[stop_uuid] = next_stop_id
            next_stop_id += 1

    route_uuid_to_current_keys: dict[str, list[tuple[str, str, str]]] = {}
    for route_key, route_uuid in route_map.items():
        route_uuid_to_current_keys.setdefault(route_uuid, []).append(route_key)

    stop_uuid_to_current_keys: dict[str, list[tuple[str, str]]] = {}
    for stop_key, stop_uuid in stop_map.items():
        stop_uuid_to_current_keys.setdefault(stop_uuid, []).append(stop_key)

    route_identity_rows: list[tuple] = []
    all_route_uuids = sorted(set(prev_route_identity.keys()) | set(route_uuid_to_current_keys.keys()))
    for route_uuid in all_route_uuids:
        prev_row = prev_route_identity.get(route_uuid)
        current_keys = route_uuid_to_current_keys.get(route_uuid, [])
        active = 1 if current_keys else 0
        if current_keys:
            site_id, route_type, _source_route_id = current_keys[0]
            canonical_name = route_obs_new[current_keys[0]]["route_name"]
        else:
            site_id = str(prev_row["site_id"])
            route_type = str(prev_row["route_type"])
            canonical_name = str(prev_row["canonical_name"])
        first_seen = str(prev_row["first_seen_build"]) if prev_row else build_id
        last_seen = build_id if active else str(prev_row["last_seen_build"])
        route_identity_rows.append(
            (
                route_uuid,
                int(route_uuid_to_route_id[route_uuid]),
                site_id,
                route_type,
                canonical_name,
                first_seen,
                last_seen,
                int(active),
            )
        )

    route_source_rows: list[tuple] = []
    all_route_source_keys = sorted(set(prev_route_source.keys()) | set(route_map.keys()))
    for source_key in all_route_source_keys:
        prev_row = prev_route_source.get(source_key)
        if source_key in route_map:
            route_uuid = route_map[source_key]
            first_seen = str(prev_row["first_seen_build"]) if prev_row else build_id
            last_seen = build_id
            active = 1
        else:
            route_uuid = str(prev_row["route_uuid"])
            first_seen = str(prev_row["first_seen_build"])
            last_seen = str(prev_row["last_seen_build"])
            active = 0
        route_source_rows.append(
            (
                source_key[0],
                source_key[1],
                source_key[2],
                route_uuid,
                first_seen,
                last_seen,
                int(active),
            )
        )

    stop_identity_rows: list[tuple] = []
    all_stop_uuids = sorted(set(prev_stop_identity.keys()) | set(stop_uuid_to_current_keys.keys()))
    for stop_uuid in all_stop_uuids:
        prev_row = prev_stop_identity.get(stop_uuid)
        current_keys = stop_uuid_to_current_keys.get(stop_uuid, [])
        active = 1 if current_keys else 0
        if current_keys:
            site_id, stop_code = current_keys[0]
            current_obs = stop_obs_new[(site_id, stop_code)]
            canonical_name = str(current_obs["name"])
            lat = float(current_obs["lat"])
            lon = float(current_obs["lon"])
        else:
            site_id = str(prev_row["site_id"])
            canonical_name = str(prev_row["canonical_name"])
            lat = float(prev_row["lat"])
            lon = float(prev_row["lon"])
        first_seen = str(prev_row["first_seen_build"]) if prev_row else build_id
        last_seen = build_id if active else str(prev_row["last_seen_build"])
        stop_identity_rows.append(
            (
                stop_uuid,
                int(stop_uuid_to_stop_id[stop_uuid]),
                site_id,
                canonical_name,
                float(lat),
                float(lon),
                first_seen,
                last_seen,
                int(active),
            )
        )

    stop_source_rows: list[tuple] = []
    all_stop_source_keys = sorted(set(prev_stop_source.keys()) | set(stop_map.keys()))
    for source_key in all_stop_source_keys:
        prev_row = prev_stop_source.get(source_key)
        if source_key in stop_map:
            stop_uuid = stop_map[source_key]
            first_seen = str(prev_row["first_seen_build"]) if prev_row else build_id
            last_seen = build_id
            active = 1
        else:
            stop_uuid = str(prev_row["stop_uuid"])
            first_seen = str(prev_row["first_seen_build"])
            last_seen = str(prev_row["last_seen_build"])
            active = 0
        stop_source_rows.append(
            (
                source_key[0],
                source_key[1],
                stop_uuid,
                first_seen,
                last_seen,
                int(active),
            )
        )

    active_route_rows: list[tuple] = []
    for route_uuid, keys in sorted(route_uuid_to_current_keys.items()):
        route_key = sorted(keys)[0]
        source_route_id = route_key[2]
        route_obs = route_obs_new[route_key]
        active_route_rows.append(
            (
                int(route_uuid_to_route_id[route_uuid]),
                route_uuid,
                route_key[0],
                source_route_id,
                str(route_obs["route_name"]),
                route_key[1],
            )
        )

    active_stop_rows: list[tuple] = []
    for stop_uuid, keys in sorted(stop_uuid_to_current_keys.items()):
        stop_key = sorted(keys)[0]
        stop_obs = stop_obs_new[stop_key]
        active_stop_rows.append(
            (
                int(stop_uuid_to_stop_id[stop_uuid]),
                stop_uuid,
                stop_key[0],
                stop_key[1],
                str(stop_obs["name"]),
                float(stop_obs["lat"]),
                float(stop_obs["lon"]),
            )
        )

    return {
        "route_uuid_to_route_id": route_uuid_to_route_id,
        "stop_uuid_to_stop_id": stop_uuid_to_stop_id,
        "route_identity_rows": route_identity_rows,
        "route_source_rows": route_source_rows,
        "stop_identity_rows": stop_identity_rows,
        "stop_source_rows": stop_source_rows,
        "active_route_rows": active_route_rows,
        "active_stop_rows": active_stop_rows,
    }


def _write_identity_artifacts(auto_items: list[dict], review_items: list[dict], conflict_items: list[dict]) -> None:
    _write_jsonl(AUTO_APPLIED_JSONL, auto_items)
    _write_jsonl(REVIEW_QUEUE_JSONL, review_items)
    _write_jsonl(CONFLICTS_JSONL, conflict_items)


def _load_variant_blocks(raw_dir: Path) -> tuple[list[dict], list[tuple[str, int]]]:
    blocks: list[dict] = []
    file_stats: list[tuple[str, int]] = []
    raw_files = sorted(f for f in os.listdir(raw_dir) if os.path.isfile(raw_dir / f))
    for fname in raw_files:
        parsed = parse_filename(fname)
        if not parsed:
            continue
        site_id, route_type, day_type = parsed
        fpath = raw_dir / fname
        try:
            content = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        parsed_blocks = parse_html_file(content)
        if not parsed_blocks:
            continue
        for block in parsed_blocks:
            blocks.append(
                {
                    "site_id": site_id,
                    "route_type": route_type,
                    "day_type": day_type,
                    "source_route_id": str(block["source_route_id"]),
                    "route_name": str(block["route_name"]),
                    "departure_time": str(block["departure_time"]),
                    "company": str(block.get("company") or ""),
                    "bus_count": block.get("bus_count"),
                    "route_showmap_tail": str(block.get("route_showmap_tail") or ""),
                    "stops": list(block["stops"]),
                }
            )
        file_stats.append((fname, len(parsed_blocks)))
    return blocks, file_stats


def _build_database_file(
    output_db_path: Path,
    build_id: str,
    mode: str,
    identity_state: dict,
    blocks: list[dict],
    existing_segment_cache: dict[tuple[float, float, float, float], tuple[str, float | None]],
    existing_no_route_cache: dict[tuple[float, float, float, float], tuple[str, str | None]],
    otp_graphql_url: str,
    otp_max_parallel: int,
    otp_max_rps: int,
    otp_request_retries: int,
    otp_failed_retry_rounds: int,
) -> None:
    if output_db_path.exists():
        output_db_path.unlink()

    otp_client = OTPPolylineClient(
        otp_graphql_url,
        max_retries=otp_request_retries,
        rate_limiter=RateLimiter(otp_max_rps),
    )

    conn = sqlite3.connect(str(output_db_path))
    conn.executescript(SCHEMA_SQL)

    with open(SITES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                "INSERT OR IGNORE INTO site (site_id, name) VALUES (?, ?)",
                (row["site_id"].strip(), row["site_name"].strip()),
            )
    conn.execute("INSERT OR REPLACE INTO build_metadata (key, value) VALUES ('build_id', ?)", (build_id,))
    conn.execute("INSERT OR REPLACE INTO build_metadata (key, value) VALUES ('mode', ?)", (mode,))
    conn.execute("INSERT OR REPLACE INTO build_metadata (key, value) VALUES ('generated_at_utc', ?)", (_utc_now_iso(),))

    conn.executemany(
        """
        INSERT OR REPLACE INTO route_identity (
            route_uuid, route_id, site_id, route_type, canonical_name,
            first_seen_build, last_seen_build, active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        identity_state["route_identity_rows"],
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO route_source (
            site_id, route_type, source_route_id, route_uuid,
            first_seen_build, last_seen_build, active
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        identity_state["route_source_rows"],
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO stop_identity (
            stop_uuid, stop_id, site_id, canonical_name, lat, lon,
            first_seen_build, last_seen_build, active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        identity_state["stop_identity_rows"],
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO stop_source (
            site_id, stop_code, stop_uuid,
            first_seen_build, last_seen_build, active
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        identity_state["stop_source_rows"],
    )

    conn.executemany(
        """
        INSERT OR REPLACE INTO route (
            route_id, route_uuid, site_id, source_route_id, route_name, route_type
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        identity_state["active_route_rows"],
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO stop (
            stop_id, stop_uuid, site_id, stop_code, name, lat, lon
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        identity_state["active_stop_rows"],
    )
    conn.commit()

    route_uuid_by_source: dict[tuple[str, str, str], str] = {}
    for site_id, route_type, source_route_id, route_uuid, _f, _l, active in identity_state["route_source_rows"]:
        if int(active) != 1:
            continue
        route_uuid_by_source[(str(site_id), str(route_type), str(source_route_id))] = str(route_uuid)

    stop_uuid_by_source: dict[tuple[str, str], str] = {}
    for site_id, stop_code, stop_uuid, _f, _l, active in identity_state["stop_source_rows"]:
        if int(active) != 1:
            continue
        stop_uuid_by_source[(str(site_id), str(stop_code))] = str(stop_uuid)

    route_id_by_uuid = {
        str(route_uuid): int(route_id)
        for route_uuid, route_id, *_rest in identity_state["route_identity_rows"]
    }
    stop_id_by_uuid = {
        str(stop_uuid): int(stop_id)
        for stop_uuid, stop_id, *_rest in identity_state["stop_identity_rows"]
    }
    stop_coords_by_id = {
        int(stop_id): (float(lat), float(lon))
        for stop_id, _stop_uuid, _site_id, _stop_code, _name, lat, lon in identity_state["active_stop_rows"]
    }

    segment_pairs: set[tuple[int, int]] = set()
    total_variants = 0
    for block in blocks:
        route_key = (
            str(block["site_id"]),
            str(block["route_type"]),
            str(block["source_route_id"]),
        )
        route_uuid = route_uuid_by_source.get(route_key)
        if not route_uuid:
            continue
        route_id = route_id_by_uuid.get(route_uuid)
        if route_id is None:
            continue

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO service_variant(route_id, day_type, departure_time, company, bus_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(route_id),
                str(block["day_type"]),
                str(block["departure_time"]),
                str(block["company"]),
                block.get("bus_count"),
            ),
        )
        if cur.lastrowid and cur.rowcount > 0:
            variant_id = int(cur.lastrowid)
        else:
            row = conn.execute(
                """
                SELECT variant_id
                FROM service_variant
                WHERE route_id = ? AND day_type = ? AND departure_time = ? AND company = ?
                """,
                (
                    int(route_id),
                    str(block["day_type"]),
                    str(block["departure_time"]),
                    str(block["company"]),
                ),
            ).fetchone()
            if row is None:
                continue
            variant_id = int(row[0])
        total_variants += 1

        conn.execute("DELETE FROM variant_stop WHERE variant_id = ?", (variant_id,))
        prev_stop_id: int | None = None
        prev_lat_lon: tuple[float, float] | None = None
        cumulative_distance_m = 0.0
        for seq, stop in enumerate(block["stops"], start=1):
            stop_key = (str(block["site_id"]), str(stop["stop_code"]))
            stop_uuid = stop_uuid_by_source.get(stop_key)
            if not stop_uuid:
                continue
            stop_id = stop_id_by_uuid.get(stop_uuid)
            if stop_id is None:
                continue
            lat_r = round(float(stop["lat"]), 7)
            lon_r = round(float(stop["lon"]), 7)

            if prev_lat_lon is not None:
                cumulative_distance_m += _haversine_m(
                    float(prev_lat_lon[0]),
                    float(prev_lat_lon[1]),
                    float(lat_r),
                    float(lon_r),
                )
            estimated_elapsed_min = int(
                round((cumulative_distance_m / 1000.0) * REPORT_ESTIMATED_MINUTES_PER_KM)
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO variant_stop (
                    variant_id, seq, stop_id, cumulative_distance_m, estimated_elapsed_min
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(variant_id),
                    int(seq),
                    int(stop_id),
                    float(cumulative_distance_m),
                    int(estimated_elapsed_min),
                ),
            )
            if prev_stop_id is not None and prev_stop_id != stop_id:
                segment_pairs.add((prev_stop_id, stop_id))
            prev_stop_id = int(stop_id)
            prev_lat_lon = (lat_r, lon_r)

            stop_coords_by_id[int(stop_id)] = (lat_r, lon_r)

    conn.execute("DELETE FROM stop_rtree")
    conn.execute(
        """
        INSERT INTO stop_rtree(stop_id, min_lat, max_lat, min_lon, max_lon)
        SELECT stop_id, lat, lat, lon, lon FROM stop
        """
    )
    conn.execute("DELETE FROM stop_scope")
    conn.execute(
        """
        INSERT INTO stop_scope (stop_id, site_id, route_type, day_type)
        SELECT DISTINCT
            vs.stop_id,
            r.site_id,
            r.route_type,
            sv.day_type
        FROM variant_stop vs
        JOIN service_variant sv ON sv.variant_id = vs.variant_id
        JOIN route r ON r.route_id = sv.route_id
        """
    )
    conn.commit()

    conn.execute("DELETE FROM stop_segment_polyline")
    conn.execute("DELETE FROM stop_segment_no_route")
    total_segment_pairs = len(segment_pairs)
    polyline_inserted = 0
    polyline_reused = 0
    polyline_otp_ok = 0
    polyline_failed = 0
    no_route_reused = 0
    no_route_new = 0
    retry_failure_final: Counter[str] = Counter()
    pending_otp_segments: list[SegmentTask] = []
    if total_segment_pairs:
        print(f"  OTP endpoint: {otp_graphql_url}")
        print(
            f"  stop_segment_polyline building... ({total_segment_pairs} pairs, "
            f"polyline_cache_hit={len(existing_segment_cache)}, "
            f"no_route_cache_hit={len(existing_no_route_cache)})"
        )

    for from_stop_id, to_stop_id in sorted(segment_pairs):
        from_coord = stop_coords_by_id.get(from_stop_id)
        to_coord = stop_coords_by_id.get(to_stop_id)
        if not from_coord or not to_coord:
            polyline_failed += 1
            continue
        cache_key = _segment_coord_key(from_coord, to_coord)
        cached = existing_segment_cache.get(cache_key)
        if cached and cached[0]:
            points, distance_m = cached
            conn.execute(
                """
                INSERT OR REPLACE INTO stop_segment_polyline (
                    from_stop_id, to_stop_id, encoded_polyline, distance_m
                ) VALUES (?, ?, ?, ?)
                """,
                (from_stop_id, to_stop_id, points, distance_m),
            )
            polyline_inserted += 1
            polyline_reused += 1
            continue
        cached_no_route = existing_no_route_cache.get(cache_key)
        if cached_no_route:
            reason, detail = cached_no_route
            conn.execute(
                """
                INSERT OR REPLACE INTO stop_segment_no_route (
                    from_stop_id, to_stop_id, reason, detail
                ) VALUES (?, ?, ?, ?)
                """,
                (from_stop_id, to_stop_id, reason, detail),
            )
            no_route_reused += 1
            continue
        pending_otp_segments.append((from_stop_id, to_stop_id, from_coord, to_coord))

    conn.commit()

    total_pending = len(pending_otp_segments)
    if total_pending:
        total_attempts = otp_failed_retry_rounds + 1
        print(
            f"  OTP requests: {total_pending} "
            f"(max_parallel={otp_max_parallel}, max_rps={otp_max_rps}, "
            f"request_retries={otp_request_retries}, attempts={total_attempts})"
        )
        remaining_tasks = pending_otp_segments
        for attempt_idx in range(1, total_attempts + 1):
            if not remaining_tasks:
                break
            ok_count, no_route_count, retry_tasks, status_counts = _run_otp_batch(
                conn,
                otp_client,
                remaining_tasks,
                max_parallel=otp_max_parallel,
                progress_label=f"otp attempt {attempt_idx}/{total_attempts}",
            )
            polyline_inserted += ok_count
            polyline_otp_ok += ok_count
            no_route_new += no_route_count
            remaining_tasks = [task for task, _status in retry_tasks]
            retry_failure_final = Counter(status for _task, status in retry_tasks)
            if remaining_tasks and attempt_idx < total_attempts:
                print(
                    f"    retry queued: {len(remaining_tasks)} segment(s) "
                    f"for attempt {attempt_idx + 1}/{total_attempts} "
                    f"(timeout={retry_failure_final['timeout']}, "
                    f"empty_itineraries={status_counts['empty_itineraries']})"
                )
        polyline_failed += len(remaining_tasks)
    else:
        print("  OTP requests: 0 (all segment polylines reused from existing data.db)")

    if polyline_otp_ok == 0 and polyline_inserted >= polyline_reused:
        polyline_otp_ok = polyline_inserted - polyline_reused

    site_count = conn.execute("SELECT COUNT(*) FROM site").fetchone()[0]
    route_count = conn.execute("SELECT COUNT(*) FROM route").fetchone()[0]
    variant_count = conn.execute("SELECT COUNT(*) FROM service_variant").fetchone()[0]
    stop_count = conn.execute("SELECT COUNT(*) FROM stop").fetchone()[0]
    vs_count = conn.execute("SELECT COUNT(*) FROM variant_stop").fetchone()[0]
    rtree_count = conn.execute("SELECT COUNT(*) FROM stop_rtree").fetchone()[0]
    stop_scope_count = conn.execute("SELECT COUNT(*) FROM stop_scope").fetchone()[0]
    segment_polyline_count = conn.execute("SELECT COUNT(*) FROM stop_segment_polyline").fetchone()[0]
    segment_no_route_count = conn.execute("SELECT COUNT(*) FROM stop_segment_no_route").fetchone()[0]

    timeout_failed = retry_failure_final.get("timeout", 0)
    empty_itinerary_seen = no_route_reused + no_route_new
    other_failed = polyline_failed - timeout_failed
    if other_failed < 0:
        other_failed = 0

    print("\n=== Build Complete ===")
    print(f"  Build ID:         {build_id}")
    print(f"  Mode:             {mode}")
    print(f"  Sites:            {site_count}")
    print(f"  Routes:           {route_count}")
    print(f"  Service Variants: {variant_count}")
    print(f"  Stops:            {stop_count}")
    print(f"  Variant Stops:    {vs_count}")
    print(f"  Stop Scopes:      {stop_scope_count}")
    print(f"  RTree entries:    {rtree_count}")
    print(f"  Segment pairs:    {total_segment_pairs}")
    print(
        "  Segment polylines:"
        f"{segment_polyline_count} (reused={polyline_reused}, "
        f"otp_ok={polyline_otp_ok}, failed={polyline_failed})"
    )
    print(
        "  No-route cache:   "
        f"{segment_no_route_count} (reused={no_route_reused}, "
        f"new_empty_itineraries={no_route_new})"
    )
    print(
        "  OTP fail reasons: "
        f"timeout={timeout_failed}, empty_itineraries_seen={empty_itinerary_seen}, "
        f"other={other_failed}"
    )
    print(f"  DB file:          {output_db_path}")
    conn.close()


def build_database(
    *,
    mode: str = "prepare",
    raw_dir: Path = RAW_DIR,
    output_db_path: Path | None = None,
    baseline_db_path: Path = DB_PATH,
) -> None:
    if mode not in {"prepare", "release"}:
        raise ValueError("mode must be one of: prepare, release")
    if output_db_path is None:
        output_db_path = DATA_DIR / ("data.prepare.db" if mode == "prepare" else "data.db")

    _ensure_override_files(IDENTITY_DIR)
    route_alias_overrides = _load_route_alias_overrides()
    stop_alias_overrides = _load_stop_alias_overrides()

    build_id = _build_id()
    env_map = _load_env_file(ENV_PATH)
    otp_graphql_url = _resolve_otp_graphql_url(env_map)
    otp_max_parallel = _env_int(env_map, "OTP_HTTP_MAX_PARALLEL", 50, minimum=1)
    otp_max_rps = _env_int(env_map, "OTP_HTTP_MAX_RPS", 50, minimum=1)
    otp_request_retries = _env_int(env_map, "OTP_HTTP_REQUEST_RETRIES", 2, minimum=0)
    otp_failed_retry_rounds = _env_int(env_map, "OTP_HTTP_FAILED_RETRY_ROUNDS", 2, minimum=0)
    existing_segment_cache = _load_existing_segment_cache(baseline_db_path)
    existing_no_route_cache = _load_existing_no_route_cache(baseline_db_path)
    existing_state = _load_existing_identity_state(baseline_db_path)

    blocks, file_stats = _load_variant_blocks(raw_dir)
    if not blocks:
        raise RuntimeError(f"No route blocks found in raw directory: {raw_dir}")
    for fname, count in file_stats:
        print(f"  {fname}: {count} blocks parsed")

    route_obs_new = _build_route_observations(blocks)
    stop_obs_new = _build_stop_observations(blocks)

    route_map, route_auto, route_review, route_conflicts = _assign_route_mapping(
        route_obs_new,
        existing_state,
        route_alias_overrides,
    )
    stop_map, stop_auto, stop_review, stop_conflicts = _assign_stop_mapping(
        stop_obs_new,
        existing_state,
        stop_alias_overrides,
        blocks,
        route_map,
    )

    auto_items = route_auto + stop_auto
    review_items = route_review + stop_review
    conflict_items = route_conflicts + stop_conflicts
    _write_identity_artifacts(auto_items, review_items, conflict_items)

    print(
        "  Identity review summary: "
        f"auto={len(auto_items)}, review={len(review_items)}, conflicts={len(conflict_items)}"
    )
    print(f"  Review queue:      {REVIEW_QUEUE_JSONL}")
    print(f"  Conflict queue:    {CONFLICTS_JSONL}")
    print(f"  Auto-applied log:  {AUTO_APPLIED_JSONL}")

    if mode == "release" and (review_items or conflict_items):
        raise RuntimeError(
            "Release build blocked: unresolved identity review/conflicts exist. "
            "Review queue를 반영한 후 다시 실행하세요."
        )

    identity_state = _materialize_identity_state(
        build_id=build_id,
        existing_state=existing_state,
        route_map=route_map,
        route_obs_new=route_obs_new,
        stop_map=stop_map,
        stop_obs_new=stop_obs_new,
    )

    _build_database_file(
        output_db_path=output_db_path,
        build_id=build_id,
        mode=mode,
        identity_state=identity_state,
        blocks=blocks,
        existing_segment_cache=existing_segment_cache,
        existing_no_route_cache=existing_no_route_cache,
        otp_graphql_url=otp_graphql_url,
        otp_max_parallel=otp_max_parallel,
        otp_max_rps=otp_max_rps,
        otp_request_retries=otp_request_retries,
        otp_failed_retry_rounds=otp_failed_retry_rounds,
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build shuttle data DB with identity review workflow.")
    parser.add_argument(
        "--mode",
        choices=("prepare", "release"),
        default="prepare",
        help="prepare: review 큐 포함 1차 산출물 생성, release: review/conflict 0건일 때만 최종본 생성",
    )
    parser.add_argument(
        "--raw-dir",
        default=str(RAW_DIR),
        help="raw 파일 디렉토리 경로 (.html 또는 .json 확장자 허용)",
    )
    parser.add_argument(
        "--output-db",
        default="",
        help="출력 DB 파일 경로 (미지정 시 mode에 따라 data.prepare.db 또는 data.db)",
    )
    parser.add_argument(
        "--baseline-db",
        default=str(DB_PATH),
        help="기존 identity/cache 로드용 기준 DB 경로",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    build_database(
        mode=str(args.mode),
        raw_dir=Path(str(args.raw_dir)),
        output_db_path=Path(str(args.output_db)) if str(args.output_db).strip() else None,
        baseline_db_path=Path(str(args.baseline_db)),
    )
