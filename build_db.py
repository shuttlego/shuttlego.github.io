#!/usr/bin/env python3
"""
Raw HTML 파일(data/raw/)을 파싱하여 SQLite DB(data/data.db)를 생성하는 ETL 스크립트.

Usage:
    python build_db.py
"""

import csv
import os
import re
import sqlite3
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "data.db"
SITES_CSV = DATA_DIR / "sites.csv"
ENV_PATH = Path(__file__).resolve().parent / ".env"

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
    route_id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    route_name TEXT NOT NULL,
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    UNIQUE(site_id, route_name, route_type)
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
    stop_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    UNIQUE(lat, lon)
);

CREATE TABLE IF NOT EXISTS variant_stop (
    variant_id INTEGER NOT NULL REFERENCES service_variant(variant_id),
    seq INTEGER NOT NULL,
    stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
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

CREATE INDEX IF NOT EXISTS idx_route_site_type ON route(site_id, route_type);
CREATE INDEX IF NOT EXISTS idx_variant_route_day ON service_variant(route_id, day_type);
CREATE INDEX IF NOT EXISTS idx_variant_stop_stop ON variant_stop(stop_id);
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


def parse_html_file(content: str) -> list[dict]:
    """
    HTML 내용을 파싱하여 노선 블록 리스트를 반환.
    각 블록: {route_name, departure_time, company, bus_count, stops: [(name, lat, lon), ...]}
    """
    if "등록된 게시글이 없습니다" in content:
        return []

    blocks_raw = re.split(r'<th scope="row">노선명</th>', content)[1:]
    if not blocks_raw:
        return []

    results = []
    seen_keys: set[tuple[str, str]] = set()  # (raw_route_id, departure_time) 중복 제거

    for block in blocks_raw:
        # ── 노선명 추출 ──
        td_match = re.search(r"<td[^>]*>(.*?)</td>", block, re.DOTALL)
        if not td_match:
            continue
        td_clean = _strip_html(td_match.group(1))

        # showMap 인자에서 raw_route_id, departure_time 추출
        sm_match = re.search(r"showMap\(([^)]+)\)", td_clean)
        if not sm_match:
            continue
        args = [a.strip().strip("'") for a in sm_match.group(1).split(",")]
        if len(args) < 7:
            continue
        raw_route_id = args[0]
        dep_time_raw = args[1]

        # 중복 제거 (첫 등장만 사용)
        dedup_key = (raw_route_id, dep_time_raw)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        # 노선명: '>' 뒤, '/a>' 앞
        name_match = re.search(r">(.+?)/a>$", td_clean)
        if not name_match:
            # fallback: '>' 뒤의 텍스트
            parts = td_clean.rsplit(">", 1)
            route_name = parts[-1].strip() if len(parts) > 1 else td_clean
        else:
            route_name = name_match.group(1).strip()

        if not route_name:
            continue

        # ── 메타데이터 추출 ──
        company_match = re.search(
            r"업체명.*?<td[^>]*>([^<]+)</td>", block, re.DOTALL
        )
        company = company_match.group(1).strip() if company_match else ""

        time_match = re.search(r"출발시간.*?<td>.*?>(\d{4})", block, re.DOTALL)
        departure_time = (
            _format_time(time_match.group(1)) if time_match else _format_time(dep_time_raw)
        )

        bus_match = re.search(r"운행대수.*?<td[^>]*>(\d+)</td>", block, re.DOTALL)
        bus_count = int(bus_match.group(1)) if bus_match else None

        # ── 경유지(정류장) 추출 ──
        stop_section = re.search(
            r"경유지.*?<td[^>]*>(.*?)</td>", block, re.DOTALL
        )
        stops: list[tuple[str, float, float]] = []
        if stop_section:
            stop_html = stop_section.group(1)
            stop_pattern = re.compile(
                r"showMap\('[^']+',\s*'[^']+',\s*'([^']+)',\s*'([^']+)',\s*'[^']+',\s*false,\s*\d+\)[^>]*>([^<]+)<"
            )
            for m in stop_pattern.finditer(stop_html):
                lat_str, lon_str, stop_name = m.group(1), m.group(2), m.group(3).strip()
                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                except ValueError:
                    continue
                if stop_name:
                    stops.append((stop_name, lat, lon))

        if not stops:
            continue

        results.append(
            {
                "route_name": route_name,
                "departure_time": departure_time,
                "company": company,
                "bus_count": bus_count,
                "stops": stops,
            }
        )

    return results


# ── DB 적재 ───────────────────────────────────────────────────
def build_database() -> None:
    env_map = _load_env_file(ENV_PATH)
    otp_graphql_url = _resolve_otp_graphql_url(env_map)
    otp_max_parallel = _env_int(env_map, "OTP_HTTP_MAX_PARALLEL", 50, minimum=1)
    otp_max_rps = _env_int(env_map, "OTP_HTTP_MAX_RPS", 50, minimum=1)
    otp_request_retries = _env_int(env_map, "OTP_HTTP_REQUEST_RETRIES", 2, minimum=0)
    otp_failed_retry_rounds = _env_int(env_map, "OTP_HTTP_FAILED_RETRY_ROUNDS", 2, minimum=0)
    existing_segment_cache = _load_existing_segment_cache(DB_PATH)
    existing_no_route_cache = _load_existing_no_route_cache(DB_PATH)

    # 기존 DB 삭제 후 재생성
    if DB_PATH.exists():
        DB_PATH.unlink()

    otp_client = OTPPolylineClient(
        otp_graphql_url,
        max_retries=otp_request_retries,
        rate_limiter=RateLimiter(otp_max_rps),
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA_SQL)

    # 1) sites.csv → site 테이블
    with open(SITES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                "INSERT OR IGNORE INTO site (site_id, name) VALUES (?, ?)",
                (row["site_id"].strip(), row["site_name"].strip()),
            )
    conn.commit()

    # 캐시: stop (lat, lon) → stop_id
    stop_cache: dict[tuple[float, float], int] = {}
    # 캐시: stop_id → (lat, lon)
    stop_coords_by_id: dict[int, tuple[float, float]] = {}
    # 캐시: route (site_id, route_name, route_type) → route_id
    route_cache: dict[tuple[str, str, str], int] = {}
    # 모든 variant에서 인접 정류장 쌍 수집 (방향 포함)
    segment_pairs: set[tuple[int, int]] = set()

    total_variants = 0
    total_stops_inserted = 0

    # 2) raw 파일 순회
    raw_files = sorted(f for f in os.listdir(RAW_DIR) if os.path.isfile(RAW_DIR / f))
    for fname in raw_files:
        parsed = parse_filename(fname)
        if not parsed:
            print(f"  Skip (bad filename): {fname}")
            continue
        site_id, route_type, day_type = parsed

        fpath = RAW_DIR / fname
        try:
            content = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  Skip (read error): {fname}: {e}")
            continue

        blocks = parse_html_file(content)
        if not blocks:
            continue

        for blk in blocks:
            route_name = blk["route_name"]
            departure_time = blk["departure_time"]
            company = blk["company"]
            bus_count = blk["bus_count"]
            stop_list = blk["stops"]

            # route upsert
            route_key = (site_id, route_name, route_type)
            if route_key not in route_cache:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO route (site_id, route_name, route_type) VALUES (?, ?, ?)",
                    route_key,
                )
                if cur.lastrowid and cur.rowcount > 0:
                    route_cache[route_key] = cur.lastrowid
                else:
                    row = conn.execute(
                        "SELECT route_id FROM route WHERE site_id=? AND route_name=? AND route_type=?",
                        route_key,
                    ).fetchone()
                    route_cache[route_key] = row[0]
            route_id = route_cache[route_key]

            # service_variant upsert
            cur = conn.execute(
                "INSERT OR IGNORE INTO service_variant (route_id, day_type, departure_time, company, bus_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (route_id, day_type, departure_time, company, bus_count),
            )
            if cur.lastrowid and cur.rowcount > 0:
                variant_id = cur.lastrowid
            else:
                row = conn.execute(
                    "SELECT variant_id FROM service_variant "
                    "WHERE route_id=? AND day_type=? AND departure_time=? AND company=?",
                    (route_id, day_type, departure_time, company),
                ).fetchone()
                if not row:
                    continue
                variant_id = row[0]
            total_variants += 1

            # stops + variant_stop
            # 기존 variant_stop 삭제 후 재삽입
            conn.execute("DELETE FROM variant_stop WHERE variant_id=?", (variant_id,))
            prev_stop_id: int | None = None
            for seq, (stop_name, lat, lon) in enumerate(stop_list, start=1):
                # stop upsert
                # 위경도를 소수점 7자리로 반올림하여 유니크 키로 사용
                lat_r = round(lat, 7)
                lon_r = round(lon, 7)
                stop_key = (lat_r, lon_r)
                if stop_key not in stop_cache:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO stop (name, lat, lon) VALUES (?, ?, ?)",
                        (stop_name, lat_r, lon_r),
                    )
                    if cur.lastrowid and cur.rowcount > 0:
                        stop_cache[stop_key] = cur.lastrowid
                        total_stops_inserted += 1
                    else:
                        row = conn.execute(
                            "SELECT stop_id FROM stop WHERE lat=? AND lon=?",
                            (lat_r, lon_r),
                        ).fetchone()
                        stop_cache[stop_key] = row[0]
                stop_id = stop_cache[stop_key]
                stop_coords_by_id[stop_id] = (lat_r, lon_r)

                conn.execute(
                    "INSERT OR REPLACE INTO variant_stop (variant_id, seq, stop_id) VALUES (?, ?, ?)",
                    (variant_id, seq, stop_id),
                )
                if prev_stop_id is not None and prev_stop_id != stop_id:
                    segment_pairs.add((prev_stop_id, stop_id))
                prev_stop_id = stop_id

        conn.commit()
        print(f"  {fname}: {len(blocks)} blocks loaded")

    # 3) stop_rtree 동기화
    conn.execute("DELETE FROM stop_rtree")
    conn.execute(
        "INSERT INTO stop_rtree (stop_id, min_lat, max_lat, min_lon, max_lon) "
        "SELECT stop_id, lat, lat, lon, lon FROM stop"
    )

    # 4) stop_scope 동기화
    # 정류장별로 (site_id, route_type, day_type) 소속 관계를 물리화한다.
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

    # 5) stop_segment_polyline 동기화
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

    # 혹시 재사용만 있었고 OTP 성공이 0일 수 있으므로 안전 계산
    if polyline_otp_ok == 0 and polyline_inserted >= polyline_reused:
        polyline_otp_ok = polyline_inserted - polyline_reused

    # 통계
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

    print(f"\n=== Build Complete ===")
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
    print(f"  DB file:          {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    build_database()
