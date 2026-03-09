"""
SQLite 기반 데이터 접근 레이어.
data/data.db 를 read-only로 열어 검색 쿼리를 수행한다.
"""

import logging
import math
import os
import re
import sqlite3
import threading
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from prometheus_client import Counter

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "data.db"

_logger = logging.getLogger(__name__)
_db_path = str(DB_PATH)
_db_state_lock = threading.Lock()
_db_generation_lock = threading.Lock()
_db_local = threading.local()
_db_generation = 0
_db_paths_by_generation: dict[int, str] = {0: _db_path}
_has_stop_scope: bool | None = None
_has_stop_segment_polyline: bool | None = None
_has_variant_stop_estimated_elapsed_min: bool | None = None
_has_route_uuid: bool | None = None
_has_route_source_route_id: bool | None = None
_has_stop_uuid: bool | None = None
_has_stop_code: bool | None = None
_endpoint_cache_lock = threading.Lock()
_endpoint_cache: dict[tuple[int, str, str, str], dict] = {}
_route_metadata_cache_lock = threading.Lock()
_route_metadata_cache: dict[tuple[int, int, str], dict | None] = {}
_report_stop_exclusion_lock = threading.Lock()
_report_stop_exclusion_cache: dict[tuple[int, str, str], set[int]] = {}
_ENDPOINT_GROUP_DISTANCE_M = 100.0
_ENDPOINT_GRID_CELL_DEG = _ENDPOINT_GROUP_DISTANCE_M / 111000.0
_REPORT_SAME_NAME_GROUP_DISTANCE_M = 20.0
_REPORT_ESTIMATED_MINUTES_PER_KM = 2.0
ROUTE_OPTION_SKIPS = Counter(
    "shuttle_route_option_candidate_skipped_total",
    "Route option candidates skipped while building route options",
    ["reason"],
)


def _record_route_option_skip(reason: str) -> None:
    safe_reason = str(reason or "").strip() or "unknown"
    ROUTE_OPTION_SKIPS.labels(reason=safe_reason).inc()


def get_db_generation() -> int:
    with _db_generation_lock:
        return int(_db_generation)


def _current_thread_generation() -> int:
    pinned_generation = getattr(_db_local, "pinned_generation", None)
    with _db_generation_lock:
        active_generation = int(_db_generation)
        if isinstance(pinned_generation, int) and pinned_generation in _db_paths_by_generation:
            return int(pinned_generation)
        return active_generation


def pin_current_thread_db_generation(generation: int) -> int:
    with _db_generation_lock:
        if int(generation) in _db_paths_by_generation:
            selected = int(generation)
        else:
            selected = int(_db_generation)
    setattr(_db_local, "pinned_generation", selected)
    return selected


def clear_current_thread_db_generation_pin() -> None:
    if hasattr(_db_local, "pinned_generation"):
        delattr(_db_local, "pinned_generation")


def _open_connection(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def init_db(db_path: str | None = None) -> int:
    """SQLite DB 연결 설정. 각 스레드는 자체 read-only connection을 사용한다."""
    global _db_path
    global _db_generation
    global _has_stop_scope
    global _has_stop_segment_polyline
    global _has_variant_stop_estimated_elapsed_min
    global _has_route_uuid
    global _has_route_source_route_id
    global _has_stop_uuid
    global _has_stop_code
    path = db_path or str(DB_PATH)
    current_conn = getattr(_db_local, "conn", None)
    if current_conn is not None:
        try:
            current_conn.close()
        except sqlite3.Error:
            pass
    with _db_state_lock:
        conn = _open_connection(path)
    with _db_generation_lock:
        _db_path = path
        _db_generation += 1
        generation = int(_db_generation)
        _db_paths_by_generation[generation] = path
        # keep a bounded generation-path index
        max_entries = 32
        if len(_db_paths_by_generation) > max_entries:
            for old_generation in sorted(_db_paths_by_generation.keys())[:-max_entries]:
                _db_paths_by_generation.pop(old_generation, None)
    with _endpoint_cache_lock:
        _endpoint_cache.clear()
    with _route_metadata_cache_lock:
        _route_metadata_cache.clear()
    with _report_stop_exclusion_lock:
        _report_stop_exclusion_cache.clear()
    setattr(_db_local, "conn_path", path)
    setattr(_db_local, "conn", conn)
    setattr(_db_local, "conn_generation", generation)
    _has_stop_scope = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_scope' LIMIT 1"
        ).fetchone()
    )
    _has_stop_segment_polyline = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_segment_polyline' LIMIT 1"
        ).fetchone()
    )
    variant_stop_columns = {
        str(row["name"] or "")
        for row in conn.execute("PRAGMA table_info(variant_stop)").fetchall()
    }
    _has_variant_stop_estimated_elapsed_min = "estimated_elapsed_min" in variant_stop_columns
    route_columns = {
        str(row["name"] or "")
        for row in conn.execute("PRAGMA table_info(route)").fetchall()
    }
    stop_columns = {
        str(row["name"] or "")
        for row in conn.execute("PRAGMA table_info(stop)").fetchall()
    }
    _has_route_uuid = "route_uuid" in route_columns
    _has_route_source_route_id = "source_route_id" in route_columns
    _has_stop_uuid = "stop_uuid" in stop_columns
    _has_stop_code = "stop_code" in stop_columns
    return generation


def _db() -> sqlite3.Connection:
    conn = getattr(_db_local, "conn", None)
    conn_path = getattr(_db_local, "conn_path", None)
    conn_generation = getattr(_db_local, "conn_generation", None)
    desired_generation = _current_thread_generation()
    with _db_generation_lock:
        desired_path = _db_paths_by_generation.get(desired_generation, _db_path)
    if conn is None or conn_path != desired_path or conn_generation != desired_generation:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        with _db_state_lock:
            try:
                conn = _open_connection(desired_path)
            except sqlite3.Error:
                with _db_generation_lock:
                    desired_path = _db_path
                    desired_generation = int(_db_generation)
                conn = _open_connection(desired_path)
        setattr(_db_local, "conn", conn)
        setattr(_db_local, "conn_path", desired_path)
        setattr(_db_local, "conn_generation", desired_generation)
    return conn


def _row_get(row, key: str, index: int | None = None, default=None):
    if row is None:
        return default
    try:
        if isinstance(row, sqlite3.Row):
            value = row[key]
            return default if value is None else value
    except (IndexError, KeyError, TypeError):
        pass
    if isinstance(row, dict):
        value = row.get(key, default)
        return default if value is None else value
    if index is not None and isinstance(row, (tuple, list)):
        if 0 <= index < len(row):
            value = row[index]
            return default if value is None else value
    try:
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def _coerce_float(value) -> float | None:
    try:
        if value is None:
            return None
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _coerce_int(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _route_metadata_cache_key(route_id: int, day_type: str) -> tuple[int, int, str]:
    return _current_thread_generation(), int(route_id), str(day_type)


def _clone_route_metadata(payload: dict) -> dict:
    return {
        "all_departure_times": list(payload.get("all_departure_times", [])),
        "companies": list(payload.get("companies", [])),
        "route_stops": [dict(stop) for stop in payload.get("route_stops", [])],
        "first_variant_id": _coerce_int(payload.get("first_variant_id")),
    }


def _get_route_metadata(
    db: sqlite3.Connection,
    route_id: int,
    day_type: str,
) -> dict | None:
    cache_key = _route_metadata_cache_key(route_id, day_type)
    with _route_metadata_cache_lock:
        cached = _route_metadata_cache.get(cache_key)
    if cached is not None:
        return _clone_route_metadata(cached)

    rows = db.execute(
        """
        SELECT variant_id, departure_time, company, bus_count
        FROM service_variant
        WHERE route_id = ? AND day_type = ?
        ORDER BY departure_time
        """,
        (route_id, day_type),
    ).fetchall()
    if not rows:
        _record_route_option_skip("missing_variants")
        return None

    all_times = sorted(
        {
            str(value).strip()
            for row in rows
            for value in [_row_get(row, "departure_time", index=1)]
            if str(value).strip()
        }
    )
    companies = sorted(
        {
            str(value).strip()
            for row in rows
            for value in [_row_get(row, "company", index=2)]
            if str(value).strip()
        }
    )
    first_variant_id = None
    for row in rows:
        first_variant_id = _coerce_int(_row_get(row, "variant_id", index=0))
        if first_variant_id is not None:
            break
    if first_variant_id is None:
        _record_route_option_skip("missing_variant_id")
        return None

    route_stops = _get_variant_stops(db, first_variant_id)
    if not route_stops:
        _record_route_option_skip("missing_route_stops")
        return None

    payload = {
        "all_departure_times": all_times,
        "companies": companies,
        "route_stops": [dict(stop) for stop in route_stops],
        "first_variant_id": first_variant_id,
    }
    with _route_metadata_cache_lock:
        existing = _route_metadata_cache.get(cache_key)
        if existing is not None:
            return _clone_route_metadata(existing)
        _route_metadata_cache[cache_key] = dict(payload)
    return _clone_route_metadata(payload)


def _supports_stop_scope() -> bool:
    global _has_stop_scope
    if _has_stop_scope is None:
        db = _db()
        _has_stop_scope = bool(
            db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_scope' LIMIT 1"
            ).fetchone()
        )
    return bool(_has_stop_scope)


def _supports_stop_segment_polyline() -> bool:
    global _has_stop_segment_polyline
    if _has_stop_segment_polyline is None:
        db = _db()
        _has_stop_segment_polyline = bool(
            db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_segment_polyline' LIMIT 1"
            ).fetchone()
        )
    return bool(_has_stop_segment_polyline)


def _supports_variant_stop_estimated_elapsed_min() -> bool:
    global _has_variant_stop_estimated_elapsed_min
    if _has_variant_stop_estimated_elapsed_min is None:
        db = _db()
        columns = {
            str(row["name"] or "")
            for row in db.execute("PRAGMA table_info(variant_stop)").fetchall()
        }
        _has_variant_stop_estimated_elapsed_min = "estimated_elapsed_min" in columns
    return bool(_has_variant_stop_estimated_elapsed_min)


def _supports_route_uuid() -> bool:
    global _has_route_uuid
    if _has_route_uuid is None:
        db = _db()
        columns = {
            str(row["name"] or "")
            for row in db.execute("PRAGMA table_info(route)").fetchall()
        }
        _has_route_uuid = "route_uuid" in columns
    return bool(_has_route_uuid)


def _supports_route_source_route_id() -> bool:
    global _has_route_source_route_id
    if _has_route_source_route_id is None:
        db = _db()
        columns = {
            str(row["name"] or "")
            for row in db.execute("PRAGMA table_info(route)").fetchall()
        }
        _has_route_source_route_id = "source_route_id" in columns
    return bool(_has_route_source_route_id)


def _supports_stop_uuid() -> bool:
    global _has_stop_uuid
    if _has_stop_uuid is None:
        db = _db()
        columns = {
            str(row["name"] or "")
            for row in db.execute("PRAGMA table_info(stop)").fetchall()
        }
        _has_stop_uuid = "stop_uuid" in columns
    return bool(_has_stop_uuid)


def _supports_stop_code() -> bool:
    global _has_stop_code
    if _has_stop_code is None:
        db = _db()
        columns = {
            str(row["name"] or "")
            for row in db.execute("PRAGMA table_info(stop)").fetchall()
        }
        _has_stop_code = "stop_code" in columns
    return bool(_has_stop_code)


# ── 유틸리티 ───────────────────────────────────────────────────
def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return _haversine_km(lat1, lon1, lat2, lon2) * 1000.0


def _get_variant_stops(db: sqlite3.Connection, variant_id: int) -> list[dict]:
    has_polyline = _supports_stop_segment_polyline()
    has_estimated_elapsed = _supports_variant_stop_estimated_elapsed_min()
    stop_uuid_sql = "s.stop_uuid AS stop_uuid,\n                " if _supports_stop_uuid() else "NULL AS stop_uuid,\n                "
    stop_code_sql = "s.stop_code AS stop_code,\n                " if _supports_stop_code() else "NULL AS stop_code,\n                "
    estimated_elapsed_sql = (
        "vs.estimated_elapsed_min AS estimated_elapsed_min,\n                "
        if has_estimated_elapsed
        else "NULL AS estimated_elapsed_min,\n                "
    )
    if has_polyline:
        query = """
            SELECT
                vs.seq,
                vs.stop_id,
                s.name,
                s.lat,
                s.lon,
                {stop_uuid_sql}
                {stop_code_sql}
                {estimated_elapsed_sql}
                sp.encoded_polyline AS next_encoded_polyline
            FROM variant_stop vs
            JOIN stop s ON s.stop_id = vs.stop_id
            LEFT JOIN variant_stop vs_next
                ON vs_next.variant_id = vs.variant_id
               AND vs_next.seq = vs.seq + 1
            LEFT JOIN stop_segment_polyline sp
               ON sp.from_stop_id = vs.stop_id
               AND sp.to_stop_id = vs_next.stop_id
            WHERE vs.variant_id = ?
            ORDER BY vs.seq
            """.format(
                stop_uuid_sql=stop_uuid_sql,
                stop_code_sql=stop_code_sql,
                estimated_elapsed_sql=estimated_elapsed_sql,
            )
        rows = db.execute(query, (variant_id,)).fetchall()
    else:
        query = """
            SELECT
                vs.seq,
                vs.stop_id,
                s.name,
                s.lat,
                s.lon,
                {stop_uuid_sql}
                {stop_code_sql}
                {estimated_elapsed_sql}
                NULL AS next_encoded_polyline
            FROM variant_stop vs
            JOIN stop s ON s.stop_id = vs.stop_id
            WHERE vs.variant_id = ?
            ORDER BY vs.seq
            """.format(
                stop_uuid_sql=stop_uuid_sql,
                stop_code_sql=stop_code_sql,
                estimated_elapsed_sql=estimated_elapsed_sql,
            )
        rows = db.execute(query, (variant_id,)).fetchall()

    items: list[dict] = []
    for sr in rows:
        lat = _coerce_float(_row_get(sr, "lat", index=3))
        lng = _coerce_float(_row_get(sr, "lon", index=4))
        seq = _coerce_int(_row_get(sr, "seq", index=0))
        stop_id = _coerce_int(_row_get(sr, "stop_id", index=1))
        stop_name = str(_row_get(sr, "name", index=2, default="") or "").strip()
        if seq is None or stop_id is None or not stop_name:
            _record_route_option_skip("invalid_variant_stop_shape")
            continue
        if lat is None or lng is None:
            _record_route_option_skip("invalid_variant_stop_coords")
            continue
        stop_uuid = str(_row_get(sr, "stop_uuid", default="") or "").strip() or None
        stop_code = str(_row_get(sr, "stop_code", default="") or "").strip() or None
        estimated_elapsed = _coerce_int(_row_get(sr, "estimated_elapsed_min"))
        next_encoded_polyline = str(_row_get(sr, "next_encoded_polyline", default="") or "")
        items.append(
            {
                "sequence": seq,
                "stop_id": stop_id,
                "stop_uuid": stop_uuid,
                "stop_code": stop_code,
                "stop_name": stop_name,
                "lat": lat,
                "lng": lng,
                "estimated_elapsed_min": estimated_elapsed,
                "next_encoded_polyline": next_encoded_polyline,
            }
        )
    if not items:
        return []
    cumulative_distance_m = 0.0
    prev_lat: float | None = None
    prev_lng: float | None = None
    for item in items:
        lat = float(item["lat"])
        lng = float(item["lng"])
        if prev_lat is not None and prev_lng is not None:
            cumulative_distance_m += _haversine_m(prev_lat, prev_lng, lat, lng)
        if item.get("estimated_elapsed_min") is None:
            item["estimated_elapsed_min"] = int(
                round((cumulative_distance_m / 1000.0) * _REPORT_ESTIMATED_MINUTES_PER_KM)
            )
        prev_lat = lat
        prev_lng = lng
    return items


# ── 공개 API ──────────────────────────────────────────────────
def get_sites() -> list[dict]:
    rows = _db().execute("SELECT site_id, name FROM site ORDER BY site_id").fetchall()
    return [{"site_id": r["site_id"], "site_name": r["name"]} for r in rows]


def get_available_day_types(site_id: str) -> list[str]:
    """
    선택 사업장에서 출퇴근 노선(commute_in/commute_out)이 실제 존재하는 day_type 목록을 반환한다.
    '테스트' 노선은 제외한다.
    """
    rows = _db().execute(
        """
        SELECT DISTINCT sv.day_type
        FROM route ro
        JOIN service_variant sv ON sv.route_id = ro.route_id
        WHERE ro.site_id = ?
          AND ro.route_type IN ('commute_in', 'commute_out')
          AND ro.route_name NOT LIKE '%테스트%'
        ORDER BY
          CASE sv.day_type
            WHEN 'weekday' THEN 1
            WHEN 'monday' THEN 2
            WHEN 'saturday' THEN 3
            WHEN 'holiday' THEN 4
            WHEN 'familyday' THEN 5
            ELSE 99
          END,
          sv.day_type
        """,
        (site_id,),
    ).fetchall()
    return [str(r["day_type"]) for r in rows if r["day_type"]]


def get_db_updated_at() -> str:
    """DB 파일의 마지막 수정 시각을 KST 문자열로 반환."""
    try:
        mtime = os.path.getmtime(DB_PATH)
        kst = timezone(timedelta(hours=9))
        dt = datetime.fromtimestamp(mtime, tz=kst)
        return dt.strftime("%Y-%m-%d %H:%M")
    except OSError:
        return ""


def _normalize_endpoint_name(raw_name: str | None) -> str:
    """플랫폼/홈 번호 표기를 제거해 endpoint 명칭을 정규화한다."""
    name = (raw_name or "").strip()
    if not name:
        return ""

    # 말단 구두점 제거 (동일 명칭 중복 방지)
    name = re.sub(r"\s*[.]+\s*$", "", name)

    # (야외.1번플랫폼) -> (야외), (타워.22번플랫폼) -> (타워)
    name = re.sub(
        r"\(\s*([^()]+?)\s*[.·]\s*[A-Za-z]?\d+(?:-\d+)?\s*번?\s*(?:플랫폼|플래폼|승강장|홈)\s*\)",
        r"(\1)",
        name,
    )

    # 숫자/번호 기반 플랫폼/홈 괄호 제거: (31번/32번플랫폼), (41번플래폼), (27옆플랫폼)
    name = re.sub(
        r"\s*\(\s*[^()]*\d[^()]*?(?:플랫폼|플래폼|승강장|홈)[^()]*\)\s*",
        "",
        name,
    )

    # (A플랫폼), (B 플랫폼) 제거
    name = re.sub(r"\s*\(\s*[A-Z]\s*플랫폼\s*\)\s*", "", name)

    # 숫자+홈 토큰 제거(문자열 중간 포함): 회사7번홈(정문) -> 회사(정문)
    name = re.sub(r"\s*[A-Za-z]?\d+(?:-\d+)?\s*번?\s*홈", "", name)

    # P2 야외승강장(19) -> P2 야외승강장
    name = re.sub(r"\s*\(\s*\d+(?:-\d+)?\s*\)\s*$", "", name)

    # DSR동 전면도로(1번정류장) -> DSR동 전면도로
    name = re.sub(r"\s*\(\s*[A-Za-z]?\d+(?:-\d+)?\s*번?\s*정류장\s*\)\s*$", "", name)

    # 공백/괄호 포맷 정리
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"\(\s+", "(", name)
    name = re.sub(r"\s+\)", ")", name)
    name = re.sub(r"\s+\(", "(", name)
    return name.strip()


def _endpoint_route_type(direction: str) -> str:
    if direction == "depart":
        return "commute_in"
    if direction == "arrive":
        return "commute_out"
    raise ValueError("direction must be 'depart' or 'arrive'")


def _build_endpoint_cache(site_id: str, day_type: str, direction: str) -> dict:
    """
    사업장/요일/방향별 endpoint 인덱스를 1회 생성한다.
    반환:
    {
        "options": [{"endpoint_name": str, "route_count": int}, ...],
        "endpoint_to_routes": {endpoint_name: {route_id, ...}},
    }

    규칙:
    - 동일 명칭 정규화(_normalize_endpoint_name)
    - endpoint 정류장끼리 100m 이내면 하나의 옵션 그룹으로 병합
    - 옵션명 구성요소는 반드시 같은 방향 노선의 실제 endpoint 명칭만 사용
    - 옵션별 route 집합은 "선택된 사업장 기점을 해당 노선이 1회 이상 포함하는지"
      기준으로 매핑한다.
      (즉, 실제 시/종점뿐 아니라 같은 노선 내 경유 기점도 포함한다.)
    """
    db = _db()
    route_type = _endpoint_route_type(direction)
    endpoint_seq_col = "es.max_seq" if direction == "depart" else "es.min_seq"

    endpoint_rows = db.execute(
        f"""
        WITH target_routes AS (
            SELECT route_id
            FROM route
            WHERE site_id = ?
              AND route_type = ?
              AND route_name NOT LIKE '%테스트%'
        ),
        first_variant AS (
            SELECT sv.route_id, MIN(sv.variant_id) AS variant_id
            FROM service_variant sv
            JOIN target_routes tr ON tr.route_id = sv.route_id
            WHERE sv.day_type = ?
            GROUP BY sv.route_id
        ),
        endpoint_seq AS (
            SELECT vs.variant_id, MIN(vs.seq) AS min_seq, MAX(vs.seq) AS max_seq
            FROM variant_stop vs
            JOIN first_variant fv ON fv.variant_id = vs.variant_id
            GROUP BY vs.variant_id
        )
        SELECT
            fv.route_id,
            ro.route_name,
            vs_ep.stop_id AS endpoint_stop_id,
            st_ep.name AS endpoint_name,
            st_ep.lat AS endpoint_lat,
            st_ep.lon AS endpoint_lon
        FROM first_variant fv
        JOIN route ro ON ro.route_id = fv.route_id
        JOIN endpoint_seq es ON es.variant_id = fv.variant_id
        JOIN variant_stop vs_ep
            ON vs_ep.variant_id = fv.variant_id
           AND vs_ep.seq = {endpoint_seq_col}
        LEFT JOIN stop st_ep ON st_ep.stop_id = vs_ep.stop_id
        """,
        (site_id, route_type, day_type),
    ).fetchall()

    if not endpoint_rows:
        return {"options": [], "endpoint_to_routes": {}}

    name_to_direct_routes: dict[str, set[int]] = defaultdict(set)
    name_to_endpoint_stops: dict[str, set[int]] = defaultdict(set)
    route_name_by_id: dict[int, str] = {}
    stop_coords: dict[int, tuple[float, float]] = {}

    for row in endpoint_rows:
        normalized = _normalize_endpoint_name(row["endpoint_name"])
        if not normalized:
            continue
        rid = int(row["route_id"])
        sid = int(row["endpoint_stop_id"])
        name_to_direct_routes[normalized].add(rid)
        name_to_endpoint_stops[normalized].add(sid)
        if rid not in route_name_by_id:
            route_name_by_id[rid] = str(row["route_name"] or "").strip()
        try:
            lat = float(row["endpoint_lat"])
            lon = float(row["endpoint_lon"])
        except (TypeError, ValueError):
            continue
        stop_coords[sid] = (lat, lon)

    if not name_to_direct_routes:
        return {"options": [], "endpoint_to_routes": {}}

    # 옵션 구성요소로 사용되는 endpoint 명칭(정규화값)만 별도 집계한다.
    candidate_component_names = set(name_to_direct_routes.keys())
    component_name_to_serviceable_routes: dict[str, set[int]] = defaultdict(set)

    # "선택한 기점을 포함하는 모든 노선" 매핑을 위해,
    # day_type의 모든 variant 정류장을 스캔해 옵션 구성요소 명칭과 일치하는 route를 수집한다.
    serviceable_rows = db.execute(
        """
        SELECT DISTINCT
            sv.route_id,
            ro.route_name,
            st.name AS stop_name
        FROM route ro
        JOIN service_variant sv ON sv.route_id = ro.route_id
        JOIN variant_stop vs ON vs.variant_id = sv.variant_id
        JOIN stop st ON st.stop_id = vs.stop_id
        WHERE ro.site_id = ?
          AND ro.route_type = ?
          AND sv.day_type = ?
          AND ro.route_name NOT LIKE '%테스트%'
        """,
        (site_id, route_type, day_type),
    ).fetchall()

    for row in serviceable_rows:
        normalized = _normalize_endpoint_name(row["stop_name"])
        if not normalized or normalized not in candidate_component_names:
            continue
        rid = int(row["route_id"])
        component_name_to_serviceable_routes[normalized].add(rid)
        if rid not in route_name_by_id:
            route_name_by_id[rid] = str(row["route_name"] or "").strip()

    stop_grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    for sid, (lat, lon) in stop_coords.items():
        gx = math.floor(lat / _ENDPOINT_GRID_CELL_DEG)
        gy = math.floor(lon / _ENDPOINT_GRID_CELL_DEG)
        stop_grid[(gx, gy)].append(sid)

    def nearby_stop_ids(origin_sid: int) -> set[int]:
        coord = stop_coords.get(origin_sid)
        if coord is None:
            return set()
        origin_lat, origin_lon = coord
        gx = math.floor(origin_lat / _ENDPOINT_GRID_CELL_DEG)
        gy = math.floor(origin_lon / _ENDPOINT_GRID_CELL_DEG)
        nearby: set[int] = set()
        for dx in (-2, -1, 0, 1, 2):
            for dy in (-2, -1, 0, 1, 2):
                for cand_sid in stop_grid.get((gx + dx, gy + dy), []):
                    cand_lat, cand_lon = stop_coords[cand_sid]
                    if _haversine_m(origin_lat, origin_lon, cand_lat, cand_lon) <= _ENDPOINT_GROUP_DISTANCE_M:
                        nearby.add(cand_sid)
        return nearby

    endpoint_stop_to_names: dict[int, set[str]] = defaultdict(set)
    endpoint_stop_ids: set[int] = set()
    for name, stop_ids in name_to_endpoint_stops.items():
        for sid in stop_ids:
            endpoint_stop_to_names[sid].add(name)
            endpoint_stop_ids.add(sid)

    # endpoint stop끼리 100m 근접한 관계를 구성
    endpoint_nearby_map: dict[int, set[int]] = {}
    for sid in endpoint_stop_ids:
        endpoint_nearby_map[sid] = nearby_stop_ids(sid).intersection(endpoint_stop_ids)

    # 이름 기준 DSU로 endpoint name 클러스터링
    parent: dict[str, str] = {name: name for name in name_to_direct_routes}
    rank: dict[str, int] = {name: 0 for name in name_to_direct_routes}

    def find_name(name: str) -> str:
        while parent[name] != name:
            parent[name] = parent[parent[name]]
            name = parent[name]
        return name

    def union_name(a: str, b: str) -> None:
        ra = find_name(a)
        rb = find_name(b)
        if ra == rb:
            return
        if rank[ra] < rank[rb]:
            ra, rb = rb, ra
        parent[rb] = ra
        if rank[ra] == rank[rb]:
            rank[ra] += 1

    for sid, names in endpoint_stop_to_names.items():
        nearby_endpoint_stops = endpoint_nearby_map.get(sid, set())
        if not nearby_endpoint_stops:
            continue
        for near_sid in nearby_endpoint_stops:
            near_names = endpoint_stop_to_names.get(near_sid)
            if not near_names:
                continue
            for n1 in names:
                for n2 in near_names:
                    union_name(n1, n2)

    cluster_to_names: dict[str, set[str]] = defaultdict(set)
    for name in name_to_direct_routes.keys():
        cluster_to_names[find_name(name)].add(name)

    endpoint_to_routes: dict[str, set[int]] = {}
    endpoint_option_meta: dict[str, dict] = {}
    for clustered_names in cluster_to_names.values():
        direct_route_ids: set[int] = set()
        for name in clustered_names:
            direct_route_ids.update(name_to_direct_routes.get(name, set()))

        component_name_to_routes: dict[str, set[int]] = defaultdict(set)
        for name in clustered_names:
            component_name_to_routes[name].update(name_to_direct_routes.get(name, set()))

        if component_name_to_routes:
            primary_name = sorted(
                component_name_to_routes.keys(),
                key=lambda name: (-len(component_name_to_routes[name]), name),
            )[0]
            remaining_names = sorted([name for name in component_name_to_routes.keys() if name != primary_name])
            ordered_names = [primary_name] + remaining_names
        else:
            ordered_names = sorted(clustered_names)
            primary_name = ordered_names[0] if ordered_names else ""

        option_name = " / ".join(ordered_names)
        dedup_name = option_name
        suffix = 2
        while dedup_name in endpoint_to_routes:
            dedup_name = f"{option_name} ({suffix})"
            suffix += 1
        # route 매핑은 canonical endpoint만이 아니라,
        # 동일 사업장 기점을 경유/포함하는 노선까지 포함한다.
        mapped_route_ids: set[int] = set()
        for name in clustered_names:
            mapped_route_ids.update(component_name_to_serviceable_routes.get(name, set()))
        if not mapped_route_ids:
            # 예외적으로 서비스 가능 매핑이 비어 있으면 기존 canonical 매핑을 폴백으로 사용한다.
            mapped_route_ids = set(direct_route_ids)
        endpoint_to_routes[dedup_name] = mapped_route_ids
        display_lines = [primary_name] + [f"  {name}" for name in ordered_names[1:]]
        endpoint_option_meta[dedup_name] = {
            "endpoint_primary_name": primary_name,
            "endpoint_components": ordered_names,
            "endpoint_display_name": "\n".join(display_lines),
        }

    options: list[dict] = []
    endpoint_search_blob: dict[str, str] = {}
    for endpoint, route_ids in endpoint_to_routes.items():
        meta = endpoint_option_meta.get(endpoint, {})
        options.append(
            {
                "endpoint_name": endpoint,
                "route_count": len(route_ids),
                "endpoint_primary_name": str(meta.get("endpoint_primary_name", "")),
                "endpoint_components": list(meta.get("endpoint_components", [])),
                "endpoint_display_name": str(meta.get("endpoint_display_name", "")),
            }
        )
        route_names = sorted(
            {
                route_name_by_id.get(rid, "").strip()
                for rid in route_ids
                if route_name_by_id.get(rid, "").strip()
            }
        )
        endpoint_search_blob[endpoint] = "\n".join(name.casefold() for name in route_names)
    options.sort(key=lambda item: item["endpoint_name"])
    return {
        "options": options,
        "endpoint_to_routes": endpoint_to_routes,
        "endpoint_search_blob": endpoint_search_blob,
    }


def _get_or_build_endpoint_cache(site_id: str, day_type: str, direction: str) -> dict:
    key = (_current_thread_generation(), site_id, day_type, direction)
    with _endpoint_cache_lock:
        cached = _endpoint_cache.get(key)
    if cached is not None:
        return cached

    built = _build_endpoint_cache(site_id, day_type, direction)
    with _endpoint_cache_lock:
        existing = _endpoint_cache.get(key)
        if existing is not None:
            return existing
        _endpoint_cache[key] = built
        return built


def warm_endpoint_cache() -> None:
    """
    endpoint 캐시를 사전 워밍업한다.
    앱 시작 시 1회 수행하면 이후 요청은 캐시를 재사용한다.
    """
    db = _db()
    site_rows = db.execute("SELECT site_id FROM site ORDER BY site_id").fetchall()
    day_rows = db.execute("SELECT DISTINCT day_type FROM service_variant ORDER BY day_type").fetchall()
    site_ids = [str(r["site_id"]) for r in site_rows]
    day_types = [str(r["day_type"]) for r in day_rows]
    for site_id in site_ids:
        for day_type in day_types:
            _get_or_build_endpoint_cache(site_id, day_type, "depart")
            _get_or_build_endpoint_cache(site_id, day_type, "arrive")


def get_endpoint_options(site_id: str, day_type: str, direction: str) -> list[dict]:
    """사업장/요일/방향별 endpoint 옵션(정규화 + route_count 포함) 조회."""
    cache_entry = _get_or_build_endpoint_cache(site_id, day_type, direction)
    return [dict(item) for item in cache_entry["options"]]


def search_endpoint_options(
    site_id: str,
    day_type: str,
    direction: str,
    route_keyword: str,
) -> list[dict]:
    """
    route_keyword(노선명 부분 일치)를 포함한 노선이 속한 endpoint 옵션만 반환한다.
    """
    cache_entry = _get_or_build_endpoint_cache(site_id, day_type, direction)
    options = cache_entry["options"]
    keyword = (route_keyword or "").strip()
    if not keyword:
        return [dict(item) for item in options]

    keyword_cf = keyword.casefold()
    endpoint_search_blob: dict[str, str] = cache_entry.get("endpoint_search_blob", {})
    filtered: list[dict] = []
    for item in options:
        endpoint_name = str(item.get("endpoint_name", ""))
        blob = endpoint_search_blob.get(endpoint_name, "")
        if keyword_cf in blob:
            filtered.append(dict(item))
    return filtered


def get_endpoint_route_ids(
    site_id: str,
    day_type: str,
    direction: str,
    selected_endpoints: list[str] | None,
) -> set[int]:
    """
    선택 endpoint에 매핑되는 route_id 집합을 반환.
    route 매핑 기준은 "해당 endpoint 구성요소(사업장 기점)를 노선이 1회 이상 포함하는가"이다.
    - selected_endpoints is None: 해당 방향의 전체 endpoint(route) 반환
    - selected_endpoints == []: 빈 집합 반환
    """
    cache_entry = _get_or_build_endpoint_cache(site_id, day_type, direction)
    endpoint_to_routes: dict[str, set[int]] = cache_entry["endpoint_to_routes"]

    if selected_endpoints is None:
        all_ids: set[int] = set()
        for route_ids in endpoint_to_routes.values():
            all_ids.update(route_ids)
        return all_ids

    selected_set = {name.strip() for name in selected_endpoints if name and name.strip()}
    if not selected_set:
        return set()

    allowed: set[int] = set()
    for endpoint_name in selected_set:
        route_ids = endpoint_to_routes.get(endpoint_name)
        if route_ids:
            allowed.update(route_ids)
    return allowed


def _find_nearby_stops(
    lat: float,
    lon: float,
    max_stops: int = 50,
    site_id: str | None = None,
    route_type: str | None = None,
    day_type: str | None = None,
) -> list[tuple[int, str, float, float, float]]:
    """
    RTree 기반 근접 정류장 검색.
    바운딩 박스를 점진 확장하여 max_stops개 이상 후보 확보 후 거리순 정렬.
    site_id/route_type/day_type이 주어지면 해당 범위의 정류장만 후보로 사용한다.
    반환: [(stop_id, name, lat, lon, distance_m), ...]
    """
    db = _db()
    delta = 0.01  # 약 1.1km
    clean_day_type = str(day_type or "").strip() or None
    use_scope = site_id is not None and route_type is not None
    use_scope_table = use_scope and _supports_stop_scope()
    for _ in range(8):  # 최대 약 2.56도 확장
        if use_scope_table:
            day_type_scope_sql = "AND ss.day_type = ?" if clean_day_type else ""
            params: list[object] = [
                site_id,
                route_type,
            ]
            if clean_day_type:
                params.append(clean_day_type)
            params.extend(
                [
                    lat - delta,
                    lat + delta,
                    lon - delta,
                    lon + delta,
                ]
            )
            rows = db.execute(
                f"""
                SELECT DISTINCT s.stop_id, s.name, s.lat, s.lon
                FROM stop_scope ss
                JOIN stop_rtree r ON r.stop_id = ss.stop_id
                JOIN stop s ON s.stop_id = ss.stop_id
                WHERE ss.site_id = ?
                  AND ss.route_type = ?
                  {day_type_scope_sql}
                  AND s.lat IS NOT NULL
                  AND s.lon IS NOT NULL
                  AND r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                tuple(params),
            ).fetchall()
        elif use_scope:
            # stop_scope 없는 구버전 DB 호환용 폴백
            day_type_scope_sql = "AND sv.day_type = ?" if clean_day_type else ""
            params = [
                site_id,
                route_type,
            ]
            if clean_day_type:
                params.append(clean_day_type)
            params.extend(
                [
                    lat - delta,
                    lat + delta,
                    lon - delta,
                    lon + delta,
                ]
            )
            rows = db.execute(
                f"""
                SELECT DISTINCT s.stop_id, s.name, s.lat, s.lon
                FROM route ro
                JOIN service_variant sv ON sv.route_id = ro.route_id
                JOIN variant_stop vs ON vs.variant_id = sv.variant_id
                JOIN stop s ON s.stop_id = vs.stop_id
                JOIN stop_rtree r ON r.stop_id = s.stop_id
                WHERE ro.site_id = ?
                  AND ro.route_type = ?
                  {day_type_scope_sql}
                  AND s.lat IS NOT NULL
                  AND s.lon IS NOT NULL
                  AND r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                tuple(params),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT s.stop_id, s.name, s.lat, s.lon
                FROM stop_rtree r
                JOIN stop s ON s.stop_id = r.stop_id
                WHERE s.lat IS NOT NULL
                  AND s.lon IS NOT NULL
                  AND r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                (lat - delta, lat + delta, lon - delta, lon + delta),
            ).fetchall()
        if len(rows) >= max_stops:
            break
        delta *= 2

    results = []
    for r in rows:
        stop_lat = _coerce_float(_row_get(r, "lat", index=2))
        stop_lon = _coerce_float(_row_get(r, "lon", index=3))
        stop_id = _coerce_int(_row_get(r, "stop_id", index=0))
        stop_name = str(_row_get(r, "name", index=1, default="") or "").strip()
        if stop_lat is None or stop_lon is None or stop_id is None or not stop_name:
            _record_route_option_skip("invalid_nearby_stop")
            continue
        d = _haversine_m(lat, lon, stop_lat, stop_lon)
        results.append((stop_id, stop_name, stop_lat, stop_lon, d))
    results.sort(key=lambda x: x[4])
    return results[:max_stops]


def _find_routes_from_stops(
    db: sqlite3.Connection,
    stop_ids: list[int],
    stop_dist: dict[int, tuple[str, float, float, float]],
    site_id: str,
    route_type: str,
    day_type: str,
    include_route_ids: set[int] | None = None,
    exclude_route_name_keyword: str | None = None,
) -> dict[int, tuple[float, int, str]]:
    """
    정류장 목록에서 해당 조건의 노선을 찾아 route별 최소 거리 반환.
    반환: {route_id: (min_dist, best_stop_id, route_name)}
    """
    if not stop_ids:
        return {}
    placeholders = ",".join("?" * len(stop_ids))
    query = f"""
        SELECT DISTINCT r.route_id, r.route_name, r.route_type,
               vs.stop_id
        FROM variant_stop vs
        JOIN service_variant sv ON sv.variant_id = vs.variant_id
        JOIN route r ON r.route_id = sv.route_id
        WHERE vs.stop_id IN ({placeholders})
          AND r.site_id = ?
          AND r.route_type = ?
          AND sv.day_type = ?
    """
    params = stop_ids + [site_id, route_type, day_type]
    rows = db.execute(query, params).fetchall()

    route_best: dict[int, tuple[float, int, str]] = {}
    for r in rows:
        rid = _coerce_int(_row_get(r, "route_id", index=0))
        rname = str(_row_get(r, "route_name", index=1, default="") or "")
        route_type_value = str(_row_get(r, "route_type", index=2, default="") or "")
        sid = _coerce_int(_row_get(r, "stop_id", index=3))
        if rid is None or sid is None or sid not in stop_dist or not rname or not route_type_value:
            _record_route_option_skip("invalid_route_lookup_row")
            continue
        if include_route_ids is not None and rid not in include_route_ids:
            continue
        if exclude_route_name_keyword and exclude_route_name_keyword in rname:
            continue
        dist = stop_dist[sid][3]
        if rid not in route_best or dist < route_best[rid][0]:
            route_best[rid] = (dist, sid, rname)
    return route_best


def find_nearest_route_options(
    site_id: str,
    route_type: str,
    lat: float,
    lon: float,
    day_type: str = "weekday",
    max_routes: int = 5,
    exclude_route_ids: list[int] | None = None,
    include_route_ids: set[int] | None = None,
    exclude_route_name_keyword: str | None = None,
) -> list[dict]:
    """
    가까운 정류장 기반 유니크 노선 최대 max_routes개 반환.
    노선 수가 부족하면 탐색 범위를 점진 확장하여 max_routes개를 확보한다.
    exclude_route_ids가 주어지면 해당 노선을 결과에서 제외한다.
    include_route_ids가 주어지면 해당 노선 집합만 후보로 사용한다.
    exclude_route_name_keyword가 주어지면 route_name에 해당 키워드가 포함된 노선을 제외한다.

    반환 각 요소:
    {
        "route_id": int,
        "route_name": str,
        "route_type": str,
        "nearest_stop": {"stop_id": int, "name": str, "lat": float, "lon": float},
        "distance_m": float,
        "all_departure_times": [str, ...],
        "companies": [str, ...],
        "route_stops": [
            {
                "sequence": int,
                "stop_name": str,
                "lat": float,
                "lon": float,
                "next_encoded_polyline": str,
            },
            ...
        ],
    }
    """
    db = _db()
    excluded = set(exclude_route_ids) if exclude_route_ids else set()
    include_ids = set(include_route_ids) if include_route_ids is not None else None

    if include_ids is not None and not include_ids:
        return []

    # 점진 확장: 50 → 150 → 500개 정류장으로 넓혀가며 max_routes개 노선 확보
    route_best: dict[int, tuple[float, int, str]] = {}
    stop_dist: dict[int, tuple[str, float, float, float]] = {}
    searched_stop_ids: set[int] = set()

    for max_stops in (50, 150, 500):
        # 전역 정류장이 아닌, 요청 범위(site/route/day)의 정류장에서만 근접 후보를 고른다.
        nearby = _find_nearby_stops(
            lat,
            lon,
            max_stops=max_stops,
            site_id=site_id,
            route_type=route_type,
            day_type=day_type,
        )
        if not nearby:
            return []

        # 이전 라운드에서 이미 검색한 정류장 제외 (증분 검색)
        new_stop_ids = []
        for sid, name, slat, slon, dist in nearby:
            if sid not in searched_stop_ids:
                stop_dist[sid] = (name, slat, slon, dist)
                new_stop_ids.append(sid)
                searched_stop_ids.add(sid)

        # 새 정류장에서만 노선 검색 (이미 찾은 노선과 합산)
        if new_stop_ids:
            new_routes = _find_routes_from_stops(
                db,
                new_stop_ids,
                stop_dist,
                site_id,
                route_type,
                day_type,
                include_route_ids=include_ids,
                exclude_route_name_keyword=exclude_route_name_keyword,
            )
            for rid, (dist, sid, rname) in new_routes.items():
                if rid in excluded:
                    continue
                if rid not in route_best or dist < route_best[rid][0]:
                    route_best[rid] = (dist, sid, rname)

        if len(route_best) >= max_routes:
            break

    # 거리순 정렬, 상위 max_routes
    sorted_routes = sorted(route_best.items(), key=lambda x: x[1][0])[:max_routes]
    route_uuid_by_id: dict[int, str] = {}
    if sorted_routes and _supports_route_uuid():
        route_ids = [int(route_id) for route_id, _info in sorted_routes]
        placeholders = ",".join("?" for _ in route_ids)
        rows = db.execute(
            f"SELECT route_id, route_uuid FROM route WHERE route_id IN ({placeholders})",
            tuple(route_ids),
        ).fetchall()
        for row in rows:
            route_uuid = str(_row_get(row, "route_uuid", default="") or "").strip()
            if not route_uuid:
                continue
            route_uuid_by_id[int(_row_get(row, "route_id", default=0) or 0)] = route_uuid

    stop_uuid_by_id: dict[int, str] = {}
    stop_code_by_id: dict[int, str] = {}
    if sorted_routes and (_supports_stop_uuid() or _supports_stop_code()):
        stop_ids = [int(best_stop_id) for _route_id, (_dist, best_stop_id, _name) in sorted_routes]
        placeholders = ",".join("?" for _ in stop_ids)
        select_cols = ["stop_id"]
        if _supports_stop_uuid():
            select_cols.append("stop_uuid")
        if _supports_stop_code():
            select_cols.append("stop_code")
        rows = db.execute(
            f"SELECT {', '.join(select_cols)} FROM stop WHERE stop_id IN ({placeholders})",
            tuple(stop_ids),
        ).fetchall()
        for row in rows:
            stop_id = int(_row_get(row, "stop_id", default=0) or 0)
            if stop_id <= 0:
                continue
            stop_uuid = str(_row_get(row, "stop_uuid", default="") or "").strip()
            stop_code = str(_row_get(row, "stop_code", default="") or "").strip()
            if stop_uuid:
                stop_uuid_by_id[stop_id] = stop_uuid
            if stop_code:
                stop_code_by_id[stop_id] = stop_code

    results = []
    for route_id, (dist, best_stop_id, route_name) in sorted_routes:
        sname, slat, slon, sdist = stop_dist[best_stop_id]
        metadata = _get_route_metadata(db, route_id, day_type)
        if metadata is None:
            _logger.warning(
                "Skipping route option candidate due to invalid route metadata",
                extra={"route_id": route_id, "day_type": day_type},
            )
            continue

        results.append(
            {
                "route_id": route_id,
                "route_uuid": route_uuid_by_id.get(int(route_id)),
                "route_name": route_name,
                "route_type": route_type,
                "nearest_stop": {
                    "stop_id": best_stop_id,
                    "stop_uuid": stop_uuid_by_id.get(int(best_stop_id)),
                    "stop_code": stop_code_by_id.get(int(best_stop_id)),
                    "name": sname,
                    "lat": slat,
                    "lon": slon,
                },
                "distance_m": round(sdist),
                "all_departure_times": list(metadata["all_departure_times"]),
                "companies": list(metadata["companies"]),
                "route_stops": [dict(stop) for stop in metadata["route_stops"]],
            }
        )

    return results


def get_route_detail(
    route_id: int,
    day_type: str = "weekday",
    departure_time: str | None = None,
) -> dict | None:
    """특정 route의 전체 출발시간표 + 선택 출발편의 전체 경유지 반환."""
    db = _db()
    route_uuid_sql = ", route_uuid" if _supports_route_uuid() else ", NULL AS route_uuid"
    source_route_id_sql = ", source_route_id" if _supports_route_source_route_id() else ", NULL AS source_route_id"
    route = db.execute(
        f"SELECT route_id, site_id, route_name, route_type{route_uuid_sql}{source_route_id_sql} "
        "FROM route WHERE route_id = ?",
        (route_id,),
    ).fetchone()
    if not route:
        return None

    variants = db.execute(
        """
        SELECT variant_id, departure_time, company, bus_count
        FROM service_variant
        WHERE route_id = ? AND day_type = ?
        ORDER BY departure_time
        """,
        (route_id, day_type),
    ).fetchall()

    timetable = []
    for v in variants:
        departure_time_value = str(_row_get(v, "departure_time", index=1, default="") or "").strip()
        if not departure_time_value:
            _record_route_option_skip("invalid_timetable_departure")
            continue
        timetable.append(
            {
                "departure_time": departure_time_value,
                "company": _row_get(v, "company", index=2),
                "bus_count": _row_get(v, "bus_count", index=3),
            }
        )

    selected_variant = None
    if variants:
        if departure_time:
            clean_departure_time = str(departure_time).strip()
            for variant in variants:
                if str(_row_get(variant, "departure_time", index=1, default="") or "").strip() == clean_departure_time:
                    selected_variant = variant
                    break
        if selected_variant is None:
            selected_variant = variants[0]

    selected_variant_id = (
        _coerce_int(_row_get(selected_variant, "variant_id", index=0))
        if selected_variant is not None
        else None
    )
    route_stops = _get_variant_stops(db, selected_variant_id) if selected_variant_id is not None else []
    selected_departure_time_value = (
        str(_row_get(selected_variant, "departure_time", index=1, default="") or "").strip()
        if selected_variant
        else None
    )
    selected_companies = []
    if selected_departure_time_value:
        selected_companies = sorted(
            set(
                str(_row_get(v, "company", index=2, default="") or "").strip()
                for v in variants
                if str(_row_get(v, "departure_time", index=1, default="") or "").strip() == selected_departure_time_value
                and str(_row_get(v, "company", index=2, default="") or "").strip()
            )
        )

    return {
        "route_id": route["route_id"],
        "route_uuid": (str(route["route_uuid"] or "").strip() or None),
        "source_route_id": (str(route["source_route_id"] or "").strip() or None),
        "site_id": route["site_id"],
        "route_name": route["route_name"],
        "route_type": route["route_type"],
        "selected_departure_time": selected_departure_time_value,
        "selected_variant_id": selected_variant_id,
        "selected_operator": ", ".join(selected_companies),
        "timetable": timetable,
        "route_stops": route_stops,
    }


def _clock_diff_minutes(a: str, b: str) -> int:
    a_m = _time_to_minutes(a)
    b_m = _time_to_minutes(b)
    if a_m < 0 or b_m < 0:
        return 10**9
    diff = abs(a_m - b_m)
    return min(diff, 1440 - diff)


def _time_to_service_day_minutes(t: str) -> int:
    """04:00를 0분으로 보는 서비스일 축(0~1439)으로 변환."""
    raw_minutes = _time_to_minutes(t)
    if raw_minutes < 0:
        return -1
    if raw_minutes < 240:
        raw_minutes += 1440
    return raw_minutes - 240


def _service_day_delta_minutes(current_time: str, departure_time: str) -> int:
    """
    current_time 기준으로 가장 가까운 departure_time occurrence와의 signed delta.
    음수면 과거 출발편, 양수면 미래 출발편.
    """
    now_minutes = _time_to_service_day_minutes(current_time)
    dep_minutes = _time_to_service_day_minutes(departure_time)
    if now_minutes < 0 or dep_minutes < 0:
        return 10**9
    candidates = (
        dep_minutes - now_minutes,
        (dep_minutes + 1440) - now_minutes,
        (dep_minutes - 1440) - now_minutes,
    )
    return min(candidates, key=lambda value: abs(value))


def _is_departure_in_report_window(
    current_time: str,
    departure_time: str,
    past_minutes: int,
    future_minutes: int,
) -> bool:
    delta = _service_day_delta_minutes(current_time, departure_time)
    if delta == 10**9:
        return False
    return (-int(past_minutes)) <= delta <= int(future_minutes)


def _get_report_excluded_stop_ids(site_id: str, day_type: str | None) -> set[int]:
    clean_day_type = str(day_type or "").strip()
    cache_day_key = clean_day_type if clean_day_type else "__all__"
    key = (_current_thread_generation(), str(site_id), cache_day_key)
    with _report_stop_exclusion_lock:
        cached = _report_stop_exclusion_cache.get(key)
        if cached is not None:
            return set(cached)

    db = _db()
    excluded: set[int] = set()
    for direction in ("depart", "arrive"):
        route_type = _endpoint_route_type(direction)
        endpoint_seq_col = "es.max_seq" if direction == "depart" else "es.min_seq"
        day_type_filter_sql = "AND sv.day_type = ?" if clean_day_type else ""
        params: list[object] = [site_id, route_type]
        if clean_day_type:
            params.append(clean_day_type)
        rows = db.execute(
            f"""
            WITH target_routes AS (
                SELECT route_id
                FROM route
                WHERE site_id = ?
                  AND route_type = ?
                  AND route_name NOT LIKE '%테스트%'
            ),
            filtered_variant AS (
                SELECT DISTINCT sv.variant_id
                FROM service_variant sv
                JOIN target_routes tr ON tr.route_id = sv.route_id
                WHERE 1 = 1
                  {day_type_filter_sql}
            ),
            endpoint_seq AS (
                SELECT vs.variant_id, MIN(vs.seq) AS min_seq, MAX(vs.seq) AS max_seq
                FROM variant_stop vs
                JOIN filtered_variant fv ON fv.variant_id = vs.variant_id
                GROUP BY vs.variant_id
            )
            SELECT DISTINCT vs_ep.stop_id AS endpoint_stop_id
            FROM filtered_variant fv
            JOIN endpoint_seq es ON es.variant_id = fv.variant_id
            JOIN variant_stop vs_ep
              ON vs_ep.variant_id = fv.variant_id
             AND vs_ep.seq = {endpoint_seq_col}
            """,
            tuple(params),
        ).fetchall()
        excluded.update(int(row["endpoint_stop_id"]) for row in rows)

    with _report_stop_exclusion_lock:
        existing = _report_stop_exclusion_cache.get(key)
        if existing is not None:
            return set(existing)
        _report_stop_exclusion_cache[key] = set(excluded)
    return excluded


def get_report_nearby_stops(
    site_id: str,
    day_type: str | None,
    lat: float,
    lon: float,
    primary_radius_m: float = 150.0,
    fallback_radius_m: float = 250.0,
) -> dict:
    """
    도착시간 제보용 근처 정류장 후보를 반환한다.
    - 출근 종착지 / 퇴근 출발지(endpoint 정류장)는 제외한다.
    """
    clean_day_type = str(day_type or "").strip() or None
    excluded_stop_ids = _get_report_excluded_stop_ids(site_id, clean_day_type)
    nearby_in = _find_nearby_stops(
        lat,
        lon,
        max_stops=150,
        site_id=site_id,
        route_type="commute_in",
        day_type=clean_day_type,
    )
    nearby_out = _find_nearby_stops(
        lat,
        lon,
        max_stops=150,
        site_id=site_id,
        route_type="commute_out",
        day_type=clean_day_type,
    )

    stop_map: dict[int, tuple[str, float, float, float]] = {}
    for sid, name, slat, slon, dist in nearby_in + nearby_out:
        stop_id = int(sid)
        if stop_id in excluded_stop_ids:
            continue
        if stop_id not in stop_map or dist < stop_map[stop_id][3]:
            stop_map[stop_id] = (str(name or "").strip(), float(slat), float(slon), float(dist))

    def _group_named_stops(items: list[dict]) -> list[dict]:
        grouped_items: list[dict] = []
        grouped_by_name: dict[str, list[dict]] = {}
        for item in sorted(
            items,
            key=lambda value: (
                int(value["distance_m"]),
                str(value["stop_name"]),
                int(value["stop_id"]),
            ),
        ):
            stop_name = str(item["stop_name"] or "").strip()
            group_key = stop_name if stop_name else f"__stop_{int(item['stop_id'])}"
            candidates = grouped_by_name.get(group_key) or []
            group = None
            for existing in candidates:
                for plat, plng in existing["_points"]:
                    if _haversine_m(
                        float(item["lat"]),
                        float(item["lng"]),
                        float(plat),
                        float(plng),
                    ) <= _REPORT_SAME_NAME_GROUP_DISTANCE_M:
                        group = existing
                        break
                if group is not None:
                    break
            if group is None:
                group = {
                    "stop_id": int(item["stop_id"]),
                    "stop_ids": [int(item["stop_id"])],
                    "stop_name": stop_name,
                    "lat": float(item["lat"]),
                    "lng": float(item["lng"]),
                    "distance_m": int(item["distance_m"]),
                    "_points": [(float(item["lat"]), float(item["lng"]))],
                }
                grouped_by_name.setdefault(group_key, []).append(group)
                grouped_items.append(group)
                continue
            stop_id = int(item["stop_id"])
            if stop_id not in group["stop_ids"]:
                group["stop_ids"].append(stop_id)
                group["_points"].append((float(item["lat"]), float(item["lng"])))
        grouped_items.sort(
            key=lambda item: (
                int(item["distance_m"]),
                str(item["stop_name"]),
                int(item["stop_id"]),
            )
        )
        for item in grouped_items:
            item.pop("_points", None)
        return grouped_items

    def _stops_within(limit_m: float) -> list[dict]:
        items: list[dict] = []
        for stop_id, (name, slat, slon, dist_m) in stop_map.items():
            if dist_m <= float(limit_m):
                items.append(
                    {
                        "stop_id": int(stop_id),
                        "stop_name": str(name),
                        "lat": float(slat),
                        "lng": float(slon),
                        "distance_m": int(round(dist_m)),
                    }
                )
        return _group_named_stops(items)

    stops = _stops_within(primary_radius_m)
    fallback_attempted = False
    if not stops and fallback_radius_m > primary_radius_m:
        fallback_attempted = True
        stops = _stops_within(fallback_radius_m)
    used_fallback = fallback_attempted and bool(stops)

    return {
        "radius_m": float(fallback_radius_m if fallback_attempted else primary_radius_m),
        "used_fallback_radius": used_fallback,
        "stops": stops,
    }


def find_recent_report_stop_index_by_route(
    site_id: str,
    day_type: str | None,
    grouped_stops: list[dict] | tuple[dict, ...] | None,
    route_id: int | None,
    route_uuid: str | None = None,
) -> int:
    if not grouped_stops:
        return -1
    clean_route_uuid = str(route_uuid or "").strip()
    try:
        preferred_route_id = int(route_id or 0)
    except (TypeError, ValueError):
        preferred_route_id = 0
    use_route_uuid = bool(clean_route_uuid and _supports_route_uuid())
    if (not use_route_uuid) and preferred_route_id <= 0:
        return -1

    stop_id_to_index: dict[int, int] = {}
    normalized_stop_ids: list[int] = []
    for index, stop in enumerate(grouped_stops):
        stop = stop or {}
        raw_stop_ids = list(stop.get("stop_ids") or [])
        if not raw_stop_ids and stop.get("stop_id") is not None:
            raw_stop_ids = [stop.get("stop_id")]
        for raw_value in raw_stop_ids:
            try:
                stop_id = int(raw_value)
            except (TypeError, ValueError):
                continue
            if stop_id <= 0:
                continue
            if stop_id not in stop_id_to_index:
                stop_id_to_index[stop_id] = index
            if stop_id not in normalized_stop_ids:
                normalized_stop_ids.append(stop_id)

    if not normalized_stop_ids:
        return -1

    db = _db()
    placeholders = ",".join("?" for _ in normalized_stop_ids)
    clean_day_type = str(day_type or "").strip()
    day_filter_sql = "AND sv.day_type = ?" if clean_day_type else ""
    if use_route_uuid:
        params: list[object] = list(normalized_stop_ids)
        params.append(site_id)
        if clean_day_type:
            params.append(clean_day_type)
        params.append(clean_route_uuid)
        rows = db.execute(
            f"""
            SELECT DISTINCT vs.stop_id
            FROM variant_stop vs
            JOIN service_variant sv ON sv.variant_id = vs.variant_id
            JOIN route r ON r.route_id = sv.route_id
            WHERE vs.stop_id IN ({placeholders})
              AND r.site_id = ?
              {day_filter_sql}
              AND r.route_uuid = ?
              AND r.route_type IN ('commute_in', 'commute_out')
              AND r.route_name NOT LIKE '%테스트%'
            """,
            tuple(params),
        ).fetchall()
    else:
        params = list(normalized_stop_ids)
        params.append(site_id)
        if clean_day_type:
            params.append(clean_day_type)
        params.append(preferred_route_id)
        rows = db.execute(
            f"""
            SELECT DISTINCT vs.stop_id
            FROM variant_stop vs
            JOIN service_variant sv ON sv.variant_id = vs.variant_id
            JOIN route r ON r.route_id = sv.route_id
            WHERE vs.stop_id IN ({placeholders})
              AND r.site_id = ?
              {day_filter_sql}
              AND r.route_id = ?
              AND r.route_type IN ('commute_in', 'commute_out')
              AND r.route_name NOT LIKE '%테스트%'
            """,
            tuple(params),
        ).fetchall()
    if not rows:
        return -1

    matching_indices: set[int] = set()
    for row in rows:
        try:
            stop_id = int(row["stop_id"])
        except (TypeError, ValueError):
            continue
        group_index = stop_id_to_index.get(stop_id)
        if group_index is not None:
            matching_indices.add(group_index)

    if not matching_indices:
        return -1

    for index in range(len(grouped_stops)):
        if index in matching_indices:
            return index
    return -1


def _sort_report_departure_times(values: set[str]) -> list[str]:
    def _time_sort_key(value: str) -> tuple[int, str]:
        clean = str(value or "").strip()
        minutes = _time_to_minutes(clean)
        return (minutes if minutes >= 0 else 9999, clean)

    return sorted(
        {str(value or "").strip() for value in values if str(value or "").strip()},
        key=_time_sort_key,
    )


def _service_day_minutes_to_time(minutes: int) -> str:
    normalized = int(minutes) % 1440
    clock_minutes = (normalized + 240) % 1440
    return f"{clock_minutes // 60:02d}:{clock_minutes % 60:02d}"


def _shift_service_day_time(time_value: str, delta_minutes: int) -> str:
    base_minutes = _time_to_service_day_minutes(time_value)
    if base_minutes < 0:
        return str(time_value or "").strip()
    return _service_day_minutes_to_time(base_minutes + int(delta_minutes))


def _select_default_report_time(
    available_times: list[str],
    current_time: str,
    preferred_time: str | None = None,
) -> str:
    if preferred_time:
        clean_preferred = str(preferred_time).strip()
        if clean_preferred and clean_preferred in available_times:
            return clean_preferred
    best_value = ""
    best_abs_delta = 10**9
    for value in available_times:
        delta = _service_day_delta_minutes(current_time, value)
        if abs(delta) < best_abs_delta:
            best_abs_delta = abs(delta)
            best_value = value
    if best_value:
        return best_value
    return str(available_times[0] if available_times else "")


def _select_default_report_time_with_elapsed(
    available_times: list[str],
    current_time: str,
    elapsed_minutes_by_time: list[int | None] | tuple[int | None, ...] | None = None,
) -> str:
    if not available_times:
        return ""
    elapsed_values = list(elapsed_minutes_by_time or [])
    best_value = ""
    best_abs_delta = 10**9
    best_elapsed = -1
    for idx, value in enumerate(available_times):
        elapsed_minutes = elapsed_values[idx] if idx < len(elapsed_values) else None
        elapsed_int = int(elapsed_minutes) if elapsed_minutes is not None else 0
        reference_time = (
            _shift_service_day_time(current_time, -elapsed_int)
            if elapsed_int > 0
            else current_time
        )
        delta = _service_day_delta_minutes(reference_time, value)
        if (
            abs(delta) < best_abs_delta
            or (abs(delta) == best_abs_delta and elapsed_int > best_elapsed)
        ):
            best_abs_delta = abs(delta)
            best_value = value
            best_elapsed = elapsed_int
    if best_value:
        return str(best_value)
    return str(available_times[0] if available_times else "")


def get_report_route_candidates_for_stop(
    site_id: str,
    day_type: str | None,
    stop_id: int | list[int] | tuple[int, ...],
    current_time: str,
    primary_past_minutes: int = 20,
    primary_future_minutes: int = 5,
    fallback_windows: list[tuple[int, int]] | tuple[tuple[int, int], ...] | None = None,
    max_candidates: int = 50,
) -> dict:
    """
    선택한 정류장(동일명 그룹 가능)을 기준으로 도착시간 제보 노선 후보를 반환한다.
    - 각 route의 출발시각 중 하나라도 시간 윈도우에 포함되면 후보로 남긴다.
    - 별도 예외 없이 항상 주어진 시간 윈도우를 적용한다.
    """
    db = _db()
    clean_day_type = str(day_type or "").strip()
    if isinstance(stop_id, (list, tuple)):
        requested_stop_ids = [int(value) for value in stop_id]
    else:
        requested_stop_ids = [int(stop_id)]

    normalized_stop_ids: list[int] = []
    excluded_stop_ids = _get_report_excluded_stop_ids(site_id, clean_day_type or None)
    for value in requested_stop_ids:
        if value in excluded_stop_ids or value in normalized_stop_ids:
            continue
        normalized_stop_ids.append(value)

    if not normalized_stop_ids:
        return {
            "stop_id": int(requested_stop_ids[0]) if requested_stop_ids else 0,
            "stop_ids": [int(value) for value in requested_stop_ids],
            "stop_name": "",
            "day_type": (clean_day_type or None),
            "candidates": [],
            "total_route_count": 0,
            "time_filter_applied": False,
            "window_past_minutes": int(primary_past_minutes),
            "window_future_minutes": int(primary_future_minutes),
            "window_relaxed": False,
        }

    stop_placeholders = ",".join("?" for _ in normalized_stop_ids)
    route_uuid_select = "r.route_uuid AS route_uuid," if _supports_route_uuid() else "NULL AS route_uuid,"
    stop_uuid_select = "s.stop_uuid AS stop_uuid," if _supports_stop_uuid() else "NULL AS stop_uuid,"
    day_type_filter_sql = "AND sv.day_type = ?" if clean_day_type else ""
    params: list[object] = list(normalized_stop_ids)
    params.append(site_id)
    if clean_day_type:
        params.append(clean_day_type)
    rows = db.execute(
        f"""
        SELECT
            r.route_id,
            {route_uuid_select}
            r.route_name,
            r.route_type,
            sv.day_type AS day_type,
            sv.departure_time,
            MIN(sv.variant_id) AS variant_id,
            vs.stop_id,
            {stop_uuid_select}
            MIN(vs.seq) AS stop_sequence,
            s.name AS stop_name
        FROM variant_stop vs
        JOIN service_variant sv ON sv.variant_id = vs.variant_id
        JOIN route r ON r.route_id = sv.route_id
        JOIN stop s ON s.stop_id = vs.stop_id
        WHERE vs.stop_id IN ({stop_placeholders})
          AND r.site_id = ?
          AND r.route_type IN ('commute_in', 'commute_out')
          {day_type_filter_sql}
          AND r.route_name NOT LIKE '%테스트%'
        GROUP BY
            r.route_id,
            route_uuid,
            r.route_name,
            r.route_type,
            sv.day_type,
            sv.departure_time,
            vs.stop_id,
            stop_uuid,
            s.name
        """,
        tuple(params),
    ).fetchall()
    if not rows:
        return {
            "stop_id": int(normalized_stop_ids[0]),
            "stop_ids": list(normalized_stop_ids),
            "stop_name": "",
            "day_type": (clean_day_type or None),
            "candidates": [],
            "total_route_count": 0,
            "time_filter_applied": False,
            "window_past_minutes": int(primary_past_minutes),
            "window_future_minutes": int(primary_future_minutes),
            "window_relaxed": False,
        }

    raw_items: list[dict] = []
    stop_name = str(rows[0]["stop_name"] or "").strip()
    for row in rows:
        departure_time = str(row["departure_time"] or "").strip()
        raw_items.append(
            {
                "route_id": int(row["route_id"]),
                "route_uuid": (str(row["route_uuid"] or "").strip() or None),
                "route_name": str(row["route_name"] or "").strip(),
                "route_type": str(row["route_type"] or "").strip(),
                "direction": "depart" if str(row["route_type"]) == "commute_in" else "arrive",
                "day_type": str(row["day_type"] or "").strip(),
                "departure_time": departure_time,
                "variant_id": int(row["variant_id"]),
                "stop_id": int(row["stop_id"]),
                "stop_uuid": (str(row["stop_uuid"] or "").strip() or None),
                "stop_name": str(row["stop_name"] or "").strip(),
                "stop_sequence": int(row["stop_sequence"] or 0),
                "_delta_minutes": _service_day_delta_minutes(current_time, departure_time),
            }
        )

    total_route_count = len({int(item["route_id"]) for item in raw_items})
    time_filter_applied = True
    windows: list[tuple[int, int]] = [(int(primary_past_minutes), int(primary_future_minutes))]
    fallback_window_values = ((30, 5), (45, 5)) if fallback_windows is None else fallback_windows
    for past_minutes, future_minutes in fallback_window_values:
        pair = (int(past_minutes), int(future_minutes))
        if pair not in windows:
            windows.append(pair)

    filtered_items: list[dict] = []
    applied_window = windows[0]
    window_relaxed = False
    for idx, (past_minutes, future_minutes) in enumerate(windows):
        current_filtered = [
            item
            for item in raw_items
            if _is_departure_in_report_window(
                current_time,
                str(item["departure_time"]),
                past_minutes,
                future_minutes,
            )
        ]
        if current_filtered:
            filtered_items = current_filtered
            applied_window = (past_minutes, future_minutes)
            window_relaxed = idx > 0
            break
    if not filtered_items:
        last_window = windows[-1]
        return {
            "stop_id": int(normalized_stop_ids[0]),
            "stop_ids": list(normalized_stop_ids),
            "stop_name": stop_name,
            "day_type": (clean_day_type or None),
            "candidates": [],
            "total_route_count": int(total_route_count),
            "time_filter_applied": True,
            "window_past_minutes": int(last_window[0]),
            "window_future_minutes": int(last_window[1]),
            "window_relaxed": bool(len(windows) > 1),
        }

    day_type_rank = {"weekday": 1, "monday": 2, "saturday": 3, "holiday": 4, "familyday": 5}

    def _sort_day_types(values: set[str]) -> list[str]:
        return sorted(
            {str(value or "").strip() for value in values if str(value or "").strip()},
            key=lambda value: (day_type_rank.get(value, 99), value),
        )

    grouped_by_route: dict[int, list[dict]] = defaultdict(list)
    for item in filtered_items:
        grouped_by_route[int(item["route_id"])].append(item)

    route_candidates: list[dict] = []
    for route_items in grouped_by_route.values():
        route_items_sorted = sorted(
            route_items,
            key=lambda item: (
                abs(int(item["_delta_minutes"])),
                int(item["stop_sequence"]),
                str(item["departure_time"]),
                str(item["route_name"]),
            ),
        )
        if not route_items_sorted:
            continue
        best_item = route_items_sorted[0]
        day_type_options = _sort_day_types({str(item.get("day_type") or "") for item in route_items_sorted})
        variant_stop_cache: dict[int, list[dict]] = {}

        def _serialize_context_stop(stop_row: dict | None) -> dict | None:
            if not stop_row:
                return None
            return {
                "sequence": int(stop_row["sequence"]),
                "stop_id": int(stop_row["stop_id"]),
                "stop_uuid": (str(stop_row.get("stop_uuid") or "").strip() or None),
                "stop_name": str(stop_row["stop_name"] or "").strip(),
            }

        def _build_departure_context(source_item: dict | None) -> dict:
            if not source_item:
                return {
                    "prev": None,
                    "current": None,
                    "next": None,
                    "has_before": False,
                    "has_after": False,
                    "estimated_elapsed_min": 0,
                }
            variant_id = int(source_item["variant_id"])
            variant_stops = variant_stop_cache.get(variant_id)
            if variant_stops is None:
                variant_stops = _get_variant_stops(db, variant_id)
                variant_stop_cache[variant_id] = variant_stops
            target_index = -1
            target_stop_id = int(source_item["stop_id"])
            target_sequence = int(source_item["stop_sequence"])
            for idx, stop_row in enumerate(variant_stops):
                if int(stop_row["stop_id"]) == target_stop_id and int(stop_row["sequence"]) == target_sequence:
                    target_index = idx
                    break
            if target_index < 0:
                for idx, stop_row in enumerate(variant_stops):
                    if int(stop_row["sequence"]) == target_sequence:
                        target_index = idx
                        break
            if target_index < 0:
                for idx, stop_row in enumerate(variant_stops):
                    if int(stop_row["stop_id"]) == target_stop_id:
                        target_index = idx
                        break
            if target_index < 0:
                return {
                    "prev": None,
                    "current": None,
                    "next": None,
                    "has_before": False,
                    "has_after": False,
                    "estimated_elapsed_min": 0,
                }
            prev_stop = variant_stops[target_index - 1] if target_index > 0 else None
            current_stop = variant_stops[target_index]
            next_stop = variant_stops[target_index + 1] if target_index + 1 < len(variant_stops) else None
            return {
                "prev": _serialize_context_stop(prev_stop),
                "current": _serialize_context_stop(current_stop),
                "next": _serialize_context_stop(next_stop),
                "has_before": bool(target_index > 1),
                "has_after": bool(target_index + 2 < len(variant_stops)),
                "estimated_elapsed_min": int(current_stop.get("estimated_elapsed_min") or 0),
            }

        if not clean_day_type:
            preview_context = _build_departure_context(best_item)
            route_candidates.append(
                {
                    "route_id": int(best_item["route_id"]),
                    "route_uuid": (str(best_item.get("route_uuid") or "").strip() or None),
                    "route_name": str(best_item["route_name"]),
                    "route_type": str(best_item["route_type"]),
                    "direction": str(best_item["direction"]),
                    "day_type": None,
                    "day_type_options": list(day_type_options),
                    "selected_day_type": str(day_type_options[0] if day_type_options else ""),
                    "stop_id": int(best_item["stop_id"]),
                    "stop_uuid": (str(best_item.get("stop_uuid") or "").strip() or None),
                    "stop_name": str(best_item["stop_name"]),
                    "stop_sequence": int(best_item["stop_sequence"]),
                    "departure_times": [],
                    "departure_sequences": [],
                    "departure_contexts": [preview_context],
                    "selected_departure_time": "",
                    "_best_abs_delta_m": abs(int(best_item["_delta_minutes"])),
                }
            )
            continue

        available_times = _sort_report_departure_times(
            {str(item["departure_time"]) for item in route_items_sorted}
        )
        sequence_by_time: dict[str, int] = {}
        context_source_by_time: dict[str, dict] = {}
        for item in route_items_sorted:
            departure_time = str(item["departure_time"])
            stop_sequence = int(item["stop_sequence"])
            existing_sequence = sequence_by_time.get(departure_time)
            if existing_sequence is None or stop_sequence < existing_sequence:
                sequence_by_time[departure_time] = stop_sequence
                context_source_by_time[departure_time] = item
        departure_sequences = [str(sequence_by_time.get(str(value), "")) for value in available_times]

        departure_contexts: list[dict] = []
        departure_elapsed_minutes: list[int] = []
        for value in available_times:
            context = _build_departure_context(context_source_by_time.get(str(value)))
            departure_contexts.append(context)
            departure_elapsed_minutes.append(int(context.get("estimated_elapsed_min") or 0))
        route_candidates.append(
            {
                "route_id": int(best_item["route_id"]),
                "route_uuid": (str(best_item.get("route_uuid") or "").strip() or None),
                "route_name": str(best_item["route_name"]),
                "route_type": str(best_item["route_type"]),
                "direction": str(best_item["direction"]),
                "day_type": clean_day_type,
                "day_type_options": [clean_day_type],
                "selected_day_type": clean_day_type,
                "stop_id": int(best_item["stop_id"]),
                "stop_uuid": (str(best_item.get("stop_uuid") or "").strip() or None),
                "stop_name": str(best_item["stop_name"]),
                "stop_sequence": int(best_item["stop_sequence"]),
                "departure_times": available_times,
                "departure_sequences": departure_sequences,
                "departure_contexts": departure_contexts,
                "selected_departure_time": _select_default_report_time_with_elapsed(
                    available_times,
                    current_time,
                    departure_elapsed_minutes,
                ),
                "_best_abs_delta_m": abs(int(best_item["_delta_minutes"])),
            }
        )

    ranked = sorted(
        route_candidates,
        key=lambda item: (
            int(item["_best_abs_delta_m"]),
            str(item["route_name"]),
            int(item["stop_sequence"]),
        ),
    )[: max(1, int(max_candidates))]

    return {
        "stop_id": int(normalized_stop_ids[0]),
        "stop_ids": list(normalized_stop_ids),
        "stop_name": stop_name,
        "day_type": (clean_day_type or None),
        "candidates": [
            {
                "route_id": int(item["route_id"]),
                "route_uuid": (str(item.get("route_uuid") or "").strip() or None),
                "route_name": str(item["route_name"]),
                "route_type": str(item["route_type"]),
                "direction": str(item["direction"]),
                "day_type": (str(item.get("day_type") or "").strip() or None),
                "day_type_options": [
                    str(value or "").strip()
                    for value in list(item.get("day_type_options") or [])
                    if str(value or "").strip()
                ],
                "selected_day_type": str(item.get("selected_day_type") or "").strip(),
                "stop_id": int(item["stop_id"]),
                "stop_uuid": (str(item.get("stop_uuid") or "").strip() or None),
                "stop_name": str(item["stop_name"]),
                "stop_sequence": int(item["stop_sequence"]),
                "departure_times": list(item["departure_times"]),
                "departure_sequences": list(item.get("departure_sequences") or []),
                "departure_contexts": list(item.get("departure_contexts") or []),
                "selected_departure_time": str(item["selected_departure_time"]),
            }
            for item in ranked
        ],
        "total_route_count": int(total_route_count),
        "time_filter_applied": bool(time_filter_applied),
        "window_past_minutes": int(applied_window[0]),
        "window_future_minutes": int(applied_window[1]),
        "window_relaxed": bool(window_relaxed),
    }


def get_report_candidate_options(
    site_id: str,
    day_type: str,
    lat: float,
    lon: float,
    current_time: str,
    primary_radius_m: float = 150.0,
    fallback_radius_m: float = 250.0,
    max_candidates: int = 3,
) -> dict:
    """
    현재 위치 근처 정류장 기반 제보 후보를 반환한다.

    반환:
    {
        "radius_m": float,
        "used_fallback_radius": bool,
        "candidates": [
            {
                "route_id": int,
                "route_name": str,
                "route_type": str,
                "direction": "depart" | "arrive",
                "stop_id": int,
                "stop_name": str,
                "stop_sequence": int,
                "distance_m": int,
                "departure_times": [str, ...],
                "selected_departure_time": str,
            },
            ...
        ],
    }
    """
    db = _db()
    nearby = _find_nearby_stops(
        lat,
        lon,
        max_stops=150,
        site_id=site_id,
        route_type="commute_in",
        day_type=day_type,
    )
    nearby_out = _find_nearby_stops(
        lat,
        lon,
        max_stops=150,
        site_id=site_id,
        route_type="commute_out",
        day_type=day_type,
    )

    stop_map: dict[int, tuple[str, float, float, float]] = {}
    for sid, name, slat, slon, dist in nearby + nearby_out:
        if sid not in stop_map or dist < stop_map[sid][3]:
            stop_map[sid] = (name, slat, slon, dist)

    if not stop_map:
        return {"radius_m": float(primary_radius_m), "used_fallback_radius": False, "candidates": []}

    def stop_ids_within(limit_m: float) -> list[int]:
        return [sid for sid, data in stop_map.items() if data[3] <= limit_m]

    candidate_stop_ids = stop_ids_within(primary_radius_m)
    used_fallback = False
    if not candidate_stop_ids and fallback_radius_m > primary_radius_m:
        candidate_stop_ids = stop_ids_within(fallback_radius_m)
        used_fallback = bool(candidate_stop_ids)
    if not candidate_stop_ids:
        return {
            "radius_m": float(fallback_radius_m if used_fallback else primary_radius_m),
            "used_fallback_radius": used_fallback,
            "candidates": [],
        }

    placeholders = ",".join("?" * len(candidate_stop_ids))
    route_uuid_select = "r.route_uuid AS route_uuid," if _supports_route_uuid() else "NULL AS route_uuid,"
    stop_uuid_select = "s.stop_uuid AS stop_uuid," if _supports_stop_uuid() else "NULL AS stop_uuid,"
    rows = db.execute(
        f"""
        SELECT
            r.route_id,
            {route_uuid_select}
            r.route_name,
            r.route_type,
            sv.departure_time,
            MIN(sv.variant_id) AS variant_id,
            vs.stop_id,
            {stop_uuid_select}
            MIN(vs.seq) AS stop_sequence,
            s.name AS stop_name
        FROM variant_stop vs
        JOIN service_variant sv ON sv.variant_id = vs.variant_id
        JOIN route r ON r.route_id = sv.route_id
        JOIN stop s ON s.stop_id = vs.stop_id
        WHERE vs.stop_id IN ({placeholders})
          AND r.site_id = ?
          AND r.route_type IN ('commute_in', 'commute_out')
          AND sv.day_type = ?
          AND r.route_name NOT LIKE '%테스트%'
        GROUP BY
            r.route_id,
            route_uuid,
            r.route_name,
            r.route_type,
            sv.departure_time,
            vs.stop_id,
            stop_uuid,
            s.name
        """,
        tuple(candidate_stop_ids + [site_id, day_type]),
    ).fetchall()

    deduped: dict[tuple[int, str, int], dict] = {}
    for row in rows:
        stop_id = int(row["stop_id"])
        stop_info = stop_map.get(stop_id)
        if stop_info is None:
            continue
        key = (int(row["route_id"]), str(row["departure_time"] or "").strip(), stop_id)
        candidate = {
            "route_id": int(row["route_id"]),
            "route_uuid": (str(row["route_uuid"] or "").strip() or None),
            "route_name": str(row["route_name"] or "").strip(),
            "route_type": str(row["route_type"] or "").strip(),
            "direction": "depart" if str(row["route_type"]) == "commute_in" else "arrive",
            "departure_time": str(row["departure_time"] or "").strip(),
            "variant_id": int(row["variant_id"]),
            "stop_id": stop_id,
            "stop_uuid": (str(row["stop_uuid"] or "").strip() or None),
            "stop_name": str(row["stop_name"] or stop_info[0] or "").strip(),
            "stop_sequence": int(row["stop_sequence"] or 0),
            "distance_m": round(stop_info[3]),
            "time_diff_m": _clock_diff_minutes(current_time, str(row["departure_time"] or "").strip()),
        }
        existing = deduped.get(key)
        if existing is None:
            deduped[key] = candidate
            continue
        existing_key = (
            int(existing["distance_m"]),
            int(existing["time_diff_m"]),
            int(existing["stop_sequence"]),
            str(existing["route_name"]),
        )
        next_key = (
            int(candidate["distance_m"]),
            int(candidate["time_diff_m"]),
            int(candidate["stop_sequence"]),
            str(candidate["route_name"]),
        )
        if next_key < existing_key:
            deduped[key] = candidate

    route_candidates: list[dict] = []
    grouped_by_route: dict[int, list[dict]] = defaultdict(list)
    for item in deduped.values():
        grouped_by_route[int(item["route_id"])].append(item)

    def _time_sort_key(value: str) -> tuple[int, str]:
        t = str(value or "").strip()
        mins = _time_to_minutes(t)
        return (mins if mins >= 0 else 9999, t)

    for route_items in grouped_by_route.values():
        sorted_items = sorted(
            route_items,
            key=lambda item: (
                int(item["distance_m"]),
                int(item["time_diff_m"]),
                int(item["stop_sequence"]),
                str(item["departure_time"]),
                str(item["stop_name"]),
            ),
        )
        if not sorted_items:
            continue
        best_item = sorted_items[0]
        chosen_stop_id = int(best_item["stop_id"])
        same_stop_items = [
            item for item in sorted_items
            if int(item["stop_id"]) == chosen_stop_id
        ]
        available_times = sorted(
            {
                str(item["departure_time"] or "").strip()
                for item in same_stop_items
                if str(item["departure_time"] or "").strip()
            },
            key=_time_sort_key,
        )
        default_time = get_nearest_departure_time(available_times, current_time) if available_times else None
        if default_time is None and available_times:
            default_time = available_times[0]
        route_candidates.append(
            {
                "route_id": int(best_item["route_id"]),
                "route_uuid": (str(best_item.get("route_uuid") or "").strip() or None),
                "route_name": str(best_item["route_name"]),
                "route_type": str(best_item["route_type"]),
                "direction": str(best_item["direction"]),
                "stop_id": chosen_stop_id,
                "stop_uuid": (str(best_item.get("stop_uuid") or "").strip() or None),
                "stop_name": str(best_item["stop_name"]),
                "stop_sequence": int(best_item["stop_sequence"]),
                "distance_m": int(best_item["distance_m"]),
                "departure_times": available_times,
                "selected_departure_time": str(default_time or ""),
                "_best_time_diff_m": int(best_item["time_diff_m"]),
            }
        )

    ranked = sorted(
        route_candidates,
        key=lambda item: (
            int(item["distance_m"]),
            int(item["_best_time_diff_m"]),
            str(item["route_name"]),
            int(item["stop_sequence"]),
        ),
    )[: max(1, int(max_candidates))]

    return {
        "radius_m": float(fallback_radius_m if used_fallback else primary_radius_m),
        "used_fallback_radius": used_fallback,
        "candidates": [
            {
                "route_id": int(item["route_id"]),
                "route_uuid": (str(item.get("route_uuid") or "").strip() or None),
                "route_name": str(item["route_name"]),
                "route_type": str(item["route_type"]),
                "direction": str(item["direction"]),
                "stop_id": int(item["stop_id"]),
                "stop_uuid": (str(item.get("stop_uuid") or "").strip() or None),
                "stop_name": str(item["stop_name"]),
                "stop_sequence": int(item["stop_sequence"]),
                "distance_m": int(item["distance_m"]),
                "departure_times": list(item["departure_times"]),
                "selected_departure_time": str(item["selected_departure_time"]),
            }
            for item in ranked
        ],
    }


def get_report_tuple_meta(
    site_id: str,
    day_type: str,
    route_id: int,
    departure_time: str,
    stop_id: int,
) -> dict | None:
    """제보 가능한 (route, departure_time, stop) 조합을 검증한다."""
    db = _db()
    route_uuid_sql = ", r.route_uuid AS route_uuid" if _supports_route_uuid() else ", NULL AS route_uuid"
    source_route_id_sql = ", r.source_route_id AS source_route_id" if _supports_route_source_route_id() else ", NULL AS source_route_id"
    stop_uuid_sql = ", s.stop_uuid AS stop_uuid" if _supports_stop_uuid() else ", NULL AS stop_uuid"
    stop_code_sql = ", s.stop_code AS stop_code" if _supports_stop_code() else ", NULL AS stop_code"
    row = db.execute(
        f"""
        SELECT
            r.route_id,
            r.route_name,
            r.route_type,
            sv.variant_id,
            sv.departure_time,
            vs.stop_id,
            vs.seq AS stop_sequence,
            s.name AS stop_name,
            s.lat,
            s.lon
            {route_uuid_sql}
            {source_route_id_sql}
            {stop_uuid_sql}
            {stop_code_sql}
        FROM route r
        JOIN service_variant sv ON sv.route_id = r.route_id
        JOIN variant_stop vs ON vs.variant_id = sv.variant_id
        JOIN stop s ON s.stop_id = vs.stop_id
        WHERE r.site_id = ?
          AND r.route_id = ?
          AND r.route_type IN ('commute_in', 'commute_out')
          AND sv.day_type = ?
          AND sv.departure_time = ?
          AND vs.stop_id = ?
          AND r.route_name NOT LIKE '%테스트%'
        ORDER BY sv.variant_id ASC
        LIMIT 1
        """,
        (site_id, int(route_id), str(day_type), str(departure_time), int(stop_id)),
    ).fetchone()
    if row is None:
        return None
    route_type = str(row["route_type"] or "").strip()
    return {
        "route_id": int(row["route_id"]),
        "route_uuid": (str(row["route_uuid"] or "").strip() or None),
        "source_route_id": (str(row["source_route_id"] or "").strip() or None),
        "route_name": str(row["route_name"] or "").strip(),
        "route_type": route_type,
        "direction": "depart" if route_type == "commute_in" else "arrive",
        "variant_id": int(row["variant_id"]),
        "departure_time": str(row["departure_time"] or "").strip(),
        "stop_id": int(row["stop_id"]),
        "stop_uuid": (str(row["stop_uuid"] or "").strip() or None),
        "stop_code": (str(row["stop_code"] or "").strip() or None),
        "stop_name": str(row["stop_name"] or "").strip(),
        "stop_sequence": int(row["stop_sequence"] or 0),
        "stop_lat": float(row["lat"]),
        "stop_lng": float(row["lon"]),
    }


def get_report_tuple_meta_by_uuid(
    site_id: str,
    day_type: str,
    route_uuid: str,
    departure_time: str,
    stop_uuid: str,
) -> dict | None:
    """UUID 기준으로 제보 가능한 (route, departure_time, stop) 조합을 검증한다."""
    if not (_supports_route_uuid() and _supports_stop_uuid()):
        return None

    clean_route_uuid = str(route_uuid or "").strip()
    clean_stop_uuid = str(stop_uuid or "").strip()
    if not clean_route_uuid or not clean_stop_uuid:
        return None

    db = _db()
    source_route_id_sql = ", r.source_route_id AS source_route_id" if _supports_route_source_route_id() else ", NULL AS source_route_id"
    stop_code_sql = ", s.stop_code AS stop_code" if _supports_stop_code() else ", NULL AS stop_code"
    row = db.execute(
        f"""
        SELECT
            r.route_id,
            r.route_uuid,
            r.route_name,
            r.route_type,
            sv.variant_id,
            sv.departure_time,
            vs.stop_id,
            vs.seq AS stop_sequence,
            s.stop_uuid,
            s.name AS stop_name,
            s.lat,
            s.lon
            {source_route_id_sql}
            {stop_code_sql}
        FROM route r
        JOIN service_variant sv ON sv.route_id = r.route_id
        JOIN variant_stop vs ON vs.variant_id = sv.variant_id
        JOIN stop s ON s.stop_id = vs.stop_id
        WHERE r.site_id = ?
          AND r.route_uuid = ?
          AND r.route_type IN ('commute_in', 'commute_out')
          AND sv.day_type = ?
          AND sv.departure_time = ?
          AND s.stop_uuid = ?
          AND r.route_name NOT LIKE '%테스트%'
        ORDER BY sv.variant_id ASC
        LIMIT 1
        """,
        (site_id, clean_route_uuid, str(day_type), str(departure_time), clean_stop_uuid),
    ).fetchone()
    if row is None:
        return None
    route_type = str(row["route_type"] or "").strip()
    return {
        "route_id": int(row["route_id"]),
        "route_uuid": (str(row["route_uuid"] or "").strip() or None),
        "source_route_id": (str(row["source_route_id"] or "").strip() or None),
        "route_name": str(row["route_name"] or "").strip(),
        "route_type": route_type,
        "direction": "depart" if route_type == "commute_in" else "arrive",
        "variant_id": int(row["variant_id"]),
        "departure_time": str(row["departure_time"] or "").strip(),
        "stop_id": int(row["stop_id"]),
        "stop_uuid": (str(row["stop_uuid"] or "").strip() or None),
        "stop_code": (str(row["stop_code"] or "").strip() or None),
        "stop_name": str(row["stop_name"] or "").strip(),
        "stop_sequence": int(row["stop_sequence"] or 0),
        "stop_lat": float(row["lat"]),
        "stop_lng": float(row["lon"]),
    }


# ── 시간 관련 유틸 (기존 호환) ──────────────────────────────────
def _time_to_minutes(t: str) -> int:
    """'HH:MM' → 분 단위 (0~1439)."""
    parts = t.strip().split(":")
    if len(parts) != 2:
        return -1
    try:
        h, m = int(parts[0]), int(parts[1])
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h * 60 + m
    except ValueError:
        pass
    return -1


def _parse_time_or_range(t: str) -> tuple[int, int]:
    t = t.strip()
    if "~" in t:
        parts = t.split("~", 1)
        start_m = _time_to_minutes(parts[0].strip())
        end_m = _time_to_minutes(parts[1].strip())
        if start_m < 0 or end_m < 0:
            return (-1, -1)
        return (start_m, end_m)
    m = _time_to_minutes(t)
    return (m, m) if m >= 0 else (-1, -1)


def get_nearest_departure_time(times: list[str], time_str: str) -> str | None:
    """시각 리스트에서 time_str 이후 가장 가까운 출발 시각 반환."""
    if not times:
        return None
    user_m = _time_to_minutes(time_str)
    if user_m < 0:
        return None

    def sort_key(t: str) -> int:
        s, _ = _parse_time_or_range(t)
        return s if s >= 0 else 9999

    for t in sorted(times, key=sort_key):
        start_m, end_m = _parse_time_or_range(t)
        if start_m < 0:
            continue
        if start_m <= user_m <= end_m:
            return t
        if start_m >= user_m:
            return t
    return None
