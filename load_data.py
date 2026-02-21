"""
SQLite 기반 데이터 접근 레이어.
data/data.db 를 read-only로 열어 검색 쿼리를 수행한다.
"""

import math
import os
import re
import sqlite3
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "data.db"

_conn: sqlite3.Connection | None = None
_has_stop_scope: bool | None = None
_endpoint_cache_lock = threading.Lock()
_endpoint_cache: dict[tuple[str, str, str], dict] = {}


def init_db(db_path: str | None = None) -> None:
    """SQLite DB 연결. 앱 시작 시 1회 호출."""
    global _conn, _has_stop_scope
    path = db_path or str(DB_PATH)
    _conn = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True, check_same_thread=False)
    _conn.row_factory = sqlite3.Row
    _conn.execute("PRAGMA query_only=ON")
    _has_stop_scope = bool(
        _conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stop_scope' LIMIT 1"
        ).fetchone()
    )


def _db() -> sqlite3.Connection:
    if _conn is None:
        init_db()
    return _conn


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


# ── 공개 API ──────────────────────────────────────────────────
def get_sites() -> list[dict]:
    rows = _db().execute("SELECT site_id, name FROM site ORDER BY site_id").fetchall()
    return [{"site_id": r["site_id"], "site_name": r["name"]} for r in rows]


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
    """
    db = _db()
    route_type = _endpoint_route_type(direction)

    if direction == "depart":
        endpoint_expr = "st_max.name"
    else:
        endpoint_expr = "st_min.name"

    rows = db.execute(
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
            {endpoint_expr} AS endpoint_name
        FROM first_variant fv
        JOIN endpoint_seq es ON es.variant_id = fv.variant_id
        LEFT JOIN variant_stop vs_min
            ON vs_min.variant_id = fv.variant_id
           AND vs_min.seq = es.min_seq
        LEFT JOIN stop st_min ON st_min.stop_id = vs_min.stop_id
        LEFT JOIN variant_stop vs_max
            ON vs_max.variant_id = fv.variant_id
           AND vs_max.seq = es.max_seq
        LEFT JOIN stop st_max ON st_max.stop_id = vs_max.stop_id
        """,
        (site_id, route_type, day_type),
    ).fetchall()

    endpoint_to_routes: dict[str, set[int]] = {}
    for row in rows:
        normalized = _normalize_endpoint_name(row["endpoint_name"])
        if not normalized:
            continue
        endpoint_to_routes.setdefault(normalized, set()).add(int(row["route_id"]))

    options = [
        {"endpoint_name": endpoint, "route_count": len(route_ids)}
        for endpoint, route_ids in endpoint_to_routes.items()
    ]
    options.sort(key=lambda item: item["endpoint_name"])
    return {"options": options, "endpoint_to_routes": endpoint_to_routes}


def _get_or_build_endpoint_cache(site_id: str, day_type: str, direction: str) -> dict:
    key = (site_id, day_type, direction)
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


def get_endpoint_route_ids(
    site_id: str,
    day_type: str,
    direction: str,
    selected_endpoints: list[str] | None,
) -> set[int]:
    """
    선택 endpoint에 매핑되는 route_id 집합을 반환.
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
    use_scope = site_id is not None and route_type is not None and day_type is not None
    use_scope_table = use_scope and _supports_stop_scope()
    for _ in range(8):  # 최대 약 2.56도 확장
        if use_scope_table:
            rows = db.execute(
                """
                SELECT DISTINCT s.stop_id, s.name, s.lat, s.lon
                FROM stop_scope ss
                JOIN stop_rtree r ON r.stop_id = ss.stop_id
                JOIN stop s ON s.stop_id = ss.stop_id
                WHERE ss.site_id = ?
                  AND ss.route_type = ?
                  AND ss.day_type = ?
                  AND r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                (
                    site_id,
                    route_type,
                    day_type,
                    lat - delta,
                    lat + delta,
                    lon - delta,
                    lon + delta,
                ),
            ).fetchall()
        elif use_scope:
            # stop_scope 없는 구버전 DB 호환용 폴백
            rows = db.execute(
                """
                SELECT DISTINCT s.stop_id, s.name, s.lat, s.lon
                FROM route ro
                JOIN service_variant sv ON sv.route_id = ro.route_id
                JOIN variant_stop vs ON vs.variant_id = sv.variant_id
                JOIN stop s ON s.stop_id = vs.stop_id
                JOIN stop_rtree r ON r.stop_id = s.stop_id
                WHERE ro.site_id = ?
                  AND ro.route_type = ?
                  AND sv.day_type = ?
                  AND r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                (
                    site_id,
                    route_type,
                    day_type,
                    lat - delta,
                    lat + delta,
                    lon - delta,
                    lon + delta,
                ),
            ).fetchall()
        else:
            rows = db.execute(
                """
                SELECT s.stop_id, s.name, s.lat, s.lon
                FROM stop_rtree r
                JOIN stop s ON s.stop_id = r.stop_id
                WHERE r.min_lat >= ? AND r.max_lat <= ?
                  AND r.min_lon >= ? AND r.max_lon <= ?
                """,
                (lat - delta, lat + delta, lon - delta, lon + delta),
            ).fetchall()
        if len(rows) >= max_stops:
            break
        delta *= 2

    results = []
    for r in rows:
        d = _haversine_m(lat, lon, r["lat"], r["lon"])
        results.append((r["stop_id"], r["name"], r["lat"], r["lon"], d))
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
        rid = r["route_id"]
        rname = r["route_name"]
        if include_route_ids is not None and rid not in include_route_ids:
            continue
        if exclude_route_name_keyword and exclude_route_name_keyword in rname:
            continue
        sid = r["stop_id"]
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
        "route_stops": [{"sequence": int, "stop_name": str, "lat": float, "lon": float}, ...],
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

    results = []
    for route_id, (dist, best_stop_id, route_name) in sorted_routes:
        sname, slat, slon, sdist = stop_dist[best_stop_id]

        # 전체 출발시간 + 업체 목록
        variants = db.execute(
            """
            SELECT departure_time, company, bus_count
            FROM service_variant
            WHERE route_id = ? AND day_type = ?
            ORDER BY departure_time
            """,
            (route_id, day_type),
        ).fetchall()

        all_times = sorted(set(v["departure_time"] for v in variants))
        companies = sorted(set(v["company"] for v in variants if v["company"]))

        # 대표 경유지 (첫 variant의 stops)
        first_variant = db.execute(
            "SELECT variant_id FROM service_variant WHERE route_id = ? AND day_type = ? LIMIT 1",
            (route_id, day_type),
        ).fetchone()

        route_stops = []
        if first_variant:
            stops_rows = db.execute(
                """
                SELECT vs.seq, s.name, s.lat, s.lon
                FROM variant_stop vs
                JOIN stop s ON s.stop_id = vs.stop_id
                WHERE vs.variant_id = ?
                ORDER BY vs.seq
                """,
                (first_variant["variant_id"],),
            ).fetchall()
            route_stops = [
                {"sequence": sr["seq"], "stop_name": sr["name"], "lat": sr["lat"], "lng": sr["lon"]}
                for sr in stops_rows
            ]

        results.append(
            {
                "route_id": route_id,
                "route_name": route_name,
                "route_type": route_type,
                "nearest_stop": {
                    "stop_id": best_stop_id,
                    "name": sname,
                    "lat": slat,
                    "lon": slon,
                },
                "distance_m": round(sdist),
                "all_departure_times": all_times,
                "companies": companies,
                "route_stops": route_stops,
            }
        )

    return results


def get_route_detail(route_id: int, day_type: str = "weekday") -> dict | None:
    """특정 route의 전체 출발시간표 + 전체 경유지 반환."""
    db = _db()
    route = db.execute(
        "SELECT route_id, site_id, route_name, route_type FROM route WHERE route_id = ?",
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

    timetable = [
        {
            "departure_time": v["departure_time"],
            "company": v["company"],
            "bus_count": v["bus_count"],
        }
        for v in variants
    ]

    # 대표 경유지
    route_stops = []
    if variants:
        stops_rows = db.execute(
            """
            SELECT vs.seq, s.name, s.lat, s.lon
            FROM variant_stop vs
            JOIN stop s ON s.stop_id = vs.stop_id
            WHERE vs.variant_id = ?
            ORDER BY vs.seq
            """,
            (variants[0]["variant_id"],),
        ).fetchall()
        route_stops = [
            {"sequence": sr["seq"], "stop_name": sr["name"], "lat": sr["lat"], "lng": sr["lon"]}
            for sr in stops_rows
        ]

    return {
        "route_id": route["route_id"],
        "site_id": route["site_id"],
        "route_name": route["route_name"],
        "route_type": route["route_type"],
        "timetable": timetable,
        "route_stops": route_stops,
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
