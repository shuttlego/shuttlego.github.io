"""
SQLite 기반 데이터 접근 레이어.
data/data.db 를 read-only로 열어 검색 쿼리를 수행한다.
"""

import math
import os
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
DB_PATH = DATA_DIR / "data.db"

_conn: sqlite3.Connection | None = None
_has_stop_scope: bool | None = None


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
        sid = r["stop_id"]
        dist = stop_dist[sid][3]
        if rid not in route_best or dist < route_best[rid][0]:
            route_best[rid] = (dist, sid, r["route_name"])
    return route_best


def find_nearest_route_options(
    site_id: str,
    route_type: str,
    lat: float,
    lon: float,
    day_type: str = "weekday",
    max_routes: int = 5,
    exclude_route_ids: list[int] | None = None,
) -> list[dict]:
    """
    가까운 정류장 기반 유니크 노선 최대 max_routes개 반환.
    노선 수가 부족하면 탐색 범위를 점진 확장하여 max_routes개를 확보한다.
    exclude_route_ids가 주어지면 해당 노선을 결과에서 제외한다.

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
                db, new_stop_ids, stop_dist, site_id, route_type, day_type
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
