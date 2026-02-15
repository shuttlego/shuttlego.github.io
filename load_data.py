"""
시작 시 data 폴더 CSV 4개를 읽어 메모리에 보관.
"""
import csv
import math
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"

sites: list[dict] = []
routes: list[dict] = []
departure_times: dict[int, list[str]] = {}  # route_id -> [time, ...]
stops: dict[int, list[dict]] = {}  # route_id -> [{sequence, stop_name, lat, lng}, ...]


def _haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def load_all() -> None:
    global sites, routes, departure_times, stops
    sites = []
    routes = []
    departure_times = {}
    stops = {}

    with open(DATA_DIR / "sites.csv", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            sites.append({"site_id": int(row["site_id"]), "site_name": row["site_name"]})

    with open(DATA_DIR / "routes.csv", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            routes.append({
                "route_id": int(row["route_id"]),
                "site_id": int(row["site_id"]),
                "route_name": row["route_name"],
                "type": int(row["type"]),
                "operator": row.get("operator", ""),
                "notes": row.get("notes", ""),
            })

    with open(DATA_DIR / "departure_times.csv", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rid = int(row["route_id"])
            if rid not in departure_times:
                departure_times[rid] = []
            departure_times[rid].append(row["time"].strip())

    with open(DATA_DIR / "stops.csv", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rid = int(row["route_id"])
            if rid not in stops:
                stops[rid] = []
            lat_s, lng_s = (row.get("lat") or "").strip(), (row.get("lng") or "").strip()
            try:
                lat = float(lat_s) if lat_s else None
                lng = float(lng_s) if lng_s else None
            except ValueError:
                lat, lng = None, None
            stops[rid].append({
                "sequence": int(row["sequence"]),
                "stop_name": row["stop_name"],
                "lat": lat,
                "lng": lng,
            })
    for rid in stops:
        stops[rid].sort(key=lambda s: s["sequence"])


def get_routes_by_site_and_type(site_id: int, route_type: int) -> list[dict]:
    """site_id, type(1=출근, 2=퇴근)에 해당하는 노선 목록."""
    return [r for r in routes if r["site_id"] == site_id and r["type"] == route_type]


def find_nearest_stop(site_id: int, route_type: int, lat: float, lng: float) -> tuple[dict, dict, dict] | None:
    """
    해당 사업장의 출근(1) 또는 퇴근(2) 노선들의 정류장 중 (lat,lng)에서 가장 가까운 정류장을 찾음.
    반환: (route, stop, route_stops) 또는 None.
    """
    candidate_routes = get_routes_by_site_and_type(site_id, route_type)
    best = None
    best_dist = float("inf")
    for r in candidate_routes:
        rid = r["route_id"]
        for stop in stops.get(rid, []):
            if stop["lat"] is None or stop["lng"] is None:
                continue
            d = _haversine_km(lat, lng, stop["lat"], stop["lng"])
            if d < best_dist:
                best_dist = d
                best = (r, stop, stops[rid])
    return best


def find_nearest_route_options(
    site_id: int,
    route_type: int,
    lat: float,
    lng: float,
    max_routes: int = 3,
) -> list[tuple[dict, dict, list[dict]]]:
    """
    가까운 정류장 순으로 노선당 첫 등장만 채택해, 유니크 노선 최대 max_routes개 반환.
    각 요소: (route, nearest_stop, route_stops).
    """
    candidate_routes = get_routes_by_site_and_type(site_id, route_type)
    candidates: list[tuple[float, dict, dict]] = []
    for r in candidate_routes:
        rid = r["route_id"]
        for stop in stops.get(rid, []):
            if stop["lat"] is None or stop["lng"] is None:
                continue
            d = _haversine_km(lat, lng, stop["lat"], stop["lng"])
            candidates.append((d, stop, r))
    candidates.sort(key=lambda x: x[0])

    seen_route_ids: set[int] = set()
    results: list[tuple[dict, dict, list[dict]]] = []
    for _d, stop, route in candidates:
        rid = route["route_id"]
        if rid in seen_route_ids:
            continue
        seen_route_ids.add(rid)
        results.append((route, stop, stops[rid]))
        if len(results) >= max_routes:
            break
    return results


def get_terminus_stop(route_stops: list[dict]) -> dict | None:
    """sequence 최대인 정류장 (종착지)."""
    if not route_stops:
        return None
    return max(route_stops, key=lambda s: s["sequence"])


def get_first_stop(route_stops: list[dict]) -> dict | None:
    """sequence 최소인 정류장 (기점)."""
    if not route_stops:
        return None
    return min(route_stops, key=lambda s: s["sequence"])


def _time_to_minutes(t: str) -> int:
    """'HH:mm' -> 분 단위 (0~1439)."""
    parts = t.strip().split(":")
    if len(parts) != 2:
        return -1
    try:
        h, m = int(parts[0], 10), int(parts[1], 10)
        if 0 <= h <= 23 and 0 <= m <= 59:
            return h * 60 + m
    except ValueError:
        pass
    return -1


def _parse_time_or_range(t: str) -> tuple[int, int]:
    """
    'HH:MM' 또는 'HH:MM ~ HH:MM' -> (시작분, 종료분). 단일이면 (m, m).
    파싱 실패 시 (-1, -1).
    """
    t = t.strip()
    if "~" in t:
        parts = t.split("~", 1)
        if len(parts) != 2:
            return (-1, -1)
        start_m = _time_to_minutes(parts[0].strip())
        end_m = _time_to_minutes(parts[1].strip())
        if start_m < 0 or end_m < 0:
            return (-1, -1)
        return (start_m, end_m)
    m = _time_to_minutes(t)
    if m < 0:
        return (-1, -1)
    return (m, m)


def _departure_sort_key(t: str) -> int:
    """정렬용: 시작 시각(분)."""
    start_m, _ = _parse_time_or_range(t)
    return start_m if start_m >= 0 else 9999


def get_nearest_departure_time(route_id: int, time_str: str) -> str | None:
    """
    사용자 시각 time_str(HH:mm) 기준으로 해당 노선의 가장 가까운 미래 출발 시각을 반환.
    단일 시각 또는 범위('HH:MM ~ HH:MM') 모두 처리. 사용자 시각이 범위 안에 있으면 그 범위 반환.
    없으면(당일 마지막 배차 이후면) None을 반환.
    """
    times = departure_times.get(route_id)
    if not times:
        return None
    user_m = _time_to_minutes(time_str)
    if user_m < 0:
        return None
    sorted_times = sorted(times, key=_departure_sort_key)
    for t in sorted_times:
        start_m, end_m = _parse_time_or_range(t)
        if start_m < 0:
            continue
        if start_m <= user_m <= end_m:
            return t
        if start_m >= user_m:
            return t
    return None


def get_all_departure_times(route_id: int) -> list[str]:
    """노선의 전체 출발 시각 목록을 시작 시각 순 정렬하여 반환. 단일/범위 모두 포함."""
    times = departure_times.get(route_id)
    if not times:
        return []
    return sorted(times, key=_departure_sort_key)
