"""
셔틀 안내 백엔드 API.
검색은 카카오 로컬 REST API, 노선 데이터는 SQLite(data/data.db) 기반.
프론트엔드는 별도 호스팅 (GitHub Pages 등).
"""

import math
import os
import time as _time

import requests
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    REGISTRY,
    CONTENT_TYPE_LATEST,
)

import load_data
from load_data import (
    find_nearest_route_options,
    get_nearest_departure_time,
    get_route_detail,
    get_sites,
    init_db,
)

app = Flask(__name__)

# ── CORS: 프론트엔드 도메인 허용 ──────────────────────────
_cors_origins = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:5500,http://localhost:3000,http://127.0.0.1:5500",
)
CORS(app, origins=[o.strip() for o in _cors_origins.split(",") if o.strip()])

KAKAO_REST_API_KEY = os.environ.get("KAKAO_REST_API_KEY", "")
SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
SEARCH_SIZE = 15

# ── route_type 하위 호환 매핑 ──────────────────────────────
_ROUTE_TYPE_COMPAT = {"1": "commute_in", "2": "commute_out", "5": "shuttle"}

# ── Prometheus 메트릭 정의 ──────────────────────────────────
REQUEST_COUNT = Counter(
    "shuttle_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)

REQUEST_LATENCY = Histogram(
    "shuttle_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "endpoint"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

KAKAO_API_CALLS = Counter(
    "shuttle_kakao_api_calls_total",
    "Total calls to Kakao Local API",
    ["status"],
)

SHUTTLE_SEARCH_COUNT = Counter(
    "shuttle_search_total",
    "Shuttle route search count",
    ["direction"],
)

IN_PROGRESS_REQUESTS = Gauge(
    "shuttle_in_progress_requests",
    "Number of requests currently being processed",
)

APP_INFO = Gauge(
    "shuttle_app_info",
    "Application metadata",
    ["version"],
)
APP_INFO.labels(version=os.environ.get("APP_VERSION", "1.0.0")).set(1)


# ── 요청 전/후 훅 (메트릭 수집) ──────────────────────────────
@app.before_request
def _before_request():
    request._prom_start_time = _time.time()
    IN_PROGRESS_REQUESTS.inc()


@app.after_request
def _after_request(response):
    if request.path == "/metrics":
        IN_PROGRESS_REQUESTS.dec()
        return response
    latency = _time.time() - getattr(request, "_prom_start_time", _time.time())
    endpoint = request.path
    method = request.method
    status = str(response.status_code)
    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(latency)
    IN_PROGRESS_REQUESTS.dec()
    return response


# ── Prometheus / Health 엔드포인트 ───────────────────────────
@app.route("/metrics")
def metrics():
    """Prometheus 메트릭 노출."""
    return Response(generate_latest(REGISTRY), mimetype=CONTENT_TYPE_LATEST)


@app.route("/health")
def health():
    """Traefik 헬스체크용."""
    return jsonify({"status": "healthy"})


# ── 비즈니스 로직 ────────────────────────────────────────────
def haversine_distance_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def search_places(query: str) -> list[dict]:
    if not KAKAO_REST_API_KEY:
        return []
    headers = {"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"}
    params = {"query": query, "size": SEARCH_SIZE}
    try:
        resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        KAKAO_API_CALLS.labels(status="success").inc()
        return resp.json().get("documents") or []
    except Exception:
        KAKAO_API_CALLS.labels(status="error").inc()
        return []


def _resolve_route_type(raw: str | None, default: str) -> str:
    """route_type 파라미터 해석. 숫자('1','2','5') 또는 문자열 모두 지원."""
    if not raw:
        return default
    return _ROUTE_TYPE_COMPAT.get(raw, raw)


# ── API 엔드포인트 ───────────────────────────────────────────
@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"documents": []})
    return jsonify({"documents": search_places(q)})


@app.route("/api/sites")
def api_sites():
    return jsonify({"sites": get_sites()})


@app.route("/api/shuttle/depart/options")
def api_shuttle_depart_options():
    """출근 노선 후보 최대 5개."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_in",
        lat=lat,
        lon=lng,
        day_type=day_type,
        max_routes=5,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    for r in results:
        ns = r["nearest_stop"]
        route_stops = r["route_stops"]
        terminus = route_stops[-1] if route_stops else None

        board_time = (
            get_nearest_departure_time(r["all_departure_times"], time_param)
            if time_param
            else None
        )
        if time_param and board_time is None:
            continue

        positions = [
            {"lat": lat, "lng": lng, "label": "출발"},
            {"lat": ns["lat"], "lng": ns["lon"], "label": ns["name"]},
        ]
        if terminus:
            positions.append({"lat": terminus["lat"], "lng": terminus["lng"], "label": "종착"})

        message = (
            f"{place_name}에서 출근하기 위해서는 {ns['name']} 정류장에서 "
            f"{r['route_name']} 출근 버스(노선)를 탑승하세요."
        )

        payload = {
            "positions": positions,
            "message": message,
            "route_name": r["route_name"],
            "route_id": r["route_id"],
            "operator": ", ".join(r["companies"]),
            "nearest_stop_name": ns["name"],
            "terminus_name": terminus["stop_name"] if terminus else "",
            "distance_m": r["distance_m"],
            "all_departure_times": r["all_departure_times"],
            "route_stops": route_stops,
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)

    return jsonify({"options": options})


@app.route("/api/shuttle/depart")
def api_shuttle_depart():
    """출근: 가장 가까운 1개 노선."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()

    results = find_nearest_route_options(
        site_id=site_id, route_type="commute_in", lat=lat, lon=lng,
        day_type=day_type, max_routes=1,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404

    r = results[0]
    ns = r["nearest_stop"]
    route_stops = r["route_stops"]
    terminus = route_stops[-1] if route_stops else None

    board_time = (
        get_nearest_departure_time(r["all_departure_times"], time_param)
        if time_param
        else None
    )

    positions = [
        {"lat": lat, "lng": lng, "label": "출발"},
        {"lat": ns["lat"], "lng": ns["lon"], "label": ns["name"]},
    ]
    if terminus:
        positions.append({"lat": terminus["lat"], "lng": terminus["lng"], "label": "종착"})

    message = (
        f"{place_name}에서 출근하기 위해서는 {ns['name']} 정류장에서 "
        f"{r['route_name']} 출근 버스(노선)를 탑승하세요."
    )

    payload = {
        "positions": positions,
        "message": message,
        "route_name": r["route_name"],
        "nearest_stop_name": ns["name"],
        "terminus_name": terminus["stop_name"] if terminus else "",
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/shuttle/arrive/options")
def api_shuttle_arrive_options():
    """퇴근 노선 후보 최대 5개."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_out",
        lat=lat,
        lon=lng,
        day_type=day_type,
        max_routes=5,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    for r in results:
        ns = r["nearest_stop"]
        route_stops = r["route_stops"]
        first = route_stops[0] if route_stops else None

        board_time = (
            get_nearest_departure_time(r["all_departure_times"], time_param)
            if time_param
            else None
        )
        if time_param and board_time is None:
            continue

        positions = []
        if first:
            positions.append({"lat": first["lat"], "lng": first["lng"], "label": first["stop_name"]})
        positions.append({"lat": ns["lat"], "lng": ns["lon"], "label": ns["name"]})
        positions.append({"lat": lat, "lng": lng, "label": "도착"})

        message = (
            f"{place_name}(으)로 가기 위해서는 {r['route_name']} 퇴근 버스를 "
            f"{first['stop_name'] if first else ''}에서 탑승하고 {ns['name']}에서 하차하세요."
        )
        distance_m = haversine_distance_m(lat, lng, ns["lat"], ns["lon"])

        payload = {
            "positions": positions,
            "message": message,
            "route_name": r["route_name"],
            "route_id": r["route_id"],
            "operator": ", ".join(r["companies"]),
            "start_stop_name": first["stop_name"] if first else "",
            "getoff_stop_name": ns["name"],
            "distance_m": round(distance_m),
            "all_departure_times": r["all_departure_times"],
            "route_stops": route_stops,
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)

    return jsonify({"options": options})


@app.route("/api/shuttle/arrive")
def api_shuttle_arrive():
    """퇴근: 가장 가까운 1개 노선."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()

    results = find_nearest_route_options(
        site_id=site_id, route_type="commute_out", lat=lat, lon=lng,
        day_type=day_type, max_routes=1,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404

    r = results[0]
    ns = r["nearest_stop"]
    route_stops = r["route_stops"]
    first = route_stops[0] if route_stops else None

    board_time = (
        get_nearest_departure_time(r["all_departure_times"], time_param)
        if time_param
        else None
    )

    positions = []
    if first:
        positions.append({"lat": first["lat"], "lng": first["lng"], "label": first["stop_name"]})
    positions.append({"lat": ns["lat"], "lng": ns["lon"], "label": ns["name"]})
    positions.append({"lat": lat, "lng": lng, "label": "도착"})

    message = (
        f"{place_name}(으)로 가기 위해서는 {r['route_name']} 퇴근 버스를 "
        f"{first['stop_name'] if first else ''}에서 탑승하고 {ns['name']}에서 하차하세요."
    )

    payload = {
        "positions": positions,
        "message": message,
        "route_name": r["route_name"],
        "start_stop_name": first["stop_name"] if first else "",
        "getoff_stop_name": ns["name"],
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/route/<int:route_id>/detail")
def api_route_detail(route_id: int):
    """특정 route의 전체 시간표 + 경유지."""
    day_type = request.args.get("day_type", default="weekday")
    detail = get_route_detail(route_id, day_type)
    if not detail:
        return jsonify({"error": "노선을 찾을 수 없습니다."}), 404
    return jsonify(detail)


# ── 시작 시 DB 초기화 ─────────────────────────────────────────
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=8081)
