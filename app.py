"""
셔틀 안내 백엔드 API.
검색은 카카오 로컬 REST API, data 폴더 CSV는 시작 시 전부 로드.
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
    find_nearest_stop,
    get_all_departure_times,
    get_first_stop,
    get_nearest_departure_time,
    get_terminus_stop,
    load_all,
)

app = Flask(__name__)

# ── CORS: 프론트엔드 도메인 허용 ──────────────────────────
# CORS_ORIGINS 환경변수로 허용 origin을 설정 (쉼표 구분).
# 예) CORS_ORIGINS=http://localhost:5500,https://shuttlego.github.io
_cors_origins = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:5500,http://localhost:3000,http://127.0.0.1:5500",
)
CORS(app, origins=[o.strip() for o in _cors_origins.split(",") if o.strip()])

KAKAO_REST_API_KEY = os.environ.get("KAKAO_REST_API_KEY", "")
SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
SEARCH_SIZE = 15

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

UNIQUE_USER_REQUESTS = Counter(
    "shuttle_user_requests_total",
    "Total requests per unique user (by client IP)",
    ["client_ip"],
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


# ── 유틸리티 ────────────────────────────────────────────────
def _get_client_ip() -> str:
    """Traefik 뒤에서 실제 클라이언트 IP 추출 (X-Forwarded-For)."""
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


# ── 요청 전/후 훅 (메트릭 수집) ──────────────────────────────
@app.before_request
def _before_request():
    request._prom_start_time = _time.time()
    IN_PROGRESS_REQUESTS.inc()
    client_ip = _get_client_ip()
    UNIQUE_USER_REQUESTS.labels(client_ip=client_ip).inc()


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
    """위경도 두 점 사이 직선 거리(미터). Haversine 근사."""
    R = 6371000  # 지구 반경 m
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def search_places(query: str) -> list[dict]:
    """카카오 로컬 API 키워드 검색. 최대 15건 반환."""
    if not KAKAO_REST_API_KEY:
        return []
    headers = {"Authorization": f"KakaoAK {KAKAO_REST_API_KEY}"}
    params = {"query": query, "size": SEARCH_SIZE}
    try:
        resp = requests.get(SEARCH_URL, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        KAKAO_API_CALLS.labels(status="success").inc()
        data = resp.json()
        return data.get("documents") or []
    except Exception:
        KAKAO_API_CALLS.labels(status="error").inc()
        return []


# ── API 엔드포인트 ───────────────────────────────────────────
@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"documents": []})
    documents = search_places(q)
    return jsonify({"documents": documents})


@app.route("/api/sites")
def api_sites():
    """사업장 목록 (site_id, site_name)."""
    return jsonify({"sites": load_data.sites})


@app.route("/api/shuttle/depart")
def api_shuttle_depart():
    """
    출근: 목적지(사업장)로 가기. place_lat, place_lng에서 가장 가까운 출근(type=1) 정류장 찾기.
    반환: 출발지(선택 장소), 탑승 정류장, 종착지(노선 terminus) 3점 + 메시지.
    """
    site_id = request.args.get("site_id", type=int, default=1)
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()
    result = find_nearest_stop(site_id, 1, lat, lng)  # type 1 = 출근
    if not result:
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404
    route, nearest_stop, route_stops = result
    terminus = get_terminus_stop(route_stops)
    if not terminus:
        return jsonify({"error": "노선 종착지 없음"}), 404
    board_time = get_nearest_departure_time(route["route_id"], time_param) if time_param else None
    positions = [
        {"lat": lat, "lng": lng, "label": "출발"},
        {"lat": nearest_stop["lat"], "lng": nearest_stop["lng"], "label": nearest_stop["stop_name"]},
        {"lat": terminus["lat"], "lng": terminus["lng"], "label": "종착"},
    ]
    message = f"{place_name}에서 출근하기 위해서는 {nearest_stop['stop_name']} 정류장에서 {route['route_name']} 출근 버스(노선)를 탑승하세요."
    payload = {
        "positions": positions,
        "message": message,
        "route_name": route["route_name"],
        "nearest_stop_name": nearest_stop["stop_name"],
        "terminus_name": terminus["stop_name"],
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/shuttle/depart/options")
def api_shuttle_depart_options():
    """
    출근 노선 후보 최대 5개. 각 옵션은 단일 출근 API와 동일한 형태.
    """
    site_id = request.args.get("site_id", type=int, default=1)
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()
    results = find_nearest_route_options(site_id, 1, lat, lng, max_routes=5)
    if not results:
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404
    options = []
    for route, nearest_stop, route_stops in results:
        terminus = get_terminus_stop(route_stops)
        if not terminus:
            continue
        board_time = get_nearest_departure_time(route["route_id"], time_param) if time_param else None
        if time_param and board_time is None:
            continue
        positions = [
            {"lat": lat, "lng": lng, "label": "출발"},
            {"lat": nearest_stop["lat"], "lng": nearest_stop["lng"], "label": nearest_stop["stop_name"]},
            {"lat": terminus["lat"], "lng": terminus["lng"], "label": "종착"},
        ]
        message = f"{place_name}에서 출근하기 위해서는 {nearest_stop['stop_name']} 정류장에서 {route['route_name']} 출근 버스(노선)를 탑승하세요."
        distance_m = haversine_distance_m(lat, lng, nearest_stop["lat"], nearest_stop["lng"])
        payload = {
            "positions": positions,
            "message": message,
            "route_name": route["route_name"],
            "route_id": route["route_id"],
            "operator": route.get("operator", ""),
            "notes": route.get("notes", ""),
            "nearest_stop_name": nearest_stop["stop_name"],
            "terminus_name": terminus["stop_name"],
            "distance_m": round(distance_m),
            "all_departure_times": get_all_departure_times(route["route_id"]),
            "route_stops": [
                {"sequence": s["sequence"], "stop_name": s["stop_name"], "lat": s["lat"], "lng": s["lng"]}
                for s in route_stops
            ],
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)
    return jsonify({"options": options})


@app.route("/api/shuttle/arrive")
def api_shuttle_arrive():
    """
    퇴근: 선택한 장소로 가기. 해당 사업장 퇴근(type=2) 노선 중 가장 가까운 하차 정류장 찾기.
    반환: 기점(노선 sequence=1), 하차 정류장, 도착지(선택 장소) 3점 + 메시지.
    """
    site_id = request.args.get("site_id", type=int, default=1)
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()
    result = find_nearest_stop(site_id, 2, lat, lng)  # type 2 = 퇴근
    if not result:
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404
    route, nearest_stop, route_stops = result
    first = get_first_stop(route_stops)
    if not first:
        return jsonify({"error": "노선 기점 없음"}), 404
    board_time = get_nearest_departure_time(route["route_id"], time_param) if time_param else None
    positions = [
        {"lat": first["lat"], "lng": first["lng"], "label": first["stop_name"]},
        {"lat": nearest_stop["lat"], "lng": nearest_stop["lng"], "label": nearest_stop["stop_name"]},
        {"lat": lat, "lng": lng, "label": "도착"},
    ]
    message = f"{place_name}(으)로 가기 위해서는 {route['route_name']} 퇴근 버스를 {first['stop_name']}에서 탑승하고 {nearest_stop['stop_name']}에서 하차하세요."
    payload = {
        "positions": positions,
        "message": message,
        "route_name": route["route_name"],
        "start_stop_name": first["stop_name"],
        "getoff_stop_name": nearest_stop["stop_name"],
        "getoff_stop_sequence": nearest_stop["sequence"],
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/shuttle/arrive/options")
def api_shuttle_arrive_options():
    """
    퇴근 노선 후보 최대 5개. 각 옵션은 단일 퇴근 API와 동일한 형태.
    """
    site_id = request.args.get("site_id", type=int, default=1)
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()
    results = find_nearest_route_options(site_id, 2, lat, lng, max_routes=5)
    if not results:
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404
    options = []
    for route, nearest_stop, route_stops in results:
        first = get_first_stop(route_stops)
        if not first:
            continue
        board_time = get_nearest_departure_time(route["route_id"], time_param) if time_param else None
        if time_param and board_time is None:
            continue
        positions = [
            {"lat": first["lat"], "lng": first["lng"], "label": first["stop_name"]},
            {"lat": nearest_stop["lat"], "lng": nearest_stop["lng"], "label": nearest_stop["stop_name"]},
            {"lat": lat, "lng": lng, "label": "도착"},
        ]
        message = f"{place_name}(으)로 가기 위해서는 {route['route_name']} 퇴근 버스를 {first['stop_name']}에서 탑승하고 {nearest_stop['stop_name']}에서 하차하세요."
        distance_m = haversine_distance_m(lat, lng, nearest_stop["lat"], nearest_stop["lng"])
        payload = {
            "positions": positions,
            "message": message,
            "route_name": route["route_name"],
            "route_id": route["route_id"],
            "operator": route.get("operator", ""),
            "notes": route.get("notes", ""),
            "start_stop_name": first["stop_name"],
            "getoff_stop_name": nearest_stop["stop_name"],
            "getoff_stop_sequence": nearest_stop["sequence"],
            "distance_m": round(distance_m),
            "all_departure_times": get_all_departure_times(route["route_id"]),
            "route_stops": [
                {"sequence": s["sequence"], "stop_name": s["stop_name"], "lat": s["lat"], "lng": s["lng"]}
                for s in route_stops
            ],
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)
    return jsonify({"options": options})


# 시작 시 data 로드 (import 시 1회)
load_all()

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=8081)
