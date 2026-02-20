"""
셔틀 안내 백엔드 API.
검색은 카카오 로컬 REST API, 노선 데이터는 SQLite(data/data.db) 기반.
프론트엔드는 별도 호스팅 (GitHub Pages 등).
"""

import hashlib
import hmac
import json
import math
import os
import threading
import time as _time
import uuid
from datetime import datetime

import requests
from flask import Flask, jsonify, make_response, request, Response
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
    get_db_updated_at,
    get_nearest_departure_time,
    get_route_detail,
    get_sites,
    init_db,
)

try:
    import jwt  # type: ignore[import-untyped]
except Exception:  # pragma: no cover - 의존성 미설치 환경에서 graceful fallback
    jwt = None

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


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


GITHUB_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com").rstrip("/")
GITHUB_APP_ID = os.environ.get("GITHUB_APP_ID", "").strip()
GITHUB_INSTALLATION_ID = os.environ.get("GITHUB_INSTALLATION_ID", "").strip()
GITHUB_REPO_OWNER = os.environ.get("GITHUB_REPO_OWNER", "").strip()
GITHUB_REPO_NAME = os.environ.get("GITHUB_REPO_NAME", "").strip()
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
GITHUB_ISSUE_CACHE_TTL_SEC = _env_int("GITHUB_ISSUE_CACHE_TTL_SEC", 60)
ISSUE_SUBMIT_RATE_LIMIT_SEC = _env_int("ISSUE_SUBMIT_RATE_LIMIT_SEC", 10)
ISSUE_SUBMIT_DEDUPE_WINDOW_SEC = _env_int("ISSUE_SUBMIT_DEDUPE_WINDOW_SEC", 300)

GITHUB_APP_PRIVATE_KEY = os.environ.get("GITHUB_APP_PRIVATE_KEY", "").replace("\\n", "\n").strip()
if not GITHUB_APP_PRIVATE_KEY:
    private_key_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH", "").strip()
    if private_key_path:
        try:
            with open(private_key_path, "r", encoding="utf-8") as f:
                GITHUB_APP_PRIVATE_KEY = f.read().strip()
        except OSError:
            GITHUB_APP_PRIVATE_KEY = ""

_github_token_cache = {"token": "", "expires_at": 0.0}
_github_token_lock = threading.Lock()
_issue_cache: dict[str, tuple[float, dict]] = {}
_issue_cache_lock = threading.Lock()
_issue_submit_tracker_lock = threading.Lock()
_issue_submit_recent_by_client: dict[str, float] = {}
_issue_submit_recent_hash: dict[str, float] = {}

# ── route_type 하위 호환 매핑 ──────────────────────────────
_ROUTE_TYPE_COMPAT = {"1": "commute_in", "2": "commute_out", "5": "shuttle"}
_OPTIONS_CANDIDATE_LIMIT = 100

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


def _nearest_route_stop_for_user(
    route_stops: list[dict],
    user_lat: float,
    user_lng: float,
    *,
    exclude_first: bool = False,
    exclude_last: bool = False,
) -> dict | None:
    """노선 정류장 목록에서 사용자 좌표 기준 가장 가까운 정류장을 반환."""
    if not route_stops:
        return None
    start = 1 if exclude_first else 0
    end = len(route_stops) - (1 if exclude_last else 0)
    if end <= start:
        return None
    candidates = route_stops[start:end]
    best = min(
        candidates,
        key=lambda s: haversine_distance_m(user_lat, user_lng, s["lat"], s["lng"]),
    )
    return {
        "name": best["stop_name"],
        "lat": best["lat"],
        "lon": best["lng"],
    }


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


class GitHubConfigError(RuntimeError):
    pass


class GitHubAPIError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


def _ensure_github_issue_config() -> None:
    missing = []
    if not GITHUB_APP_ID:
        missing.append("GITHUB_APP_ID")
    if not GITHUB_INSTALLATION_ID:
        missing.append("GITHUB_INSTALLATION_ID")
    if not GITHUB_REPO_OWNER:
        missing.append("GITHUB_REPO_OWNER")
    if not GITHUB_REPO_NAME:
        missing.append("GITHUB_REPO_NAME")
    if not GITHUB_APP_PRIVATE_KEY:
        missing.append("GITHUB_APP_PRIVATE_KEY (or GITHUB_APP_PRIVATE_KEY_PATH)")
    if jwt is None:
        missing.append("PyJWT dependency")
    if missing:
        raise GitHubConfigError("GitHub 이슈 연동 설정이 누락되었습니다: " + ", ".join(missing))


def _build_github_app_jwt() -> str:
    _ensure_github_issue_config()
    now = int(_time.time())
    payload = {"iat": now - 60, "exp": now + (9 * 60), "iss": GITHUB_APP_ID}
    token = jwt.encode(payload, GITHUB_APP_PRIVATE_KEY, algorithm="RS256")  # type: ignore[union-attr]
    return token.decode("utf-8") if isinstance(token, bytes) else str(token)


def _parse_iso8601_timestamp(value: str | None) -> float:
    if not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _extract_github_error_message(resp: requests.Response) -> str:
    try:
        payload = resp.json()
    except ValueError:
        return resp.text.strip() or "GitHub API 요청에 실패했습니다."

    message = str(payload.get("message", "")).strip()
    errors = payload.get("errors")
    if errors:
        try:
            serialized = json.dumps(errors, ensure_ascii=False)
        except (TypeError, ValueError):
            serialized = str(errors)
        message = (message + " / " + serialized).strip(" /")
    return message or "GitHub API 요청에 실패했습니다."


def _github_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _get_github_installation_token(force_refresh: bool = False) -> str:
    _ensure_github_issue_config()
    now = _time.time()

    with _github_token_lock:
        cached_token = str(_github_token_cache.get("token", ""))
        cached_expires_at = float(_github_token_cache.get("expires_at", 0.0))
        if not force_refresh and cached_token and (cached_expires_at - now > 60):
            return cached_token

        app_jwt = _build_github_app_jwt()
        url = f"{GITHUB_API_BASE}/app/installations/{GITHUB_INSTALLATION_ID}/access_tokens"
        headers = {
            "Authorization": f"Bearer {app_jwt}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        try:
            resp = requests.post(url, headers=headers, timeout=10)
        except requests.RequestException as exc:
            raise GitHubAPIError(502, f"GitHub 설치 토큰 발급 실패: {exc}") from exc

        if resp.status_code >= 400:
            raise GitHubAPIError(resp.status_code, _extract_github_error_message(resp))

        try:
            payload = resp.json()
        except ValueError as exc:
            raise GitHubAPIError(502, "GitHub 설치 토큰 응답을 파싱할 수 없습니다.") from exc

        token = str(payload.get("token", "")).strip()
        expires_at = _parse_iso8601_timestamp(payload.get("expires_at"))
        if not token or expires_at <= 0:
            raise GitHubAPIError(502, "GitHub 설치 토큰 응답이 올바르지 않습니다.")

        _github_token_cache["token"] = token
        _github_token_cache["expires_at"] = expires_at
        return token


def _github_api_request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_payload: dict | None = None,
    force_refresh: bool = False,
):
    token = _get_github_installation_token(force_refresh=force_refresh)
    url = f"{GITHUB_API_BASE}{path}"
    try:
        resp = requests.request(
            method=method,
            url=url,
            headers=_github_headers(token),
            params=params,
            json=json_payload,
            timeout=15,
        )
    except requests.RequestException as exc:
        raise GitHubAPIError(502, f"GitHub API 호출 실패: {exc}") from exc

    if resp.status_code == 401 and not force_refresh:
        return _github_api_request(
            method,
            path,
            params=params,
            json_payload=json_payload,
            force_refresh=True,
        )

    if resp.status_code >= 400:
        raise GitHubAPIError(resp.status_code, _extract_github_error_message(resp))

    if resp.status_code == 204 or not resp.text:
        return {}
    try:
        return resp.json()
    except ValueError as exc:
        raise GitHubAPIError(502, "GitHub API 응답(JSON) 파싱에 실패했습니다.") from exc


def _normalize_label_names(labels: list | None) -> list[str]:
    result: list[str] = []
    for label in labels or []:
        if isinstance(label, dict):
            name = str(label.get("name", "")).strip()
        else:
            name = str(label).strip()
        if not name:
            continue
        clipped = name[:50]
        if clipped not in result:
            result.append(clipped)
    return result


def _normalize_issue(issue: dict) -> dict:
    user = issue.get("user") or {}
    return {
        "number": issue.get("number"),
        "title": issue.get("title") or "",
        "body": issue.get("body") or "",
        "state": issue.get("state") or "open",
        "labels": _normalize_label_names(issue.get("labels")),
        "url": issue.get("html_url") or "",
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "user": user.get("login") or "",
        "comments": issue.get("comments") or 0,
    }


def _normalize_comment(comment: dict) -> dict:
    user = comment.get("user") or {}
    return {
        "id": comment.get("id"),
        "body": comment.get("body") or "",
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "user": user.get("login") or "",
        "url": comment.get("html_url") or "",
    }


def _issue_cache_get(cache_key: str) -> dict | None:
    with _issue_cache_lock:
        cached = _issue_cache.get(cache_key)
        if not cached:
            return None
        expires_at, payload = cached
        if expires_at <= _time.time():
            _issue_cache.pop(cache_key, None)
            return None
        return payload


def _issue_cache_set(cache_key: str, payload: dict, ttl_sec: int | None = None) -> None:
    ttl = GITHUB_ISSUE_CACHE_TTL_SEC if ttl_sec is None else max(1, ttl_sec)
    with _issue_cache_lock:
        _issue_cache[cache_key] = (_time.time() + ttl, payload)


def _invalidate_issue_cache() -> None:
    with _issue_cache_lock:
        _issue_cache.clear()


def _compact_meta_label_part(raw: object) -> str:
    text = str(raw or "").strip().lower().replace(" ", "-")
    safe = "".join(ch for ch in text if ch.isalnum() or ch in "-_:")
    return safe[:24]


def _labels_from_meta(meta: dict | None) -> list[str]:
    labels: list[str] = []
    if not isinstance(meta, dict):
        return labels
    for key, value in list(meta.items())[:8]:
        key_part = _compact_meta_label_part(key)
        val_part = _compact_meta_label_part(value)
        if not key_part or not val_part:
            continue
        label = f"{key_part}:{val_part}"[:50]
        if label not in labels:
            labels.append(label)
    return labels


def _compose_issue_body(body: str, meta: dict | None) -> str:
    content = (body or "").strip()
    if not isinstance(meta, dict) or not meta:
        return content
    meta_json = json.dumps(meta, ensure_ascii=False, indent=2)
    meta_block = f"meta\n```json\n{meta_json}\n```"
    if not content:
        return meta_block
    return f"{content}\n\n---\n{meta_block}"


def _parse_issue_labels_input(raw_labels) -> list[str]:
    if not isinstance(raw_labels, list):
        return []
    cleaned: list[str] = []
    for item in raw_labels:
        label = str(item).strip()
        if not label:
            continue
        clipped = label[:50]
        if clipped not in cleaned:
            cleaned.append(clipped)
    return cleaned[:20]


def _get_issue_client_identity() -> tuple[str, str, bool]:
    cookie_id = request.cookies.get("issue_client_id", "").strip()
    is_new_cookie = False
    if not cookie_id:
        cookie_id = uuid.uuid4().hex
        is_new_cookie = True

    forwarded_ip = request.headers.get("CF-Connecting-IP", "").strip()
    if not forwarded_ip:
        forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
        forwarded_ip = forwarded_for.split(",")[0].strip() if forwarded_for else ""
    ip = forwarded_ip or request.remote_addr or "unknown"
    return f"{ip}:{cookie_id}", cookie_id, is_new_cookie


def _enforce_issue_submit_limits(client_key: str, payload_hash: str) -> tuple[bool, str]:
    now = _time.time()
    max_window = max(ISSUE_SUBMIT_RATE_LIMIT_SEC, ISSUE_SUBMIT_DEDUPE_WINDOW_SEC)

    with _issue_submit_tracker_lock:
        for key, ts in list(_issue_submit_recent_by_client.items()):
            if now - ts > max_window:
                _issue_submit_recent_by_client.pop(key, None)
        for key, ts in list(_issue_submit_recent_hash.items()):
            if now - ts > ISSUE_SUBMIT_DEDUPE_WINDOW_SEC:
                _issue_submit_recent_hash.pop(key, None)

        last_submit_ts = _issue_submit_recent_by_client.get(client_key)
        if last_submit_ts and (now - last_submit_ts) < ISSUE_SUBMIT_RATE_LIMIT_SEC:
            wait_sec = max(1, int(ISSUE_SUBMIT_RATE_LIMIT_SEC - (now - last_submit_ts)))
            return False, f"제보 등록은 {ISSUE_SUBMIT_RATE_LIMIT_SEC}초에 1회만 가능합니다. {wait_sec}초 후 다시 시도해 주세요."

        dup_ts = _issue_submit_recent_hash.get(payload_hash)
        if dup_ts and (now - dup_ts) < ISSUE_SUBMIT_DEDUPE_WINDOW_SEC:
            return False, "동일한 제목/내용의 제보가 최근에 등록되었습니다. 잠시 후 다시 시도해 주세요."

        _issue_submit_recent_by_client[client_key] = now
        _issue_submit_recent_hash[payload_hash] = now
        return True, ""


def _attach_issue_cookie_if_needed(resp, cookie_id: str, should_set_cookie: bool):
    if not should_set_cookie:
        return resp
    resp.set_cookie(
        "issue_client_id",
        cookie_id,
        max_age=60 * 60 * 24 * 365,
        httponly=True,
        samesite="Lax",
        secure=request.is_secure,
    )
    return resp


def _verify_github_webhook_signature(payload: bytes, received_signature: str | None) -> bool:
    if not GITHUB_WEBHOOK_SECRET:
        return False
    if not received_signature:
        return False
    expected = "sha256=" + hmac.new(
        GITHUB_WEBHOOK_SECRET.encode("utf-8"), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, received_signature)


# ── API 엔드포인트 ───────────────────────────────────────────
@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"documents": []})
    return jsonify({"documents": search_places(q)})


@app.route("/api/sites")
def api_sites():
    return jsonify({"sites": get_sites(), "db_updated_at": get_db_updated_at()})


@app.route("/api/issues", methods=["GET", "POST"])
def api_issues():
    if request.method == "POST":
        try:
            _ensure_github_issue_config()
        except GitHubConfigError as exc:
            return jsonify({"error": str(exc)}), 503

        payload = request.get_json(silent=True) or {}
        title = str(payload.get("title", "")).strip()
        body = str(payload.get("body", "")).strip()
        labels = _parse_issue_labels_input(payload.get("labels"))
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

        if not title:
            return jsonify({"error": "title은 필수입니다."}), 400
        if not body:
            return jsonify({"error": "body는 필수입니다."}), 400

        client_key, cookie_id, should_set_cookie = _get_issue_client_identity()
        payload_hash = hashlib.sha256(f"{title}\n{body}".encode("utf-8")).hexdigest()
        allowed, limit_message = _enforce_issue_submit_limits(client_key, payload_hash)
        if not allowed:
            status_code = 409 if "동일한 제목" in limit_message else 429
            response = make_response(jsonify({"error": limit_message}), status_code)
            return _attach_issue_cookie_if_needed(response, cookie_id, should_set_cookie)

        merged_labels = []
        for label in labels + _labels_from_meta(meta):
            if label not in merged_labels:
                merged_labels.append(label)

        issue_payload = {
            "title": title,
            "body": _compose_issue_body(body, meta),
        }
        if merged_labels:
            issue_payload["labels"] = merged_labels

        try:
            created = _github_api_request(
                "POST",
                f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues",
                json_payload=issue_payload,
            )
        except GitHubAPIError as exc:
            response = make_response(jsonify({"error": str(exc)}), exc.status_code)
            return _attach_issue_cookie_if_needed(response, cookie_id, should_set_cookie)

        _invalidate_issue_cache()
        normalized = _normalize_issue(created)
        response = make_response(
            jsonify(
                {
                    "issue": normalized,
                    "issueNumber": normalized.get("number"),
                    "url": normalized.get("url"),
                    "state": normalized.get("state"),
                    "createdAt": normalized.get("created_at"),
                }
            ),
            201,
        )
        return _attach_issue_cookie_if_needed(response, cookie_id, should_set_cookie)

    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    state = request.args.get("state", default="all", type=str).strip().lower()
    if state not in {"open", "closed", "all"}:
        return jsonify({"error": "state는 open|closed|all만 허용됩니다."}), 400

    labels_raw = request.args.get("labels", default="", type=str)
    labels = [s.strip() for s in labels_raw.split(",") if s.strip()]
    q = request.args.get("q", default="", type=str).strip()
    page = max(1, request.args.get("page", default=1, type=int) or 1)
    per_page = request.args.get("per_page", default=10, type=int) or 10
    per_page = max(1, min(per_page, 50))

    cache_key = f"issues:list:{state}:{','.join(labels)}:{q}:{page}:{per_page}"
    cached = _issue_cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    total_count = None
    has_next = False
    try:
        if q:
            query_parts = [f"repo:{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}", "is:issue"]
            if state in {"open", "closed"}:
                query_parts.append(f"is:{state}")
            for label in labels:
                query_parts.append(f'label:"{label}"')
            query_parts.append(q)
            search_payload = _github_api_request(
                "GET",
                "/search/issues",
                params={
                    "q": " ".join(query_parts),
                    "sort": "updated",
                    "order": "desc",
                    "page": page,
                    "per_page": per_page,
                },
            )
            raw_items = search_payload.get("items") or []
            total_count = int(search_payload.get("total_count", 0))
            has_next = (page * per_page) < min(total_count, 1000)
        else:
            list_params = {
                "state": state,
                "page": page,
                "per_page": per_page,
                "sort": "updated",
                "direction": "desc",
            }
            if labels:
                list_params["labels"] = ",".join(labels)
            raw_items = _github_api_request(
                "GET",
                f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues",
                params=list_params,
            )
            if not isinstance(raw_items, list):
                raw_items = []
            has_next = len(raw_items) >= per_page
    except GitHubAPIError as exc:
        return jsonify({"error": str(exc)}), exc.status_code

    issues = []
    for item in raw_items:
        if item.get("pull_request"):
            continue
        issues.append(_normalize_issue(item))

    response_payload = {
        "issues": issues,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "has_prev": page > 1,
            "has_next": has_next,
            "total_count": total_count,
        },
    }
    _issue_cache_set(cache_key, response_payload)
    return jsonify(response_payload)


@app.route("/api/issues/<int:issue_number>")
def api_issue_detail(issue_number: int):
    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    cache_key = f"issues:detail:{issue_number}"
    cached = _issue_cache_get(cache_key)
    if cached is not None:
        return jsonify(cached)

    try:
        issue = _github_api_request(
            "GET",
            f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues/{issue_number}",
        )
        comments = _github_api_request(
            "GET",
            f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues/{issue_number}/comments",
            params={"page": 1, "per_page": 100, "sort": "created", "direction": "asc"},
        )
    except GitHubAPIError as exc:
        return jsonify({"error": str(exc)}), exc.status_code

    if not isinstance(comments, list):
        comments = []

    response_payload = {
        "issue": _normalize_issue(issue),
        "comments": [_normalize_comment(c) for c in comments],
    }
    _issue_cache_set(cache_key, response_payload)
    return jsonify(response_payload)


@app.route("/api/issues/<int:issue_number>/comments", methods=["POST"])
def api_issue_comment_create(issue_number: int):
    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    payload = request.get_json(silent=True) or {}
    body = str(payload.get("body", "")).strip()
    if not body:
        return jsonify({"error": "댓글 내용(body)은 필수입니다."}), 400

    try:
        comment = _github_api_request(
            "POST",
            f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues/{issue_number}/comments",
            json_payload={"body": body},
        )
    except GitHubAPIError as exc:
        return jsonify({"error": str(exc)}), exc.status_code

    _invalidate_issue_cache()
    return jsonify({"comment": _normalize_comment(comment)}), 201


@app.route("/api/github/webhook", methods=["POST"])
def api_github_webhook():
    raw_payload = request.get_data(cache=True)
    signature = request.headers.get("X-Hub-Signature-256")
    if not _verify_github_webhook_signature(raw_payload, signature):
        return jsonify({"error": "유효하지 않은 webhook 서명입니다."}), 401

    event = request.headers.get("X-GitHub-Event", "")
    payload = request.get_json(silent=True) or {}
    action = payload.get("action")

    if event in {"issues", "issue_comment"}:
        if action in {
            "opened",
            "edited",
            "closed",
            "reopened",
            "labeled",
            "unlabeled",
            "created",
            "deleted",
        }:
            _invalidate_issue_cache()

    return jsonify({"ok": True})


@app.route("/api/shuttle/depart/options")
def api_shuttle_depart_options():
    """출근 노선 후보 최대 5개."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    exclude_raw = request.args.get("exclude_route_ids", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    exclude_ids = []
    if exclude_raw:
        exclude_ids = [int(x) for x in exclude_raw.split(",") if x.strip().isdigit()]

    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_in",
        lat=lat,
        lon=lng,
        day_type=day_type,
        # 후처리(탑승/하차 동일 정류장 제외, 시간 필터) 이후에도
        # 실제 반환 5개를 채울 수 있도록 후보를 넉넉히 가져온다.
        max_routes=_OPTIONS_CANDIDATE_LIMIT,
        exclude_route_ids=exclude_ids or None,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    all_last_times = []  # 시간 필터로 빈 결과 시 마지막 출발시간 추적용
    for r in results:
        ns = {
            "name": r["nearest_stop"]["name"],
            "lat": r["nearest_stop"]["lat"],
            "lon": r["nearest_stop"]["lon"],
        }
        route_stops = r["route_stops"]
        terminus = route_stops[-1] if route_stops else None

        # 탑승 정류장과 하차(종착) 정류장이 동일하면 같은 노선 내에서 대체 탑승 정류장 탐색
        if terminus and ns["name"] == terminus["stop_name"]:
            alt = _nearest_route_stop_for_user(route_stops, lat, lng, exclude_last=True)
            if not alt:
                continue
            ns = alt

        if time_param:
            all_last_times.extend(r["all_departure_times"])

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
            "distance_m": round(haversine_distance_m(lat, lng, ns["lat"], ns["lon"])),
            "all_departure_times": r["all_departure_times"],
            "route_stops": route_stops,
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)
        if len(options) >= 5:
            break

    resp = {"options": options}
    if not options and all_last_times:
        resp["last_departure_time"] = sorted(set(all_last_times))[-1]
    return jsonify(resp)


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
    ns = {
        "name": r["nearest_stop"]["name"],
        "lat": r["nearest_stop"]["lat"],
        "lon": r["nearest_stop"]["lon"],
    }
    route_stops = r["route_stops"]
    terminus = route_stops[-1] if route_stops else None
    if terminus and ns["name"] == terminus["stop_name"]:
        alt = _nearest_route_stop_for_user(route_stops, lat, lng, exclude_last=True)
        if alt:
            ns = alt

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
    exclude_raw = request.args.get("exclude_route_ids", "").strip()
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400

    exclude_ids = []
    if exclude_raw:
        exclude_ids = [int(x) for x in exclude_raw.split(",") if x.strip().isdigit()]

    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_out",
        lat=lat,
        lon=lng,
        day_type=day_type,
        # 후처리(탑승/하차 동일 정류장 제외, 시간 필터) 이후에도
        # 실제 반환 5개를 채울 수 있도록 후보를 넉넉히 가져온다.
        max_routes=_OPTIONS_CANDIDATE_LIMIT,
        exclude_route_ids=exclude_ids or None,
    )
    if not results:
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    all_last_times = []
    for r in results:
        ns = {
            "name": r["nearest_stop"]["name"],
            "lat": r["nearest_stop"]["lat"],
            "lon": r["nearest_stop"]["lon"],
        }
        route_stops = r["route_stops"]
        first = route_stops[0] if route_stops else None

        # 탑승(출발) 정류장과 하차 정류장이 동일하면 같은 노선 내에서 대체 하차 정류장 탐색
        if first and first["stop_name"] == ns["name"]:
            alt = _nearest_route_stop_for_user(route_stops, lat, lng, exclude_first=True)
            if not alt:
                continue
            ns = alt

        if time_param:
            all_last_times.extend(r["all_departure_times"])

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
        if len(options) >= 5:
            break

    resp = {"options": options}
    if not options and all_last_times:
        resp["last_departure_time"] = sorted(set(all_last_times))[-1]
    return jsonify(resp)


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
    ns = {
        "name": r["nearest_stop"]["name"],
        "lat": r["nearest_stop"]["lat"],
        "lon": r["nearest_stop"]["lon"],
    }
    route_stops = r["route_stops"]
    first = route_stops[0] if route_stops else None
    if first and first["stop_name"] == ns["name"]:
        alt = _nearest_route_stop_for_user(route_stops, lat, lng, exclude_first=True)
        if alt:
            ns = alt

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
