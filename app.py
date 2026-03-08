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
import queue
import random
import secrets
import threading
import time as _time
import uuid
import atexit
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests
from flask import Flask, jsonify, make_response, request, Response
from flask_cors import CORS
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    REGISTRY,
    generate_latest,
    multiprocess,
)

import load_data
import user_store
from load_data import (
    find_nearest_route_options,
    get_available_day_types,
    get_endpoint_options,
    get_endpoint_route_ids,
    get_db_updated_at,
    get_nearest_departure_time,
    get_report_candidate_options,
    get_report_nearby_stops,
    get_report_route_candidates_for_stop,
    get_report_tuple_meta,
    get_report_tuple_meta_by_uuid,
    find_recent_report_stop_index_by_route,
    get_route_detail,
    get_sites,
    init_db,
    search_endpoint_options,
    warm_endpoint_cache,
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
CORS(
    app,
    origins=[o.strip() for o in _cors_origins.split(",") if o.strip()],
    supports_credentials=True,
)

KAKAO_REST_API_KEY = os.environ.get("KAKAO_REST_API_KEY", "")
SEARCH_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"
SEARCH_SIZE = 15


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_rate(name: str, default: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return min(1.0, max(0.0, value))


GITHUB_API_BASE = os.environ.get("GITHUB_API_BASE", "https://api.github.com").rstrip("/")
GITHUB_APP_ID = os.environ.get("GITHUB_APP_ID", "").strip()
GITHUB_INSTALLATION_ID = os.environ.get("GITHUB_INSTALLATION_ID", "").strip()
GITHUB_REPO_OWNER = os.environ.get("GITHUB_REPO_OWNER", "").strip()
GITHUB_REPO_NAME = os.environ.get("GITHUB_REPO_NAME", "").strip()
GITHUB_WEBHOOK_SECRET = os.environ.get("GITHUB_WEBHOOK_SECRET", "")
GITHUB_ISSUE_CACHE_TTL_SEC = _env_int("GITHUB_ISSUE_CACHE_TTL_SEC", 300)
NOTICE_HEAD_CACHE_TTL_SEC = max(60, _env_int("NOTICE_HEAD_CACHE_TTL_SEC", 900))
ISSUE_SUBMIT_RATE_LIMIT_SEC = _env_int("ISSUE_SUBMIT_RATE_LIMIT_SEC", 10)
ISSUE_SUBMIT_DEDUPE_WINDOW_SEC = _env_int("ISSUE_SUBMIT_DEDUPE_WINDOW_SEC", 300)

AUTH_SESSION_COOKIE_NAME = os.environ.get("AUTH_SESSION_COOKIE_NAME", "sg_session").strip() or "sg_session"
AUTH_SESSION_TTL_DAYS = max(1, _env_int("SESSION_TTL_DAYS", 90))
AUTH_SESSION_SLIDING = _env_bool("SESSION_SLIDING", True)
AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC = max(0, _env_int("SESSION_TOUCH_MIN_INTERVAL_SEC", 0))
AUTH_COOKIE_DOMAIN = os.environ.get("SESSION_COOKIE_DOMAIN", "").strip() or None
HISTORY_VISITOR_COOKIE_NAME = (
    os.environ.get("HISTORY_VISITOR_COOKIE_NAME", "sg_history_visitor").strip() or "sg_history_visitor"
)
HISTORY_VISITOR_COOKIE_TTL_DAYS = max(1, _env_int("HISTORY_VISITOR_COOKIE_TTL_DAYS", 365))
AUTH_CLEANUP_INTERVAL_SEC = max(60, _env_int("AUTH_CLEANUP_INTERVAL_SEC", 3600))
PLACE_SEARCH_HISTORY_RETENTION_MONTHS = max(1, _env_int("PLACE_SEARCH_HISTORY_RETENTION_MONTHS", 1))
ROUTE_SEARCH_HISTORY_RETENTION_MONTHS = max(1, _env_int("ROUTE_SEARCH_HISTORY_RETENTION_MONTHS", 6))
BI_DB_SNAPSHOT_REFRESH_SEC = max(30, _env_int("BI_DB_SNAPSHOT_REFRESH_SEC", 300))
AUTH_GITHUB_CLEANUP_INTERVAL_SEC = max(30, _env_int("GITHUB_CLEANUP_INTERVAL_SEC", 60))
AUTH_GITHUB_CLEANUP_DEADLINE_HOURS = max(1, _env_int("GITHUB_CLEANUP_DEADLINE_HOURS", 24))
AUTH_NICKNAME_MIN_LEN = max(1, _env_int("AUTH_NICKNAME_MIN_LEN", 2))
AUTH_NICKNAME_MAX_LEN = max(AUTH_NICKNAME_MIN_LEN, _env_int("AUTH_NICKNAME_MAX_LEN", 20))
AUTH_PENDING_SIGNUP_TTL_SEC = max(60, _env_int("AUTH_PENDING_SIGNUP_TTL_SEC", 900))
AUTH_OAUTH_STATE_TTL_SEC = max(60, _env_int("AUTH_OAUTH_STATE_TTL_SEC", 600))
AUTH_METRICS_REFRESH_SEC = max(30, _env_int("AUTH_METRICS_REFRESH_SEC", 300))
APP_REFRESH_AUTH_METRICS_ON_STARTUP = _env_bool("APP_REFRESH_AUTH_METRICS_ON_STARTUP", True)
APP_WARM_ENDPOINT_CACHE_ON_STARTUP = _env_bool("APP_WARM_ENDPOINT_CACHE_ON_STARTUP", True)
APP_ENABLE_BACKGROUND_WORKERS = _env_bool("APP_ENABLE_BACKGROUND_WORKERS", True)
HISTORY_WRITE_MODE = str(os.environ.get("HISTORY_WRITE_MODE", "sync") or "sync").strip().lower()
if HISTORY_WRITE_MODE not in {"off", "sync", "async"}:
    HISTORY_WRITE_MODE = "sync"
HISTORY_WRITE_SAMPLE_RATE_PLACE_USER = _env_rate("HISTORY_WRITE_SAMPLE_RATE_PLACE_USER", 1.0)
HISTORY_WRITE_SAMPLE_RATE_PLACE_ANON = _env_rate("HISTORY_WRITE_SAMPLE_RATE_PLACE_ANON", 1.0)
HISTORY_WRITE_SAMPLE_RATE_ROUTE_USER = _env_rate("HISTORY_WRITE_SAMPLE_RATE_ROUTE_USER", 1.0)
HISTORY_WRITE_SAMPLE_RATE_ROUTE_ANON = _env_rate("HISTORY_WRITE_SAMPLE_RATE_ROUTE_ANON", 1.0)
HISTORY_WRITE_SAMPLING_ALGORITHM = str(
    os.environ.get("HISTORY_WRITE_SAMPLING_ALGORITHM", "deterministic_hash") or "deterministic_hash"
).strip().lower()
if HISTORY_WRITE_SAMPLING_ALGORITHM not in {"deterministic_hash", "random"}:
    HISTORY_WRITE_SAMPLING_ALGORITHM = "deterministic_hash"
HISTORY_WRITE_SAMPLING_BUCKET_SECONDS = max(1, _env_int("HISTORY_WRITE_SAMPLING_BUCKET_SECONDS", 300))
HISTORY_WRITE_SAMPLING_SALT = str(os.environ.get("HISTORY_WRITE_SAMPLING_SALT", "history-v1") or "history-v1")
HISTORY_WRITE_QUEUE_MAX_ITEMS = max(100, _env_int("HISTORY_WRITE_QUEUE_MAX_ITEMS", 10000))
HISTORY_WRITE_QUEUE_FULL_STRATEGY = str(
    os.environ.get("HISTORY_WRITE_QUEUE_FULL_STRATEGY", "drop") or "drop"
).strip().lower()
if HISTORY_WRITE_QUEUE_FULL_STRATEGY not in {"drop", "sync_fallback"}:
    HISTORY_WRITE_QUEUE_FULL_STRATEGY = "drop"
HISTORY_WRITE_BATCH_MAX_ITEMS = max(1, _env_int("HISTORY_WRITE_BATCH_MAX_ITEMS", 200))
HISTORY_WRITE_BATCH_FLUSH_INTERVAL_MS = max(10, _env_int("HISTORY_WRITE_BATCH_FLUSH_INTERVAL_MS", 250))
HISTORY_WRITE_BATCH_FLUSH_INTERVAL_SEC = HISTORY_WRITE_BATCH_FLUSH_INTERVAL_MS / 1000.0
HISTORY_WRITE_BATCH_MAX_RETRY = max(0, _env_int("HISTORY_WRITE_BATCH_MAX_RETRY", 2))
HISTORY_WRITE_BATCH_RETRY_BACKOFF_MS = max(10, _env_int("HISTORY_WRITE_BATCH_RETRY_BACKOFF_MS", 100))
HISTORY_WRITE_BATCH_RETRY_BACKOFF_SEC = HISTORY_WRITE_BATCH_RETRY_BACKOFF_MS / 1000.0
HISTORY_WRITE_SHUTDOWN_DRAIN_TIMEOUT_MS = max(100, _env_int("HISTORY_WRITE_SHUTDOWN_DRAIN_TIMEOUT_MS", 2000))
HISTORY_WRITE_SHUTDOWN_DRAIN_TIMEOUT_SEC = HISTORY_WRITE_SHUTDOWN_DRAIN_TIMEOUT_MS / 1000.0
AUTH_FRONTEND_CALLBACK_URL = os.environ.get(
    "AUTH_FRONTEND_CALLBACK_URL", "https://shuttle-go.com/auth-callback.html"
).strip()
AUTH_FRONTEND_ALLOWED_ORIGINS = [
    v.strip()
    for v in os.environ.get(
        "AUTH_FRONTEND_ALLOWED_ORIGINS",
        "https://shuttle-go.com,http://localhost:8080,http://127.0.0.1:8080",
    ).split(",")
    if v.strip()
]
BACKGROUND_TASK_LEASE_STALE_SEC = 300
BACKGROUND_TASK_LEASE_BUFFER_SEC = 30
AUTH_CLEANUP_LEASE_SEC = max(90, AUTH_CLEANUP_INTERVAL_SEC + BACKGROUND_TASK_LEASE_BUFFER_SEC)
BI_SNAPSHOT_LEASE_SEC = max(90, BI_DB_SNAPSHOT_REFRESH_SEC + BACKGROUND_TASK_LEASE_BUFFER_SEC)
GITHUB_CLEANUP_LEASE_SEC = max(90, AUTH_GITHUB_CLEANUP_INTERVAL_SEC + BACKGROUND_TASK_LEASE_BUFFER_SEC)
BACKGROUND_WORKER_OWNER_ID = f"{uuid.uuid4().hex}:{os.getpid()}"

GOOGLE_OAUTH_CLIENT_ID = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
GOOGLE_OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
GOOGLE_OAUTH_AUTHORIZE_URL = os.environ.get(
    "GOOGLE_OAUTH_AUTHORIZE_URL", "https://accounts.google.com/o/oauth2/v2/auth"
).strip()
GOOGLE_OAUTH_TOKEN_URL = os.environ.get(
    "GOOGLE_OAUTH_TOKEN_URL", "https://oauth2.googleapis.com/token"
).strip()
GOOGLE_OAUTH_USERINFO_URL = os.environ.get(
    "GOOGLE_OAUTH_USERINFO_URL", "https://openidconnect.googleapis.com/v1/userinfo"
).strip()
GOOGLE_OAUTH_REDIRECT_URI = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "").strip()

AUTH_RESERVED_NICKNAMES = {"shuttlego"}
if GITHUB_REPO_OWNER.strip():
    AUTH_RESERVED_NICKNAMES.add(GITHUB_REPO_OWNER.strip().casefold())

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
_issue_cache_version_local = 0
_issue_cache_version_lock = threading.Lock()
_notice_head_revalidate_inflight: set[str] = set()
_notice_head_revalidate_lock = threading.Lock()
_issue_submit_tracker_lock = threading.Lock()
_issue_submit_recent_by_client: dict[str, float] = {}
_issue_submit_recent_hash: dict[str, float] = {}
_auth_background_started = False
_auth_background_lock = threading.Lock()
_history_write_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=HISTORY_WRITE_QUEUE_MAX_ITEMS)
_history_writer_started = False
_history_writer_thread: threading.Thread | None = None
_history_writer_lock = threading.Lock()
_history_writer_stop_event = threading.Event()
_history_writer_shutdown_registered = False

# ── route_type 하위 호환 매핑 ──────────────────────────────
_ROUTE_TYPE_COMPAT = {"1": "commute_in", "2": "commute_out", "5": "shuttle"}
_OPTIONS_CANDIDATE_LIMIT = 100
_OPTIONS_RESPONSE_LIMIT = 3
_DEFAULT_MAX_DISTANCE_KM = 3.0
_MAX_DISTANCE_KM = 30.0
_DEFAULT_ROUTE_SEARCH_MODE = "time"
_DEFAULT_SITE_ID = "0000011"
_REPORT_PRIMARY_RADIUS_M = max(50.0, _env_float("ARRIVAL_REPORT_PRIMARY_RADIUS_M", 150.0))
_REPORT_FALLBACK_RADIUS_M = max(
    _REPORT_PRIMARY_RADIUS_M,
    _env_float("ARRIVAL_REPORT_FALLBACK_RADIUS_M", 250.0),
)
_REPORT_TIME_SKEW_SEC = max(60, _env_int("ARRIVAL_REPORT_TIME_SKEW_SEC", 300))
_REPORT_COOLDOWN_SEC = max(1, _env_int("ARRIVAL_REPORT_COOLDOWN_SEC", 180))
_REPORT_ALLOWED_PAST_SEC = max(60, _env_int("ARRIVAL_REPORT_ALLOWED_PAST_SEC", 600))
_REPORT_ALLOWED_FUTURE_SEC = max(0, _env_int("ARRIVAL_REPORT_ALLOWED_FUTURE_SEC", 180))
_REPORT_EDIT_WINDOW_SEC = max(60, _env_int("ARRIVAL_REPORT_EDIT_WINDOW_SEC", 180))
_KST = timezone(timedelta(hours=9))
_SITE_ID_DEFAULT_ENDPOINTS = {
    "/api/shuttle/day-types",
    "/api/shuttle/endpoint-options",
    "/api/shuttle/endpoint-options/search",
    "/api/shuttle/depart/options",
    "/api/shuttle/depart",
    "/api/shuttle/arrive/options",
    "/api/shuttle/arrive",
}
_SITE_ID_METRIC_EXTRA_ENDPOINTS = {"/api/me/preferences/endpoint"}
_ROUTE_SEARCH_SOURCE_VALUES = {"search", "map", "unknown"}
_PROMETHEUS_MULTIPROC_DIR_ENV = "PROMETHEUS_MULTIPROC_DIR"
_HISTORY_EVENT_PLACE_USER = "place_user"
_HISTORY_EVENT_PLACE_ANON = "place_anon"
_HISTORY_EVENT_ROUTE_USER = "route_user"
_HISTORY_EVENT_ROUTE_ANON = "route_anon"
_HISTORY_EVENT_TYPES = {
    _HISTORY_EVENT_PLACE_USER,
    _HISTORY_EVENT_PLACE_ANON,
    _HISTORY_EVENT_ROUTE_USER,
    _HISTORY_EVENT_ROUTE_ANON,
}

OTP_BASE_URL = os.environ.get("OTP_BASE_URL", "http://otp:8082").rstrip("/")
OTP_PLAN_PATH = os.environ.get("OTP_PLAN_PATH", "/otp/gtfs/v1").strip() or "/otp/gtfs/v1"
if not OTP_PLAN_PATH.startswith("/"):
    OTP_PLAN_PATH = "/" + OTP_PLAN_PATH
OTP_WALK_ENABLED = os.environ.get("OTP_WALK_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
OTP_WALK_HTTP_TIMEOUT_SEC = max(0.2, _env_float("OTP_WALK_HTTP_TIMEOUT_SEC", 8.0))
OTP_WALK_DURATION_MULTIPLIER = max(0.0, _env_float("OTP_WALK_DURATION_MULTIPLIER", 1.25))
OTP_WALK_CACHE_SIZE = max(100, _env_int("OTP_WALK_CACHE_SIZE", 10000))
_walk_path_cache: OrderedDict[str, dict] = OrderedDict()
_walk_path_cache_lock = threading.Lock()

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

REQUEST_SIZE_TOTAL = Counter(
    "shuttle_http_request_size_bytes_total",
    "Total HTTP request size in bytes",
    ["method", "endpoint"],
)

RESPONSE_SIZE_TOTAL = Counter(
    "shuttle_http_response_size_bytes_total",
    "Total HTTP response size in bytes",
    ["method", "endpoint", "status"],
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
    multiprocess_mode="livesum",
)

APP_INFO = Gauge(
    "shuttle_app_info",
    "Application metadata",
    ["version"],
    multiprocess_mode="livemax",
)
APP_INFO.labels(version=os.environ.get("APP_VERSION", "1.0.0")).set(1)

AUTH_CUMULATIVE_USERS = Gauge(
    "shuttle_auth_cumulative_users",
    "Cumulative number of users who have signed up",
    multiprocess_mode="mostrecent",
)

AUTH_CUMULATIVE_DELETED_USERS = Gauge(
    "shuttle_auth_cumulative_deleted_users",
    "Cumulative number of users who have deleted their accounts",
    multiprocess_mode="mostrecent",
)

AUTH_DAILY_NEW_USERS = Gauge(
    "shuttle_auth_daily_new_users",
    "Number of users who signed up within the last 24 hours",
    multiprocess_mode="mostrecent",
)

AUTH_USER_DB_SIZE_BYTES = Gauge(
    "shuttle_auth_user_db_size_bytes",
    "Total size of user SQLite files (db + wal) in bytes",
    multiprocess_mode="mostrecent",
)

AUTH_DAU = Gauge(
    "shuttle_auth_dau",
    "Daily active users (distinct active users who logged in within the last 24 hours)",
    multiprocess_mode="mostrecent",
)

AUTH_MAU = Gauge(
    "shuttle_auth_mau",
    "Monthly active users (distinct active users who logged in within the last 30 days)",
    multiprocess_mode="mostrecent",
)

API_REQUEST_AUTH_STATE_COUNT = Counter(
    "shuttle_api_requests_by_auth_total",
    "Total API requests grouped by authentication state",
    ["auth_state"],
)

API_REQUEST_SITE_COUNT = Counter(
    "shuttle_api_requests_by_site_total",
    "Total API requests grouped by site and endpoint",
    ["site_id", "endpoint"],
)

ROUTE_SEARCH_SOURCE_COUNT = Counter(
    "shuttle_route_search_by_source_total",
    "Route search requests grouped by direction and place source",
    ["direction", "source"],
)

HISTORY_WRITE_ENQUEUE_TOTAL = Counter(
    "shuttle_history_write_enqueue_total",
    "History write enqueue attempts grouped by result",
    ["event_type", "result"],
)

HISTORY_WRITE_DROPPED_TOTAL = Counter(
    "shuttle_history_write_dropped_total",
    "History write events dropped before commit",
    ["event_type", "reason"],
)

HISTORY_WRITE_FLUSH_TOTAL = Counter(
    "shuttle_history_write_flush_total",
    "History write flush attempts grouped by result",
    ["result"],
)

HISTORY_WRITE_FLUSH_BATCH_SIZE = Histogram(
    "shuttle_history_write_flush_batch_size",
    "History write flush batch size",
    buckets=[1, 2, 5, 10, 20, 50, 100, 200, 500, 1000],
)

HISTORY_WRITE_FLUSH_DURATION = Histogram(
    "shuttle_history_write_flush_duration_seconds",
    "History write flush duration in seconds",
    buckets=[0.001, 0.003, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

HISTORY_WRITE_QUEUE_SIZE = Gauge(
    "shuttle_history_write_queue_size",
    "Current queued history write events",
    multiprocess_mode="livesum",
)


# ── 요청 전/후 훅 (메트릭 수집) ──────────────────────────────
@app.before_request
def _before_request():
    request._prom_start_time = _time.time()
    request._prom_track_in_progress = request.path != "/metrics"
    if request._prom_track_in_progress:
        IN_PROGRESS_REQUESTS.inc()


def _safe_request_size_bytes() -> int:
    url_size = len((request.path or "").encode("utf-8"))
    if request.query_string:
        url_size += len(request.query_string)
    content_length = request.content_length
    if isinstance(content_length, int) and content_length >= 0:
        return content_length + url_size
    try:
        body = request.get_data(cache=True, as_text=False)
    except Exception:
        return url_size
    return (len(body) if body else 0) + url_size


def _safe_response_size_bytes(response) -> int:
    try:
        content_length = response.calculate_content_length()
    except Exception:
        content_length = None
    if isinstance(content_length, int) and content_length >= 0:
        return content_length
    try:
        body = response.get_data()
    except Exception:
        return 0
    return len(body) if body else 0


def _current_auth_state_for_metrics() -> str:
    user, _ = _get_current_user(with_touch=False)
    return "authenticated" if user is not None else "anonymous"


def _is_business_api_endpoint_for_auth_metrics(endpoint: str) -> bool:
    if endpoint in {"/api/search", "/api/sites", "/api/issues"}:
        return True
    return (
        endpoint.startswith("/api/shuttle/")
        or endpoint.startswith("/api/issues/")
        or endpoint.startswith("/api/route/")
    )


def _extract_site_id_for_metrics(endpoint: str) -> str | None:
    if not (endpoint.startswith("/api/shuttle/") or endpoint in _SITE_ID_METRIC_EXTRA_ENDPOINTS):
        return None
    site_id = str(request.args.get("site_id") or "").strip()
    if not site_id and request.method in {"POST", "PUT", "PATCH"}:
        payload = request.get_json(silent=True)
        if isinstance(payload, dict):
            site_id = str(payload.get("site_id") or "").strip()
    if not site_id and endpoint in _SITE_ID_DEFAULT_ENDPOINTS:
        site_id = _DEFAULT_SITE_ID
    return site_id or None


def _user_db_size_bytes() -> int:
    if not user_store.is_sqlite_backend():
        return 0
    db_path = str(user_store.get_db_path())
    total = 0
    # -shm은 SQLite의 공유 메모리 보조 파일이라 저장 데이터 크기 지표로는 노이즈가 크다.
    for suffix in ("", "-wal"):
        try:
            total += os.path.getsize(db_path + suffix)
        except OSError:
            continue
    return total


def _refresh_auth_business_metrics() -> None:
    snapshot = user_store.get_user_metrics_snapshot(daily_window_hours=24)
    AUTH_CUMULATIVE_USERS.set(float(snapshot.get("cumulative_users") or 0))
    AUTH_CUMULATIVE_DELETED_USERS.set(float(snapshot.get("cumulative_deleted_users") or 0))
    AUTH_DAILY_NEW_USERS.set(float(snapshot.get("daily_new_users") or 0))
    AUTH_DAU.set(float(snapshot.get("dau") or 0))
    AUTH_MAU.set(float(snapshot.get("mau") or 0))
    AUTH_USER_DB_SIZE_BYTES.set(float(_user_db_size_bytes()))


def _route_search_source_from_request() -> str:
    raw_source = str(
        request.args.get("place_source")
        or request.args.get("search_source")
        or ""
    ).strip().lower()
    if raw_source in {"search", "map"}:
        return raw_source

    place_name = str(request.args.get("place_name") or "").strip()
    if place_name == "선택한 위치":
        return "map"
    if place_name and place_name != "선택한 장소":
        return "search"
    return "unknown"


def _record_route_search_source_metric(direction: str) -> None:
    safe_direction = str(direction or "").strip().lower()
    if safe_direction not in {"depart", "arrive"}:
        return
    source = _route_search_source_from_request()
    if source not in _ROUTE_SEARCH_SOURCE_VALUES:
        source = "unknown"
    ROUTE_SEARCH_SOURCE_COUNT.labels(direction=safe_direction, source=source).inc()


def _history_sample_rate(event_type: str) -> float:
    if event_type == _HISTORY_EVENT_PLACE_USER:
        return HISTORY_WRITE_SAMPLE_RATE_PLACE_USER
    if event_type == _HISTORY_EVENT_PLACE_ANON:
        return HISTORY_WRITE_SAMPLE_RATE_PLACE_ANON
    if event_type == _HISTORY_EVENT_ROUTE_USER:
        return HISTORY_WRITE_SAMPLE_RATE_ROUTE_USER
    if event_type == _HISTORY_EVENT_ROUTE_ANON:
        return HISTORY_WRITE_SAMPLE_RATE_ROUTE_ANON
    return 1.0


def _history_mark_dropped(event_type: str, reason: str) -> None:
    safe_event_type = event_type if event_type in _HISTORY_EVENT_TYPES else "unknown"
    HISTORY_WRITE_DROPPED_TOTAL.labels(event_type=safe_event_type, reason=str(reason or "unknown")).inc()


def _history_should_sample(event_type: str, sample_key: str) -> bool:
    rate = _history_sample_rate(event_type)
    if rate <= 0.0:
        return False
    if rate >= 1.0:
        return True
    if HISTORY_WRITE_SAMPLING_ALGORITHM == "random":
        return random.random() < rate
    bucket = int(_time.time() // HISTORY_WRITE_SAMPLING_BUCKET_SECONDS)
    basis = f"{HISTORY_WRITE_SAMPLING_SALT}|{event_type}|{bucket}|{sample_key}"
    digest = hashlib.sha256(basis.encode("utf-8")).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    threshold = int(rate * ((1 << 64) - 1))
    return value <= threshold


def _normalize_selected_endpoints_for_history(selected_endpoints: list[str] | None) -> list[str]:
    clean: list[str] = []
    if isinstance(selected_endpoints, list):
        for item in selected_endpoints:
            value = str(item or "").strip()
            if value and value not in clean:
                clean.append(value)
    return clean


def _persist_history_events(events: list[dict[str, Any]]) -> int:
    if not events:
        return 0
    place_user_rows: list[tuple[int, str, str | None]] = []
    place_anon_rows: list[tuple[str, str, str | None]] = []
    route_user_rows: list[tuple[int, str, str, str, float, float, list[str] | None, str | None, str | None]] = []
    route_anon_rows: list[tuple[str, str, str, str, float, float, list[str] | None, str | None, str | None]] = []
    for event in events:
        event_type = str(event.get("event_type") or "").strip().lower()
        searched_at = str(event.get("searched_at") or "").strip() or user_store.utc_iso()
        try:
            if event_type == _HISTORY_EVENT_PLACE_USER:
                place_user_rows.append(
                    (
                        int(event.get("user_id")),
                        str(event.get("keyword") or ""),
                        searched_at,
                    )
                )
            elif event_type == _HISTORY_EVENT_PLACE_ANON:
                place_anon_rows.append(
                    (
                        str(event.get("visitor_id") or ""),
                        str(event.get("keyword") or ""),
                        searched_at,
                    )
                )
            elif event_type == _HISTORY_EVENT_ROUTE_USER:
                route_user_rows.append(
                    (
                        int(event.get("user_id")),
                        str(event.get("site_id") or ""),
                        str(event.get("day_type") or ""),
                        str(event.get("direction") or ""),
                        float(event.get("lat")),
                        float(event.get("lng")),
                        _normalize_selected_endpoints_for_history(event.get("selected_endpoints")),
                        (str(event.get("place_name")).strip()[:120] if event.get("place_name") else None),
                        searched_at,
                    )
                )
            elif event_type == _HISTORY_EVENT_ROUTE_ANON:
                route_anon_rows.append(
                    (
                        str(event.get("visitor_id") or ""),
                        str(event.get("site_id") or ""),
                        str(event.get("day_type") or ""),
                        str(event.get("direction") or ""),
                        float(event.get("lat")),
                        float(event.get("lng")),
                        _normalize_selected_endpoints_for_history(event.get("selected_endpoints")),
                        (str(event.get("place_name")).strip()[:120] if event.get("place_name") else None),
                        searched_at,
                    )
                )
        except (TypeError, ValueError):
            _history_mark_dropped(event_type, "invalid_event")
    persisted = 0
    if place_user_rows:
        persisted += user_store.bulk_add_place_history(place_user_rows)
    if place_anon_rows:
        persisted += user_store.bulk_add_anonymous_place_history(place_anon_rows)
    if route_user_rows:
        persisted += user_store.bulk_add_route_history(route_user_rows)
    if route_anon_rows:
        persisted += user_store.bulk_add_anonymous_route_history(route_anon_rows)
    return persisted


def _flush_history_events(events: list[dict[str, Any]], *, source: str) -> bool:
    if not events:
        return True
    start = _time.perf_counter()
    HISTORY_WRITE_FLUSH_BATCH_SIZE.observe(float(len(events)))
    try:
        for attempt in range(HISTORY_WRITE_BATCH_MAX_RETRY + 1):
            try:
                _persist_history_events(events)
                HISTORY_WRITE_FLUSH_TOTAL.labels(result="success").inc()
                return True
            except Exception:
                if attempt >= HISTORY_WRITE_BATCH_MAX_RETRY:
                    HISTORY_WRITE_FLUSH_TOTAL.labels(result="error").inc()
                    app.logger.exception(
                        "History write flush failed source=%s attempts=%s size=%s",
                        source,
                        attempt + 1,
                        len(events),
                    )
                    for event in events:
                        _history_mark_dropped(str(event.get("event_type") or ""), "flush_error")
                    return False
                _time.sleep(HISTORY_WRITE_BATCH_RETRY_BACKOFF_SEC * (attempt + 1))
        return False
    finally:
        HISTORY_WRITE_FLUSH_DURATION.observe(max(0.0, _time.perf_counter() - start))


def _history_writer_loop() -> None:
    pending: list[dict[str, Any]] = []
    while True:
        if _history_writer_stop_event.is_set() and not pending and _history_write_queue.empty():
            break
        timeout = HISTORY_WRITE_BATCH_FLUSH_INTERVAL_SEC
        if _history_writer_stop_event.is_set():
            timeout = min(0.05, HISTORY_WRITE_BATCH_FLUSH_INTERVAL_SEC)
        try:
            item = _history_write_queue.get(timeout=timeout)
            HISTORY_WRITE_QUEUE_SIZE.dec()
            pending.append(item)
            if len(pending) >= HISTORY_WRITE_BATCH_MAX_ITEMS:
                _flush_history_events(pending, source="async")
                pending = []
        except queue.Empty:
            if pending:
                _flush_history_events(pending, source="async")
                pending = []
        except Exception:
            app.logger.exception("History write loop crashed")
            _time.sleep(0.05)


def _shutdown_history_writer() -> None:
    global _history_writer_started, _history_writer_thread
    if HISTORY_WRITE_MODE != "async":
        return
    with _history_writer_lock:
        thread = _history_writer_thread
        if thread is None or not _history_writer_started:
            return
        _history_writer_stop_event.set()
    thread_alive = thread.is_alive()
    if thread_alive:
        thread.join(timeout=HISTORY_WRITE_SHUTDOWN_DRAIN_TIMEOUT_SEC)
    thread_alive = thread.is_alive()
    if thread_alive:
        app.logger.warning(
            "History writer shutdown timeout queue_size=%s",
            _history_write_queue.qsize(),
        )
    with _history_writer_lock:
        if thread_alive:
            _history_writer_started = True
            _history_writer_thread = thread
        else:
            _history_writer_started = False
            _history_writer_thread = None
            _history_writer_stop_event.clear()


def _start_history_writer() -> None:
    global _history_writer_started, _history_writer_thread, _history_writer_shutdown_registered
    if HISTORY_WRITE_MODE != "async":
        return
    with _history_writer_lock:
        if _history_writer_started:
            return
        _history_writer_stop_event.clear()
        worker = threading.Thread(
            target=_history_writer_loop,
            daemon=True,
            name="history-write-loop",
        )
        try:
            worker.start()
        except Exception:
            app.logger.exception("Failed to start history writer thread")
            return
        _history_writer_thread = worker
        _history_writer_started = True
        if not _history_writer_shutdown_registered:
            atexit.register(_shutdown_history_writer)
            _history_writer_shutdown_registered = True


def _enqueue_history_event(event_type: str, event: dict[str, Any]) -> None:
    safe_event_type = event_type if event_type in _HISTORY_EVENT_TYPES else "unknown"
    if HISTORY_WRITE_MODE == "off":
        HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=safe_event_type, result="disabled").inc()
        _history_mark_dropped(safe_event_type, "disabled")
        return
    if HISTORY_WRITE_MODE == "sync":
        HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=safe_event_type, result="sync").inc()
        _flush_history_events([event], source="sync")
        return
    _start_history_writer()
    try:
        _history_write_queue.put_nowait(event)
        HISTORY_WRITE_QUEUE_SIZE.inc()
        HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=safe_event_type, result="queued").inc()
        return
    except queue.Full:
        if HISTORY_WRITE_QUEUE_FULL_STRATEGY != "sync_fallback":
            HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=safe_event_type, result="queue_full").inc()
            _history_mark_dropped(safe_event_type, "queue_full")
            return
    HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=safe_event_type, result="sync_fallback").inc()
    _flush_history_events([event], source="sync_fallback")


def _record_place_search_history(keyword: str, user: dict | None) -> None:
    clean_keyword = str(keyword or "").strip()
    if not clean_keyword:
        return
    try:
        searched_at = user_store.utc_iso()
        if user is not None:
            user_id = int(user["id"])
            event_type = _HISTORY_EVENT_PLACE_USER
            sample_key = f"user:{user_id}|{clean_keyword.casefold()[:120]}"
            if not _history_should_sample(event_type, sample_key):
                HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=event_type, result="sampled_out").inc()
                _history_mark_dropped(event_type, "sampled_out")
                return
            _enqueue_history_event(
                event_type,
                {
                    "event_type": event_type,
                    "user_id": user_id,
                    "keyword": clean_keyword,
                    "searched_at": searched_at,
                },
            )
            return
        visitor_id = _ensure_history_visitor_id()
        event_type = _HISTORY_EVENT_PLACE_ANON
        sample_key = f"visitor:{visitor_id}|{clean_keyword.casefold()[:120]}"
        if not _history_should_sample(event_type, sample_key):
            HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=event_type, result="sampled_out").inc()
            _history_mark_dropped(event_type, "sampled_out")
            return
        _enqueue_history_event(
            event_type,
            {
                "event_type": event_type,
                "visitor_id": visitor_id,
                "keyword": clean_keyword,
                "searched_at": searched_at,
            },
        )
    except Exception:
        app.logger.exception("Failed to save place search history")


def _record_route_search_history(
    *,
    auth_user: dict | None,
    site_id: str,
    day_type: str,
    direction: str,
    lat: float,
    lng: float,
    selected_endpoints: list[str] | None,
    place_name: str | None,
) -> None:
    safe_direction = str(direction or "").strip().lower()
    if safe_direction not in {"depart", "arrive"}:
        return
    try:
        clean_site_id = str(site_id or "").strip()
        clean_day_type = str(day_type or "").strip()
        clean_place_name = str(place_name or "").strip()[:120] or None
        clean_selected_endpoints = _normalize_selected_endpoints_for_history(selected_endpoints)
        searched_at = user_store.utc_iso()
        lat_value = float(lat)
        lng_value = float(lng)
        selected_key = ",".join(clean_selected_endpoints)
        if auth_user is not None:
            user_id = int(auth_user["id"])
            event_type = _HISTORY_EVENT_ROUTE_USER
            sample_key = (
                f"user:{user_id}|{clean_site_id}|{clean_day_type}|{safe_direction}|"
                f"{lat_value:.5f}|{lng_value:.5f}|{selected_key}|{clean_place_name or ''}"
            )
            if not _history_should_sample(event_type, sample_key):
                HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=event_type, result="sampled_out").inc()
                _history_mark_dropped(event_type, "sampled_out")
                return
            _enqueue_history_event(
                event_type,
                {
                    "event_type": event_type,
                    "user_id": user_id,
                    "site_id": clean_site_id,
                    "day_type": clean_day_type,
                    "direction": safe_direction,
                    "lat": lat_value,
                    "lng": lng_value,
                    "selected_endpoints": clean_selected_endpoints,
                    "place_name": clean_place_name,
                    "searched_at": searched_at,
                },
            )
            return
        visitor_id = _ensure_history_visitor_id()
        event_type = _HISTORY_EVENT_ROUTE_ANON
        sample_key = (
            f"visitor:{visitor_id}|{clean_site_id}|{clean_day_type}|{safe_direction}|"
            f"{lat_value:.5f}|{lng_value:.5f}|{selected_key}|{clean_place_name or ''}"
        )
        if not _history_should_sample(event_type, sample_key):
            HISTORY_WRITE_ENQUEUE_TOTAL.labels(event_type=event_type, result="sampled_out").inc()
            _history_mark_dropped(event_type, "sampled_out")
            return
        _enqueue_history_event(
            event_type,
            {
                "event_type": event_type,
                "visitor_id": visitor_id,
                "site_id": clean_site_id,
                "day_type": clean_day_type,
                "direction": safe_direction,
                "lat": lat_value,
                "lng": lng_value,
                "selected_endpoints": clean_selected_endpoints,
                "place_name": clean_place_name,
                "searched_at": searched_at,
            },
        )
    except Exception:
        app.logger.exception("Failed to save %s route history", safe_direction)


def _metrics_registry():
    multiproc_dir = os.environ.get(_PROMETHEUS_MULTIPROC_DIR_ENV, "").strip()
    if not multiproc_dir:
        return REGISTRY
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return registry


@app.after_request
def _after_request(response):
    if request.path == "/metrics":
        return response
    latency = _time.time() - getattr(request, "_prom_start_time", _time.time())
    url_rule = getattr(request, "url_rule", None)
    if url_rule is not None and getattr(url_rule, "rule", None):
        endpoint = url_rule.rule
    elif request.path.startswith("/api/"):
        endpoint = "/api/_unmatched"
    else:
        endpoint = request.path
    method = request.method
    status = str(response.status_code)
    request_size = _safe_request_size_bytes()
    response_size = _safe_response_size_bytes(response)
    REQUEST_COUNT.labels(method=method, endpoint=endpoint, status=status).inc()
    REQUEST_LATENCY.labels(method=method, endpoint=endpoint).observe(latency)
    REQUEST_SIZE_TOTAL.labels(method=method, endpoint=endpoint).inc(request_size)
    RESPONSE_SIZE_TOTAL.labels(method=method, endpoint=endpoint, status=status).inc(response_size)
    # 비즈니스 메트릭은 Flask가 실제로 매칭한 API 라우트만 집계한다.
    # 이렇게 해야 /api/.env 같은 스캔 요청이 /api/_unmatched로 집계되지 않는다.
    matched_api_endpoint = url_rule is not None and endpoint.startswith("/api/")
    if matched_api_endpoint and _is_business_api_endpoint_for_auth_metrics(endpoint):
        auth_state = _current_auth_state_for_metrics()
        API_REQUEST_AUTH_STATE_COUNT.labels(auth_state=auth_state).inc()
    if matched_api_endpoint:
        site_id = _extract_site_id_for_metrics(endpoint)
        if site_id:
            API_REQUEST_SITE_COUNT.labels(site_id=site_id, endpoint=endpoint).inc()
    if getattr(request, "_prom_track_in_progress", False):
        IN_PROGRESS_REQUESTS.dec()
    return _attach_history_visitor_cookie_if_needed(response)


# ── Prometheus / Health 엔드포인트 ───────────────────────────
@app.route("/metrics")
def metrics():
    """Prometheus 메트릭 노출."""
    return Response(generate_latest(_metrics_registry()), mimetype=CONTENT_TYPE_LATEST)


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
    exclude_stop_ids: set[int] | None = None,
    min_sequence: int | None = None,
    max_sequence: int | None = None,
) -> dict | None:
    """노선 정류장 목록에서 사용자 좌표 기준 가장 가까운 정류장을 반환."""
    if not route_stops:
        return None
    start = 1 if exclude_first else 0
    end = len(route_stops) - (1 if exclude_last else 0)
    if end <= start:
        return None
    candidates = route_stops[start:end]
    if exclude_stop_ids:
        filtered = []
        for stop in candidates:
            sid = stop.get("stop_id")
            try:
                if sid is not None and int(sid) in exclude_stop_ids:
                    continue
            except (TypeError, ValueError):
                pass
            filtered.append(stop)
        candidates = filtered
    if min_sequence is not None or max_sequence is not None:
        filtered = []
        for stop in candidates:
            seq_raw = stop.get("sequence")
            try:
                seq = int(seq_raw)
            except (TypeError, ValueError):
                continue
            if min_sequence is not None and seq < min_sequence:
                continue
            if max_sequence is not None and seq > max_sequence:
                continue
            filtered.append(stop)
        candidates = filtered
    if not candidates:
        return None
    best = min(
        candidates,
        key=lambda s: haversine_distance_m(user_lat, user_lng, s["lat"], s["lng"]),
    )
    return {
        "name": best["stop_name"],
        "lat": best["lat"],
        "lon": best["lng"],
        "stop_id": best.get("stop_id"),
        "sequence": best.get("sequence"),
    }


def _find_route_stop_sequence(
    route_stops: list[dict],
    *,
    stop_id: int | None = None,
    stop_name: str | None = None,
) -> int | None:
    """route_stops에서 stop_id/stop_name으로 정류장 순번을 찾는다."""
    if stop_id is not None:
        for stop in route_stops:
            try:
                if int(stop.get("stop_id")) == int(stop_id):
                    seq = stop.get("sequence")
                    return int(seq) if seq is not None else None
            except (TypeError, ValueError):
                continue
    if stop_name:
        target = str(stop_name).strip()
        for stop in route_stops:
            if str(stop.get("stop_name", "")).strip() == target:
                seq = stop.get("sequence")
                try:
                    return int(seq) if seq is not None else None
                except (TypeError, ValueError):
                    return None
    return None


def _segment_polylines_between_sequences(
    route_stops: list[dict],
    *,
    from_sequence: int | None,
    to_sequence: int | None,
) -> list[str]:
    if from_sequence is None or to_sequence is None:
        return []
    try:
        start_seq = int(from_sequence)
        end_seq = int(to_sequence)
    except (TypeError, ValueError):
        return []
    if end_seq <= start_seq:
        return []

    stop_by_seq: dict[int, dict] = {}
    for stop in route_stops:
        seq_raw = stop.get("sequence")
        try:
            seq = int(seq_raw)
        except (TypeError, ValueError):
            continue
        if seq not in stop_by_seq:
            stop_by_seq[seq] = stop

    segments: list[str] = []
    for seq in range(start_seq, end_seq):
        stop = stop_by_seq.get(seq)
        if not stop:
            continue
        encoded = str(stop.get("next_encoded_polyline") or "").strip()
        if encoded:
            segments.append(encoded)
    return segments


def _normalize_stop_name_for_endpoint_matching(name: str | None) -> str:
    if not name:
        return ""
    try:
        return load_data._normalize_endpoint_name(str(name))  # type: ignore[attr-defined]
    except Exception:
        return str(name).strip()


def _option_component_names(option: dict) -> set[str]:
    names: set[str] = set()
    components = option.get("endpoint_components")
    if isinstance(components, list):
        for comp in components:
            normalized = _normalize_stop_name_for_endpoint_matching(str(comp))
            if normalized:
                names.add(normalized)
    primary = _normalize_stop_name_for_endpoint_matching(str(option.get("endpoint_primary_name", "")))
    if primary:
        names.add(primary)
    if not names:
        raw = str(option.get("endpoint_name", ""))
        for token in raw.split("/"):
            normalized = _normalize_stop_name_for_endpoint_matching(token)
            if normalized:
                names.add(normalized)
    return names


def _build_selected_endpoint_component_set(
    site_id: str,
    day_type: str,
    direction: str,
    selected_endpoints: list[str] | None,
) -> set[str] | None:
    options = get_endpoint_options(site_id, day_type, direction)
    if not options:
        return None

    option_by_name: dict[str, dict] = {}
    for opt in options:
        name = str(opt.get("endpoint_name", "")).strip()
        if name:
            option_by_name[name] = opt

    if selected_endpoints is None:
        selected_names = set(option_by_name.keys())
    else:
        selected_names = {str(name).strip() for name in selected_endpoints if str(name).strip()}

    if not selected_names:
        return None

    selected_components: set[str] = set()
    for endpoint_name in selected_names:
        opt = option_by_name.get(endpoint_name)
        if not opt:
            continue
        selected_components.update(_option_component_names(opt))
    return selected_components or None


def _choose_endpoint_display_stop(
    route_stops: list[dict],
    *,
    direction: str,
    selected_components: set[str] | None,
) -> dict | None:
    if not route_stops:
        return None

    canonical_index = (len(route_stops) - 1) if direction == "depart" else 0
    canonical_stop = route_stops[canonical_index]
    if not selected_components:
        return canonical_stop

    normalized_stop_names = [
        _normalize_stop_name_for_endpoint_matching(str(stop.get("stop_name", "")))
        for stop in route_stops
    ]
    matched_indices = [
        idx for idx, normalized in enumerate(normalized_stop_names)
        if normalized and normalized in selected_components
    ]
    if not matched_indices:
        return canonical_stop

    canonical_name = normalized_stop_names[canonical_index]
    if canonical_name and canonical_name in selected_components:
        return canonical_stop

    target_index = max(matched_indices) if direction == "depart" else min(matched_indices)
    return route_stops[target_index]


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


def _parse_max_distance_km(raw: str | None) -> float:
    """최대 정류장 거리(km) 파라미터 파싱/검증."""
    if raw is None or raw.strip() == "":
        return _DEFAULT_MAX_DISTANCE_KM
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("max_distance_km은 0~30 범위의 숫자여야 합니다.") from exc
    if not math.isfinite(value) or value < 0 or value > _MAX_DISTANCE_KM:
        raise ValueError("max_distance_km은 0~30 범위여야 합니다.")
    return value


def _parse_route_search_mode(raw: str | None) -> str:
    value = str(raw or "").strip().lower()
    return value if value in {"distance", "time"} else _DEFAULT_ROUTE_SEARCH_MODE


def _clock_minutes_from_text(raw: str | None) -> int | None:
    text = str(raw or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except (TypeError, ValueError):
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return None
    return (hour * 60) + minute


def _signed_minutes_from_reference(target_time: str | None, reference_time: str | None) -> int | None:
    target_minutes = _clock_minutes_from_text(target_time)
    reference_minutes = _clock_minutes_from_text(reference_time)
    if target_minutes is None or reference_minutes is None:
        return None
    delta = target_minutes - reference_minutes
    while delta <= -720:
        delta += 1440
    while delta > 720:
        delta -= 1440
    return delta


def _sort_route_options_by_search_mode(options: list[dict], search_mode: str, reference_time: str | None) -> list[dict]:
    normalized_mode = _parse_route_search_mode(search_mode)
    items = list(options or [])
    if normalized_mode == "time" and reference_time:
        def time_key(item: dict) -> tuple:
            delta = _signed_minutes_from_reference(item.get("board_time"), reference_time)
            if delta is None:
                return (2, float(item.get("distance_m") or 0), str(item.get("route_name") or ""))
            return (
                0 if delta >= 0 else 1,
                abs(delta),
                float(item.get("distance_m") or 0),
                str(item.get("board_time") or ""),
                str(item.get("route_name") or ""),
            )
        return sorted(items, key=time_key)
    return sorted(
        items,
        key=lambda item: (
            float(item.get("distance_m") or 0),
            str(item.get("board_time") or ""),
            str(item.get("route_name") or ""),
        ),
    )


def _parse_selected_endpoints() -> list[str] | None:
    """
    selected_endpoints 파라미터 파싱.
    - 파라미터 미존재: None
    - 존재하지만 값 없음: []
    - 값 존재: ["endpoint1", "endpoint2", ...] (중복 제거, 입력 순서 유지)
    """
    if "selected_endpoints" not in request.args:
        return None

    raw_values = request.args.getlist("selected_endpoints")
    tokens: list[str] = []
    for raw in raw_values:
        if raw is None:
            continue
        tokens.extend(raw.split(","))

    deduped: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        name = token.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        deduped.append(name)
    return deduped


def _is_truthy_param(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _walk_path_cache_key(from_lat: float, from_lng: float, to_lat: float, to_lng: float) -> str:
    return f"{from_lat:.6f},{from_lng:.6f}->{to_lat:.6f},{to_lng:.6f}"


def _walk_path_cache_get(cache_key: str) -> dict | None:
    with _walk_path_cache_lock:
        cached = _walk_path_cache.get(cache_key)
        if cached is None:
            return None
        _walk_path_cache.move_to_end(cache_key)
        return dict(cached)


def _walk_path_cache_set(cache_key: str, payload: dict) -> None:
    with _walk_path_cache_lock:
        _walk_path_cache[cache_key] = dict(payload)
        _walk_path_cache.move_to_end(cache_key)
        while len(_walk_path_cache) > OTP_WALK_CACHE_SIZE:
            _walk_path_cache.popitem(last=False)


def _fetch_walk_path_from_otp(from_lat: float, from_lng: float, to_lat: float, to_lng: float) -> dict:
    query = (
        "query { "
        f"plan(from:{{lat:{from_lat},lon:{from_lng}}} "
        f"to:{{lat:{to_lat},lon:{to_lng}}} "
        "transportModes:[{mode:WALK}]) { "
        "itineraries { duration legs { distance legGeometry { points } } } "
        "} }"
    )
    url = f"{OTP_BASE_URL}{OTP_PLAN_PATH}"
    response = requests.post(
        url,
        json={"query": query},
        timeout=OTP_WALK_HTTP_TIMEOUT_SEC,
    )
    response.raise_for_status()
    payload = response.json() if response.text else {}

    data = payload.get("data") if isinstance(payload, dict) else None
    plan = data.get("plan") if isinstance(data, dict) else None
    itineraries = plan.get("itineraries") if isinstance(plan, dict) else None
    if not itineraries:
        return {
            "has_route": False,
            "status": "empty_itineraries",
            "encoded_polyline": "",
            "duration_sec": None,
            "distance_m": None,
        }

    first = itineraries[0] if isinstance(itineraries[0], dict) else {}
    legs = first.get("legs") if isinstance(first, dict) else []
    encoded = ""
    distance_m = 0.0
    if isinstance(legs, list):
        for leg in legs:
            if not isinstance(leg, dict):
                continue
            try:
                distance_m += float(leg.get("distance") or 0.0)
            except (TypeError, ValueError):
                pass
            geom = leg.get("legGeometry")
            points = geom.get("points") if isinstance(geom, dict) else None
            if not encoded and points:
                encoded = str(points).strip()

    duration_sec: int | None
    try:
        raw_duration = first.get("duration")
        duration_sec = int(float(raw_duration)) if raw_duration is not None else None
    except (TypeError, ValueError):
        duration_sec = None
    if duration_sec is not None and duration_sec > 0:
        duration_sec = int(math.ceil(duration_sec * OTP_WALK_DURATION_MULTIPLIER))

    return {
        "has_route": bool(encoded),
        "status": "ok" if encoded else "empty_geometry",
        "encoded_polyline": encoded,
        "duration_sec": duration_sec,
        "distance_m": distance_m if distance_m > 0 else None,
    }


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


def _is_allowed_github_image_host(hostname: str) -> bool:
    host = str(hostname or "").strip().lower().rstrip(".")
    if not host:
        return False
    if host in {
        "github.com",
        "raw.githubusercontent.com",
        "user-images.githubusercontent.com",
        "private-user-images.githubusercontent.com",
        "objects.githubusercontent.com",
        "camo.githubusercontent.com",
        "media.githubusercontent.com",
    }:
        return True
    return host.endswith(".githubusercontent.com")


def _normalize_github_image_url(raw_url: str) -> str:
    parsed = urlparse(str(raw_url or "").strip())
    host = str(parsed.hostname or "").strip().lower().rstrip(".")
    if host != "github.com":
        return parsed.geturl()
    path = str(parsed.path or "")
    if not path.startswith("/user-attachments/assets/"):
        return parsed.geturl()
    query_items = parse_qsl(parsed.query, keep_blank_values=True)
    has_raw = any(str(k).strip().lower() == "raw" for k, _ in query_items)
    if not has_raw:
        query_items.append(("raw", "1"))
    return urlunparse(parsed._replace(query=urlencode(query_items)))


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


def _extract_prefixed_label_value(labels: list[str], prefix: str) -> str:
    key = str(prefix or "").strip().lower()
    if not key:
        return ""
    marker = key + ":"
    for raw in labels or []:
        label = str(raw or "").strip()
        if not label:
            continue
        if label.lower().startswith(marker):
            value = label[len(marker) :].strip()
            if value:
                return value
    return ""


def _normalize_issue_label_nickname(raw: object) -> str:
    text = str(raw or "")
    if not text:
        return ""
    normalized = " ".join(text.replace("\r", " ").replace("\n", " ").split())
    return normalized[:41]


def _build_issue_nickname_label(raw: object) -> str:
    nickname = _normalize_issue_label_nickname(raw)
    if not nickname:
        return ""
    return f"nickname:{nickname}"[:50]


def _collect_issue_author_user_ids(raw_items: list[dict]) -> list[str]:
    user_ids: list[str] = []
    seen: set[str] = set()
    for item in raw_items or []:
        if item.get("pull_request"):
            continue
        labels = _normalize_label_names(item.get("labels"))
        user_id = _extract_prefixed_label_value(labels, "user-id")
        if not user_id or user_id in seen:
            continue
        seen.add(user_id)
        user_ids.append(user_id)
    return user_ids


def _normalize_issue(issue: dict, nickname_by_public_user_id: dict[str, str] | None = None) -> dict:
    user = issue.get("user") or {}
    login = str(user.get("login") or "")
    body, _ = _split_issue_body_and_meta(str(issue.get("body") or ""))
    labels = _normalize_label_names(issue.get("labels"))
    owner = (GITHUB_REPO_OWNER or "").strip().lower()
    is_repo_owner = bool(owner and login.strip().lower() == owner)
    is_notice = is_repo_owner
    author_user_id = _extract_prefixed_label_value(labels, "user-id")
    author_nickname_label = _extract_prefixed_label_value(labels, "nickname")
    display_name = ""
    if author_user_id and isinstance(nickname_by_public_user_id, dict):
        display_name = str(nickname_by_public_user_id.get(author_user_id) or "").strip()
    if not display_name and author_nickname_label:
        display_name = author_nickname_label
    if not display_name and is_repo_owner:
        display_name = login
    return {
        "number": issue.get("number"),
        "title": issue.get("title") or "",
        "body": body,
        "state": issue.get("state") or "open",
        "labels": labels,
        "url": issue.get("html_url") or "",
        "created_at": issue.get("created_at"),
        "updated_at": issue.get("updated_at"),
        "closed_at": issue.get("closed_at"),
        "user": login,
        "display_name": display_name,
        "author_user_id": author_user_id,
        "is_repo_owner": is_repo_owner,
        "is_notice": is_notice,
        "comments": issue.get("comments") or 0,
    }


def _with_issue_like_state(issues: list[dict], like_state_map: dict[int, dict] | None = None) -> list[dict]:
    like_state_map = like_state_map or {}
    enriched: list[dict] = []
    for issue in issues or []:
        item = dict(issue or {})
        try:
            issue_number = int(item.get("number") or 0)
        except (TypeError, ValueError):
            issue_number = 0
        like_state = like_state_map.get(issue_number) or {}
        item["like_count"] = int(like_state.get("like_count") or 0)
        item["liked_by_me"] = bool(like_state.get("liked_by_me"))
        enriched.append(item)
    return enriched


def _normalize_comment(comment: dict) -> dict:
    user = comment.get("user") or {}
    login = str(user.get("login") or "")
    body, meta = _split_issue_body_and_meta(str(comment.get("body") or ""))
    nickname = ""
    user_id = ""
    if isinstance(meta, dict):
        nickname = str(meta.get("nickname") or "").strip()[:40]
        user_id = str(meta.get("user-id") or "").strip()[:64]
    owner = (GITHUB_REPO_OWNER or "").strip().lower()
    is_repo_owner = bool(owner and login.strip().lower() == owner)
    user_type = str(user.get("type") or "").strip().lower()
    is_bot = (
        user_type == "bot"
        or login.lower().endswith("[bot]")
        or isinstance(comment.get("performed_via_github_app"), dict)
    )
    if nickname:
        display_name = nickname
    elif is_bot:
        display_name = ""
    else:
        display_name = login
    return {
        "id": comment.get("id"),
        "body": body,
        "created_at": comment.get("created_at"),
        "updated_at": comment.get("updated_at"),
        "user": login,
        "display_name": display_name,
        "nickname": nickname,
        "user_id": user_id,
        "is_bot": is_bot,
        "is_repo_owner": is_repo_owner,
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


def _issue_cache_get_allow_stale(cache_key: str) -> tuple[dict | None, bool]:
    with _issue_cache_lock:
        cached = _issue_cache.get(cache_key)
        if not cached:
            return None, False
        expires_at, payload = cached
        return payload, expires_at > _time.time()


def _issue_cache_set(cache_key: str, payload: dict, ttl_sec: int | None = None) -> None:
    ttl = GITHUB_ISSUE_CACHE_TTL_SEC if ttl_sec is None else max(1, ttl_sec)
    with _issue_cache_lock:
        _issue_cache[cache_key] = (_time.time() + ttl, payload)


def _notice_head_cache_key(per_page: int) -> tuple[str, int]:
    capped_per_page = max(1, min(int(per_page or 100), 100))
    return f"issues:v6:notice-head:{capped_per_page}", capped_per_page


def _extract_notice_rows(cached: dict | None) -> list[dict]:
    return list((cached or {}).get("notices") or [])


def _fetch_notice_head_rows(*, per_page: int) -> list[dict]:
    owner = (GITHUB_REPO_OWNER or "").strip()
    if not owner:
        return []

    notice_rows: list[dict] = []
    max_pages = 5
    for page in range(1, max_pages + 1):
        raw_items = _github_api_request(
            "GET",
            f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues",
            params={
                "state": "all",
                "page": page,
                "per_page": per_page,
                "sort": "created",
                "direction": "desc",
                "creator": owner,
            },
        )
        if not isinstance(raw_items, list):
            raw_items = []
        for item in raw_items:
            if item.get("pull_request"):
                continue
            normalized = _normalize_issue(item)
            if not normalized.get("is_notice"):
                continue
            try:
                issue_number = int(normalized.get("number") or 0)
            except (TypeError, ValueError):
                issue_number = 0
            if issue_number <= 0:
                continue
            notice_rows.append(
                {
                    "number": issue_number,
                    "title": str(normalized.get("title") or "").strip(),
                    "created_at": normalized.get("created_at"),
                }
            )
        if len(raw_items) < per_page:
            break
    notice_rows.sort(key=lambda row: int(row.get("number") or 0), reverse=True)
    return notice_rows


def _refresh_notice_head_cache(cache_key: str, *, per_page: int) -> list[dict]:
    notice_rows = _fetch_notice_head_rows(per_page=per_page)
    _issue_cache_set(cache_key, {"notices": notice_rows}, ttl_sec=NOTICE_HEAD_CACHE_TTL_SEC)
    return notice_rows


def _run_notice_head_revalidate(cache_key: str, *, per_page: int) -> None:
    try:
        _refresh_notice_head_cache(cache_key, per_page=per_page)
    except GitHubAPIError as exc:
        app.logger.warning("Notice head cache revalidate failed for %s: %s", cache_key, exc)
    except Exception:
        app.logger.exception("Notice head cache revalidate crashed for %s", cache_key)
    finally:
        with _notice_head_revalidate_lock:
            _notice_head_revalidate_inflight.discard(cache_key)


def _trigger_notice_head_revalidate(cache_key: str, *, per_page: int) -> None:
    with _notice_head_revalidate_lock:
        if cache_key in _notice_head_revalidate_inflight:
            return
        _notice_head_revalidate_inflight.add(cache_key)
    worker = threading.Thread(
        target=_run_notice_head_revalidate,
        kwargs={"cache_key": cache_key, "per_page": per_page},
        daemon=True,
        name=f"notice-head-revalidate-{per_page}",
    )
    try:
        worker.start()
    except Exception:
        with _notice_head_revalidate_lock:
            _notice_head_revalidate_inflight.discard(cache_key)
        app.logger.exception("Failed to start notice head cache revalidate thread for %s", cache_key)


def _clear_issue_cache_local() -> None:
    with _issue_cache_lock:
        _issue_cache.clear()


def _set_local_issue_cache_version(version: int) -> bool:
    global _issue_cache_version_local
    next_version = max(0, int(version or 0))
    with _issue_cache_version_lock:
        changed = next_version != _issue_cache_version_local
        _issue_cache_version_local = next_version
        return changed


def _sync_issue_cache_version() -> None:
    try:
        shared_version = int(user_store.get_issue_cache_version() or 0)
    except Exception:
        app.logger.exception("Failed to fetch issue cache version")
        return
    if _set_local_issue_cache_version(shared_version):
        _clear_issue_cache_local()


def _invalidate_issue_cache(distributed: bool = True) -> None:
    _clear_issue_cache_local()
    if not distributed:
        return
    try:
        shared_version = int(user_store.bump_issue_cache_version() or 0)
    except Exception:
        app.logger.exception("Failed to bump issue cache version")
        return
    _set_local_issue_cache_version(shared_version)


def _load_notice_head_cache(*, per_page: int = 100, stale_while_revalidate: bool = False) -> list[dict]:
    _sync_issue_cache_version()
    cache_key, capped_per_page = _notice_head_cache_key(per_page)
    cached, is_fresh_cache = _issue_cache_get_allow_stale(cache_key)

    if stale_while_revalidate and cached is not None:
        if not is_fresh_cache:
            _trigger_notice_head_revalidate(cache_key, per_page=capped_per_page)
        return _extract_notice_rows(cached)

    if not is_fresh_cache:
        try:
            notice_rows = _refresh_notice_head_cache(cache_key, per_page=capped_per_page)
        except GitHubAPIError as exc:
            if cached is None:
                raise
            app.logger.warning("Falling back to stale notice head cache for %s due to GitHub error: %s", cache_key, exc)
        else:
            cached = {"notices": notice_rows}

    return _extract_notice_rows(cached)


def _resolve_notice_actor() -> tuple[dict | None, int | None, str | None]:
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        return auth_user, int(auth_user["id"]), None
    return None, None, _ensure_history_visitor_id()


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


def _split_issue_body_and_meta(raw_body: str) -> tuple[str, dict]:
    full = str(raw_body or "")
    if not full:
        return "", {}

    marker_full = "\n\n---\nmeta\n```json\n"
    marker_meta_only = "meta\n```json\n"

    if full.startswith(marker_meta_only) and full.endswith("\n```"):
        raw_meta = full[len(marker_meta_only) : -len("\n```")]
        try:
            parsed = json.loads(raw_meta)
        except Exception:
            return full, {}
        if isinstance(parsed, dict):
            return "", parsed
        return full, {}

    if marker_full not in full or not full.endswith("\n```"):
        return full, {}

    body_part, tail = full.rsplit(marker_full, 1)
    raw_meta = tail[: -len("\n```")]
    try:
        parsed = json.loads(raw_meta)
    except Exception:
        return full, {}

    if isinstance(parsed, dict):
        return body_part.rstrip(), parsed
    return full, {}


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


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _hash_session_token(raw_token: str) -> str:
    return hashlib.sha256(str(raw_token).encode("utf-8")).hexdigest()


def _auth_cookie_secure() -> bool:
    override = os.environ.get("SESSION_COOKIE_SECURE")
    if override is None:
        return bool(request.is_secure)
    return str(override).strip().lower() in {"1", "true", "yes", "on"}


def _validate_nickname(nickname: str) -> str | None:
    value = str(nickname or "").strip()
    if len(value) < AUTH_NICKNAME_MIN_LEN or len(value) > AUTH_NICKNAME_MAX_LEN:
        return f"닉네임은 {AUTH_NICKNAME_MIN_LEN}~{AUTH_NICKNAME_MAX_LEN}자여야 합니다."
    if value.casefold() in AUTH_RESERVED_NICKNAMES:
        return "해당 닉네임은 사용할 수 없습니다."
    return None


def _auth_cookie_samesite() -> str:
    configured = os.environ.get("SESSION_COOKIE_SAMESITE", "").strip()
    if configured:
        return configured
    return "None" if _auth_cookie_secure() else "Lax"


def _set_auth_session_cookie(resp, raw_token: str) -> None:
    max_age = 60 * 60 * 24 * AUTH_SESSION_TTL_DAYS
    resp.set_cookie(
        AUTH_SESSION_COOKIE_NAME,
        raw_token,
        max_age=max_age,
        httponly=True,
        secure=_auth_cookie_secure(),
        samesite=_auth_cookie_samesite(),
        domain=AUTH_COOKIE_DOMAIN,
        path="/",
    )


def _clear_auth_session_cookie(resp) -> None:
    resp.set_cookie(
        AUTH_SESSION_COOKIE_NAME,
        "",
        max_age=0,
        httponly=True,
        secure=_auth_cookie_secure(),
        samesite=_auth_cookie_samesite(),
        domain=AUTH_COOKIE_DOMAIN,
        path="/",
    )


def _normalize_history_visitor_cookie(raw_value: str | None) -> str:
    value = str(raw_value or "").strip()
    if not value or len(value) > 64:
        return ""
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if any(ch not in allowed for ch in value):
        return ""
    return value


def _ensure_history_visitor_id() -> str:
    if getattr(request, "_history_visitor_loaded", False):
        return str(getattr(request, "_history_visitor_id", "") or "")

    visitor_id = _normalize_history_visitor_cookie(request.cookies.get(HISTORY_VISITOR_COOKIE_NAME))
    should_set_cookie = False
    if not visitor_id:
        visitor_id = uuid.uuid4().hex
        should_set_cookie = True

    request._history_visitor_loaded = True
    request._history_visitor_id = visitor_id
    request._history_visitor_should_set_cookie = should_set_cookie
    return visitor_id


def _attach_history_visitor_cookie_if_needed(resp):
    if not getattr(request, "_history_visitor_should_set_cookie", False):
        return resp
    visitor_id = str(getattr(request, "_history_visitor_id", "") or "").strip()
    if not visitor_id:
        return resp
    resp.set_cookie(
        HISTORY_VISITOR_COOKIE_NAME,
        visitor_id,
        max_age=60 * 60 * 24 * HISTORY_VISITOR_COOKIE_TTL_DAYS,
        httponly=True,
        secure=_auth_cookie_secure(),
        samesite=_auth_cookie_samesite(),
        domain=AUTH_COOKIE_DOMAIN,
        path="/",
    )
    request._history_visitor_should_set_cookie = False
    return resp


def _session_token_from_request() -> str:
    return (request.cookies.get(AUTH_SESSION_COOKIE_NAME) or "").strip()


def _client_ip() -> str:
    forwarded_ip = request.headers.get("CF-Connecting-IP", "").strip()
    if not forwarded_ip:
        forwarded_for = request.headers.get("X-Forwarded-For", "").strip()
        forwarded_ip = forwarded_for.split(",")[0].strip() if forwarded_for else ""
    return forwarded_ip or request.remote_addr or "unknown"


def _create_auth_session_for_user(user: dict) -> str:
    raw_token = secrets.token_urlsafe(48)
    token_hash = _hash_session_token(raw_token)
    expires_at = _utc_now() + timedelta(days=AUTH_SESSION_TTL_DAYS)
    user_store.create_session(
        user_id=int(user["id"]),
        session_token_hash=token_hash,
        expires_at=expires_at,
        user_agent=request.headers.get("User-Agent", ""),
        ip=_client_ip(),
    )
    return raw_token


def _get_current_user(with_touch: bool = False) -> tuple[dict | None, str | None]:
    if getattr(request, "_auth_user_cache_loaded", False):
        user = getattr(request, "_auth_user_cache_user", None)
        token_hash = getattr(request, "_auth_user_cache_token_hash", None)
    else:
        raw_token = _session_token_from_request()
        if not raw_token:
            request._auth_user_cache_loaded = True
            request._auth_user_cache_user = None
            request._auth_user_cache_token_hash = None
            request._auth_user_cache_touched = False
            return None, None
        token_hash = _hash_session_token(raw_token)
        user = user_store.get_user_by_session_hash(token_hash)
        request._auth_user_cache_loaded = True
        request._auth_user_cache_user = user
        request._auth_user_cache_token_hash = token_hash
        request._auth_user_cache_touched = False

    if with_touch and user is not None and not getattr(request, "_auth_user_cache_touched", False):
        if AUTH_SESSION_SLIDING:
            user_store.touch_session(
                token_hash,
                expires_at=_utc_now() + timedelta(days=AUTH_SESSION_TTL_DAYS),
                min_interval_sec=AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC,
            )
        else:
            user_store.touch_session(
                token_hash,
                expires_at=None,
                min_interval_sec=AUTH_SESSION_TOUCH_MIN_INTERVAL_SEC,
            )
        request._auth_user_cache_touched = True
    return user, token_hash


def _auth_required() -> tuple[dict | None, str | None, Response | None]:
    user, token_hash = _get_current_user(with_touch=True)
    if user is None:
        resp = make_response(jsonify({"error": "로그인이 필요합니다."}), 401)
        if token_hash is not None:
            _clear_auth_session_cookie(resp)
        return None, token_hash, resp
    return user, token_hash, None


def _is_dev_request() -> bool:
    host = (request.host or "").split(":", 1)[0].strip().lower()
    origin = (request.headers.get("Origin") or "").strip().lower()
    if host in {"localhost", "127.0.0.1"} or host.startswith("192.168."):
        return True
    return (
        origin.startswith("http://localhost")
        or origin.startswith("http://127.0.0.1")
        or origin.startswith("http://192.168.")
    )


def _is_mobile_report_request() -> bool:
    if _is_dev_request():
        return True
    ch_mobile = (request.headers.get("Sec-CH-UA-Mobile") or "").strip()
    ch_platform = (request.headers.get("Sec-CH-UA-Platform") or "").strip().strip('"').lower()
    user_agent = (request.headers.get("User-Agent") or "").strip().lower()
    if ch_mobile == "?1":
        return True
    if ch_platform in {"android", "ios", "ipados"}:
        return True
    mobile_tokens = ("android", "iphone", "ipad", "ipod", "mobile", "tablet", "; wv")
    return any(token in user_agent for token in mobile_tokens)


def _mobile_report_guard() -> Response | None:
    if _is_mobile_report_request():
        return None
    return make_response(jsonify({"error": "모바일 기기에서만 사용할 수 있습니다."}), 403)


def _parse_client_reported_at(raw_value: object) -> datetime | None:
    dt = user_store.parse_iso(str(raw_value or "").strip())
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _service_date_from_client_reported_at(client_dt: datetime) -> tuple[str, int]:
    local_dt = client_dt.astimezone(_KST)
    if local_dt.hour < 4:
        local_dt = local_dt - timedelta(days=1)
    return local_dt.date().isoformat(), (local_dt.hour * 60 + local_dt.minute)


def _is_client_reported_at_within_allowed_window(
    client_dt: datetime,
    server_dt: datetime,
    *,
    past_sec: int = _REPORT_ALLOWED_PAST_SEC,
    future_sec: int = _REPORT_ALLOWED_FUTURE_SEC,
) -> bool:
    delta_sec = (client_dt - server_dt).total_seconds()
    return (-float(past_sec) <= delta_sec <= float(future_sec))


def _display_arrival_report_reason(reason: str | None) -> str:
    code = str(reason or "").strip()
    if code == "user_deleted":
        return "사용자 삭제"
    if code == "client_clock_skew":
        return "기기 시각 오차 5분 초과"
    if code == "superseded":
        return "최신 제보로 대체됨"
    return code


def _serialize_arrival_report_item(
    item: dict,
    nickname_map: dict[int, str] | None = None,
    like_state_map: dict[int, dict] | None = None,
    viewer_user_id: int | None = None,
    viewer_visitor_id: str | None = None,
) -> dict:
    nickname_map = nickname_map or {}
    like_state_map = like_state_map or {}
    user_id = item.get("user_id")
    reporter_name = "-"
    if user_id is not None:
        try:
            reporter_name = str(nickname_map.get(int(user_id)) or "").strip() or "-"
        except (TypeError, ValueError):
            reporter_name = "-"
    try:
        report_id = int(item["id"])
    except (TypeError, ValueError):
        report_id = 0
    like_state = like_state_map.get(report_id) or {}
    like_count = int(like_state.get("like_count") or 0)
    liked_by_me = bool(like_state.get("liked_by_me"))
    viewer_visitor = str(viewer_visitor_id or "").strip() or None
    like_disabled = False
    if viewer_user_id is not None:
        try:
            like_disabled = user_id is not None and int(user_id) == int(viewer_user_id)
        except (TypeError, ValueError):
            like_disabled = False
    else:
        owner_visitor_id = str(item.get("visitor_id") or "").strip() or None
        like_disabled = (
            user_id is None
            and viewer_visitor is not None
            and owner_visitor_id == viewer_visitor
        )
    if (not bool(item.get("eta_included"))) or bool(item.get("deleted_at")):
        like_disabled = True
    return {
        "id": int(item["id"]),
        "route_id": int(item["route_id"]),
        "route_uuid": (str(item.get("route_uuid") or "").strip() or None),
        "route_name": str(item["route_name"]),
        "route_type": str(item["route_type"]),
        "day_type": str(item["day_type"]),
        "direction": str(item["direction"]),
        "departure_time": str(item["departure_time"]),
        "stop_id": int(item["stop_id"]),
        "stop_uuid": (str(item.get("stop_uuid") or "").strip() or None),
        "stop_name": str(item["stop_name"]),
        "stop_sequence": item.get("stop_sequence"),
        "client_reported_at": str(item["client_reported_at"]),
        "server_received_at": str(item["server_received_at"]),
        "service_date": str(item["service_date"]),
        "time_valid": bool(item.get("time_valid")),
        "client_server_delta_sec": int(item.get("client_server_delta_sec") or 0),
        "eta_included": bool(item.get("eta_included")),
        "eta_excluded": bool(item.get("eta_excluded")),
        "eta_exclude_reason": str(item.get("eta_exclude_reason") or ""),
        "report_state": str(item.get("report_state") or ""),
        "report_state_label": str(item.get("report_state_label") or ""),
        "report_note": str(item.get("report_note") or ""),
        "credit_awarded": bool(item.get("credit_awarded")),
        "credit_value": round(float(item.get("credit_value") or 0.0), 6),
        "is_deleted": bool(item.get("deleted_at")),
        "deleted_at": item.get("deleted_at"),
        "deleted_reason": _display_arrival_report_reason(item.get("deleted_reason")),
        "reporter_name": reporter_name,
        "like_count": like_count,
        "liked_by_me": liked_by_me,
        "like_disabled": bool(like_disabled),
    }


def _serialize_report_browser_detail_item(
    item: dict,
    *,
    like_state_map: dict[int, dict] | None = None,
) -> dict:
    like_state_map = like_state_map or {}
    try:
        report_id = int(item.get("id") or 0)
    except (TypeError, ValueError):
        report_id = 0
    like_state = like_state_map.get(report_id) or {}
    return {
        "id": report_id,
        "client_reported_at": str(item.get("client_reported_at") or ""),
        "report_state": str(item.get("report_state") or ""),
        "report_state_label": str(item.get("report_state_label") or ""),
        "report_note": str(item.get("report_note") or ""),
        "credit_awarded": bool(item.get("credit_awarded")),
        "credit_value": round(float(item.get("credit_value") or 0.0), 6),
        "like_count": int(like_state.get("like_count") or 0),
    }


def _serialize_user_profile(user: dict) -> dict:
    return {
        "id": user.get("public_user_id"),
        "nickname": user.get("nickname") or "",
        "email": user.get("email"),
        "email_consent": bool(user.get("email_consent")),
        "created_at": user.get("created_at"),
    }


def _origin_allowed_for_auth(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    if not parsed.netloc:
        return False
    origin = f"{parsed.scheme}://{parsed.netloc}".lower().rstrip("/")
    allowed = {o.lower().rstrip("/") for o in AUTH_FRONTEND_ALLOWED_ORIGINS}
    return origin in allowed


def _resolve_frontend_callback_url(raw_next: str | None) -> str:
    candidate = (raw_next or "").strip()
    if candidate and _origin_allowed_for_auth(candidate):
        return candidate
    if _origin_allowed_for_auth(AUTH_FRONTEND_CALLBACK_URL):
        return AUTH_FRONTEND_CALLBACK_URL
    return "https://shuttle-go.com/auth-callback.html"


def _build_frontend_redirect(base_url: str, params: dict[str, str | int | None]) -> str:
    clean = {k: str(v) for k, v in params.items() if v is not None and str(v) != ""}
    if not clean:
        return base_url
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(clean)}"


def _frontend_home_url(callback_url: str) -> str:
    parsed = urlparse((callback_url or "").strip())
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}/"
    return "/"


def _google_callback_redirect_uri() -> str:
    if GOOGLE_OAUTH_REDIRECT_URI:
        return GOOGLE_OAUTH_REDIRECT_URI
    return request.url_root.rstrip("/") + "/api/auth/google/callback"


def _build_google_authorize_url(state: str) -> str:
    redirect_uri = _google_callback_redirect_uri()
    query = {
        "response_type": "code",
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "scope": "openid email profile",
        "state": state,
        "include_granted_scopes": "true",
        "prompt": "select_account",
    }
    return f"{GOOGLE_OAUTH_AUTHORIZE_URL}?{urlencode(query)}"


def _google_exchange_code(code: str) -> dict:
    if not GOOGLE_OAUTH_CLIENT_SECRET:
        raise ValueError("구글 로그인 설정이 누락되었습니다: GOOGLE_OAUTH_CLIENT_SECRET")
    payload = {
        "grant_type": "authorization_code",
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
        "redirect_uri": _google_callback_redirect_uri(),
        "code": code,
    }
    resp = requests.post(GOOGLE_OAUTH_TOKEN_URL, data=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json() if resp.text else {}
    access_token = str(data.get("access_token") or "").strip()
    if not access_token:
        raise ValueError("구글 access_token 응답이 비어있습니다.")
    return data


def _google_fetch_profile(access_token: str) -> dict:
    resp = requests.get(
        GOOGLE_OAUTH_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json() if resp.text else {}
    google_sub = str(data.get("sub") or "").strip()
    if not google_sub:
        raise ValueError("구글 사용자 식별자(sub)가 비어있습니다.")
    email = str(data.get("email") or "").strip() or None
    email_verified = bool(data.get("email_verified")) if email else False
    nickname_hint = str(data.get("name") or "").strip() or None
    return {
        "provider": "google",
        "provider_user_id": google_sub,
        "provider_email": email,
        "provider_email_verified": email_verified,
        "provider_nickname": nickname_hint,
    }


def _mask_email(email: str | None) -> str:
    value = (email or "").strip()
    if "@" not in value:
        return ""
    local, domain = value.split("@", 1)
    if len(local) <= 2:
        local_masked = local[:1] + "*"
    else:
        local_masked = local[:2] + "*" * (len(local) - 2)
    return f"{local_masked}@{domain}"


def _github_cleanup_labels_for_deleted_user(public_user_id: str) -> None:
    _ensure_github_issue_config()
    target_label = f"user-id:{public_user_id}"
    query = f'repo:{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME} is:issue label:"{target_label}"'
    page = 1
    per_page = 100
    while True:
        payload = _github_api_request(
            "GET",
            "/search/issues",
            params={
                "q": query,
                "sort": "created",
                "order": "desc",
                "page": page,
                "per_page": per_page,
            },
        )
        items = payload.get("items") if isinstance(payload, dict) else []
        if not isinstance(items, list) or not items:
            break
        for item in items:
            issue_number = item.get("number")
            if not isinstance(issue_number, int):
                continue
            labels = _normalize_label_names(item.get("labels"))
            next_labels = [label for label in labels if label != target_label]
            if "deleted-user" not in next_labels:
                next_labels.append("deleted-user")
            _github_api_request(
                "PATCH",
                f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues/{issue_number}",
                json_payload={"labels": next_labels},
            )
        if len(items) < per_page:
            break
        page += 1


def _auth_cleanup_loop() -> None:
    while True:
        try:
            _run_exclusive_background_task(
                "auth-cleanup",
                lambda: user_store.cleanup_old_data(
                    place_history_retention_months=PLACE_SEARCH_HISTORY_RETENTION_MONTHS,
                    route_history_retention_months=ROUTE_SEARCH_HISTORY_RETENTION_MONTHS,
                ),
                stale_after_sec=AUTH_CLEANUP_LEASE_SEC,
            )
        except Exception:
            app.logger.exception("Auth cleanup loop failed")
        _time.sleep(AUTH_CLEANUP_INTERVAL_SEC)


def _auth_metrics_loop() -> None:
    while True:
        try:
            _refresh_auth_business_metrics()
        except Exception:
            app.logger.exception("Auth metrics loop failed")
        _time.sleep(AUTH_METRICS_REFRESH_SEC)


def _bi_snapshot_loop() -> None:
    while True:
        try:
            _run_exclusive_background_task(
                "bi-snapshot",
                user_store.refresh_bi_snapshot,
                stale_after_sec=BI_SNAPSHOT_LEASE_SEC,
            )
        except Exception:
            app.logger.exception("BI snapshot loop failed")
        _time.sleep(BI_DB_SNAPSHOT_REFRESH_SEC)


def _run_exclusive_background_task(
    task_name: str,
    callback: Callable[[], Any],
    stale_after_sec: int = BACKGROUND_TASK_LEASE_STALE_SEC,
) -> Any | None:
    acquired = user_store.try_acquire_background_task_lease(
        task_name,
        BACKGROUND_WORKER_OWNER_ID,
        stale_after_sec=stale_after_sec,
    )
    if not acquired:
        return None
    return callback()


def _github_cleanup_loop() -> None:
    while True:
        sleep_for = 0.0
        try:
            result = _run_exclusive_background_task(
                "github-cleanup",
                run_github_cleanup_once,
                stale_after_sec=GITHUB_CLEANUP_LEASE_SEC,
            )
            if not result:
                sleep_for = float(AUTH_GITHUB_CLEANUP_INTERVAL_SEC)
        except Exception:
            app.logger.exception("GitHub cleanup loop failed")
            sleep_for = float(AUTH_GITHUB_CLEANUP_INTERVAL_SEC)
        if sleep_for > 0:
            _time.sleep(sleep_for)


def run_github_cleanup_once() -> bool:
    job = user_store.claim_due_github_cleanup_job()
    if not job:
        return False
    try:
        _github_cleanup_labels_for_deleted_user(str(job.get("public_user_id") or ""))
        user_store.complete_github_cleanup_job(int(job["id"]))
    except Exception as exc:
        attempts = int(job.get("attempts") or 1)
        retry_after = min(60 * 60, max(30, attempts * 30))
        user_store.fail_github_cleanup_job(int(job["id"]), str(exc), retry_after_sec=retry_after)
    return True


def _start_auth_background_workers() -> None:
    global _auth_background_started
    with _auth_background_lock:
        if _auth_background_started:
            return
        _auth_background_started = True
        threading.Thread(target=_auth_metrics_loop, daemon=True, name="auth-metrics-loop").start()
        threading.Thread(target=_auth_cleanup_loop, daemon=True, name="auth-cleanup-loop").start()
        threading.Thread(target=_github_cleanup_loop, daemon=True, name="github-cleanup-loop").start()


def _run_startup_hooks() -> None:
    init_db()
    user_store.init_db()
    _start_history_writer()
    if APP_REFRESH_AUTH_METRICS_ON_STARTUP:
        try:
            _refresh_auth_business_metrics()
        except Exception:
            app.logger.exception("Failed to initialize auth business metrics")
    if APP_WARM_ENDPOINT_CACHE_ON_STARTUP:
        try:
            warm_endpoint_cache()
        except Exception:
            app.logger.exception("Endpoint cache warm-up failed")
    if APP_ENABLE_BACKGROUND_WORKERS:
        _start_auth_background_workers()


@app.errorhandler(user_store.ReadOnlyModeError)
def handle_user_store_read_only(exc: user_store.ReadOnlyModeError):
    message = str(exc) or "사용자 정보 갱신이 잠시 중단되었습니다. 잠시 후 다시 시도해 주세요."
    wants_json = str(request.args.get("json", "")).strip().lower() in {"1", "true", "yes", "on"}
    if request.path in {"/api/auth/google/start", "/api/auth/google/callback"} and (
        request.path.endswith("/callback") or not wants_json
    ):
        redirect_url = _build_frontend_redirect(
            _resolve_frontend_callback_url(request.args.get("next")),
            {"result": "error", "message": message},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp
    return jsonify({"error": message, "code": "user_store_read_only"}), 503


# ── API 엔드포인트 ───────────────────────────────────────────
@app.route("/api/auth/providers")
def api_auth_providers():
    return jsonify(
        {
            "providers": [
                {
                    "provider": "google",
                    "enabled": bool(GOOGLE_OAUTH_CLIENT_ID),
                }
            ]
        }
    )


@app.route("/api/auth/google/start")
def api_auth_google_start():
    if not GOOGLE_OAUTH_CLIENT_ID:
        return jsonify({"error": "구글 로그인 설정이 누락되었습니다."}), 503

    next_url = _resolve_frontend_callback_url(request.args.get("next"))
    state = user_store.save_oauth_state("google", next_url, ttl_sec=AUTH_OAUTH_STATE_TTL_SEC)
    authorize_url = _build_google_authorize_url(state)
    if _is_truthy_param(request.args.get("json")):
        return jsonify({"authorize_url": authorize_url})

    resp = make_response("", 302)
    resp.headers["Location"] = authorize_url
    return resp


@app.route("/api/auth/google/callback")
def api_auth_google_callback():
    fallback_next_url = _resolve_frontend_callback_url(None)
    state = request.args.get("state", "").strip()
    code = request.args.get("code", "").strip()
    oauth_error = request.args.get("error", "").strip()
    oauth_error_description = request.args.get("error_description", "").strip()

    state_row = user_store.consume_oauth_state("google", state) if state else None
    next_url = str(state_row.get("next_url")) if state_row else fallback_next_url
    next_url = _resolve_frontend_callback_url(next_url)

    if state_row is None:
        redirect_url = _build_frontend_redirect(
            next_url,
            {"result": "error", "message": "로그인 상태값이 유효하지 않습니다. 다시 시도해 주세요."},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp

    if oauth_error:
        redirect_url = _build_frontend_redirect(
            next_url,
            {
                "result": "error",
                "message": oauth_error_description or oauth_error or "구글 로그인에 실패했습니다.",
            },
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp

    if not code:
        redirect_url = _build_frontend_redirect(
            next_url,
            {"result": "error", "message": "인가 코드가 없습니다."},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp

    try:
        token_payload = _google_exchange_code(code)
        access_token = str(token_payload.get("access_token") or "").strip()
        profile = _google_fetch_profile(access_token)
    except requests.RequestException as exc:
        redirect_url = _build_frontend_redirect(
            next_url,
            {"result": "error", "message": f"구글 로그인 요청 실패: {exc}"},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp
    except ValueError as exc:
        redirect_url = _build_frontend_redirect(
            next_url,
            {"result": "error", "message": str(exc)},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp

    provider_user_id = str(profile.get("provider_user_id") or "").strip()
    if not provider_user_id:
        redirect_url = _build_frontend_redirect(
            next_url,
            {"result": "error", "message": "구글 사용자 식별자를 확인할 수 없습니다."},
        )
        resp = make_response("", 302)
        resp.headers["Location"] = redirect_url
        return resp

    existing_user = user_store.get_user_by_provider("google", provider_user_id)
    if existing_user is not None:
        user_store.update_last_login("google", provider_user_id)
        try:
            _refresh_auth_business_metrics()
        except Exception:
            app.logger.exception("Failed to refresh auth metrics after login")
        raw_token = _create_auth_session_for_user(existing_user)
        redirect_url = _frontend_home_url(next_url)
        resp = make_response("", 302)
        _set_auth_session_cookie(resp, raw_token)
        resp.headers["Location"] = redirect_url
        return resp

    signup_token = user_store.save_pending_signup(
        provider="google",
        provider_user_id=provider_user_id,
        provider_email=profile.get("provider_email"),
        provider_email_verified=bool(profile.get("provider_email_verified")),
        provider_nickname=profile.get("provider_nickname"),
        ttl_sec=AUTH_PENDING_SIGNUP_TTL_SEC,
    )
    redirect_url = _build_frontend_redirect(
        next_url,
        {
            "result": "signup_required",
            "provider": "google",
            "signup_token": signup_token,
            "nickname_hint": profile.get("provider_nickname") or "",
            "email_available": 1 if profile.get("provider_email") else 0,
            "email_masked": _mask_email(profile.get("provider_email")),
        },
    )
    resp = make_response("", 302)
    resp.headers["Location"] = redirect_url
    return resp


@app.route("/api/auth/pending-signup")
def api_auth_pending_signup():
    token = request.args.get("token", "").strip()
    if not token:
        return jsonify({"error": "token이 필요합니다."}), 400
    pending = user_store.get_pending_signup(token)
    if pending is None:
        return jsonify({"error": "가입 세션이 만료되었거나 유효하지 않습니다."}), 400
    return jsonify(
        {
            "provider": pending.get("provider"),
            "nickname_hint": pending.get("provider_nickname") or "",
            "email_available": bool(pending.get("provider_email")),
            "email_masked": _mask_email(pending.get("provider_email")),
            "expires_at": pending.get("expires_at"),
        }
    )


@app.route("/api/auth/signup-complete", methods=["POST"])
def api_auth_signup_complete():
    payload = request.get_json(silent=True) or {}
    signup_token = str(payload.get("signup_token", "")).strip()
    nickname = str(payload.get("nickname", "")).strip()
    email_consent = bool(payload.get("email_consent"))

    if not signup_token:
        return jsonify({"error": "signup_token이 필요합니다."}), 400
    nickname_error = _validate_nickname(nickname)
    if nickname_error:
        return jsonify({"error": nickname_error}), 400

    try:
        user = user_store.consume_pending_signup_and_create_user(
            signup_token,
            nickname=nickname,
            email_consent=email_consent,
        )
    except user_store.NicknameConflictError as exc:
        return jsonify({"error": str(exc)}), 409
    except user_store.PendingSignupError as exc:
        return jsonify({"error": str(exc)}), 400
    except user_store.ReadOnlyModeError:
        raise
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("signup-complete failed")
        return jsonify({"error": "회원 가입 처리에 실패했습니다."}), 500

    try:
        _refresh_auth_business_metrics()
    except Exception:
        app.logger.exception("Failed to refresh auth metrics after signup")

    raw_token = _create_auth_session_for_user(user)
    prefs = user_store.get_preferences(int(user["id"]))
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "user": _serialize_user_profile(user),
                "preferences": {
                    "selected_site_id": prefs.get("selected_site_id"),
                    "route_search_mode": prefs.get("route_search_mode"),
                    "route_max_distance_km": prefs.get("route_max_distance_km"),
                },
            }
        )
    )
    _set_auth_session_cookie(resp, raw_token)
    return resp


@app.route("/api/auth/me")
def api_auth_me():
    user, token_hash = _get_current_user(with_touch=True)
    if user is None:
        resp = make_response(jsonify({"logged_in": False, "user": None, "preferences": {}}))
        if token_hash is not None:
            _clear_auth_session_cookie(resp)
        return resp
    prefs = user_store.get_preferences(int(user["id"]))
    return jsonify(
        {
            "logged_in": True,
            "user": _serialize_user_profile(user),
            "preferences": {
                "selected_site_id": prefs.get("selected_site_id"),
                "route_search_mode": prefs.get("route_search_mode"),
                "route_max_distance_km": prefs.get("route_max_distance_km"),
            },
        }
    )


@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    _, token_hash = _get_current_user(with_touch=False)
    if token_hash:
        user_store.delete_session(token_hash)
    resp = make_response(jsonify({"ok": True}))
    _clear_auth_session_cookie(resp)
    return resp


@app.route("/api/me/profile", methods=["PATCH"])
def api_me_profile():
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error
    payload = request.get_json(silent=True) or {}
    nickname = str(payload.get("nickname", "")).strip()
    nickname_error = _validate_nickname(nickname)
    if nickname_error:
        return jsonify({"error": nickname_error}), 400
    try:
        updated = user_store.set_user_nickname(int(user["id"]), nickname)
    except user_store.NicknameConflictError as exc:
        return jsonify({"error": str(exc)}), 409
    except user_store.ReadOnlyModeError:
        raise
    except Exception:
        app.logger.exception("profile update failed")
        return jsonify({"error": "닉네임 변경에 실패했습니다."}), 500
    return jsonify({"ok": True, "user": _serialize_user_profile(updated)})


@app.route("/api/me/preferences", methods=["GET", "PATCH"])
def api_me_preferences():
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error

    user_id = int(user["id"])
    if request.method == "GET":
        prefs = user_store.get_preferences(user_id)
        return jsonify(
            {
                "selected_site_id": prefs.get("selected_site_id"),
                "route_search_mode": prefs.get("route_search_mode"),
                "route_max_distance_km": prefs.get("route_max_distance_km"),
                "updated_at": prefs.get("updated_at"),
            }
        )

    payload = request.get_json(silent=True) or {}
    pref_unset = user_store._PREFERENCE_UNSET
    selected_site_id = payload.get("selected_site_id", pref_unset)
    if selected_site_id is not pref_unset:
        selected_site_id = str(selected_site_id).strip() or None
    route_search_mode = payload.get("route_search_mode", pref_unset)
    if route_search_mode is not pref_unset:
        route_search_mode = _parse_route_search_mode(route_search_mode)
    route_max_distance_km = payload.get("route_max_distance_km", pref_unset)
    if route_max_distance_km is not pref_unset:
        try:
            route_max_distance_km = _parse_max_distance_km(str(route_max_distance_km))
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
    prefs = user_store.set_preferences(
        user_id,
        selected_site_id=selected_site_id,
        route_search_mode=route_search_mode,
        route_max_distance_km=route_max_distance_km,
    )
    return jsonify(
        {
            "ok": True,
            "selected_site_id": prefs.get("selected_site_id"),
            "route_search_mode": prefs.get("route_search_mode"),
            "route_max_distance_km": prefs.get("route_max_distance_km"),
            "updated_at": prefs.get("updated_at"),
        }
    )


@app.route("/api/me/preferences/endpoint", methods=["GET", "PATCH"])
def api_me_endpoint_preferences():
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error

    user_id = int(user["id"])
    if request.method == "GET":
        site_id = request.args.get("site_id", "").strip()
        day_type = request.args.get("day_type", "").strip()
        direction = request.args.get("direction", "").strip()
        if direction not in {"depart", "arrive"}:
            return jsonify({"error": "direction은 depart|arrive만 허용됩니다."}), 400
        if not site_id or not day_type:
            return jsonify({"error": "site_id, day_type은 필수입니다."}), 400
        selected = user_store.get_endpoint_preference(user_id, site_id, day_type, direction)
        return jsonify(
            {
                "site_id": site_id,
                "day_type": day_type,
                "direction": direction,
                "selected_endpoints": selected or [],
                "has_saved": selected is not None,
            }
        )

    payload = request.get_json(silent=True) or {}
    site_id = str(payload.get("site_id", "")).strip()
    day_type = str(payload.get("day_type", "")).strip()
    direction = str(payload.get("direction", "")).strip()
    selected_endpoints = payload.get("selected_endpoints")
    if direction not in {"depart", "arrive"}:
        return jsonify({"error": "direction은 depart|arrive만 허용됩니다."}), 400
    if not site_id or not day_type:
        return jsonify({"error": "site_id, day_type은 필수입니다."}), 400
    if not isinstance(selected_endpoints, list):
        return jsonify({"error": "selected_endpoints는 배열이어야 합니다."}), 400

    user_store.set_endpoint_preference(
        user_id=user_id,
        site_id=site_id,
        day_type=day_type,
        direction=direction,
        endpoint_names=selected_endpoints,
    )
    return jsonify({"ok": True})


@app.route("/api/me/history/place-search")
def api_me_place_search_history():
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error
    cursor = request.args.get("cursor", default=None, type=int)
    limit = request.args.get("limit", default=20, type=int) or 20
    data = user_store.list_place_history(int(user["id"]), cursor=cursor, limit=limit)
    return jsonify(data)


@app.route("/api/me/history/place-search/<int:history_id>", methods=["DELETE"])
def api_me_place_search_history_delete(history_id: int):
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error
    ok = user_store.delete_place_history_item(int(user["id"]), history_id)
    if not ok:
        return jsonify({"error": "해당 히스토리를 찾을 수 없습니다."}), 404
    return jsonify({"ok": True})


@app.route("/api/me/history/route-search")
def api_me_route_search_history():
    user, _, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error
    cursor = request.args.get("cursor", default=None, type=int)
    limit = request.args.get("limit", default=20, type=int) or 20
    data = user_store.list_route_history(int(user["id"]), cursor=cursor, limit=limit)
    return jsonify(data)


@app.route("/api/me/reports")
def api_me_reports():
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        viewer_visitor_id = None
    else:
        user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    cursor = request.args.get("cursor", default=None, type=int)
    limit = request.args.get("limit", default=20, type=int) or 20
    data = user_store.list_my_arrival_reports(user_id, cursor=cursor, limit=limit, visitor_id=viewer_visitor_id)
    user_ids = [
        int(item["user_id"])
        for item in data.get("items", [])
        if item.get("user_id") is not None
    ]
    like_state_map = user_store.get_arrival_report_like_state([
        int(item["id"])
        for item in data.get("items", [])
        if item.get("id") is not None
    ], user_id=user_id, visitor_id=viewer_visitor_id)
    nickname_map = user_store.get_active_nickname_map_by_user_ids(user_ids)
    resp = make_response(
        jsonify(
            {
                "items": [
                    _serialize_arrival_report_item(
                        item,
                        nickname_map=nickname_map,
                        like_state_map=like_state_map,
                        viewer_user_id=user_id,
                        viewer_visitor_id=viewer_visitor_id,
                    )
                    for item in data.get("items", [])
                ],
                "next_cursor": data.get("next_cursor"),
                "has_more": bool(data.get("has_more")),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/basic-index")
def api_me_reports_basic_index():
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        viewer_visitor_id = None
    else:
        user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    data = user_store.list_my_arrival_report_basic_index(
        user_id,
        visitor_id=viewer_visitor_id,
    )
    resp = make_response(jsonify(data))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/route-index")
def api_me_reports_route_index():
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        viewer_visitor_id = None
    else:
        user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    data = user_store.list_my_arrival_report_route_index(
        user_id,
        visitor_id=viewer_visitor_id,
    )
    resp = make_response(jsonify(data))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/basic-group")
def api_me_reports_basic_group():
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        viewer_visitor_id = None
    else:
        user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    route_id = request.args.get("route_id", type=int)
    stop_id = request.args.get("stop_id", type=int)
    route_uuid = request.args.get("route_uuid", default="", type=str).strip() or None
    stop_uuid = request.args.get("stop_uuid", default="", type=str).strip() or None
    day_type = str(request.args.get("day_type") or "").strip()
    departure_time = str(request.args.get("departure_time") or "").strip()
    has_id_pair = route_id is not None and stop_id is not None
    has_uuid_pair = bool(route_uuid and stop_uuid)
    if (not has_id_pair and not has_uuid_pair) or not day_type or not departure_time:
        return jsonify({"error": "route_id/stop_id 또는 route_uuid/stop_uuid와 day_type, departure_time은 필수입니다."}), 400
    cursor = request.args.get("cursor", default=None, type=int)
    limit = request.args.get("limit", default=20, type=int) or 20
    data = user_store.list_my_arrival_report_group_items(
        user_id,
        int(route_id or 0),
        day_type,
        departure_time,
        int(stop_id or 0),
        cursor,
        limit=limit,
        route_uuid=route_uuid,
        stop_uuid=stop_uuid,
        visitor_id=viewer_visitor_id,
    )
    like_state_map = user_store.get_arrival_report_like_state(
        [
            int(item["id"])
            for item in data.get("items", [])
            if item.get("id") is not None
        ]
    )
    resp = make_response(
        jsonify(
            {
                "items": [
                    _serialize_report_browser_detail_item(
                        item,
                        like_state_map=like_state_map,
                    )
                    for item in data.get("items", [])
                ],
                "next_cursor": data.get("next_cursor"),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/route-group")
def api_me_reports_route_group():
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        viewer_visitor_id = None
    else:
        user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    route_id = request.args.get("route_id", type=int)
    stop_id = request.args.get("stop_id", type=int)
    route_uuid = request.args.get("route_uuid", default="", type=str).strip() or None
    stop_uuid = request.args.get("stop_uuid", default="", type=str).strip() or None
    day_type = str(request.args.get("day_type") or "").strip()
    departure_time = str(request.args.get("departure_time") or "").strip()
    has_id_pair = route_id is not None and stop_id is not None
    has_uuid_pair = bool(route_uuid and stop_uuid)
    if (not has_id_pair and not has_uuid_pair) or not day_type or not departure_time:
        return jsonify({"error": "route_id/stop_id 또는 route_uuid/stop_uuid와 day_type, departure_time은 필수입니다."}), 400
    cursor = request.args.get("cursor", default=None, type=int)
    limit = request.args.get("limit", default=20, type=int) or 20
    data = user_store.list_my_arrival_report_route_departure_items(
        user_id,
        int(route_id or 0),
        int(stop_id or 0),
        day_type,
        departure_time,
        cursor,
        limit=limit,
        route_uuid=route_uuid,
        stop_uuid=stop_uuid,
        visitor_id=viewer_visitor_id,
    )
    like_state_map = user_store.get_arrival_report_like_state(
        [
            int(item["id"])
            for item in data.get("items", [])
            if item.get("id") is not None
        ]
    )
    resp = make_response(
        jsonify(
            {
                "items": [
                    _serialize_report_browser_detail_item(
                        item,
                        like_state_map=like_state_map,
                    )
                    for item in data.get("items", [])
                ],
                "next_cursor": data.get("next_cursor"),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/<int:report_id>", methods=["DELETE"])
def api_me_reports_delete(report_id: int):
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()
    ok = user_store.soft_delete_arrival_report(user_id, report_id, visitor_id=visitor_id)
    if not ok:
        return jsonify({"error": "해당 제보를 찾을 수 없습니다."}), 404
    resp = make_response(jsonify({"ok": True}))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me/reports/<int:report_id>", methods=["PATCH"])
def api_me_reports_update(report_id: int):
    user, _ = _get_current_user(with_touch=False)
    if user is not None:
        user_id = int(user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()
    payload = request.get_json(silent=True) or {}
    arrival_time = str(payload.get("arrival_time") or "").strip()
    if not arrival_time:
        return jsonify({"error": "arrival_time이 필요합니다."}), 400
    try:
        item = user_store.update_arrival_report_time(
            user_id,
            report_id,
            arrival_time,
            visitor_id=visitor_id,
            max_age_sec=(0 if _is_dev_request() else _REPORT_EDIT_WINDOW_SEC),
            allowed_past_sec=(None if _is_dev_request() else _REPORT_ALLOWED_PAST_SEC),
            allowed_future_sec=(None if _is_dev_request() else _REPORT_ALLOWED_FUTURE_SEC),
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if item is None:
        return jsonify({"error": "해당 제보를 찾을 수 없습니다."}), 404
    nickname_map = {}
    if item.get("user_id") is not None:
        nickname_map = user_store.get_active_nickname_map_by_user_ids([int(item["user_id"])])
    like_state_map = user_store.get_arrival_report_like_state(
        [int(item["id"])],
        user_id=user_id,
        visitor_id=visitor_id,
    )
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "report": _serialize_arrival_report_item(
                    item,
                    nickname_map=nickname_map,
                    like_state_map=like_state_map,
                    viewer_user_id=user_id,
                    viewer_visitor_id=visitor_id,
                ),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/me", methods=["DELETE"])
def api_me_delete():
    user, token_hash, auth_error = _auth_required()
    if auth_error is not None:
        return auth_error
    deleted = user_store.delete_user_and_enqueue_cleanup(
        int(user["id"]),
        deadline_hours=AUTH_GITHUB_CLEANUP_DEADLINE_HOURS,
    )
    if deleted is None:
        return jsonify({"error": "이미 탈퇴된 사용자입니다."}), 404
    try:
        _refresh_auth_business_metrics()
    except Exception:
        app.logger.exception("Failed to refresh auth metrics after account deletion")
    if token_hash:
        user_store.delete_session(token_hash)
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "cleanup": {
                    "github_label_cleanup": True,
                    "deadline_at": deleted.get("deadline_at"),
                },
            }
        )
    )
    _clear_auth_session_cookie(resp)
    return resp


@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"documents": []})
    documents = search_places(q)
    user, _ = _get_current_user(with_touch=False)
    _record_place_search_history(q, user)
    return jsonify({"documents": documents})


@app.route("/api/sites")
def api_sites():
    return jsonify({"sites": get_sites(), "db_updated_at": get_db_updated_at()})


@app.route("/api/shuttle/day-types")
def api_shuttle_day_types():
    """사업장별로 실제 출퇴근 노선이 존재하는 day_type 목록 반환."""
    site_id = request.args.get("site_id", default="0000011")
    day_types = get_available_day_types(site_id)
    return jsonify({"site_id": site_id, "day_types": day_types})


@app.route("/api/shuttle/endpoint-options")
def api_shuttle_endpoint_options():
    """
    사업장/요일별 endpoint 선택 옵션 반환.
    - depart_terminus_options: 출근(commute_in) 종착지 목록
    - arrive_start_options: 퇴근(commute_out) 출발지 목록
    """
    site_id = request.args.get("site_id", default="0000011")
    day_type = request.args.get("day_type", default="weekday")
    try:
        depart_options = get_endpoint_options(site_id, day_type, "depart")
        arrive_options = get_endpoint_options(site_id, day_type, "arrive")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(
        {
            "site_id": site_id,
            "day_type": day_type,
            "depart_terminus_options": depart_options,
            "arrive_start_options": arrive_options,
        }
    )


@app.route("/api/shuttle/endpoint-options/search")
def api_shuttle_endpoint_options_search():
    """
    route_keyword(노선명 부분 일치) 기준으로 endpoint 옵션을 필터링해 반환한다.
    """
    site_id = request.args.get("site_id", default="0000011")
    day_type = request.args.get("day_type", default="weekday")
    direction = request.args.get("direction", default="depart")
    route_keyword = request.args.get("keyword", default="", type=str)
    if direction not in {"depart", "arrive"}:
        return jsonify({"error": "direction must be 'depart' or 'arrive'"}), 400

    try:
        options = search_endpoint_options(site_id, day_type, direction, route_keyword)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    return jsonify(
        {
            "site_id": site_id,
            "day_type": day_type,
            "direction": direction,
            "keyword": route_keyword,
            "options": options,
        }
    )


@app.route("/api/reports/candidates")
def api_report_candidates():
    report_guard = _mobile_report_guard()
    if report_guard is not None:
        return report_guard
    site_id = request.args.get("site_id", default=_DEFAULT_SITE_ID).strip() or _DEFAULT_SITE_ID
    day_type = request.args.get("day_type", default="weekday").strip() or "weekday"
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    now_time = request.args.get("now_time", default="", type=str).strip()
    if lat is None or lng is None or not now_time:
        return jsonify({"error": "site_id, day_type, lat, lng, now_time은 필수입니다."}), 400
    if not (math.isfinite(lat) and math.isfinite(lng)):
        return jsonify({"error": "lat, lng는 유효한 숫자여야 합니다."}), 400
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()
    data = get_report_candidate_options(
        site_id=site_id,
        day_type=day_type,
        lat=float(lat),
        lon=float(lng),
        current_time=now_time,
        primary_radius_m=_REPORT_PRIMARY_RADIUS_M,
        fallback_radius_m=_REPORT_FALLBACK_RADIUS_M,
        max_candidates=50,
    )
    candidates = list(data.get("candidates") or [])
    route_ids = []
    route_uuids: list[str] = []
    for item in candidates:
        try:
            route_id = int(item.get("route_id"))
        except (TypeError, ValueError):
            route_id = None
        if route_id is not None and route_id not in route_ids:
            route_ids.append(route_id)
        route_uuid = str(item.get("route_uuid") or "").strip()
        if route_uuid and route_uuid not in route_uuids:
            route_uuids.append(route_uuid)
    preference = user_store.get_recent_arrival_report_hint(
        user_id=user_id,
        visitor_id=visitor_id,
        site_id=site_id,
        day_type=day_type,
        route_ids=route_ids,
        route_uuids=route_uuids,
        within_hours=4,
    )
    history_applied = False
    if preference:
        preferred_route_id = int(preference["route_id"])
        preferred_route_uuid = str(preference.get("route_uuid") or "").strip()
        preferred_time = str(preference.get("departure_time") or "").strip()
        prioritized: list[dict] = []
        remainder: list[dict] = []
        for item in candidates:
            try:
                route_id = int(item.get("route_id"))
            except (TypeError, ValueError):
                route_id = -1
            item_route_uuid = str(item.get("route_uuid") or "").strip()
            route_matched = False
            if preferred_route_uuid and item_route_uuid:
                route_matched = item_route_uuid == preferred_route_uuid
            elif route_id > 0:
                route_matched = route_id == preferred_route_id
            if route_matched:
                if preferred_time and preferred_time in (item.get("departure_times") or []):
                    item["selected_departure_time"] = preferred_time
                item["preferred_from_history"] = True
                prioritized.append(item)
                history_applied = True
            else:
                remainder.append(item)
        if prioritized:
            candidates = prioritized + remainder
    data["history_applied"] = history_applied
    data["candidates"] = candidates[:3]
    resp = make_response(jsonify(data))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports/nearby-stops")
def api_report_nearby_stops():
    report_guard = _mobile_report_guard()
    if report_guard is not None:
        return report_guard
    site_id = request.args.get("site_id", default=_DEFAULT_SITE_ID).strip() or _DEFAULT_SITE_ID
    day_type = request.args.get("day_type", default="", type=str).strip() or None
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    if lat is None or lng is None:
        return jsonify({"error": "site_id, lat, lng는 필수입니다."}), 400
    if not (math.isfinite(lat) and math.isfinite(lng)):
        return jsonify({"error": "lat, lng는 유효한 숫자여야 합니다."}), 400
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()
    data = get_report_nearby_stops(
        site_id=site_id,
        day_type=day_type,
        lat=float(lat),
        lon=float(lng),
        primary_radius_m=_REPORT_PRIMARY_RADIUS_M,
        fallback_radius_m=_REPORT_FALLBACK_RADIUS_M,
    )
    preference = user_store.get_recent_arrival_report_hint(
        user_id=user_id,
        visitor_id=visitor_id,
        site_id=site_id,
        day_type=day_type,
        within_hours=4,
    )
    data["recent_preference"] = preference
    data["requested_day_type"] = str(day_type or "")
    preferred_day_type = ""
    if preference:
        preferred_day_type = str(preference.get("day_type") or "").strip()
    preferred_stop_index = find_recent_report_stop_index_by_route(
        site_id,
        preferred_day_type or day_type,
        data.get("stops") or [],
        preference.get("route_id") if preference else None,
        preference.get("route_uuid") if preference else None,
    )
    if preferred_stop_index >= 0:
        data["preferred_stop_index"] = preferred_stop_index
    resp = make_response(jsonify(data))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports/candidate-routes")
def api_report_candidate_routes():
    report_guard = _mobile_report_guard()
    if report_guard is not None:
        return report_guard
    site_id = request.args.get("site_id", default=_DEFAULT_SITE_ID).strip() or _DEFAULT_SITE_ID
    day_type = request.args.get("day_type", default="", type=str).strip() or None
    now_time = request.args.get("now_time", default="", type=str).strip()
    stop_ids_param = request.args.get("stop_ids", default="", type=str).strip()
    stop_ids: list[int] = []
    if stop_ids_param:
        for raw in stop_ids_param.split(","):
            raw_value = str(raw or "").strip()
            if not raw_value:
                continue
            try:
                stop_value = int(raw_value)
            except (TypeError, ValueError):
                return jsonify({"error": "stop_ids는 정수 목록이어야 합니다."}), 400
            if stop_value not in stop_ids:
                stop_ids.append(stop_value)
    else:
        stop_id = request.args.get("stop_id", type=int)
        if stop_id is not None:
            stop_ids.append(int(stop_id))
    if not stop_ids or not now_time:
        return jsonify({"error": "site_id, stop_id(or stop_ids), now_time은 필수입니다."}), 400

    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()

    data = get_report_route_candidates_for_stop(
        site_id=site_id,
        day_type=day_type,
        stop_id=stop_ids,
        current_time=now_time,
        primary_past_minutes=180,
        primary_future_minutes=5,
        fallback_windows=(),
        max_candidates=50,
    )
    candidates = list(data.get("candidates") or [])
    route_ids: list[int] = []
    route_uuids: list[str] = []
    for item in candidates:
        try:
            route_id = int(item.get("route_id"))
        except (TypeError, ValueError):
            route_id = None
        if route_id is not None and route_id not in route_ids:
            route_ids.append(route_id)
        route_uuid = str(item.get("route_uuid") or "").strip()
        if route_uuid and route_uuid not in route_uuids:
            route_uuids.append(route_uuid)

    preference = user_store.get_recent_arrival_report_hint(
        user_id=user_id,
        visitor_id=visitor_id,
        site_id=site_id,
        day_type=day_type,
        route_ids=route_ids,
        route_uuids=route_uuids,
        within_hours=4,
    )
    history_applied = False
    if preference:
        preferred_route_id = int(preference["route_id"])
        preferred_route_uuid = str(preference.get("route_uuid") or "").strip()
        preferred_time = str(preference.get("departure_time") or "").strip()
        preferred_day_type = str(preference.get("day_type") or "").strip()
        prioritized: list[dict] = []
        remainder: list[dict] = []
        for item in candidates:
            try:
                route_id = int(item.get("route_id"))
            except (TypeError, ValueError):
                route_id = -1
            item_route_uuid = str(item.get("route_uuid") or "").strip()
            route_matched = False
            if preferred_route_uuid and item_route_uuid:
                route_matched = item_route_uuid == preferred_route_uuid
            elif route_id > 0:
                route_matched = route_id == preferred_route_id
            if route_matched:
                if (
                    day_type
                    and preferred_time
                    and preferred_time in (item.get("departure_times") or [])
                ):
                    item["selected_departure_time"] = preferred_time
                elif (not day_type) and preferred_day_type:
                    option_values = [str(value or "").strip() for value in list(item.get("day_type_options") or [])]
                    if preferred_day_type in option_values:
                        item["selected_day_type"] = preferred_day_type
                item["preferred_from_history"] = True
                prioritized.append(item)
                history_applied = True
            else:
                remainder.append(item)
        if prioritized:
            candidates = prioritized + remainder

    data["history_applied"] = history_applied
    data["recent_preference"] = preference
    data["requested_day_type"] = str(day_type or "")
    data["candidates"] = candidates
    resp = make_response(jsonify(data))
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports", methods=["POST"])
def api_submit_report():
    report_guard = _mobile_report_guard()
    if report_guard is not None:
        return report_guard
    payload = request.get_json(silent=True) or {}
    site_id = str(payload.get("site_id") or "").strip() or _DEFAULT_SITE_ID
    day_type = str(payload.get("day_type") or "").strip() or "weekday"
    departure_time = str(payload.get("departure_time") or "").strip()
    client_reported_at = _parse_client_reported_at(payload.get("client_reported_at"))
    route_uuid = str(payload.get("route_uuid") or "").strip() or None
    stop_uuid = str(payload.get("stop_uuid") or "").strip() or None
    route_id: int | None = None
    stop_id: int | None = None
    try:
        if payload.get("route_id") is not None:
            route_id = int(payload.get("route_id"))
        if payload.get("stop_id") is not None:
            stop_id = int(payload.get("stop_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "route_id, stop_id는 정수여야 합니다."}), 400
    if not departure_time or client_reported_at is None:
        return jsonify({"error": "departure_time, client_reported_at은 필수입니다."}), 400
    if (route_id is None or stop_id is None) and (not route_uuid or not stop_uuid):
        return jsonify({"error": "route_id/stop_id 또는 route_uuid/stop_uuid가 필요합니다."}), 400

    tuple_meta = None
    if route_uuid and stop_uuid:
        tuple_meta = get_report_tuple_meta_by_uuid(
            site_id=site_id,
            day_type=day_type,
            route_uuid=route_uuid,
            departure_time=departure_time,
            stop_uuid=stop_uuid,
        )
    if tuple_meta is None and route_id is not None and stop_id is not None:
        tuple_meta = get_report_tuple_meta(
            site_id=site_id,
            day_type=day_type,
            route_id=route_id,
            departure_time=departure_time,
            stop_id=stop_id,
        )
    if not tuple_meta:
        return jsonify({"error": "유효한 제보 대상이 아닙니다."}), 400
    route_id = int(tuple_meta["route_id"])
    stop_id = int(tuple_meta["stop_id"])
    route_uuid = str(tuple_meta.get("route_uuid") or "").strip() or None
    stop_uuid = str(tuple_meta.get("stop_uuid") or "").strip() or None

    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()

    service_date, reported_clock_minutes = _service_date_from_client_reported_at(client_reported_at)
    server_received_dt = _utc_now()
    if (not _is_dev_request()) and (not _is_client_reported_at_within_allowed_window(client_reported_at, server_received_dt)):
        return jsonify({"error": "정류장 도착 시간은 현재 시각 기준 10분 전부터 3분 후까지만 제보할 수 있습니다."}), 400
    client_server_delta_sec = int(abs((server_received_dt - client_reported_at).total_seconds()))
    wait_sec = user_store.get_arrival_report_cooldown_wait_seconds(
        user_id=user_id,
        visitor_id=visitor_id,
        route_id=route_id,
        route_uuid=route_uuid,
        departure_time=departure_time,
        stop_id=stop_id,
        stop_uuid=stop_uuid,
        service_date=service_date,
        cooldown_sec=_REPORT_COOLDOWN_SEC,
    )
    if wait_sec > 0:
        if user_id is not None:
            return (
                jsonify(
                    {
                        "error": "같은 노선/출발시각/정류장 제보는 3분 안에 반복할 수 없습니다. 제보 기록 메뉴에서 제보 시각을 수정할 수 있습니다."
                    }
                ),
                429,
            )
        return jsonify({"error": "같은 노선/출발시각/정류장 제보는 3분 안에 반복할 수 없습니다."}), 429

    time_valid = True

    item = user_store.add_arrival_report(
        user_id=user_id,
        visitor_id=visitor_id,
        site_id=site_id,
        day_type=day_type,
        direction=str(tuple_meta["direction"]),
        route_id=route_id,
        route_uuid=route_uuid,
        route_name=str(tuple_meta["route_name"]),
        route_type=str(tuple_meta["route_type"]),
        departure_time=departure_time,
        stop_id=stop_id,
        stop_uuid=stop_uuid,
        stop_name=str(tuple_meta["stop_name"]),
        stop_sequence=tuple_meta.get("stop_sequence"),
        stop_lat=tuple_meta.get("stop_lat"),
        stop_lng=tuple_meta.get("stop_lng"),
        client_reported_at=user_store.utc_iso(client_reported_at),
        server_received_at=user_store.utc_iso(server_received_dt),
        service_date=service_date,
        reported_clock_minutes=reported_clock_minutes,
        client_server_delta_sec=client_server_delta_sec,
        time_valid=time_valid,
    )
    nickname_map = {}
    if item.get("user_id") is not None:
        nickname_map = user_store.get_active_nickname_map_by_user_ids([int(item["user_id"])])
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "report": _serialize_arrival_report_item(item, nickname_map=nickname_map),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports/eta-detail")
def api_report_eta_detail():
    route_id = request.args.get("route_id", type=int)
    stop_id = request.args.get("stop_id", type=int)
    route_uuid = request.args.get("route_uuid", default="", type=str).strip() or None
    stop_uuid = request.args.get("stop_uuid", default="", type=str).strip() or None
    day_type = request.args.get("day_type", default="weekday").strip() or "weekday"
    departure_time = request.args.get("departure_time", default="", type=str).strip()
    has_id_pair = route_id is not None and stop_id is not None
    has_uuid_pair = bool(route_uuid and stop_uuid)
    if (not has_id_pair and not has_uuid_pair) or not departure_time:
        return jsonify({"error": "route_id/stop_id 또는 route_uuid/stop_uuid와 departure_time은 필수입니다."}), 400
    items = user_store.list_arrival_reports_for_tuple(
        route_id=int(route_id or 0),
        route_uuid=route_uuid,
        day_type=day_type,
        departure_time=departure_time,
        stop_id=int(stop_id or 0),
        stop_uuid=stop_uuid,
        limit=200,
    )
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        viewer_user_id = int(auth_user["id"])
        viewer_visitor_id = None
    else:
        viewer_user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()
    user_ids = [
        int(item["user_id"])
        for item in items
        if item.get("user_id") is not None
    ]
    like_state_map = user_store.get_arrival_report_like_state(
        [int(item["id"]) for item in items if item.get("id") is not None],
        user_id=viewer_user_id,
        visitor_id=viewer_visitor_id,
    )
    nickname_map = user_store.get_active_nickname_map_by_user_ids(user_ids)
    resp = make_response(
        jsonify(
            {
                "items": [
                    _serialize_arrival_report_item(
                        item,
                        nickname_map=nickname_map,
                        like_state_map=like_state_map,
                        viewer_user_id=viewer_user_id,
                        viewer_visitor_id=viewer_visitor_id,
                    )
                    for item in items
                ]
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports/<int:report_id>/like", methods=["POST"])
def api_report_like_toggle(report_id: int):
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()
    try:
        result = user_store.toggle_arrival_report_like(
            report_id,
            user_id=user_id,
            visitor_id=visitor_id,
        )
    except ValueError as exc:
        code = str(exc)
        if code == "self_like_not_allowed":
            return jsonify({"error": "본인 제보에는 좋아요를 누를 수 없습니다."}), 400
        if code == "like_not_allowed_for_previous":
            return jsonify({"error": "현재 ETA에 반영되지 않는 이전 제보에는 좋아요를 누를 수 없습니다."}), 400
        return jsonify({"error": "좋아요를 처리하지 못했습니다."}), 400
    if result is None:
        return jsonify({"error": "해당 제보를 찾을 수 없습니다."}), 404
    resp = make_response(
        jsonify(
            {
                "ok": True,
                "report_id": int(result["report_id"]),
                "liked": bool(result["liked"]),
                "like_count": int(result["like_count"]),
            }
        )
    )
    return _attach_history_visitor_cookie_if_needed(resp)


@app.route("/api/reports/leaderboard")
def api_report_leaderboard():
    window = request.args.get("window", default="all", type=str).strip().lower() or "all"
    if window not in {"all", "7d", "30d"}:
        return jsonify({"error": "window는 all|7d|30d만 지원합니다."}), 400
    return jsonify({"items": user_store.list_arrival_leaderboard(window=window, limit=100)})


@app.route("/api/shuttle/walk-path")
def api_shuttle_walk_path():
    from_lat = request.args.get("from_lat", type=float)
    from_lng = request.args.get("from_lng", type=float)
    to_lat = request.args.get("to_lat", type=float)
    to_lng = request.args.get("to_lng", type=float)
    if from_lat is None or from_lng is None or to_lat is None or to_lng is None:
        return jsonify({"error": "from_lat, from_lng, to_lat, to_lng required"}), 400
    if not (
        math.isfinite(from_lat)
        and math.isfinite(from_lng)
        and math.isfinite(to_lat)
        and math.isfinite(to_lng)
    ):
        return jsonify({"error": "coordinates must be finite numbers"}), 400

    if not OTP_WALK_ENABLED:
        return jsonify(
            {
                "walk_otp_enabled": False,
                "has_route": False,
                "status": "disabled",
                "encoded_polyline": "",
                "duration_sec": None,
                "distance_m": None,
                "source": "disabled",
            }
        )

    cache_key = _walk_path_cache_key(from_lat, from_lng, to_lat, to_lng)
    cached = _walk_path_cache_get(cache_key)
    if cached is not None:
        cached["source"] = "cache"
        cached["walk_otp_enabled"] = True
        return jsonify(cached)

    try:
        fetched = _fetch_walk_path_from_otp(from_lat, from_lng, to_lat, to_lng)
    except requests.Timeout:
        return jsonify({"error": "otp_walk_timeout"}), 504
    except requests.RequestException as exc:
        return jsonify({"error": f"otp_walk_request_failed: {exc}"}), 502
    except ValueError:
        return jsonify({"error": "otp_walk_invalid_response"}), 502

    cache_payload = {
        "has_route": bool(fetched.get("has_route")),
        "status": str(fetched.get("status") or ""),
        "encoded_polyline": str(fetched.get("encoded_polyline") or ""),
        "duration_sec": fetched.get("duration_sec"),
        "distance_m": fetched.get("distance_m"),
    }
    _walk_path_cache_set(cache_key, cache_payload)
    cache_payload["source"] = "otp"
    cache_payload["walk_otp_enabled"] = True
    return jsonify(cache_payload)


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
        meta = dict(meta or {})
        auth_user, _ = _get_current_user(with_touch=False)
        anon_nickname = ""

        if auth_user is not None:
            meta.pop("user-id", None)
            meta["nickname"] = str(auth_user.get("nickname") or "")
            meta["user-id"] = str(auth_user.get("public_user_id") or "")
        else:
            # 비로그인 사용자는 user-id 라벨을 임의 주입할 수 없다.
            meta.pop("user-id", None)
            anon_nickname = _normalize_issue_label_nickname(meta.get("nickname"))
            if anon_nickname:
                meta["nickname"] = anon_nickname
            else:
                meta.pop("nickname", None)

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

        label_meta = dict(meta or {})
        label_meta.pop("nickname", None)
        merged_labels = []
        for label in labels + _labels_from_meta(label_meta):
            if label not in merged_labels:
                merged_labels.append(label)
        if auth_user is not None:
            uid_label = f"user-id:{_compact_meta_label_part(auth_user.get('public_user_id'))}"
            if uid_label and uid_label not in merged_labels:
                merged_labels.append(uid_label)
        elif anon_nickname:
            nickname_label = _build_issue_nickname_label(anon_nickname)
            if nickname_label and nickname_label not in merged_labels:
                merged_labels.append(nickname_label)

        issue_payload = {
            "title": title,
            "body": body,
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
        normalized["like_count"] = 0
        normalized["liked_by_me"] = False
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

    _sync_issue_cache_version()

    state = request.args.get("state", default="all", type=str).strip().lower()
    if state not in {"open", "closed", "all"}:
        return jsonify({"error": "state는 open|closed|all만 허용됩니다."}), 400

    kind = request.args.get("kind", default="all", type=str).strip().lower()
    if kind not in {"all", "issue", "notice"}:
        return jsonify({"error": "kind는 all|issue|notice만 허용됩니다."}), 400

    labels_raw = request.args.get("labels", default="", type=str)
    labels = [s.strip() for s in labels_raw.split(",") if s.strip()]
    q = request.args.get("q", default="", type=str).strip()
    page = max(1, request.args.get("page", default=1, type=int) or 1)
    per_page = request.args.get("per_page", default=10, type=int) or 10
    per_page = max(1, min(per_page, 100))

    cache_key = f"issues:v6:list:{kind}:{state}:{','.join(labels)}:{q}:{page}:{per_page}"
    cached, is_fresh_cache = _issue_cache_get_allow_stale(cache_key)
    if not is_fresh_cache:
        owner = (GITHUB_REPO_OWNER or "").strip()
        q_lower = q.casefold()
        list_page_size = min(100, max(1, per_page + 1))
        try:
            if kind == "notice" and not owner:
                raw_items: list[dict] = []
                has_next = False
            else:
                list_params: dict[str, object] = {
                    "state": state,
                    "page": page,
                    "per_page": list_page_size,
                    "sort": "created",
                    "direction": "desc",
                }
                if labels:
                    list_params["labels"] = ",".join(labels)
                if kind == "notice" and owner:
                    list_params["creator"] = owner
                raw_items = _github_api_request(
                    "GET",
                    f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues",
                    params=list_params,
                )
                if not isinstance(raw_items, list):
                    raw_items = []
                has_next = len(raw_items) > per_page
                if has_next:
                    raw_items = raw_items[:per_page]
        except GitHubAPIError as exc:
            if cached is None:
                return jsonify({"error": str(exc)}), exc.status_code
            app.logger.warning("Falling back to stale issue cache for %s due to GitHub error: %s", cache_key, exc)
        else:
            nickname_by_public_user_id: dict[str, str] = {}
            try:
                author_user_ids = _collect_issue_author_user_ids(raw_items)
                if author_user_ids:
                    nickname_by_public_user_id = user_store.get_active_nickname_map_by_public_user_ids(author_user_ids)
            except Exception:
                app.logger.exception("Failed to resolve issue author nicknames")

            normalized_issues: list[dict] = []
            for item in raw_items:
                if item.get("pull_request"):
                    continue
                normalized = _normalize_issue(item, nickname_by_public_user_id=nickname_by_public_user_id)
                if kind == "issue" and normalized.get("is_notice"):
                    continue
                if kind == "notice" and not normalized.get("is_notice"):
                    continue
                if kind == "all" and state in {"open", "closed"} and normalized.get("is_notice"):
                    # Backward compatibility: 기존 open/closed 조회에서는 공지를 제외했다.
                    continue
                if q_lower:
                    haystack = (
                        f"{normalized.get('title') or ''}\n"
                        f"{normalized.get('body') or ''}\n"
                        f"{normalized.get('display_name') or ''}\n"
                        f"{normalized.get('user') or ''}"
                    ).casefold()
                    if q_lower not in haystack:
                        continue
                normalized_issues.append(normalized)

            cached = {
                "issues": normalized_issues,
                "pagination": {
                    "page": page,
                    "per_page": per_page,
                    "has_prev": page > 1,
                    "has_next": bool(has_next),
                    "total_count": None,
                },
            }
            _issue_cache_set(cache_key, cached)

    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        viewer_user_id = int(auth_user["id"])
        viewer_visitor_id = None
    else:
        viewer_user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()

    cached_issues = list(cached.get("issues") or [])
    issue_numbers: list[int] = []
    for issue in cached_issues:
        try:
            issue_number = int((issue or {}).get("number") or 0)
        except (TypeError, ValueError):
            issue_number = 0
        if issue_number > 0:
            issue_numbers.append(issue_number)
    like_state_map = user_store.get_issue_like_state(
        issue_numbers,
        user_id=viewer_user_id,
        visitor_id=viewer_visitor_id,
    )

    response_payload = {
        "issues": _with_issue_like_state(cached_issues, like_state_map=like_state_map),
        "pagination": dict(cached.get("pagination") or {}),
    }
    response = make_response(jsonify(response_payload))
    if auth_user is None:
        return _attach_history_visitor_cookie_if_needed(response)
    return response


@app.route("/api/issues/<int:issue_number>")
def api_issue_detail(issue_number: int):
    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    _sync_issue_cache_version()

    cache_key = f"issues:detail:{issue_number}"
    cached, is_fresh_cache = _issue_cache_get_allow_stale(cache_key)
    if not is_fresh_cache:
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
            if cached is None:
                return jsonify({"error": str(exc)}), exc.status_code
            app.logger.warning("Falling back to stale issue detail cache for %s due to GitHub error: %s", cache_key, exc)
        else:
            if not isinstance(comments, list):
                comments = []

            issue_nickname_map: dict[str, str] = {}
            try:
                issue_author_user_ids = _collect_issue_author_user_ids([issue])
                if issue_author_user_ids:
                    issue_nickname_map = user_store.get_active_nickname_map_by_public_user_ids(issue_author_user_ids)
            except Exception:
                app.logger.exception("Failed to resolve issue author nickname in detail")

            cached = {
                "issue": _normalize_issue(issue, nickname_by_public_user_id=issue_nickname_map),
                "comments": [_normalize_comment(c) for c in comments],
            }
            _issue_cache_set(cache_key, cached)

    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        viewer_user_id = int(auth_user["id"])
        viewer_visitor_id = None
    else:
        viewer_user_id = None
        viewer_visitor_id = _ensure_history_visitor_id()

    cached_issue = dict(cached.get("issue") or {})
    try:
        normalized_issue_number = int(cached_issue.get("number") or issue_number)
    except (TypeError, ValueError):
        normalized_issue_number = int(issue_number)
    like_state_map = user_store.get_issue_like_state(
        [normalized_issue_number],
        user_id=viewer_user_id,
        visitor_id=viewer_visitor_id,
    )
    issue_with_like = _with_issue_like_state([cached_issue], like_state_map=like_state_map)
    response_payload = {
        "issue": issue_with_like[0] if issue_with_like else cached_issue,
        "comments": list(cached.get("comments") or []),
    }
    response = make_response(jsonify(response_payload))
    if auth_user is None:
        return _attach_history_visitor_cookie_if_needed(response)
    return response


@app.route("/api/notices/hint")
def api_notice_hint():
    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    try:
        notices = _load_notice_head_cache(per_page=100, stale_while_revalidate=True)
    except GitHubAPIError as exc:
        return jsonify({"error": str(exc)}), exc.status_code

    auth_user, user_id, visitor_id = _resolve_notice_actor()
    try:
        last_ack_notice_number = int(
            user_store.get_notice_ack_number(user_id=user_id, visitor_id=visitor_id) or 0
        )
    except Exception:
        app.logger.exception("Failed to read notice ack state")
        last_ack_notice_number = 0

    latest_notice_number = 0
    latest_notice_title = ""
    unseen_count = 0
    latest_unseen_notice_number = 0
    latest_unseen_notice_title = ""
    for notice in notices:
        try:
            notice_number = int(notice.get("number") or 0)
        except (TypeError, ValueError):
            continue
        if notice_number <= 0:
            continue
        if latest_notice_number <= 0:
            latest_notice_number = notice_number
            latest_notice_title = str(notice.get("title") or "").strip()
        if notice_number > last_ack_notice_number:
            unseen_count += 1
            if latest_unseen_notice_number <= 0:
                latest_unseen_notice_number = notice_number
                latest_unseen_notice_title = str(notice.get("title") or "").strip()

    response = make_response(
        jsonify(
            {
                "unseen_count": int(unseen_count),
                "latest_notice_number": int(latest_notice_number),
                "latest_notice_title": latest_notice_title,
                "latest_unseen_notice_number": int(latest_unseen_notice_number),
                "latest_unseen_notice_title": latest_unseen_notice_title,
                "last_ack_notice_number": int(max(0, last_ack_notice_number)),
            }
        )
    )
    if auth_user is None:
        return _attach_history_visitor_cookie_if_needed(response)
    return response


@app.route("/api/notices/ack", methods=["POST"])
def api_notice_ack():
    auth_user, user_id, visitor_id = _resolve_notice_actor()
    payload = request.get_json(silent=True) or {}
    raw_notice_number = payload.get("notice_number")
    ack_notice_number = None
    if raw_notice_number is not None and raw_notice_number != "":
        try:
            ack_notice_number = max(0, int(raw_notice_number))
        except (TypeError, ValueError):
            return jsonify({"error": "notice_number는 0 이상의 정수여야 합니다."}), 400

    if ack_notice_number is None:
        try:
            _ensure_github_issue_config()
            notices = _load_notice_head_cache(per_page=100)
        except GitHubConfigError:
            notices = []
        except GitHubAPIError as exc:
            return jsonify({"error": str(exc)}), exc.status_code
        if notices:
            try:
                ack_notice_number = max(0, int(notices[0].get("number") or 0))
            except (TypeError, ValueError):
                ack_notice_number = 0
        else:
            ack_notice_number = 0

    try:
        saved_ack_number = int(
            user_store.ack_notice_number(
                int(ack_notice_number),
                user_id=user_id,
                visitor_id=visitor_id,
            )
        )
    except ValueError as exc:
        if str(exc) == "ack_actor_required":
            return jsonify({"error": "공지 확인 사용자 식별에 실패했습니다."}), 400
        return jsonify({"error": "공지 확인 상태 저장에 실패했습니다."}), 400

    response = make_response(
        jsonify(
            {
                "ok": True,
                "ack_notice_number": int(saved_ack_number),
            }
        )
    )
    if auth_user is None:
        return _attach_history_visitor_cookie_if_needed(response)
    return response


@app.route("/api/issues/<int:issue_number>/comments", methods=["POST"])
def api_issue_comment_create(issue_number: int):
    try:
        _ensure_github_issue_config()
    except GitHubConfigError as exc:
        return jsonify({"error": str(exc)}), 503

    payload = request.get_json(silent=True) or {}
    body = str(payload.get("body", "")).strip()
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    meta = dict(meta or {})
    auth_user, _ = _get_current_user(with_touch=False)

    if auth_user is not None:
        meta["nickname"] = str(auth_user.get("nickname") or "").strip()
        meta["user-id"] = str(auth_user.get("public_user_id") or "").strip()
    else:
        meta.pop("user-id", None)
        meta_nickname = str(meta.get("nickname") or "").strip()
        if meta_nickname:
            meta["nickname"] = meta_nickname
        else:
            meta.pop("nickname", None)

    nickname = str(meta.get("nickname") or "").strip()
    if nickname:
        meta["nickname"] = nickname[:40]
    else:
        meta.pop("nickname", None)

    if not body:
        return jsonify({"error": "댓글 내용(body)은 필수입니다."}), 400

    comment_body = _compose_issue_body(body, meta if meta else None)

    try:
        comment = _github_api_request(
            "POST",
            f"/repos/{GITHUB_REPO_OWNER}/{GITHUB_REPO_NAME}/issues/{issue_number}/comments",
            json_payload={"body": comment_body},
        )
    except GitHubAPIError as exc:
        return jsonify({"error": str(exc)}), exc.status_code

    _invalidate_issue_cache()
    return jsonify({"comment": _normalize_comment(comment)}), 201


@app.route("/api/issues/<int:issue_number>/like", methods=["POST"])
def api_issue_like_toggle(issue_number: int):
    auth_user, _ = _get_current_user(with_touch=False)
    if auth_user is not None:
        user_id = int(auth_user["id"])
        visitor_id = None
    else:
        user_id = None
        visitor_id = _ensure_history_visitor_id()

    try:
        result = user_store.toggle_issue_like(
            int(issue_number),
            user_id=user_id,
            visitor_id=visitor_id,
        )
    except ValueError as exc:
        code = str(exc)
        if code == "invalid_issue_number":
            return jsonify({"error": "유효하지 않은 이슈 번호입니다."}), 400
        if code == "like_actor_required":
            return jsonify({"error": "좋아요 사용자 식별에 실패했습니다."}), 400
        return jsonify({"error": "좋아요를 처리하지 못했습니다."}), 400

    response = make_response(
        jsonify(
            {
                "ok": True,
                "issue_number": int(result["issue_number"]),
                "liked": bool(result["liked"]),
                "like_count": int(result["like_count"]),
            }
        )
    )
    if auth_user is None:
        return _attach_history_visitor_cookie_if_needed(response)
    return response


@app.route("/api/github/image-proxy")
def api_github_image_proxy():
    raw_url = str(request.args.get("url", "") or "").strip()
    if not raw_url:
        return jsonify({"error": "url 쿼리 파라미터가 필요합니다."}), 400
    if len(raw_url) > 2048:
        return jsonify({"error": "url 길이가 너무 깁니다."}), 400

    parsed = urlparse(raw_url)
    scheme = str(parsed.scheme or "").lower()
    hostname = str(parsed.hostname or "").strip().lower().rstrip(".")
    if scheme not in {"http", "https"}:
        return jsonify({"error": "http/https URL만 허용됩니다."}), 400
    if not _is_allowed_github_image_host(hostname):
        return jsonify({"error": "허용되지 않은 이미지 호스트입니다."}), 403

    target_url = _normalize_github_image_url(raw_url)
    max_image_bytes = 20 * 1024 * 1024
    timeout_sec = 20
    method_error = "GitHub 이미지 요청에 실패했습니다."

    forward_headers: dict[str, str] = {
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "User-Agent": "shuttle-go-image-proxy/1.0",
    }
    if_none_match = str(request.headers.get("If-None-Match", "") or "").strip()
    if if_none_match:
        forward_headers["If-None-Match"] = if_none_match
    if_modified_since = str(request.headers.get("If-Modified-Since", "") or "").strip()
    if if_modified_since:
        forward_headers["If-Modified-Since"] = if_modified_since

    should_try_auth = bool(
        GITHUB_APP_ID and GITHUB_INSTALLATION_ID and GITHUB_REPO_OWNER and GITHUB_REPO_NAME and jwt is not None
    )

    def _fetch_image(force_refresh: bool = False) -> requests.Response:
        req_headers = dict(forward_headers)
        if should_try_auth:
            try:
                token = _get_github_installation_token(force_refresh=force_refresh)
            except (GitHubAPIError, GitHubConfigError):
                token = ""
            if token:
                req_headers["Authorization"] = f"Bearer {token}"
        return requests.get(
            target_url,
            headers=req_headers,
            timeout=timeout_sec,
            stream=True,
            allow_redirects=True,
        )

    try:
        resp = _fetch_image(force_refresh=False)
    except requests.RequestException as exc:
        return jsonify({"error": f"{method_error}: {exc}"}), 502

    if resp.status_code == 401 and should_try_auth:
        resp.close()
        try:
            resp = _fetch_image(force_refresh=True)
        except requests.RequestException as exc:
            return jsonify({"error": f"{method_error}: {exc}"}), 502

    if resp.status_code == 304:
        proxy_resp = make_response("", 304)
        for header_name in ("Cache-Control", "ETag", "Last-Modified", "Expires"):
            header_value = str(resp.headers.get(header_name, "") or "").strip()
            if header_value:
                proxy_resp.headers[header_name] = header_value
        resp.close()
        return proxy_resp

    if resp.status_code >= 400:
        status_code = int(resp.status_code)
        resp.close()
        return jsonify({"error": f"GitHub 이미지 응답 실패({status_code})"}), status_code

    final_url = str(resp.url or "").strip()
    final_host = str(urlparse(final_url).hostname or "").strip().lower().rstrip(".")
    if not _is_allowed_github_image_host(final_host):
        resp.close()
        return jsonify({"error": "GitHub 이미지 리다이렉트 대상이 허용되지 않았습니다."}), 502

    content_type = str(resp.headers.get("Content-Type", "") or "").strip()
    lowered_type = content_type.lower()
    if not lowered_type.startswith("image/"):
        resp.close()
        return jsonify({"error": "이미지 콘텐츠 타입이 아닙니다."}), 502

    content_length_raw = str(resp.headers.get("Content-Length", "") or "").strip()
    if content_length_raw.isdigit() and int(content_length_raw) > max_image_bytes:
        resp.close()
        return jsonify({"error": "이미지 파일이 너무 큽니다."}), 413

    data_chunks: list[bytes] = []
    total_size = 0
    try:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total_size += len(chunk)
            if total_size > max_image_bytes:
                resp.close()
                return jsonify({"error": "이미지 파일이 너무 큽니다."}), 413
            data_chunks.append(chunk)
    except requests.RequestException as exc:
        resp.close()
        return jsonify({"error": f"이미지 다운로드 중 오류가 발생했습니다: {exc}"}), 502
    finally:
        resp.close()

    response = Response(b"".join(data_chunks), content_type=content_type or "application/octet-stream")
    response.headers["Cache-Control"] = str(resp.headers.get("Cache-Control", "public, max-age=3600"))
    for header_name in ("ETag", "Last-Modified", "Expires"):
        header_value = str(resp.headers.get(header_name, "") or "").strip()
        if header_value:
            response.headers[header_name] = header_value
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


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
    """출근 노선 후보 최대 3개."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    exclude_raw = request.args.get("exclude_route_ids", "").strip()
    use_endpoint_filter = _is_truthy_param(request.args.get("use_endpoint_filter"))
    selected_endpoints = _parse_selected_endpoints()
    auth_user, _ = _get_current_user(with_touch=False)
    max_distance_raw = request.args.get("max_distance_km")
    search_mode = _parse_route_search_mode(request.args.get("search_mode"))
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    try:
        max_distance_km = _parse_max_distance_km(max_distance_raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    max_distance_m = max_distance_km * 1000.0

    exclude_ids = []
    if exclude_raw:
        exclude_ids = [int(x) for x in exclude_raw.split(",") if x.strip().isdigit()]

    def _record_route_search() -> None:
        _record_route_search_history(
            auth_user=auth_user,
            site_id=site_id,
            day_type=day_type,
            direction="depart",
            lat=float(lat),
            lng=float(lng),
            selected_endpoints=selected_endpoints,
            place_name=place_name,
        )

    include_route_ids: set[int] | None = None
    selected_endpoint_components: set[str] | None = None
    if use_endpoint_filter or selected_endpoints is not None:
        try:
            include_route_ids = get_endpoint_route_ids(
                site_id=site_id,
                day_type=day_type,
                direction="depart",
                selected_endpoints=selected_endpoints,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        if not include_route_ids:
            _record_route_search()
            return jsonify({"options": [], "walk_otp_enabled": OTP_WALK_ENABLED})
        selected_endpoint_components = _build_selected_endpoint_component_set(
            site_id=site_id,
            day_type=day_type,
            direction="depart",
            selected_endpoints=selected_endpoints,
        )

    SHUTTLE_SEARCH_COUNT.labels(direction="depart").inc()
    _record_route_search_source_metric("depart")

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_in",
        lat=lat,
        lon=lng,
        day_type=day_type,
        # 후처리(탑승/하차 동일 정류장 제외, 시간 필터) 이후에도
        # 실제 반환 3개를 채울 수 있도록 후보를 넉넉히 가져온다.
        max_routes=_OPTIONS_CANDIDATE_LIMIT,
        exclude_route_ids=exclude_ids or None,
        include_route_ids=include_route_ids,
    )
    if not results:
        if include_route_ids is not None:
            _record_route_search()
            return jsonify({"options": [], "walk_otp_enabled": OTP_WALK_ENABLED})
        _record_route_search()
        return jsonify({"error": "해당 사업장의 출근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    all_last_times = []  # 시간 필터로 빈 결과 시 마지막 출발시간 추적용
    for r in results:
        ns = {
            "name": r["nearest_stop"]["name"],
            "lat": r["nearest_stop"]["lat"],
            "lon": r["nearest_stop"]["lon"],
            "stop_id": r["nearest_stop"].get("stop_id"),
        }
        route_stops = r["route_stops"]
        terminus = _choose_endpoint_display_stop(
            route_stops,
            direction="depart",
            selected_components=selected_endpoint_components,
        )
        terminus_seq = _find_route_stop_sequence(
            route_stops,
            stop_id=terminus.get("stop_id") if terminus else None,
            stop_name=terminus.get("stop_name") if terminus else None,
        )
        nearest_seq = _find_route_stop_sequence(
            route_stops,
            stop_id=r["nearest_stop"].get("stop_id"),
            stop_name=ns["name"],
        )
        if nearest_seq is not None:
            ns["sequence"] = nearest_seq

        needs_boarding_adjust = False
        max_boarding_seq: int | None = None
        if terminus and terminus_seq is not None:
            if nearest_seq is None or nearest_seq >= terminus_seq:
                needs_boarding_adjust = True
                max_boarding_seq = terminus_seq - 1
        elif terminus and ns["name"] == terminus["stop_name"]:
            needs_boarding_adjust = True

        # 출근은 항상 탑승 정류장 순번이 종착지보다 앞서도록 보정
        if needs_boarding_adjust:
            exclude_stop_ids: set[int] = set()
            try:
                sid = terminus.get("stop_id")
                if sid is not None:
                    exclude_stop_ids.add(int(sid))
            except (TypeError, ValueError):
                pass
            is_canonical_terminus = bool(route_stops) and terminus is route_stops[-1]
            alt = _nearest_route_stop_for_user(
                route_stops,
                lat,
                lng,
                exclude_last=is_canonical_terminus,
                exclude_stop_ids=exclude_stop_ids or None,
                max_sequence=max_boarding_seq,
            )
            if not alt:
                continue
            ns = alt
            nearest_seq = _find_route_stop_sequence(
                route_stops,
                stop_id=ns.get("stop_id"),
                stop_name=ns.get("name"),
            )
            if nearest_seq is not None:
                ns["sequence"] = nearest_seq

        distance_m = haversine_distance_m(lat, lng, ns["lat"], ns["lon"])
        if distance_m > max_distance_m:
            continue

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
        bus_segment_polylines = _segment_polylines_between_sequences(
            route_stops,
            from_sequence=ns.get("sequence"),
            to_sequence=terminus_seq,
        )

        payload = {
            "positions": positions,
            "message": message,
            "route_name": r["route_name"],
            "route_id": r["route_id"],
            "operator": ", ".join(r["companies"]),
            "nearest_stop_name": ns["name"],
            "nearest_stop_sequence": ns.get("sequence"),
            "terminus_name": terminus["stop_name"] if terminus else "",
            "terminus_sequence": terminus_seq,
            "distance_m": round(distance_m),
            "all_departure_times": r["all_departure_times"],
            "route_stops": route_stops,
            "bus_segment_polylines": bus_segment_polylines,
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)

    options = _sort_route_options_by_search_mode(options, search_mode, time_param)[:_OPTIONS_RESPONSE_LIMIT]
    resp = {"options": options, "walk_otp_enabled": OTP_WALK_ENABLED, "search_mode": search_mode}
    if not options and all_last_times:
        resp["last_departure_time"] = sorted(set(all_last_times))[-1]
    _record_route_search()
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
    _record_route_search_source_metric("depart")

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
        "stop_id": r["nearest_stop"].get("stop_id"),
    }
    route_stops = r["route_stops"]
    terminus = route_stops[-1] if route_stops else None
    terminus_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=terminus.get("stop_id") if terminus else None,
        stop_name=terminus.get("stop_name") if terminus else None,
    )
    nearest_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=r["nearest_stop"].get("stop_id"),
        stop_name=ns["name"],
    )
    if nearest_seq is not None:
        ns["sequence"] = nearest_seq
    needs_boarding_adjust = False
    max_boarding_seq: int | None = None
    if terminus and terminus_seq is not None:
        if nearest_seq is None or nearest_seq >= terminus_seq:
            needs_boarding_adjust = True
            max_boarding_seq = terminus_seq - 1
    elif terminus and ns["name"] == terminus["stop_name"]:
        needs_boarding_adjust = True
    if needs_boarding_adjust:
        exclude_stop_ids: set[int] = set()
        try:
            sid = terminus.get("stop_id") if terminus else None
            if sid is not None:
                exclude_stop_ids.add(int(sid))
        except (TypeError, ValueError):
            pass
        alt = _nearest_route_stop_for_user(
            route_stops,
            lat,
            lng,
            exclude_last=True,
            exclude_stop_ids=exclude_stop_ids or None,
            max_sequence=max_boarding_seq,
        )
        if alt:
            ns = alt
        else:
            return jsonify({"error": "해당 노선에서 탑승 가능한 정류장을 찾을 수 없습니다."}), 404
    nearest_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=ns.get("stop_id"),
        stop_name=ns.get("name"),
    )
    if nearest_seq is not None:
        ns["sequence"] = nearest_seq

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
    bus_segment_polylines = _segment_polylines_between_sequences(
        route_stops,
        from_sequence=ns.get("sequence"),
        to_sequence=terminus_seq,
    )

    payload = {
        "positions": positions,
        "message": message,
        "route_name": r["route_name"],
        "nearest_stop_name": ns["name"],
        "nearest_stop_sequence": ns.get("sequence"),
        "terminus_name": terminus["stop_name"] if terminus else "",
        "terminus_sequence": terminus_seq,
        "route_stops": route_stops,
        "bus_segment_polylines": bus_segment_polylines,
        "walk_otp_enabled": OTP_WALK_ENABLED,
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/shuttle/arrive/options")
def api_shuttle_arrive_options():
    """퇴근 노선 후보 최대 3개."""
    site_id = request.args.get("site_id", default="0000011")
    lat = request.args.get("lat", type=float)
    lng = request.args.get("lng", type=float)
    place_name = request.args.get("place_name", default="선택한 장소")
    time_param = request.args.get("time", "").strip()
    day_type = request.args.get("day_type", default="weekday")
    exclude_raw = request.args.get("exclude_route_ids", "").strip()
    use_endpoint_filter = _is_truthy_param(request.args.get("use_endpoint_filter"))
    selected_endpoints = _parse_selected_endpoints()
    auth_user, _ = _get_current_user(with_touch=False)
    max_distance_raw = request.args.get("max_distance_km")
    search_mode = _parse_route_search_mode(request.args.get("search_mode"))
    if lat is None or lng is None:
        return jsonify({"error": "lat, lng required"}), 400
    try:
        max_distance_km = _parse_max_distance_km(max_distance_raw)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    max_distance_m = max_distance_km * 1000.0

    exclude_ids = []
    if exclude_raw:
        exclude_ids = [int(x) for x in exclude_raw.split(",") if x.strip().isdigit()]

    def _record_route_search() -> None:
        _record_route_search_history(
            auth_user=auth_user,
            site_id=site_id,
            day_type=day_type,
            direction="arrive",
            lat=float(lat),
            lng=float(lng),
            selected_endpoints=selected_endpoints,
            place_name=place_name,
        )

    include_route_ids: set[int] | None = None
    selected_endpoint_components: set[str] | None = None
    if use_endpoint_filter or selected_endpoints is not None:
        try:
            include_route_ids = get_endpoint_route_ids(
                site_id=site_id,
                day_type=day_type,
                direction="arrive",
                selected_endpoints=selected_endpoints,
            )
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400
        if not include_route_ids:
            _record_route_search()
            return jsonify({"options": [], "walk_otp_enabled": OTP_WALK_ENABLED})
        selected_endpoint_components = _build_selected_endpoint_component_set(
            site_id=site_id,
            day_type=day_type,
            direction="arrive",
            selected_endpoints=selected_endpoints,
        )

    SHUTTLE_SEARCH_COUNT.labels(direction="arrive").inc()
    _record_route_search_source_metric("arrive")

    results = find_nearest_route_options(
        site_id=site_id,
        route_type="commute_out",
        lat=lat,
        lon=lng,
        day_type=day_type,
        # 후처리(탑승/하차 동일 정류장 제외, 시간 필터) 이후에도
        # 실제 반환 3개를 채울 수 있도록 후보를 넉넉히 가져온다.
        max_routes=_OPTIONS_CANDIDATE_LIMIT,
        exclude_route_ids=exclude_ids or None,
        include_route_ids=include_route_ids,
    )
    if not results:
        if include_route_ids is not None:
            _record_route_search()
            return jsonify({"options": [], "walk_otp_enabled": OTP_WALK_ENABLED})
        _record_route_search()
        return jsonify({"error": "해당 사업장의 퇴근 노선/정류장을 찾을 수 없습니다."}), 404

    options = []
    all_last_times = []
    for r in results:
        ns = {
            "name": r["nearest_stop"]["name"],
            "lat": r["nearest_stop"]["lat"],
            "lon": r["nearest_stop"]["lon"],
            "stop_id": r["nearest_stop"].get("stop_id"),
        }
        route_stops = r["route_stops"]
        nearest_seq = _find_route_stop_sequence(
            route_stops,
            stop_id=r["nearest_stop"].get("stop_id"),
            stop_name=ns["name"],
        )
        if nearest_seq is not None:
            ns["sequence"] = nearest_seq
        first = _choose_endpoint_display_stop(
            route_stops,
            direction="arrive",
            selected_components=selected_endpoint_components,
        )
        first_seq = _find_route_stop_sequence(
            route_stops,
            stop_id=first.get("stop_id") if first else None,
            stop_name=first.get("stop_name") if first else None,
        )

        needs_getoff_adjust = False
        min_getoff_seq: int | None = None
        if first and first_seq is not None:
            if nearest_seq is None or nearest_seq <= first_seq:
                needs_getoff_adjust = True
                min_getoff_seq = first_seq + 1
        elif first and first["stop_name"] == ns["name"]:
            needs_getoff_adjust = True

        # 퇴근은 항상 하차 정류장 순번이 탑승 정류장보다 뒤로 오도록 보정
        if needs_getoff_adjust:
            exclude_stop_ids: set[int] = set()
            try:
                sid = first.get("stop_id")
                if sid is not None:
                    exclude_stop_ids.add(int(sid))
            except (TypeError, ValueError):
                pass
            is_canonical_start = bool(route_stops) and first is route_stops[0]
            alt = _nearest_route_stop_for_user(
                route_stops,
                lat,
                lng,
                exclude_first=is_canonical_start,
                exclude_stop_ids=exclude_stop_ids or None,
                min_sequence=min_getoff_seq,
            )
            if not alt:
                continue
            ns = alt
            nearest_seq = _find_route_stop_sequence(
                route_stops,
                stop_id=ns.get("stop_id"),
                stop_name=ns.get("name"),
            )
            if nearest_seq is not None:
                ns["sequence"] = nearest_seq

        positions = []
        if first:
            positions.append({"lat": first["lat"], "lng": first["lng"], "label": first["stop_name"]})
        positions.append({"lat": ns["lat"], "lng": ns["lon"], "label": ns["name"]})
        positions.append({"lat": lat, "lng": lng, "label": "도착"})

        message = (
            f"{place_name}(으)로 가기 위해서는 {r['route_name']} 퇴근 버스를 "
            f"{first['stop_name'] if first else ''}에서 탑승하고 {ns['name']}에서 하차하세요."
        )
        bus_segment_polylines = _segment_polylines_between_sequences(
            route_stops,
            from_sequence=first_seq,
            to_sequence=ns.get("sequence"),
        )
        distance_m = haversine_distance_m(lat, lng, ns["lat"], ns["lon"])
        if distance_m > max_distance_m:
            continue

        if time_param:
            all_last_times.extend(r["all_departure_times"])

        board_time = (
            get_nearest_departure_time(r["all_departure_times"], time_param)
            if time_param
            else None
        )
        if time_param and board_time is None:
            continue

        payload = {
            "positions": positions,
            "message": message,
            "route_name": r["route_name"],
            "route_id": r["route_id"],
            "operator": ", ".join(r["companies"]),
            "start_stop_name": first["stop_name"] if first else "",
            "start_stop_sequence": first_seq,
            "getoff_stop_name": ns["name"],
            "getoff_stop_sequence": ns.get("sequence"),
            "distance_m": round(distance_m),
            "all_departure_times": r["all_departure_times"],
            "route_stops": route_stops,
            "bus_segment_polylines": bus_segment_polylines,
        }
        if board_time is not None:
            payload["board_time"] = board_time
        options.append(payload)

    options = _sort_route_options_by_search_mode(options, search_mode, time_param)[:_OPTIONS_RESPONSE_LIMIT]
    resp = {"options": options, "walk_otp_enabled": OTP_WALK_ENABLED, "search_mode": search_mode}
    if not options and all_last_times:
        resp["last_departure_time"] = sorted(set(all_last_times))[-1]
    _record_route_search()
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
    _record_route_search_source_metric("arrive")

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
    nearest_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=r["nearest_stop"].get("stop_id"),
        stop_name=ns["name"],
    )
    if nearest_seq is not None:
        ns["sequence"] = nearest_seq
    first = route_stops[0] if route_stops else None
    first_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=first.get("stop_id") if first else None,
        stop_name=first.get("stop_name") if first else None,
    )
    needs_getoff_adjust = False
    min_getoff_seq: int | None = None
    if first and first_seq is not None:
        if nearest_seq is None or nearest_seq <= first_seq:
            needs_getoff_adjust = True
            min_getoff_seq = first_seq + 1
    elif first and first["stop_name"] == ns["name"]:
        needs_getoff_adjust = True
    if needs_getoff_adjust:
        exclude_stop_ids: set[int] = set()
        try:
            sid = first.get("stop_id") if first else None
            if sid is not None:
                exclude_stop_ids.add(int(sid))
        except (TypeError, ValueError):
            pass
        alt = _nearest_route_stop_for_user(
            route_stops,
            lat,
            lng,
            exclude_first=True,
            exclude_stop_ids=exclude_stop_ids or None,
            min_sequence=min_getoff_seq,
        )
        if alt:
            ns = alt
        else:
            return jsonify({"error": "해당 노선에서 하차 가능한 정류장을 찾을 수 없습니다."}), 404
    nearest_seq = _find_route_stop_sequence(
        route_stops,
        stop_id=ns.get("stop_id"),
        stop_name=ns.get("name"),
    )
    if nearest_seq is not None:
        ns["sequence"] = nearest_seq

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
    bus_segment_polylines = _segment_polylines_between_sequences(
        route_stops,
        from_sequence=first_seq,
        to_sequence=ns.get("sequence"),
    )

    payload = {
        "positions": positions,
        "message": message,
        "route_name": r["route_name"],
        "start_stop_name": first["stop_name"] if first else "",
        "start_stop_sequence": first_seq,
        "getoff_stop_name": ns["name"],
        "getoff_stop_sequence": ns.get("sequence"),
        "route_stops": route_stops,
        "bus_segment_polylines": bus_segment_polylines,
        "walk_otp_enabled": OTP_WALK_ENABLED,
    }
    if board_time is not None:
        payload["board_time"] = board_time
    return jsonify(payload)


@app.route("/api/route/<int:route_id>/detail")
@app.route("/api/route/<int:route_id>/variant-detail")
def api_route_detail(route_id: int):
    """특정 route의 전체 시간표 + 선택 출발편 경유지 + ETA."""
    day_type = request.args.get("day_type", default="weekday")
    departure_time = request.args.get("departure_time", default="", type=str).strip() or None
    detail = get_route_detail(route_id, day_type, departure_time=departure_time)
    if not detail:
        return jsonify({"error": "노선을 찾을 수 없습니다."}), 404
    selected_departure_time = str(detail.get("selected_departure_time") or "").strip()
    route_uuid = str(detail.get("route_uuid") or "").strip() or None
    stop_ids = []
    stop_uuid_by_id: dict[int, str] = {}
    for stop in detail.get("route_stops", []) or []:
        try:
            stop_id_value = int(stop.get("stop_id"))
        except (TypeError, ValueError):
            continue
        stop_ids.append(stop_id_value)
        stop_uuid = str(stop.get("stop_uuid") or "").strip()
        if stop_uuid:
            stop_uuid_by_id[stop_id_value] = stop_uuid
    if selected_departure_time and stop_ids:
        eta_map = user_store.get_arrival_eta_map(
            route_id=route_id,
            route_uuid=route_uuid,
            day_type=day_type,
            departure_time=selected_departure_time,
            stop_ids=stop_ids,
            stop_uuid_by_id=stop_uuid_by_id,
        )
        for stop in detail.get("route_stops", []) or []:
            try:
                stop_id = int(stop.get("stop_id"))
            except (TypeError, ValueError):
                continue
            eta = eta_map.get(stop_id)
            if not eta:
                continue
            stop["eta_time"] = eta.get("eta_time")
            stop["eta_report_count"] = int(eta.get("report_count") or 0)
    return jsonify(detail)


# ── 시작 시 DB 초기화 ─────────────────────────────────────────
_run_startup_hooks()

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=8081)
