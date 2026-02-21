#!/usr/bin/env python3
"""shuttle-go 프로덕션 백엔드 실행 스크립트

기본 실행은 backend만 재빌드/재시작한다.
--all 옵션을 주면 Docker Compose 전체 스택(traefik, backend, prometheus, node-exporter, grafana)을 재시작한다.
컨테이너가 올라오면 스크립트는 즉시 종료된다.

사용법:
    python run_prd_backend.py                  # backend만 재빌드 + 재시작
    python run_prd_backend.py --all            # 전체 스택 빌드 + 시작
    python run_prd_backend.py --down           # backend만 종료
    python run_prd_backend.py --down --all     # 전체 스택 종료
    python run_prd_backend.py --timeout 120    # health check 타임아웃 변경(초)
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(PROJECT_DIR, ".env")
BACKEND_SERVICE = "backend"
DEFAULT_TIMEOUT = 60
DEFAULT_BACKEND_HTTP_PORT = 80
HEALTH_POLL_INTERVAL_SEC = 1.0
BACKEND_HTTP_PORT = DEFAULT_BACKEND_HTTP_PORT
HEALTH_URL = ""


def log(msg: str) -> None:
    print(f"\033[1;36m[shuttle-go]\033[0m {msg}", flush=True)


def err(msg: str) -> None:
    print(f"\033[1;31m[shuttle-go]\033[0m {msg}", flush=True)


def run(cmd: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, cwd=PROJECT_DIR, **kwargs)


def load_env_file(path: str) -> dict[str, str]:
    values: dict[str, str] = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("export "):
                stripped = stripped[len("export ") :].strip()
            if "=" not in stripped:
                continue
            key, raw_value = stripped.split("=", 1)
            key = key.strip()
            value = raw_value.strip()
            if (
                len(value) >= 2
                and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'"))
            ):
                value = value[1:-1]
            values[key] = value
    return values


def get_env_int(env_map: dict[str, str], key: str, default: int) -> int:
    raw = os.environ.get(key, env_map.get(key, str(default))).strip()
    try:
        value = int(raw)
    except ValueError:
        err(f"{key} 값이 정수가 아닙니다: {raw}")
        sys.exit(1)
    if value < 1 or value > 65535:
        err(f"{key} 값은 1~65535 범위여야 합니다: {value}")
        sys.exit(1)
    return value


def local_url(path: str, port: int, scheme: str = "http") -> str:
    default_port = 80 if scheme == "http" else 443
    if port == default_port:
        return f"{scheme}://localhost{path}"
    return f"{scheme}://localhost:{port}{path}"


def configure_runtime_health_url() -> None:
    global BACKEND_HTTP_PORT, HEALTH_URL
    env_map = load_env_file(ENV_FILE)
    BACKEND_HTTP_PORT = get_env_int(env_map, "BACKEND_HTTP_PORT", DEFAULT_BACKEND_HTTP_PORT)
    HEALTH_URL = local_url("/health", BACKEND_HTTP_PORT, scheme="http")


def get_backend_container_id() -> str:
    result = run(
        f"docker compose ps -q {BACKEND_SERVICE}",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def tail_backend_logs(lines: int = 80) -> None:
    log(f"backend 최근 로그({lines}줄):")
    run(f"docker compose logs --tail={lines} {BACKEND_SERVICE}")


def _is_healthy_response(resp: urllib.request.addinfourl) -> bool:
    if resp.status != 200:
        return False
    body = resp.read().decode("utf-8", errors="ignore").strip()
    if not body:
        return True
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return True
    status = payload.get("status")
    return isinstance(status, str) and status.lower() == "healthy"


def wait_backend_healthy(timeout_sec: int) -> bool:
    log(
        "백엔드 health endpoint 대기 … "
        f"(URL: {HEALTH_URL}, 최대 {timeout_sec}초, {int(HEALTH_POLL_INTERVAL_SEC)}초 간격 폴링)"
    )
    deadline = time.monotonic() + timeout_sec
    last_error = ""
    while True:
        poll_started = time.monotonic()
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=HEALTH_POLL_INTERVAL_SEC) as resp:
                if _is_healthy_response(resp):
                    log("백엔드 정상 (healthy)")
                    return True
                last_error = f"unexpected response status={resp.status}"
        except Exception as exc:
            last_error = str(exc)

        now = time.monotonic()
        if now >= deadline:
            break
        sleep_for = min(
            HEALTH_POLL_INTERVAL_SEC,
            max(0.0, HEALTH_POLL_INTERVAL_SEC - (now - poll_started)),
            deadline - now,
        )
        if sleep_for > 0:
            time.sleep(sleep_for)

    err(
        f"{timeout_sec}초 내 /health 확인이 실패했습니다 "
        f"(URL: {HEALTH_URL}, 마지막 오류: {last_error or 'unknown'})"
    )
    tail_backend_logs()
    return False


def compose_down(all_services: bool = False) -> None:
    if all_services:
        log("Docker Compose 전체 종료 중 …")
        run("docker compose down", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log("Docker Compose 전체 종료 완료")
        return

    log("backend 종료 중 …")
    run(
        f"docker compose rm -f -s {BACKEND_SERVICE}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log("backend 종료 완료")


def compose_up_core(timeout_sec: int) -> None:
    # 다운타임 최소화를 위해 먼저 이미지 빌드 후 컨테이너 교체
    log("backend 이미지 빌드 중 …")
    build_ret = run(f"docker compose build {BACKEND_SERVICE}")
    if build_ret.returncode != 0:
        err("docker compose build backend 실패")
        sys.exit(1)

    existing_backend = get_backend_container_id()
    if existing_backend:
        log("기존 backend 컨테이너 감지 → 교체 시작")
        rm_ret = run(f"docker compose rm -f -s {BACKEND_SERVICE}")
        if rm_ret.returncode != 0:
            err("기존 backend 컨테이너 정리에 실패했습니다")
            sys.exit(1)

    log("새 backend 컨테이너 시작 …")
    up_ret = run(f"docker compose up -d --no-build --force-recreate {BACKEND_SERVICE}")
    if up_ret.returncode != 0:
        err("docker compose up -d backend 실패")
        sys.exit(1)

    if not wait_backend_healthy(timeout_sec):
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  backend 재시작 완료")
    log("  확인: docker compose ps backend")
    log("═══════════════════════════════════════════")
    run(f"docker compose ps {BACKEND_SERVICE}")
    print()


def compose_up_all(timeout_sec: int) -> None:
    # --all은 전체 스택 재시작
    result = run("docker compose ps -q", capture_output=True, text=True)
    if result.stdout.strip():
        log("기존 컨테이너 감지 → 전체 재시작합니다 …")
        run("docker compose down", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    log("Docker Compose 전체 빌드 & 시작 …")
    ret = run("docker compose up -d --build")
    if ret.returncode != 0:
        err("docker compose up -d --build 실패")
        sys.exit(1)

    if not wait_backend_healthy(timeout_sec):
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  Docker Compose 서비스 실행 완료")
    log("  확인: docker compose ps")
    log("═══════════════════════════════════════════")
    run("docker compose ps")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="shuttle-go 프로덕션 백엔드 실행")
    parser.add_argument("--down", action="store_true", help="종료 (기본: backend만, --all: 전체)")
    parser.add_argument("--all", action="store_true", help="전체 스택 재시작/종료")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"health check 타임아웃(초), 기본 {DEFAULT_TIMEOUT}",
    )
    args = parser.parse_args()

    if args.timeout < 1:
        err("--timeout은 1 이상의 정수여야 합니다.")
        sys.exit(1)

    if args.down:
        compose_down(all_services=args.all)
        return

    configure_runtime_health_url()

    if args.all:
        compose_up_all(args.timeout)
    else:
        compose_up_core(args.timeout)


if __name__ == "__main__":
    main()
