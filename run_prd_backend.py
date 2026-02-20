#!/usr/bin/env python3
"""shuttle-go 프로덕션 백엔드 실행 스크립트

Docker Compose로 backend 서비스만 빌드/실행한다.

사용법:
    python run_prd_backend.py                  # backend 빌드 + 시작
    python run_prd_backend.py --down           # backend 컨테이너 종료/삭제
    python run_prd_backend.py --timeout 120    # health check 타임아웃 변경(초)
"""

import argparse
import os
import subprocess
import sys
import time

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKEND_SERVICE = "backend"
DEFAULT_TIMEOUT = 60
HEALTH_POLL_INTERVAL_SEC = 1.0


def log(msg: str) -> None:
    print(f"\033[1;36m[shuttle-go]\033[0m {msg}", flush=True)


def err(msg: str) -> None:
    print(f"\033[1;31m[shuttle-go]\033[0m {msg}", flush=True)


def run(cmd: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, cwd=PROJECT_DIR, **kwargs)


def get_backend_container_id() -> str:
    result = run(
        f"docker compose ps -q {BACKEND_SERVICE}",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def get_backend_health_status(container_id: str) -> str:
    if not container_id:
        return "not_found"
    inspect_cmd = (
        "docker inspect --format "
        "'{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "
        f"{container_id}"
    )
    result = run(inspect_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return "unknown"
    status = result.stdout.strip()
    return status or "unknown"


def tail_backend_logs(lines: int = 80) -> None:
    log(f"backend 최근 로그({lines}줄):")
    run(f"docker compose logs --tail={lines} {BACKEND_SERVICE}")


def wait_backend_healthy(timeout_sec: int) -> bool:
    log(
        "백엔드 health check 대기 … "
        f"(최대 {timeout_sec}초, {int(HEALTH_POLL_INTERVAL_SEC)}초 간격 폴링)"
    )
    deadline = time.time() + timeout_sec
    last_status = ""
    while time.time() < deadline:
        container_id = get_backend_container_id()
        status = get_backend_health_status(container_id)
        if status != last_status:
            log(f"health 상태: {status}")
            last_status = status
        if status == "healthy":
            log("백엔드 정상 (healthy)")
            return True
        time.sleep(HEALTH_POLL_INTERVAL_SEC)

    err(
        f"{timeout_sec}초 내 healthy 상태가 되지 않았습니다 "
        f"(마지막 상태: {last_status or 'unknown'})"
    )
    tail_backend_logs()
    return False


def compose_backend_down() -> None:
    log("backend 컨테이너 종료/삭제 중 …")
    run(
        f"docker compose rm -sf {BACKEND_SERVICE}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log("backend 종료/삭제 완료")


def compose_backend_up(timeout_sec: int) -> None:
    log("backend 빌드 & 시작 …")
    ret = run(f"docker compose up -d --build {BACKEND_SERVICE}")
    if ret.returncode != 0:
        err("backend 시작 실패 (docker compose up -d --build backend)")
        sys.exit(1)

    if not wait_backend_healthy(timeout_sec):
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  backend 서비스 실행 완료")
    log("  확인: docker compose ps backend")
    log("═══════════════════════════════════════════")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="shuttle-go 프로덕션 백엔드 실행")
    parser.add_argument("--down", action="store_true", help="backend 컨테이너 종료/삭제")
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
        compose_backend_down()
        return

    compose_backend_up(args.timeout)


if __name__ == "__main__":
    main()
