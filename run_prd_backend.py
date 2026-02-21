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

    if args.all:
        compose_up_all(args.timeout)
    else:
        compose_up_core(args.timeout)


if __name__ == "__main__":
    main()
