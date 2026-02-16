#!/usr/bin/env python3
"""shuttle-go 개발 환경 통합 실행 스크립트

Docker Compose(backend, traefik, prometheus, grafana …)를 (재)시작하고
프론트엔드 정적 파일 서버를 함께 띄운다.

사용법:
    python run_dev.py              # 전체 시작 (Docker + 프론트엔드)
    python run_dev.py --frontend   # 프론트엔드 서버만 시작
    python run_dev.py --down       # 전체 종료
"""

import argparse
import http.server
import os
import signal
import socket
import subprocess
import sys
import threading
import time

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_PORT = 8080
HEALTH_URL = "http://localhost/health"
COMPOSE_TIMEOUT = 60  # 컨테이너 healthy 대기 최대 초


# ── 유틸 ──────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"\033[1;36m[shuttle-go]\033[0m {msg}", flush=True)


def err(msg: str) -> None:
    print(f"\033[1;31m[shuttle-go]\033[0m {msg}", flush=True)


def run(cmd: str, **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, shell=True, cwd=PROJECT_DIR, **kwargs)


def is_port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


# ── Docker Compose ────────────────────────────────────────

def compose_down() -> None:
    log("Docker Compose 종료 중 …")
    run("docker compose down", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    log("Docker Compose 종료 완료")


def compose_up() -> None:
    # 이미 동작 중인 컨테이너가 있으면 재시작
    result = run(
        "docker compose ps -q",
        capture_output=True, text=True,
    )
    if result.stdout.strip():
        log("기존 컨테이너 감지 → 재시작합니다 …")
        run("docker compose down", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    log("Docker Compose 빌드 & 시작 …")
    ret = run("docker compose up -d --build")
    if ret.returncode != 0:
        err("docker compose up 실패")
        sys.exit(1)

    # backend healthy 대기
    log("백엔드 health check 대기 …")
    deadline = time.time() + COMPOSE_TIMEOUT
    while time.time() < deadline:
        try:
            import urllib.request
            with urllib.request.urlopen(HEALTH_URL, timeout=3) as resp:
                if resp.status == 200:
                    log("백엔드 정상 (healthy)")
                    return
        except Exception:
            pass
        time.sleep(1)

    err(f"{COMPOSE_TIMEOUT}초 내에 백엔드가 응답하지 않았습니다")
    sys.exit(1)


# ── 프론트엔드 정적 서버 ──────────────────────────────────

def start_frontend() -> None:
    if is_port_in_use(FRONTEND_PORT):
        log(f"포트 {FRONTEND_PORT} 이미 사용 중 → 기존 프로세스 종료 시도")
        run(f"lsof -ti :{FRONTEND_PORT} | xargs kill -9 2>/dev/null")
        time.sleep(0.5)

    os.chdir(PROJECT_DIR)

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        """BrokenPipe / ConnectionReset 등 클라이언트 끊김 에러를 조용히 무시"""
        def log_message(self, format, *args):
            # 기본 로그는 그대로 출력
            super().log_message(format, *args)

    class QuietServer(http.server.ThreadingHTTPServer):
        def handle_error(self, request, client_address):
            import traceback
            exc = sys.exc_info()[1]
            if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
                log(f"⚠ 클라이언트 연결 끊김 ({client_address[0]}:{client_address[1]}) — 무시")
            else:
                super().handle_error(request, client_address)

    httpd = QuietServer(("0.0.0.0", FRONTEND_PORT), QuietHandler)

    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    log(f"프론트엔드 서버 시작 → http://localhost:{FRONTEND_PORT}")
    return httpd


def wait_forever(httpd) -> None:
    """Ctrl+C 대기 후 프론트엔드 서버 종료"""
    try:
        signal.pause()
    except (KeyboardInterrupt, AttributeError):
        # Windows에는 signal.pause()가 없으므로 fallback
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass

    httpd.shutdown()
    log("프론트엔드 서버 종료")


# ── 메인 ──────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="shuttle-go 개발 환경 통합 실행")
    parser.add_argument("--down", action="store_true", help="전체 종료")
    parser.add_argument("--frontend", action="store_true", help="프론트엔드 서버만 시작")
    args = parser.parse_args()

    if args.down:
        compose_down()
        log(f"포트 {FRONTEND_PORT} 정리")
        run(f"lsof -ti :{FRONTEND_PORT} | xargs kill -9 2>/dev/null")
        log("완료")
        return

    if args.frontend:
        httpd = start_frontend()
        print()
        log("═══════════════════════════════════════════")
        log(f"  프론트엔드  → http://localhost:{FRONTEND_PORT}")
        log("═══════════════════════════════════════════")
        log("Ctrl+C 로 종료")
        print()
        wait_forever(httpd)
        return

    compose_up()
    httpd = start_frontend()

    # 컨테이너 상태 출력
    log("실행 중인 서비스:")
    run("docker compose ps")

    print()
    log("═══════════════════════════════════════════")
    log(f"  프론트엔드  → http://localhost:{FRONTEND_PORT}")
    log(f"  백엔드 API  → http://localhost/api/sites")
    log(f"  Grafana     → http://localhost/grafana/")
    log("═══════════════════════════════════════════")
    log("Ctrl+C 로 프론트엔드 서버 종료 (Docker 컨테이너는 유지)")
    log("전체 종료: python run_dev.py --down")
    print()

    wait_forever(httpd)


if __name__ == "__main__":
    main()
