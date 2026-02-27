#!/usr/bin/env python3
"""shuttle-go 프로덕션 백엔드 실행 스크립트

기본 실행은 backend blue/green 롤아웃으로 동작한다.
- 현재 활성 슬롯(blue 또는 green)을 유지한 상태로 반대 슬롯을 새 버전으로 기동
- 반대 슬롯 health 확인 후 점진적으로 트래픽 전환
- 전환 완료 후 기존 슬롯 종료
- 새 슬롯 health 실패 시 즉시 롤백(기존 슬롯 트래픽 유지)

사용법:
    python run_prd_backend.py                  # backend blue/green 롤아웃
    python run_prd_backend.py --backend        # 위와 동일
    python run_prd_backend.py --all            # 전체 스택 빌드 + 시작
    python run_prd_backend.py --all-no-otp     # otp 제외 전체 스택 빌드 + 시작
    python run_prd_backend.py --otp            # otp만 재시작
    python run_prd_backend.py --traefik --grafana  # 지정 컴포넌트만 재시작
    python run_prd_backend.py --down           # backend-blue, backend-green 종료
    python run_prd_backend.py --down --all     # 전체 스택 종료
    python run_prd_backend.py --down --all-no-otp # otp 제외 전체 스택 종료
    python run_prd_backend.py --down --otp     # otp만 종료
    python run_prd_backend.py --timeout 120    # health check 타임아웃 변경(초)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_FILE = os.path.join(PROJECT_DIR, ".env")
BACKEND_TRAFFIC_FILE = os.path.join(
    PROJECT_DIR, "monitoring", "traefik", "dynamic", "backend-traffic.yml"
)

BACKEND_BLUE_SERVICE = "backend-blue"
BACKEND_GREEN_SERVICE = "backend-green"
BACKEND_SERVICES = (BACKEND_BLUE_SERVICE, BACKEND_GREEN_SERVICE)
BACKEND_ALIAS = "backend"
GATEWAY_SERVICE = "traefik"

SERVICE_ORDER = (
    "traefik",
    BACKEND_BLUE_SERVICE,
    BACKEND_GREEN_SERVICE,
    "prometheus",
    "node-exporter",
    "cadvisor",
    "grafana",
    "otp",
)
ALL_EXCEPT_OTP_SERVICES = tuple(name for name in SERVICE_ORDER if name != "otp")

DEFAULT_TIMEOUT = 60
DEFAULT_BACKEND_HTTP_PORT = 80
DEFAULT_PROMETHEUS_HOST_PORT = 9090
HEALTH_POLL_INTERVAL_SEC = 1.0
TRAEFIK_RELOAD_WAIT_SEC = 2.0

CANARY_STEPS = (
    (10, 5),   # 신규 슬롯 10% 트래픽, 5초 관찰
    (50, 5),   # 신규 슬롯 50% 트래픽, 5초 관찰
    (100, 0),  # 신규 슬롯 100% 트래픽
)

BACKEND_HTTP_PORT = DEFAULT_BACKEND_HTTP_PORT
PROMETHEUS_HOST_PORT = DEFAULT_PROMETHEUS_HOST_PORT
HEALTH_URL = ""

_ACTIVE_SLOT_RE = re.compile(r"active_slot:\s*(blue|green)", re.IGNORECASE)


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
                stripped = stripped[len("export "):].strip()
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
    global BACKEND_HTTP_PORT, PROMETHEUS_HOST_PORT, HEALTH_URL
    env_map = load_env_file(ENV_FILE)
    BACKEND_HTTP_PORT = get_env_int(env_map, "BACKEND_HTTP_PORT", DEFAULT_BACKEND_HTTP_PORT)
    PROMETHEUS_HOST_PORT = get_env_int(
        env_map, "PROMETHEUS_HOST_PORT", DEFAULT_PROMETHEUS_HOST_PORT
    )
    HEALTH_URL = local_url("/health", BACKEND_HTTP_PORT, scheme="http")


def ordered_services(services: list[str] | tuple[str, ...]) -> list[str]:
    service_set = set(services)
    return [name for name in SERVICE_ORDER if name in service_set]


def selected_services_from_args(args: argparse.Namespace) -> list[str]:
    selected: list[str] = []
    if args.traefik:
        selected.append("traefik")
    if args.backend:
        selected.extend(BACKEND_SERVICES)
    if args.prometheus:
        selected.append("prometheus")
    if args.node_exporter:
        selected.append("node-exporter")
    if args.grafana:
        selected.append("grafana")
    if args.otp:
        selected.append("otp")
    return ordered_services(selected)


def ensure_gateway_running() -> None:
    gateway_running = run(
        f"docker compose ps --status running -q {GATEWAY_SERVICE}",
        capture_output=True,
        text=True,
    )
    if gateway_running.returncode != 0:
        err("traefik 실행 상태 확인에 실패했습니다")
        sys.exit(1)
    if gateway_running.stdout.strip():
        return

    log("traefik 미실행 상태 감지 -> 시작")
    gateway_ret = run(f"docker compose up -d {GATEWAY_SERVICE}")
    if gateway_ret.returncode != 0:
        err("traefik 시작 실패")
        sys.exit(1)


def get_service_container_id(service: str) -> str:
    result = run(
        f"docker compose ps -q {service}",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def is_service_running(service: str) -> bool:
    result = run(
        f"docker compose ps --status running -q {service}",
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return False
    return bool(result.stdout.strip())


def tail_service_logs(service: str, lines: int = 80) -> None:
    log(f"{service} 최근 로그({lines}줄):")
    run(f"docker compose logs --tail={lines} {service}")


def get_service_health_status(service: str) -> str:
    container_id = get_service_container_id(service)
    if not container_id:
        return "missing"
    inspect = run(
        "docker inspect --format "
        "'{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "
        f"{container_id}",
        capture_output=True,
        text=True,
    )
    if inspect.returncode != 0:
        return "unknown"
    return (inspect.stdout or "").strip().lower() or "unknown"


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


def wait_public_backend_healthy(timeout_sec: int) -> bool:
    log(
        "공개 health endpoint 대기 … "
        f"(URL: {HEALTH_URL}, 최대 {timeout_sec}초, {int(HEALTH_POLL_INTERVAL_SEC)}초 간격)"
    )
    deadline = time.monotonic() + timeout_sec
    last_error = ""
    while True:
        poll_started = time.monotonic()
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=HEALTH_POLL_INTERVAL_SEC) as resp:
                if _is_healthy_response(resp):
                    log("공개 health 정상")
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
        f"{timeout_sec}초 내 공개 health 확인 실패 "
        f"(URL: {HEALTH_URL}, 마지막 오류: {last_error or 'unknown'})"
    )
    return False


def wait_service_healthy(service: str, timeout_sec: int) -> bool:
    log(f"{service} health 대기 … (최대 {timeout_sec}초)")
    deadline = time.monotonic() + timeout_sec
    last_status = "unknown"
    while True:
        status = get_service_health_status(service)
        last_status = status
        if status in {"healthy", "running"}:
            log(f"{service} 정상 상태 ({status})")
            return True

        now = time.monotonic()
        if now >= deadline:
            break
        time.sleep(HEALTH_POLL_INTERVAL_SEC)

    err(f"{service} health 확인 실패 (마지막 상태: {last_status})")
    tail_service_logs(service)
    return False


def slot_to_service(slot: str) -> str:
    if slot == "green":
        return BACKEND_GREEN_SERVICE
    return BACKEND_BLUE_SERVICE


def other_slot(slot: str) -> str:
    return "green" if slot == "blue" else "blue"


def _weights_for_new_slot(new_slot: str, new_percent: int) -> tuple[int, int]:
    new_percent = max(0, min(100, int(new_percent)))
    old_percent = 100 - new_percent
    if new_slot == "blue":
        return new_percent, old_percent
    return old_percent, new_percent


def _weights_for_active_slot(active_slot: str) -> tuple[int, int]:
    if active_slot == "green":
        return (0, 100)
    return (100, 0)


def detect_active_slot_from_file() -> str:
    try:
        with open(BACKEND_TRAFFIC_FILE, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError:
        return "blue"

    m = _ACTIVE_SLOT_RE.search(content)
    if m:
        return m.group(1).lower()

    blue_m = re.search(
        r"name:\s*backend-blue\s*\n\s*weight:\s*(\d+)", content, flags=re.IGNORECASE
    )
    green_m = re.search(
        r"name:\s*backend-green\s*\n\s*weight:\s*(\d+)", content, flags=re.IGNORECASE
    )
    if blue_m and green_m:
        blue_weight = int(blue_m.group(1))
        green_weight = int(green_m.group(1))
        return "green" if green_weight > blue_weight else "blue"

    return "blue"


def write_backend_traffic_file(
    *,
    active_slot: str,
    blue_weight: int,
    green_weight: int,
    sleep_for_reload: bool = True,
) -> None:
    active = "green" if active_slot == "green" else "blue"
    bw = max(0, int(blue_weight))
    gw = max(0, int(green_weight))
    total = bw + gw
    if total <= 0:
        bw, gw = 100, 0
    elif total != 100:
        bw = int(round(bw * 100.0 / total))
        gw = 100 - bw

    content = (
        f"# active_slot: {active}\n"
        "# This file is managed by run_prd_backend.py for blue/green backend rollout.\n\n"
        "http:\n"
        "  services:\n"
        "    backend-blue:\n"
        "      loadBalancer:\n"
        "        passHostHeader: true\n"
        "        servers:\n"
        "          - url: \"http://backend-blue:8081\"\n"
        "    backend-green:\n"
        "      loadBalancer:\n"
        "        passHostHeader: true\n"
        "        servers:\n"
        "          - url: \"http://backend-green:8081\"\n"
        "    backend-active:\n"
        "      weighted:\n"
        "        services:\n"
        f"          - name: backend-blue\n"
        f"            weight: {bw}\n"
        f"          - name: backend-green\n"
        f"            weight: {gw}\n"
    )

    os.makedirs(os.path.dirname(BACKEND_TRAFFIC_FILE), exist_ok=True)
    tmp_path = f"{BACKEND_TRAFFIC_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp_path, BACKEND_TRAFFIC_FILE)
    if sleep_for_reload and TRAEFIK_RELOAD_WAIT_SEC > 0:
        time.sleep(TRAEFIK_RELOAD_WAIT_SEC)


def rollback_to_slot(active_slot: str, standby_service: str) -> None:
    bw, gw = _weights_for_active_slot(active_slot)
    write_backend_traffic_file(active_slot=active_slot, blue_weight=bw, green_weight=gw)
    run(
        f"docker compose rm -f -s {standby_service}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def ensure_initial_backend_active(timeout_sec: int, active_slot: str) -> bool:
    active_service = slot_to_service(active_slot)
    standby_service = slot_to_service(other_slot(active_slot))
    active_running = is_service_running(active_service)
    standby_running = is_service_running(standby_service)

    if active_running:
        return True

    if standby_running:
        return False

    log(f"기존 백엔드 미실행 상태 -> {active_service} 초기 기동")
    ret = run(f"docker compose up -d --build {active_service}")
    if ret.returncode != 0:
        err(f"{active_service} 초기 기동 실패")
        sys.exit(1)
    if not wait_service_healthy(active_service, timeout_sec):
        sys.exit(1)
    bw, gw = _weights_for_active_slot(active_slot)
    write_backend_traffic_file(active_slot=active_slot, blue_weight=bw, green_weight=gw)
    if not wait_public_backend_healthy(min(timeout_sec, 30)):
        sys.exit(1)
    return True


def compose_up_core(timeout_sec: int) -> None:
    ensure_gateway_running()

    if not is_service_running(BACKEND_BLUE_SERVICE) and not is_service_running(BACKEND_GREEN_SERVICE):
        initial_slot = detect_active_slot_from_file()
        ensure_initial_backend_active(timeout_sec, initial_slot)
        print()
        log("═══════════════════════════════════════════")
        log(f"  초기 배포 완료 (active={slot_to_service(initial_slot)})")
        log(f"  확인: docker compose ps {BACKEND_BLUE_SERVICE} {BACKEND_GREEN_SERVICE}")
        log("═══════════════════════════════════════════")
        run(f"docker compose ps {BACKEND_BLUE_SERVICE} {BACKEND_GREEN_SERVICE}")
        print()
        return

    file_slot = detect_active_slot_from_file()
    if not ensure_initial_backend_active(timeout_sec, file_slot):
        file_slot = other_slot(file_slot)

    active_slot = file_slot
    active_service = slot_to_service(active_slot)
    standby_slot = other_slot(active_slot)
    standby_service = slot_to_service(standby_slot)

    if not wait_service_healthy(active_service, timeout_sec):
        err(f"활성 슬롯 {active_service}가 비정상입니다. 롤아웃을 중단합니다.")
        sys.exit(1)

    bw, gw = _weights_for_active_slot(active_slot)
    write_backend_traffic_file(active_slot=active_slot, blue_weight=bw, green_weight=gw)

    log(
        f"blue/green 롤아웃 시작 (active={active_service}, standby={standby_service})"
    )
    ret = run(f"docker compose up -d --build --force-recreate {standby_service}")
    if ret.returncode != 0:
        err(f"{standby_service} 기동 실패 -> 기존 슬롯 유지")
        sys.exit(1)

    if not wait_service_healthy(standby_service, timeout_sec):
        err(f"{standby_service} health 실패 -> 롤백")
        rollback_to_slot(active_slot, standby_service)
        sys.exit(1)

    for new_percent, observe_sec in CANARY_STEPS:
        blue_weight, green_weight = _weights_for_new_slot(standby_slot, new_percent)
        marker_slot = standby_slot if new_percent == 100 else active_slot
        log(
            f"트래픽 전환: {BACKEND_BLUE_SERVICE}={blue_weight}%, "
            f"{BACKEND_GREEN_SERVICE}={green_weight}%"
        )
        write_backend_traffic_file(
            active_slot=marker_slot,
            blue_weight=blue_weight,
            green_weight=green_weight,
        )
        if observe_sec > 0:
            time.sleep(observe_sec)
        standby_status = get_service_health_status(standby_service)
        if standby_status not in {"healthy", "running"}:
            err(f"{standby_service} 상태 악화({standby_status}) -> 롤백")
            rollback_to_slot(active_slot, standby_service)
            sys.exit(1)

    if not wait_public_backend_healthy(min(timeout_sec, 30)):
        err("공개 health 점검 실패 -> 롤백")
        rollback_to_slot(active_slot, standby_service)
        sys.exit(1)

    log(f"기존 슬롯 종료: {active_service}")
    run(
        f"docker compose rm -f -s {active_service}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    print()
    log("═══════════════════════════════════════════")
    log(
        f"  backend blue/green 전환 완료 (active={standby_service}, "
        f"traffic: {BACKEND_BLUE_SERVICE}/{BACKEND_GREEN_SERVICE} = "
        f"{_weights_for_active_slot(standby_slot)[0]}/"
        f"{_weights_for_active_slot(standby_slot)[1]})"
    )
    log(f"  확인: docker compose ps {BACKEND_BLUE_SERVICE} {BACKEND_GREEN_SERVICE}")
    log("═══════════════════════════════════════════")
    run(f"docker compose ps {BACKEND_BLUE_SERVICE} {BACKEND_GREEN_SERVICE}")
    print()


def compose_down(all_services: bool = False) -> None:
    if all_services:
        log("Docker Compose 전체 종료 중 …")
        run(
            "docker compose down --remove-orphans",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log("Docker Compose 전체 종료 완료")
        return

    log("backend(blue/green) 종료 중 …")
    run(
        f"docker compose rm -f -s {BACKEND_BLUE_SERVICE} {BACKEND_GREEN_SERVICE}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    write_backend_traffic_file(active_slot="blue", blue_weight=100, green_weight=0, sleep_for_reload=False)
    log("backend(blue/green) 종료 완료")


def compose_down_selected(services: list[str]) -> None:
    ordered = ordered_services(services)
    if not ordered:
        return
    service_expr = " ".join(ordered)
    log(f"선택 구성요소 종료 중 … ({service_expr})")
    run(
        f"docker compose rm -f -s {service_expr}",
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if any(s in BACKEND_SERVICES for s in ordered):
        if not is_service_running(BACKEND_BLUE_SERVICE) and not is_service_running(BACKEND_GREEN_SERVICE):
            write_backend_traffic_file(
                active_slot="blue",
                blue_weight=100,
                green_weight=0,
                sleep_for_reload=False,
            )
    log("선택 구성요소 종료 완료")


def compose_up_selected(services: list[str], timeout_sec: int) -> None:
    ordered = ordered_services(services)
    includes_backend = any(s in BACKEND_SERVICES for s in ordered)
    includes_gateway = GATEWAY_SERVICE in ordered

    if includes_backend:
        compose_up_core(timeout_sec)
        ordered = [s for s in ordered if s not in BACKEND_SERVICES]

    if not ordered:
        return

    if not includes_gateway and includes_backend:
        ensure_gateway_running()

    service_expr = " ".join(ordered)
    log(f"선택 구성요소 시작 … ({service_expr})")
    ret = run(f"docker compose up -d --build --force-recreate {service_expr}")
    if ret.returncode != 0:
        err("선택 구성요소 docker compose up 실패")
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  선택 구성요소 재시작 완료")
    log(f"  확인: docker compose ps {service_expr}")
    log("═══════════════════════════════════════════")
    run(f"docker compose ps {service_expr}")
    print()


def compose_up_all(timeout_sec: int) -> None:
    result = run("docker compose ps -q", capture_output=True, text=True)
    if result.stdout.strip():
        log("기존 컨테이너 감지 -> 전체 재시작")
        run(
            "docker compose down --remove-orphans",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    log("Docker Compose 전체 빌드 & 시작 …")
    ret = run("docker compose up -d --build")
    if ret.returncode != 0:
        err("docker compose up -d --build 실패")
        sys.exit(1)

    active_slot = detect_active_slot_from_file()
    active_service = slot_to_service(active_slot)
    if not wait_service_healthy(active_service, timeout_sec):
        sys.exit(1)
    if not wait_public_backend_healthy(min(timeout_sec, 30)):
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  Docker Compose 서비스 실행 완료")
    log("  확인: docker compose ps")
    log("═══════════════════════════════════════════")
    run("docker compose ps")
    print()


def compose_up_all_except_otp(timeout_sec: int) -> None:
    service_expr = " ".join(ALL_EXCEPT_OTP_SERVICES)
    log(f"OTP 제외 전체 구성요소 재시작 … ({service_expr})")
    ret = run(f"docker compose up -d --build --force-recreate {service_expr}")
    if ret.returncode != 0:
        err("OTP 제외 전체 구성요소 docker compose up 실패")
        sys.exit(1)

    active_slot = detect_active_slot_from_file()
    active_service = slot_to_service(active_slot)
    if not wait_service_healthy(active_service, timeout_sec):
        sys.exit(1)
    if not wait_public_backend_healthy(min(timeout_sec, 30)):
        sys.exit(1)

    print()
    log("═══════════════════════════════════════════")
    log("  OTP 제외 구성요소 실행 완료")
    log(f"  확인: docker compose ps {service_expr}")
    log("═══════════════════════════════════════════")
    run(f"docker compose ps {service_expr}")
    print()


def print_access_summary(args: argparse.Namespace, selected_services: list[str]) -> None:
    print()
    log("═══════════════════════════════════════════")
    log(f"  백엔드 API  -> {local_url('/api/sites', BACKEND_HTTP_PORT, scheme='http')}")
    if args.all or args.all_no_otp or "prometheus" in selected_services:
        log(f"  Prometheus  -> {local_url('/', PROMETHEUS_HOST_PORT, scheme='http')}")
    if args.all or args.all_no_otp or "grafana" in selected_services:
        log(f"  Grafana     -> {local_url('/grafana/', BACKEND_HTTP_PORT, scheme='http')}")
    if args.all or "otp" in selected_services:
        log("  OTP 내부주소 -> http://otp:8082 (Docker 네트워크)")
    log("═══════════════════════════════════════════")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="shuttle-go 프로덕션 백엔드 실행")
    parser.add_argument(
        "--down",
        action="store_true",
        help="종료 (기본: backend blue/green, --all: 전체, --all-no-otp: otp 제외 전체)",
    )
    parser.add_argument("--all", action="store_true", help="전체 스택 재시작/종료")
    parser.add_argument("--all-no-otp", action="store_true", help="otp 제외 전체 스택 재시작/종료")
    parser.add_argument("--traefik", action="store_true", help="traefik만 재시작/종료")
    parser.add_argument("--backend", action="store_true", help="backend blue/green 롤아웃")
    parser.add_argument("--prometheus", action="store_true", help="prometheus만 재시작/종료")
    parser.add_argument(
        "--node-exporter",
        dest="node_exporter",
        action="store_true",
        help="node-exporter만 재시작/종료",
    )
    parser.add_argument("--grafana", action="store_true", help="grafana만 재시작/종료")
    parser.add_argument("--otp", action="store_true", help="otp만 재시작/종료")
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"health check 타임아웃(초), 기본 {DEFAULT_TIMEOUT}",
    )
    args = parser.parse_args()
    selected_services = selected_services_from_args(args)
    all_no_otp_services = ordered_services(ALL_EXCEPT_OTP_SERVICES)

    if args.timeout < 1:
        err("--timeout은 1 이상의 정수여야 합니다.")
        sys.exit(1)
    if args.all and args.all_no_otp:
        parser.error("--all과 --all-no-otp는 함께 사용할 수 없습니다.")
    if (args.all or args.all_no_otp) and selected_services:
        parser.error(
            "--all/--all-no-otp와 개별 컴포넌트 플래그("
            "--traefik/--backend/--prometheus/--node-exporter/--grafana/--otp)"
            "는 함께 사용할 수 없습니다."
        )

    if args.down:
        if args.all:
            compose_down(all_services=True)
        elif args.all_no_otp:
            compose_down_selected(all_no_otp_services)
        elif selected_services:
            compose_down_selected(selected_services)
        else:
            compose_down(all_services=False)
        return

    configure_runtime_health_url()

    if args.all:
        compose_up_all(args.timeout)
    elif args.all_no_otp:
        compose_up_all_except_otp(args.timeout)
    elif selected_services:
        compose_up_selected(selected_services, args.timeout)
    else:
        compose_up_core(args.timeout)
    print_access_summary(args, selected_services)


if __name__ == "__main__":
    main()
