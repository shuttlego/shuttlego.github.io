#!/usr/bin/env python3
"""
data/raw_geo/ 내 파일에서 showMap 패턴을 파싱해
- data/tmp_geo_info.csv: 정류장명, 위도, 경도 (중복 제거)
- data/geo_route_info.csv: 노선별 정류장 순서 (한 줄에 "노선명: 정류장1-정류장2-...")
"""

import csv
import re
from pathlib import Path

# 프로젝트 루트 기준 data 디렉토리
_ROOT = Path(__file__).resolve().parent.parent
RAW_GEO_DIR = _ROOT / "data" / "raw_geo"
TMP_GEO_INFO_PATH = _ROOT / "data" / "tmp_geo_info.csv"
GEO_ROUTE_INFO_PATH = _ROOT / "data" / "geo_route_info.csv"


def strip_html(text: str) -> str:
    """HTML 태그를 제거하고 앞뒤 공백을 정리한다."""
    return re.sub(r"<[^>]+>", "", text).strip()


def normalize_spaces(text: str) -> str:
    """줄바꿈·연속 공백을 한 칸으로 줄인 뒤 앞뒤 공백을 제거한다."""
    return re.sub(r"\s+", " ", text).strip()


# showMap 패턴 (한 번만 컴파일)
_SHOWMAP_PATTERN = re.compile(
    r"<a\s+href=\"javascript:showMap\("
    r"'[^']*',\s*'[^']*',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)'"  # 위도, 경도, siIdx
    r"[^)]*\);\"" r"[^>]*>([\s\S]*?)</a>",
    re.IGNORECASE,
)


def parse_showmap_matches(content: str) -> list[tuple[str, str, str, str]]:
    """
    HTML에서 showMap 패턴을 모두 추출.
    반환: [(위도, 경도, siIdx, raw_name), ...]
    """
    result = []
    for m in _SHOWMAP_PATTERN.finditer(content):
        lat, lon, si_idx, raw_name = m.group(1), m.group(2), m.group(3), m.group(4)
        result.append((lat, lon, si_idx, raw_name))
    return result


def extract_routes_from_content(
    content: str,
) -> list[tuple[str, list[tuple[str, str, str]]]]:
    """
    siIdx='0'인 행을 노선 시작으로 해서, 한 파일에서 여러 노선을 순서대로 추출.
    반환: [(노선명, [(정류장명, 위도, 경도), ...]), ...]
    """
    matches = parse_showmap_matches(content)
    routes: list[tuple[str, list[tuple[str, str, str]]]] = []
    current_name: str | None = None
    current_stations: list[tuple[str, str, str]] = []

    for lat, lon, si_idx, raw_name in matches:
        name = normalize_spaces(strip_html(raw_name))
        if not name:
            continue
        if si_idx == "0":
            # 이전 노선이 있으면 저장 후 새 노선 시작
            if current_name is not None and current_stations:
                routes.append((current_name, current_stations))
            current_name = name
            current_stations = []
        else:
            current_stations.append((name, lat, lon))

    if current_name is not None and current_stations:
        routes.append((current_name, current_stations))

    return routes


def unique_station_order(stations: list[tuple[str, str, str]]) -> list[str]:
    """(정류장명, 위도, 경도) 리스트에서 정류장명만 첫 등장 순서로 반환."""
    seen: set[str] = set()
    order: list[str] = []
    for name, _, _ in stations:
        if name not in seen:
            seen.add(name)
            order.append(name)
    return order


def main() -> None:
    seen: set[tuple[str, str, str]] = set()
    rows: list[tuple[str, str, str]] = []
    route_lines: list[str] = []  # "노선명: 정류장1-정류장2-..."
    seen_route_line: set[str] = set()  # 중복 제거

    for path in sorted(RAW_GEO_DIR.iterdir()):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"Skip {path.name}: {e}")
            continue
        routes = extract_routes_from_content(text)
        for route_name, stations in routes:
            for name, lat, lon in stations:
                key = (name, lat, lon)
                if key not in seen:
                    seen.add(key)
                    rows.append((name, lat, lon))
            ordered_names = unique_station_order(stations)
            if ordered_names:
                line = f"{route_name}: {'-'.join(ordered_names)}"
                if line not in seen_route_line:
                    seen_route_line.add(line)
                    route_lines.append(line)

    TMP_GEO_INFO_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TMP_GEO_INFO_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["stop_name", "lat", "lng"])
        writer.writerows(rows)

    with GEO_ROUTE_INFO_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["route_info"])  # "노선명: 정류장1-정류장2-..."
        writer.writerows([[line] for line in route_lines])

    print(f"총 {len(rows)}개 정류장 정보를 {TMP_GEO_INFO_PATH}에 저장했습니다.")
    print(f"총 {len(route_lines)}개 노선 정보를 {GEO_ROUTE_INFO_PATH}에 저장했습니다.")


if __name__ == "__main__":
    main()
