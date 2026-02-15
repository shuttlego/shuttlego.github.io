#!/usr/bin/env python3
"""
tmp_geo_info.csv → geo_info.csv, tmp_stops.csv → stops.csv 로 복사한 뒤,
stops.csv 위·경도를 geo_info.csv로 보강하고, 없으면 정류장 정보 없음 출력 후 stops.csv 갱신.

실행 전에 routes.csv, tmp_stops.csv(또는 stops.csv) 가 있어야 한다.
extract_geo_info.py, xlsx_to_csv.py 실행 후 이 스크립트를 실행하면 된다.
"""

import csv
import re
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = _ROOT / "data"
STOPS_PATH = DATA_DIR / "stops.csv"
TMP_STOPS_PATH = DATA_DIR / "tmp_stops.csv"
ROUTES_PATH = DATA_DIR / "routes.csv"
GEO_INFO_PATH = DATA_DIR / "geo_info.csv"
TMP_GEO_INFO_PATH = DATA_DIR / "tmp_geo_info.csv"
GEO_ROUTE_INFO_PATH = DATA_DIR / "geo_route_info.csv"
RAW_GEO_DIR = DATA_DIR / "raw_geo"

TYPE_출근 = 1
TYPE_퇴근 = 2


def _geo_key(name: str) -> str:
    """정류장명 비교용: 공백을 모두 제거한 키."""
    return "".join((name or "").strip().split())


def load_geo_info(path: Path) -> dict[str, tuple[str, str]]:
    """geo_info.csv → (공백 제거한 정류장명) → (lat, lng)."""
    result: dict[str, tuple[str, str]] = {}
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("stop_name") or "").strip()
            if not name:
                continue
            key = _geo_key(name)
            if key not in result:
                result[key] = (
                    (row.get("lat") or "").strip(),
                    (row.get("lng") or "").strip(),
                )
    return result


def fill_stops_from_geo_info(
    stops: list[tuple[int, int, str, str, str]],
    geo_info: dict[str, tuple[str, str]],
) -> list[tuple[int, int, str, str, str]]:
    """
    (route_id, sequence, stop_name, lat, lng) 리스트에서 비어 있는 lat/lng를 geo_info로 채움.
    xlsx_to_csv.py 에서 1차 보강용, fill_stop_geo.py 에서 2차 보강용으로 사용.
    """
    result = []
    for route_id, sequence, stop_name, lat, lng in stops:
        if (not lat or not lng) and stop_name:
            key = _geo_key(stop_name)
            if key in geo_info:
                lat, lng = geo_info[key]
        result.append((route_id, sequence, stop_name, lat or "", lng or ""))
    return result


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_stations_ordered(content: str) -> list[tuple[str, str, str]]:
    """raw_geo HTML에서 showMap 패턴으로 (정류장명, 위도, 경도) 순서대로 추출. siIdx='0' 제외."""
    pattern = re.compile(
        r"<a\s+href=\"javascript:showMap\("
        r"'[^']*',\s*'[^']*',\s*'([^']+)',\s*'([^']+)',\s*'([^']+)'"
        r"[^)]*\);\"" r"[^>]*>([\s\S]*?)</a>",
        re.IGNORECASE,
    )
    result = []
    for m in pattern.finditer(content):
        lat, lon, si_idx, raw_name = m.group(1), m.group(2), m.group(3), m.group(4)
        if si_idx == "0":
            continue
        name = _normalize_spaces(_strip_html(raw_name))
        if name:
            result.append((name, lat, lon))
    return result


def load_raw_geo_ordered() -> dict[str, list[tuple[str, str, str]]]:
    """파일명 → 해당 파일 내 정류장 (이름, 위도, 경도) 순서 리스트."""
    out: dict[str, list[tuple[str, str, str]]] = {}
    if not RAW_GEO_DIR.exists():
        return out
    for path in sorted(RAW_GEO_DIR.iterdir()):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        out[path.name] = _extract_stations_ordered(text)
    return out


def suggest_stations_from_raw_geo(
    prev_key: str,
    next_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> list[tuple[str, str]]:
    """직전/직후 정류장(공백 제거 키) 사이에 정확히 1개만 있는 raw_geo 패턴만 찾아, 그 정류장명 후보 수집.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만 반환.
    반환: [(파일명, 정류장명), ...]
    """
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        idx: int
        if not prev_key and next_key:
            try:
                i_next = keys.index(next_key)
            except ValueError:
                continue
            expected_next = (stop_index + 1) if stop_index >= 0 else 1
            if i_next != expected_next:
                continue
            idx = i_next - 1
        elif prev_key and not next_key:
            try:
                i_prev = keys.index(prev_key)
            except ValueError:
                continue
            if stop_index >= 0:
                if i_prev != stop_index - 1:
                    continue
            else:
                if i_prev != len(keys) - 2:
                    continue
            idx = i_prev + 1
        elif prev_key and next_key:
            try:
                i_prev = keys.index(prev_key)
            except ValueError:
                continue
            if stop_index >= 0 and i_prev != stop_index - 1:
                continue
            try:
                i_next = keys.index(next_key, i_prev + 1)
            except ValueError:
                continue
            if i_next != i_prev + 2:
                continue
            idx = i_prev + 1
        else:
            continue
        if idx < 0 or idx >= len(keys):
            continue
        name = stations[idx][0]
        if name not in seen:
            seen.add(name)
            candidates.append((fname, name))
    return candidates


def suggest_first_stop_from_raw_geo_by_two_next(
    next_key: str,
    next_next_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> list[tuple[str, str]]:
    """sequence 1(첫 정류장) 정보 없을 때: 직후 2개(B, C)가 raw_geo에서 연속으로 등장하면 그 앞 1개 정류장을 후보로 수집.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만.
    반환: [(파일명, 정류장명), ...]
    """
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(1, len(keys) - 1):
            if keys[i] == next_key and keys[i + 1] == next_next_key:
                if stop_index >= 0 and (i - 1) != stop_index:
                    break
                name = stations[i - 1][0]
                if name not in seen:
                    seen.add(name)
                    candidates.append((fname, name))
                break
    return candidates


def get_single_suggestion_with_coords_first_stop(
    next_key: str,
    next_next_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> tuple[str, str, str] | None:
    """직후 2개 연속 일치 시 그 앞 1개 정류장의 (이름, lat, lng). 후보가 1개가 아니면 None.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만."""
    candidates: list[tuple[str, str, str]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(1, len(keys) - 1):
            if keys[i] == next_key and keys[i + 1] == next_next_key:
                if stop_index >= 0 and (i - 1) != stop_index:
                    break
                candidates.append(stations[i - 1])
                break
    if not candidates:
        return None
    names = {c[0] for c in candidates}
    if len(names) != 1:
        return None
    return candidates[0]


def suggest_stations_from_raw_geo_by_two_prev(
    prev_prev_key: str,
    prev_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> list[tuple[str, str]]:
    """직후가 없을 때: 직전 정류장 2개가 연속으로 일치하는 raw_geo 구간에서, 그 다음 1개 정류장명을 후보로 수집.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만.
    반환: [(파일명, 정류장명), ...]
    """
    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(len(keys) - 2):
            if keys[i] == prev_prev_key and keys[i + 1] == prev_key:
                if stop_index >= 0 and (i + 2) != stop_index:
                    break
                name = stations[i + 2][0]
                if name not in seen:
                    seen.add(name)
                    candidates.append((fname, name))
                break
    return candidates


def get_single_suggestion_with_coords_by_two_prev(
    prev_prev_key: str,
    prev_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> tuple[str, str, str] | None:
    """직전 2개 연속 일치 시 그 다음 1개 정류장의 (이름, lat, lng). 후보가 1개가 아니면 None.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만."""
    candidates: list[tuple[str, str, str]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(len(keys) - 2):
            if keys[i] == prev_prev_key and keys[i + 1] == prev_key:
                if stop_index >= 0 and (i + 2) != stop_index:
                    break
                candidates.append(stations[i + 2])
                break
    if not candidates:
        return None
    names = {c[0] for c in candidates}
    if len(names) != 1:
        return None
    return candidates[0]


def suggest_consecutive_from_raw_geo(
    prev_key: str,
    next_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, list[str]]]:
    """raw_geo에서 prev 다음에 정확히 gap_size개 정류장이 있고 그 다음에 next가 오는 패턴을 찾아,
    각 파일별로 (파일명, [정류장1, ..., 정류장N]) 리스트들을 반환. (N = gap_size)
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만 반환."""
    if gap_size <= 0:
        return []
    result: list[tuple[str, list[str]]] = []
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        try:
            i_prev = keys.index(prev_key)
        except ValueError:
            continue
        if gap_start_index >= 0 and i_prev != gap_start_index - 1:
            continue
        i_next = i_prev + gap_size + 1
        if i_next >= len(keys) or keys[i_next] != next_key:
            continue
        names = [stations[i_prev + 1 + k][0] for k in range(gap_size)]
        result.append((fname, names))
    return result


def get_consecutive_with_coords(
    prev_key: str,
    next_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, str, str]] | None:
    """suggest_consecutive_from_raw_geo와 동일 패턴에서, 모든 매칭이 동일한 N개 정류장 열이면
    [(이름1, lat1, lng1), ..., (이름N, latN, lngN)] 반환. 후보가 0개거나 서로 다르면 None.
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만."""
    if gap_size <= 0:
        return None
    candidates: list[list[tuple[str, str, str]]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        try:
            i_prev = keys.index(prev_key)
        except ValueError:
            continue
        if gap_start_index >= 0 and i_prev != gap_start_index - 1:
            continue
        i_next = i_prev + gap_size + 1
        if i_next >= len(keys) or keys[i_next] != next_key:
            continue
        row = [stations[i_prev + 1 + k] for k in range(gap_size)]
        candidates.append(row)
    if not candidates:
        return None
    first_names = tuple(candidates[0][k][0] for k in range(gap_size))
    for c in candidates[1:]:
        if tuple(c[k][0] for k in range(gap_size)) != first_names:
            return None
    return candidates[0]


def suggest_consecutive_from_raw_geo_by_two_next(
    next_key: str,
    next_next_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, list[str]]]:
    """직전 없을 때(시작 구간): raw_geo에서 직후 2개(next, next_next)가 연속으로 나오는 앞 gap_size개 정류장을 후보로.
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만 반환."""
    if gap_size <= 0:
        return []
    result: list[tuple[str, list[str]]] = []
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(gap_size, len(keys) - 1):
            if keys[i] == next_key and keys[i + 1] == next_next_key:
                if gap_start_index >= 0 and (i - gap_size) != gap_start_index:
                    break
                names = [stations[i - gap_size + k][0] for k in range(gap_size)]
                result.append((fname, names))
                break
    return result


def suggest_consecutive_from_raw_geo_by_two_prev(
    prev_prev_key: str,
    prev_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, list[str]]]:
    """직후 없을 때(끝 구간): raw_geo에서 직전 2개(prev_prev, prev)가 연속으로 나오는 뒤 gap_size개 정류장을 후보로.
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만 반환."""
    if gap_size <= 0:
        return []
    result: list[tuple[str, list[str]]] = []
    for fname, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(len(keys) - 1 - gap_size):
            if keys[i] == prev_prev_key and keys[i + 1] == prev_key:
                if gap_start_index >= 0 and (i + 2) != gap_start_index:
                    break
                names = [stations[i + 2 + k][0] for k in range(gap_size)]
                result.append((fname, names))
                break
    return result


def get_consecutive_with_coords_by_two_next(
    next_key: str,
    next_next_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, str, str]] | None:
    """직후 2개 연속 패턴에서 gap_size개 앞 정류장의 (이름, lat, lng) 리스트. 후보가 1종이 아니면 None.
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만."""
    if gap_size <= 0:
        return None
    candidates: list[list[tuple[str, str, str]]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(gap_size, len(keys) - 1):
            if keys[i] == next_key and keys[i + 1] == next_next_key:
                if gap_start_index >= 0 and (i - gap_size) != gap_start_index:
                    break
                row = [stations[i - gap_size + k] for k in range(gap_size)]
                candidates.append(row)
                break
    if not candidates:
        return None
    first_names = tuple(candidates[0][k][0] for k in range(gap_size))
    for c in candidates[1:]:
        if tuple(c[k][0] for k in range(gap_size)) != first_names:
            return None
    return candidates[0]


def get_consecutive_with_coords_by_two_prev(
    prev_prev_key: str,
    prev_key: str,
    gap_size: int,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    gap_start_index: int = -1,
) -> list[tuple[str, str, str]] | None:
    """직전 2개 연속 패턴에서 gap_size개 뒤 정류장의 (이름, lat, lng) 리스트. 후보가 1종이 아니면 None.
    gap_start_index >= 0이면, 갭 시작 위치(0-based)가 일치하는 경우만."""
    if gap_size <= 0:
        return None
    candidates: list[list[tuple[str, str, str]]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        for i in range(len(keys) - 1 - gap_size):
            if keys[i] == prev_prev_key and keys[i + 1] == prev_key:
                if gap_start_index >= 0 and (i + 2) != gap_start_index:
                    break
                row = [stations[i + 2 + k] for k in range(gap_size)]
                candidates.append(row)
                break
    if not candidates:
        return None
    first_names = tuple(candidates[0][k][0] for k in range(gap_size))
    for c in candidates[1:]:
        if tuple(c[k][0] for k in range(gap_size)) != first_names:
            return None
    return candidates[0]


def get_single_suggestion_with_coords(
    prev_key: str,
    next_key: str,
    raw_geo_ordered: dict[str, list[tuple[str, str, str]]],
    stop_index: int = -1,
) -> tuple[str, str, str] | None:
    """직전/직후 사이에 정확히 1개만 있는 패턴에서 (정류장명, lat, lng) 반환. 후보가 1개가 아니면 None.
    stop_index >= 0이면, 해당 위치(0-based)가 일치하는 경우만."""
    candidates: list[tuple[str, str, str]] = []
    for _file, stations in raw_geo_ordered.items():
        keys = [_geo_key(name) for name, _, _ in stations]
        idx: int
        if not prev_key and next_key:
            try:
                i_next = keys.index(next_key)
            except ValueError:
                continue
            expected_next = (stop_index + 1) if stop_index >= 0 else 1
            if i_next != expected_next:
                continue
            idx = i_next - 1
        elif prev_key and not next_key:
            try:
                i_prev = keys.index(prev_key)
            except ValueError:
                continue
            if stop_index >= 0:
                if i_prev != stop_index - 1:
                    continue
            else:
                if i_prev != len(keys) - 2:
                    continue
            idx = i_prev + 1
        elif prev_key and next_key:
            try:
                i_prev = keys.index(prev_key)
            except ValueError:
                continue
            if stop_index >= 0 and i_prev != stop_index - 1:
                continue
            try:
                i_next = keys.index(next_key, i_prev + 1)
            except ValueError:
                continue
            if i_next != i_prev + 2:
                continue
            idx = i_prev + 1
        else:
            continue
        if idx < 0 or idx >= len(keys):
            continue
        name, lat, lng = stations[idx]
        candidates.append((name, lat, lng))
    if not candidates:
        return None
    names = {c[0] for c in candidates}
    if len(names) != 1:
        return None
    return candidates[0]


def load_routes(path: Path) -> tuple[dict[int, str], dict[int, int]]:
    """route_id → route_name, route_id → type (1=출근, 2=퇴근)."""
    rid_to_name: dict[int, str] = {}
    rid_to_type: dict[int, int] = {}
    if not path.exists():
        return rid_to_name, rid_to_type
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rid = int(row.get("route_id", 0))
            except ValueError:
                continue
            rid_to_name[rid] = (row.get("route_name") or "").strip()
            try:
                rid_to_type[rid] = int(row.get("type", 1))
            except ValueError:
                rid_to_type[rid] = TYPE_출근
    return rid_to_name, rid_to_type


def load_stops(path: Path) -> list[tuple[int, int, str, str, str]]:
    """(route_id, sequence, stop_name, lat, lng) 리스트."""
    rows = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rid = int(row.get("route_id", 0))
                seq = int(row.get("sequence", 0))
            except ValueError:
                continue
            name = (row.get("stop_name") or "").strip()
            lat = (row.get("lat") or "").strip()
            lng = (row.get("lng") or "").strip()
            rows.append((rid, seq, name, lat, lng))
    return rows


def load_geo_route_info(path: Path) -> list[tuple[str, list[str]]]:
    """geo_route_info.csv → [(노선명, [정류장1, 정류장2, ...]), ...]. route_info 컬럼 '노선명: A-B-C' 형식."""
    result: list[tuple[str, list[str]]] = []
    if not path.exists():
        return result
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            line = (row.get("route_info") or "").strip()
            if ":" not in line:
                continue
            name_part, rest = line.split(":", 1)
            route_name = name_part.strip()
            stations = [s.strip() for s in rest.split("-") if s.strip()]
            if route_name and stations:
                result.append((route_name, stations))
    return result


def _find_route_stations(
    route_name: str,
    sequence: int,
    route_info_list: list[tuple[str, list[str]]],
) -> tuple[str, str]:
    """노선명·sequence에 맞는 직전/직후 정류장명을 geo_route_info 기준으로 반환. 없으면 ('', '')."""
    # 1) 노선명 완전 일치 2) 노선명이 route_info 노선명을 포함 3) route_info 노선명이 노선명을 포함
    candidates: list[tuple[str, list[str]]] = []
    for info_name, stations in route_info_list:
        if info_name == route_name:
            candidates.insert(0, (info_name, stations))
            break
    if not candidates:
        for info_name, stations in route_info_list:
            if route_name in info_name or info_name in route_name:
                candidates.append((info_name, stations))
    if not candidates:
        return ("", "")
    _, stations = candidates[0]
    # sequence는 1-based, 인덱스는 0-based
    idx = sequence - 1
    if idx < 0 or idx >= len(stations):
        return ("", "")
    prev_name = stations[idx - 1] if idx >= 1 else ""
    next_name = stations[idx + 1] if idx + 1 < len(stations) else ""
    return (prev_name, next_name)


def build_consecutive_gaps(
    missing_list: list[tuple[str, int, int, str, str, str, int]],
) -> list[tuple[int, int, int, str, str, list[tuple[int, str]], str, int]]:
    """연속으로 정보가 없는 정류장 구간(gap) 목록 반환.
    반환: [(route_id, seq_start, seq_end, prev_name, next_name, [(seq, stop_name), ...], route_name, route_type), ...]
    """
    by_route: dict[int, list[tuple[int, str, str, str, str, str, int]]] = {}
    for stop_name, route_id, sequence, route_name, prev_name, next_name, route_type in missing_list:
        by_route.setdefault(route_id, []).append(
            (sequence, stop_name, route_name, prev_name, next_name, route_type)
        )
    gaps: list[tuple[int, int, int, str, str, list[tuple[int, str]], str, int]] = []
    for route_id, rows in by_route.items():
        rows.sort(key=lambda r: r[0])
        i = 0
        while i < len(rows):
            seq_start = rows[i][0]
            seq_end = seq_start
            stops_in_gap: list[tuple[int, str]] = [(rows[i][0], rows[i][1])]
            prev_name = rows[i][3]
            next_name = rows[i][4]
            route_name = rows[i][2]
            route_type = rows[i][5]
            j = i + 1
            while j < len(rows) and rows[j][0] == seq_end + 1:
                seq_end = rows[j][0]
                stops_in_gap.append((rows[j][0], rows[j][1]))
                next_name = rows[j][4]
                j += 1
            gaps.append((route_id, seq_start, seq_end, prev_name, next_name, stops_in_gap, route_name, route_type))
            i = j
    return gaps


def build_prev_next(
    stops: list[tuple[int, int, str, str, str]],
) -> dict[tuple[int, int], tuple[str, str]]:
    """(route_id, sequence) → (직전 정류장명, 직후 정류장명)."""
    by_route: dict[int, list[tuple[int, str]]] = {}
    for rid, seq, name, _lat, _lng in stops:
        by_route.setdefault(rid, []).append((seq, name))
    ctx: dict[tuple[int, int], tuple[str, str]] = {}
    for rid, seq_list in by_route.items():
        seq_list.sort(key=lambda x: x[0])
        for i, (seq, name) in enumerate(seq_list):
            prev_name = seq_list[i - 1][1] if i > 0 else ""
            next_name = seq_list[i + 1][1] if i + 1 < len(seq_list) else ""
            ctx[(rid, seq)] = (prev_name, next_name)
    return ctx


def main() -> int:
    if not ROUTES_PATH.exists():
        print(f"파일이 없습니다: {ROUTES_PATH}")
        return 1

    # tmp → 실제 파일 복사 (tmp가 있으면 geo_info.csv, stops.csv를 덮어씀)
    if TMP_GEO_INFO_PATH.exists():
        shutil.copy2(TMP_GEO_INFO_PATH, GEO_INFO_PATH)
        print(f"{TMP_GEO_INFO_PATH} → {GEO_INFO_PATH} 복사")
    if TMP_STOPS_PATH.exists():
        shutil.copy2(TMP_STOPS_PATH, STOPS_PATH)
        print(f"{TMP_STOPS_PATH} → {STOPS_PATH} 복사")

    if not STOPS_PATH.exists():
        print(f"파일이 없습니다: {STOPS_PATH} (tmp_stops.csv 또는 stops.csv 필요)")
        return 1

    rid_to_name, rid_to_type = load_routes(ROUTES_PATH)
    stops = load_stops(STOPS_PATH)
    route_ctx = build_prev_next(stops)

    geo_info = load_geo_info(GEO_INFO_PATH)
    print(f"geo_info.csv 로드: 정류장 {len(geo_info)}개")
    raw_geo_ordered = load_raw_geo_ordered()
    print(f"raw_geo 로드: 파일 {len(raw_geo_ordered)}개")
    route_info_list = load_geo_route_info(GEO_ROUTE_INFO_PATH)
    if route_info_list:
        print(f"geo_route_info.csv 로드: 노선 {len(route_info_list)}개")

    # 2차 보강: 비어 있는 위경도만 geo_info로 채움
    updated = fill_stops_from_geo_info(stops, geo_info)

    # 여전히 비어 있는 정류장 → 정류장 정보 없음 목록
    missing_list: list[tuple[str, int, int, str, str, str, int]] = []
    for route_id, sequence, stop_name, lat, lng in updated:
        if stop_name and (not lat or not lng):
            prev_name, next_name = route_ctx.get((route_id, sequence), ("", ""))
            route_name = rid_to_name.get(route_id, "")
            route_type = rid_to_type.get(route_id, TYPE_출근)
            missing_list.append((stop_name, route_id, sequence, route_name, prev_name, next_name, route_type))

    # 노선별 정류장 수 vs 정보 없는 정류장 수 → 모든 정류장이 정보 없으면 raw_geo에 해당 노선이 없는 것으로 간주
    route_total: dict[int, int] = {}
    route_missing: dict[int, int] = {}
    for route_id, _seq, _name, _lat, _lng in updated:
        route_total[route_id] = route_total.get(route_id, 0) + 1
    for _sn, route_id, _seq, _rn, _pn, _nn, _rt in missing_list:
        route_missing[route_id] = route_missing.get(route_id, 0) + 1
    fully_missing_route_ids: set[int] = {
        rid for rid in route_total
        if route_total.get(rid, 0) > 0 and route_missing.get(rid, 0) == route_total[rid]
    }
    # 직전/직후가 1개 이하로 남은 노선(중간 전부 없음)도 raw_geo 누락 후보로 마지막에 안내
    likely_missing_from_raw_geo: set[int] = {
        rid for rid in route_total
        if route_total.get(rid, 0) > 0 and route_missing.get(rid, 0) >= route_total[rid] - 1
    }

    gaps = build_consecutive_gaps(missing_list)
    in_gap_seqs: set[tuple[int, int]] = set()

    # 1) 연속 N개(≥2) gap 처리: 직전/직후 유무와 관계없이 묶어서 출력. 직전·직후 모두 있으면 raw_geo 패턴 후보 제안
    for route_id, seq_start, seq_end, prev_name, next_name, stops_in_gap, route_name, route_type in gaps:
        if route_id in fully_missing_route_ids:
            continue
        if len(stops_in_gap) < 2:
            continue
        N = len(stops_in_gap)
        # 연속 구간은 항상 묶어서 출력하므로 단일 정류장 루프에서 제외
        for seq, _ in stops_in_gap:
            in_gap_seqs.add((route_id, seq))
        prev_key = _geo_key(prev_name) if prev_name else ""
        next_key = _geo_key(next_name) if next_name else ""
        prev_repr = repr(prev_name) if prev_name else "(없음)"
        next_repr = repr(next_name) if next_name else "(없음)"
        type_label = "출근" if route_type == TYPE_출근 else "퇴근"
        stop_names_repr = ", ".join(repr(sn) for _seq, sn in stops_in_gap)
        print(f"  [정류장 정보 없음 (연속{N}개)] {type_label} 노선={route_name!r} sequence={seq_start}~{seq_end} 정류장={stop_names_repr}")
        print(f"    직전: {prev_repr}")
        print(f"    직후: {next_repr}")
        def apply_gap_candidates(coords_list: list[tuple[str, str, str]]) -> None:
            choice = input("    (1) stops.csv 수정 (2) geo_info.csv 수정 (3) 수정하지 않음 [기본 1]: ").strip() or "1"
            if choice == "1":
                for idx, (seq, _stop_name) in enumerate(stops_in_gap):
                    cname, clat, clng = coords_list[idx]
                    for i, row in enumerate(updated):
                        if row[0] == route_id and row[1] == seq and (not row[3] or not row[4]):
                            updated[i] = (row[0], row[1], cname, clat, clng)
                            break
            elif choice == "2":
                write_header = not TMP_GEO_INFO_PATH.exists()
                for idx, (seq, stop_name_cur) in enumerate(stops_in_gap):
                    cname, clat, clng = coords_list[idx]
                    row = [stop_name_cur, clat, clng]
                    with GEO_INFO_PATH.open("a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow(row)
                    with TMP_GEO_INFO_PATH.open("a", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        if write_header:
                            w.writerow(["stop_name", "lat", "lng"])
                            write_header = False
                        w.writerow(row)
            print()

        gap_idx = seq_start - 1  # 0-based index of gap start in raw_geo
        candidate_lists: list[tuple[str, list[str]]] = []
        coords_list = None
        if prev_key and next_key:
            candidate_lists = suggest_consecutive_from_raw_geo(prev_key, next_key, N, raw_geo_ordered, gap_idx)
            if candidate_lists:
                unique_map: dict[tuple[str, ...], list[str]] = {}
                for src, names in candidate_lists:
                    unique_map.setdefault(tuple(names), []).append(src)
                unique = [list(k) for k in unique_map]
                sources_str = ", ".join(sorted({s for sl in unique_map.values() for s in sl}))
                print(f"    후보: {unique} (raw_geo [{sources_str}] 직전-직후 사이 {N}개 연속)")
                if len(unique) == 1:
                    coords_list = get_consecutive_with_coords(prev_key, next_key, N, raw_geo_ordered, gap_idx)
        elif not prev_key and next_key and route_total.get(route_id, 0) >= seq_end + 2:
            next_next_name = route_ctx.get((route_id, seq_end + 1), ("", ""))[1]
            next_next_key = _geo_key(next_next_name) if next_next_name else ""
            if next_next_key:
                candidate_lists = suggest_consecutive_from_raw_geo_by_two_next(
                    next_key, next_next_key, N, raw_geo_ordered, gap_idx
                )
                if candidate_lists:
                    unique_map = {}
                    for src, names in candidate_lists:
                        unique_map.setdefault(tuple(names), []).append(src)
                    unique = [list(k) for k in unique_map]
                    sources_str = ", ".join(sorted({s for sl in unique_map.values() for s in sl}))
                    print(f"    후보: {unique} (raw_geo [{sources_str}] 직후 2개 연속 일치 앞 {N}개)")
                    if len(unique) == 1:
                        coords_list = get_consecutive_with_coords_by_two_next(
                            next_key, next_next_key, N, raw_geo_ordered, gap_idx
                        )
        elif prev_key and not next_key and seq_start >= 3:
            prev_prev_name = route_ctx.get((route_id, seq_start - 1), ("", ""))[0]
            prev_prev_key = _geo_key(prev_prev_name) if prev_prev_name else ""
            if prev_prev_key:
                candidate_lists = suggest_consecutive_from_raw_geo_by_two_prev(
                    prev_prev_key, prev_key, N, raw_geo_ordered, gap_idx
                )
                if candidate_lists:
                    unique_map = {}
                    for src, names in candidate_lists:
                        unique_map.setdefault(tuple(names), []).append(src)
                    unique = [list(k) for k in unique_map]
                    sources_str = ", ".join(sorted({s for sl in unique_map.values() for s in sl}))
                    print(f"    후보: {unique} (raw_geo [{sources_str}] 직전 2개 연속 일치 뒤 {N}개)")
                    if len(unique) == 1:
                        coords_list = get_consecutive_with_coords_by_two_prev(
                            prev_prev_key, prev_key, N, raw_geo_ordered, gap_idx
                        )
        if candidate_lists:
            if coords_list is not None:
                apply_gap_candidates(coords_list)
        else:
            if not prev_key and not next_key:
                print(f"    후보: (직전·직후 모두 없음으로 raw_geo 패턴 검색 불가)")
            elif not prev_key:
                print(f"    후보: (직전 없음, 직후 2개 패턴 없음)")
            elif not next_key:
                print(f"    후보: (직후 없음, 직전 2개 패턴 없음)")
            else:
                print(f"    후보: (raw_geo에서 직전-직후 사이 {N}개 패턴 없음)")
        print()

    # 2) 단일(또는 gap에 포함되지 않은) 정류장 정보 없음 출력 (중복 제거)
    seen_key: set[str] = set()
    for stop_name, route_id, sequence, route_name, prev_name, next_name, route_type in missing_list:
        if route_id in fully_missing_route_ids:
            continue
        if (route_id, sequence) in in_gap_seqs:
            continue
        key = _geo_key(stop_name)
        if key in seen_key:
            continue
        seen_key.add(key)
        prev_key = _geo_key(prev_name) if prev_name else ""
        next_key = _geo_key(next_name) if next_name else ""
        prev_prev_name = ""
        if sequence >= 2:
            prev_prev_name = route_ctx.get((route_id, sequence - 1), ("", ""))[0]
        prev_prev_key = _geo_key(prev_prev_name) if prev_prev_name else ""

        stop_idx = sequence - 1  # 0-based index
        suggestions: list[tuple[str, str]] = suggest_stations_from_raw_geo(
            prev_key, next_key, raw_geo_ordered, stop_idx
        )
        used_route_info_fallback = False
        if not suggestions and (not prev_key or not next_key) and route_info_list:
            prev_alt, next_alt = _find_route_stations(route_name, sequence, route_info_list)
            prev_key_alt = prev_key or (_geo_key(prev_alt) if prev_alt else "")
            next_key_alt = next_key or (_geo_key(next_alt) if next_alt else "")
            if prev_key_alt or next_key_alt:
                suggestions = suggest_stations_from_raw_geo(
                    prev_key_alt, next_key_alt, raw_geo_ordered, stop_idx
                )
                if suggestions:
                    used_route_info_fallback = True
                    prev_key, next_key = prev_key_alt, next_key_alt
        used_two_prev_fallback = False
        if not suggestions and not next_key and prev_key and prev_prev_key:
            suggestions = suggest_stations_from_raw_geo_by_two_prev(
                prev_prev_key, prev_key, raw_geo_ordered, stop_idx
            )
            if suggestions:
                used_two_prev_fallback = True
        # sequence 1이고 노선 정류장 3개 이상일 때: 직후 2개(B, C)가 raw_geo에서 연속으로 나오면 그 앞 정류장을 후보로
        used_first_stop_two_next_fallback = False
        if not suggestions and sequence == 1 and route_total.get(route_id, 0) >= 3 and next_key:
            next_next_name = route_ctx.get((route_id, 2), ("", ""))[1]
            next_next_key = _geo_key(next_next_name) if next_next_name else ""
            if next_next_key:
                suggestions = suggest_first_stop_from_raw_geo_by_two_next(
                    next_key, next_next_key, raw_geo_ordered, stop_idx
                )
                if suggestions:
                    used_first_stop_two_next_fallback = True

        sugg_names = [name for _, name in suggestions]
        sugg_sources = sorted({src for src, _ in suggestions})
        sources_label = f" [{', '.join(sugg_sources)}]" if sugg_sources else ""

        prev_repr = repr(prev_name) if prev_name else "(없음)"
        next_repr = repr(next_name) if next_name else "(없음)"
        type_label = "출근" if route_type == TYPE_출근 else "퇴근"
        print(f"  [정류장 정보 없음] {type_label} 노선={route_name!r} sequence={sequence} 정류장={stop_name!r}")
        print(f"    직전: {prev_repr}")
        print(f"    직후: {next_repr}")
        if suggestions:
            if used_first_stop_two_next_fallback:
                print(f"    후보: {sugg_names} (raw_geo{sources_label} 직후 2개 연속 일치 구간)")
            elif used_two_prev_fallback:
                print(f"    후보: {sugg_names} (raw_geo{sources_label} 직전 2개 연속 일치 구간)")
            elif used_route_info_fallback:
                print(f"    후보: {sugg_names} (geo_route_info 노선·sequence로 직전/직후 보정{sources_label})")
            else:
                print(f"    후보: {sugg_names} (raw_geo{sources_label})")
        else:
            print(f"    후보: (raw_geo에서 직전/직후로 없음)")
        print()

        if len(suggestions) == 1:
            if used_first_stop_two_next_fallback:
                single = get_single_suggestion_with_coords_first_stop(
                    next_key, next_next_key, raw_geo_ordered, stop_idx
                )
            elif used_two_prev_fallback:
                single = get_single_suggestion_with_coords_by_two_prev(
                    prev_prev_key, prev_key, raw_geo_ordered, stop_idx
                )
            else:
                single = get_single_suggestion_with_coords(
                    prev_key, next_key, raw_geo_ordered, stop_idx
                )
            if single is not None:
                cand_name, cand_lat, cand_lng = single
                choice = input("    (1) stops.csv 수정 (2) geo_info.csv 수정 (3) 수정하지 않음 [기본 1]: ").strip() or "1"
                if choice == "1":
                    for i, row in enumerate(updated):
                        if _geo_key(row[2]) == key and (not row[3] or not row[4]):
                            updated[i] = (row[0], row[1], cand_name, cand_lat, cand_lng)
                elif choice == "2":
                    row = [stop_name, cand_lat, cand_lng]
                    with GEO_INFO_PATH.open("a", newline="", encoding="utf-8") as f:
                        csv.writer(f).writerow(row)
                    write_header = not TMP_GEO_INFO_PATH.exists()
                    with TMP_GEO_INFO_PATH.open("a", newline="", encoding="utf-8") as f:
                        w = csv.writer(f)
                        if write_header:
                            w.writerow(["stop_name", "lat", "lng"])
                        w.writerow(row)
                print()

    # 3) 모든 정류장 또는 직전/직후 1개 이하만 남은 노선 → raw_geo에 해당 노선이 없는 것 같다고 맨 마지막에 안내
    if likely_missing_from_raw_geo:
        names_sorted = sorted(rid_to_name.get(rid, "") or str(rid) for rid in likely_missing_from_raw_geo)
        print(f"  [raw_geo에 노선 정보 없음] 해당 노선 정보가 raw_geo에 없는 것 같습니다: {names_sorted}")
        print()

    # stops.csv 갱신
    with STOPS_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "sequence", "stop_name", "lat", "lng"])
        for rid, seq, name, lat, lng in updated:
            w.writerow([rid, seq, name, lat, lng])

    # 다음 실행 시 유지되도록 수정된 결과를 tmp에 덮어쓰기
    shutil.copy2(GEO_INFO_PATH, TMP_GEO_INFO_PATH)
    shutil.copy2(STOPS_PATH, TMP_STOPS_PATH)
    print(f"저장 완료: {STOPS_PATH}")
    print(f"반영 완료: {GEO_INFO_PATH} → {TMP_GEO_INFO_PATH}, {STOPS_PATH} → {TMP_STOPS_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
