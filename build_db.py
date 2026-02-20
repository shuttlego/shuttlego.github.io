#!/usr/bin/env python3
"""
Raw HTML 파일(data/raw/)을 파싱하여 SQLite DB(data/data.db)를 생성하는 ETL 스크립트.

Usage:
    python build_db.py
"""

import csv
import os
import re
import sqlite3
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
RAW_DIR = DATA_DIR / "raw"
DB_PATH = DATA_DIR / "data.db"
SITES_CSV = DATA_DIR / "sites.csv"

# ── 파일명 매핑 ───────────────────────────────────────────────
ROUTE_TYPE_MAP = {"1": "commute_in", "2": "commute_out", "5": "shuttle"}
DAY_TYPE_MAP = {
    "123001": "weekday",
    "123002": "saturday",
    "123003": "holiday",
    "123004": "monday",
    "123007": "familyday",
}

# ── SQLite 스키마 ─────────────────────────────────────────────
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS site (
    site_id TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS route (
    route_id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT NOT NULL REFERENCES site(site_id),
    route_name TEXT NOT NULL,
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    UNIQUE(site_id, route_name, route_type)
);

CREATE TABLE IF NOT EXISTS service_variant (
    variant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    route_id INTEGER NOT NULL REFERENCES route(route_id),
    day_type TEXT NOT NULL CHECK(day_type IN ('weekday','saturday','holiday','monday','familyday')),
    departure_time TEXT NOT NULL,
    company TEXT,
    bus_count INTEGER,
    UNIQUE(route_id, day_type, departure_time, company)
);

CREATE TABLE IF NOT EXISTS stop (
    stop_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    lat REAL NOT NULL,
    lon REAL NOT NULL,
    UNIQUE(lat, lon)
);

CREATE TABLE IF NOT EXISTS variant_stop (
    variant_id INTEGER NOT NULL REFERENCES service_variant(variant_id),
    seq INTEGER NOT NULL,
    stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    PRIMARY KEY(variant_id, seq)
);

CREATE TABLE IF NOT EXISTS stop_scope (
    stop_id INTEGER NOT NULL REFERENCES stop(stop_id),
    site_id TEXT NOT NULL REFERENCES site(site_id),
    route_type TEXT NOT NULL CHECK(route_type IN ('commute_in','commute_out','shuttle')),
    day_type TEXT NOT NULL CHECK(day_type IN ('weekday','saturday','holiday','monday','familyday')),
    PRIMARY KEY(stop_id, site_id, route_type, day_type)
);

CREATE VIRTUAL TABLE IF NOT EXISTS stop_rtree USING rtree(
    stop_id, min_lat, max_lat, min_lon, max_lon
);

CREATE INDEX IF NOT EXISTS idx_route_site_type ON route(site_id, route_type);
CREATE INDEX IF NOT EXISTS idx_variant_route_day ON service_variant(route_id, day_type);
CREATE INDEX IF NOT EXISTS idx_variant_stop_stop ON variant_stop(stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_scope_site_type_day ON stop_scope(site_id, route_type, day_type, stop_id);
CREATE INDEX IF NOT EXISTS idx_stop_scope_stop ON stop_scope(stop_id);
"""


# ── HTML 파싱 ─────────────────────────────────────────────────
def parse_filename(filename: str) -> tuple[str, str, str] | None:
    """파일명에서 (site_id, route_type, day_type) 추출. 실패 시 None."""
    base = filename.rsplit(".", 1)[0]  # 확장자 제거
    parts = base.split("_")
    if len(parts) != 3:
        return None
    site_id, rt_code, dt_code = parts
    route_type = ROUTE_TYPE_MAP.get(rt_code)
    day_type = DAY_TYPE_MAP.get(dt_code)
    if not route_type or not day_type:
        return None
    return site_id, route_type, day_type


def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", "", text).strip()


def _format_time(hhmm: str) -> str:
    """'0612' → '06:12'"""
    hhmm = hhmm.strip()
    if len(hhmm) == 4 and hhmm.isdigit():
        return f"{hhmm[:2]}:{hhmm[2:]}"
    return hhmm


def parse_html_file(content: str) -> list[dict]:
    """
    HTML 내용을 파싱하여 노선 블록 리스트를 반환.
    각 블록: {route_name, departure_time, company, bus_count, stops: [(name, lat, lon), ...]}
    """
    if "등록된 게시글이 없습니다" in content:
        return []

    blocks_raw = re.split(r'<th scope="row">노선명</th>', content)[1:]
    if not blocks_raw:
        return []

    results = []
    seen_keys: set[tuple[str, str]] = set()  # (raw_route_id, departure_time) 중복 제거

    for block in blocks_raw:
        # ── 노선명 추출 ──
        td_match = re.search(r"<td[^>]*>(.*?)</td>", block, re.DOTALL)
        if not td_match:
            continue
        td_clean = _strip_html(td_match.group(1))

        # showMap 인자에서 raw_route_id, departure_time 추출
        sm_match = re.search(r"showMap\(([^)]+)\)", td_clean)
        if not sm_match:
            continue
        args = [a.strip().strip("'") for a in sm_match.group(1).split(",")]
        if len(args) < 7:
            continue
        raw_route_id = args[0]
        dep_time_raw = args[1]

        # 중복 제거 (첫 등장만 사용)
        dedup_key = (raw_route_id, dep_time_raw)
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)

        # 노선명: '>' 뒤, '/a>' 앞
        name_match = re.search(r">(.+?)/a>$", td_clean)
        if not name_match:
            # fallback: '>' 뒤의 텍스트
            parts = td_clean.rsplit(">", 1)
            route_name = parts[-1].strip() if len(parts) > 1 else td_clean
        else:
            route_name = name_match.group(1).strip()

        if not route_name:
            continue

        # ── 메타데이터 추출 ──
        company_match = re.search(
            r"업체명.*?<td[^>]*>([^<]+)</td>", block, re.DOTALL
        )
        company = company_match.group(1).strip() if company_match else ""

        time_match = re.search(r"출발시간.*?<td>.*?>(\d{4})", block, re.DOTALL)
        departure_time = (
            _format_time(time_match.group(1)) if time_match else _format_time(dep_time_raw)
        )

        bus_match = re.search(r"운행대수.*?<td[^>]*>(\d+)</td>", block, re.DOTALL)
        bus_count = int(bus_match.group(1)) if bus_match else None

        # ── 경유지(정류장) 추출 ──
        stop_section = re.search(
            r"경유지.*?<td[^>]*>(.*?)</td>", block, re.DOTALL
        )
        stops: list[tuple[str, float, float]] = []
        if stop_section:
            stop_html = stop_section.group(1)
            stop_pattern = re.compile(
                r"showMap\('[^']+',\s*'[^']+',\s*'([^']+)',\s*'([^']+)',\s*'[^']+',\s*false,\s*\d+\)[^>]*>([^<]+)<"
            )
            for m in stop_pattern.finditer(stop_html):
                lat_str, lon_str, stop_name = m.group(1), m.group(2), m.group(3).strip()
                try:
                    lat = float(lat_str)
                    lon = float(lon_str)
                except ValueError:
                    continue
                if stop_name:
                    stops.append((stop_name, lat, lon))

        if not stops:
            continue

        results.append(
            {
                "route_name": route_name,
                "departure_time": departure_time,
                "company": company,
                "bus_count": bus_count,
                "stops": stops,
            }
        )

    return results


# ── DB 적재 ───────────────────────────────────────────────────
def build_database() -> None:
    # 기존 DB 삭제 후 재생성
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(str(DB_PATH))
    conn.executescript(SCHEMA_SQL)

    # 1) sites.csv → site 테이블
    with open(SITES_CSV, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                "INSERT OR IGNORE INTO site (site_id, name) VALUES (?, ?)",
                (row["site_id"].strip(), row["site_name"].strip()),
            )
    conn.commit()

    # 캐시: stop (lat, lon) → stop_id
    stop_cache: dict[tuple[float, float], int] = {}
    # 캐시: route (site_id, route_name, route_type) → route_id
    route_cache: dict[tuple[str, str, str], int] = {}

    total_variants = 0
    total_stops_inserted = 0

    # 2) raw 파일 순회
    raw_files = sorted(f for f in os.listdir(RAW_DIR) if os.path.isfile(RAW_DIR / f))
    for fname in raw_files:
        parsed = parse_filename(fname)
        if not parsed:
            print(f"  Skip (bad filename): {fname}")
            continue
        site_id, route_type, day_type = parsed

        fpath = RAW_DIR / fname
        try:
            content = fpath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  Skip (read error): {fname}: {e}")
            continue

        blocks = parse_html_file(content)
        if not blocks:
            continue

        for blk in blocks:
            route_name = blk["route_name"]
            departure_time = blk["departure_time"]
            company = blk["company"]
            bus_count = blk["bus_count"]
            stop_list = blk["stops"]

            # route upsert
            route_key = (site_id, route_name, route_type)
            if route_key not in route_cache:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO route (site_id, route_name, route_type) VALUES (?, ?, ?)",
                    route_key,
                )
                if cur.lastrowid and cur.rowcount > 0:
                    route_cache[route_key] = cur.lastrowid
                else:
                    row = conn.execute(
                        "SELECT route_id FROM route WHERE site_id=? AND route_name=? AND route_type=?",
                        route_key,
                    ).fetchone()
                    route_cache[route_key] = row[0]
            route_id = route_cache[route_key]

            # service_variant upsert
            cur = conn.execute(
                "INSERT OR IGNORE INTO service_variant (route_id, day_type, departure_time, company, bus_count) "
                "VALUES (?, ?, ?, ?, ?)",
                (route_id, day_type, departure_time, company, bus_count),
            )
            if cur.lastrowid and cur.rowcount > 0:
                variant_id = cur.lastrowid
            else:
                row = conn.execute(
                    "SELECT variant_id FROM service_variant "
                    "WHERE route_id=? AND day_type=? AND departure_time=? AND company=?",
                    (route_id, day_type, departure_time, company),
                ).fetchone()
                if not row:
                    continue
                variant_id = row[0]
            total_variants += 1

            # stops + variant_stop
            # 기존 variant_stop 삭제 후 재삽입
            conn.execute("DELETE FROM variant_stop WHERE variant_id=?", (variant_id,))
            for seq, (stop_name, lat, lon) in enumerate(stop_list, start=1):
                # stop upsert
                # 위경도를 소수점 7자리로 반올림하여 유니크 키로 사용
                lat_r = round(lat, 7)
                lon_r = round(lon, 7)
                stop_key = (lat_r, lon_r)
                if stop_key not in stop_cache:
                    cur = conn.execute(
                        "INSERT OR IGNORE INTO stop (name, lat, lon) VALUES (?, ?, ?)",
                        (stop_name, lat_r, lon_r),
                    )
                    if cur.lastrowid and cur.rowcount > 0:
                        stop_cache[stop_key] = cur.lastrowid
                        total_stops_inserted += 1
                    else:
                        row = conn.execute(
                            "SELECT stop_id FROM stop WHERE lat=? AND lon=?",
                            (lat_r, lon_r),
                        ).fetchone()
                        stop_cache[stop_key] = row[0]
                stop_id = stop_cache[stop_key]

                conn.execute(
                    "INSERT OR REPLACE INTO variant_stop (variant_id, seq, stop_id) VALUES (?, ?, ?)",
                    (variant_id, seq, stop_id),
                )

        conn.commit()
        print(f"  {fname}: {len(blocks)} blocks loaded")

    # 3) stop_rtree 동기화
    conn.execute("DELETE FROM stop_rtree")
    conn.execute(
        "INSERT INTO stop_rtree (stop_id, min_lat, max_lat, min_lon, max_lon) "
        "SELECT stop_id, lat, lat, lon, lon FROM stop"
    )

    # 4) stop_scope 동기화
    # 정류장별로 (site_id, route_type, day_type) 소속 관계를 물리화한다.
    conn.execute("DELETE FROM stop_scope")
    conn.execute(
        """
        INSERT INTO stop_scope (stop_id, site_id, route_type, day_type)
        SELECT DISTINCT
            vs.stop_id,
            r.site_id,
            r.route_type,
            sv.day_type
        FROM variant_stop vs
        JOIN service_variant sv ON sv.variant_id = vs.variant_id
        JOIN route r ON r.route_id = sv.route_id
        """
    )
    conn.commit()

    # 통계
    site_count = conn.execute("SELECT COUNT(*) FROM site").fetchone()[0]
    route_count = conn.execute("SELECT COUNT(*) FROM route").fetchone()[0]
    variant_count = conn.execute("SELECT COUNT(*) FROM service_variant").fetchone()[0]
    stop_count = conn.execute("SELECT COUNT(*) FROM stop").fetchone()[0]
    vs_count = conn.execute("SELECT COUNT(*) FROM variant_stop").fetchone()[0]
    rtree_count = conn.execute("SELECT COUNT(*) FROM stop_rtree").fetchone()[0]
    stop_scope_count = conn.execute("SELECT COUNT(*) FROM stop_scope").fetchone()[0]

    print(f"\n=== Build Complete ===")
    print(f"  Sites:            {site_count}")
    print(f"  Routes:           {route_count}")
    print(f"  Service Variants: {variant_count}")
    print(f"  Stops:            {stop_count}")
    print(f"  Variant Stops:    {vs_count}")
    print(f"  Stop Scopes:      {stop_scope_count}")
    print(f"  RTree entries:    {rtree_count}")
    print(f"  DB file:          {DB_PATH}")

    conn.close()


if __name__ == "__main__":
    build_database()
