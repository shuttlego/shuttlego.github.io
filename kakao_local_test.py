"""
카카오 로컬 API 테스트: 장소 검색 → 결과 목록 → 선택한 장소의 위도/경도 조회

- (1) 키워드로 장소 검색
- (2) 검색 결과 목록 표시
- (3) 사용자가 선택한 장소의 위도(latitude), 경도(longitude) 출력

문서: https://developers.kakao.com/docs/latest/ko/local/dev-guide#search-by-keyword
API 키: 카카오 디벨로퍼스 > 앱 > 앱 키 > REST API 키 (로컬 API 권한 활성화)
"""
from __future__ import annotations

import os
import sys
from typing import Any

import requests

BASE_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"


def get_api_key() -> str | None:
    """환경변수 KAKAO_REST_API_KEY 반환. 없으면 None."""
    return os.environ.get("KAKAO_REST_API_KEY")


def search_places(api_key: str, query: str, *, page: int = 1, size: int = 15) -> list[dict[str, Any]]:
    """
    키워드로 장소 검색. documents 리스트 반환.
    카카오 API: x=경도(longitude), y=위도(latitude).
    """
    headers = {"Authorization": f"KakaoAK {api_key}"}
    params = {"query": query, "page": page, "size": size}
    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    return data.get("documents") or []


def run_interactive() -> None:
    """(1) 검색 (2) 결과 목록 출력 (3) 번호 선택 시 해당 장소 위도/경도 출력."""
    api_key = get_api_key()
    if not api_key:
        print("환경변수 KAKAO_REST_API_KEY 를 설정해주세요.")
        print("예: export KAKAO_REST_API_KEY=your_rest_api_key")
        sys.exit(1)

    query = input("검색할 장소 키워드: ").strip()
    if not query:
        print("키워드를 입력해주세요.")
        sys.exit(1)

    places = search_places(api_key, query)
    if not places:
        print("검색 결과가 없습니다.")
        return

    print(f"\n[검색 결과 총 {len(places)}건]\n")
    for i, doc in enumerate(places, start=1):
        name = doc.get("place_name", "")
        addr = doc.get("address_name", "")
        print(f"  {i}. {name}  |  {addr}")

    try:
        choice = input(f"\n선택할 번호 (1~{len(places)}): ").strip()
        idx = int(choice)
        if idx < 1 or idx > len(places):
            print("유효한 번호를 입력해주세요.")
            return
    except ValueError:
        print("숫자를 입력해주세요.")
        return

    selected = places[idx - 1]
    # API 문서: x=경도(longitude), y=위도(latitude). 응답은 문자열일 수 있음.
    x = selected.get("x")
    y = selected.get("y")
    longitude = float(x) if x else None
    latitude = float(y) if y else None

    print("\n[선택한 장소]")
    print(f"  장소명: {selected.get('place_name', '')}")
    print(f"  주소:   {selected.get('address_name', '')}")
    print(f"  위도(latitude):  {latitude}")
    print(f"  경도(longitude): {longitude}")


if __name__ == "__main__":
    run_interactive()
