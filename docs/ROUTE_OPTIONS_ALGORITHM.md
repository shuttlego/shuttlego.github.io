# 노선 최대 3개 안내 — 구현 아이디어

## 목표

- 사용자 위치 기준 **가장 가까운 정류장들**을 거리 순으로 보되,
- **노선(route_id) 기준 유니크하게 최대 3개**만 안내하고,
- 각 노선에서는 **그 노선 소속 정류장 중 가장 가까운 정류장 1개**만 사용한다.

예: 가까운 정류장 5개가  
A(노선1), B(노선1), C(노선1), D(노선2), E(노선3) 이면  
→ 노선 3개만 안내하고, 노선1은 A/B/C 중 가장 가까운 정류장(즉 A)을 사용.

---

## 알고리즘

### 1단계: 후보 (거리, 정류장, 노선) 목록 만들기

- `site_id`, `route_type`(출근 1 / 퇴근 2)에 해당하는 **모든 노선**의 **모든 정류장**에 대해  
  사용자 위치 `(lat, lng)`와의 거리 `d`를 계산한다.
- 각각 `(d, stop, route)` 형태로 리스트에 넣는다.  
  (이미 있는 `_haversine_km`, `get_routes_by_site_and_type`, `stops[route_id]` 활용.)

### 2단계: 거리 순 정렬

- 위 리스트를 **거리 d 오름차순**으로 정렬한다.  
  → “가까운 정류장들” 순서가 된다.

### 3단계: “노선당 첫 등장 = 해당 노선에서 가장 가까운 정류장”

- 정렬된 리스트를 **앞에서부터** 순회한다.
- **이미 선택한 노선(route_id)은 건너뛴다.**
- **처음 등장하는 노선**이면:
  - 이 정류장이 곧 “이 노선에서 사용자에게 가장 가까운 정류장”이다 (거리 순이므로).
  - 이 노선을 “선택된 노선”에 넣고, 결과에 `(route, stop, route_stops)`를 추가한다.
- **선택된 노선이 3개**가 되면 중단한다.

의사 코드:

```text
candidates = []
for route in get_routes_by_site_and_type(site_id, route_type):
    for stop in stops[route["route_id"]]:
        d = haversine(lat, lng, stop)
        candidates.append((d, stop, route))

candidates.sort(key=lambda x: x[0])   # 거리 순

seen_route_ids = set()
results = []
for d, stop, route in candidates:
    if route["route_id"] in seen_route_ids:
        continue
    seen_route_ids.add(route["route_id"])
    results.append((route, stop, stops[route["route_id"]]))
    if len(results) >= 3:
        break
return results
```

이렇게 하면:

- “가장 가까운 정류장들”이 아니라 “가장 가까운 **정류장 순서**”를 기준으로,
- 그 순서에서 **노선별로 첫 등장할 때만** 채택하므로,
- 각 노선에 대해 **그 노선 소속 정류장 중 가장 가까운 정류장 1개**만 쓰게 되고,
- 최대 3개 **유니크 노선**만 반환한다.

---

## 반환 형태 제안

기존 `find_nearest_stop`이 `(route, stop, route_stops) | None` 한 건만 반환하므로,  
“최대 3개” 버전은 **리스트**로 두는 게 자연스럽다.

- 함수 시그니처 예:  
  `find_nearest_route_options(site_id: int, route_type: int, lat: float, lng: float, max_routes: int = 3) -> list[tuple[dict, dict, list[dict]]]`
- 각 요소는 기존과 동일한 `(route, nearest_stop, route_stops)`.
- 순서: 사용자 위치에서 “그 노선의 가장 가까운 정류장”까지의 거리가 가까운 순 (위 알고리즘에서 채택된 순서가 이미 그 순서임).

API에서는 이 리스트를 그대로 JSON 배열로 넘기면 되고,  
프론트에서는 “노선 3개 중 하나 선택 → 선택한 노선의 (route, stop, route_stops)로 기존처럼 지도/메시지 생성”하면 된다.

---

## 예시 (의도 확인)

- 정류장 거리 순:  
  1. 0.2km A(노선1), 2. 0.3km B(노선1), 3. 0.4km C(노선1), 4. 0.5km D(노선2), 5. 0.6km E(노선3)
- 1번째: 노선1 첫 등장 → (노선1, A, 노선1 정류장 목록) 추가.
- 2·3번째: 노선1 이미 있음 → 스킵.
- 4번째: 노선2 첫 등장 → (노선2, D, …) 추가.
- 5번째: 노선3 첫 등장 → (노선3, E, …) 추가.
- 결과: 3개 노선, 노선1은 A, 노선2는 D, 노선3은 E 사용. ✓

---

## 기존 코드와의 공존

- `find_nearest_stop`: 기존처럼 “딱 1개 노선”이 필요할 때(예: PC에서 단일 노선만 표시) 그대로 사용.
- `find_nearest_route_options`: “최대 3개 노선 후보”가 필요할 때(모바일·새 API)만 사용하도록 하면, 기존 동작을 바꾸지 않고 확장할 수 있다.

이렇게 구현하면 말씀하신 “가장 가까운 정류장들 기준 유니크 노선 최대 3개, 각 노선은 그중 가장 가까운 정류장으로 안내”가 그대로 만족된다.
