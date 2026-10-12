# 실시간 버스 위치 / GPS ETA 작업 공간

이 checkout은 `codex/live-bus-gps-eta` 기능 브랜치다.
원래 `/Users/brad/shuttle-go-web`의 main checkout은 다른 수정 작업을 위해 그대로 둔다.

대응 backend checkout:
`/Users/brad/.codex/worktrees/live-bus-gps-eta/shuttle-go-backend`

최종 설계 및 Dev 검증 계획:
`docs/design/live-bus-positions-and-gps-eta-final-2026-10-06.md` (backend checkout)

공통 frontend 변경은 이 checkout과 backend 기능 checkout에 함께 반영한다.
별도 앱 버전 HTML을 만들지 않는다. main 파일이나 main의 8080 서버를 시험 때문에 교체하지 않는다.
초기 기능 preview는 8180을 사용하고 Dev 실환경 배포는 전용 시험 시간대에 진행한다.
새 asset은 Pages 배포 workflow에 포함하고 원본 APK·decompiled vendor 코드·secret을 web 배포에 넣지 않는다.

2026-10-11 후속 상태: GPS 위치/ETA·도착 제보·혼잡도·경로 개선 기능과 관련 화면의 구현 및 Dev 검증을 완료했다.
공유 프론트는 `/Users/brad/shuttle-go-backend`와 `/Users/brad/shuttle-go-web`에 동일하게 반영한다.
출발편 전환 중 유효한 위치를 유지하고 선택 색상을 먼저 반영하며, 무입력 5분으로 중단된 흐린 말풍선으로도 위치 확인을 재개한다. JS 자원 버전은 15다.
최근 검증 기록은 backend의 `docs/analysis/gps-vehicle-bubble-handoff-dev-validation-2026-10-11.md`와 `docs/analysis/gps-idle-marker-resume-dev-validation-2026-10-11.md`를 따른다.
사용자 요청에 따라 구현 커밋과 후속 변경을 두 저장소의 같은 기능 브랜치에 푸시한다. private GPS·배포 자료는 Git 제외 규칙을 유지한다.

2026-10-12 후속 상태: GPS 나이의 정밀한 시간 계산, 카드·지도·군집 신선도 표시 및 제한된 Dev 진단을 추가했다.
공유 JS 자원 버전은 17, CSS는 9다. 비로그인 정류장 선택 안내는 “로그인하면 정류장 선택이 저장돼요. 우측 상단 ☰ 메뉴에서 로그인 해보세요.”로 다듬었다.
backend의 `docs/analysis/gps-stale-marker-and-status-dev-validation-2026-10-12.md`에 서버·브라우저·실제 Dev 관찰 결과와 실제 앱 기기 검증의 한계를 기록했다.
