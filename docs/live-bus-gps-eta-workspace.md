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

이번 단계에서는 설계 문서와 작업 공간만 준비했다. 기능 구현, commit/push, Dev/Prd 배포는 아직 수행하지 않았다.
