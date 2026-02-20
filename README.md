# Shuttle-Go (셔틀 검색)

셔틀버스 노선 검색 서비스. 출발지를 선택하면 가장 가까운 셔틀 정류장과 노선을 안내합니다.

- **프론트엔드**: GitHub Pages ([shuttlego.github.io](https://shuttlego.github.io))
- **백엔드**: Flask API + Docker Compose (Traefik, Prometheus, Grafana)

---

## 백엔드 띄우기

### 1. 사전 준비

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) 설치
- [카카오 디벨로퍼스](https://developers.kakao.com)에서 앱 생성 후 **REST API 키** 발급

### 2. 환경변수 설정 (.env)

프로젝트 루트에 `.env` 파일을 만듭니다. `.env.example`을 복사해서 시작하세요.

```bash
cp .env.example .env
```

`.env` 파일을 열고 값을 채웁니다:

```env
# 필수: 카카오 REST API 키
KAKAO_REST_API_KEY=여기에_발급받은_키_입력

# 도메인 설정 (상용 배포 시)
API_DOMAIN=api.shuttle-go.com
# 개발용 API 도메인 (Webhook 테스트 등)
API_DEV_DOMAIN=api-dev.shuttle-go.com
GRAFANA_DOMAIN=grafana.shuttle-go.com

# 로컬 Traefik 바인딩 포트 (기본: 80/443)
BACKEND_HTTP_PORT=80
BACKEND_HTTPS_PORT=443

# 로컬 프론트엔드 포트 (run_dev.py)
FRONTEND_PORT=8080

# CORS 허용 origin (로컬 개발 + 상용)
# FRONTEND_PORT를 바꿨다면 해당 포트 origin도 추가해야 합니다.
CORS_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://shuttlego.github.io

# Grafana 관리자 계정
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=원하는_비밀번호

# 앱 버전
APP_VERSION=1.0.0

# GitHub Issue 연동 (GitHub App)
GITHUB_APP_ID=123456
GITHUB_INSTALLATION_ID=987654
GITHUB_REPO_OWNER=your-org-or-user
GITHUB_REPO_NAME=your-report-repo
GITHUB_APP_PRIVATE_KEY=-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----
# 또는 private key 파일 경로(컨테이너에서 접근 가능한 경로여야 함)
GITHUB_APP_PRIVATE_KEY_PATH=/run/secrets/github_app_private_key.pem
GITHUB_WEBHOOK_SECRET=랜덤_시크릿

# 선택: 이슈 목록 캐시/등록 제한
GITHUB_ISSUE_CACHE_TTL_SEC=60
ISSUE_SUBMIT_RATE_LIMIT_SEC=10
ISSUE_SUBMIT_DEDUPE_WINDOW_SEC=300
```

> `.env` 파일은 `.gitignore`에 포함되어 있어 git에 올라가지 않습니다.
> `GITHUB_APP_PRIVATE_KEY_PATH`를 사용할 때 Docker로 실행 중이면, Mac 로컬 경로(예: `/Users/...`)를 그대로 넣어도 컨테이너 내부에서 읽을 수 없습니다. 컨테이너에서 보이는 경로로 마운트하거나 `GITHUB_APP_PRIVATE_KEY`(문자열) 방식으로 설정하세요.

### 3. 실행 (run_dev.py)

`run_dev.py`를 사용하면 Docker Compose(백엔드·Traefik·Prometheus·Grafana)와 프론트엔드 정적 서버를 한 번에 관리할 수 있습니다.

```bash
# 전체 시작 — Docker Compose 빌드·(재)시작 + 프론트엔드(:8080) 서버
python run_dev.py

# 프론트엔드 서버만 시작 — Docker 컨테이너가 이미 떠 있을 때
python run_dev.py --frontend

# 전체 종료 — Docker Compose down + 프론트엔드 서버 정리
python run_dev.py --down
```

- 이미 동작 중인 컨테이너가 있으면 자동으로 재시작합니다.
- 백엔드 health check(`/health`)가 통과할 때까지 대기한 뒤 프론트엔드를 띄웁니다.
- `Ctrl+C`를 누르면 프론트엔드 서버만 종료되고 Docker 컨테이너는 유지됩니다.
- `BACKEND_HTTP_PORT/BACKEND_HTTPS_PORT`를 바꾸면 Traefik 바인딩 포트가 함께 변경되고, `run_dev.py`가 프론트엔드의 개발 API 주소도 자동으로 맞춥니다.
- dev HTTPS 외부 접근은 `API_DEV_DOMAIN` 라우터를 사용합니다. 예: `https://api-dev.shuttle-go.com:8443/api/sites`
- 로컬 프론트에서 호스트가 `localhost`, `127.0.0.1`, `192.168.*` 이면 dev 카카오 JS 키를 사용합니다.

### 3-1. 프로덕션 백엔드만 실행 (run_prd_backend.py)

프론트엔드 없이 backend만 빌드/실행할 때 사용합니다.

```bash
# backend만 빌드 + 실행
python run_prd_backend.py

# backend만 종료/삭제
python run_prd_backend.py --down

# health check 타임아웃 변경(초)
python run_prd_backend.py --timeout 120
```

- `docker compose up -d --build backend`로 backend만 시작합니다.
- health 상태를 1초 간격으로 폴링하고, `healthy`가 되면 즉시 대기를 종료합니다.

### 4. 수동 실행

Docker Compose와 프론트엔드를 각각 따로 실행할 수도 있습니다.

```bash
# 백엔드 (Docker Compose)
docker compose up -d

# 프론트엔드 (별도 터미널)
python3 -m http.server 8080 --bind 0.0.0.0
```

첫 실행 시 이미지 빌드 + 다운로드에 1~2분 걸립니다. 이후부터는 몇 초 안에 시작됩니다.

### 5. 확인

| 서비스 | URL | 설명 |
|--------|-----|------|
| 프론트엔드 | `http://localhost:${FRONTEND_PORT}` | 셔틀 검색 화면 |
| 백엔드 API | `http://localhost:${BACKEND_HTTP_PORT}/api/sites` | 사업장 목록 JSON |
| 백엔드 API (HTTPS) | `https://localhost:${BACKEND_HTTPS_PORT}/api/sites` | HTTPS 라우팅 확인 |
| 헬스체크 | `http://localhost:${BACKEND_HTTP_PORT}/health` | `{"status":"healthy"}` |
| 헬스체크 (HTTPS) | `https://localhost:${BACKEND_HTTPS_PORT}/health` | HTTPS 라우팅 확인 |
| Grafana | `http://localhost:${BACKEND_HTTP_PORT}/grafana/` | 모니터링 대시보드 |

### 이슈 API (GitHub 연동)

- `GET /api/issues?state=all&page=1&per_page=10` : 이슈 목록/상태 조회
- `GET /api/issues/{number}` : 이슈 본문 + 댓글 조회
- `POST /api/issues` : 이슈(건의) 등록
- `POST /api/issues/{number}/comments` : 댓글 등록
- `POST /api/github/webhook` : GitHub webhook 수신 (서명 검증)

Webhook 등록 예시:
- dev: `https://api-dev.shuttle-go.com:8443/api/github/webhook`
- prd: `https://api.shuttle-go.com/api/github/webhook`

### 6. 종료

```bash
# run_dev.py 사용 시
python run_dev.py --down

# 수동 종료
docker compose down
```

데이터(Prometheus, Grafana)를 포함해 완전히 삭제하려면:

```bash
docker compose down -v
```

---

## 데이터 빌드

### build_db.py

Raw HTML 파일(`data/raw/`)과 사업장 목록(`data/sites.csv`)을 파싱하여 SQLite DB(`data/data.db`)를 생성합니다.

- **입력**: `data/raw/` 안의 HTML 파일 + `data/sites.csv`
- **출력**: `data/data.db`

```bash
python build_db.py
```

새로운 노선 데이터를 반영할 때 이 스크립트 하나만 실행하면 됩니다.
