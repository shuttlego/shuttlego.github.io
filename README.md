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
GRAFANA_DOMAIN=grafana.shuttle-go.com

# CORS 허용 origin (로컬 개발 + 상용)
CORS_ORIGINS=http://localhost:8080,http://127.0.0.1:8080,https://shuttlego.github.io

# Grafana 관리자 계정
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=원하는_비밀번호

# 앱 버전
APP_VERSION=1.0.0
```

> `.env` 파일은 `.gitignore`에 포함되어 있어 git에 올라가지 않습니다.

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
| 프론트엔드 | http://localhost:8080 | 셔틀 검색 화면 |
| 백엔드 API | http://localhost/api/sites | 사업장 목록 JSON |
| 헬스체크 | http://localhost/health | `{"status":"healthy"}` |
| Grafana | http://localhost/grafana/ | 모니터링 대시보드 |

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
