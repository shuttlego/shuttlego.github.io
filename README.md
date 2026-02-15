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

### 3. 실행

```bash
docker compose up -d
```

첫 실행 시 이미지 빌드 + 다운로드에 1~2분 걸립니다. 이후부터는 몇 초 안에 시작됩니다.

### 4. 확인

| 서비스 | URL | 설명 |
|--------|-----|------|
| 백엔드 API | http://localhost/api/sites | 사업장 목록 JSON |
| 헬스체크 | http://localhost/health | `{"status":"healthy"}` |
| Grafana | http://localhost/grafana/ | 모니터링 대시보드 |

### 5. 프론트엔드 (로컬 개발)

별도 터미널에서:

```bash
python3 -m http.server 8080 --bind 0.0.0.0
```

브라우저에서 http://localhost:8080 접속.

### 6. 종료

```bash
docker compose down
```

데이터(Prometheus, Grafana)를 포함해 완전히 삭제하려면:

```bash
docker compose down -v
```

---

## Scripts

`scripts/` 디렉토리에는 셔틀 노선 데이터를 가공하는 유틸리티 스크립트가 있습니다.

### xlsx_to_csv.py

엑셀 파일(`data/raw/*.xlsx`)을 파싱하여 CSV로 변환합니다.

- **입력**: `data/raw/` 안의 엑셀 파일 (출근/퇴근 시트)
- **출력**:
  - `data/routes.csv` — 노선 정보 (노선명, 방향, 운영사 등)
  - `data/departure_times.csv` — 노선별 출발 시각
  - `data/tmp_stops.csv` — 정류장 목록 (위경도는 비어 있음)

```bash
python scripts/xlsx_to_csv.py
```

### extract_geo_info.py

HTML 파일(`data/raw_geo/`)에서 정류장 좌표(위경도)를 추출합니다.

- **입력**: `data/raw_geo/` 안의 HTML 파일 (지도 `showMap()` 호출 포함)
- **출력**:
  - `data/tmp_geo_info.csv` — 정류장명 + 위경도
  - `data/geo_route_info.csv` — 노선별 정류장 순서

```bash
python scripts/extract_geo_info.py
```

### fill_stop_geo.py

정류장 목록에 위경도 좌표를 채워 넣습니다. `extract_geo_info.py`의 결과와 카카오 API를 활용합니다.

- **입력**: `data/tmp_stops.csv` + `data/tmp_geo_info.csv`
- **출력**: `data/stops.csv` — 위경도가 채워진 최종 정류장 데이터

```bash
python scripts/fill_stop_geo.py
```

### 데이터 가공 순서

새로운 엑셀 데이터를 반영할 때는 순서대로 실행합니다:

```
xlsx_to_csv.py → extract_geo_info.py → fill_stop_geo.py
```
