# 프론트엔드/백엔드 저장소 분리 가이드

## 1. 분리 개요

| 구분 | 저장소 | 호스팅 | 포함 내용 |
|------|--------|--------|-----------|
| **Frontend** | `shuttlego/shuttlego.github.io` (public) | GitHub Pages | 정적 웹(HTML/JS/CSS), 백엔드 도메인 변수화 |
| **Backend** | private repo | Public Cloud VM | Flask API, 데이터, VM 스크립트, Docker Compose(모니터링·Ingress) |

---

## 2. 에이전트가 두 프로젝트를 왔다갔다 하는 방법

**가능합니다.** 아래 중 하나를 쓰면 됩니다.

### 방법 A: 부모 폴더에 두 레포 함께 클론 (권장)

```
~/projects/
  shuttle-go-frontend/   # git clone https://github.com/shuttlego/shuttlego.github.io.git
  shuttle-go-backend/    # private repo
```

- Cursor에서 **상위 폴더(`~/projects/`)를 워크스페이스로 열기** → 한 번에 두 프로젝트 모두 보임.
- 에이전트는 같은 대화 안에서 `shuttle-go-frontend/`, `shuttle-go-backend/`를 모두 읽고 수정 가능.
- 각 레포는 독립 git이므로 커밋/푸시는 각 디렉터리에서 따로 하면 됨.

### 방법 B: Cursor Multi-root Workspace

- `File > Add Folder to Workspace`로 프론트/백엔드 폴더를 둘 다 추가.
- `.code-workspace` 파일로 저장해 두면, 나중에 그 파일만 열어도 두 프로젝트가 함께 로드됨.

### 방법 C: 프로젝트마다 Cursor 창을 따로 열기

- 프론트용 창, 백엔드용 창을 각각 열어서 작업.
- 에이전트는 **창(컨텍스트)마다 한 레포만** 보이므로, “지금 이 창은 백엔드만” / “이 창은 프론트만”으로 쓰면 됨.
- 두 레포를 동시에 수정하려면 A 또는 B가 유리함.

**정리**: 에이전트가 두 레포를 동시에 보려면 **같은 워크스페이스에 두 폴더가 들어가 있으면 됨** (A 또는 B). 한 번에 한 레포만 보이게 하려면 C.

---

## 3. 개발 시 편하게 쓰는 방법

### 3.1 로컬 개발 환경

- **프론트**: 로컬에서 정적 서버 또는 GitHub Pages 미리보기.
  - 백엔드 URL을 `http://localhost:5000` 같은 **환경 변수/설정 파일**로 두고, 배포 시에만 `https://api.your-backend.com` 등으로 바꿈.
- **백엔드**: 기존처럼 `flask run` 또는 Docker로 로컬에서 실행.
- 프론트와 백엔드를 **같은 머신에서 동시에 띄우고**, 프론트만 백엔드 URL만 바꿔가며 쓰면 됨.

### 3.2 백엔드 URL 변수화 (프론트)

- **빌드 타임**: Vite/Webpack 등이 있다면 `import.meta.env.VITE_API_URL` 같은 값으로 빌드.
- **런타임**: `index.html` 로드 시 `config.js` 또는 `window.__CONFIG__`를 한 번 읽어서 `API_BASE`로 사용.
- GitHub Pages에서는 `https://shuttlego.github.io/config.js`처럼 같은 오리진에서 config를 서브하면, 배포 시 백엔드 URL만 그 파일에서 바꾸면 됨.

### 3.3 CORS (백엔드)

- 프론트가 `shuttlego.github.io`에서, 백엔드는 다른 도메인에서 돌아가므로 **CORS 설정 필수**.
- Flask에서 `flask-cors` 또는 수동으로:
  - `Access-Control-Allow-Origin: https://shuttlego.github.io`
  - 로컬 개발용으로 `http://localhost:*` 허용하면 편함.

### 3.4 카카오 JavaScript 키

- 지금은 서버에서 `index.html`에 주입하고 있음.
- 정적 프론트로 분리 후에는:
  - **옵션 1**: 빌드 시 환경 변수로 주입 (키가 빌드 결과물에 포함됨).
  - **옵션 2**: `config.js`에서 런타임에 로드 (키를 레포에 안 넣고, 배포 시만 config에 넣거나 별도 배포 채널로 주입).

---

## 4. 분리 시 체크리스트

### Frontend 저장소 (shuttlego.github.io)

- [ ] `templates/index.html` → 정적 `index.html`로 이전 (Jinja 변수 제거 후 `config.js` 또는 빌드 env로 대체).
- [ ] 모든 API 호출을 **백엔드 base URL 변수** 기준으로 변경 (예: `fetch(API_BASE + '/api/sites')`).
- [ ] 카카오 JS 키: `config.js` 또는 env로 주입.
- [ ] GitHub Pages 설정: 소스 branch 또는 `gh-pages` 등 설정.

### Backend 저장소 (private)

- [ ] `app.py`: `@app.route("/")` 및 `render_template("index.html", ...)` 제거. API 라우트만 유지.
- [ ] CORS 허용: `shuttlego.github.io`, `localhost` (개발용).
- [ ] VM에서 백엔드 실행용 스크립트 (예: systemd, Docker 실행 명령).
- [ ] Docker Compose: 앱 + Prometheus + Grafana + Ingress 컨트롤러 구성.
- [ ] `data/`, `load_data.py` 등 기존 데이터·로직은 백엔드 레포에 유지.

---

## 5. 요약

- **에이전트**: 두 레포를 **한 워크스페이스(부모 폴더 또는 multi-root)에 넣으면** 같은 대화에서 왔다 갔다 하며 수정 가능.
- **개발 편의**: 프론트는 “백엔드 URL만 설정해서 바꾸는 구조”로 두고, 로컬에서는 백엔드만 띄운 뒤 프론트가 `localhost`를 바라보게 하면 됨.
- **분리 후**: 프론트는 정적 호스팅 + 백엔드 URL 변수화, 백엔드는 API 전용 + CORS + 인프라(스크립트, Docker Compose)만 갖추면 됨.
