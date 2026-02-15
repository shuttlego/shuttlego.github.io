# GitHub Pages (shuttlego.github.io) 호스팅 구조

## URL과 저장소 이름

- **사용자/조직 사이트**로 `https://shuttlego.github.io` (경로 없음)를 쓰려면 **저장소 이름이 반드시 `shuttlego.github.io`** 여야 합니다.
- 지금 저장소 이름이 `shuttle-go`이면 URL은 `https://shuttlego.github.io/shuttle-go/` 가 됩니다.
- 따라서 **`https://shuttlego.github.io`** 를 쓰려면:
  1. GitHub에서 새 저장소 `shuttlego.github.io` 생성 후 코드 푸시, **또**
  2. 기존 `shuttle-go` 저장소 이름을 `shuttlego.github.io` 로 변경 (Settings → Repository name)

---

## GitHub Pages가 서빙하는 위치

- **Deploy from a branch** 로 설정한 뒤, **Branch**는 보통 `main`, **Folder**는 `/ (root)` 또는 `/docs` 중 하나를 선택합니다.
- **Folder: root** 인 경우: 저장소 **루트**에 있는 파일이 사이트 루트가 됩니다.
  - 루트의 `index.html` → `https://shuttlego.github.io/`
- **Folder: /docs** 인 경우: **docs** 폴더 안이 사이트 루트가 됩니다.
  - `docs/index.html` → `https://shuttlego.github.io/`

즉, **지금처럼 `templates/index.html` 만 있으면 GitHub Pages가 그걸 기본 페이지로 쓰지 않습니다.**  
Pages가 서빙할 **진입 파일을 루트 또는 /docs 에 두어야** 합니다.

---

## 파일 구조 제안

### 옵션 A: 루트에 `index.html` 두기 (권장)

저장소 루트에 `index.html` 을 두고, Pages는 루트 기준으로 배포하는 방식입니다.

```
shuttlego.github.io/          # 저장소 이름을 이렇게 둔 경우
├── index.html                 # ← GitHub Pages 진입용 (여기 있으면 됨)
├── app.py
├── load_data.py
├── templates/                 # Flask에서만 사용 시 (선택)
│   └── ...
├── data/
├── docs/
└── ...
```

**할 일 요약**

1. 저장소 이름을 `shuttlego.github.io` 로 변경(또는 새로 생성).
2. **`templates/index.html`** 내용을 **루트의 `index.html`** 로 복사(이동)해 두기.
3. GitHub: **Settings → Pages → Source**: Branch `main`, Folder **/ (root)** 선택.

이후에는 루트의 `index.html` 이 `https://shuttlego.github.io/` 로 서빙됩니다.

- **로컬 개발**: Flask에서 같은 루트 `index.html` 을 서빙하도록 라우트만 바꾸면 됩니다 (아래 참고).
- **카카오 JS 키**: 지금처럼 Flask가 템플릿에 키를 넣어 주면, 로컬에서는 환경변수 그대로 사용.  
  Pages에서만 정적으로 쓸 경우에는 `config.js` 등으로 키를 두고, 카카오 콘솔에서 `https://shuttlego.github.io` 만 허용하면 됩니다.

### 옵션 B: `/docs` 에서 배포

- **Folder: /docs** 로 두면, **docs 폴더 안**이 사이트 루트가 됩니다.
- 예: `docs/index.html` 을 두고, Pages 설정에서 **Branch: main, Folder: /docs** 선택.

```
shuttlego.github.io/
├── app.py
├── load_data.py
├── templates/
│   └── index.html
├── docs/
│   └── index.html    # ← Pages 진입용
└── ...
```

- 장점: 프로젝트 루트는 기존 구조 유지.
- 단점: `templates/index.html` 과 `docs/index.html` 를 동기화해야 할 수 있음.

---

## Flask에서 루트 `index.html` 서빙 (로컬/백엔드)

루트에 `index.html` 을 두었다면, Flask가 **루트의 `index.html`** 을 서빙하도록 바꾸면 됩니다.

**예시 (app.py):**

```python
from flask import send_from_directory

@app.route("/")
def index():
    # 루트의 index.html 서빙 (카카오 키는 아직 템플릿이 아니므로 config.js 등으로 별도 처리하거나,
    # 기존처럼 templates/index.html 을 쓰려면 그대로 render_template 사용)
    return send_from_directory(app.root_path, "index.html")
```

- **카카오 JS 키**를 계속 서버에서 넣고 싶으면:  
  루트 `index.html` 을 **Jinja 템플릿으로 옮기지 않고** 두고, Flask에서 `send_from_directory` 로 보내는 대신  
  `render_template` 을 쓰려면 `index.html` 을 다시 `templates/` 에 두고, Pages 배포용으로만 루트에 **빌드/복사본**을 두는 방식도 가능합니다.  
  즉, “진입 파일만 루트에 둔다”는 점만 지키면 됩니다.

---

## 정리

| 하고 싶은 것 | 할 일 |
|-------------|--------|
| URL을 `https://shuttlego.github.io` 로 쓰기 | 저장소 이름을 `shuttlego.github.io` 로 하기 |
| Pages가 첫 화면을 서빙하게 하기 | `index.html` 을 **저장소 루트** 또는 **docs/** 에 두기 |
| 개발/상용 키 구분 | 둘 다 환경변수로 두고, 로컬은 .env, 상용은 서버 env에만 넣기 (코드 수정 없음) |

이렇게 하면 `templates/` 구조와 무관하게, GitHub Pages는 “루트 또는 docs의 `index.html`”만 보면 되고,  
개발 시에는 Flask가 같은 파일(또는 templates 버전)을 서빙하도록만 맞추면 됩니다.
