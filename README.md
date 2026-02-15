# Python Project

새 파이썬 프로젝트입니다.

## 설정

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 실행

```bash
python main.py
```

### 지도 웹앱 (장소 검색)

상단 검색바, 지도, 하단 검색 결과(최대 15건) + 지도 마커.

```bash
# 환경변수 설정 후 (REST API 키 + JavaScript 키)
export KAKAO_REST_API_KEY=your_rest_api_key
export KAKAO_JAVASCRIPT_KEY=your_javascript_key
python app.py
```

브라우저에서 http://localhost:5000 접속. 카카오 디벨로퍼스에서 **플랫폼 > Web**에 `http://localhost:5000` 도메인 등록 필요.
