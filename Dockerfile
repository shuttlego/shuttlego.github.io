FROM python:3.12-slim

WORKDIR /app

# 의존성 먼저 설치 (레이어 캐싱)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY app.py load_data.py ./

# data/ 디렉토리는 볼륨으로 마운트 (이미지에 포함하지 않음)

EXPOSE 8081

# 프로덕션: gunicorn (각 worker에서 app import 시 load_all() 실행)
CMD ["gunicorn", \
     "--bind", "0.0.0.0:8081", \
     "--workers", "2", \
     "--threads", "2", \
     "--timeout", "30", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "--forwarded-allow-ips", "*", \
     "app:app"]
