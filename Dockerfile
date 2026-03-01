FROM python:3.12-slim

WORKDIR /app

# 의존성 먼저 설치 (레이어 캐싱)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY app.py load_data.py user_store.py gunicorn.conf.py start-backend.sh ./
RUN chmod +x /app/start-backend.sh

# data/ 디렉토리는 볼륨으로 마운트 (이미지에 포함하지 않음)

EXPOSE 8081

# 프로덕션: gunicorn 멀티프로세스 Prometheus 집계 포함
CMD ["/app/start-backend.sh"]
