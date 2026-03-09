FROM python:3.12-slim

WORKDIR /app

# 의존성 먼저 설치 (레이어 캐싱)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 코드 복사
COPY app.py load_data.py user_store.py runtime_db_sync.py s3_db_sync.py k8s_lease.py tmap_collection.py build_db.py run_tmap_collector.py migrate_user_store_to_postgres.py run_auth_cleanup.py run_github_cleanup.py gunicorn.conf.py start-backend.sh ./
RUN chmod +x /app/start-backend.sh

# 정적 노선 DB를 이미지에 포함
RUN mkdir -p /app/data
COPY data/data.db /app/data/data.db
COPY data/sites.csv /app/data/sites.csv
COPY data/identity /app/data/identity
COPY data/raw /app/data/raw

EXPOSE 8081

# 프로덕션: gunicorn 멀티프로세스 Prometheus 집계 포함
CMD ["/app/start-backend.sh"]
