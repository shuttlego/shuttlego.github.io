#!/bin/sh
set -eu

PROMETHEUS_MULTIPROC_DIR="${PROMETHEUS_MULTIPROC_DIR:-/tmp/prometheus-multiproc}"
export PROMETHEUS_MULTIPROC_DIR

mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
find "$PROMETHEUS_MULTIPROC_DIR" -mindepth 1 -maxdepth 1 -type f -name "*.db" -delete

exec gunicorn \
  --bind 0.0.0.0:8081 \
  --workers 2 \
  --threads 2 \
  --timeout 30 \
  --access-logfile - \
  --error-logfile - \
  --forwarded-allow-ips "*" \
  --config /app/gunicorn.conf.py \
  app:app
