#!/usr/bin/env python3
"""Run queued GitHub cleanup jobs once or until the queue is empty."""

from __future__ import annotations

import argparse
import os

os.environ.setdefault("APP_ENABLE_BACKGROUND_WORKERS", "0")
os.environ.setdefault("APP_WARM_ENDPOINT_CACHE_ON_STARTUP", "0")
os.environ.setdefault("APP_REFRESH_AUTH_METRICS_ON_STARTUP", "0")

import app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run queued GitHub cleanup jobs")
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=20,
        help="maximum jobs to process per invocation (0 = until queue is empty)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    processed = 0
    while args.max_jobs <= 0 or processed < max(1, int(args.max_jobs)):
        if not app.run_github_cleanup_once():
            break
        processed += 1
    print(f"[github-cleanup] processed={processed}", flush=True)


if __name__ == "__main__":
    main()
