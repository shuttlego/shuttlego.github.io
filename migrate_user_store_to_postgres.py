#!/usr/bin/env python3
"""Copy the SQLite user store into the configured Postgres backend."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import user_store

TABLE_ORDER = (
    "users",
    "social_accounts",
    "user_preferences",
    "user_sessions",
    "user_endpoint_preferences",
    "place_search_history",
    "route_search_history",
    "anonymous_place_search_history",
    "anonymous_route_search_history",
    "arrival_reports",
    "arrival_report_likes",
    "consent_events",
    "oauth_states",
    "pending_signups",
    "github_cleanup_jobs",
    "background_task_leases",
)


def log(message: str) -> None:
    print(f"[migrate-user-store] {message}", flush=True)


def fail(message: str) -> None:
    print(f"[migrate-user-store] ERROR: {message}", file=sys.stderr, flush=True)
    raise SystemExit(1)


def _table_columns(conn, table_name: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    columns: list[str] = []
    for row in rows:
        name = str(row["name"] or "").strip()
        if name:
            columns.append(name)
    return columns


def _clear_target(conn) -> None:
    for table_name in reversed(TABLE_ORDER):
        conn.execute(f"DELETE FROM {table_name}")


def _copy_table(source_conn: sqlite3.Connection, target_conn, table_name: str) -> int:
    source_columns = _table_columns(source_conn, table_name)
    if not source_columns:
        return 0

    target_columns = _table_columns(target_conn, table_name)
    target_column_set = set(target_columns)
    columns = [column for column in source_columns if column in target_column_set]
    if not columns:
        return 0
    missing_in_source = [column for column in target_columns if column not in source_columns]
    if missing_in_source:
        log(
            f"{table_name}: source missing columns -> {', '.join(missing_in_source)} "
            "(target default/NULL 사용)"
        )

    column_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    rows = source_conn.execute(
        f"SELECT {column_sql} FROM {table_name} ORDER BY ROWID ASC"
    ).fetchall()
    insert_sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"
    for row in rows:
        target_conn.execute(insert_sql, tuple(row[column] for column in columns))
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SQLite user store -> Postgres full copy")
    parser.add_argument(
        "--source",
        default=str(user_store.DEFAULT_USER_DB_PATH),
        help=f"source SQLite DB path (default: {user_store.DEFAULT_USER_DB_PATH})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_path = Path(args.source).expanduser().resolve()
    if not source_path.exists():
        fail(f"source DB not found: {source_path}")
    if not user_store.is_postgres_backend():
        fail("USER_STORE_BACKEND=postgres 환경에서만 실행할 수 있습니다.")

    log(f"source={source_path}")
    try:
        user_store.init_db()
        target_conn = user_store._connect()
    except RuntimeError as exc:
        fail(str(exc))

    source_conn = sqlite3.connect(str(source_path))
    source_conn.row_factory = sqlite3.Row

    try:
        _clear_target(target_conn)
        copied_counts: dict[str, int] = {}
        for table_name in TABLE_ORDER:
            copied_counts[table_name] = _copy_table(source_conn, target_conn, table_name)
        target_conn.commit()
    except Exception as exc:
        try:
            target_conn.rollback()
        except Exception:
            pass
        fail(f"migration failed: {exc}")
    finally:
        source_conn.close()
        target_conn.close()

    user_store.reset_identity_sequences()

    total_rows = 0
    for table_name in TABLE_ORDER:
        count = int(copied_counts.get(table_name, 0))
        total_rows += count
        log(f"{table_name}: {count}")
    log(f"done (total rows copied: {total_rows})")


if __name__ == "__main__":
    main()
