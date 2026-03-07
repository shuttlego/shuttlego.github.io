#!/usr/bin/env python3
"""Run user store retention cleanup once."""

from __future__ import annotations

import argparse
import os

import user_store


def _env_int(name: str, default: int) -> int:
    try:
        return max(1, int(os.environ.get(name, str(default))))
    except (TypeError, ValueError):
        return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run auth/user retention cleanup once")
    parser.add_argument(
        "--place-history-retention-months",
        type=int,
        default=_env_int("PLACE_SEARCH_HISTORY_RETENTION_MONTHS", 1),
    )
    parser.add_argument(
        "--route-history-retention-months",
        type=int,
        default=_env_int("ROUTE_SEARCH_HISTORY_RETENTION_MONTHS", 6),
    )
    parser.add_argument(
        "--no-prune-auth-states",
        action="store_true",
        help="keep oauth/pending signup rows",
    )
    parser.add_argument(
        "--no-prune-sessions",
        action="store_true",
        help="keep expired sessions",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    user_store.init_db()
    user_store.cleanup_old_data(
        place_history_retention_months=max(1, int(args.place_history_retention_months)),
        route_history_retention_months=max(1, int(args.route_history_retention_months)),
        prune_auth_states=not bool(args.no_prune_auth_states),
        prune_sessions=not bool(args.no_prune_sessions),
    )
    print("[auth-cleanup] completed", flush=True)


if __name__ == "__main__":
    main()
