"""User/account/auth persistence for Shuttle-Go.

This module manages a writable SQLite database that is separate from route data.db.
"""

from __future__ import annotations

import json
import os
import secrets
import sqlite3
import threading
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

DEFAULT_USER_DB_DIR = Path(__file__).resolve().parent / "user-data"
DEFAULT_USER_DB_PATH = DEFAULT_USER_DB_DIR / "user.db"

_db_path_lock = threading.Lock()
_db_path: Path = DEFAULT_USER_DB_PATH


class UserStoreError(Exception):
    """Base error for user store operations."""


class NicknameConflictError(UserStoreError):
    """Raised when nickname is already taken."""


class PendingSignupError(UserStoreError):
    """Raised when pending signup token is invalid/expired."""


class SessionNotFoundError(UserStoreError):
    """Raised when the session does not exist."""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    value = dt or utc_now()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    raw = str(ts).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def normalize_nickname(nickname: str) -> str:
    return unicodedata.normalize("NFKC", str(nickname or "").strip()).casefold()


def validate_nickname(nickname: str, min_len: int = 2, max_len: int = 20) -> str:
    value = unicodedata.normalize("NFKC", str(nickname or "").strip())
    if not value:
        raise ValueError("닉네임은 필수입니다.")
    if len(value) < min_len or len(value) > max_len:
        raise ValueError(f"닉네임은 {min_len}~{max_len}자여야 합니다.")
    return value


def configure(db_path: str | None = None) -> Path:
    global _db_path
    with _db_path_lock:
        if db_path:
            target = Path(db_path).expanduser().resolve()
        else:
            target = Path(os.environ.get("USER_DB_PATH", str(DEFAULT_USER_DB_PATH))).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        _db_path = target
        return _db_path


def get_db_path() -> Path:
    with _db_path_lock:
        return _db_path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(get_db_path()), timeout=10, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


@contextmanager
def _conn_ctx() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db(db_path: str | None = None) -> None:
    configure(db_path)
    with _conn_ctx() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              public_user_id TEXT NOT NULL UNIQUE,
              nickname TEXT NOT NULL,
              nickname_norm TEXT NOT NULL UNIQUE,
              email TEXT,
              email_consent INTEGER NOT NULL DEFAULT 0,
              status TEXT NOT NULL DEFAULT 'active',
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              deleted_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS social_accounts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              provider TEXT NOT NULL,
              provider_user_id TEXT NOT NULL,
              provider_email TEXT,
              provider_email_verified INTEGER NOT NULL DEFAULT 0,
              provider_nickname TEXT,
              linked_at TEXT NOT NULL,
              last_login_at TEXT,
              UNIQUE(provider, provider_user_id),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_sessions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_token_hash TEXT NOT NULL UNIQUE,
              user_id INTEGER NOT NULL,
              expires_at TEXT NOT NULL,
              created_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              user_agent TEXT,
              ip TEXT,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
              user_id INTEGER PRIMARY KEY,
              selected_site_id TEXT,
              updated_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_endpoint_preferences (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              site_id TEXT NOT NULL,
              day_type TEXT NOT NULL,
              direction TEXT NOT NULL,
              endpoint_names_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              UNIQUE(user_id, site_id, day_type, direction),
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS place_search_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              keyword TEXT NOT NULL,
              searched_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS route_search_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              site_id TEXT NOT NULL,
              day_type TEXT NOT NULL,
              direction TEXT NOT NULL,
              lat REAL NOT NULL,
              lng REAL NOT NULL,
              selected_endpoints_json TEXT NOT NULL,
              place_name TEXT,
              searched_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS consent_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              consent_type TEXT NOT NULL,
              consented INTEGER NOT NULL,
              context TEXT,
              created_at TEXT NOT NULL,
              FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS oauth_states (
              state TEXT PRIMARY KEY,
              provider TEXT NOT NULL,
              next_url TEXT NOT NULL,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_signups (
              token TEXT PRIMARY KEY,
              provider TEXT NOT NULL,
              provider_user_id TEXT NOT NULL,
              provider_email TEXT,
              provider_email_verified INTEGER NOT NULL DEFAULT 0,
              provider_nickname TEXT,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              consumed_at TEXT,
              UNIQUE(provider, provider_user_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS github_cleanup_jobs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              public_user_id TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',
              attempts INTEGER NOT NULL DEFAULT 0,
              next_attempt_at TEXT NOT NULL,
              deadline_at TEXT NOT NULL,
              last_error TEXT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_social_provider_uid ON social_accounts(provider, provider_user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_hash ON user_sessions(session_token_hash)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_exp ON user_sessions(expires_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_place_history_user_id_id ON place_search_history(user_id, id DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_place_history_searched_at ON place_search_history(searched_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_route_history_user_id_id ON route_search_history(user_id, id DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_route_history_searched_at ON route_search_history(searched_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cleanup_jobs_due ON github_cleanup_jobs(status, next_attempt_at, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cleanup_jobs_public_uid ON github_cleanup_jobs(public_user_id)"
        )
        conn.commit()


def _row_to_user(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "public_user_id": str(row["public_user_id"]),
        "nickname": str(row["nickname"]),
        "email": row["email"],
        "email_consent": bool(int(row["email_consent"])),
        "status": str(row["status"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def get_user_by_id(user_id: int) -> dict | None:
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT id, public_user_id, nickname, email, email_consent, status, created_at, updated_at
            FROM users
            WHERE id = ? AND status = 'active'
            """,
            (int(user_id),),
        ).fetchone()
        return _row_to_user(row)


def get_user_by_provider(provider: str, provider_user_id: str) -> dict | None:
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.public_user_id, u.nickname, u.email, u.email_consent, u.status, u.created_at, u.updated_at
            FROM social_accounts s
            JOIN users u ON u.id = s.user_id
            WHERE s.provider = ? AND s.provider_user_id = ? AND u.status = 'active'
            LIMIT 1
            """,
            (str(provider), str(provider_user_id)),
        ).fetchone()
        return _row_to_user(row)


def get_active_nickname_map_by_public_user_ids(public_user_ids: list[str]) -> dict[str, str]:
    unique_ids: list[str] = []
    seen: set[str] = set()
    for raw in public_user_ids or []:
        uid = str(raw or "").strip()
        if not uid or uid in seen:
            continue
        seen.add(uid)
        unique_ids.append(uid)
        if len(unique_ids) >= 1000:
            break

    if not unique_ids:
        return {}

    placeholders = ",".join("?" for _ in unique_ids)
    query = (
        "SELECT public_user_id, nickname "
        "FROM users "
        f"WHERE status = 'active' AND public_user_id IN ({placeholders})"
    )
    with _conn_ctx() as conn:
        rows = conn.execute(query, tuple(unique_ids)).fetchall()

    result: dict[str, str] = {}
    for row in rows:
        key = str(row["public_user_id"] or "").strip()
        value = str(row["nickname"] or "").strip()
        if key and value:
            result[key] = value
    return result


def save_oauth_state(provider: str, next_url: str, ttl_sec: int = 600) -> str:
    now = utc_now()
    state = secrets.token_urlsafe(24)
    with _conn_ctx() as conn:
        conn.execute(
            """
            INSERT INTO oauth_states(state, provider, next_url, created_at, expires_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                state,
                str(provider),
                str(next_url),
                utc_iso(now),
                utc_iso(now + timedelta(seconds=max(60, int(ttl_sec)))),
            ),
        )
        conn.commit()
    return state


def consume_oauth_state(provider: str, state: str) -> dict | None:
    now = utc_iso()
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT state, provider, next_url, expires_at
            FROM oauth_states
            WHERE state = ? AND provider = ?
            LIMIT 1
            """,
            (str(state), str(provider)),
        ).fetchone()
        if row is None:
            return None
        conn.execute("DELETE FROM oauth_states WHERE state = ?", (str(state),))
        conn.commit()
        if str(row["expires_at"]) <= now:
            return None
        return {
            "state": row["state"],
            "provider": row["provider"],
            "next_url": row["next_url"],
            "expires_at": row["expires_at"],
        }


def save_pending_signup(
    provider: str,
    provider_user_id: str,
    provider_email: str | None,
    provider_email_verified: bool,
    provider_nickname: str | None,
    ttl_sec: int = 900,
) -> str:
    now = utc_now()
    token = secrets.token_urlsafe(32)
    with _conn_ctx() as conn:
        conn.execute(
            "DELETE FROM pending_signups WHERE provider = ? AND provider_user_id = ?",
            (str(provider), str(provider_user_id)),
        )
        conn.execute(
            """
            INSERT INTO pending_signups(
                token, provider, provider_user_id, provider_email,
                provider_email_verified, provider_nickname,
                created_at, expires_at, consumed_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                token,
                str(provider),
                str(provider_user_id),
                (str(provider_email).strip() if provider_email else None),
                1 if provider_email_verified else 0,
                (str(provider_nickname).strip() if provider_nickname else None),
                utc_iso(now),
                utc_iso(now + timedelta(seconds=max(60, int(ttl_sec)))),
            ),
        )
        conn.commit()
    return token


def get_pending_signup(token: str) -> dict | None:
    now = utc_iso()
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT token, provider, provider_user_id, provider_email,
                   provider_email_verified, provider_nickname, expires_at, consumed_at
            FROM pending_signups
            WHERE token = ?
            LIMIT 1
            """,
            (str(token),),
        ).fetchone()
        if row is None:
            return None
        if row["consumed_at"]:
            return None
        if str(row["expires_at"]) <= now:
            return None
        return {
            "token": row["token"],
            "provider": row["provider"],
            "provider_user_id": row["provider_user_id"],
            "provider_email": row["provider_email"],
            "provider_email_verified": bool(int(row["provider_email_verified"] or 0)),
            "provider_nickname": row["provider_nickname"],
            "expires_at": row["expires_at"],
        }


def _new_public_user_id() -> str:
    return "u_" + secrets.token_hex(8)


def consume_pending_signup_and_create_user(
    token: str,
    nickname: str,
    email_consent: bool,
) -> dict:
    clean_nickname = validate_nickname(nickname)
    nickname_norm = normalize_nickname(clean_nickname)
    now = utc_iso()

    with _conn_ctx() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT token, provider, provider_user_id, provider_email,
                   provider_email_verified, provider_nickname, expires_at, consumed_at
            FROM pending_signups
            WHERE token = ?
            LIMIT 1
            """,
            (str(token),),
        ).fetchone()
        if row is None or row["consumed_at"] or str(row["expires_at"]) <= now:
            conn.rollback()
            raise PendingSignupError("가입 세션이 만료되었거나 유효하지 않습니다.")

        provider = str(row["provider"])
        provider_user_id = str(row["provider_user_id"])

        existing_user = conn.execute(
            """
            SELECT u.id, u.public_user_id, u.nickname, u.email, u.email_consent, u.status, u.created_at, u.updated_at
            FROM social_accounts s
            JOIN users u ON u.id = s.user_id
            WHERE s.provider = ? AND s.provider_user_id = ? AND u.status = 'active'
            LIMIT 1
            """,
            (provider, provider_user_id),
        ).fetchone()
        if existing_user is not None:
            conn.execute(
                "UPDATE pending_signups SET consumed_at = ? WHERE token = ?",
                (now, str(token)),
            )
            conn.commit()
            return _row_to_user(existing_user) or {}

        nickname_conflict = conn.execute(
            "SELECT 1 FROM users WHERE nickname_norm = ? AND status = 'active' LIMIT 1",
            (nickname_norm,),
        ).fetchone()
        if nickname_conflict is not None:
            conn.rollback()
            raise NicknameConflictError("이미 사용 중인 닉네임입니다.")

        public_user_id = ""
        user_id: int | None = None
        for _ in range(8):
            candidate = _new_public_user_id()
            try:
                conn.execute(
                    """
                    INSERT INTO users(public_user_id, nickname, nickname_norm, email, email_consent, status, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, 'active', ?, ?)
                    """,
                    (
                        candidate,
                        clean_nickname,
                        nickname_norm,
                        (str(row["provider_email"]).strip() if email_consent and row["provider_email"] else None),
                        1 if email_consent else 0,
                        now,
                        now,
                    ),
                )
                user_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                public_user_id = candidate
                break
            except sqlite3.IntegrityError:
                continue
        if user_id is None:
            conn.rollback()
            raise UserStoreError("사용자 ID 생성에 실패했습니다.")

        conn.execute(
            """
            INSERT INTO social_accounts(
              user_id, provider, provider_user_id, provider_email,
              provider_email_verified, provider_nickname, linked_at, last_login_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                provider,
                provider_user_id,
                (str(row["provider_email"]).strip() if row["provider_email"] else None),
                1 if bool(int(row["provider_email_verified"] or 0)) else 0,
                (str(row["provider_nickname"]).strip() if row["provider_nickname"] else None),
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT INTO user_preferences(user_id, selected_site_id, updated_at) VALUES(?, NULL, ?)",
            (user_id, now),
        )
        conn.execute(
            """
            INSERT INTO consent_events(user_id, consent_type, consented, context, created_at)
            VALUES(?, 'email', ?, 'signup', ?)
            """,
            (user_id, 1 if email_consent else 0, now),
        )
        conn.execute("UPDATE pending_signups SET consumed_at = ? WHERE token = ?", (now, str(token)))
        conn.commit()
        return {
            "id": user_id,
            "public_user_id": public_user_id,
            "nickname": clean_nickname,
            "email": (str(row["provider_email"]).strip() if email_consent and row["provider_email"] else None),
            "email_consent": bool(email_consent),
            "status": "active",
            "created_at": now,
            "updated_at": now,
        }


def create_session(
    user_id: int,
    session_token_hash: str,
    expires_at: datetime,
    user_agent: str | None = None,
    ip: str | None = None,
) -> None:
    now = utc_iso()
    with _conn_ctx() as conn:
        conn.execute(
            """
            INSERT INTO user_sessions(session_token_hash, user_id, expires_at, created_at, last_seen_at, user_agent, ip)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(session_token_hash),
                int(user_id),
                utc_iso(expires_at),
                now,
                now,
                (str(user_agent).strip()[:300] if user_agent else None),
                (str(ip).strip()[:100] if ip else None),
            ),
        )
        conn.commit()


def get_user_by_session_hash(session_token_hash: str) -> dict | None:
    now = utc_iso()
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT u.id, u.public_user_id, u.nickname, u.email, u.email_consent, u.status, u.created_at, u.updated_at,
                   s.expires_at
            FROM user_sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.session_token_hash = ?
              AND s.expires_at > ?
              AND u.status = 'active'
            LIMIT 1
            """,
            (str(session_token_hash), now),
        ).fetchone()
        return _row_to_user(row)


def touch_session(session_token_hash: str, expires_at: datetime | None = None) -> None:
    now = utc_iso()
    with _conn_ctx() as conn:
        if expires_at is None:
            conn.execute(
                "UPDATE user_sessions SET last_seen_at = ? WHERE session_token_hash = ?",
                (now, str(session_token_hash)),
            )
        else:
            conn.execute(
                "UPDATE user_sessions SET last_seen_at = ?, expires_at = ? WHERE session_token_hash = ?",
                (now, utc_iso(expires_at), str(session_token_hash)),
            )
        conn.commit()


def delete_session(session_token_hash: str) -> None:
    with _conn_ctx() as conn:
        conn.execute("DELETE FROM user_sessions WHERE session_token_hash = ?", (str(session_token_hash),))
        conn.commit()


def delete_all_sessions_for_user(user_id: int) -> None:
    with _conn_ctx() as conn:
        conn.execute("DELETE FROM user_sessions WHERE user_id = ?", (int(user_id),))
        conn.commit()


def update_last_login(provider: str, provider_user_id: str) -> None:
    with _conn_ctx() as conn:
        conn.execute(
            """
            UPDATE social_accounts
            SET last_login_at = ?
            WHERE provider = ? AND provider_user_id = ?
            """,
            (utc_iso(), str(provider), str(provider_user_id)),
        )
        conn.commit()


def get_user_metrics_snapshot(
    daily_window_hours: int = 24,
    mau_window_days: int = 30,
    new_signup_window_hours: int = 24,
) -> dict:
    daily_hours = max(1, int(daily_window_hours))
    mau_days = max(1, int(mau_window_days))
    signup_hours = max(1, int(new_signup_window_hours))
    daily_cutoff = utc_iso(utc_now() - timedelta(hours=daily_hours))
    mau_cutoff = utc_iso(utc_now() - timedelta(days=mau_days))
    signup_cutoff = utc_iso(utc_now() - timedelta(hours=signup_hours))
    with _conn_ctx() as conn:
        active_users_row = conn.execute(
            "SELECT COUNT(*) AS cnt FROM users WHERE status = 'active'"
        ).fetchone()
        cumulative_users_row = conn.execute(
            "SELECT seq AS cnt FROM sqlite_sequence WHERE name = 'users' LIMIT 1"
        ).fetchone()
        daily_active_row = conn.execute(
            """
            SELECT COUNT(DISTINCT s.user_id) AS cnt
            FROM social_accounts s
            JOIN users u ON u.id = s.user_id
            WHERE u.status = 'active'
              AND s.last_login_at IS NOT NULL
              AND s.last_login_at >= ?
            """,
            (daily_cutoff,),
        ).fetchone()
        monthly_active_row = conn.execute(
            """
            SELECT COUNT(DISTINCT s.user_id) AS cnt
            FROM social_accounts s
            JOIN users u ON u.id = s.user_id
            WHERE u.status = 'active'
              AND s.last_login_at IS NOT NULL
              AND s.last_login_at >= ?
            """,
            (mau_cutoff,),
        ).fetchone()
        daily_new_signup_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM users
            WHERE created_at >= ?
            """,
            (signup_cutoff,),
        ).fetchone()
    active_users = int(active_users_row["cnt"] if active_users_row else 0)
    cumulative_users = int(cumulative_users_row["cnt"] if cumulative_users_row else 0)
    if cumulative_users < active_users:
        cumulative_users = active_users
    cumulative_deleted_users = max(0, cumulative_users - active_users)
    dau = int(daily_active_row["cnt"] if daily_active_row else 0)
    mau = int(monthly_active_row["cnt"] if monthly_active_row else 0)
    daily_new_users = int(daily_new_signup_row["cnt"] if daily_new_signup_row else 0)
    return {
        "cumulative_users": cumulative_users,
        "cumulative_deleted_users": cumulative_deleted_users,
        "active_users": active_users,
        "daily_new_users": daily_new_users,
        "dau": dau,
        "mau": mau,
        "daily_logged_in_users": dau,
    }


def get_preferences(user_id: int) -> dict:
    with _conn_ctx() as conn:
        row = conn.execute(
            "SELECT selected_site_id, updated_at FROM user_preferences WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        if row is None:
            now = utc_iso()
            conn.execute(
                "INSERT INTO user_preferences(user_id, selected_site_id, updated_at) VALUES(?, NULL, ?)",
                (int(user_id), now),
            )
            conn.commit()
            return {"selected_site_id": None, "updated_at": now}
        return {
            "selected_site_id": row["selected_site_id"],
            "updated_at": row["updated_at"],
        }


def set_selected_site(user_id: int, site_id: str | None) -> dict:
    now = utc_iso()
    clean_site_id = str(site_id).strip() if site_id is not None else None
    with _conn_ctx() as conn:
        conn.execute(
            """
            INSERT INTO user_preferences(user_id, selected_site_id, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET selected_site_id = excluded.selected_site_id, updated_at = excluded.updated_at
            """,
            (int(user_id), clean_site_id or None, now),
        )
        conn.commit()
    return {"selected_site_id": clean_site_id or None, "updated_at": now}


def get_endpoint_preference(user_id: int, site_id: str, day_type: str, direction: str) -> list[str] | None:
    with _conn_ctx() as conn:
        row = conn.execute(
            """
            SELECT endpoint_names_json
            FROM user_endpoint_preferences
            WHERE user_id = ? AND site_id = ? AND day_type = ? AND direction = ?
            LIMIT 1
            """,
            (int(user_id), str(site_id), str(day_type), str(direction)),
        ).fetchone()
        if row is None:
            return None
        try:
            parsed = json.loads(row["endpoint_names_json"] or "[]")
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, list):
            return None
        out: list[str] = []
        for item in parsed:
            name = str(item or "").strip()
            if name and name not in out:
                out.append(name)
        return out


def set_endpoint_preference(
    user_id: int,
    site_id: str,
    day_type: str,
    direction: str,
    endpoint_names: list[str],
) -> None:
    if direction not in {"depart", "arrive"}:
        raise ValueError("direction must be depart|arrive")
    clean: list[str] = []
    for item in endpoint_names:
        name = str(item or "").strip()
        if not name:
            continue
        if name in clean:
            continue
        clean.append(name)
    payload = json.dumps(clean, ensure_ascii=False)
    now = utc_iso()
    with _conn_ctx() as conn:
        conn.execute(
            """
            INSERT INTO user_endpoint_preferences(user_id, site_id, day_type, direction, endpoint_names_json, updated_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, site_id, day_type, direction)
            DO UPDATE SET endpoint_names_json = excluded.endpoint_names_json, updated_at = excluded.updated_at
            """,
            (int(user_id), str(site_id), str(day_type), str(direction), payload, now),
        )
        conn.commit()


def add_place_history(user_id: int, keyword: str) -> None:
    clean = str(keyword or "").strip()
    if not clean:
        return
    now = utc_iso()
    with _conn_ctx() as conn:
        conn.execute(
            "INSERT INTO place_search_history(user_id, keyword, searched_at) VALUES(?, ?, ?)",
            (int(user_id), clean[:120], now),
        )
        conn.commit()


def list_place_history(user_id: int, cursor: int | None, limit: int = 20) -> dict:
    page_limit = max(1, min(int(limit), 100))
    with _conn_ctx() as conn:
        if cursor is None:
            rows = conn.execute(
                """
                SELECT id, keyword, searched_at
                FROM place_search_history
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(user_id), page_limit + 1),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, keyword, searched_at
                FROM place_search_history
                WHERE user_id = ? AND id < ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(user_id), int(cursor), page_limit + 1),
            ).fetchall()

    has_more = len(rows) > page_limit
    sliced = rows[:page_limit]
    next_cursor = int(sliced[-1]["id"]) if has_more and sliced else None
    items = [
        {
            "id": int(row["id"]),
            "keyword": str(row["keyword"]),
            "searched_at": str(row["searched_at"]),
        }
        for row in sliced
    ]
    return {
        "items": items,
        "next_cursor": next_cursor,
        "has_more": has_more,
    }


def delete_place_history_item(user_id: int, history_id: int) -> bool:
    with _conn_ctx() as conn:
        cur = conn.execute(
            "DELETE FROM place_search_history WHERE user_id = ? AND id = ?",
            (int(user_id), int(history_id)),
        )
        conn.commit()
        return cur.rowcount > 0


def add_route_history(
    user_id: int,
    site_id: str,
    day_type: str,
    direction: str,
    lat: float,
    lng: float,
    selected_endpoints: list[str] | None,
    place_name: str | None,
) -> None:
    if direction not in {"depart", "arrive"}:
        return
    selected = []
    if isinstance(selected_endpoints, list):
        for item in selected_endpoints:
            value = str(item or "").strip()
            if value and value not in selected:
                selected.append(value)
    payload = json.dumps(selected, ensure_ascii=False)
    with _conn_ctx() as conn:
        conn.execute(
            """
            INSERT INTO route_search_history(
              user_id, site_id, day_type, direction, lat, lng,
              selected_endpoints_json, place_name, searched_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(user_id),
                str(site_id),
                str(day_type),
                str(direction),
                float(lat),
                float(lng),
                payload,
                (str(place_name).strip()[:120] if place_name else None),
                utc_iso(),
            ),
        )
        conn.commit()


def list_route_history(user_id: int, cursor: int | None, limit: int = 20) -> dict:
    page_limit = max(1, min(int(limit), 100))
    with _conn_ctx() as conn:
        if cursor is None:
            rows = conn.execute(
                """
                SELECT id, site_id, day_type, direction, lat, lng, selected_endpoints_json, place_name, searched_at
                FROM route_search_history
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(user_id), page_limit + 1),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, site_id, day_type, direction, lat, lng, selected_endpoints_json, place_name, searched_at
                FROM route_search_history
                WHERE user_id = ? AND id < ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(user_id), int(cursor), page_limit + 1),
            ).fetchall()

    has_more = len(rows) > page_limit
    sliced = rows[:page_limit]
    next_cursor = int(sliced[-1]["id"]) if has_more and sliced else None
    items = []
    for row in sliced:
        try:
            selected = json.loads(row["selected_endpoints_json"] or "[]")
        except json.JSONDecodeError:
            selected = []
        if not isinstance(selected, list):
            selected = []
        selected_clean = [str(item).strip() for item in selected if str(item).strip()]
        items.append(
            {
                "id": int(row["id"]),
                "site_id": str(row["site_id"]),
                "day_type": str(row["day_type"]),
                "direction": str(row["direction"]),
                "lat": float(row["lat"]),
                "lng": float(row["lng"]),
                "selected_endpoints": selected_clean,
                "place_name": row["place_name"],
                "searched_at": str(row["searched_at"]),
            }
        )
    return {"items": items, "next_cursor": next_cursor, "has_more": has_more}


def set_user_nickname(user_id: int, nickname: str) -> dict:
    clean_nickname = validate_nickname(nickname)
    nickname_norm = normalize_nickname(clean_nickname)
    now = utc_iso()
    with _conn_ctx() as conn:
        try:
            cur = conn.execute(
                """
                UPDATE users
                SET nickname = ?, nickname_norm = ?, updated_at = ?
                WHERE id = ? AND status = 'active'
                """,
                (clean_nickname, nickname_norm, now, int(user_id)),
            )
            if cur.rowcount <= 0:
                conn.rollback()
                raise UserStoreError("사용자를 찾을 수 없습니다.")
            conn.commit()
        except sqlite3.IntegrityError as exc:
            conn.rollback()
            raise NicknameConflictError("이미 사용 중인 닉네임입니다.") from exc
    user = get_user_by_id(int(user_id))
    if user is None:
        raise UserStoreError("사용자를 찾을 수 없습니다.")
    return user


def upsert_email_consent(user_id: int, consented: bool, context: str) -> None:
    with _conn_ctx() as conn:
        conn.execute(
            "INSERT INTO consent_events(user_id, consent_type, consented, context, created_at) VALUES(?, 'email', ?, ?, ?)",
            (int(user_id), 1 if consented else 0, str(context), utc_iso()),
        )
        conn.commit()


def cleanup_old_data(
    history_retention_days: int = 7,
    prune_auth_states: bool = True,
    prune_sessions: bool = True,
) -> None:
    now = utc_iso()
    retention = max(1, int(history_retention_days))
    cutoff = utc_iso(utc_now() - timedelta(days=retention))
    with _conn_ctx() as conn:
        conn.execute("DELETE FROM place_search_history WHERE searched_at < ?", (cutoff,))
        conn.execute("DELETE FROM route_search_history WHERE searched_at < ?", (cutoff,))
        if prune_auth_states:
            conn.execute("DELETE FROM oauth_states WHERE expires_at <= ?", (now,))
            conn.execute(
                "DELETE FROM pending_signups WHERE expires_at <= ? OR (consumed_at IS NOT NULL AND consumed_at <= ?)",
                (now, utc_iso(utc_now() - timedelta(days=1))),
            )
        if prune_sessions:
            conn.execute("DELETE FROM user_sessions WHERE expires_at <= ?", (now,))
        conn.commit()


def delete_user_and_enqueue_cleanup(user_id: int, deadline_hours: int = 24) -> dict | None:
    now_dt = utc_now()
    now = utc_iso(now_dt)
    deadline_at = utc_iso(now_dt + timedelta(hours=max(1, int(deadline_hours))))
    with _conn_ctx() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT id, public_user_id FROM users WHERE id = ? AND status = 'active'",
            (int(user_id),),
        ).fetchone()
        if row is None:
            conn.rollback()
            return None
        public_user_id = str(row["public_user_id"])
        conn.execute("DELETE FROM users WHERE id = ?", (int(user_id),))
        conn.execute(
            """
            INSERT INTO github_cleanup_jobs(public_user_id, status, attempts, next_attempt_at, deadline_at, last_error, created_at, updated_at)
            VALUES(?, 'pending', 0, ?, ?, NULL, ?, ?)
            """,
            (public_user_id, now, deadline_at, now, now),
        )
        conn.commit()
        return {"public_user_id": public_user_id, "deadline_at": deadline_at}


def claim_due_github_cleanup_job() -> dict | None:
    now = utc_iso()
    with _conn_ctx() as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT id, public_user_id, status, attempts, next_attempt_at, deadline_at, last_error, created_at, updated_at
            FROM github_cleanup_jobs
            WHERE status IN ('pending', 'failed')
              AND next_attempt_at <= ?
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (now,),
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        cur = conn.execute(
            """
            UPDATE github_cleanup_jobs
            SET status = 'in_progress', attempts = attempts + 1, updated_at = ?
            WHERE id = ? AND status IN ('pending', 'failed')
            """,
            (now, int(row["id"])),
        )
        if cur.rowcount <= 0:
            conn.commit()
            return None
        updated = conn.execute(
            """
            SELECT id, public_user_id, status, attempts, next_attempt_at, deadline_at, last_error, created_at, updated_at
            FROM github_cleanup_jobs
            WHERE id = ?
            """,
            (int(row["id"]),),
        ).fetchone()
        conn.commit()
    if updated is None:
        return None
    return {
        "id": int(updated["id"]),
        "public_user_id": str(updated["public_user_id"]),
        "status": str(updated["status"]),
        "attempts": int(updated["attempts"]),
        "next_attempt_at": str(updated["next_attempt_at"]),
        "deadline_at": str(updated["deadline_at"]),
        "last_error": updated["last_error"],
        "created_at": str(updated["created_at"]),
        "updated_at": str(updated["updated_at"]),
    }


def complete_github_cleanup_job(job_id: int) -> None:
    now = utc_iso()
    with _conn_ctx() as conn:
        conn.execute(
            "UPDATE github_cleanup_jobs SET status = 'completed', updated_at = ?, last_error = NULL WHERE id = ?",
            (now, int(job_id)),
        )
        conn.commit()


def fail_github_cleanup_job(job_id: int, error: str, retry_after_sec: int = 300) -> None:
    now_dt = utc_now()
    now = utc_iso(now_dt)
    next_try = utc_iso(now_dt + timedelta(seconds=max(30, int(retry_after_sec))))
    with _conn_ctx() as conn:
        row = conn.execute(
            "SELECT deadline_at FROM github_cleanup_jobs WHERE id = ? LIMIT 1",
            (int(job_id),),
        ).fetchone()
        if row is None:
            return
        deadline = parse_iso(str(row["deadline_at"]))
        status = "failed"
        if deadline is not None and now_dt > deadline:
            status = "failed"
            next_try = now
        conn.execute(
            """
            UPDATE github_cleanup_jobs
            SET status = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (status, str(error or "")[:1000], next_try, now, int(job_id)),
        )
        conn.commit()


def get_cleanup_job_stats() -> dict:
    with _conn_ctx() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM github_cleanup_jobs GROUP BY status"
        ).fetchall()
    stats = {"pending": 0, "failed": 0, "in_progress": 0, "completed": 0}
    for row in rows:
        stats[str(row["status"])] = int(row["cnt"])
    return stats
