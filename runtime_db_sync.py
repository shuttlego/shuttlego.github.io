"""Runtime DB sync worker for backend pods."""

from __future__ import annotations

import hashlib
import logging
import os
import random
import sqlite3
import threading
from pathlib import Path
from typing import Callable

from s3_db_sync import ManifestRecord, S3DbConfig, S3DbStore

LOG = logging.getLogger("runtime-db-sync")


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _sanitize_version_file_name(version_key: str) -> str:
    token = Path(str(version_key or "").strip()).name.strip()
    if not token:
        token = "data.unknown.db"
    if not token.endswith(".db"):
        token = f"{token}.db"
    return token.replace("/", "_")


class RuntimeDbSyncWorker:
    def __init__(
        self,
        *,
        enabled: bool,
        interval_sec: int,
        jitter_sec: int,
        local_db_path: Path,
        default_prefix: str,
    ):
        self.enabled = enabled
        self.interval_sec = max(30, int(interval_sec))
        self.jitter_sec = max(0, int(jitter_sec))
        self.local_db_path = local_db_path
        self.default_prefix = default_prefix
        self.keep_versions = max(2, int(os.environ.get("APP_DB_S3_SYNC_KEEP_VERSIONS", "8")))
        self.versions_dir = self.local_db_path.parent / "versions"
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._current_version_key = ""
        self._current_manifest_updated_at_utc = ""
        self._store: S3DbStore | None = None
        self.active_db_path = self.local_db_path

        if not self.enabled:
            return

        try:
            config = S3DbConfig.from_env(default_prefix=self.default_prefix)
            if config.enabled:
                self._store = S3DbStore(config)
            else:
                LOG.info("DB sync disabled: missing DB_S3_BUCKET/manifest settings")
                self.enabled = False
        except Exception:
            LOG.exception("DB sync disabled due to initialization error")
            self.enabled = False

    def _target_path_for_manifest(self, manifest: ManifestRecord) -> Path:
        name = _sanitize_version_file_name(manifest.db_key)
        return self.versions_dir / name

    def _local_snapshot_matches_manifest(self, local_path: Path, manifest: ManifestRecord) -> bool:
        if not local_path.exists():
            return False
        try:
            size_bytes = int(local_path.stat().st_size)
        except OSError:
            return False
        if int(manifest.size_bytes or 0) > 0 and size_bytes != int(manifest.size_bytes):
            return False
        expected = str(manifest.sha256 or "").strip().lower()
        if expected:
            try:
                actual = _sha256_file(local_path).lower()
            except OSError:
                return False
            if actual != expected:
                return False
        return True

    @staticmethod
    def _validate_snapshot(local_path: Path) -> None:
        conn = sqlite3.connect(f"file:{local_path}?mode=ro&immutable=1", uri=True)
        try:
            conn.row_factory = sqlite3.Row
            required_tables = ("site", "route", "service_variant", "variant_stop", "stop")
            for table in required_tables:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
                    (table,),
                ).fetchone()
                if row is None:
                    raise RuntimeError(f"Missing required table: {table}")
            # smoke queries for schema/index integrity
            conn.execute("SELECT COUNT(*) FROM route").fetchone()
            conn.execute("SELECT COUNT(*) FROM service_variant").fetchone()
            conn.execute("SELECT COUNT(*) FROM variant_stop").fetchone()
            conn.execute("SELECT COUNT(*) FROM stop").fetchone()
        finally:
            conn.close()

    def _cleanup_old_versions(self) -> None:
        try:
            files = [p for p in self.versions_dir.glob("*.db") if p.is_file()]
        except OSError:
            return
        if len(files) <= self.keep_versions:
            return
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        keep: set[Path] = set(files[: self.keep_versions])
        keep.add(self.active_db_path)
        for path in files:
            if path in keep:
                continue
            try:
                path.unlink()
            except OSError:
                continue

    def sync_once(self, on_updated: Callable[[Path, str, str | None], None] | None = None) -> bool:
        if not self.enabled or self._store is None:
            return False
        try:
            manifest = self._store.load_manifest()
        except Exception:
            LOG.exception("DB sync failed while loading manifest")
            return False
        if manifest is None:
            return False

        if self._current_version_key and manifest.db_key == self._current_version_key and self.active_db_path.exists():
            return False

        candidate_path = self._target_path_for_manifest(manifest)
        try:
            candidate_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            LOG.exception("DB sync failed creating versions directory path=%s", candidate_path.parent)
            return False

        try:
            if not self._local_snapshot_matches_manifest(candidate_path, manifest):
                self._store.download_manifest_db(manifest, candidate_path)
            self._validate_snapshot(candidate_path)
        except Exception:
            # safety guard: do not switch active DB if candidate download/validation fails
            LOG.exception("DB sync candidate rejected version=%s path=%s", manifest.db_key, candidate_path)
            return False

        self._current_version_key = manifest.db_key
        self._current_manifest_updated_at_utc = manifest.updated_at_utc
        self.active_db_path = candidate_path
        self._cleanup_old_versions()
        LOG.info(
            "DB sync prepared version=%s path=%s updated_at_utc=%s",
            manifest.db_key,
            candidate_path,
            manifest.updated_at_utc,
        )

        if on_updated is not None:
            try:
                on_updated(candidate_path, manifest.db_key, manifest.updated_at_utc)
            except Exception:
                LOG.exception("DB sync on_updated callback failed")
        return True

    def start(self, on_updated: Callable[[Path, str, str | None], None] | None = None) -> None:
        if not self.enabled:
            return
        if self._thread is not None and self._thread.is_alive():
            return

        def _loop() -> None:
            while not self._stop_event.is_set():
                self.sync_once(on_updated=on_updated)
                sleep_for = float(self.interval_sec)
                if self.jitter_sec > 0:
                    sleep_for += random.uniform(0.0, float(self.jitter_sec))
                self._stop_event.wait(timeout=sleep_for)

        self._thread = threading.Thread(target=_loop, daemon=True, name="db-sync-loop")
        self._thread.start()

    def stop(self, timeout_sec: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=timeout_sec)
