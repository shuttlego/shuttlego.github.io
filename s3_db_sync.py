"""S3-backed SQLite DB manifest and sync utilities.

Manifest format:
{
  "schema_version": 1,
  "db_key": "db/dev/versions/data.20260309193000.db",
  "sha256": "...",
  "size_bytes": 123,
  "updated_at_utc": "2026-03-09T10:30:00Z",
  "updated_by": "tmap-collector",
  "metadata": {...}
}
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import boto3
except Exception:  # pragma: no cover - runtime optional
    boto3 = None


@dataclasses.dataclass(frozen=True)
class S3DbConfig:
    bucket: str
    prefix: str
    manifest_key: str
    region: str

    @staticmethod
    def from_env(*, default_prefix: str = "db/dev") -> "S3DbConfig":
        bucket = str(os.environ.get("DB_S3_BUCKET", "") or "").strip()
        prefix = str(os.environ.get("DB_S3_PREFIX", default_prefix) or default_prefix).strip().strip("/")
        manifest_key = str(os.environ.get("DB_S3_MANIFEST_KEY", "") or "").strip().strip("/")
        region = str(os.environ.get("AWS_REGION", "ap-northeast-2") or "ap-northeast-2").strip()
        if not manifest_key:
            manifest_key = f"{prefix}/latest.json"
        return S3DbConfig(
            bucket=bucket,
            prefix=prefix,
            manifest_key=manifest_key,
            region=region,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.bucket and self.manifest_key)


@dataclasses.dataclass(frozen=True)
class ManifestRecord:
    db_key: str
    sha256: str
    size_bytes: int
    updated_at_utc: str
    updated_by: str
    metadata: dict[str, Any]

    @staticmethod
    def from_json(payload: dict[str, Any]) -> "ManifestRecord":
        return ManifestRecord(
            db_key=str(payload.get("db_key") or "").strip(),
            sha256=str(payload.get("sha256") or "").strip().lower(),
            size_bytes=int(payload.get("size_bytes") or 0),
            updated_at_utc=str(payload.get("updated_at_utc") or "").strip(),
            updated_by=str(payload.get("updated_by") or "").strip(),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclasses.dataclass(frozen=True)
class SyncResult:
    updated: bool
    version_key: str
    reason: str


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


class S3DbStore:
    def __init__(self, config: S3DbConfig):
        self.config = config
        if not config.enabled:
            raise ValueError("S3DbConfig is not enabled")
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 DB sync")
        self._client = boto3.client("s3", region_name=config.region)

    def load_manifest(self) -> ManifestRecord | None:
        try:
            resp = self._client.get_object(Bucket=self.config.bucket, Key=self.config.manifest_key)
        except self._client.exceptions.NoSuchKey:
            return None
        except Exception:
            return None
        raw = resp.get("Body").read()
        payload = json.loads(raw.decode("utf-8"))
        rec = ManifestRecord.from_json(payload)
        if not rec.db_key:
            return None
        return rec

    def download_manifest_db(self, manifest: ManifestRecord, dst_path: Path) -> None:
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            prefix=f".{dst_path.name}.",
            suffix=".tmp",
            dir=str(dst_path.parent),
            delete=False,
        ) as tmp:
            tmp_path = Path(tmp.name)
        try:
            self._client.download_file(self.config.bucket, manifest.db_key, str(tmp_path))
            if manifest.sha256:
                actual = _sha256_file(tmp_path)
                if actual.lower() != manifest.sha256.lower():
                    raise RuntimeError(
                        f"Downloaded DB checksum mismatch key={manifest.db_key} expected={manifest.sha256} actual={actual}"
                    )
            os.replace(str(tmp_path), str(dst_path))
        finally:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass

    def sync_latest_to_local(self, dst_path: Path, *, current_version_key: str = "") -> SyncResult:
        manifest = self.load_manifest()
        if manifest is None:
            return SyncResult(False, "", "manifest_not_found")
        if current_version_key and manifest.db_key == current_version_key:
            return SyncResult(False, manifest.db_key, "already_latest")
        self.download_manifest_db(manifest, dst_path)
        return SyncResult(True, manifest.db_key, "updated")

    def publish_local_db(
        self,
        local_db_path: Path,
        *,
        updated_by: str,
        metadata: dict[str, Any] | None = None,
    ) -> ManifestRecord:
        if not local_db_path.exists():
            raise FileNotFoundError(str(local_db_path))
        if metadata is None:
            metadata = {}
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y%m%d%H%M%S")
        version_key = f"{self.config.prefix}/versions/data.{stamp}.db"
        checksum = _sha256_file(local_db_path)
        size_bytes = int(local_db_path.stat().st_size)

        self._client.upload_file(str(local_db_path), self.config.bucket, version_key)

        payload = {
            "schema_version": 1,
            "db_key": version_key,
            "sha256": checksum,
            "size_bytes": size_bytes,
            "updated_at_utc": _utc_now_iso(),
            "updated_by": str(updated_by or "unknown"),
            "metadata": metadata,
        }
        self._client.put_object(
            Bucket=self.config.bucket,
            Key=self.config.manifest_key,
            Body=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json",
        )

        return ManifestRecord.from_json(payload)
