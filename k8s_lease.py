"""Best-effort Kubernetes Lease lock helper.

If Kubernetes client or in-cluster config is unavailable, the lock gracefully
falls back to "not acquired" mode.
"""

from __future__ import annotations

import dataclasses
import os
import time
from datetime import datetime, timedelta, timezone

try:
    from kubernetes import client, config
    from kubernetes.client import ApiException
except Exception:  # pragma: no cover - optional dependency
    client = None
    config = None
    ApiException = Exception


@dataclasses.dataclass
class LeaseSettings:
    enabled: bool
    name: str
    namespace: str
    holder_identity: str
    lease_duration_sec: int
    acquire_timeout_sec: int

    @staticmethod
    def from_env(
        *,
        default_name: str = "db-publish-lease",
        default_namespace: str = "app",
        default_duration_sec: int = 120,
        default_timeout_sec: int = 60,
    ) -> "LeaseSettings":
        enabled = str(os.environ.get("K8S_LEASE_ENABLED", "1") or "1").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        name = str(os.environ.get("K8S_LEASE_NAME", default_name) or default_name).strip()
        namespace = str(
            os.environ.get("K8S_LEASE_NAMESPACE")
            or os.environ.get("POD_NAMESPACE")
            or default_namespace
        ).strip()
        holder = str(
            os.environ.get("K8S_LEASE_HOLDER_ID")
            or f"{os.environ.get('HOSTNAME', 'unknown')}:{os.getpid()}"
        ).strip()
        duration = max(30, int(os.environ.get("K8S_LEASE_DURATION_SEC", str(default_duration_sec))))
        timeout = max(5, int(os.environ.get("K8S_LEASE_ACQUIRE_TIMEOUT_SEC", str(default_timeout_sec))))
        return LeaseSettings(
            enabled=enabled,
            name=name,
            namespace=namespace,
            holder_identity=holder,
            lease_duration_sec=duration,
            acquire_timeout_sec=timeout,
        )


class LeaseLock:
    def __init__(self, settings: LeaseSettings):
        self.settings = settings
        self._api = None
        self.acquired = False

    def _load_api(self) -> bool:
        if not self.settings.enabled or client is None or config is None:
            return False
        if self._api is not None:
            return True
        try:
            config.load_incluster_config()
        except Exception:
            return False
        self._api = client.CoordinationV1Api()
        return True

    @staticmethod
    def _to_datetime(value) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        raw = str(value).strip()
        if not raw:
            return None
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
        except Exception:
            return None

    def _is_expired(self, lease) -> bool:
        spec = getattr(lease, "spec", None)
        if spec is None:
            return True
        renew_time = self._to_datetime(getattr(spec, "renew_time", None))
        acquire_time = self._to_datetime(getattr(spec, "acquire_time", None))
        base = renew_time or acquire_time
        if base is None:
            return True
        duration = int(getattr(spec, "lease_duration_seconds", self.settings.lease_duration_sec) or self.settings.lease_duration_sec)
        return datetime.now(timezone.utc) >= (base + timedelta(seconds=duration))

    def acquire(self) -> bool:
        if not self._load_api():
            return False

        deadline = time.monotonic() + float(self.settings.acquire_timeout_sec)
        while time.monotonic() < deadline:
            now = datetime.now(timezone.utc)
            now_str = now.isoformat().replace("+00:00", "Z")
            try:
                lease = self._api.read_namespaced_lease(
                    name=self.settings.name,
                    namespace=self.settings.namespace,
                )
            except ApiException as exc:
                if getattr(exc, "status", None) == 404:
                    body = client.V1Lease(
                        metadata=client.V1ObjectMeta(
                            name=self.settings.name,
                            namespace=self.settings.namespace,
                        ),
                        spec=client.V1LeaseSpec(
                            holder_identity=self.settings.holder_identity,
                            lease_duration_seconds=self.settings.lease_duration_sec,
                            acquire_time=now_str,
                            renew_time=now_str,
                        ),
                    )
                    try:
                        self._api.create_namespaced_lease(self.settings.namespace, body)
                        self.acquired = True
                        return True
                    except ApiException:
                        time.sleep(1.0)
                        continue
                return False
            except Exception:
                return False

            spec = getattr(lease, "spec", None)
            holder = str(getattr(spec, "holder_identity", "") or "")
            expired = self._is_expired(lease)
            if holder and holder != self.settings.holder_identity and not expired:
                time.sleep(1.0)
                continue

            body = client.V1Lease(
                metadata=client.V1ObjectMeta(
                    name=self.settings.name,
                    namespace=self.settings.namespace,
                    resource_version=lease.metadata.resource_version,
                ),
                spec=client.V1LeaseSpec(
                    holder_identity=self.settings.holder_identity,
                    lease_duration_seconds=self.settings.lease_duration_sec,
                    acquire_time=getattr(spec, "acquire_time", None) or now_str,
                    renew_time=now_str,
                ),
            )
            try:
                self._api.replace_namespaced_lease(
                    name=self.settings.name,
                    namespace=self.settings.namespace,
                    body=body,
                )
                self.acquired = True
                return True
            except ApiException:
                time.sleep(1.0)
                continue
            except Exception:
                return False
        return False

    def release(self) -> None:
        if not self.acquired or self._api is None:
            return
        try:
            lease = self._api.read_namespaced_lease(
                name=self.settings.name,
                namespace=self.settings.namespace,
            )
            spec = getattr(lease, "spec", None)
            holder = str(getattr(spec, "holder_identity", "") or "")
            if holder != self.settings.holder_identity:
                self.acquired = False
                return
            body = client.V1Lease(
                metadata=client.V1ObjectMeta(
                    name=self.settings.name,
                    namespace=self.settings.namespace,
                    resource_version=lease.metadata.resource_version,
                ),
                spec=client.V1LeaseSpec(
                    holder_identity="",
                    lease_duration_seconds=self.settings.lease_duration_sec,
                    renew_time=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                ),
            )
            self._api.replace_namespaced_lease(
                name=self.settings.name,
                namespace=self.settings.namespace,
                body=body,
            )
        except Exception:
            pass
        finally:
            self.acquired = False

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
        return False
