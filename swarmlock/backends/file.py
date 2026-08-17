"""
File-Backed Lock Backend.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path
from typing import Dict, Any, Optional

from swarmlock.protocol import SwarmlockBackendProtocol
from swarmlock.types import (
    AcquireRequest,
    Lease,
    LeaseAcquireError,
    LeaseExpiredError,
    LeaseNotHeldError,
    LockConflictError,
    ReleaseRequest,
    RenewRequest,
)


class FileBackend(SwarmlockBackendProtocol):
    """
    File-based lock backend storing state in a JSON registry.
    """

    def __init__(self, registry_file: Optional[str | Path] = None) -> None:
        if registry_file is None:
            home = os.environ.get("SWARMLOCK_HOME", os.path.expanduser("~"))
            registry_file = Path(home) / ".antigravity" / "swarmlock_registry.json"
        self._registry_file = Path(registry_file)
        self._registry_file.parent.mkdir(parents=True, exist_ok=True)

    def _read_data(self) -> Dict[str, Any]:
        if not self._registry_file.exists():
            return {}
        try:
            with open(self._registry_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}

    def _write_data(self, data: Dict[str, Any]) -> None:
        tmp_file = self._registry_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_file, self._registry_file)

    def _prune(self, data: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        kept = {}
        for res, raw in data.items():
            if now < raw.get("expires_at", 0):
                kept[res] = raw
        return kept

    def _dict_to_lease(self, raw: Dict[str, Any]) -> Lease:
        return Lease(
            lease_id=raw["lease_id"],
            resource=raw["resource"],
            holder=raw["holder"],
            expires_at=raw["expires_at"],
            ttl_seconds=raw["ttl_seconds"],
            acquisition_count=raw.get("acquisition_count", 1),
            idempotency_key=raw.get("idempotency_key"),
            created_at=raw.get("created_at", time.time()),
            metadata=raw.get("metadata", {}),
        )

    def _lease_to_dict(self, lease: Lease) -> Dict[str, Any]:
        return {
            "lease_id": lease.lease_id,
            "resource": lease.resource,
            "holder": lease.holder,
            "expires_at": lease.expires_at,
            "ttl_seconds": lease.ttl_seconds,
            "acquisition_count": lease.acquisition_count,
            "idempotency_key": lease.idempotency_key,
            "created_at": lease.created_at,
            "metadata": lease.metadata,
        }

    async def acquire(self, request: AcquireRequest) -> Lease:
        data = self._read_data()
        data = self._prune(data)
        existing_raw = data.get(request.resource)

        if existing_raw is not None and time.time() < existing_raw["expires_at"]:
            if existing_raw["holder"] == request.holder:
                # GRO-4763 Idempotency Key check
                if (
                    request.idempotency_key is not None
                    and existing_raw.get("idempotency_key") == request.idempotency_key
                ):
                    return self._dict_to_lease(existing_raw)

                if request.reentrant:
                    existing_raw["acquisition_count"] = existing_raw.get("acquisition_count", 1) + 1
                    existing_raw["expires_at"] = time.time() + request.ttl_seconds
                    self._write_data(data)
                    return self._dict_to_lease(existing_raw)

                existing_raw["acquisition_count"] = existing_raw.get("acquisition_count", 1) + 1
                existing_raw["expires_at"] = time.time() + request.ttl_seconds
                self._write_data(data)
                return self._dict_to_lease(existing_raw)
            else:
                raise LockConflictError(request.resource, existing_raw["holder"])

        now = time.time()
        lease = Lease(
            lease_id=str(uuid.uuid4()),
            resource=request.resource,
            holder=request.holder,
            expires_at=now + request.ttl_seconds,
            ttl_seconds=request.ttl_seconds,
            acquisition_count=1,
            idempotency_key=request.idempotency_key,
            created_at=now,
            metadata=request.metadata,
        )

        data[request.resource] = self._lease_to_dict(lease)
        self._write_data(data)
        return lease

    async def release(self, request: ReleaseRequest) -> bool:
        data = self._read_data()
        data = self._prune(data)
        existing_raw = data.get(request.resource)

        if existing_raw is None:
            return False

        if existing_raw["holder"] != request.holder:
            raise LeaseNotHeldError(f"Lease not held by '{request.holder}'")

        if existing_raw["lease_id"] != request.lease_id:
            return False

        acq_count = existing_raw.get("acquisition_count", 1)
        if acq_count > 1:
            existing_raw["acquisition_count"] = acq_count - 1
            self._write_data(data)
            return True

        del data[request.resource]
        self._write_data(data)
        return True

    async def renew(self, request: RenewRequest) -> Lease:
        data = self._read_data()
        data = self._prune(data)
        existing_raw = data.get(request.resource)

        if existing_raw is None or time.time() >= existing_raw["expires_at"]:
            raise LeaseExpiredError(f"Lease for '{request.resource}' expired")

        if existing_raw["holder"] != request.holder or existing_raw["lease_id"] != request.lease_id:
            raise LeaseNotHeldError(f"Lease for '{request.resource}' not held by caller")

        existing_raw["expires_at"] = time.time() + request.extend_seconds
        existing_raw["ttl_seconds"] = request.extend_seconds
        self._write_data(data)
        return self._dict_to_lease(existing_raw)

    async def get_lease(self, resource: str) -> Optional[Lease]:
        data = self._read_data()
        data = self._prune(data)
        raw = data.get(resource)
        if raw and time.time() < raw["expires_at"]:
            return self._dict_to_lease(raw)
        return None
