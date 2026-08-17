"""
Redis-Backed Lock Backend.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any, Dict, Optional

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


class RedisBackend(SwarmlockBackendProtocol):
    """
    Redis-based lock backend (uses redis-py if available, with internal storage fallback).
    """

    def __init__(self, redis_client: Optional[Any] = None, prefix: str = "swarmlock:") -> None:
        self._redis = redis_client
        self._prefix = prefix
        # Fallback dictionary if redis client is not provided in mock/unit tests
        self._fallback_store: Dict[str, Dict[str, Any]] = {}

    def _key(self, resource: str) -> str:
        return f"{self._prefix}{resource}"

    async def acquire(self, request: AcquireRequest) -> Lease:
        key = self._key(request.resource)
        now = time.time()

        if self._redis is not None:
            # Redis-based execution
            val = await self._redis.get(key) if hasattr(self._redis, "get") else None
            if val:
                data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
                if now < data["expires_at"]:
                    if data["holder"] == request.holder:
                        # GRO-4763 Idempotency Key check
                        if (
                            request.idempotency_key is not None
                            and data.get("idempotency_key") == request.idempotency_key
                        ):
                            return Lease(**data)

                        data["acquisition_count"] += 1
                        data["expires_at"] = now + request.ttl_seconds
                        encoded = json.dumps(data)
                        if hasattr(self._redis, "setex"):
                            await self._redis.setex(key, int(request.ttl_seconds), encoded)
                        return Lease(**data)
                    else:
                        raise LockConflictError(request.resource, data["holder"])

            lease_data = {
                "lease_id": str(uuid.uuid4()),
                "resource": request.resource,
                "holder": request.holder,
                "expires_at": now + request.ttl_seconds,
                "ttl_seconds": request.ttl_seconds,
                "acquisition_count": 1,
                "idempotency_key": request.idempotency_key,
                "created_at": now,
                "metadata": request.metadata,
            }
            if hasattr(self._redis, "setex"):
                await self._redis.setex(key, int(request.ttl_seconds), json.dumps(lease_data))
            return Lease(**lease_data)

        # Fallback memory store for testing without Redis server
        existing = self._fallback_store.get(key)
        if existing and now < existing["expires_at"]:
            if existing["holder"] == request.holder:
                if (
                    request.idempotency_key is not None
                    and existing.get("idempotency_key") == request.idempotency_key
                ):
                    return Lease(**existing)

                existing["acquisition_count"] += 1
                existing["expires_at"] = now + request.ttl_seconds
                return Lease(**existing)
            else:
                raise LockConflictError(request.resource, existing["holder"])

        lease_dict = {
            "lease_id": str(uuid.uuid4()),
            "resource": request.resource,
            "holder": request.holder,
            "expires_at": now + request.ttl_seconds,
            "ttl_seconds": request.ttl_seconds,
            "acquisition_count": 1,
            "idempotency_key": request.idempotency_key,
            "created_at": now,
            "metadata": request.metadata,
        }
        self._fallback_store[key] = lease_dict
        return Lease(**lease_dict)

    async def release(self, request: ReleaseRequest) -> bool:
        key = self._key(request.resource)
        if self._redis is not None:
            val = await self._redis.get(key) if hasattr(self._redis, "get") else None
            if not val:
                return False
            data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
            if data["holder"] != request.holder:
                raise LeaseNotHeldError(f"Lease not held by '{request.holder}'")
            if data["lease_id"] != request.lease_id:
                return False
            if data["acquisition_count"] > 1:
                data["acquisition_count"] -= 1
                if hasattr(self._redis, "set"):
                    await self._redis.set(key, json.dumps(data))
                return True
            if hasattr(self._redis, "delete"):
                await self._redis.delete(key)
            return True

        existing = self._fallback_store.get(key)
        if not existing:
            return False
        if existing["holder"] != request.holder:
            raise LeaseNotHeldError(f"Lease not held by '{request.holder}'")
        if existing["lease_id"] != request.lease_id:
            return False
        if existing["acquisition_count"] > 1:
            existing["acquisition_count"] -= 1
            return True
        del self._fallback_store[key]
        return True

    async def renew(self, request: RenewRequest) -> Lease:
        key = self._key(request.resource)
        now = time.time()
        if self._redis is not None:
            val = await self._redis.get(key) if hasattr(self._redis, "get") else None
            if not val:
                raise LeaseExpiredError(f"Lease for '{request.resource}' expired")
            data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
            if data["holder"] != request.holder or data["lease_id"] != request.lease_id:
                raise LeaseNotHeldError(f"Lease not held by caller")
            data["expires_at"] = now + request.extend_seconds
            data["ttl_seconds"] = request.extend_seconds
            if hasattr(self._redis, "setex"):
                await self._redis.setex(key, int(request.extend_seconds), json.dumps(data))
            return Lease(**data)

        existing = self._fallback_store.get(key)
        if not existing or now >= existing["expires_at"]:
            raise LeaseExpiredError(f"Lease for '{request.resource}' expired")
        if existing["holder"] != request.holder or existing["lease_id"] != request.lease_id:
            raise LeaseNotHeldError(f"Lease not held by caller")
        existing["expires_at"] = now + request.extend_seconds
        existing["ttl_seconds"] = request.extend_seconds
        return Lease(**existing)

    async def get_lease(self, resource: str) -> Optional[Lease]:
        key = self._key(resource)
        now = time.time()
        if self._redis is not None:
            val = await self._redis.get(key) if hasattr(self._redis, "get") else None
            if val:
                data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
                if now < data["expires_at"]:
                    return Lease(**data)
            return None

        existing = self._fallback_store.get(key)
        if existing and now < existing["expires_at"]:
            return Lease(**existing)
        return None
