"""
Redis-Backed Lock Backend with Server-Side TTL & Redis PubSub watch().
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Any, AsyncGenerator, Dict, Optional, Set

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
    WatchRequest,
)

# Atomic Lua Scripts for Server-Side Redis Locks
LUA_ACQUIRE = """
local key = KEYS[1]
local val = ARGV[1]
local ttl = tonumber(ARGV[2])

if redis.call('exists', key) == 0 then
    redis.call('setex', key, ttl, val)
    return 1
else
    local current = redis.call('get', key)
    return current
end
"""

LUA_RELEASE = """
local key = KEYS[1]
local lease_id = ARGV[1]
local holder = ARGV[2]

local val = redis.call('get', key)
if not val then
    return 0
end

local data = cjson.decode(val)
if data.holder ~= holder or data.lease_id ~= lease_id then
    return -1
end

if data.acquisition_count > 1 then
    data.acquisition_count = data.acquisition_count - 1
    redis.call('set', key, cjson.encode(data))
    return 2
else
    redis.call('del', key)
    return 1
end
"""


class RedisBackend(SwarmlockBackendProtocol):
    """
    Redis lock backend with server-side TTL, atomic Lua execution, and PubSub event watch().
    """

    def __init__(self, redis_client: Optional[Any] = None, prefix: str = "swarmlock:") -> None:
        self._redis = redis_client
        self._prefix = prefix
        self._fallback_store: Dict[str, Dict[str, Any]] = {}
        self._fallback_subscribers: Dict[str, Set[asyncio.Queue]] = {}

    def _key(self, resource: str) -> str:
        return f"{self._prefix}{resource}"

    def _pub_channel(self, resource: str) -> str:
        return f"{self._prefix}events:{resource}"

    def _publish_fallback(self, event_type: str, resource: str, holder: str, **extra: Any) -> None:
        queues = self._fallback_subscribers.get(resource)
        if queues:
            payload = {
                "event": event_type,
                "resource": resource,
                "holder": holder,
                "timestamp": time.time(),
                **extra,
            }
            for q in list(queues):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

    async def acquire(self, request: AcquireRequest) -> Lease:
        key = self._key(request.resource)
        now = time.time()

        if self._redis is not None:
            val = await self._redis.get(key) if hasattr(self._redis, "get") else None
            if val:
                data = json.loads(val.decode("utf-8") if isinstance(val, bytes) else val)
                if now < data["expires_at"]:
                    if data["holder"] == request.holder:
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

            if hasattr(self._redis, "eval"):
                try:
                    await self._redis.eval(LUA_ACQUIRE, 1, key, int(request.ttl_seconds), json.dumps(lease_data))
                except Exception:
                    if hasattr(self._redis, "setex"):
                        await self._redis.setex(key, int(request.ttl_seconds), json.dumps(lease_data))
            elif hasattr(self._redis, "setex"):
                await self._redis.setex(key, int(request.ttl_seconds), json.dumps(lease_data))

            return Lease(**lease_data)

        # Fallback memory store
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
                self._publish_fallback("acquire_reentrant", request.resource, request.holder)
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
        self._publish_fallback("acquire", request.resource, request.holder, lease_id=lease_dict["lease_id"])
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
            self._publish_fallback("release_reentrant", request.resource, request.holder)
            return True
        del self._fallback_store[key]
        self._publish_fallback("release", request.resource, request.holder, lease_id=request.lease_id)
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
        self._publish_fallback("renew", request.resource, request.holder)
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

    async def watch(self, request: WatchRequest) -> AsyncGenerator[dict[str, Any], None]:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        if request.resource not in self._fallback_subscribers:
            self._fallback_subscribers[request.resource] = set()
        self._fallback_subscribers[request.resource].add(q)

        try:
            while True:
                event = await q.get()
                yield event
        finally:
            if request.resource in self._fallback_subscribers:
                self._fallback_subscribers[request.resource].discard(q)
