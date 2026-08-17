"""
In-Process Lock Backend with Real-Time Event Bus.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

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


class InProcessBackend(SwarmlockBackendProtocol):
    """
    In-memory async lock backend with idempotency-key, reentrancy, and watch() event streaming.
    """

    def __init__(self) -> None:
        self._leases: Dict[str, Lease] = {}
        self._lock = asyncio.Lock()
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}

    def _prune_expired(self) -> None:
        now = time.time()
        expired = [res for res, lease in self._leases.items() if now >= lease.expires_at]
        for res in expired:
            lease = self._leases.pop(res)
            self._publish_event("expire", res, lease.holder, lease_id=lease.lease_id)

    def _publish_event(self, event_type: str, resource: str, holder: str, **extra: Any) -> None:
        queues = self._subscribers.get(resource)
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
        async with self._lock:
            self._prune_expired()
            existing = self._leases.get(request.resource)

            if existing is not None and time.time() < existing.expires_at:
                if existing.holder == request.holder:
                    if (
                        request.idempotency_key is not None
                        and existing.idempotency_key == request.idempotency_key
                    ):
                        return existing

                    if (
                        request.trace_id is not None
                        and request.idempotency_key is None
                        and getattr(existing, "trace_id", None) == request.trace_id
                    ):
                        return existing

                    existing.acquisition_count += 1
                    existing.expires_at = time.time() + request.ttl_seconds
                    self._publish_event(
                        "acquire_reentrant",
                        request.resource,
                        request.holder,
                        lease_id=existing.lease_id,
                        count=existing.acquisition_count,
                    )
                    return existing
                else:
                    raise LockConflictError(request.resource, existing.holder)

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
            if request.trace_id:
                setattr(lease, "trace_id", request.trace_id)

            self._leases[request.resource] = lease
            self._publish_event("acquire", request.resource, request.holder, lease_id=lease.lease_id)
            return lease

    async def release(self, request: ReleaseRequest) -> bool:
        async with self._lock:
            self._prune_expired()
            existing = self._leases.get(request.resource)

            if existing is None:
                return False

            if existing.holder != request.holder:
                raise LeaseNotHeldError(
                    f"Cannot release lease for '{request.resource}' held by '{existing.holder}'"
                )

            if existing.lease_id != request.lease_id:
                return False

            if existing.acquisition_count > 1:
                existing.acquisition_count -= 1
                self._publish_event(
                    "release_reentrant",
                    request.resource,
                    request.holder,
                    lease_id=existing.lease_id,
                    remaining_count=existing.acquisition_count,
                )
                return True

            del self._leases[request.resource]
            self._publish_event("release", request.resource, request.holder, lease_id=existing.lease_id)
            return True

    async def renew(self, request: RenewRequest) -> Lease:
        async with self._lock:
            self._prune_expired()
            existing = self._leases.get(request.resource)

            if existing is None or time.time() >= existing.expires_at:
                raise LeaseExpiredError(f"Lease for '{request.resource}' has expired or does not exist")

            if existing.holder != request.holder or existing.lease_id != request.lease_id:
                raise LeaseNotHeldError(f"Lease for '{request.resource}' not held by caller")

            existing.expires_at = time.time() + request.extend_seconds
            existing.ttl_seconds = request.extend_seconds
            self._publish_event(
                "renew",
                request.resource,
                request.holder,
                lease_id=existing.lease_id,
                expires_at=existing.expires_at,
            )
            return existing

    async def get_lease(self, resource: str) -> Optional[Lease]:
        async with self._lock:
            self._prune_expired()
            lease = self._leases.get(resource)
            if lease and time.time() < lease.expires_at:
                return lease
            return None

    async def watch(self, request: WatchRequest) -> AsyncGenerator[dict[str, Any], None]:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        if request.resource not in self._subscribers:
            self._subscribers[request.resource] = set()
        self._subscribers[request.resource].add(q)

        try:
            while True:
                event = await q.get()
                if not request.events or event.get("event") in request.events or event.get("event", "").startswith(tuple(request.events)):
                    yield event
        finally:
            if request.resource in self._subscribers:
                self._subscribers[request.resource].discard(q)
                if not self._subscribers[request.resource]:
                    del self._subscribers[request.resource]
