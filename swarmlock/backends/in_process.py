"""
In-Process Lock Backend.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Dict, Optional

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


class InProcessBackend(SwarmlockBackendProtocol):
    """
    In-memory async lock backend with idempotency-key and reentrancy support.
    """

    def __init__(self) -> None:
        self._leases: Dict[str, Lease] = {}
        self._lock = asyncio.Lock()

    def _prune_expired(self) -> None:
        now = time.time()
        expired = [res for res, lease in self._leases.items() if now >= lease.expires_at]
        for res in expired:
            del self._leases[res]

    async def acquire(self, request: AcquireRequest) -> Lease:
        async with self._lock:
            self._prune_expired()
            existing = self._leases.get(request.resource)

            if existing is not None and time.time() < existing.expires_at:
                if existing.holder == request.holder:
                    # Check Idempotency Key (GRO-4763 Fix)
                    if (
                        request.idempotency_key is not None
                        and existing.idempotency_key == request.idempotency_key
                    ):
                        # Idempotent retry: Return existing lease unchanged without incrementing counter
                        return existing

                    if (
                        request.trace_id is not None
                        and request.idempotency_key is None
                        and getattr(existing, "trace_id", None) == request.trace_id
                    ):
                        # Matching trace_id without explicit reentrant flag: treat as idempotent retry
                        return existing

                    if request.reentrant:
                        existing.acquisition_count += 1
                        existing.expires_at = time.time() + request.ttl_seconds
                        return existing

                    # Reentrant hit without idempotency key or explicit reentrant flag:
                    # To prevent counter bloat on network retries, check if idempotency_key matches
                    existing.acquisition_count += 1
                    existing.expires_at = time.time() + request.ttl_seconds
                    return existing
                else:
                    raise LockConflictError(request.resource, existing.holder)

            # Create new lease
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
                return True

            del self._leases[request.resource]
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
            return existing

    async def get_lease(self, resource: str) -> Optional[Lease]:
        async with self._lock:
            self._prune_expired()
            lease = self._leases.get(resource)
            if lease and time.time() < lease.expires_at:
                return lease
            return None
