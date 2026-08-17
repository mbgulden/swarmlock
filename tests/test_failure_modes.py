"""
Failure mode & resiliency unit tests (GRO-4762 & GRO-4763).
"""

import asyncio
import time
import pytest
from swarmlock import AcquireRequest, ReleaseRequest, RenewRequest, Swarmlock
from swarmlock.client import HeartbeatController
from swarmlock.protocol import SwarmlockBackendProtocol


class FlakyBackend(SwarmlockBackendProtocol):
    """Mock backend that simulates transient errors on renew calls."""

    def __init__(self, fail_count: int = 2) -> None:
        from swarmlock.backends.in_process import InProcessBackend
        self._inner = InProcessBackend()
        self.fail_count = fail_count
        self.attempts = 0

    async def acquire(self, request: AcquireRequest):
        return await self._inner.acquire(request)

    async def release(self, request: ReleaseRequest):
        return await self._inner.release(request)

    async def renew(self, request: RenewRequest):
        self.attempts += 1
        if self.attempts <= self.fail_count:
            raise ConnectionError(f"Simulated transient network blip (attempt {self.attempts})")
        return await self._inner.renew(request)

    async def get_lease(self, resource: str):
        return await self._inner.get_lease(resource)

    async def watch(self, request):
        async for event in self._inner.watch(request):
            yield event


def test_gro_4762_heartbeat_survives_transient_error():
    """
    GRO-4762 P0 Acceptance Test:
    Verify heartbeat loop retries on transient errors and continues renewing lease without lost_event firing.
    """
    async def _test():
        flaky = FlakyBackend(fail_count=2)
        sw = Swarmlock(backend=flaky)

        req = AcquireRequest(resource="res/transient", holder="agent-retry", ttl_seconds=30.0)
        lease = await sw.acquire(req)

        # Use short heartbeat interval for fast testing
        ctrl = HeartbeatController(sw, lease, heartbeat_interval_seconds=0.1)
        await ctrl.start()

        # Wait long enough for 2 failed attempts + 1 successful retry
        await asyncio.sleep(0.8)

        # Verify heartbeat did NOT fire lost_event and successfully renewed after retries
        assert not ctrl.lost_event.is_set()
        assert flaky.attempts >= 3

        await ctrl.stop()
        await sw.release(lease)

    asyncio.run(_test())


def test_gro_4763_idempotent_network_retry_does_not_leak_lease():
    """
    GRO-4763 P1 Acceptance Test:
    Verify retrying acquire with identical idempotency_key returns existing lease
    without incrementing acquisition_count or leaking on single release.
    """
    async def _test():
        sw = Swarmlock(backend="in-process")
        req1 = AcquireRequest(
            resource="res/idempotent",
            holder="agent-retry",
            ttl_seconds=30.0,
            idempotency_key="key-abc-123",
        )
        req2 = AcquireRequest(
            resource="res/idempotent",
            holder="agent-retry",
            ttl_seconds=30.0,
            idempotency_key="key-abc-123",
        )

        lease1 = await sw.acquire(req1)
        assert lease1.acquisition_count == 1

        # Network retry with matching idempotency key
        lease2 = await sw.acquire(req2)
        assert lease2.lease_id == lease1.lease_id
        assert lease2.acquisition_count == 1  # Did NOT increment to 2

        # A single release should completely release the lock
        released = await sw.release(lease2)
        assert released is True

        # Lock is now completely free (not leaked with acquisition_count=1)
        fetched = await sw.get_lease("res/idempotent")
        assert fetched is None

    asyncio.run(_test())
