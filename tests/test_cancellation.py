"""
Cancellation safety unit tests (GRO-4764).
"""

import asyncio
import pytest
from swarmlock import AcquireRequest, Swarmlock


def test_gro_4764_async_lease_context_cancellation_during_aenter():
    """
    GRO-4764 P1 Acceptance Test:
    Verify that cancelling a task inside AsyncLeaseContext.__aenter__ does not leak the lease.
    """
    async def _test():
        sw = Swarmlock(backend="in-process")
        req = AcquireRequest(resource="res/cancel-safety", holder="agent-cancel", ttl_seconds=30.0)

        ctx = sw.lease(req)

        async def enter_task():
            async with ctx:
                await asyncio.sleep(10.0)

        task = asyncio.create_task(enter_task())

        # Wait until lease is acquired
        while True:
            lease = await sw.get_lease("res/cancel-safety")
            if lease is not None:
                break
            await asyncio.sleep(0.01)

        # Cancel task mid-execution
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        # Lease must be cleaned up and released (not leaked)
        fetched_after = await sw.get_lease("res/cancel-safety")
        assert fetched_after is None

    asyncio.run(_test())
