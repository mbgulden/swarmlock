"""
Unit tests for core Swarmlock client & context managers.
"""

import asyncio
import pytest
from swarmlock import (
    AcquireRequest,
    Lease,
    LockConflictError,
    ReleaseRequest,
    RenewRequest,
    Swarmlock,
)


def test_acquire_and_release_in_process():
    async def _test():
        sw = Swarmlock(backend="in-process")
        req = AcquireRequest(resource="workspace/1", holder="agent-a", ttl_seconds=10.0)

        lease = await sw.acquire(req)
        assert lease.resource == "workspace/1"
        assert lease.holder == "agent-a"
        assert lease.acquisition_count == 1
        assert not lease.is_expired

        released = await sw.release(lease)
        assert released is True

        fetched = await sw.get_lease("workspace/1")
        assert fetched is None

    asyncio.run(_test())


def test_lock_conflict():
    async def _test():
        sw = Swarmlock(backend="in-process")
        req1 = AcquireRequest(resource="workspace/1", holder="agent-a", ttl_seconds=10.0)
        req2 = AcquireRequest(resource="workspace/1", holder="agent-b", ttl_seconds=10.0)

        await sw.acquire(req1)

        with pytest.raises(LockConflictError) as exc_info:
            await sw.acquire(req2)

        assert exc_info.value.resource == "workspace/1"
        assert exc_info.value.current_holder == "agent-a"

    asyncio.run(_test())


def test_async_lease_context():
    async def _test():
        sw = Swarmlock(backend="in-process")
        req = AcquireRequest(resource="workspace/2", holder="agent-a", ttl_seconds=5.0)

        async with sw.lease(req) as lease:
            assert lease.resource == "workspace/2"
            fetched = await sw.get_lease("workspace/2")
            assert fetched is not None
            assert fetched.holder == "agent-a"

        fetched_after = await sw.get_lease("workspace/2")
        assert fetched_after is None

    asyncio.run(_test())


def test_renew_lease():
    async def _test():
        sw = Swarmlock(backend="in-process")
        req = AcquireRequest(resource="workspace/3", holder="agent-a", ttl_seconds=2.0)

        lease = await sw.acquire(req)
        original_exp = lease.expires_at

        renew_req = RenewRequest(
            lease_id=lease.lease_id,
            resource="workspace/3",
            holder="agent-a",
            extend_seconds=20.0,
        )
        renewed = await sw.renew(renew_req)

        assert renewed.expires_at > original_exp
        await sw.release(renewed)

    asyncio.run(_test())
