"""
Unit tests for SyncSwarmlock wrapper and CLI entrypoint.
"""

import subprocess
import pytest
from swarmlock import AcquireRequest, SyncSwarmlock, Swarmlock


def test_sync_swarmlock_acquire_and_release():
    sw = SyncSwarmlock(backend="in-process")
    req = AcquireRequest(resource="workspace/sync1", holder="agent-sync", ttl_seconds=10.0)

    lease = sw.acquire(req)
    assert lease.resource == "workspace/sync1"
    assert lease.holder == "agent-sync"

    fetched = sw.get_lease("workspace/sync1")
    assert fetched is not None
    assert fetched.holder == "agent-sync"

    released = sw.release(lease)
    assert released is True

    fetched_after = sw.get_lease("workspace/sync1")
    assert fetched_after is None


def test_sync_lease_context_manager():
    sw = SyncSwarmlock(backend="in-process")
    req = AcquireRequest(resource="workspace/sync2", holder="agent-sync", ttl_seconds=10.0)

    with sw.lease(req) as lease:
        assert lease.resource == "workspace/sync2"
        fetched = sw.get_lease("workspace/sync2")
        assert fetched is not None

    fetched_after = sw.get_lease("workspace/sync2")
    assert fetched_after is None
