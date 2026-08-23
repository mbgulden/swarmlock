"""
Integration tests for IPCBackend and Swarmlock(backend="ipc") / SyncSwarmlock(backend="ipc").
"""

import asyncio
import os
import tempfile
import threading
import time
import pytest
from pathlib import Path

from swarmlock import AcquireRequest, ReleaseRequest, RenewRequest, Swarmlock, SyncSwarmlock, LockConflictError
from swarmlock.daemon import SwarmlockDaemon


@pytest.mark.asyncio
async def test_swarmlock_ipc_backend_async_lifecycle():
    if os.name == "nt":
        pytest.skip("Unix domain sockets test runs on Linux")

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = Path(tmpdir) / "swarmlock.sock"
        db_path = Path(tmpdir) / "fencing.db"

        daemon = SwarmlockDaemon(socket_path=sock_path, db_path=db_path)
        await daemon.start()

        try:
            client = Swarmlock(backend="ipc", socket_path=sock_path)

            # 1. Acquire
            req = AcquireRequest(resource="file:src/models/user.py", holder="agent-model", ttl_seconds=30.0)
            lease = await client.acquire(req)
            assert lease.holder == "agent-model"
            assert lease.metadata.get("fence_token") is not None
            assert lease.metadata.get("version") == 2

            # 2. Get Lease
            fetched = await client.get_lease("file:src/models/user.py")
            assert fetched is not None
            assert fetched.holder == "agent-model"

            # 3. Conflict
            req2 = AcquireRequest(resource="file:src/models/user.py", holder="agent-other", ttl_seconds=10.0)
            with pytest.raises(LockConflictError):
                await client.acquire(req2)

            # 4. Release
            rel_ok = await client.release(lease)
            assert rel_ok is True

            # 5. Verify freed
            fetched_after = await client.get_lease("file:src/models/user.py")
            assert fetched_after is None
        finally:
            await daemon.stop()


def test_sync_swarmlock_ipc_backend_context_manager():
    if os.name == "nt":
        pytest.skip("Unix domain sockets test runs on Linux")

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = Path(tmpdir) / "swarmlock.sock"
        db_path = Path(tmpdir) / "fencing.db"

        daemon = SwarmlockDaemon(socket_path=sock_path, db_path=db_path)
        loop = asyncio.new_event_loop()
        
        def run_daemon():
            asyncio.set_event_loop(loop)
            loop.run_until_complete(daemon.start())
            loop.run_forever()

        th = threading.Thread(target=run_daemon, daemon=True)
        th.start()
        time.sleep(0.1)

        try:
            sync_client = SyncSwarmlock(backend="ipc", socket_path=sock_path)
            req = AcquireRequest(resource="file:src/routes/auth.py", holder="sync-agent", ttl_seconds=20.0)

            # Test synchronous context manager
            with sync_client.lease(req, heartbeat=False) as lease:
                assert lease.holder == "sync-agent"
                active = sync_client.get_lease("file:src/routes/auth.py")
                assert active is not None
                assert active.holder == "sync-agent"

            # After exit, resource must be released
            freed = sync_client.get_lease("file:src/routes/auth.py")
            assert freed is None
        finally:
            loop.call_soon_threadsafe(loop.stop)
            th.join(timeout=2.0)
