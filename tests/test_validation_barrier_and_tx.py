"""
Unit tests for 2PL VALIDATING state machine, Tailscale host resolution, and tx_id grouping.
"""

import asyncio
import json
import os
import tempfile
import pytest
from pathlib import Path

from swarmlock.daemon import SwarmlockDaemon, get_tailscale_ip
from swarmlock.types import LeaseState


@pytest.mark.asyncio
async def test_daemon_2pl_validating_and_tx_lifecycle():
    if os.name == "nt":
        pytest.skip("Unix domain sockets test runs on Linux")

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = Path(tmpdir) / "swarmlock.sock"
        db_path = Path(tmpdir) / "fencing.db"

        daemon = SwarmlockDaemon(socket_path=sock_path, db_path=db_path)
        await daemon.start()

        try:
            reader, writer = await asyncio.open_unix_connection(path=str(sock_path))

            # 1. Acquire with tx_id
            writer.write(json.dumps({
                "action": "ACQUIRE_WRITE",
                "resource": "file:src/billing.py",
                "holder": "agent-saga",
                "tx_id": "tx_refactor_99"
            }).encode() + b"\n")
            await writer.drain()
            acq_resp = json.loads((await reader.readline()).decode())
            assert acq_resp["status"] == "GRANTED"
            assert acq_resp["state"] == LeaseState.ACQUIRED.value
            lid = acq_resp["lock_id"]

            # 2. Transition to VALIDATING (swarmproof barrier)
            writer.write(json.dumps({
                "action": "VALIDATE",
                "lock_id": lid
            }).encode() + b"\n")
            await writer.drain()
            val_resp = json.loads((await reader.readline()).decode())
            assert val_resp["status"] == "VALIDATING"

            # 3. Status inspects state
            writer.write(json.dumps({"action": "STATUS"}).encode() + b"\n")
            await writer.drain()
            status_resp = json.loads((await reader.readline()).decode())
            lock_entry = status_resp["active_locks"][0]
            assert lock_entry["state"] == LeaseState.VALIDATING.value
            assert lock_entry["tx_id"] == "tx_refactor_99"

            # 4. Commit and Release
            writer.write(json.dumps({
                "action": "COMMIT",
                "resource": "file:src/billing.py",
                "holder": "agent-saga",
                "lock_id": lid
            }).encode() + b"\n")
            await writer.drain()
            rel_resp = json.loads((await reader.readline()).decode())
            assert rel_resp["status"] == "RELEASED"

            # 5. Acquire multi-file tx and test RELEASE_ALL_BY_TX
            writer.write(json.dumps({
                "action": "ACQUIRE_WRITE",
                "resource": "file:src/a.py",
                "holder": "agent-saga",
                "tx_id": "tx_batch_1"
            }).encode() + b"\n")
            await writer.drain()
            await reader.readline()

            writer.write(json.dumps({
                "action": "ACQUIRE_WRITE",
                "resource": "file:src/b.py",
                "holder": "agent-saga",
                "tx_id": "tx_batch_1"
            }).encode() + b"\n")
            await writer.drain()
            await reader.readline()

            # Abort/Release whole transaction
            writer.write(json.dumps({
                "action": "RELEASE_ALL_BY_TX",
                "tx_id": "tx_batch_1"
            }).encode() + b"\n")
            await writer.drain()
            tx_resp = json.loads((await reader.readline()).decode())
            assert tx_resp["status"] == "RELEASED"
            assert tx_resp["released_count"] == 2

            writer.close()
            await writer.wait_closed()
        finally:
            await daemon.stop()


def test_tailscale_ip_resolution():
    ip = get_tailscale_ip()
    assert isinstance(ip, str)
    assert len(ip.split(".")) == 4
