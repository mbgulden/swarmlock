"""
End-to-End integration tests for SwarmlockDaemon over async IPC.
"""

import asyncio
import json
import os
import tempfile
import pytest
from pathlib import Path
from swarmlock.daemon import SwarmlockDaemon


@pytest.mark.asyncio
async def test_daemon_wire_protocol_e2e():
    if os.name == "nt":
        pytest.skip("Unix domain sockets test runs on Linux")

    with tempfile.TemporaryDirectory() as tmpdir:
        sock_path = Path(tmpdir) / "swarmlock.sock"
        db_path = Path(tmpdir) / "fencing.db"

        daemon = SwarmlockDaemon(socket_path=sock_path, db_path=db_path)
        await daemon.start()

        try:
            reader, writer = await asyncio.open_unix_connection(path=str(sock_path))

            # 1. PING test
            writer.write(json.dumps({"action": "PING"}).encode() + b"\n")
            await writer.drain()
            resp = json.loads((await reader.readline()).decode())
            assert resp["status"] == "PONG"

            # 2. ACQUIRE_READ test
            writer.write(json.dumps({
                "action": "ACQUIRE_READ",
                "resource": "file:src/auth/jwt.py",
                "holder": "agent1",
                "content": "def login():\n    return False\n"
            }).encode() + b"\n")
            await writer.drain()
            read_resp = json.loads((await reader.readline()).decode())
            assert read_resp["status"] == "GRANTED"
            assert read_resp["version"] == 1
            fence1 = read_resp["fence_token"]

            # 3. Agent 2 updates the file -> increments version to 2
            writer.write(json.dumps({
                "action": "ACQUIRE_WRITE",
                "resource": "file:src/auth/jwt.py",
                "holder": "agent2",
                "expected_version": 1,
                "new_content": "def login():\n    check_mfa()\n    return True\n"
            }).encode() + b"\n")
            await writer.drain()
            write_resp2 = json.loads((await reader.readline()).decode())
            assert write_resp2["status"] == "GRANTED"
            assert write_resp2["version"] == 2

            # Agent 2 releases
            writer.write(json.dumps({
                "action": "RELEASE",
                "resource": "file:src/auth/jwt.py",
                "holder": "agent2",
                "lock_id": write_resp2["lock_id"]
            }).encode() + b"\n")
            await writer.drain()
            rel_resp = json.loads((await reader.readline()).decode())
            assert rel_resp["status"] == "RELEASED"

            # 4. Agent 1 tries to write with STALE version 1 -> MUST TRIGGER STALE_READ_CONFLICT with DIFF
            writer.write(json.dumps({
                "action": "ACQUIRE_WRITE",
                "resource": "file:src/auth/jwt.py",
                "holder": "agent1",
                "expected_version": 1,
                "base_content": "def login():\n    return False\n"
            }).encode() + b"\n")
            await writer.drain()
            stale_resp = json.loads((await reader.readline()).decode())
            assert stale_resp["status"] == "STALE_READ_CONFLICT"
            assert stale_resp["base_version"] == 1
            assert stale_resp["current_version"] == 2
            assert "+    check_mfa()" in stale_resp["diff_delta"]

            writer.close()
            await writer.wait_closed()
        finally:
            await daemon.stop()
