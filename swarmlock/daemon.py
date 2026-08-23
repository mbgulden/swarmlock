"""
High-Throughput Async IPC Daemon for SwarmLock (swarmlockd) v2.
Serves Unix Domain Sockets, Windows Named Pipes, and Tailscale TCP.
Supports 2-Phase Validation State Machine (VALIDATING, COMMITTED, REVERTED) and tx_id grouping.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from swarmlock.deadlock import DeadlockGraphArbiter
from swarmlock.diff_engine import SemanticDiffEngine
from swarmlock.fencing import DurableFencingTokenGenerator
from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey
from swarmlock.types import LeaseState


def get_tailscale_ip() -> str:
    """Attempt to discover Tailscale IPv4 address (100.x.y.z)."""
    try:
        res = subprocess.run(["tailscale", "ip", "-4"], capture_output=True, text=True, timeout=2.0)
        if res.returncode == 0:
            ip = res.stdout.strip().splitlines()[0].strip()
            if ip.startswith("100."):
                return ip
    except Exception:
        pass
    return "127.0.0.1"


class SwarmlockDaemon:
    def __init__(
        self,
        socket_path: Optional[str | Path] = None,
        tcp_port: Optional[int] = None,
        tcp_host: Optional[str] = None,
        db_path: Optional[str | Path] = None
    ):
        if socket_path is None:
            if os.name == "nt":
                self.socket_path = r"\\.\pipe\swarmlock"
            else:
                self.socket_path = "/tmp/swarmlock.sock"
        else:
            self.socket_path = str(socket_path)

        self.tcp_port = tcp_port
        if tcp_host is None or tcp_host in ["0.0.0.0", "tailscale"]:
            self.tcp_host = get_tailscale_ip()
        else:
            self.tcp_host = tcp_host

        self.fencing = DurableFencingTokenGenerator(db_path=db_path)
        self.hierarchy = HierarchyLockEngine()
        self.deadlock = DeadlockGraphArbiter()

        # Map lock_id -> extra metadata (state, tx_id, trace_id)
        self._lease_states: Dict[str, Dict[str, Any]] = {}

        self._running = False
        self._server: Optional[asyncio.AbstractServer] = None
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._client_tasks: Set[asyncio.Task] = set()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task:
            self._client_tasks.add(task)
        try:
            while self._running:
                line = await reader.readline()
                if not line:
                    break

                try:
                    payload = json.loads(line.decode("utf-8").strip())
                    response = await self.dispatch(payload)
                except Exception as e:
                    response = {"status": "ERROR", "error": str(e)}

                out_bytes = json.dumps(response).encode("utf-8") + b"\n"
                writer.write(out_bytes)
                await writer.drain()
        except asyncio.CancelledError:
            pass
        finally:
            if task:
                self._client_tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def dispatch(self, req: Dict[str, Any]) -> Dict[str, Any]:
        action = req.get("action", "").upper()
        resource_str = req.get("resource", "")
        holder = req.get("holder") or req.get("agent_id", "anonymous")
        ttl = float(req.get("ttl", 60.0))
        lock_id = req.get("lock_id") or str(uuid.uuid4())
        tx_id = req.get("tx_id")
        trace_id = req.get("trace_id")

        if action == "PING":
            return {"status": "PONG", "timestamp": time.time(), "tailscale_host": self.tcp_host}

        if action in ["ACQUIRE_READ", "READ_LEASE"]:
            r_key = ResourceKey.parse(resource_str)
            token = self.fencing.next_token()
            
            snapshot_content = req.get("content")
            if snapshot_content is not None:
                self.hierarchy.record_snapshot(r_key, snapshot_content)

            ok, conflict, ver = self.hierarchy.acquire_lock(
                lock_id=lock_id,
                holder=holder,
                resource=r_key,
                mode=LockMode.IS,
                fence_token=token,
                ttl_seconds=ttl,
                metadata=req.get("metadata")
            )
            if not ok and conflict:
                return {
                    "status": "BLOCKED",
                    "reason": f"Conflict with active lock held by {conflict.holder}",
                    "holder": conflict.holder,
                    "version": ver
                }
            self._lease_states[lock_id] = {
                "state": LeaseState.ACQUIRED.value,
                "tx_id": tx_id,
                "trace_id": trace_id
            }
            return {
                "status": "GRANTED",
                "lock_id": lock_id,
                "fence_token": token,
                "version": ver,
                "state": LeaseState.ACQUIRED.value,
                "resource": str(r_key)
            }

        elif action in ["ACQUIRE_WRITE", "ACQUIRE", "WRITE_LOCK"]:
            r_key = ResourceKey.parse(resource_str)
            token = self.fencing.next_token()

            expected_version = req.get("expected_version") or req.get("base_version")
            current_version = self.hierarchy.get_version(r_key)
            
            if expected_version is not None and expected_version < current_version:
                base_content = req.get("base_content")
                current_content = req.get("current_content") or self.hierarchy.get_snapshot(r_key)
                diff = SemanticDiffEngine.generate_diff(r_key.path, base_content, current_content)
                return {
                    "status": "STALE_READ_CONFLICT",
                    "resource": str(r_key),
                    "base_version": expected_version,
                    "current_version": current_version,
                    "diff_delta": diff,
                    "message": f"Resource {r_key} was modified (v{expected_version} -> v{current_version}). Rebase required."
                }

            has_deadlock, victim = self.deadlock.check_and_record_wait(holder, str(r_key))
            if has_deadlock and victim == holder:
                return {
                    "status": "DEADLOCK_PREEMPTED",
                    "victim": holder,
                    "resource": str(r_key),
                    "message": "Deadlock cycle detected. Transaction preempted."
                }

            ok, conflict, ver = self.hierarchy.acquire_lock(
                lock_id=lock_id,
                holder=holder,
                resource=r_key,
                mode=LockMode.X,
                fence_token=token,
                ttl_seconds=ttl,
                metadata=req.get("metadata")
            )
            if not ok and conflict:
                self.deadlock.check_and_record_wait(holder, str(r_key))
                return {
                    "status": "BLOCKED",
                    "reason": f"Resource {r_key} locked by {conflict.holder}",
                    "holder": conflict.holder,
                    "version": ver
                }

            self.deadlock.record_grant(str(r_key), holder)
            new_version = self.hierarchy.increment_version_and_propagate(r_key)
            if req.get("new_content"):
                self.hierarchy.record_snapshot(r_key, req.get("new_content"))

            self._lease_states[lock_id] = {
                "state": LeaseState.ACQUIRED.value,
                "tx_id": tx_id,
                "trace_id": trace_id,
                "resource": str(r_key),
                "holder": holder
            }

            return {
                "status": "GRANTED",
                "lock_id": lock_id,
                "fence_token": token,
                "version": new_version,
                "state": LeaseState.ACQUIRED.value,
                "resource": str(r_key)
            }

        elif action == "VALIDATE":
            # 2PL State transition: ACQUIRED -> VALIDATING
            if lock_id in self._lease_states:
                self._lease_states[lock_id]["state"] = LeaseState.VALIDATING.value
                return {"status": "VALIDATING", "lock_id": lock_id}
            return {"status": "NOT_FOUND", "lock_id": lock_id}

        elif action in ["RELEASE", "UNLOCK", "COMMIT"]:
            r_key = ResourceKey.parse(resource_str) if resource_str else None
            clean_lid = req.get("lock_id")
            if not clean_lid or clean_lid == "":
                clean_lid = None
            released = self.hierarchy.release_lock(clean_lid, holder, r_key)
            if r_key:
                self.deadlock.record_release(str(r_key), holder)
            if clean_lid and clean_lid in self._lease_states:
                del self._lease_states[clean_lid]
            return {
                "status": "RELEASED" if released else "NOT_HELD",
                "lock_id": lock_id,
                "resource": str(r_key) if r_key else ""
            }

        elif action == "RELEASE_ALL_BY_TX":
            # SwarmSaga abort / commit: release all locks matching tx_id
            target_tx = req.get("tx_id")
            released_count = 0
            for lid, info in list(self._lease_states.items()):
                if info.get("tx_id") == target_tx:
                    res_str = info.get("resource")
                    h = info.get("holder", holder)
                    rk = ResourceKey.parse(res_str) if res_str else None
                    self.hierarchy.release_lock(lid, h, rk)
                    if rk:
                        self.deadlock.record_release(str(rk), h)
                    del self._lease_states[lid]
                    released_count += 1
            return {"status": "RELEASED", "tx_id": target_tx, "released_count": released_count}

        elif action == "STATUS":
            active = self.hierarchy.get_active_locks()
            return {
                "status": "OK",
                "active_locks": [
                    {
                        "lock_id": l.lock_id,
                        "holder": l.holder,
                        "resource": str(l.resource),
                        "mode": l.mode.value,
                        "fence_token": l.fence_token,
                        "version": l.version,
                        "state": self._lease_states.get(l.lock_id, {}).get("state", LeaseState.ACQUIRED.value),
                        "tx_id": self._lease_states.get(l.lock_id, {}).get("tx_id"),
                        "trace_id": self._lease_states.get(l.lock_id, {}).get("trace_id"),
                        "remaining_seconds": max(0.0, l.expires_at - time.time())
                    }
                    for l in active
                ],
                "current_fence_token": self.fencing.current_token,
                "tailscale_ip": self.tcp_host
            }

        return {"status": "UNKNOWN_ACTION", "action": action}

    async def start(self) -> None:
        self._running = True
        if os.name != "nt" or not str(self.socket_path).startswith(r"\\"):
            if os.path.exists(self.socket_path):
                os.remove(self.socket_path)
            self._server = await asyncio.start_unix_server(self.handle_client, path=self.socket_path)
            os.chmod(self.socket_path, 0o777)

        if self.tcp_port is not None:
            self._tcp_server = await asyncio.start_server(self.handle_client, host=self.tcp_host, port=self.tcp_port)

    async def stop(self) -> None:
        self._running = False
        for t in list(self._client_tasks):
            t.cancel()
        if self._server:
            self._server.close()
            try:
                await self._server.wait_closed()
            except Exception:
                pass
            if os.path.exists(self.socket_path):
                try:
                    os.remove(self.socket_path)
                except Exception:
                    pass
        if self._tcp_server:
            self._tcp_server.close()
            try:
                await self._tcp_server.wait_closed()
            except Exception:
                pass
