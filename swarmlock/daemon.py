"""
High-Throughput Async IPC Daemon for SwarmLock (swarmlockd).
Serves Unix Domain Sockets, Windows Named Pipes, and Tailscale TCP.
Coordinates Durable Fencing, Hierarchical MVCC, Deadlock DAG, and Semantic Diffs.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Set

from swarmlock.deadlock import DeadlockGraphArbiter
from swarmlock.diff_engine import SemanticDiffEngine
from swarmlock.fencing import DurableFencingTokenGenerator
from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey


class SwarmlockDaemon:
    def __init__(
        self,
        socket_path: Optional[str | Path] = None,
        tcp_port: Optional[int] = None,
        tcp_host: str = "0.0.0.0",
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
        self.tcp_host = tcp_host

        self.fencing = DurableFencingTokenGenerator(db_path=db_path)
        self.hierarchy = HierarchyLockEngine()
        self.deadlock = DeadlockGraphArbiter()

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

        if action == "PING":
            return {"status": "PONG", "timestamp": time.time()}

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
            return {
                "status": "GRANTED",
                "lock_id": lock_id,
                "fence_token": token,
                "version": ver,
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

            return {
                "status": "GRANTED",
                "lock_id": lock_id,
                "fence_token": token,
                "version": new_version,
                "resource": str(r_key)
            }

        elif action in ["RELEASE", "UNLOCK"]:
            r_key = ResourceKey.parse(resource_str) if resource_str else None
            clean_lid = req.get("lock_id")
            if not clean_lid or clean_lid == "":
                clean_lid = None
            released = self.hierarchy.release_lock(clean_lid, holder, r_key)
            if r_key:
                self.deadlock.record_release(str(r_key), holder)
            return {
                "status": "RELEASED" if released else "NOT_HELD",
                "lock_id": lock_id,
                "resource": str(r_key) if r_key else ""
            }

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
                        "remaining_seconds": max(0.0, l.expires_at - time.time())
                    }
                    for l in active
                ],
                "current_fence_token": self.fencing.current_token
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
