"""
IPC-Backed Lock Backend for SwarmLock.
Connects to local swarmlockd daemon over Unix domain socket or Tailscale TCP.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, Optional

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
    WatchRequest,
)


class IPCBackend(SwarmlockBackendProtocol):
    """
    Sub-millisecond IPC client connecting to active swarmlockd daemon.
    """

    def __init__(
        self,
        socket_path: Optional[str | Path] = None,
        tcp_host: Optional[str] = None,
        tcp_port: Optional[int] = None,
        timeout: float = 5.0
    ):
        if socket_path is None and tcp_host is None:
            if os.name == "nt":
                self.socket_path = r"\\.\pipe\swarmlock"
            else:
                self.socket_path = "/tmp/swarmlock.sock"
        else:
            self.socket_path = str(socket_path) if socket_path else None

        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.timeout = timeout

    async def _send_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.tcp_host and self.tcp_port:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.tcp_host, self.tcp_port),
                timeout=self.timeout
            )
        elif self.socket_path:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(self.socket_path),
                timeout=self.timeout
            )
        else:
            raise ConnectionError("No IPC socket or TCP host configured")

        try:
            line = json.dumps(payload).encode("utf-8") + b"\n"
            writer.write(line)
            await writer.drain()

            raw_resp = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
            if not raw_resp:
                raise ConnectionError("Empty response from swarmlockd")
            return json.loads(raw_resp.decode("utf-8").strip())
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def acquire(self, request: AcquireRequest) -> Lease:
        payload = {
            "action": "ACQUIRE_WRITE",
            "resource": request.resource,
            "holder": request.holder,
            "ttl": request.ttl_seconds,
            "metadata": request.metadata
        }
        res = await self._send_request(payload)
        status = res.get("status")

        if status == "GRANTED":
            now = time.time()
            return Lease(
                lease_id=res["lock_id"],
                resource=request.resource,
                holder=request.holder,
                expires_at=now + request.ttl_seconds,
                ttl_seconds=request.ttl_seconds,
                metadata={
                    "fence_token": res.get("fence_token"),
                    "version": res.get("version"),
                    **request.metadata
                }
            )
        elif status == "BLOCKED":
            raise LockConflictError(request.resource, res.get("holder", "another_agent"))
        elif status == "STALE_READ_CONFLICT":
            raise LeaseAcquireError(f"STALE_READ_CONFLICT: {res.get('message')}")
        elif status == "DEADLOCK_PREEMPTED":
            raise LeaseAcquireError(f"DEADLOCK_PREEMPTED: {res.get('message')}")
        else:
            raise LeaseAcquireError(res.get("reason") or res.get("error") or "Unknown acquire error")

    async def release(self, request: ReleaseRequest) -> bool:
        payload = {
            "action": "RELEASE",
            "resource": request.resource,
            "holder": request.holder,
            "lock_id": request.lease_id
        }
        res = await self._send_request(payload)
        return res.get("status") == "RELEASED"

    async def renew(self, request: RenewRequest) -> Lease:
        payload = {
            "action": "ACQUIRE_WRITE",
            "resource": request.resource,
            "holder": request.holder,
            "ttl": request.extend_seconds
        }
        res = await self._send_request(payload)
        if res.get("status") == "GRANTED":
            now = time.time()
            return Lease(
                lease_id=res["lock_id"],
                resource=request.resource,
                holder=request.holder,
                expires_at=now + request.extend_seconds,
                ttl_seconds=request.extend_seconds,
                metadata={"fence_token": res.get("fence_token"), "version": res.get("version")}
            )
        raise LeaseExpiredError(f"Failed to renew lease for '{request.resource}'")

    async def get_lease(self, resource: str) -> Optional[Lease]:
        res = await self._send_request({"action": "STATUS"})
        if res.get("status") == "OK":
            for l in res.get("active_locks", []):
                if l["resource"] == resource or l["resource"].endswith(f":{resource}"):
                    now = time.time()
                    return Lease(
                        lease_id=l["lock_id"],
                        resource=l["resource"],
                        holder=l["holder"],
                        expires_at=now + l["remaining_seconds"],
                        ttl_seconds=l["remaining_seconds"],
                        metadata={"fence_token": l["fence_token"], "version": l["version"]}
                    )
        return None

    async def watch(self, request: WatchRequest) -> AsyncGenerator[dict[str, Any], None]:
        last_lease_id = None
        while True:
            lease = await self.get_lease(request.resource)
            current_id = lease.lease_id if lease else None
            if current_id != last_lease_id:
                if lease:
                    yield {
                        "event": "acquire",
                        "resource": request.resource,
                        "holder": lease.holder,
                        "lease_id": lease.lease_id,
                        "timestamp": time.time(),
                    }
                else:
                    yield {
                        "event": "release",
                        "resource": request.resource,
                        "holder": request.holder,
                        "timestamp": time.time(),
                    }
                last_lease_id = current_id
            await asyncio.sleep(0.1)
