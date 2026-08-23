"""
Swarmlock Command Line Interface (CLI) v2.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from swarmlock import AcquireRequest, ReleaseRequest, RenewRequest, Swarmlock, LockConflictError
from swarmlock.daemon import SwarmlockDaemon


def send_ipc_command(req: dict, socket_path: str = "/tmp/swarmlock.sock") -> Optional[dict]:
    """Send synchronous IPC command to running swarmlockd daemon if active."""
    if not os.path.exists(socket_path):
        return None
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2.0)
            client.connect(socket_path)
            client.sendall(json.dumps(req).encode("utf-8") + b"\n")
            line = client.recv(8192)
            if line:
                return json.loads(line.decode("utf-8").strip())
    except Exception:
        pass
    return None


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="swarmlock",
        description="Centralized distributed file & workspace locking CLI for multi-agent systems",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # daemon
    p_daemon = sub.add_parser("daemon", help="Run the SwarmLock IPC Daemon (swarmlockd)")
    p_daemon.add_argument("--socket", default=None, help="Unix domain socket path (default: /tmp/swarmlock.sock)")
    p_daemon.add_argument("--tcp-port", type=int, default=None, help="Optional TCP port for remote Tailscale access")
    p_daemon.add_argument("--tcp-host", default="0.0.0.0", help="TCP bind host (default: 0.0.0.0)")
    p_daemon.add_argument("--db-path", default=None, help="Fencing DB path (default: ~/.swarmlock/fencing_tokens.db)")

    # lock
    p_lock = sub.add_parser("lock", help="Acquire a resource lock")
    p_lock.add_argument("resource", help="Resource path or identifier")
    p_lock.add_argument("holder", nargs="?", default="default_agent", help="Agent identifier claiming the lock")
    p_lock.add_argument("--ttl", type=float, default=60.0, help="Lease TTL in seconds (default: 60)")
    p_lock.add_argument("--mode", default="X", choices=["IS", "IX", "S", "X"], help="Lock mode (default: X)")
    p_lock.add_argument("--backend", default="auto", choices=["auto", "ipc", "file", "redis", "in-process"], help="Backend type")

    # unlock
    p_unlock = sub.add_parser("unlock", help="Release a resource lock")
    p_unlock.add_argument("resource", help="Resource path or identifier")
    p_unlock.add_argument("holder", nargs="?", default="default_agent", help="Agent identifier releasing the lock")
    p_unlock.add_argument("lease_id", nargs="?", default=None, help="Optional Lease ID to release")
    p_unlock.add_argument("--backend", default="auto", choices=["auto", "ipc", "file", "redis", "in-process"], help="Backend type")

    # exec
    p_exec = sub.add_parser("exec", help="Run command inside an atomic lock envelope")
    p_exec.add_argument("--resource", required=True, help="Resource path or identifier")
    p_exec.add_argument("--holder", default="exec_agent", help="Agent identifier")
    p_exec.add_argument("--ttl", type=float, default=60.0, help="Lease TTL in seconds")
    p_exec.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to execute")

    # status
    p_status = sub.add_parser("status", help="Get lease status for a resource")
    p_status.add_argument("resource", help="Resource path or identifier")
    p_status.add_argument("--backend", default="auto", choices=["auto", "ipc", "file", "redis", "in-process"], help="Backend type")

    # top
    p_top = sub.add_parser("top", help="Live telemetry dashboard of active locks and queues")
    p_top.add_argument("--socket", default="/tmp/swarmlock.sock", help="Unix socket path")

    args = parser.parse_args()

    if args.command == "daemon":
        print(f"🚀 Starting SwarmLock v2 Daemon on socket={args.socket or '/tmp/swarmlock.sock'} tcp={args.tcp_port}...")
        daemon = SwarmlockDaemon(
            socket_path=args.socket,
            tcp_port=args.tcp_port,
            tcp_host=args.tcp_host,
            db_path=args.db_path
        )
        async def run_d():
            await daemon.start()
            print("🟢 SwarmLock Daemon active and listening.")
            while True:
                await asyncio.sleep(3600)
        try:
            asyncio.run(run_d())
        except (KeyboardInterrupt, SystemExit):
            print("Stopping daemon...")
            asyncio.run(daemon.stop())
            sys.exit(0)

    elif args.command == "top":
        ipc_res = send_ipc_command({"action": "STATUS"}, socket_path=args.socket)
        if not ipc_res:
            print(f"⚠️ SwarmLock daemon is not running on {args.socket}")
            sys.exit(1)
        print("=" * 70)
        print(f" 🛰️ SWARMLOCK v2 TELEMETRY  |  Fencing Token: #{ipc_res.get('current_fence_token')}")
        print("=" * 70)
        locks = ipc_res.get("active_locks", [])
        if not locks:
            print("  (No active leases - all resources free)")
        else:
            print(f"  {'RESOURCE':<28} {'HOLDER':<18} {'MODE':<6} {'TOKEN':<10} {'REMAINING':<8}")
            print("  " + "-" * 66)
            for l in locks:
                print(f"  {l['resource']:<28} {l['holder']:<18} {l['mode']:<6} #{l['fence_token']:<9} {l['remaining_seconds']:.1f}s")
        print("=" * 70)
        sys.exit(0)

    elif args.command == "exec":
        cmd_args = args.cmd
        if cmd_args and cmd_args[0] == "--":
            cmd_args = cmd_args[1:]
        if not cmd_args:
            print("❌ No command specified for swarmlock exec")
            sys.exit(1)

        # 1. Acquire lock
        ipc_res = send_ipc_command({
            "action": "ACQUIRE_WRITE",
            "resource": args.resource,
            "holder": args.holder,
            "ttl": args.ttl
        })
        lock_id = None
        if ipc_res and ipc_res.get("status") == "GRANTED":
            lock_id = ipc_res.get("lock_id")
            token = ipc_res.get("fence_token")
            print(f"🔒 [swarmlock exec] Acquired '{args.resource}' (Fence Token: #{token})")
        else:
            # Fallback to local file backend
            sw = Swarmlock(backend="file")
            lease = asyncio.run(sw.acquire(AcquireRequest(resource=args.resource, holder=args.holder, ttl_seconds=args.ttl)))
            lock_id = lease.lease_id
            print(f"🔒 [swarmlock exec] Acquired '{args.resource}' (Lease ID: {lock_id})")

        # 2. Execute command
        env = os.environ.copy()
        if lock_id:
            env["SWARMLOCK_LEASE_ID"] = str(lock_id)
            env["SWARMLOCK_RESOURCE"] = str(args.resource)
        
        exit_code = 0
        try:
            res = subprocess.run(cmd_args, env=env)
            exit_code = res.returncode
        finally:
            # 3. Always release lock
            if ipc_res:
                send_ipc_command({
                    "action": "RELEASE",
                    "resource": args.resource,
                    "holder": args.holder,
                    "lock_id": lock_id
                })
            else:
                asyncio.run(sw.release(ReleaseRequest(lease_id=lock_id, resource=args.resource, holder=args.holder)))
            print(f"🔓 [swarmlock exec] Released '{args.resource}'")
        sys.exit(exit_code)

    elif args.command == "lock":
        # Check IPC fast path first
        ipc_res = send_ipc_command({
            "action": "ACQUIRE_WRITE" if args.mode == "X" else "ACQUIRE_READ",
            "resource": args.resource,
            "holder": args.holder,
            "ttl": args.ttl
        })
        if ipc_res:
            if ipc_res.get("status") == "GRANTED":
                print(f"🔒 Locked '{args.resource}' -> '{args.holder}' (Fence: #{ipc_res.get('fence_token')}, Version: v{ipc_res.get('version')}, Lock ID: {ipc_res.get('lock_id')})")
                sys.exit(0)
            elif ipc_res.get("status") == "STALE_READ_CONFLICT":
                print(f"⛔ STALE_READ_CONFLICT: {ipc_res.get('message')}")
                if ipc_res.get("diff_delta"):
                    print(ipc_res.get("diff_delta"))
                sys.exit(2)
            else:
                print(f"❌ LOCKED: {ipc_res.get('reason') or ipc_res.get('message')}")
                sys.exit(1)

        # Fallback to standard Swarmlock client
        sw = Swarmlock(backend="file" if args.backend == "auto" else args.backend)
        try:
            lease = asyncio.run(sw.acquire(AcquireRequest(resource=args.resource, holder=args.holder, ttl_seconds=args.ttl)))
            print(f"🔒 Locked '{lease.resource}' -> '{lease.holder}' (Lease ID: {lease.lease_id}, TTL: {lease.ttl_seconds}s)")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)

    elif args.command == "unlock":
        ipc_res = send_ipc_command({
            "action": "RELEASE",
            "resource": args.resource,
            "holder": args.holder,
            "lock_id": args.lease_id or ""
        })
        if ipc_res and ipc_res.get("status") == "RELEASED":
            print(f"🔓 Unlocked '{args.resource}' (held by '{args.holder}')")
            sys.exit(0)

        sw = Swarmlock(backend="file" if args.backend == "auto" else args.backend)
        try:
            released = asyncio.run(sw.release(ReleaseRequest(lease_id=args.lease_id or "", resource=args.resource, holder=args.holder)))
            if released:
                print(f"🔓 Unlocked '{args.resource}' (held by '{args.holder}')")
                sys.exit(0)
            else:
                print(f"⚠️ Lock for '{args.resource}' was not held or already expired.")
                sys.exit(1)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)

    elif args.command == "status":
        ipc_res = send_ipc_command({"action": "STATUS"})
        if ipc_res:
            active = ipc_res.get("active_locks", [])
            match = [l for l in active if l["resource"] == args.resource]
            if match:
                l = match[0]
                print(f"🔒 Resource '{args.resource}' is LOCKED by '{l['holder']}' (Mode: {l['mode']}, Fence: #{l['fence_token']}, Remaining: {l['remaining_seconds']:.1f}s)")
            else:
                print(f"🔓 Resource '{args.resource}' is FREE (no active lease)")
            sys.exit(0)

        sw = Swarmlock(backend="file" if args.backend == "auto" else args.backend)
        try:
            lease = asyncio.run(sw.get_lease(args.resource))
            if lease:
                rem = max(0.0, lease.expires_at - time.time())
                print(f"🔒 Resource '{args.resource}' is LOCKED by '{lease.holder}' (Lease ID: {lease.lease_id}, Remaining TTL: {rem:.1f}s)")
            else:
                print(f"🔓 Resource '{args.resource}' is FREE (no active lease)")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
