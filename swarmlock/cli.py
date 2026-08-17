"""
Swarmlock Command Line Interface (CLI).
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from swarmlock import AcquireRequest, ReleaseRequest, RenewRequest, Swarmlock, LockConflictError


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="swarmlock",
        description="Centralized distributed file & workspace locking CLI for multi-agent systems",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # lock
    p_lock = sub.add_parser("lock", help="Acquire a resource lock")
    p_lock.add_argument("resource", help="Resource path or identifier")
    p_lock.add_argument("holder", help="Agent identifier claiming the lock")
    p_lock.add_argument("--ttl", type=float, default=60.0, help="Lease TTL in seconds (default: 60)")
    p_lock.add_argument("--backend", default="file", choices=["in-process", "file", "redis"], help="Backend type")

    # unlock
    p_unlock = sub.add_parser("unlock", help="Release a resource lock")
    p_unlock.add_argument("resource", help="Resource path or identifier")
    p_unlock.add_argument("holder", help="Agent identifier releasing the lock")
    p_unlock.add_argument("lease_id", help="Lease ID to release")
    p_unlock.add_argument("--backend", default="file", choices=["in-process", "file", "redis"], help="Backend type")

    # status
    p_status = sub.add_parser("status", help="Get lease status for a resource")
    p_status.add_argument("resource", help="Resource path or identifier")
    p_status.add_argument("--backend", default="file", choices=["in-process", "file", "redis"], help="Backend type")

    args = parser.parse_args()
    sw = Swarmlock(backend=args.backend)

    if args.command == "lock":
        req = AcquireRequest(resource=args.resource, holder=args.holder, ttl_seconds=args.ttl)
        try:
            import asyncio
            lease = asyncio.run(sw.acquire(req))
            print(f"🔒 Locked '{lease.resource}' -> '{lease.holder}' (Lease ID: {lease.lease_id}, TTL: {lease.ttl_seconds}s)")
            sys.exit(0)
        except LockConflictError as err:
            print(f"❌ LOCKED: {err}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error acquiring lock: {e}")
            sys.exit(1)

    elif args.command == "unlock":
        req = ReleaseRequest(lease_id=args.lease_id, resource=args.resource, holder=args.holder)
        try:
            import asyncio
            released = asyncio.run(sw.release(req))
            if released:
                print(f"🔓 Unlocked '{args.resource}' (held by '{args.holder}')")
                sys.exit(0)
            else:
                print(f"⚠️ Lock for '{args.resource}' was not held or already expired.")
                sys.exit(1)
        except Exception as e:
            print(f"❌ Error releasing lock: {e}")
            sys.exit(1)

    elif args.command == "status":
        try:
            import asyncio
            lease = asyncio.run(sw.get_lease(args.resource))
            if lease:
                rem = max(0.0, lease.expires_at - time.time())
                print(f"🔒 Resource '{args.resource}' is LOCKED by '{lease.holder}' (Lease ID: {lease.lease_id}, Remaining TTL: {rem:.1f}s)")
            else:
                print(f"🔓 Resource '{args.resource}' is FREE (no active lease)")
            sys.exit(0)
        except Exception as e:
            print(f"❌ Error fetching status: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
