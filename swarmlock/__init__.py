"""
Swarmlock — Distributed lock primitive for multi-agent swarm coordination.
"""

from swarmlock.client import Swarmlock, SyncSwarmlock, SyncLeaseContext
from swarmlock.types import (
    AcquireRequest,
    Lease,
    LeaseAcquireError,
    LeaseExpiredError,
    LeaseNotHeldError,
    LockConflictError,
    ReleaseRequest,
    RenewRequest,
    SwarmlockError,
    WatchRequest,
)

__all__ = [
    "Swarmlock",
    "SyncSwarmlock",
    "SyncLeaseContext",
    "AcquireRequest",
    "Lease",
    "ReleaseRequest",
    "RenewRequest",
    "WatchRequest",
    "SwarmlockError",
    "LockConflictError",
    "LeaseAcquireError",
    "LeaseExpiredError",
    "LeaseNotHeldError",
]
