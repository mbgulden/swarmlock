"""
Swarmlock Types & Data Models.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class SwarmlockError(Exception):
    """Base exception for all Swarmlock errors."""
    pass


class LeaseAcquireError(SwarmlockError):
    """Raised when a lease cannot be acquired."""
    pass


class LockConflictError(LeaseAcquireError):
    """Raised when a resource is held by another agent."""
    def __init__(self, resource: str, current_holder: str):
        super().__init__(f"Resource '{resource}' is currently held by '{current_holder}'")
        self.resource = resource
        self.current_holder = current_holder


class LeaseExpiredError(SwarmlockError):
    """Raised when attempting an operation on an expired lease."""
    pass


class LeaseNotHeldError(SwarmlockError):
    """Raised when releasing or renewing a lease not held by caller."""
    pass


@dataclass
class AcquireRequest:
    """Request payload to acquire a resource lock."""
    resource: str
    holder: str
    ttl_seconds: float = 60.0
    trace_id: Optional[str] = None
    idempotency_key: Optional[str] = None
    reentrant: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Lease:
    """Active lock lease handle."""
    lease_id: str
    resource: str
    holder: str
    expires_at: float  # Unix timestamp in seconds
    ttl_seconds: float
    acquisition_count: int = 1
    idempotency_key: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self.expires_at - time.time())


@dataclass
class ReleaseRequest:
    """Request payload to release a lease."""
    lease_id: str
    resource: str
    holder: str


@dataclass
class RenewRequest:
    """Request payload to extend lease TTL."""
    lease_id: str
    resource: str
    holder: str
    extend_seconds: float = 60.0


@dataclass
class WatchRequest:
    """
    Request payload for resource watch stream.
    Note: Watch streaming is on the roadmap for v0.2.
    """
    resource: str
    holder: str
    events: List[str] = field(default_factory=lambda: ["acquire", "release", "expire"])
