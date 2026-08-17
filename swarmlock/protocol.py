"""
Swarmlock Protocol Interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Optional

from swarmlock.types import AcquireRequest, Lease, ReleaseRequest, RenewRequest, WatchRequest


class SwarmlockBackendProtocol(ABC):
    """Abstract Base Class for Swarmlock backend providers."""

    @abstractmethod
    async def acquire(self, request: AcquireRequest) -> Lease:
        """Acquire a lock lease on a resource."""
        pass

    @abstractmethod
    async def release(self, request: ReleaseRequest) -> bool:
        """Release a held lock lease."""
        pass

    @abstractmethod
    async def renew(self, request: RenewRequest) -> Lease:
        """Renew/extend the TTL of an active lease."""
        pass

    @abstractmethod
    async def get_lease(self, resource: str) -> Optional[Lease]:
        """Fetch current active lease for resource, if any."""
        pass

    @abstractmethod
    async def watch(self, request: WatchRequest) -> AsyncGenerator[dict[str, Any], None]:
        """
        Watch event stream for resource lock state changes.
        """
        pass
