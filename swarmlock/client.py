"""
Swarmlock Client & Context Managers.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable, Optional, Union

from swarmlock.backends.file import FileBackend
from swarmlock.backends.in_process import InProcessBackend
from swarmlock.backends.redis_backend import RedisBackend
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
)

logger = logging.getLogger("swarmlock.client")


def is_transient_error(exc: Exception) -> bool:
    """Determine if an exception represents a transient network/IO blip."""
    if isinstance(exc, (ConnectionError, TimeoutError, asyncio.TimeoutError, OSError)):
        return True
    # Allow string matching for redis/network transient messages
    msg = str(exc).lower()
    return "timeout" in msg or "connection reset" in msg or "network" in msg or "temporarily unavailable" in msg


class HeartbeatController:
    """
    Manages background heartbeat renewals with transient failure resiliency (GRO-4762).
    """

    def __init__(
        self,
        client: Swarmlock,
        lease: Lease,
        heartbeat_interval_seconds: Optional[float] = None,
        is_transient_predicate: Callable[[Exception], bool] = is_transient_error,
    ) -> None:
        self.client = client
        self.lease = lease
        self.interval = heartbeat_interval_seconds or max(1.0, lease.ttl_seconds / 3.0)
        self.is_transient = is_transient_predicate
        self.lost_event = asyncio.Event()
        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self.running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        """
        Background heartbeat loop (GRO-4762 Fix).
        Retries transient errors with exponential backoff until lease expiration.
        """
        attempts = 0
        while self.running:
            try:
                await asyncio.sleep(self.interval)
                if not self.running:
                    break

                # Attempt renewal
                renew_req = RenewRequest(
                    lease_id=self.lease.lease_id,
                    resource=self.lease.resource,
                    holder=self.lease.holder,
                    extend_seconds=self.lease.ttl_seconds,
                )
                updated_lease = await self.client.renew(renew_req)
                self.lease = updated_lease
                attempts = 0  # Reset on successful renewal
                logger.debug(f"Heartbeat renewed lease '{self.lease.resource}'")

            except Exception as e:
                if not self.running:
                    break

                if self.is_transient(e):
                    attempts += 1
                    backoff = min(0.1 * (2 ** (attempts - 1)), 5.0)
                    now = time.time()
                    logger.warning(
                        f"Transient error renewing lease '{self.lease.resource}' (attempt {attempts}): {e}. "
                        f"Retrying in {backoff:.2f}s..."
                    )

                    if now + backoff < self.lease.expires_at:
                        await asyncio.sleep(backoff)
                        continue
                    else:
                        logger.error(
                            f"Lease '{self.lease.resource}' retry delay ({backoff:.2f}s) exceeds expiration time. Declaring lost."
                        )
                        self.lost_event.set()
                        self.running = False
                        break
                else:
                    logger.error(f"Non-transient error renewing lease '{self.lease.resource}': {e}. Heartbeat exiting.")
                    self.lost_event.set()
                    self.running = False
                    break


class AsyncLeaseContext:
    """
    Async Context Manager for acquiring and automatically releasing a lease (GRO-4764).
    """

    def __init__(self, client: Swarmlock, request: AcquireRequest, heartbeat: bool = True) -> None:
        self.client = client
        self.request = request
        self.enable_heartbeat = heartbeat
        self.lease: Optional[Lease] = None
        self.heartbeat_controller: Optional[HeartbeatController] = None

    async def __aenter__(self) -> Lease:
        """
        Acquire lease with cancellation safety (GRO-4764 Fix).
        """
        acquired_lease: Optional[Lease] = None
        try:
            acquired_lease = await self.client.acquire(self.request)
            self.lease = acquired_lease

            if self.enable_heartbeat:
                self.heartbeat_controller = HeartbeatController(self.client, acquired_lease)
                await self.heartbeat_controller.start()

            return acquired_lease

        except asyncio.CancelledError:
            # GRO-4764: If task cancelled after acquire() returned, release lease before propagating
            if acquired_lease is not None:
                try:
                    await self.client.release(
                        ReleaseRequest(
                            lease_id=acquired_lease.lease_id,
                            resource=acquired_lease.resource,
                            holder=acquired_lease.holder,
                        )
                    )
                except Exception as rel_err:
                    logger.error(f"Error releasing lease during cancellation cleanup: {rel_err}")
            raise

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.heartbeat_controller:
            await self.heartbeat_controller.stop()

        if self.lease:
            try:
                await self.client.release(
                    ReleaseRequest(
                        lease_id=self.lease.lease_id,
                        resource=self.lease.resource,
                        holder=self.lease.holder,
                    )
                )
            except Exception as e:
                logger.warning(f"Error releasing lease on context exit: {e}")


class Swarmlock:
    """
    Main Swarmlock Client API.
    """

    def __init__(
        self,
        backend: Union[str, SwarmlockBackendProtocol] = "in-process",
        **backend_kwargs: Any,
    ) -> None:
        if isinstance(backend, SwarmlockBackendProtocol):
            self.backend = backend
        elif backend == "in-process" or backend == "in_process":
            self.backend = InProcessBackend()
        elif backend == "file":
            self.backend = FileBackend(**backend_kwargs)
        elif backend == "redis":
            self.backend = RedisBackend(**backend_kwargs)
        else:
            raise ValueError(f"Unknown backend type: '{backend}'")

    async def acquire(self, request: AcquireRequest) -> Lease:
        return await self.backend.acquire(request)

    async def release(self, request: Union[ReleaseRequest, Lease]) -> bool:
        if isinstance(request, Lease):
            request = ReleaseRequest(
                lease_id=request.lease_id,
                resource=request.resource,
                holder=request.holder,
            )
        return await self.backend.release(request)

    async def renew(self, request: RenewRequest) -> Lease:
        return await self.backend.renew(request)

    async def get_lease(self, resource: str) -> Optional[Lease]:
        return await self.backend.get_lease(resource)

    async def watch(self, request: WatchRequest) -> Any:
        """Stream real-time lock events for a resource."""
        async for event in self.backend.watch(request):
            yield event

    def lease(self, request: AcquireRequest, heartbeat: bool = True) -> AsyncLeaseContext:
        """Return an AsyncLeaseContext for use in 'async with' statements."""
        return AsyncLeaseContext(self, request, heartbeat=heartbeat)


class SyncLeaseContext:
    """
    Synchronous Context Manager wrapper around AsyncLeaseContext for use in non-async code ('with').
    """

    def __init__(self, sync_client: SyncSwarmlock, request: AcquireRequest, heartbeat: bool = True) -> None:
        self.sync_client = sync_client
        self.request = request
        self.heartbeat = heartbeat
        self.async_ctx = self.sync_client.async_client.lease(request, heartbeat=heartbeat)

    def __enter__(self) -> Lease:
        return self.sync_client.run_sync(self.async_ctx.__aenter__())

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.sync_client.run_sync(self.async_ctx.__aexit__(exc_type, exc_val, exc_tb))


class SyncSwarmlock:
    """
    Synchronous API client wrapper for Swarmlock.
    Allows usage in synchronous threads/scripts without manual event loop management.
    """

    def __init__(
        self,
        backend: Union[str, SwarmlockBackendProtocol] = "in-process",
        **backend_kwargs: Any,
    ) -> None:
        self.async_client = Swarmlock(backend=backend, **backend_kwargs)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def run_sync(self, coro: Any) -> Any:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # If running inside an existing loop, execute via new thread or task runner
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(lambda: asyncio.run(coro)).result()
        else:
            return asyncio.run(coro)

    def acquire(self, request: AcquireRequest) -> Lease:
        return self.run_sync(self.async_client.acquire(request))

    def release(self, request: Union[ReleaseRequest, Lease]) -> bool:
        return self.run_sync(self.async_client.release(request))

    def renew(self, request: RenewRequest) -> Lease:
        return self.run_sync(self.async_client.renew(request))

    def get_lease(self, resource: str) -> Optional[Lease]:
        return self.run_sync(self.async_client.get_lease(resource))

    def lease(self, request: AcquireRequest, heartbeat: bool = True) -> SyncLeaseContext:
        """Return a SyncLeaseContext for use in synchronous 'with' statements."""
        return SyncLeaseContext(self, request, heartbeat=heartbeat)

