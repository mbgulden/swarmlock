"""
TDD Tests for v0.2 Event Streaming watch() Protocol.
"""

import asyncio
import pytest
from swarmlock import AcquireRequest, ReleaseRequest, Swarmlock, WatchRequest


def test_watch_request_top_level_import():
    """Verify WatchRequest is exported in top-level swarmlock module in v0.2."""
    from swarmlock import WatchRequest
    req = WatchRequest(resource="res/watch", holder="agent-watcher")
    assert req.resource == "res/watch"


def test_watch_event_streaming_in_process():
    """
    Verify watching lock events (acquire, release) on InProcessBackend.
    """
    async def _test():
        sw = Swarmlock(backend="in-process")
        events_received = []

        async def watcher_task():
            watch_req = WatchRequest(resource="res/stream", holder="agent-watcher")
            async for event in sw.watch(watch_req):
                events_received.append(event)
                if event.get("event") == "release":
                    break

        w_task = asyncio.create_task(watcher_task())
        await asyncio.sleep(0.05)

        # Trigger acquire event
        acq_req = AcquireRequest(resource="res/stream", holder="agent-worker", ttl_seconds=10.0)
        lease = await sw.acquire(acq_req)
        await asyncio.sleep(0.05)

        # Trigger release event
        await sw.release(lease)
        await asyncio.wait_for(w_task, timeout=2.0)

        assert len(events_received) >= 2
        assert events_received[0]["event"] == "acquire"
        assert events_received[0]["holder"] == "agent-worker"
        assert events_received[1]["event"] == "release"

    asyncio.run(_test())
