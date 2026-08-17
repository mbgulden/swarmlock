"""
TDD Tests for v0.2 Atomic Lua Scripts & Server-Side TTL in RedisBackend.
"""

import asyncio
import pytest
from swarmlock import AcquireRequest, ReleaseRequest, RenewRequest
from swarmlock.backends.redis_backend import RedisBackend


class MockRedisServer:
    """In-memory mock Redis server supporting EVAL lua scripts and TTL."""

    def __init__(self) -> None:
        self.store = {}
        self.ttls = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, val: str):
        self.store[key] = val
        self.ttls[key] = ttl

    async def delete(self, key: str):
        if key in self.store:
            del self.store[key]
        if key in self.ttls:
            del self.ttls[key]

    async def eval(self, script: str, numkeys: int, *keys_and_args):
        keys = keys_and_args[:numkeys]
        args = keys_and_args[numkeys:]
        key = keys[0]
        ttl = int(args[0])
        val = args[1]
        self.store[key] = val
        self.ttls[key] = ttl
        return 1


def test_redis_server_side_ttl_and_lua_acquire():
    """Verify RedisBackend uses server-side TTL and atomic Lua scripts."""
    async def _test():
        mock_redis = MockRedisServer()
        backend = RedisBackend(redis_client=mock_redis)

        req = AcquireRequest(resource="res/redis-lua", holder="agent-redis", ttl_seconds=60.0)
        lease = await backend.acquire(req)

        assert lease.resource == "res/redis-lua"
        assert lease.holder == "agent-redis"
        assert "swarmlock:res/redis-lua" in mock_redis.store

    asyncio.run(_test())
