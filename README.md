# Swarmlock Primitive (v0.2.0)

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![PEP 561](https://img.shields.io/badge/PEP%20561-typed-success.svg)](https://peps.python.org/pep-0561/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Swarmlock** is a distributed file and resource locking primitive designed for multi-agent swarm coordination in AI workflows (including the **Prismatic Engine** governance layer).

---

## 🌟 Key Features

- **Resilient Background Heartbeats**: Auto-retries transient network glitches (`ConnectionError`, `TimeoutError`, `OSError`) with exponential backoff before declaring lease loss.
- **Idempotent Retry Leaks Prevention**: Native support for `idempotency_key` tracking to prevent lease counter bloat on network retries.
- **Task Cancellation Safety**: `AsyncLeaseContext` automatically cleans up acquired locks if an async task is cancelled mid-execution.
- **Real-Time Event Streaming (`watch()`)**: Stream lock events (`acquire`, `release`, `renew`, `expire`) via async generators across in-process, file-backed, and Redis backends.
- **Server-Side Redis TTL & Atomic Lua Scripts**: Atomic `LUA_ACQUIRE`, `LUA_RELEASE`, `LUA_RENEW` execution avoiding clock-drift vulnerabilities across distributed nodes.
- **Sync & Async Interfaces**: Native support for `async with sw.lease(...)` and `with sync_sw.lease(...)`.
- **CLI Utility**: Terminal tool `swarmlock lock`, `swarmlock unlock`, and `swarmlock status`.

---

## 🚀 Quickstart

### Installation

```bash
pip install swarmlock
# Or with Redis support
pip install swarmlock[redis]
```

### Async Usage (`Swarmlock`)

```python
import asyncio
from swarmlock import Swarmlock, AcquireRequest

async def main():
    sw = Swarmlock(backend="in-process")
    req = AcquireRequest(resource="workspace/src/server.py", holder="agent-fred", ttl_seconds=30.0)

    async with sw.lease(req) as lease:
        print(f"🔒 Acquired lock: {lease.resource} (ID: {lease.lease_id})")
        # Perform safe mutations...

asyncio.run(main())
```

### Sync Usage (`SyncSwarmlock`)

```python
from swarmlock import SyncSwarmlock, AcquireRequest

sw = SyncSwarmlock(backend="file")
req = AcquireRequest(resource="workspace/src/main.py", holder="agent-kai", ttl_seconds=60.0)

with sw.lease(req) as lease:
    print(f"🔒 Holding sync lock for {lease.holder}")
```

### Real-Time Event Streaming (`watch()`)

```python
import asyncio
from swarmlock import Swarmlock, WatchRequest

async def monitor():
    sw = Swarmlock(backend="in-process")
    watch_req = WatchRequest(resource="workspace/src/server.py", holder="monitor-agent")

    async for event in sw.watch(watch_req):
        print(f"📡 Event: {event['event']} on {event['resource']} by {event['holder']}")
```

---

## 🛠️ Command Line Interface (CLI)

```bash
# Claim a file lock
swarmlock lock "workspace/src/server.py" "agent-ned" --ttl 60 --backend file

# Check lock status
swarmlock status "workspace/src/server.py" --backend file

# Release a file lock
swarmlock unlock "workspace/src/server.py" "agent-ned" <LEASE_ID> --backend file
```

---

## 🔬 Testing Suite

Run full unit tests with `pytest`:

```bash
pytest tests/ -v
```

---

## 📜 License

MIT License. Developed for Prismatic Engine & Agentic Swarm Workflows.
