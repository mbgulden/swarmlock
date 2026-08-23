#!/usr/bin/env python3
"""
Antigravity Post-Read Tool Invocation Hook for SwarmLock v2.
Automatically tracks baseline file versions when an agent reads/inspects code.
"""

import json
import os
import socket
import sys
from pathlib import Path

SOCKET_PATH = "/tmp/swarmlock.sock"
CACHE_DIR = Path.home() / ".swarmlock"
CACHE_FILE = CACHE_DIR / "session_read_versions.json"


def send_ipc(req: dict) -> dict:
    if not os.path.exists(SOCKET_PATH):
        return {"status": "NO_DAEMON"}
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(2.0)
            client.connect(SOCKET_PATH)
            client.sendall(json.dumps(req).encode("utf-8") + b"\n")
            line = client.recv(8192)
            if line:
                return json.loads(line.decode("utf-8").strip())
    except Exception as e:
        return {"status": "ERROR", "error": str(e)}
    return {"status": "TIMEOUT"}


def load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache: dict) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)
    os.replace(tmp, CACHE_FILE)


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    tool_name = payload.get("tool_name") or payload.get("name")
    args = payload.get("arguments", {})
    agent_id = os.environ.get("AGENT_ID") or payload.get("conversation_id", "default_agent")

    target_file = args.get("AbsolutePath") or args.get("TargetFile") or args.get("SearchPath") or args.get("path")
    if not target_file or not os.path.isfile(target_file):
        sys.exit(0)

    content = None
    try:
        with open(target_file, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        pass

    # Acquire optimistic read lease to register baseline snapshot
    res = send_ipc({
        "action": "ACQUIRE_READ",
        "resource": f"file:{target_file}",
        "holder": agent_id,
        "content": content,
        "ttl": 300.0
    })

    if res.get("status") == "GRANTED":
        cache = load_cache()
        cache[target_file] = {
            "version": res.get("version", 1),
            "fence_token": res.get("fence_token"),
            "holder": agent_id,
            "content": content
        }
        save_cache(cache)

    sys.exit(0)


if __name__ == "__main__":
    main()
