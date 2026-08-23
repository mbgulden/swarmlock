#!/usr/bin/env python3
"""
Antigravity Pre-Tool Invocation Hook for SwarmLock v2.
Intercepts write_to_file and replace_file_content to enforce atomic locking and MVCC stale-read checks.
"""

import json
import os
import socket
import sys
from pathlib import Path

SOCKET_PATH = "/tmp/swarmlock.sock"
CACHE_FILE = Path.home() / ".swarmlock" / "session_read_versions.json"


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


def get_cached_base_version(target_file: str) -> tuple:
    if not CACHE_FILE.exists():
        return None, None
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            entry = data.get(target_file, {})
            return entry.get("version"), entry.get("content")
    except Exception:
        return None, None


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    tool_name = payload.get("tool_name") or payload.get("name")
    args = payload.get("arguments", {})
    agent_id = os.environ.get("AGENT_ID") or payload.get("conversation_id", "default_agent")
    tx_id = os.environ.get("SWARM_TX_ID")
    trace_id = os.environ.get("SWARM_TRACE_ID")

    target_file = args.get("TargetFile") or args.get("target_file") or args.get("path")
    if not target_file:
        sys.exit(0)

    if tool_name in ["write_to_file", "replace_file_content"]:
        expected_ver, base_content = get_cached_base_version(target_file)
        new_content = args.get("CodeContent") or args.get("ReplacementContent")

        res = send_ipc({
            "action": "ACQUIRE_WRITE",
            "resource": f"file:{target_file}",
            "holder": agent_id,
            "expected_version": expected_ver,
            "base_content": base_content,
            "new_content": new_content,
            "tx_id": tx_id,
            "trace_id": trace_id,
            "ttl": 60.0
        })

        if res.get("status") == "STALE_READ_CONFLICT":
            diff = res.get("diff_delta", "")
            msg = f"⛔ STALE_READ_CONFLICT: {target_file} was modified by another agent (v{res.get('base_version')} -> v{res.get('current_version')}).\nDiff Delta:\n{diff}\nPlease re-read and re-apply changes."
            print(json.dumps({"status": "REJECTED", "message": msg}))
            sys.exit(1)

        elif res.get("status") == "BLOCKED":
            msg = f"🔒 LOCKED: {target_file} is currently held by {res.get('holder')}. Waiting for release..."
            print(json.dumps({"status": "BLOCKED", "message": msg}))
            sys.exit(1)

        elif res.get("status") == "DEADLOCK_PREEMPTED":
            msg = f"⚠️ DEADLOCK_PREEMPTED: Cycle detected for {target_file}. Transaction aborted for retry."
            print(json.dumps({"status": "ABORTED", "message": msg}))
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
