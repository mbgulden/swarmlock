#!/usr/bin/env python3
"""
Antigravity Pre-Tool Invocation Hook for SwarmLock v2.
Intercepts write_to_file and replace_file_content to enforce atomic locking and MVCC stale-read checks.
"""

import json
import os
import socket
import sys

SOCKET_PATH = "/tmp/swarmlock.sock"


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


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    tool_name = payload.get("tool_name") or payload.get("name")
    args = payload.get("arguments", {})
    agent_id = os.environ.get("AGENT_ID") or payload.get("conversation_id", "default_agent")

    target_file = args.get("TargetFile") or args.get("target_file") or args.get("path")
    if not target_file:
        sys.exit(0)

    if tool_name in ["write_to_file", "replace_file_content"]:
        res = send_ipc({
            "action": "ACQUIRE_WRITE",
            "resource": f"file:{target_file}",
            "holder": agent_id,
            "ttl": 60.0
        })

        if res.get("status") == "STALE_READ_CONFLICT":
            diff = res.get("diff_delta", "")
            msg = f"⛔ STALE_READ_CONFLICT: {target_file} was modified by another agent.\nDiff Delta:\n{diff}\nPlease re-read and re-apply changes."
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
