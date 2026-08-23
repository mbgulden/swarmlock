#!/usr/bin/env python3
"""
Antigravity Post-Tool Invocation Hook for SwarmLock v2.
Releases held locks after write_to_file / replace_file_content completes.
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
    except Exception:
        pass
    return {"status": "ERROR"}


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
        send_ipc({
            "action": "RELEASE",
            "resource": f"file:{target_file}",
            "holder": agent_id
        })

    sys.exit(0)


if __name__ == "__main__":
    main()
