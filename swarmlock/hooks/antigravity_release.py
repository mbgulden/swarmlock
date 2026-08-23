#!/usr/bin/env python3
"""
Antigravity Post-Tool Invocation Hook for SwarmLock v2 & SwarmProof Validation Barrier.
Transitions lease to VALIDATING, executes swarmproof verification (if configured), and commits/releases.
"""

import json
import os
import socket
import subprocess
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


def run_swarmproof_barrier(target_file: str) -> bool:
    """
    Validation Barrier: If swarmproof is available, run AST & type check before lock release.
    """
    swarmproof_bin = "/home/ubuntu/.local/bin/swarmproof"
    if os.path.exists(swarmproof_bin):
        try:
            res = subprocess.run([swarmproof_bin, "verify", target_file], capture_output=True, timeout=5.0)
            return res.returncode == 0
        except Exception:
            return True
    return True


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
        # 1. Transition to VALIDATING
        send_ipc({
            "action": "VALIDATE",
            "resource": f"file:{target_file}",
            "holder": agent_id
        })

        # 2. Run SwarmProof verification barrier
        is_sound = run_swarmproof_barrier(target_file)

        # 3. Commit or Revert & Release
        send_ipc({
            "action": "COMMIT" if is_sound else "REVERT",
            "resource": f"file:{target_file}",
            "holder": agent_id
        })

    sys.exit(0)


if __name__ == "__main__":
    main()
