#!/usr/bin/env python3
"""
Unified Antigravity Post-Tool Invocation Hook for Swarm Suite.
Chains SwarmLock (2PL) -> SwarmProof (Verification) -> SwarmGate (Attention Governor).
"""

import json
import os
import subprocess
import sys

SOCKET_PATH = "/tmp/swarmlock.sock"


def main():
    try:
        payload = json.loads(sys.stdin.read())
    except Exception:
        sys.exit(0)

    tool_name = payload.get("tool_name") or payload.get("name")
    args = payload.get("arguments", {})
    agent_id = os.environ.get("AGENT_ID") or payload.get("conversation_id", "default_agent")
    tx_id = os.environ.get("SWARM_TX_ID")

    target_file = args.get("TargetFile") or args.get("target_file") or args.get("path")
    if not target_file:
        sys.exit(0)

    if tool_name in ["write_to_file", "replace_file_content"]:
        # 1. Execute SwarmProof Multi-Oracle Verification
        swarmproof_bin = "/home/ubuntu/.local/bin/swarmproof"
        proof_id = None

        if os.path.exists(swarmproof_bin):
            try:
                res = subprocess.run(
                    [swarmproof_bin, "check", target_file, "--json"],
                    capture_output=True,
                    text=True,
                    timeout=10.0
                )
                if res.returncode != 0:
                    # Verification failed: swarmproof already signaled REVERT
                    print(res.stdout or res.stderr)
                    sys.exit(1)
                data = json.loads(res.stdout.strip())
                proof_id = data.get("proof", {}).get("proof_id")
            except Exception:
                pass

        # 2. Evaluate Escalation Score with SwarmGate
        swarmgate_bin = "/home/ubuntu/.local/bin/swarmgate"
        if os.path.exists(swarmgate_bin):
            try:
                eval_cmd = [swarmgate_bin, "evaluate", target_file, "--agent", agent_id, "--json"]
                if proof_id:
                    eval_cmd.extend(["--proof", proof_id])
                if tx_id:
                    eval_cmd.extend(["--tx-id", tx_id])

                res_gate = subprocess.run(eval_cmd, capture_output=True, text=True, timeout=10.0)
                gate_data = json.loads(res_gate.stdout.strip())
                tier = gate_data.get("tier")

                if tier == "TIER_3_BARRIER":
                    msg = f"🛑 SWARMGATE TIER 3 BARRIER: High-risk mutation on {target_file} suspended for operator approval.\nReview at http://100.83.32.92:8999 or run 'swarmgate review'."
                    print(json.dumps({"status": "SUSPENDED", "message": msg}))
                    sys.exit(2)
            except Exception:
                pass

    sys.exit(0)


if __name__ == "__main__":
    main()