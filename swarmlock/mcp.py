"""
Universal MCP (Model Context Protocol) Server for Prismatic Agent Hypervisor.
Exposes SwarmLock, SwarmProof, SwarmGate, SwarmSaga, and SwarmLedger to any MCP-compliant harness
(Claude Code, Cursor IDE, Hermes Agent, Windsurf, OpenCodeInterpreter).
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from swarmgate.bridge import SwarmgateBridge
from swarmgate.evaluator import EscalationEvaluator
from swarmledger.core.node import EventType
from swarmledger.storage.auditor import CryptographicAuditor
from swarmledger.storage.engine import StorageEngine
from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey
from swarmproof.bridge import SwarmproofBridge
from swarmsaga.journal.engine import JournalEngine


class PrismaticMCPServer:
    def __init__(self):
        self.lock_engine = HierarchyLockEngine()
        self.gate_eval = EscalationEvaluator()
        self.journal = JournalEngine()
        self.ledger_engine = StorageEngine()
        self.auditor = CryptographicAuditor(self.ledger_engine)

    def handle_request(self, req: Dict[str, Any]) -> Dict[str, Any]:
        method = req.get("method")
        params = req.get("params", {})
        req_id = req.get("id")

        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "tools": [
                        {
                            "name": "swarmlock_acquire",
                            "description": "Acquire concurrency lease with MVCC and monotonic fencing token.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "resource": {"type": "string"},
                                    "mode": {"type": "string", "enum": ["IS", "IX", "S", "X"]},
                                    "holder": {"type": "string"}
                                },
                                "required": ["resource", "mode"]
                            }
                        },
                        {
                            "name": "swarmproof_verify",
                            "description": "Deterministic AST, syntax, type, and test invariant verification oracle.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "target_path": {"type": "string"},
                                    "run_tests": {"type": "boolean"}
                                },
                                "required": ["target_path"]
                            }
                        },
                        {
                            "name": "swarmgate_evaluate",
                            "description": "Attention governor evaluating risk and blast radius (Tier 1/2/3).",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "resource": {"type": "string"},
                                    "lines_changed": {"type": "integer"},
                                    "dependents_count": {"type": "integer"}
                                },
                                "required": ["resource"]
                            }
                        },
                        {
                            "name": "swarmsaga_begin",
                            "description": "Begin a distributed transaction with durable WAL state journaling.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "tx_id": {"type": "string"},
                                    "description": {"type": "string"}
                                },
                                "required": ["tx_id"]
                            }
                        },
                        {
                            "name": "swarmledger_audit",
                            "description": "Cryptographically audit Merkle DAG span for tamper evidence & depth.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "span_id": {"type": "string"}
                                },
                                "required": ["span_id"]
                            }
                        }
                    ]
                }
            }

        elif method == "tools/call":
            name = params.get("name")
            args = params.get("arguments", {})
            result = self._dispatch_tool(name, args)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, indent=2)}]
                }
            }

        elif method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {"name": "prismatic-hypervisor-mcp", "version": "1.0.0"},
                    "capabilities": {"tools": {}}
                }
            }

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"Method {method} not found"}
        }

    def _dispatch_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "swarmlock_acquire":
            res_str = args["resource"]
            mode_str = args["mode"]
            mode = getattr(LockMode, mode_str, LockMode.X)
            holder = args.get("holder", "mcp_agent")
            lock_id = f"lck_{uuid.uuid4().hex[:8]}"
            fence_token = time.time_ns()
            res_key = ResourceKey.parse(res_str)
            
            granted, conflict_lock, version = self.lock_engine.acquire_lock(
                lock_id=lock_id,
                holder=holder,
                resource=res_key,
                mode=mode,
                fence_token=fence_token,
                ttl_seconds=60.0
            )
            if granted:
                return {
                    "status": "GRANTED",
                    "lease_id": lock_id,
                    "fence_token": fence_token,
                    "version": version
                }
            return {
                "status": "CONFLICT",
                "resource": res_str,
                "held_by": conflict_lock.holder if conflict_lock else "unknown"
            }

        elif name == "swarmproof_verify":
            p = Path(args["target_path"])
            passed, cert, diags = SwarmproofBridge.verify_and_settle(p, run_tests=args.get("run_tests", False))
            return {
                "passed": passed,
                "proof_id": cert.proof_id if cert else None,
                "diagnostics": [f"{d.error_type}: {d.message}" for d in diags]
            }

        elif name == "swarmgate_evaluate":
            dec = self.gate_eval.evaluate(
                resource=args["resource"],
                lines_changed=args.get("lines_changed", 1),
                dependents_count=args.get("dependents_count", 0)
            )
            return {
                "decision_id": dec.decision_id,
                "tier": dec.tier.value,
                "escalation_score": dec.escalation_score,
                "requires_human_review": dec.tier.value == "TIER_3_BARRIER"
            }

        elif name == "swarmsaga_begin":
            tx_id = args["tx_id"]
            desc = args.get("description", "")
            self.journal.begin_saga(tx_id, desc)
            return {"status": "ACTIVE", "tx_id": tx_id}

        elif name == "swarmledger_audit":
            report = self.auditor.verify_span(args["span_id"])
            return {
                "passed": report.passed,
                "verified_nodes": report.verified_nodes,
                "violations": [{"node_id": v.node_id, "error": v.error_type} for v in report.violations]
            }

        return {"error": f"Unknown tool {name}"}

    def run_stdio(self):
        """Runs standard MCP stdio loop."""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
                res = self.handle_request(req)
                sys.stdout.write(json.dumps(res) + "\n")
                sys.stdout.flush()
            except Exception as exc:
                err_resp = {"jsonrpc": "2.0", "error": {"code": -32700, "message": str(exc)}}
                sys.stdout.write(json.dumps(err_resp) + "\n")
                sys.stdout.flush()


def main():
    server = PrismaticMCPServer()
    server.run_stdio()


if __name__ == "__main__":
    main()