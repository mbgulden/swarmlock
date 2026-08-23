"""
Directed Acyclic Wait-For Graph (DAG) Deadlock Arbiter.
Dynamically detects circular wait dependencies and preempts the lower-priority victim.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class AgentMetadata:
    agent_id: str
    priority: int = 100  # Higher integer = higher priority
    sunk_token_cost: int = 0  # Tokens already consumed
    created_at: float = field(default_factory=time.time)


class DeadlockGraphArbiter:
    """
    In-memory Wait-For Dependency Graph with Cycle Detection & Preemption.
    """

    def __init__(self):
        # Maps agent_id -> resource_key that agent is currently waiting to acquire
        self._waiting: Dict[str, str] = {}
        # Maps resource_key -> agent_id holding the lock
        self._holding: Dict[str, str] = {}
        # Maps agent_id -> AgentMetadata
        self._agents: Dict[str, AgentMetadata] = {}

    def register_agent(
        self,
        agent_id: str,
        priority: int = 100,
        sunk_token_cost: int = 0
    ) -> None:
        self._agents[agent_id] = AgentMetadata(
            agent_id=agent_id,
            priority=priority,
            sunk_token_cost=sunk_token_cost
        )

    def record_grant(self, resource_key: str, holder_id: str) -> None:
        self._holding[resource_key] = holder_id
        if holder_id in self._waiting and self._waiting[holder_id] == resource_key:
            del self._waiting[holder_id]

    def record_release(self, resource_key: str, holder_id: str) -> None:
        if self._holding.get(resource_key) == holder_id:
            del self._holding[resource_key]

    def check_and_record_wait(
        self,
        requester_id: str,
        resource_key: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Records that requester_id is waiting for resource_key.
        Returns: (has_deadlock, victim_agent_id_to_preempt)
        """
        holder_id = self._holding.get(resource_key)
        if not holder_id or holder_id == requester_id:
            return False, None

        self._waiting[requester_id] = resource_key

        # Build Wait-For adjacency: Agent X -> Agent Y (X is waiting on Y)
        adj: Dict[str, List[str]] = {}
        for waiter, res in self._waiting.items():
            holder = self._holding.get(res)
            if holder and holder != waiter:
                adj.setdefault(waiter, []).append(holder)

        # Detect cycle reachable from requester_id
        cycle = self._find_cycle(adj)
        if not cycle:
            return False, None

        # Cycle detected! Pick victim with lowest (priority, sunk_token_cost)
        victim = self._pick_victim(cycle)
        if victim in self._waiting:
            del self._waiting[victim]
        return True, victim

    def _find_cycle(self, adj: Dict[str, List[str]]) -> Optional[List[str]]:
        visited: Set[str] = set()
        stack: List[str] = []

        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            stack.append(node)
            for neighbor in adj.get(node, []):
                if neighbor in stack:
                    # Cycle found: return slice from neighbor to end of stack
                    idx = stack.index(neighbor)
                    return stack[idx:]
                if neighbor not in visited:
                    res = dfs(neighbor)
                    if res:
                        return res
            stack.pop()
            return None

        for node in list(adj.keys()):
            if node not in visited:
                res = dfs(node)
                if res:
                    return res
        return None

    def _pick_victim(self, cycle: List[str]) -> str:
        """
        Picks the agent to abort: lowest priority first, then lowest sunk token cost.
        """
        candidates = []
        for agent_id in cycle:
            meta = self._agents.get(agent_id, AgentMetadata(agent_id=agent_id))
            # Tuple: (priority, sunk_token_cost, age) - lower is weaker victim
            score = (meta.priority, meta.sunk_token_cost)
            candidates.append((score, agent_id))

        candidates.sort()
        return candidates[0][1]
