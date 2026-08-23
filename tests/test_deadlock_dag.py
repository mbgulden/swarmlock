"""
Unit tests for DeadlockGraphArbiter and dynamic cycle preemption.
"""

import pytest
from swarmlock.deadlock import DeadlockGraphArbiter


def test_no_deadlock_linear_wait():
    arbiter = DeadlockGraphArbiter()
    arbiter.register_agent("agent1", priority=100)
    arbiter.register_agent("agent2", priority=100)

    # Agent 1 holds Res A
    arbiter.record_grant("file:A", "agent1")

    # Agent 2 waits for Res A (Linear dependency, no cycle)
    has_deadlock, victim = arbiter.check_and_record_wait("agent2", "file:A")
    assert has_deadlock is False
    assert victim is None


def test_circular_deadlock_detection_and_preemption():
    arbiter = DeadlockGraphArbiter()
    # Agent 1 has heavy sunk cost (10,000 tokens), Agent 2 is brand new (500 tokens)
    arbiter.register_agent("agent1", priority=100, sunk_token_cost=10000)
    arbiter.register_agent("agent2", priority=100, sunk_token_cost=500)

    # 1. Agent 1 acquires File A
    arbiter.record_grant("file:A", "agent1")
    # 2. Agent 2 acquires File B
    arbiter.record_grant("file:B", "agent2")

    # 3. Agent 1 dynamically requests File B (waiting on Agent 2)
    has_deadlock, victim = arbiter.check_and_record_wait("agent1", "file:B")
    assert has_deadlock is False

    # 4. Agent 2 dynamically requests File A -> CREATES CIRCLE (A1 -> A2 -> A1)!
    has_deadlock, victim = arbiter.check_and_record_wait("agent2", "file:A")
    assert has_deadlock is True
    # Victim MUST be Agent 2 (lower sunk token cost)
    assert victim == "agent2"


def test_3_node_deadlock_cycle():
    arbiter = DeadlockGraphArbiter()
    arbiter.register_agent("a1", priority=50)
    arbiter.register_agent("a2", priority=100)
    arbiter.register_agent("a3", priority=20)  # Lowest priority victim

    arbiter.record_grant("file:1", "a1")
    arbiter.record_grant("file:2", "a2")
    arbiter.record_grant("file:3", "a3")

    arbiter.check_and_record_wait("a1", "file:2")
    arbiter.check_and_record_wait("a2", "file:3")
    has_deadlock, victim = arbiter.check_and_record_wait("a3", "file:1")

    assert has_deadlock is True
    assert victim == "a3"
