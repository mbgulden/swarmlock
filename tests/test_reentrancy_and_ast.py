"""
Unit tests for Re-entrancy, AST symbol locking, and edge cases.
"""

import pytest
from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey


def test_reentrant_lock_acquisition():
    engine = HierarchyLockEngine()
    r = ResourceKey.parse("file:src/database.py")

    # Holder 1 acquires write lock
    ok1, conflict1, v1 = engine.acquire_lock("l1", "agent1", r, LockMode.X, 10, 60.0)
    assert ok1 is True
    assert conflict1 is None

    # Same holder acquires again (re-entrant) -> MUST SUCCEED without conflicting with self
    ok2, conflict2, v2 = engine.acquire_lock("l2", "agent1", r, LockMode.X, 11, 60.0)
    assert ok2 is True
    assert conflict2 is None


def test_ast_symbol_fine_grained_concurrency():
    engine = HierarchyLockEngine()
    r_fn1 = ResourceKey.parse("ast:src/database.py#connect")
    r_fn2 = ResourceKey.parse("ast:src/database.py#disconnect")
    r_file = ResourceKey.parse("file:src/database.py")

    # Agent 1 locks function `connect`
    ok1, c1, _ = engine.acquire_lock("l1", "agent1", r_fn1, LockMode.X, 10, 60.0)
    assert ok1 is True

    # Agent 2 locks function `disconnect` on SAME file -> MUST SUCCEED (Different AST functions!)
    ok2, c2, _ = engine.acquire_lock("l2", "agent2", r_fn2, LockMode.X, 11, 60.0)
    assert ok2 is True
    assert c2 is None

    # Agent 3 tries to lock the ENTIRE file -> MUST CONFLICT with both Agent 1 and Agent 2
    ok3, c3, _ = engine.acquire_lock("l3", "agent3", r_file, LockMode.X, 12, 60.0)
    assert ok3 is False
    assert c3 is not None
