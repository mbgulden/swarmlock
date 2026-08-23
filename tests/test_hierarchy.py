"""
Unit tests for HierarchyLockEngine and MVCC generation propagation.
"""

import pytest
from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey
from swarmlock.diff_engine import SemanticDiffEngine


def test_resource_key_parsing_and_ancestry():
    r_root = ResourceKey.parse("repo:root")
    r_dir = ResourceKey.parse("dir:src/auth")
    r_file = ResourceKey.parse("file:src/auth/jwt.py")
    r_ast = ResourceKey.parse("ast:src/auth/jwt.py#validate_token")

    assert r_root.is_ancestor_of(r_dir)
    assert r_root.is_ancestor_of(r_file)
    assert r_dir.is_ancestor_of(r_file)
    assert r_file.is_ancestor_of(r_ast)
    assert not r_file.is_ancestor_of(r_dir)


def test_hierarchical_lock_conflict():
    engine = HierarchyLockEngine()
    r_dir = ResourceKey.parse("dir:src/auth")
    r_file = ResourceKey.parse("file:src/auth/jwt.py")

    # Agent 1 acquires Exclusive lock on parent dir
    ok, conflict, v1 = engine.acquire_lock(
        lock_id="l1", holder="agent1", resource=r_dir,
        mode=LockMode.X, fence_token=100, ttl_seconds=60
    )
    assert ok is True
    assert conflict is None

    # Agent 2 attempts to acquire lock on child file -> MUST CONFLICT
    ok2, conflict2, v2 = engine.acquire_lock(
        lock_id="l2", holder="agent2", resource=r_file,
        mode=LockMode.X, fence_token=101, ttl_seconds=60
    )
    assert ok2 is False
    assert conflict2 is not None
    assert conflict2.holder == "agent1"

    # Agent 1 releases lock
    engine.release_lock(lock_id="l1", holder="agent1")

    # Now Agent 2 succeeds
    ok3, conflict3, v3 = engine.acquire_lock(
        lock_id="l2", holder="agent2", resource=r_file,
        mode=LockMode.X, fence_token=102, ttl_seconds=60
    )
    assert ok3 is True
    assert conflict3 is None


def test_mvcc_version_propagation():
    engine = HierarchyLockEngine()
    r_dir = ResourceKey.parse("dir:src/auth")
    r_file1 = ResourceKey.parse("file:src/auth/jwt.py")
    r_file2 = ResourceKey.parse("file:src/auth/session.py")

    # Access files to establish initial version (1)
    v_f1 = engine.get_version(r_file1)
    v_f2 = engine.get_version(r_file2)
    assert v_f1 == 1
    assert v_f2 == 1

    # Mutate parent directory
    new_dir_v = engine.increment_version_and_propagate(r_dir)
    assert new_dir_v == 2

    # Child files must have their generation counter incremented
    assert engine.get_version(r_file1) == 2
    assert engine.get_version(r_file2) == 2


def test_semantic_diff_generation():
    base_code = "def authenticate():\n    return False\n"
    current_code = "def authenticate():\n    check_revocation()\n    return True\n"

    diff = SemanticDiffEngine.generate_diff("src/auth/jwt.py", base_code, current_code)
    assert "--- a/src/auth/jwt.py (base)" in diff
    assert "+++ b/src/auth/jwt.py (current)" in diff
    assert "+    check_revocation()" in diff
