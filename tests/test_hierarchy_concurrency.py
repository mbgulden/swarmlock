"""Thread-safety and lease-renewal tests for HierarchyLockEngine.

Regression coverage for the hypervisor audit findings:
- the check-then-append race in acquire_lock (no threading lock guarded the
  internal _locks list, so two threads could both pass check_conflict and be
  granted conflicting leases);
- the missing lease-renewal path (a long-lived holder had no way to extend a
  TTL, so leases could expire mid-operation).
"""

import threading
import time

import pytest

from swarmlock.hierarchy import HierarchyLockEngine, LockMode, ResourceKey


def _acquire(engine, lock_id, holder, resource, ttl=60.0):
    return engine.acquire_lock(
        lock_id=lock_id,
        holder=holder,
        resource=resource,
        mode=LockMode.X,
        fence_token=1,
        ttl_seconds=ttl,
    )


def test_concurrent_acquire_grants_exactly_one_conflicting_lease():
    """32 threads racing acquire_lock(X) on one resource: exactly one wins.

    Pre-fix this flakes: the check-then-append sequence was unguarded, so two
    threads could both observe "no conflict" and both append.
    """
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:race.txt")
    n_threads = 32
    barrier = threading.Barrier(n_threads)
    granted = []

    def attempt(i):
        barrier.wait(timeout=15)
        ok, _, _ = _acquire(engine, f"l{i}", f"holder{i}", resource)
        granted.append(ok)

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert len(granted) == n_threads
    assert sum(granted) == 1, f"expected exactly one grant, got {sum(granted)}"


def test_concurrent_acquire_release_cycles_stay_consistent():
    """Interleaved acquire/release from many threads never corrupts the registry."""
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:cycle.txt")
    errors = []

    def worker(i):
        try:
            for _ in range(25):
                ok, _, _ = _acquire(engine, f"l{i}", f"holder{i}", resource, ttl=60.0)
                if ok:
                    engine.release_lock(lock_id=f"l{i}", holder=f"holder{i}")
                else:
                    time.sleep(0.001)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"worker errors: {errors}"
    assert engine.get_active_locks() == []


def test_renew_lock_extends_ttl():
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:renew.txt")
    ok, _, _ = _acquire(engine, "l1", "alice", resource, ttl=0.4)
    assert ok

    time.sleep(0.25)
    assert engine.renew_lock("l1", "alice", 60.0) is True

    # Past the original TTL: the lease must still be held.
    time.sleep(0.3)
    ok2, conflict, _ = _acquire(engine, "l2", "bob", resource, ttl=60.0)
    assert ok2 is False
    assert conflict is not None and conflict.holder == "alice"


def test_renew_lock_rejects_wrong_holder():
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:renew2.txt")
    ok, _, _ = _acquire(engine, "l1", "alice", resource, ttl=60.0)
    assert ok
    assert engine.renew_lock("l1", "mallory", 60.0) is False


def test_renew_lock_rejects_missing_and_expired():
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:renew3.txt")
    assert engine.renew_lock("nope", "alice", 60.0) is False

    ok, _, _ = _acquire(engine, "l1", "alice", resource, ttl=0.15)
    assert ok
    time.sleep(0.3)  # let it expire; purge runs on next access
    assert engine.renew_lock("l1", "alice", 60.0) is False


def test_renew_lock_rejects_bad_ttl():
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:renew4.txt")
    ok, _, _ = _acquire(engine, "l1", "alice", resource, ttl=60.0)
    assert ok
    for bad in (0, -1.5, True, "60"):
        with pytest.raises(ValueError):
            engine.renew_lock("l1", "alice", bad)


def test_renew_does_not_deadlock_under_concurrency():
    """RLock reentrancy: renew racing acquire/release must not deadlock."""
    engine = HierarchyLockEngine()
    resource = ResourceKey.parse("file:renew5.txt")
    stop = threading.Event()
    errors = []

    def holder():
        try:
            ok, _, _ = _acquire(engine, "l1", "alice", resource, ttl=0.5)
            assert ok
            while not stop.is_set():
                engine.renew_lock("l1", "alice", 0.5)
                time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def rival():
        try:
            while not stop.is_set():
                ok, _, _ = _acquire(engine, "lx", "bob", resource, ttl=0.2)
                if ok:
                    engine.release_lock(lock_id="lx", holder="bob")
                time.sleep(0.01)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    t1 = threading.Thread(target=holder)
    t2 = threading.Thread(target=rival)
    t1.start()
    t2.start()
    time.sleep(1.0)
    stop.set()
    t1.join(timeout=15)
    t2.join(timeout=15)
    assert not t1.is_alive() and not t2.is_alive(), "deadlock detected"
    assert not errors, f"errors: {errors}"
