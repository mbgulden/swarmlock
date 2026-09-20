"""
Hierarchical Intent Lock Tree and MVCC Version Vector Store.
Supports IS (Optimistic Intent Shared), IX (Intent Exclusive), S (Pessimistic Shared Read), and X (Exclusive Write).
Enforces hierarchical version propagation from parent directory mutations to children.
"""

from __future__ import annotations

import difflib
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Dict, List, Optional, Set, Tuple


class LockMode(str, Enum):
    IS = "IS"  # Optimistic Intent Shared (non-blocking read lease)
    IX = "IX"  # Intent Exclusive (writing a child node)
    S = "S"    # Pessimistic Shared Read (blocks writers)
    X = "X"    # Exclusive Write (blocks other writers and pessimistic readers)


COMPATIBILITY_MATRIX = {
    LockMode.IS: {LockMode.IS: True,  LockMode.IX: True,  LockMode.S: True,  LockMode.X: True},
    LockMode.IX: {LockMode.IS: True,  LockMode.IX: True,  LockMode.S: False, LockMode.X: False},
    LockMode.S:  {LockMode.IS: True,  LockMode.IX: False, LockMode.S: True,  LockMode.X: False},
    LockMode.X:  {LockMode.IS: True,  LockMode.IX: False, LockMode.S: False, LockMode.X: False},
}


def are_modes_compatible(mode1: LockMode, mode2: LockMode) -> bool:
    return COMPATIBILITY_MATRIX[mode1][mode2]


@dataclass
class ResourceKey:
    scheme: str
    path: str
    ast_symbol: Optional[str] = None

    @classmethod
    def parse(cls, raw: str) -> ResourceKey:
        raw = raw.strip()
        if ":" in raw:
            scheme, remainder = raw.split(":", 1)
        else:
            scheme, remainder = "file", raw

        ast_symbol = None
        if "#" in remainder:
            remainder, ast_symbol = remainder.split("#", 1)

        norm_path = str(PurePosixPath(remainder.replace("\\", "/")))
        if norm_path in [".", "root", ""]:
            norm_path = ""
        return cls(scheme=scheme.lower(), path=norm_path, ast_symbol=ast_symbol)

    def is_ancestor_of(self, other: ResourceKey) -> bool:
        if self.path == "" or self.path == ".":
            return True
        if self.path == other.path:
            return self.ast_symbol is None and other.ast_symbol is not None
        return other.path.startswith(self.path + "/")

    def __str__(self) -> str:
        s = f"{self.scheme}:{self.path}" if self.path else f"{self.scheme}:root"
        if self.ast_symbol:
            s += f"#{self.ast_symbol}"
        return s


@dataclass
class ActiveLock:
    lock_id: str
    holder: str
    resource: ResourceKey
    mode: LockMode
    fence_token: int
    version: int
    expires_at: float
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at


class HierarchyLockEngine:
    def __init__(self):
        self._versions: Dict[str, int] = {"": 1}
        self._snapshots: Dict[str, str] = {}
        self._locks: List[ActiveLock] = []
        # Guards _locks, _versions and _snapshots. RLock (not Lock) because
        # public methods call each other (e.g. acquire_lock -> check_conflict
        # -> _purge_expired). Without this, the check-then-append sequence in
        # acquire_lock races under threads: two threads can both pass
        # check_conflict before either appends, granting conflicting leases.
        self._lock = threading.RLock()

    def get_version(self, resource: ResourceKey) -> int:
        with self._lock:
            if resource.path not in self._versions:
                inherited = 1
                for path, ver in self._versions.items():
                    if path and resource.path.startswith(path + "/"):
                        inherited = max(inherited, ver)
                    elif path == "":
                        inherited = max(inherited, ver)
                self._versions[resource.path] = inherited
            return self._versions[resource.path]

    def record_snapshot(self, resource: ResourceKey, content: str) -> None:
        with self._lock:
            self._snapshots[resource.path] = content

    def get_snapshot(self, resource: ResourceKey) -> Optional[str]:
        with self._lock:
            return self._snapshots.get(resource.path)

    def increment_version_and_propagate(self, resource: ResourceKey) -> int:
        with self._lock:
            curr = self.get_version(resource) + 1
            self._versions[resource.path] = curr

            prefix = resource.path + "/" if resource.path else ""
            for path in list(self._versions.keys()):
                if prefix and path.startswith(prefix):
                    self._versions[path] = max(self._versions[path] + 1, curr)

            return curr

    def _purge_expired(self) -> None:
        # Caller must hold self._lock.
        now = time.time()
        self._locks = [l for l in self._locks if l.expires_at > now]

    def check_conflict(self, resource: ResourceKey, mode: LockMode, holder: str) -> Optional[ActiveLock]:
        with self._lock:
            self._purge_expired()
            for lock in self._locks:
                if lock.holder == holder:
                    continue

                if lock.resource.path == resource.path and lock.resource.ast_symbol == resource.ast_symbol:
                    if not are_modes_compatible(lock.mode, mode):
                        return lock

                if lock.resource.is_ancestor_of(resource):
                    if not are_modes_compatible(lock.mode, mode):
                        return lock

                if resource.is_ancestor_of(lock.resource):
                    if not are_modes_compatible(lock.mode, mode):
                        return lock

            return None

    def acquire_lock(
        self,
        lock_id: str,
        holder: str,
        resource: ResourceKey,
        mode: LockMode,
        fence_token: int,
        ttl_seconds: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Optional[ActiveLock], int]:
        # The whole check-then-append sequence runs under self._lock: two
        # threads racing here serialize, so conflicting leases can never both
        # be granted.
        with self._lock:
            conflict = self.check_conflict(resource, mode, holder)
            current_version = self.get_version(resource)
            if conflict is not None:
                return False, conflict, current_version

            lock = ActiveLock(
                lock_id=lock_id,
                holder=holder,
                resource=resource,
                mode=mode,
                fence_token=fence_token,
                version=current_version,
                expires_at=time.time() + ttl_seconds,
                metadata=metadata or {}
            )
            self._locks.append(lock)
            return True, None, current_version

    def renew_lock(self, lock_id: str, holder: str, ttl_seconds: float) -> bool:
        """Extend the TTL of a live lease held by ``holder``.

        This is the lease-renewal primitive long-lived holders (e.g. the
        Prismatic hypervisor's transaction heartbeat) call to keep a lease
        from expiring mid-operation.

        Fail-closed: returns False when the lease does not exist, has already
        expired, or is held by a different holder. A lease is never extended
        for someone who does not hold it.
        """
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, (int, float))
            or not ttl_seconds > 0
        ):
            raise ValueError(
                f"ttl_seconds must be a positive number, got {ttl_seconds!r}"
            )
        with self._lock:
            self._purge_expired()
            for lock in self._locks:
                if lock.lock_id == lock_id:
                    if lock.holder != holder or lock.is_expired:
                        return False
                    lock.expires_at = time.time() + ttl_seconds
                    return True
            return False

    def release_lock(
        self,
        lock_id: Optional[str] = None,
        holder: str = "default_agent",
        resource: Optional[ResourceKey] = None
    ) -> bool:
        with self._lock:
            self._purge_expired()
            initial_len = len(self._locks)
            if lock_id:
                self._locks = [l for l in self._locks if l.lock_id != lock_id]
            elif resource:
                self._locks = [l for l in self._locks if not (l.resource.path == resource.path and l.holder == holder)]
            else:
                self._locks = [l for l in self._locks if l.holder != holder]
            return len(self._locks) < initial_len

    def get_active_locks(self) -> List[ActiveLock]:
        with self._lock:
            self._purge_expired()
            return list(self._locks)
