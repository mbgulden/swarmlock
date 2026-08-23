"""
Durable Monotonic Fencing Token Generator.
Guarantees strictly increasing 64-bit sequence counters across daemon restarts and crashes.
"""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional


class DurableFencingTokenGenerator:
    """
    Thread-safe and restart-durable monotonic sequence generator.
    Formula on boot: Next Token = max(Persisted Token, Epoch Milliseconds * 1000) + 1.
    """

    def __init__(self, db_path: Optional[str | Path] = None):
        if db_path is None:
            base_dir = Path.home() / ".swarmlock"
            base_dir.mkdir(parents=True, exist_ok=True)
            self.db_path = base_dir / "fencing_tokens.db"
        else:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._lock = threading.Lock()
        self._current_token: int = 0
        self._init_db()

    def _init_db(self) -> None:
        """Initialize SQLite WAL database and bootstrap initial monotonic token."""
        with self._lock:
            conn = sqlite3.connect(str(self.db_path), timeout=10.0)
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS fencing_state (
                        id INTEGER PRIMARY KEY CHECK (id = 1),
                        last_token INTEGER NOT NULL,
                        updated_at REAL NOT NULL
                    );
                """)
                cursor = conn.execute("SELECT last_token FROM fencing_state WHERE id = 1")
                row = cursor.fetchone()
                persisted_token = row[0] if row else 0

                epoch_bootstrap = int(time.time() * 1000.0) * 1000
                bootstrapped = max(persisted_token, epoch_bootstrap) + 1

                conn.execute("""
                    INSERT INTO fencing_state (id, last_token, updated_at)
                    VALUES (1, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        last_token = excluded.last_token,
                        updated_at = excluded.updated_at;
                """, (bootstrapped, time.time()))
                conn.commit()
                self._current_token = bootstrapped
            finally:
                conn.close()

    def next_token(self) -> int:
        """Atomically generate and persist the next monotonic fencing token."""
        with self._lock:
            self._current_token += 1
            token = self._current_token
            conn = sqlite3.connect(str(self.db_path), timeout=5.0)
            try:
                conn.execute("""
                    UPDATE fencing_state
                    SET last_token = ?, updated_at = ?
                    WHERE id = 1;
                """, (token, time.time()))
                conn.commit()
                return token
            finally:
                conn.close()

    @property
    def current_token(self) -> int:
        """Read current high-water mark token."""
        with self._lock:
            return self._current_token
