"""
Unit tests for DurableFencingTokenGenerator.
"""

import tempfile
import threading
from pathlib import Path
import pytest
from swarmlock.fencing import DurableFencingTokenGenerator


def test_fencing_token_strict_monotonicity():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_fencing.db"
        gen = DurableFencingTokenGenerator(db_path=db_path)
        
        t1 = gen.next_token()
        t2 = gen.next_token()
        t3 = gen.next_token()
        
        assert t1 < t2 < t3
        assert t3 == gen.current_token


def test_fencing_token_survives_daemon_crash_and_restart():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_fencing.db"
        
        # Instance 1 runs and issues tokens
        gen1 = DurableFencingTokenGenerator(db_path=db_path)
        tokens_run1 = [gen1.next_token() for _ in range(5)]
        max_run1 = tokens_run1[-1]
        
        # Simulate daemon crash / restart: instantiate new generator on same DB
        gen2 = DurableFencingTokenGenerator(db_path=db_path)
        tokens_run2 = [gen2.next_token() for _ in range(5)]
        
        assert tokens_run2[0] > max_run1
        for i in range(len(tokens_run2) - 1):
            assert tokens_run2[i] < tokens_run2[i+1]


def test_fencing_token_multithreaded_concurrency():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_fencing.db"
        gen = DurableFencingTokenGenerator(db_path=db_path)
        
        collected_tokens = []
        lock = threading.Lock()
        
        def worker():
            for _ in range(50):
                t = gen.next_token()
                with lock:
                    collected_tokens.append(t)
                    
        threads = [threading.Thread(target=worker) for _ in range(8)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
            
        assert len(collected_tokens) == 400
        # All tokens must be strictly unique
        assert len(set(collected_tokens)) == 400
