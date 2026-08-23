#!/usr/bin/env python3
"""
SwarmSync: Real-Time Event-Based Repository Synchronizer for Prismatic Hypervisor.
Listens to SwarmLock event broadcasts and fast-forwards all local repositories instantly.
"""

import argparse
import json
import logging
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SwarmSync] %(message)s"
)
logger = logging.getLogger("swarmsync")

SWARMLOCK_SOCK = "/tmp/swarmlock.sock"
SWARMLOCK_HOST = os.environ.get("SWARMLOCK_HOST", "100.83.32.92")
SWARMLOCK_PORT = int(os.environ.get("SWARMLOCK_PORT", 40595))

REPOS = ["swarmlock", "swarmproof", "swarmgate", "swarmsaga", "swarmledger"]


class SwarmSyncEngine:
    def __init__(self, github_dir: Optional[Path] = None, poll_interval: float = 60.0):
        self.github_dir = github_dir or (Path.home() / "Github")
        self.poll_interval = poll_interval
        self._last_sync = 0.0

    def sync_all_repos(self) -> List[str]:
        """Runs fast-forward git pull across all 5 repos."""
        updated = []
        for r in REPOS:
            p = self.github_dir / r
            if p.exists() and (p / ".git").exists():
                try:
                    res = subprocess.run(
                        ["git", "-C", str(p), "pull", "--ff-only"],
                        capture_output=True,
                        text=True,
                        check=False
                    )
                    if res.returncode == 0 and "Already up to date" not in res.stdout:
                        logger.info("Updated %s: %s", r, res.stdout.strip())
                        updated.append(r)
                except Exception as exc:
                    logger.debug("Failed syncing %s: %s", r, exc)
        self._last_sync = time.time()
        return updated

    def run_event_loop(self):
        """Continuously listens to SwarmLock event broadcast and triggers fast-forward syncs."""
        logger.info("SwarmSync daemon started. Monitoring %s", self.github_dir)
        self.sync_all_repos()

        while True:
            try:
                # 1. Connect to event stream
                sock = None
                if os.path.exists(SWARMLOCK_SOCK):
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.connect(SWARMLOCK_SOCK)
                else:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((SWARMLOCK_HOST, SWARMLOCK_PORT))

                sock.settimeout(self.poll_interval)
                sock.sendall(json.dumps({"action": "WATCH"}).encode("utf-8") + b"\n")

                while True:
                    line = sock.recv(4096)
                    if not line:
                        break
                    try:
                        event = json.loads(line.decode("utf-8").strip())
                        # Trigger sync on commit/sync events
                        if event.get("event") in ["COMMIT", "REPO_SYNC", "RELEASE"]:
                            logger.info("Cluster event detected (%s). Fast-forwarding local repos...", event.get("event"))
                            self.sync_all_repos()
                    except json.JSONDecodeError:
                        pass

            except socket.timeout:
                # Periodic fallback poll
                self.sync_all_repos()
            except Exception as exc:
                logger.debug("Reconnecting event stream in 10s (%s)", exc)
                time.sleep(10.0)
            finally:
                if sock:
                    try:
                        sock.close()
                    except Exception:
                        pass


def main():
    parser = argparse.ArgumentParser(description="SwarmSync Real-Time Auto-Synchronizer")
    parser.add_argument("--dir", type=str, default=None, help="Github root directory")
    parser.add_argument("--interval", type=float, default=60.0, help="Fallback polling interval")
    parser.add_argument("--once", action="store_true", help="Sync once and exit")
    args = parser.parse_args()

    engine = SwarmSyncEngine(github_dir=Path(args.dir) if args.dir else None, poll_interval=args.interval)
    if args.once:
        updated = engine.sync_all_repos()
        print(f"Sync complete. Updated {len(updated)} repos: {updated}")
    else:
        engine.run_event_loop()


if __name__ == "__main__":
    main()