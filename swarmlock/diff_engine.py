"""
Semantic Diff Engine for MVCC Stale-Read Conflict Rejections.
Generates unified diffs between base_version and current_version snapshots.
"""

from __future__ import annotations

import difflib
from typing import Optional


class SemanticDiffEngine:
    """
    Generates unified diff deltas between two file content snapshots.
    """

    @staticmethod
    def generate_diff(
        resource_path: str,
        base_content: Optional[str],
        current_content: Optional[str]
    ) -> str:
        """
        Produce a clean unified diff string between base snapshot and current snapshot.
        """
        base_lines = (base_content or "").splitlines(keepends=True)
        current_lines = (current_content or "").splitlines(keepends=True)

        diff = difflib.unified_diff(
            base_lines,
            current_lines,
            fromfile=f"a/{resource_path} (base)",
            tofile=f"b/{resource_path} (current)",
            n=3
        )
        return "".join(diff)
