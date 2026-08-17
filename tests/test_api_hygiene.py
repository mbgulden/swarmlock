"""
API hygiene & packaging unit tests (GRO-4765 & GRO-4766).
"""

from pathlib import Path
import pytest


def test_gro_4765_py_typed_file_exists():
    """GRO-4765 Acceptance Test: swarmlock/py.typed marker file exists."""
    py_typed = Path(__file__).parent.parent / "swarmlock" / "py.typed"
    assert py_typed.exists(), "swarmlock/py.typed marker file missing"


def test_gro_4766_watch_request_exported_in_v02():
    """
    v0.2 Acceptance Test:
    from swarmlock import WatchRequest is supported in v0.2.
    """
    import swarmlock

    assert "WatchRequest" in swarmlock.__all__

    from swarmlock import WatchRequest
    assert WatchRequest is not None
