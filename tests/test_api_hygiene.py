"""
API hygiene & packaging unit tests (GRO-4765 & GRO-4766).
"""

from pathlib import Path
import pytest


def test_gro_4765_py_typed_file_exists():
    """GRO-4765 Acceptance Test: swarmlock/py.typed marker file exists."""
    py_typed = Path(__file__).parent.parent / "swarmlock" / "py.typed"
    assert py_typed.exists(), "swarmlock/py.typed marker file missing"


def test_gro_4766_watch_request_pruned_from_top_level_import():
    """
    GRO-4766 Acceptance Test:
    from swarmlock import WatchRequest raises ImportError.
    from swarmlock.types import WatchRequest still works.
    """
    import swarmlock

    assert "WatchRequest" not in swarmlock.__all__

    with pytest.raises(ImportError):
        from swarmlock import WatchRequest  # type: ignore

    # Internal import from swarmlock.types works
    from swarmlock.types import WatchRequest as InternalWatchRequest
    assert InternalWatchRequest is not None
