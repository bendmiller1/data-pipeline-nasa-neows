"""
Environment utilities for the NASA NeoWs Data Pipeline.

Provides helpers to adjust runtime environment flags (e.g., DEMO mode)
without modifying the persistent .env file.

Typical usage example:
    from src.utils.env import set_demo_mode_for_process
    set_demo_mode_for_process(True)
"""

from __future__ import annotations

import os


def set_demo_mode_for_process(enable_demo: bool) -> None:
    """
    Optionally force DEMO mode for the current Python process.

    This sets the environment variable DEMO_MODE to "1" when enabled, which
    downstream modules (e.g., src.config) interpret as a signal to use local
    sample data instead of live API calls.

    Args:
        enable_demo (bool): If True, sets DEMO_MODE="1" for this process.
            If False, does nothing (leaves env unchanged).
    """
    if enable_demo:
        os.environ["DEMO_MODE"] = "1"