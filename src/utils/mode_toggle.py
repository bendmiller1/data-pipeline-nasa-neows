"""
Mode toggle utilities for the NASA NeoWs Data Pipeline.

Provides helpers to adjust runtime mode flags (demo vs live)
without modifying the persistent .env file.

Typical usage example:
    from src.utils.mode_toggle import set_demo_mode_for_process
    set_demo_mode_for_process(True)
"""
# This module allows the user to set the pipeline mode (demo vs live) at runtime without changing the .env file

from __future__ import annotations

import os # Provides access to environment variables to set the mode for the current process


def set_demo_mode_for_process(enable_demo: bool) -> None: # Takes a boolean parameter: enable_demo = True to set demo mode, False to leave unchanged
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
        os.environ["DEMO_MODE"] = "1" # If enable_demo is True, sets the environment variable DEMO_MODE to "1" for the current process


def set_live_mode_for_process(enable_live: bool) -> None: # Takes a boolean parameter: enable_live = True to set live mode, False to leave unchanged
    """ 
    Force LIVE mode for the current Python process.

    This sets the environment variable DEMO_MODE to "0" when enabled, which
    downstream modules (e.g., src.config) interpret as a signal to use live
    API calls instead of sample data.

    Args:
        enable_live (bool): If True, sets DEMO_MODE="0" for this process.
            If False, does nothing (leaves env unchanged).
    """
    if enable_live:
        os.environ["DEMO_MODE"] = "0" # If enable_live is True, sets the environment variable DEMO_MODE to "0" for the current process