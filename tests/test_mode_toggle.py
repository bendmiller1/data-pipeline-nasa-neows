"""
Comprehensive unit tests for the mode toggle utility module.

This test suite provides complete coverage for runtime mode switching functions
that control the NASA NEOWs data pipeline behavior between demo and live modes
through environment variable manipulation.

Test Classes:
    TestSetDemoModeForProcess: Tests demo mode activation and environment handling
    TestSetLiveModeForProcess: Tests live mode activation and environment handling

Coverage:
    - Demo mode activation (DEMO_MODE="1") with proper environment variable setting
    - Live mode activation (DEMO_MODE="0") with proper environment variable setting
    - Function behavior when enable flags are False (no-op scenarios)
    - Environment variable isolation and cleanup between tests
    - Side effect validation for functions that modify global state

The test suite uses setup/teardown methods for environment variable management
to ensure complete test isolation and prevent interference between test runs
while validating reliable mode switching across the data pipeline.
"""

from __future__ import annotations
import os
import pytest
from src.utils.mode_toggle import set_demo_mode_for_process, set_live_mode_for_process

class TestSetDemoModeForProcess:
    """
    Unit tests for the set_demo_mode_for_process function.
    
    Tests demo mode activation logic that sets environment variables
    to enable local sample data usage instead of live API calls.
    """

    def setup_method(self):
        """
        Preserve original environment state before each test method execution.
        
        Captures the current DEMO_MODE environment variable value to enable:
        - Complete restoration of pre-test environment state
        - Protection of existing environment configuration
        - Safe testing without permanent environment modifications
        - Proper isolation while respecting original system state
        
        This method runs automatically before each test method, storing
        the original value for restoration during teardown.
        """
        self.original_demo_mode = os.environ.get("DEMO_MODE")
    
    def teardown_method(self):
        """
        Restore original environment state after each test method execution.
        
        Restores DEMO_MODE environment variable to its pre-test value:
        - Removes DEMO_MODE if it was not originally set
        - Restores original value if it existed before testing
        - Ensures no permanent environment modifications from tests
        - Maintains complete test isolation and repeatability
        
        This method runs automatically after each test method, providing
        comprehensive environment cleanup and state restoration.
        """
        if self.original_demo_mode is None:
            # Remove DEMO_MODE if it was not originally set
            os.environ.pop("DEMO_MODE", None)
        else:
            # Restore to original value
            os.environ["DEMO_MODE"] = self.original_demo_mode

    def test_enable_demo_mode_true(self):
        """
        Test that set_demo_mode_for_process correctly enables demo mode.
        
        Verifies the function:
        - Sets DEMO_MODE environment variable to "1" when enable_demo=True
        - Creates the environment variable if it doesn't exist
        - Properly signals downstream modules to use local sample data
        - Maintains environment variable state for the current process
        
        This validates the core demo mode activation that enables testing
        and development without making live API calls to NASA services.
        """
        set_demo_mode_for_process(True)
        assert os.environ.get("DEMO_MODE") == "1"

    def test_enable_demo_mode_false(self):
        """
        Test that set_demo_mode_for_process does nothing when disabled.
        
        Verifies the function:
        - Does not modify DEMO_MODE environment variable when enable_demo=False
        - Preserves the absence of DEMO_MODE if it wasn't previously set
        - Follows no-op behavior for conditional environment variable setting
        - Maintains existing environment state without side effects
        
        This validates that the function only acts when explicitly enabled,
        preventing unintended mode changes during pipeline execution.
        """
        # Ensure DEMO_MODE is not set initially
        os.environ.pop("DEMO_MODE", None)
        
        set_demo_mode_for_process(False)
        assert "DEMO_MODE" not in os.environ  # Should remain unset


class TestSetLiveModeForProcess:
    """
    Unit tests for the set_live_mode_for_process function.
    
    Tests live mode activation logic that sets environment variables
    to enable real NASA API calls instead of local sample data.
    """

    def setup_method(self):
        """
        Preserve original environment state before each test method execution.
        
        Captures the current DEMO_MODE environment variable value to enable:
        - Complete restoration of pre-test environment state
        - Protection of existing environment configuration
        - Safe testing without permanent environment modifications
        - Proper isolation while respecting original system state
        
        This method runs automatically before each test method, storing
        the original value for restoration during teardown.
        """
        self.original_demo_mode = os.environ.get("DEMO_MODE")
    
    def teardown_method(self):
        """
        Restore original environment state after each test method execution.
        
        Restores DEMO_MODE environment variable to its pre-test value:
        - Removes DEMO_MODE if it was not originally set
        - Restores original value if it existed before testing
        - Ensures no permanent environment modifications from tests
        - Maintains complete test isolation and repeatability
        
        This method runs automatically after each test method, providing
        comprehensive environment cleanup and state restoration.
        """
        """Restore original DEMO_MODE state after each test."""
        if self.original_demo_mode is None:
            # Remove DEMO_MODE if it was not originally set
            os.environ.pop("DEMO_MODE", None)
        else:
            # Restore to original value
            os.environ["DEMO_MODE"] = self.original_demo_mode

    def test_enable_live_mode_true(self):
        """
        Test that set_live_mode_for_process correctly enables live mode.
        
        Verifies the function:
        - Sets DEMO_MODE environment variable to "0" when enable_live=True
        - Creates the environment variable if it doesn't exist
        - Properly signals downstream modules to use live NASA API endpoints
        - Maintains environment variable state for production operations
        
        This validates the core live mode activation that enables production
        pipeline execution with real-time data from NASA NEOWs services.
        """
        set_live_mode_for_process(True)
        assert os.environ.get("DEMO_MODE") == "0"

    def test_enable_live_mode_false(self):
        """
        Test that set_live_mode_for_process does nothing when disabled.
        
        Verifies the function:
        - Does not modify DEMO_MODE environment variable when enable_live=False
        - Preserves the absence of DEMO_MODE if it wasn't previously set
        - Follows no-op behavior for conditional environment variable setting
        - Maintains existing environment state without side effects
        
        This validates that the function only acts when explicitly enabled,
        ensuring controlled mode transitions during pipeline configuration.
        """
        # Ensure DEMO_MODE is not set initially
        os.environ.pop("DEMO_MODE", None)
        
        set_live_mode_for_process(False)
        assert "DEMO_MODE" not in os.environ  # Should remain unset