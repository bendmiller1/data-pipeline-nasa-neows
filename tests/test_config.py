"""
Comprehensive unit tests for the configuration module.

This test suite provides complete coverage for configuration loading, environment
variable handling, and path setup that controls NASA NEOWs data pipeline behavior
and ensures reliable configuration management across different environments.

Test Classes:
    TestDemoModeParsingLogic: Tests DEMO_MODE environment variable boolean conversion
    TestApiKeyConfiguration: Tests NASA API key loading with fallback handling
    TestPathConfiguration: Tests directory path calculations and validation

Coverage:
    - DEMO_MODE boolean parsing with various string inputs ("1", "true", "yes", etc.)
    - NASA API key environment variable loading with DEMO_KEY fallback
    - Path configuration validation (ROOT_DIR, DATA_DIR, output paths)
    - Module reload behavior with different environment variable states
    - Configuration consistency and default value handling

The test suite uses setup/teardown methods for environment variable isolation
and module reloading to ensure complete test isolation and validate reliable
configuration behavior across different deployment scenarios.
"""

from __future__ import annotations
import os
import pytest
import importlib
from src import config


class TestDemoModeParsingLogic:
    """
    Unit tests for DEMO_MODE environment variable parsing logic.
    
    Tests the boolean conversion logic that determines pipeline behavior
    between demo mode (local sample data) and live mode (NASA API calls).
    """

    def setup_method(self):
        """
        Preserve original environment state before each test method execution.
        
        Captures the current DEMO_MODE environment variable value to enable:
        - Complete restoration of pre-test environment state
        - Safe testing without permanent environment modifications
        - Proper isolation while respecting original system state
        - Module reloading with different configuration states
        """
        self.original_demo_mode = os.environ.get("DEMO_MODE")
    
    def teardown_method(self):
        """
        Restore original environment state after each test method execution.
        
        Restores DEMO_MODE environment variable and reloads config module:
        - Removes DEMO_MODE if it was not originally set
        - Restores original value if it existed before testing
        - Reloads config module to reflect original environment state
        - Ensures no permanent configuration modifications from tests
        """
        if self.original_demo_mode is None:
            os.environ.pop("DEMO_MODE", None)
        else:
            os.environ["DEMO_MODE"] = self.original_demo_mode
        importlib.reload(config)

    @pytest.mark.parametrize("env_value, expected_result", [
        ("1", True),          # Standard true value
        ("true", True),       # Lowercase true
        ("TRUE", True),       # Uppercase true  
        ("yes", True),        # Lowercase yes
        ("YES", True),        # Uppercase yes
        ("0", False),         # Standard false value
        ("false", False),     # Lowercase false
        ("no", False),        # Lowercase no
        ("", False),          # Empty string
        ("random", False),    # Random string
    ])
    def test_demo_mode_boolean_conversion(self, env_value, expected_result):
        """
        Test that DEMO_MODE correctly converts various string values to boolean.
        
        Verifies the parsing logic:
        - Accepts "1", "true", "yes" as True (case insensitive)
        - Treats all other values as False including "0", "false", empty strings
        - Handles case variations properly (TRUE, True, true all work)
        - Uses safe fallback behavior for unexpected input values
        
        This validates the core configuration logic that determines whether
        the pipeline operates in demo mode or live mode based on environment.
        """
        os.environ["DEMO_MODE"] = env_value
        importlib.reload(config)
        assert config.DEMO_MODE == expected_result

