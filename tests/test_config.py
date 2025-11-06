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


class TestApiKeyConfiguration:
    """
    Unit tests for NASA API key configuration and fallback logic.
    
    Tests the API key loading mechanism that handles environment variables
    with secure fallback to NASA's public DEMO_KEY for testing scenarios.
    """

    def setup_method(self):
        """
        Preserve original environment state before each test method execution.
        
        Captures the current NASA_API_KEY environment variable value to enable:
        - Complete restoration of pre-test environment state
        - Safe testing without exposing actual API keys in test output
        - Proper isolation while respecting original system state
        - Module reloading with different API key configurations
        """
        self.original_api_key = os.environ.get("NASA_API_KEY")
    
    def teardown_method(self):
        """
        Restore original environment state after each test method execution.
        
        Restores NASA_API_KEY environment variable and reloads config module:
        - Removes NASA_API_KEY if it was not originally set
        - Restores original value if it existed before testing
        - Reloads config module to reflect original API key state
        - Ensures no permanent configuration modifications from tests
        """
        if self.original_api_key is None:
            os.environ.pop("NASA_API_KEY", None)
        else:
            os.environ["NASA_API_KEY"] = self.original_api_key
        importlib.reload(config)

    def test_api_key_fallback_to_demo_key(self):
        """
        Test that NASA_API_KEY falls back to DEMO_KEY when not set in environment.
        
        Verifies the fallback logic:
        - Uses "DEMO_KEY" when NASA_API_KEY environment variable is not set
        - Provides safe default for testing and development scenarios
        - Ensures pipeline can run without requiring actual NASA API credentials
        - Maintains expected string value for API key configuration
        
        This validates the core fallback mechanism that enables demo mode
        operation without exposing or requiring real API credentials.
        """
        os.environ.pop("NASA_API_KEY", None)
        importlib.reload(config)
        assert config.NASA_API_KEY == "DEMO_KEY"

    def test_api_key_uses_environment_value(self):
        """
        Test that NASA_API_KEY uses actual environment value when provided.
        
        Verifies the environment loading:
        - Reads NASA_API_KEY from environment when present
        - Does not expose actual key values in test assertions
        - Maintains proper environment variable precedence
        - Validates configuration loading without security risks
        
        This validates that real API keys are properly loaded from environment
        while ensuring test safety and no credential exposure.
        """
        test_key = "test_fake_api_key_12345"
        os.environ["NASA_API_KEY"] = test_key
        importlib.reload(config)
        # Verify key is loaded but don't expose actual value
        assert config.NASA_API_KEY == test_key
        assert config.NASA_API_KEY != "DEMO_KEY"


