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


class TestPathConfiguration:
    """
    Unit tests for directory path configuration and calculations.
    
    Tests the file system path setup that determines where the pipeline
    stores data, sample files, and output results relative to the project root.
    """

    def test_root_directory_calculation(self):
        """
        Test that ROOT_DIR correctly points to the project root directory.
        
        Verifies the path calculation:
        - Uses Path(__file__).resolve().parents[1] to go up from src/config.py
        - Results in the actual project root directory
        - Provides absolute path for reliable file operations
        - Maintains correct relationship to config.py location
        
        This validates the foundation path that all other directories depend on.
        """
        # ROOT_DIR should be the parent of the src directory
        expected_root = config.Path(__file__).resolve().parents[1]  # tests/ -> project root
        assert config.ROOT_DIR == expected_root
        assert config.ROOT_DIR.is_absolute()
        assert config.ROOT_DIR.exists()

    def test_data_directory_paths(self):
        """
        Test that data directory paths are correctly calculated relative to ROOT_DIR.
        
        Verifies the directory structure:
        - DATA_DIR points to ROOT_DIR / "data"
        - PROCESSED_DIR points to DATA_DIR / "processed" 
        - WAREHOUSE_DIR points to DATA_DIR / "warehouse"
        - SAMPLE_DATA_DIR points to ROOT_DIR / "sample_data"
        
        This validates the complete data directory hierarchy used by the pipeline.
        """
        assert config.DATA_DIR == config.ROOT_DIR / "data"
        assert config.PROCESSED_DIR == config.DATA_DIR / "processed"
        assert config.WAREHOUSE_DIR == config.DATA_DIR / "warehouse"
        assert config.SAMPLE_DATA_DIR == config.ROOT_DIR / "sample_data"

    def test_output_file_paths(self):
        """
        Test that output file paths are correctly calculated within data directories.
        
        Verifies the file path construction:
        - CSV_OUTPUT points to PROCESSED_DIR / "neows_latest.csv"
        - DB_PATH points to WAREHOUSE_DIR / "neows_data.db"
        - Paths use proper Path object operations for cross-platform compatibility
        - File extensions and names match expected pipeline outputs
        
        This validates the specific file paths where pipeline results are stored.
        """
        assert config.CSV_OUTPUT == config.PROCESSED_DIR / "neows_latest.csv"
        assert config.DB_PATH == config.WAREHOUSE_DIR / "neows_data.db"
        
        # Verify these are Path objects, not strings
        assert isinstance(config.CSV_OUTPUT, config.Path)
        assert isinstance(config.DB_PATH, config.Path)

    def test_api_base_url_constant(self):
        """
        Test that NASA API base URL is correctly configured.
        
        Verifies the API configuration:
        - NASA_API_BASE_URL contains the correct NASA NEOWs endpoint
        - Uses HTTPS protocol for secure communication
        - Points to the correct API version (v1)
        - Maintains expected string format for HTTP requests
        
        This validates the API endpoint configuration for live data fetching.
        """
        expected_url = "https://api.nasa.gov/neo/rest/v1"
        assert config.NASA_API_BASE_URL == expected_url
        assert config.NASA_API_BASE_URL.startswith("https://")
        assert "neo/rest/v1" in config.NASA_API_BASE_URL


class TestModuleReloadBehavior:
    """
    Unit tests for configuration module reload behavior with environment changes.
    
    Tests how the config module responds to environment variable modifications
    when reloaded, ensuring dynamic configuration updates work correctly.
    """

    def setup_method(self):
        """
        Preserve original environment state before each test method execution.
        
        Captures current environment variables to enable:
        - Complete restoration of pre-test environment state
        - Safe testing of configuration changes without permanent modifications
        - Proper isolation while respecting original system state
        - Module reloading with different configuration scenarios
        """
        self.original_demo_mode = os.environ.get("DEMO_MODE")
        self.original_api_key = os.environ.get("NASA_API_KEY")
    
    def teardown_method(self):
        """
        Restore original environment state after each test method execution.
        
        Restores all environment variables and reloads config module:
        - Removes variables if they were not originally set
        - Restores original values if they existed before testing
        - Reloads config module to reflect original environment state
        - Ensures no permanent configuration modifications from tests
        """
        if self.original_demo_mode is None:
            os.environ.pop("DEMO_MODE", None)
        else:
            os.environ["DEMO_MODE"] = self.original_demo_mode
            
        if self.original_api_key is None:
            os.environ.pop("NASA_API_KEY", None)
        else:
            os.environ["NASA_API_KEY"] = self.original_api_key
            
        importlib.reload(config)

    def test_demo_mode_changes_on_reload(self):
        """
        Test that DEMO_MODE configuration updates when environment changes and module reloads.
        
        Verifies the reload behavior:
        - Initial configuration reflects current environment state
        - Environment variable changes take effect after module reload
        - Multiple reload cycles work correctly with different values
        - Configuration stays consistent with environment variables
        
        This validates that configuration can be dynamically updated during runtime
        by modifying environment variables and reloading the config module.
        """
        # Start with demo mode disabled
        os.environ["DEMO_MODE"] = "0"
        importlib.reload(config)
        assert config.DEMO_MODE == False
        
        # Change to demo mode enabled and reload
        os.environ["DEMO_MODE"] = "1"
        importlib.reload(config)
        assert config.DEMO_MODE == True
        
        # Change back to disabled and reload
        os.environ["DEMO_MODE"] = "false"
        importlib.reload(config)
        assert config.DEMO_MODE == False

    def test_api_key_changes_on_reload(self):
        """
        Test that NASA_API_KEY configuration updates when environment changes and module reloads.
        
        Verifies the reload behavior:
        - Falls back to DEMO_KEY when environment variable is removed
        - Uses custom API key when environment variable is set
        - Multiple reload cycles work correctly with different key values
        - No exposure of actual API key values in test assertions
        
        This validates that API key configuration can be dynamically updated
        for different deployment scenarios or credential rotation.
        """
        # Start with no API key (should use DEMO_KEY)
        os.environ.pop("NASA_API_KEY", None)
        importlib.reload(config)
        assert config.NASA_API_KEY == "DEMO_KEY"
        
        # Set custom API key and reload
        test_key = "test_custom_key_67890"
        os.environ["NASA_API_KEY"] = test_key
        importlib.reload(config)
        assert config.NASA_API_KEY == test_key
        
        # Remove API key again and reload (back to fallback)
        os.environ.pop("NASA_API_KEY", None)
        importlib.reload(config)
        assert config.NASA_API_KEY == "DEMO_KEY"


