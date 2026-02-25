"""
Unit tests for SKIP_EXTERNAL_RESOLUTION configuration option.
Tests config_manager.py and graph_builder.py integration.
"""

import os
import pytest
from unittest.mock import MagicMock, patch, call
from codegraphcontext.cli.config_manager import (
    get_config_value,
    set_config_value,
    CONFIG_DESCRIPTIONS,
    CONFIG_VALIDATORS
)


class TestSkipExternalResolutionConfig:
    """Test the SKIP_EXTERNAL_RESOLUTION configuration option."""

    def test_config_exists_in_descriptions(self):
        """Test that SKIP_EXTERNAL_RESOLUTION has a description."""
        assert "SKIP_EXTERNAL_RESOLUTION" in CONFIG_DESCRIPTIONS
        assert len(CONFIG_DESCRIPTIONS["SKIP_EXTERNAL_RESOLUTION"]) > 0
        assert "external" in CONFIG_DESCRIPTIONS["SKIP_EXTERNAL_RESOLUTION"].lower()

    def test_config_has_validator(self):
        """Test that SKIP_EXTERNAL_RESOLUTION has a validator function."""
        assert "SKIP_EXTERNAL_RESOLUTION" in CONFIG_VALIDATORS
        validator = CONFIG_VALIDATORS["SKIP_EXTERNAL_RESOLUTION"]
        assert callable(validator)

    def test_validator_accepts_true(self):
        """Test that validator accepts string 'true'."""
        validator = CONFIG_VALIDATORS["SKIP_EXTERNAL_RESOLUTION"]
        assert validator("true") is True
        assert validator("True") is True
        assert validator("TRUE") is True

    def test_validator_accepts_false(self):
        """Test that validator accepts string 'false'."""
        validator = CONFIG_VALIDATORS["SKIP_EXTERNAL_RESOLUTION"]
        assert validator("false") is True
        assert validator("False") is True
        assert validator("FALSE") is True

    def test_validator_rejects_invalid_values(self):
        """Test that validator rejects invalid values."""
        validator = CONFIG_VALIDATORS["SKIP_EXTERNAL_RESOLUTION"]
        assert validator("yes") is False
        assert validator("no") is False
        assert validator("1") is False
        assert validator("0") is False
        assert validator("enabled") is False
        assert validator("") is False

    def test_default_value_is_false(self):
        """Test that default value is 'false' for backward compatibility."""
        # Clear any existing config
        with patch.dict(os.environ, {}, clear=True):
            value = get_config_value("SKIP_EXTERNAL_RESOLUTION")
            # Default should be "false" to maintain backward compatibility
            assert value is None or value.lower() == "false"

    def test_set_and_get_config_value(self):
        """Test setting and getting the configuration value."""
        # Set to true
        set_config_value("SKIP_EXTERNAL_RESOLUTION", "true")
        assert get_config_value("SKIP_EXTERNAL_RESOLUTION").lower() == "true"

        # Set to false
        set_config_value("SKIP_EXTERNAL_RESOLUTION", "false")
        assert get_config_value("SKIP_EXTERNAL_RESOLUTION").lower() == "false"

    def test_environment_variable_override(self):
        """Test that environment variable SKIP_EXTERNAL_RESOLUTION works."""
        with patch.dict(os.environ, {"SKIP_EXTERNAL_RESOLUTION": "true"}):
            value = get_config_value("SKIP_EXTERNAL_RESOLUTION")
            assert value == "true"

        with patch.dict(os.environ, {"SKIP_EXTERNAL_RESOLUTION": "false"}):
            value = get_config_value("SKIP_EXTERNAL_RESOLUTION")
            assert value == "false"


class TestSkipExternalResolutionBehavior:
    """Test the behavior of SKIP_EXTERNAL_RESOLUTION in graph_builder.py"""

    @pytest.fixture
    def mock_graph_builder(self):
        """Create a mock GraphBuilder instance."""
        with patch('codegraphcontext.tools.graph_builder.DatabaseManager'):
            with patch('codegraphcontext.tools.graph_builder.get_config_value') as mock_get_config:
                from codegraphcontext.tools.graph_builder import GraphBuilder
                builder = GraphBuilder()
                yield builder, mock_get_config

    def test_skips_external_when_enabled(self, mock_graph_builder):
        """Test that external calls are skipped when config is true."""
        builder, mock_get_config = mock_graph_builder
        mock_get_config.return_value = "true"

        # Mock the _resolve_call to return None (unresolved external call)
        with patch.object(builder, '_resolve_call', return_value=None):
            with patch.object(builder, '_create_relationship') as mock_create_rel:
                # Mock logger to verify no warning is logged
                with patch('codegraphcontext.tools.graph_builder.warning_logger') as mock_warning:
                    
                    function_calls = [
                        {"name": "externalMethod", "lookup_name": "ExternalClass"}
                    ]
                    
                    builder._create_function_calls(
                        "testFunction",
                        function_calls,
                        "test/file.java",
                        "file_id_123"
                    )

                    # verify no warning was logged
                    mock_warning.assert_not_called()
                    
                    # Verify no relationship was created for external call
                    mock_create_rel.assert_not_called()

    def test_logs_warning_when_disabled(self, mock_graph_builder):
        """Test that warnings are logged when config is false (default behavior)."""
        builder, mock_get_config = mock_graph_builder
        mock_get_config.return_value = "false"

        # Mock the _resolve_call to return None (unresolved call)
        with patch.object(builder, '_resolve_call', return_value=None):
            with patch('codegraphcontext.tools.graph_builder.warning_logger') as mock_warning:
                
                function_calls = [
                    {"name": "externalMethod", "lookup_name": "ExternalClass"}
                ]
                
                builder._create_function_calls(
                    "testFunction",
                    function_calls,
                    "test/file.java",
                    "file_id_123"
                )

                # Verify warning WAS logged (default behavior)
                mock_warning.assert_called_once()
                assert "Could not resolve call" in mock_warning.call_args[0][0]

    def test_creates_relationship_for_resolved_calls(self, mock_graph_builder):
        """Test that relationships are created for successfully resolved calls."""
        builder, mock_get_config = mock_graph_builder
        mock_get_config.return_value = "true"

        # Mock _resolve_call to return a resolved path (internal call)
        with patch.object(builder, '_resolve_call', return_value="resolved/path.java"):
            with patch.object(builder, '_create_relationship') as mock_create_rel:
                
                function_calls = [
                    {"name": "internalMethod", "lookup_name": "InternalClass"}
                ]
                
                builder._create_function_calls(
                    "testFunction",
                    function_calls,
                    "test/file.java",
                    "file_id_123"
                )

                # Verify relationship WAS created for resolved internal call
                mock_create_rel.assert_called_once()

    def test_mixed_calls_resolved_and_unresolved(self, mock_graph_builder):
        """Test mixture of resolved and unresolved calls."""
        builder, mock_get_config = mock_graph_builder
        mock_get_config.return_value = "true"

        # Mock _resolve_call to alternate between resolved and unresolved
        resolve_results = ["resolved/path1.java", None, "resolved/path2.java", None]
        with patch.object(builder, '_resolve_call', side_effect=resolve_results):
            with patch.object(builder, '_create_relationship') as mock_create_rel:
                with patch('codegraphcontext.tools.graph_builder.warning_logger') as mock_warning:
                    
                    function_calls = [
                        {"name": "internalMethod1", "lookup_name": "InternalClass1"},
                        {"name": "externalMethod1", "lookup_name": "ExternalClass1"},
                        {"name": "internalMethod2", "lookup_name": "InternalClass2"},
                        {"name": "externalMethod2", "lookup_name": "ExternalClass2"},
                    ]
                    
                    builder._create_function_calls(
                        "testFunction",
                        function_calls,
                        "test/file.java",
                        "file_id_123"
                    )

                    # Verify no warnings logged (skip external enabled)
                    mock_warning.assert_not_called()
                    
                    # Verify exactly 2 relationships created (only for resolved calls)
                    assert mock_create_rel.call_count == 2


class TestBackwardCompatibility:
    """Test that existing behavior is preserved when config is not set."""

    def test_default_behavior_unchanged(self):
        """Test that default behavior matches original (warnings + attempts)."""
        # When SKIP_EXTERNAL_RESOLUTION is not set or is "false",
        # behavior should match original cgc behavior
        
        with patch.dict(os.environ, {}, clear=True):
            from codegraphcontext.cli.config_manager import get_config_value
            
            # Default should be None or "false"
            value = get_config_value("SKIP_EXTERNAL_RESOLUTION")
            assert value is None or value.lower() == "false"

    def test_existing_configs_not_affected(self):
        """Test that other configuration options still work."""
        # Setting SKIP_EXTERNAL_RESOLUTION should not affect other configs
        set_config_value("SKIP_EXTERNAL_RESOLUTION", "true")
        set_config_value("INDEX_VARIABLES", "false")
        
        assert get_config_value("SKIP_EXTERNAL_RESOLUTION").lower() == "true"
        assert get_config_value("INDEX_VARIABLES").lower() == "false"


# Integration test (would require actual Neo4j - marked as e2e)
@pytest.mark.e2e
class TestSkipExternalResolutionE2E:
    """End-to-end tests for SKIP_EXTERNAL_RESOLUTION (requires Neo4j)."""
    
    def test_indexing_with_skip_external_enabled(self):
        """Test full indexing cycle with SKIP_EXTERNAL_RESOLUTION=true."""
        # This would be an actual integration test
        # Requires Neo4j running and test Java project
        # Should verify: no external warnings, only internal CALLS created
        pytest.skip("E2E test - requires Neo4j database")

    def test_performance_improvement(self):
        """Test that indexing is faster with SKIP_EXTERNAL_RESOLUTION=true."""
        # This would measure performance
        # Expected: significantly faster for Java projects with Spring/Commons
        pytest.skip("E2E test - requires Neo4j database and performance benchmarks")
