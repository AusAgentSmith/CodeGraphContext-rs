"""
Tests for database configuration validation and error handling.
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports (needed for direct test execution)
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from codegraphcontext.core.database import DatabaseManager


class TestConfigValidation:
    """Tests for validate_config method"""
    
    def test_validate_config_valid_neo4j_uri(self):
        """Test validation passes for valid neo4j:// URI"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_valid_bolt_uri(self):
        """Test validation passes for valid bolt:// URI"""
        is_valid, error = DatabaseManager.validate_config(
            "bolt://localhost:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_valid_secure_uri(self):
        """Test validation passes for valid neo4j+s:// URI"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j+s://localhost:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_invalid_uri_no_protocol(self):
        """Test validation fails for URI without protocol"""
        is_valid, error = DatabaseManager.validate_config(
            "localhost:7687", "neo4j", "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Invalid Neo4j URI format" in error
        assert "neo4j://" in error or "bolt://" in error
    
    def test_validate_config_invalid_uri_no_port(self):
        """Test validation fails for URI without port"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost", "neo4j", "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Invalid Neo4j URI format" in error
    
    def test_validate_config_invalid_uri_wrong_protocol(self):
        """Test validation fails for invalid protocol"""
        is_valid, error = DatabaseManager.validate_config(
            "http://localhost:7687", "neo4j", "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Invalid Neo4j URI format" in error
    
    def test_validate_config_empty_username(self):
        """Test validation fails for empty username"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "", "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Username cannot be empty" in error
        assert "neo4j" in error  # Should mention default username
    
    def test_validate_config_whitespace_username(self):
        """Test validation fails for whitespace-only username"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "   ", "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Username cannot be empty" in error
    
    def test_validate_config_none_username(self):
        """Test validation fails for None username"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", None, "password123"
        )
        assert is_valid is False
        assert error is not None
        assert "Username cannot be empty" in error
    
    def test_validate_config_empty_password(self):
        """Test validation fails for empty password"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "neo4j", ""
        )
        assert is_valid is False
        assert error is not None
        assert "Password cannot be empty" in error
    
    def test_validate_config_whitespace_password(self):
        """Test validation fails for whitespace-only password"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "neo4j", "   "
        )
        assert is_valid is False
        assert error is not None
        assert "Password cannot be empty" in error
    
    def test_validate_config_none_password(self):
        """Test validation fails for None password"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "neo4j", None
        )
        assert is_valid is False
        assert error is not None
        assert "Password cannot be empty" in error
    
    def test_validate_config_valid_with_special_chars_in_password(self):
        """Test validation passes for password with special characters"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7687", "neo4j", "p@ssw0rd!#$%"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_valid_custom_port(self):
        """Test validation passes for custom port"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://localhost:7688", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_valid_remote_host(self):
        """Test validation passes for remote host"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://example.com:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_config_valid_ip_address(self):
        """Test validation passes for IP address"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j://192.168.1.100:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None


class TestConnectionTest:
    """Tests for test_connection method"""
    
    def test_connection_invalid_host_unreachable(self):
        """Test connection fails gracefully for unreachable host"""
        is_connected, error = DatabaseManager.test_connection(
            "neo4j://unreachable-host-12345.invalid:7687", "neo4j", "password123"
        )
        assert is_connected is False
        assert error is not None
        assert "Cannot reach Neo4j server" in error or "Connection failed" in error
    
    def test_connection_invalid_port(self):
        """Test connection fails for invalid port"""
        is_connected, error = DatabaseManager.test_connection(
            "neo4j://localhost:9999", "neo4j", "password123"
        )
        assert is_connected is False
        assert error is not None
        # Should mention connectivity or unavailability
        assert any(keyword in error for keyword in ["reach", "unavailable", "failed", "Connection"])
    
    def test_connection_provides_troubleshooting_tips(self):
        """Test that error messages include troubleshooting tips"""
        is_connected, error = DatabaseManager.test_connection(
            "neo4j://localhost:9999", "neo4j", "password123"
        )
        assert is_connected is False
        assert error is not None
        # Should provide helpful troubleshooting
        assert any(keyword in error.lower() for keyword in ["troubleshoot", "check", "docker", "try"])


class TestValidationIntegration:
    """Integration tests for validation in real-world scenarios"""
    
    def test_validate_aura_style_uri(self):
        """Test validation works for Neo4j Aura-style URIs"""
        is_valid, error = DatabaseManager.validate_config(
            "neo4j+s://abc123.databases.neo4j.io:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_validate_bolt_ssc_uri(self):
        """Test validation works for bolt+ssc:// URIs"""
        is_valid, error = DatabaseManager.validate_config(
            "bolt+ssc://localhost:7687", "neo4j", "password123"
        )
        assert is_valid is True
        assert error is None
    
    def test_multiple_validation_errors_at_once(self):
        """Test validation catches the first error in priority order"""
        # URI is invalid, username empty, password empty - should catch URI first
        is_valid, error = DatabaseManager.validate_config(
            "invalid-uri", "", ""
        )
        assert is_valid is False
        assert "Invalid Neo4j URI format" in error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
