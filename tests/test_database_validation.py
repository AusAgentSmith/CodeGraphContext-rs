# tests/test_database_validation.py
import pytest
from codegraphcontext.core.database import DatabaseManager

def test_validate_config_invalid_uri():
    is_valid, error = DatabaseManager.validate_config(
        "localhost:7687", "neo4j", "password"
    )
    assert not is_valid
    assert "Invalid Neo4j URI format" in error

def test_validate_config_empty_username():
    is_valid, error = DatabaseManager.validate_config(
        "neo4j://localhost:7687", "", "password"
    )
    assert not is_valid
    assert "Username cannot be empty" in error

def test_validate_config_valid():
    is_valid, error = DatabaseManager.validate_config(
        "neo4j://localhost:7687", "neo4j", "password"
    )
    assert is_valid
    assert error is None

if __name__ == "__main__":
    pytest.main()