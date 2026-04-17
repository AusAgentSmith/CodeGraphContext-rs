# src/codegraphcontext/core/__init__.py
"""Core database management — Neo4j only."""
from typing import Optional

from .database import DatabaseManager


def get_database_manager(db_path: Optional[str] = None) -> DatabaseManager:
    """Return the Neo4j DatabaseManager singleton. ``db_path`` is accepted for API
    compatibility with previous multi-backend callers but is ignored — Neo4j is a
    remote service, so the connection is governed by ``NEO4J_URI`` / ``NEO4J_USERNAME``
    / ``NEO4J_PASSWORD``."""
    return DatabaseManager()


__all__ = ["DatabaseManager", "get_database_manager"]
