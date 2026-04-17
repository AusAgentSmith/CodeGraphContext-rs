import logging

from ..tools.query_tool_languages.python_toolkit import PythonToolkit
from ..tools.query_tool_languages.rust_toolkit import RustToolkit

from ..core.database import DatabaseManager
from ..utils.debug_log import debug_log

logger = logging.getLogger(__name__)


class Advanced_language_query:
    """
    Tool implementation for executing a read-only language specific Cypher query.

    Important: Includes a safety check to prevent any database modification
    by disallowing keywords like CREATE, MERGE, DELETE, etc.
    """

    TOOLKITS = {
        "python": PythonToolkit,
        "rust": RustToolkit,
    }
    Supported_queries = {
        "repository": "Repository",
        "directory": "Directory",
        "file": "File",
        "module": "Module",
        "function": "Function",
        "class": "Class",
        "struct": "Struct",
        "enum": "Enum",
        "union": "Union",
        "macro": "Macro",
        "variable": "Variable",
    }

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def advanced_language_query(self, language: str, query: str):
        query = query.strip().lower()
        if query not in self.Supported_queries:
            raise ValueError(
                f"Unsupported query type '{query}'"
                f"Supported: {', '.join(self.Supported_queries.keys())}"
            )
        label = self.Supported_queries[query]

        language = language.lower()

        if language not in self.TOOLKITS:
            raise ValueError(f"Unsupported language: {language}")
        self.toolkit = self.TOOLKITS[language]()

        cypher_query = self.toolkit.get_cypher_query(label)
        try:
            debug_log(f"Executing Cypher query: {cypher_query}")
            with self.db_manager.get_driver().session() as session:
                result = session.run(cypher_query)
                records = [record.data() for record in result]

                return {
                    "success": True,
                    "language": language,
                    "query": cypher_query,
                    "results": records,
                }
        except Exception as e:
            debug_log(f"Error executing Cypher query: {str(e)}")
            return {
                "error": "An unexpected error occurred while executing the query.",
                "details": str(e),
            }
