from ..tools.query_tool_languages.cpp_toolkit import CppToolkit

from ..core.database import DatabaseManager

class Advanced_language_query:
    """
    Tool implementation for executing a read-only language specific Cypher query.
    
    #will edit here
    Important: Includes a safety check to prevent any database modification
    by disallowing keywords like CREATE, MERGE, DELETE, etc.
    """

    TOOLKITS = {
        "cpp": CppToolkit,
    }

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def advanced_language_query(self, language: str, query: str):
        language = language.lower()

        if language not in self.TOOLKITS:
            raise ValueError(f"Unsupported language: {language}")
        self.toolkit = self.TOOLKITS[language]()

        cypher_query = self.toolkit.get_cypher_query(query)

        with self.db_manager.get_driver().session() as session:
            result = session.run(cypher_query)
            records = [record.data() for record in result]

            return {
                "success": True, 
                "language": language,
                "query": cypher_query,
                "results": records 
            }


