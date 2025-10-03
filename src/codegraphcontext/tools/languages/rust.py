
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import logging
import re

logger = logging.getLogger(__name__)

RUST_QUERIES = {
    "functions": """
        (function_item
            name: (identifier) @name
        ) @function_node
    """,
    "classes": """
        (struct_item
            name: (type_identifier) @name
        ) @class
    """,
    "imports": """
        (use_declaration) @import
    """,
    "calls": """
        (call_expression
            function: (identifier) @name
        )
    """,
}

class RustTreeSitterParser:
    """A Rust-specific parser using tree-sitter."""

    def __init__(self, generic_parser_wrapper):
        self.generic_parser_wrapper = generic_parser_wrapper
        self.language_name = "rust"
        self.language = generic_parser_wrapper.language
        self.parser = generic_parser_wrapper.parser

        self.queries = {
            name: self.language.query(query_str)
            for name, query_str in RUST_QUERIES.items()
        }

    def _get_node_text(self, node) -> str:
        return node.text.decode('utf-8')

    def parse(self, file_path: Path, is_dependency: bool = False) -> Dict:
        """Parses a Rust file and returns its structure."""
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            source_code = f.read()

        tree = self.parser.parse(bytes(source_code, "utf8"))
        root_node = tree.root_node

        functions = self._find_functions(root_node)
        classes = self._find_structs(root_node) # In Rust, structs are like classes
        imports = self._find_imports(root_node)
        
        return {
            "file_path": str(file_path),
            "functions": functions,
            "classes": classes,
            "variables": [],  # Placeholder
            "imports": imports,
            "function_calls": [],  # Placeholder
            "is_dependency": is_dependency,
            "lang": self.language_name,
        }

    def _find_functions(self, root_node):
        functions = []
        query = self.queries['functions']
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                func_node = node.parent
                name = self._get_node_text(node)
                functions.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": func_node.end_point[0] + 1,
                    "source_code": self._get_node_text(func_node),
                    "args": [], # Placeholder
                })
        return functions

    def _find_structs(self, root_node):
        structs = []
        query = self.queries['classes']
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                class_node = node.parent
                name = self._get_node_text(node)
                structs.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": class_node.end_point[0] + 1,
                    "source_code": self._get_node_text(class_node),
                    "bases": [], # Placeholder
                })
        return structs

    def _find_imports(self, root_node):
        imports = []
        query = self.queries['imports']
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'import':
                path = self._get_node_text(node)
                imports.append({
                    "name": path,
                    "full_import_name": path,
                    "line_number": node.start_point[0] + 1,
                    "alias": None,
                })
        return imports
