from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import logging
import re

logger = logging.getLogger(__name__)

C_QUERIES = {
    "functions": """
        (function_definition
            declarator: (function_declarator
                declarator: (identifier) @name
            )
        ) @function_node
        
        (function_definition
            declarator: (function_declarator
                declarator: (pointer_declarator
                    declarator: (identifier) @name
                )
            )
        ) @function_node
    """,
    "structs": """
        (struct_specifier
            name: (type_identifier) @name
        ) @struct
    """,
    "unions": """
        (union_specifier
            name: (type_identifier) @name
        ) @union
    """,
    "enums": """
        (enum_specifier
            name: (type_identifier) @name
        ) @enum
    """,
    "typedefs": """
        (type_definition
            declarator: (type_identifier) @name
        ) @typedef
    """,
    "imports": """
        (preproc_include
            path: [
                (string_literal) @path
                (system_lib_string) @path
            ]
        ) @import
    """,
    "calls": """
        (call_expression
            function: (identifier) @name
        )
    """,
    "variables": """
        (declaration
            declarator: (init_declarator
                declarator: (identifier) @name
            )
        )
        
        (declaration
            declarator: (init_declarator
                declarator: (pointer_declarator
                    declarator: (identifier) @name
                )
            )
        )
    """,
    "macros": """
        (preproc_def
            name: (identifier) @name
        ) @macro
    """,
}

class CTreeSitterParser:
    """A C-specific parser using tree-sitter."""

    def __init__(self, generic_parser_wrapper: Any):
        self.generic_parser_wrapper = generic_parser_wrapper
        self.language_name = "c"
        self.language = generic_parser_wrapper.language
        self.parser = generic_parser_wrapper.parser

        self.queries = {
            name: self.language.query(query_str)
            for name, query_str in C_QUERIES.items()
        }

    def _get_node_text(self, node: Any) -> str:
        return node.text.decode("utf-8")

    def parse(self, file_path: Path, is_dependency: bool = False) -> Dict[str, Any]:
        """Parses a C file and returns its structure."""
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            source_code = f.read()

        tree = self.parser.parse(bytes(source_code, "utf8"))
        root_node = tree.root_node

        functions = self._find_functions(root_node)
        classes = self._find_structs_unions_enums(root_node)
        imports = self._find_imports(root_node)
        function_calls = self._find_calls(root_node)
        variables = self._find_variables(root_node)
        macros = self._find_macros(root_node)

        return {
            "file_path": str(file_path),
            "functions": functions,
            "classes": classes,
            "variables": variables,
            "imports": imports,
            "function_calls": function_calls,
            "macros": macros,
            "is_dependency": is_dependency,
            "lang": self.language_name,
        }

    def _parse_function_args(self, params_node: Any) -> list[Dict[str, Any]]:
        """Helper to parse function arguments from a (parameter_list) node."""
        args = []
        if not params_node:
            return args
            
        for param in params_node.named_children:
            if param.type == "parameter_declaration":
                arg_info: Dict[str, Any] = {"name": "", "type": None}
                
                # Find the declarator (variable name)
                declarator = param.child_by_field_name("declarator")
                if declarator:
                    if declarator.type == "identifier":
                        arg_info["name"] = self._get_node_text(declarator)
                    elif declarator.type == "pointer_declarator":
                        inner_declarator = declarator.child_by_field_name("declarator")
                        if inner_declarator and inner_declarator.type == "identifier":
                            arg_info["name"] = self._get_node_text(inner_declarator)
                
                # Find the type
                type_node = param.child_by_field_name("type")
                if type_node:
                    arg_info["type"] = self._get_node_text(type_node)
                
                args.append(arg_info)
        return args

    def _find_functions(self, root_node: Any) -> list[Dict[str, Any]]:
        functions = []
        query = self.queries["functions"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                func_node = node.parent.parent.parent
                name = self._get_node_text(node)
                
                # Find parameters
                params_node = None
                for child in func_node.children:
                    if child.type == "function_declarator":
                        params_node = child.child_by_field_name("parameters")
                        break
                
                args = self._parse_function_args(params_node) if params_node else []

                functions.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": func_node.end_point[0] + 1,
                    "source_code": self._get_node_text(func_node),
                    "args": args,
                })
        return functions

    def _find_structs_unions_enums(self, root_node: Any) -> list[Dict[str, Any]]:
        """Find structs, unions, and enums (treated as classes in C)."""
        classes = []
        
        # Find structs
        query = self.queries["structs"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                struct_node = node.parent
                name = self._get_node_text(node)
                classes.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": struct_node.end_point[0] + 1,
                    "source_code": self._get_node_text(struct_node),
                    "bases": [],  # C doesn't have inheritance
                    "type": "struct",
                })

        # Find unions
        query = self.queries["unions"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                union_node = node.parent
                name = self._get_node_text(node)
                classes.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": union_node.end_point[0] + 1,
                    "source_code": self._get_node_text(union_node),
                    "bases": [],
                    "type": "union",
                })

        # Find enums
        query = self.queries["enums"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                enum_node = node.parent
                name = self._get_node_text(node)
                classes.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": enum_node.end_point[0] + 1,
                    "source_code": self._get_node_text(enum_node),
                    "bases": [],
                    "type": "enum",
                })

        return classes

    def _find_imports(self, root_node: Any) -> list[Dict[str, Any]]:
        imports = []
        query = self.queries["imports"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'path':
                path = self._get_node_text(node).strip('"<>')
                imports.append({
                    "name": path,
                    "full_import_name": path,
                    "line_number": node.start_point[0] + 1,
                    "alias": None,
                })
        return imports

    def _find_calls(self, root_node: Any) -> list[Dict[str, Any]]:
        """Finds all function calls."""
        calls = []
        query = self.queries["calls"]
        for node, capture_name in query.captures(root_node):
            if capture_name == "name":
                call_name = self._get_node_text(node)
                calls.append({
                    "name": call_name,
                    "full_name": call_name,  # For C, function name is the same as full name
                    "line_number": node.start_point[0] + 1,
                })
        return calls

    def _find_variables(self, root_node: Any) -> list[Dict[str, Any]]:
        """Finds variable declarations."""
        variables = []
        query = self.queries["variables"]
        for node, capture_name in query.captures(root_node):
            if capture_name == "name":
                var_name = self._get_node_text(node)
                variables.append({
                    "name": var_name,
                    "line_number": node.start_point[0] + 1,
                })
        return variables

    def _find_macros(self, root_node: Any) -> list[Dict[str, Any]]:
        """Finds preprocessor macros."""
        macros = []
        query = self.queries["macros"]
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'name':
                macro_node = node.parent
                name = self._get_node_text(node)
                macros.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": macro_node.end_point[0] + 1,
                    "source_code": self._get_node_text(macro_node),
                })
        return macros
