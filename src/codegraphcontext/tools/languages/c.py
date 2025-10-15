from pathlib import Path
from typing import Any, Dict, Optional

from tree_sitter import QueryCursor

from codegraphcontext.utils.debug_log import warning_logger

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

        (type_definition
            type: (struct_specifier)
            declarator: (type_identifier) @name
        ) @typedef_struct
    """,
    "unions": """
        (union_specifier
            name: (type_identifier) @name
        ) @union

        (type_definition
            type: (union_specifier)
            declarator: (type_identifier) @name
        ) @typedef_union
    """,
    "enums": """
        (enum_specifier
            name: (type_identifier) @name
        ) @enum

        (type_definition
            type: (enum_specifier)
            declarator: (type_identifier) @name
        ) @typedef_enum
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
        
        (declaration
            declarator: (identifier) @name
        )
        
        (declaration
            declarator: (pointer_declarator
                declarator: (identifier) @name
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
        self.query_cursors = {
            name: QueryCursor(query)
            for name, query in self.queries.items()
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

    def _get_parent_context(
        self,
        node: Any,
        types: tuple = ('function_definition', 'struct_specifier', 'union_specifier', 'enum_specifier')
    ) -> tuple:
        """Get parent context for nested constructs."""
        curr = node.parent
        while curr:
            if curr.type in types:
                name_node = curr.child_by_field_name('name')
                if name_node:
                    return self._get_node_text(name_node), curr.type, curr.start_point[0] + 1
            curr = curr.parent
        return None, None, None

    def _calculate_complexity(self, node: Any) -> int:
        """Calculate cyclomatic complexity for C functions."""
        complexity_nodes = {
            "if_statement", "for_statement", "while_statement", "do_statement",
            "switch_statement", "case_statement", "conditional_expression",
            "logical_expression", "binary_expression", "goto_statement"
        }
        count = 1
        
        def traverse(n):
            nonlocal count
            if n.type in complexity_nodes:
                count += 1
            for child in n.children:
                traverse(child)
        
        traverse(node)
        return count

    def _get_docstring(self, node: Any) -> Optional[str]:
        """Extract comments as documentation."""
        # Look for comments before the node
        if node.parent:
            for child in node.parent.children:
                if child.type == 'comment' and child.start_point[0] < node.start_point[0]:
                    return self._get_node_text(child)
        return None

    def _parse_function_args(self, params_node: Any) -> list[Dict[str, Any]]:
        """Enhanced helper to parse function arguments from a (parameter_list) node."""
        args = []
        if not params_node:
            return args
            
        for param in params_node.named_children:
            if param.type == "parameter_declaration":
                arg_info: Dict[str, Any] = {"name": "", "type": None, "is_pointer": False, "is_array": False}
                
                # Find the declarator (variable name)
                declarator = param.child_by_field_name("declarator")
                if declarator:
                    if declarator.type == "identifier":
                        arg_info["name"] = self._get_node_text(declarator)
                    elif declarator.type == "pointer_declarator":
                        arg_info["is_pointer"] = True
                        inner_declarator = declarator.child_by_field_name("declarator")
                        if inner_declarator and inner_declarator.type == "identifier":
                            arg_info["name"] = self._get_node_text(inner_declarator)
                    elif declarator.type == "array_declarator":
                        arg_info["is_array"] = True
                        inner_declarator = declarator.child_by_field_name("declarator")
                        if inner_declarator and inner_declarator.type == "identifier":
                            arg_info["name"] = self._get_node_text(inner_declarator)
                
                # Find the type
                type_node = param.child_by_field_name("type")
                if type_node:
                    arg_info["type"] = self._get_node_text(type_node)
                
                # Handle variadic arguments
                if param.type == "variadic_parameter":
                    arg_info["name"] = "..."
                    arg_info["type"] = "variadic"
                
                args.append(arg_info)
        return args

    def _find_functions(self, root_node: Any) -> list[Dict[str, Any]]:
        functions = []
        cursor = self.query_cursors["functions"]
        for _pattern_index, captures in cursor.matches(root_node):
            for capture_name, nodes in captures.items():
                if capture_name == 'name':
                    for node in nodes:
                        func_node = node.parent.parent
                        name = self._get_node_text(node)

                        # Find parameters
                        params_node = None
                        for child in func_node.children:
                            if child.type == "function_declarator":
                                params_node = child.child_by_field_name("parameters")

                        args = self._parse_function_args(params_node) if params_node else []
                        context, context_type, _ = self._get_parent_context(func_node)

                        functions.append({
                            "name": name,
                            "line_number": node.start_point[0] + 1,
                            "end_line": func_node.end_point[0] + 1,
                            "args": [arg["name"] for arg in args if arg["name"]],  # Simplified args for compatibility
                            "source": self._get_node_text(func_node),
                            "source_code": self._get_node_text(func_node),
                            "docstring": self._get_docstring(func_node),
                            "cyclomatic_complexity": self._calculate_complexity(func_node),
                            "context": context,
                            "context_type": context_type,
                            "class_context": None,
                            "decorators": [],
                            "lang": self.language_name,
                            "is_dependency": False,
                            "detailed_args": args,  # Keep detailed args for future use
                        })
        return functions

    def _find_structs_unions_enums(self, root_node: Any) -> list[Dict[str, Any]]:
        """Find structs, unions, and enums (treated as classes in C)."""
        classes = []

        # Helper function to process captures
        def process_captures(cursor, expected_type):
            for _pattern_index, captures in cursor.matches(root_node):
                for capture_name, nodes in captures.items():
                    if capture_name == 'name':
                        for node in nodes:
                            # Determine the actual struct/union/enum node and type
                            if node.parent.type in ['struct_specifier', 'union_specifier', 'enum_specifier']:
                                # Regular named struct/union/enum
                                type_node = node.parent
                                type_name = type_node.type.replace('_specifier', '')
                            else:
                                # Typedef case - node is type_identifier, parent is type_definition
                                type_definition = node.parent
                                # Find the struct/union/enum specifier within the type_definition
                                for child in type_definition.children:
                                    if child.type in ['struct_specifier', 'union_specifier', 'enum_specifier']:
                                        type_node = child
                                        type_name = child.type.replace('_specifier', '')
                                        break
                                else:
                                    # If no specifier found, skip
                                    continue

                            name = self._get_node_text(node)
                            context, context_type, _ = self._get_parent_context(type_node)

                            classes.append({
                                "name": name,
                                "line_number": node.start_point[0] + 1,
                                "end_line": type_node.end_point[0] + 1,
                                "bases": [],  # C doesn't have inheritance
                                "source": self._get_node_text(type_node),
                                "docstring": self._get_docstring(type_node),
                                "context": context,
                                "decorators": [],
                                "lang": self.language_name,
                                "is_dependency": False,
                                "type": type_name,
                            })

        # Find structs, unions, and enums
        process_captures(self.query_cursors["structs"], "struct")
        process_captures(self.query_cursors["unions"], "union")
        process_captures(self.query_cursors["enums"], "enum")

        return classes

    def _find_imports(self, root_node: Any) -> list[Dict[str, Any]]:
        imports = []
        cursor = self.query_cursors["imports"]
        for _pattern_index, captures in cursor.matches(root_node):
            for capture_name, nodes in captures.items():
                if capture_name == 'path':
                    for node in nodes:
                        path = self._get_node_text(node).strip('"<>')
                        context, context_type, _ = self._get_parent_context(node)

                        imports.append({
                            "name": path,
                            "full_import_name": path,
                            "line_number": node.start_point[0] + 1,
                            "alias": None,
                            "context": context,
                            "lang": self.language_name,
                            "is_dependency": False,
                        })
        return imports

    def _find_calls(self, root_node: Any) -> list[Dict[str, Any]]:
        """Enhanced function call detection."""
        calls = []
        cursor = self.query_cursors["calls"]
        for _pattern_index, captures in cursor.matches(root_node):
            for capture_name, nodes in captures.items():
                if capture_name == "name":
                    for node in nodes:
                        call_node = node.parent if node.parent.type == "call_expression" else node.parent.parent
                        call_name = self._get_node_text(node)

                        # Extract arguments
                        args = []
                        args_node = call_node.child_by_field_name("arguments")
                        if args_node:
                            for child in args_node.children:
                                if child.type not in ['(', ')', ',']:
                                    args.append(self._get_node_text(child))

                        context, context_type, _ = self._get_parent_context(call_node)

                        calls.append({
                            "name": call_name,
                            "full_name": call_name,  # For C, function name is the same as full name
                            "line_number": node.start_point[0] + 1,
                            "args": args,
                            "inferred_obj_type": None,
                            "context": context,
                            "class_context": None,
                            "lang": self.language_name,
                            "is_dependency": False,
                        })
        return calls

    def _find_variables(self, root_node: Any) -> list[Dict[str, Any]]:
        """Enhanced variable declaration detection."""
        variables = []
        cursor = self.query_cursors["variables"]
        for _pattern_index, captures in cursor.matches(root_node):
            for capture_name, nodes in captures.items():
                if capture_name == "name":
                    for node in nodes:
                        var_name = self._get_node_text(node)

                        # Find the declaration node
                        decl_node = node.parent
                        while decl_node and decl_node.type != "declaration":
                            decl_node = decl_node.parent

                        # Extract type information
                        var_type = None
                        is_pointer = False
                        is_array = False
                        value = None

                        if decl_node:
                            # Find type
                            for child in decl_node.children:
                                if child.type in ["primitive_type", "type_identifier", "sized_type_specifier"]:
                                    var_type = self._get_node_text(child)
                                elif child.type == "init_declarator":
                                    # Check for pointer/array
                                    if child.child_by_field_name("declarator"):
                                        declarator = child.child_by_field_name("declarator")
                                        if declarator.type == "pointer_declarator":
                                            is_pointer = True
                                        elif declarator.type == "array_declarator":
                                            is_array = True

                                    # Check for initial value
                                    value_node = child.child_by_field_name("value")
                                    if value_node:
                                        value = self._get_node_text(value_node)

                        context, context_type, _ = self._get_parent_context(node)
                        class_context, _, _ = self._get_parent_context(
                            node, types=('struct_specifier', 'union_specifier', 'enum_specifier')
                        )

                        variables.append({
                            "name": var_name,
                            "line_number": node.start_point[0] + 1,
                            "value": value,
                            "type": var_type,
                            "context": context,
                            "class_context": class_context,
                            "lang": self.language_name,
                            "is_dependency": False,
                            "is_pointer": is_pointer,
                            "is_array": is_array,
                        })
        return variables

    def _find_macros(self, root_node: Any) -> list[Dict[str, Any]]:
        """Enhanced preprocessor macro detection."""
        macros = []
        cursor = self.query_cursors["macros"]
        for _pattern_index, captures in cursor.matches(root_node):
            for capture_name, nodes in captures.items():
                if capture_name == 'name':
                    for node in nodes:
                        macro_node = node.parent
                        name = self._get_node_text(node)

                        # Extract macro value
                        value = None
                        if macro_node.child_by_field_name("value"):
                            value = self._get_node_text(macro_node.child_by_field_name("value"))

                        # Extract parameters for function-like macros
                        params = []
                        if macro_node.child_by_field_name("parameters"):
                            params_node = macro_node.child_by_field_name("parameters")
                            for child in params_node.children:
                                if child.type == "identifier":
                                    params.append(self._get_node_text(child))

                        context, context_type, _ = self._get_parent_context(macro_node)

                        macros.append({
                            "name": name,
                            "line_number": node.start_point[0] + 1,
                            "end_line": macro_node.end_point[0] + 1,
                            "source": self._get_node_text(macro_node),
                            "value": value,
                            "params": params,
                            "context": context,
                            "lang": self.language_name,
                            "is_dependency": False,
                        })
        return macros


def pre_scan_c(files: list[Path], parser_wrapper) -> dict:
    """Scans C files to create a map of function/struct/union/enum names to their file paths."""
    imports_map = {}
    query_str = """
        (function_definition
            declarator: (function_declarator
                declarator: (identifier) @name
            )
        )

        (function_definition
            declarator: (function_declarator
                declarator: (pointer_declarator
                    declarator: (identifier) @name
                )
            )
        )

        (struct_specifier
            name: (type_identifier) @name
        )

        (union_specifier
            name: (type_identifier) @name
        )

        (enum_specifier
            name: (type_identifier) @name
        )

        (type_definition
            declarator: (type_identifier) @name
        )

        (preproc_def
            name: (identifier) @name
        )
    """
    query = parser_wrapper.language.query(query_str)
    cursor = QueryCursor(query)

    for file_path in files:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                tree = parser_wrapper.parser.parse(bytes(f.read(), "utf8"))

            for _pattern_index, captures in cursor.matches(tree.root_node):
                for capture_name, nodes in captures.items():
                    if capture_name == 'name':
                        for node in nodes:
                            name = node.text.decode('utf-8')
                            if name not in imports_map:
                                imports_map[name] = []
                            imports_map[name].append(str(file_path.resolve()))
        except Exception as e:
            warning_logger(f"Tree-sitter pre-scan failed for {file_path}: {e}")
    return imports_map
