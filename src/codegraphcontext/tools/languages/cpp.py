from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List
import logging
import re

logger = logging.getLogger(__name__)

CPP_QUERIES = {
    "functions": """
        (function_definition
            declarator: (function_declarator
                declarator: (identifier) @name
                parameters: (parameter_list) @params
            )
            type: (_) @return_type
            body: (compound_statement) @body
        ) @function_node
    """,
    "classes": """
        (class_specifier
            name: (type_identifier) @name
            body: (field_declaration_list) @body
            (base_class_clause)? @bases
        ) @class
    """,
    "struct": """
        (struct_specifier
            name: (type_identifier) @name
            body: (field_declaration_list)? @body
        ) @struct
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
            function: [
                (identifier) @name
                (field_expression field: (field_identifier) @name)
                (qualified_identifier name: (identifier) @name)
            ]
        ) @call
    """,
    "methods": """
        (field_declaration
            declarator: (function_declarator
                declarator: (field_identifier) @name
                parameters: (parameter_list) @params
            )
            type: (_) @return_type
        ) @method
    """,
    "variables": """
        (declaration
            declarator: [
                (init_declarator declarator: (identifier) @name)
                (identifier) @name
            ]
            type: (_) @var_type
        ) @variable
    """,
    "namespaces": """
        (namespace_definition
            name: (namespace_identifier)? @name
            body: (declaration_list) @body
        ) @namespace
    """,
    "enums": """
        (enum_specifier
            name: (type_identifier)? @name
            body: (enumerator_list) @body
        ) @enum
    """,
    "templates": """
        (template_declaration
            parameters: (template_parameter_list) @params
        ) @template
    """,
    "comments": """
        [
            (comment) @comment
        ]
    """,
}

class CppTreeSitterParser:
    """A C++-specific parser using tree-sitter with enhanced detail extraction."""

    def __init__(self, generic_parser_wrapper):
        self.generic_parser_wrapper = generic_parser_wrapper
        self.language_name = "cpp"
        self.language = generic_parser_wrapper.language
        self.parser = generic_parser_wrapper.parser

        self.queries = {
            name: self.language.query(query_str)
            for name, query_str in CPP_QUERIES.items()
        }

    def _get_node_text(self, node) -> str:
        """Extract text from a tree-sitter node."""
        return node.text.decode('utf-8')

    def _extract_docstring(self, node, source_lines: List[str]) -> Optional[str]:
        """Extract documentation comment preceding a node."""
        if node.start_point[0] == 0:
            return None
        
        line_idx = node.start_point[0] - 1
        doc_lines = []
        while line_idx >= 0:
            line = source_lines[line_idx].strip()
            if line.startswith('///') or line.startswith('*') or line.startswith('/*'):
                doc_lines.insert(0, line)
                line_idx -= 1
            elif line.endswith('*/'):
                doc_lines.insert(0, line)
                line_idx -= 1
                # Continue until we find the start
                while line_idx >= 0 and not source_lines[line_idx].strip().startswith('/*'):
                    doc_lines.insert(0, source_lines[line_idx].strip())
                    line_idx -= 1
                if line_idx >= 0:
                    doc_lines.insert(0, source_lines[line_idx].strip())
                break
            else:
                break
        
        return '\n'.join(doc_lines) if doc_lines else None

    def parse(self, file_path: Path, is_dependency: bool = False) -> Dict:
        """Parses a C++ file and returns its structure with enhanced details."""
        with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
            source_code = f.read()

        source_lines = source_code.splitlines()
        tree = self.parser.parse(bytes(source_code, "utf8"))
        root_node = tree.root_node

        functions = self._find_functions(root_node, source_lines)
        classes = self._find_classes(root_node, source_lines)
        variables = self._find_variables(root_node, source_lines)
        imports = self._find_imports(root_node)
        function_calls = self._find_function_calls(root_node)
        namespaces = self._find_namespaces(root_node)
        enums = self._find_enums(root_node)
        
        return {
            "file_path": str(file_path),
            "functions": functions,
            "classes": classes,
            "variables": variables,
            "imports": imports,
            "function_calls": function_calls,
            "namespaces": namespaces,
            "enums": enums,
            "is_dependency": is_dependency,
            "lang": self.language_name,
        }

    def _find_functions(self, root_node, source_lines: List[str]) -> List[Dict]:
        """Extract function definitions with parameters, return types, and docstrings."""
        functions = []
        query = self.queries['functions']
        
        func_nodes = {}
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            if capture_name == 'function_node':
                func_nodes[id(node)] = {'node': node}

        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                func_node = node.parent.parent.parent
                func_id = id(func_node)
                
                if func_id not in func_nodes:
                    func_nodes[func_id] = {'node': func_node}
                
                name = self._get_node_text(node)

                params = []
                param_node = node.parent.child_by_field_name('parameters')
                if param_node:
                    params = self._extract_parameters(param_node)
                
                return_type = "void"
                for sibling in func_node.children:
                    if sibling.type not in ['function_declarator', 'compound_statement']:
                        return_type = self._get_node_text(sibling).strip()
                        break
                
                modifiers = self._extract_function_modifiers(func_node)
                docstring = self._extract_docstring(func_node, source_lines)
                
                body_calls = []
                body_node = func_node.child_by_field_name('body')
                if body_node:
                    body_calls = self._find_function_calls(body_node)
                
                functions.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": func_node.end_point[0] + 1,
                    "source_code": self._get_node_text(func_node),
                    "args": params,
                    "return_type": return_type,
                    "modifiers": modifiers,
                    "docstring": docstring,
                    "calls": body_calls,
                    "complexity": self._calculate_complexity(func_node),
                })
        
        return functions

    def _extract_parameters(self, param_list_node) -> List[Dict]:
        """Extract parameter details from a parameter list."""
        params = []
        
        for child in param_list_node.children:
            if child.type == 'parameter_declaration':
                param_type = ""
                param_name = ""
                default_value = None
                
                for subchild in child.children:
                    if subchild.type in ['type_identifier', 'primitive_type', 'qualified_identifier']:
                        param_type = self._get_node_text(subchild)
                    elif subchild.type == 'identifier':
                        param_name = self._get_node_text(subchild)
                    elif subchild.type == 'pointer_declarator':
                        param_type += "*"
                        for ptr_child in subchild.children:
                            if ptr_child.type == 'identifier':
                                param_name = self._get_node_text(ptr_child)
                    elif subchild.type == 'reference_declarator':
                        param_type += "&"
                        for ref_child in subchild.children:
                            if ref_child.type == 'identifier':
                                param_name = self._get_node_text(ref_child)
                    elif subchild.type == 'optional_parameter_declaration':
                        default_value = self._get_node_text(subchild)
                
                if param_type or param_name:
                    params.append({
                        "name": param_name or "unnamed",
                        "type": param_type.strip(),
                        "default": default_value,
                    })
        
        return params

    def _extract_function_modifiers(self, func_node) -> List[str]:
        """Extract function modifiers like static, inline, virtual, const."""
        modifiers = []
        source = self._get_node_text(func_node)
        
        modifier_keywords = ['static', 'inline', 'virtual', 'explicit', 'constexpr', 'const', 'override', 'final']
        for keyword in modifier_keywords:
            if re.search(r'\b' + keyword + r'\b', source):
                modifiers.append(keyword)
        
        return modifiers

    def _find_classes(self, root_node, source_lines: List[str]) -> List[Dict]:
        """Extract class definitions with methods, members, and inheritance."""
        classes = []
        query = self.queries['classes']
        
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                class_node = node.parent
                name = self._get_node_text(node)
                bases = self._extract_base_classes(class_node)
                methods = self._extract_class_methods(class_node, source_lines)
                members = self._extract_class_members(class_node)
                docstring = self._extract_docstring(class_node, source_lines)
                is_template = class_node.parent.type == 'template_declaration'
                
                classes.append({
                    "name": name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": class_node.end_point[0] + 1,
                    "source_code": self._get_node_text(class_node),
                    "bases": bases,
                    "methods": methods,
                    "members": members,
                    "docstring": docstring,
                    "is_template": is_template,
                })
        
        return classes

    def _extract_base_classes(self, class_node) -> List[str]:
        """Extract base class names from inheritance."""
        bases = []
        
        for child in class_node.children:
            if child.type == 'base_class_clause':
                for subchild in child.children:
                    if subchild.type in ['type_identifier', 'qualified_identifier']:
                        bases.append(self._get_node_text(subchild))
        
        return bases

    def _extract_class_methods(self, class_node, source_lines: List[str]) -> List[Dict]:
        """Extract methods from a class body."""
        methods = []
        query = self.queries['methods']
        
        body_node = class_node.child_by_field_name('body')
        if not body_node:
            return methods
        
        for match in query.captures(body_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                method_name = self._get_node_text(node)
                method_node = node.parent.parent
    
                params = []
                param_node = node.parent.child_by_field_name('parameters')
                if param_node:
                    params = self._extract_parameters(param_node)
                
                return_type = "void"
                for sibling in method_node.children:
                    if sibling.type in ['type_identifier', 'primitive_type']:
                        return_type = self._get_node_text(sibling).strip()
                        break

                access_level = self._get_access_level(method_node, body_node)
                
                methods.append({
                    "name": method_name,
                    "line_number": node.start_point[0] + 1,
                    "params": params,
                    "return_type": return_type,
                    "access": access_level,
                })
        
        return methods

    def _extract_class_members(self, class_node) -> List[Dict]:
        """Extract member variables from a class."""
        members = []
        
        body_node = class_node.child_by_field_name('body')
        if not body_node:
            return members
        
        for child in body_node.children:
            if child.type == 'field_declaration':
                member_type = ""
                member_name = ""
                
                for subchild in child.children:
                    if subchild.type in ['type_identifier', 'primitive_type']:
                        member_type = self._get_node_text(subchild)
                    elif subchild.type == 'field_declarator':
                        for field_child in subchild.children:
                            if field_child.type == 'field_identifier':
                                member_name = self._get_node_text(field_child)
                
                if member_name:
                    access_level = self._get_access_level(child, body_node)
                    members.append({
                        "name": member_name,
                        "type": member_type,
                        "access": access_level,
                        "line_number": child.start_point[0] + 1,
                    })
        
        return members

    def _get_access_level(self, node, body_node) -> str:
        """Determine the access level (public/private/protected) of a class member."""
        default_access = "private"
        current_access = default_access
        for child in body_node.children:
            if child.start_point[0] > node.start_point[0]:
                break
            if child.type == 'access_specifier':
                access_text = self._get_node_text(child).strip(':').strip()
                current_access = access_text
        
        return current_access

    def _find_variables(self, root_node, source_lines: List[str]) -> List[Dict]:
        """Extract global variable declarations."""
        variables = []
        query = self.queries['variables']
        
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name' and node.parent.parent.parent.type == 'translation_unit':
                var_name = self._get_node_text(node)
                var_node = node.parent.parent
                
                var_type = ""
                for child in var_node.children:
                    if child.type in ['type_identifier', 'primitive_type']:
                        var_type = self._get_node_text(child)
                        break
                
                variables.append({
                    "name": var_name,
                    "type": var_type,
                    "line_number": node.start_point[0] + 1,
                })
        
        return variables

    def _find_imports(self, root_node) -> List[Dict]:
        """Extract #include directives."""
        imports = []
        query = self.queries['imports']
        
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'path':
                path = self._get_node_text(node).strip('"<>')
                is_system = '<' in self._get_node_text(node.parent)
                
                imports.append({
                    "name": path,
                    "full_import_name": path,
                    "line_number": node.start_point[0] + 1,
                    "alias": None,
                    "is_system": is_system,
                })
        
        return imports

    def _find_function_calls(self, root_node) -> List[Dict]:
        """Extract function calls from the AST."""
        calls = []
        query = self.queries['calls']
        
        seen_calls = set()
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                call_name = self._get_node_text(node)
                line_num = node.start_point[0] + 1
                call_key = (call_name, line_num)
                if call_key not in seen_calls:
                    seen_calls.add(call_key)
                    calls.append({
                        "name": call_name,
                        "line_number": line_num,
                    })
        
        return calls

    def _find_namespaces(self, root_node) -> List[Dict]:
        """Extract namespace definitions."""
        namespaces = []
        query = self.queries['namespaces']
        
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                ns_name = self._get_node_text(node)
                ns_node = node.parent
                
                namespaces.append({
                    "name": ns_name,
                    "line_number": node.start_point[0] + 1,
                    "end_line": ns_node.end_point[0] + 1,
                })
        
        return namespaces

    def _find_enums(self, root_node) -> List[Dict]:
        """Extract enum definitions."""
        enums = []
        query = self.queries['enums']
        
        for match in query.captures(root_node):
            capture_name = match[1]
            node = match[0]
            
            if capture_name == 'name':
                enum_name = self._get_node_text(node)
                enum_node = node.parent
                values = []
                body_node = enum_node.child_by_field_name('body')
                if body_node:
                    for child in body_node.children:
                        if child.type == 'enumerator':
                            enum_val_name = None
                            for subchild in child.children:
                                if subchild.type == 'identifier':
                                    enum_val_name = self._get_node_text(subchild)
                                    values.append(enum_val_name)
                                    break
                
                enums.append({
                    "name": enum_name,
                    "line_number": node.start_point[0] + 1,
                    "values": values,
                })
        
        return enums

    def _calculate_complexity(self, func_node) -> int:
        """Calculate cyclomatic complexity of a function."""
        complexity = 1
        decision_keywords = ['if', 'while', 'for', 'case', '&&', '||', '?']
        source = self._get_node_text(func_node)
        
        for keyword in decision_keywords:
            if keyword in ['&&', '||', '?']:
                complexity += source.count(keyword)
            else:
                complexity += len(re.findall(r'\b' + keyword + r'\b', source))
        
        return complexity