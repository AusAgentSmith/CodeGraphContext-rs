# src/codegraphcontext/tools/import_extractor.py
"""
This module provides a utility class for extracting package and module imports
from source code files of various programming languages.
"""
import ast
import logging
import re
from pathlib import Path
from typing import Set

import stdlibs
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def debug_log(message):
    """Write debug message to a file for development and testing."""
    debug_file = os.path.expanduser("~/mcp_debug.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(debug_file, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
        f.flush()

class ImportExtractor:
    """
    A utility class that provides methods to extract import statements from
    source code files. It supports multiple languages, using the most appropriate
    parsing technique for each.
    """

    @staticmethod
    def extract_python_imports(file_path: str) -> Set[str]:
        """
        Extracts top-level imports from a Python file using the Abstract Syntax Tree (AST).
        This is a robust method that correctly handles complex import statements.
        It ignores relative imports.
        """
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=file_path)

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        # For `import a.b.c`, we only want the top-level package `a`.
                        imports.add(alias.name.split('.')[0])
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0: 
                        # This is a relative import (e.g., `from . import foo`). Ignore it.
                        pass
                    elif node.module:
                        # For `from a.b import c`, we only want the top-level package `a`.
                        imports.add(node.module.split('.')[0])
        except Exception as e:
            logger.warning(f"Error parsing or reading Python file {file_path}: {e}")
        debug_log(f"Raw imports extracted from {file_path}: {imports}")
        return imports


    @staticmethod
    def extract_javascript_imports(file_path: str) -> Set[str]:
        """
        Extracts imports from JavaScript/TypeScript files using regular expressions.
        Handles `import`, `require`, and dynamic `import()` statements.
        """
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Regex patterns for different JS/TS import syntaxes.
            patterns = [
                r'import.*?from\s+[\'"]([^\'\"]+)[\'"]',
                r'require\s*\(\s*[\'"]([^\'\"]+)[\'"]\s*\)',
                r'import\s*\(\s*[\'"]([^\'\"]+)[\'"]\s*\)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    # For scoped packages like `@scope/pkg`, include the scope.
                    pkg_name = match.split('/')[0] if not match.startswith('@') else '/'.join(match.split('/')[:2])
                    # Ignore relative imports.
                    if not match.startswith('.'):
                        imports.add(pkg_name)
        except Exception as e:
            logger.warning(f"Error reading JavaScript file {file_path}: {e}")
        
        return imports

    @staticmethod
    def extract_java_imports(file_path: str) -> Set[str]:
        """
        Extracts imports from Java files using a regular expression.
        Captures the first two parts of the import path (e.g., `java.util`).
        """
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            pattern = r'import\s+(?:static\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)'
            matches = re.findall(pattern, content)
            
            for match in matches:
                pkg_parts = match.split('.')
                if len(pkg_parts) >= 2:
                    # Capture the top-level package, e.g., `java.util` from `java.util.List`
                    imports.add(f"{pkg_parts[0]}.{pkg_parts[1]}")
        except Exception as e:
            logger.warning(f"Error reading Java file {file_path}: {e}")
        
        return imports

    def list_imports_tool(self, path: str, language: str = 'python', recursive: bool = True):
        """
        The main tool method that orchestrates the import extraction for a given path and language.
        """
        all_imports = set()
        # Map languages to their common file extensions.
        file_extensions = {
            'python': ['.py'], 'javascript': ['.js', '.jsx', '.mjs'],
            'typescript': ['.ts', '.tsx'], 'java': ['.java'],
        }
        
        # Map languages to their specific extraction function.
        extensions = file_extensions.get(language, ['.py'])
        extract_func = {
            'python': self.extract_python_imports,
            # ... other languages
        }.get(language, self.extract_python_imports)
        
        try:
            path_obj = Path(path)
            if path_obj.is_file():
                if any(str(path_obj).endswith(ext) for ext in extensions):
                    all_imports.update(extract_func(str(path_obj)))
            elif path_obj.is_dir():
                pattern = "**/*" if recursive else "*"
                for ext in extensions:
                    for file_path in path_obj.glob(f"{pattern}{ext}"):
                        if file_path.is_file():
                            all_imports.update(extract_func(str(file_path)))
            else:
                return {"error": f"Path {path} does not exist"}

            # For Python, filter out standard library modules to find third-party dependencies.
            if language == 'python':
                stdlib_modules = set(stdlibs.module_names)
                all_imports = all_imports - stdlib_modules
            
            return {
                "imports": sorted(list(all_imports)), "language": language,
                "path": path, "count": len(all_imports)
            }
        except Exception as e:
            return {"error": f"Failed to analyze imports: {str(e)}"}
