# src/codegraphcontext/tools/import_extractor.py
import ast
import logging
import re
from pathlib import Path
from typing import Set

import stdlibs

logger = logging.getLogger(__name__)

class ImportExtractor:
    """Module for extracting imports from different programming languages"""

    @staticmethod
    def extract_python_imports(file_path: str) -> Set[str]:
        """Extract imports from a Python file using AST parsing."""
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                tree = ast.parse(f.read(), filename=file_path)

            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name.split('.')[0]) # Get top-level package
                elif isinstance(node, ast.ImportFrom):
                    if node.level > 0: # Relative import
                        # As per the user's request, do not list relative imports
                        pass
                    elif node.module:
                        imports.add(node.module.split('.')[0]) # Get top-level package
        except Exception as e:
            logger.warning(f"Error parsing or reading {file_path}: {e}")
        return imports


    @staticmethod
    def extract_javascript_imports(file_path: str) -> Set[str]:
        """Extract imports from JavaScript/TypeScript files"""
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            patterns = [
                r'import.*?from\s+[\'"]([^\'\"]+)[\'"]',
                r'require\s*\(\s*[\'"]([^\'\"]+)[\'"]\s*\)',
                r'import\s*\(\s*[\'"]([^\'\"]+)[\'"]\s*\)',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, content)
                for match in matches:
                    pkg_name = match.split('/')[0] if not match.startswith('@') else '/'.join(match.split('/')[:2])
                    if not match.startswith('.'):
                        imports.add(pkg_name)
        except Exception as e:
            logger.warning(f"Error reading {file_path}: {e}")
        
        return imports

    @staticmethod
    def extract_java_imports(file_path: str) -> Set[str]:
        """Extract imports from Java files"""
        imports = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            pattern = r'import\s+(?:static\s+)?([a-zA-Z_][a-zA-Z0-9_.]*)'
            matches = re.findall(pattern, content)
            
            for match in matches:
                pkg_parts = match.split('.')
                if len(pkg_parts) >= 2:
                    imports.add(f"{pkg_parts[0]}.{pkg_parts[1]}")
        except Exception as e:
            logger.warning(f"Error reading {file_path}: {e}")
        
        return imports

    def list_imports_tool(self, path: str, language: str = 'python', recursive: bool = True):
        """Tool to list all imports from code files"""
        all_imports = set()
        file_extensions = {
            'python': ['.py'], 'javascript': ['.js', '.jsx', '.mjs'],
            'typescript': ['.ts', '.tsx'], 'java': ['.java'],
        }
        
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

            if language == 'python':
                stdlib_modules = set(stdlibs.module_names)
                all_imports = all_imports - stdlib_modules
            
            return {
                "imports": sorted(list(all_imports)), "language": language,
                "path": path, "count": len(all_imports)
            }
        except Exception as e:
            return {"error": f"Failed to analyze imports: {str(e)}"}