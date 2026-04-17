"""Tree-sitter parser dispatch by language name."""

from pathlib import Path
from typing import Dict

from tree_sitter import Language, Parser

from ..utils.tree_sitter_manager import get_tree_sitter_manager


class TreeSitterParser:
    """A generic parser wrapper for a specific language using tree-sitter."""

    def __init__(self, language_name: str):
        self.language_name = language_name
        self.ts_manager = get_tree_sitter_manager()

        self.language: Language = self.ts_manager.get_language_safe(language_name)
        self.parser = Parser(self.language)

        self.language_specific_parser = None
        if self.language_name == "python":
            from .languages.python import PythonTreeSitterParser

            self.language_specific_parser = PythonTreeSitterParser(self)
        elif self.language_name == "rust":
            from .languages.rust import RustTreeSitterParser

            self.language_specific_parser = RustTreeSitterParser(self)

    def parse(self, path: Path, is_dependency: bool = False, **kwargs) -> Dict:
        """Dispatches parsing to the language-specific parser."""
        if self.language_specific_parser:
            return self.language_specific_parser.parse(path, is_dependency, **kwargs)
        raise NotImplementedError(f"No language-specific parser implemented for {self.language_name}")
