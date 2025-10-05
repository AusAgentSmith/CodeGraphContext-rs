# src/codegraphcontext/tools/package_resolver.py
import importlib
import stdlibs
from pathlib import Path
import subprocess
from typing import Optional

from ..utils.debug_log import debug_log

def _get_python_package_path(package_name: str) -> Optional[str]:
    """
    Finds the local installation path of a Python package.
    """
    try:
        debug_log(f"Getting local path for Python package: {package_name}")
        module = importlib.import_module(package_name)
        if hasattr(module, '__file__') and module.__file__:
            module_file = Path(module.__file__)
            if module_file.name == '__init__.py':
                return str(module_file.parent)
            elif package_name in stdlibs.module_names:
                return str(module_file)
            else:
                return str(module_file.parent)
        elif hasattr(module, '__path__'):
            if isinstance(module.__path__, list) and module.__path__:
                return str(Path(module.__path__[0]))
            else:
                return str(Path(str(module.__path__)))
        return None
    except ImportError:
        return None
    except Exception as e:
        debug_log(f"Error getting local path for {package_name}: {e}")
        return None

def _get_npm_package_path(package_name: str) -> Optional[str]:
    """
    Finds the local installation path of a Node.js package using `npm root`.
    """
    try:
        debug_log(f"Getting local path for npm package: {package_name}")
        local_path = Path(f"./node_modules/{package_name}")
        if local_path.exists():
            return str(local_path.resolve())

        result = subprocess.run(["npm", "root", "-g"], capture_output=True, text=True)
        if result.returncode == 0:
            global_root = result.stdout.strip()
            package_path = Path(global_root) / package_name
            if package_path.exists():
                return str(package_path.resolve())
        return None
    except Exception as e:
        debug_log(f"Error getting npm package path for {package_name}: {e}")
        return None

def _get_c_package_path(package_name: str) -> Optional[str]:
    """
    Finds the local installation path of a C package.
    """
    try:
        debug_log(f"Getting local path for C package: {package_name}")
        
        # Try using pkg-config to find the package
        try:
            result = subprocess.run(
                ["pkg-config", "--variable=includedir", package_name],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                include_dir = Path(result.stdout.strip())
                package_path = include_dir / package_name
                if package_path.exists():
                    return str(package_path.resolve())
                if include_dir.exists():
                    return str(include_dir.resolve())
        except (subprocess.TimeoutExpired, FileNotFoundError):
            debug_log(f"pkg-config not available or timed out for {package_name}")
        
        # Search in standard system include directories
        common_include_paths = [
            "/usr/include",
            "/usr/local/include",
            "/opt/homebrew/include",
            "/opt/local/include",
            Path.home() / ".local" / "include",
        ]
        
        for base_path in common_include_paths:
            base_path = Path(base_path)
            if not base_path.exists():
                continue
            
            # Check if package exists as a directory
            package_dir = base_path / package_name
            if package_dir.exists() and package_dir.is_dir():
                return str(package_dir.resolve())
            
            # Check for header files with package name
            header_file = base_path / f"{package_name}.h"
            if header_file.exists():
                return str(header_file.resolve())
        
        # Check current directory for local installations
        local_package = Path(f"./{package_name}")
        if local_package.exists():
            return str(local_package.resolve())
        
        return None
    except Exception as e:
        debug_log(f"Error getting C package path for {package_name}: {e}")
        return None

def get_local_package_path(package_name: str, language: str) -> Optional[str]:
    """
    Dispatches to the correct package path finder based on the language.
    """
    finders = {
        "python": _get_python_package_path,
        "javascript": _get_npm_package_path,
        "c": _get_c_package_path,
    }
    finder = finders.get(language)
    if finder:
        return finder(package_name)
    return None
