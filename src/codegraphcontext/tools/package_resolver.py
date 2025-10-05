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

def _get_java_package_path(package_name: str) -> Optional[str]:
    """
    Finds the local installation path of a Java package (JAR).
    Searches in Maven and Gradle cache directories.
    
    Args:
        package_name: Package name in format "groupId:artifactId" (e.g., "com.google.code.gson:gson")
                     or just "artifactId" for simple search.
    """
    try:
        debug_log(f"Getting local path for Java package: {package_name}")
        
        # Parse package name - expect format "groupId:artifactId" or just "artifactId"
        if ':' in package_name:
            group_id, artifact_id = package_name.split(':', 1)
            # Convert group_id dots to path separators (e.g., com.google.gson -> com/google/gson)
            group_path = group_id.replace('.', '/')
        else:
            # If only artifact_id provided, search for it
            artifact_id = package_name
            group_path = None
        
        search_paths = []
        
        # Maven repository (~/.m2/repository)
        maven_repo = Path.home() / ".m2" / "repository"
        if maven_repo.exists():
            if group_path:
                # Search for specific group/artifact
                package_path = maven_repo / group_path / artifact_id
                if package_path.exists():
                    # Find the latest version directory
                    version_dirs = [d for d in package_path.iterdir() if d.is_dir()]
                    if version_dirs:
                        # Sort by name (assumes semantic versioning) and get the latest
                        latest_version = sorted(version_dirs, key=lambda x: x.name)[-1]
                        return str(latest_version.resolve())
            else:
                # Search for artifact_id in the entire Maven repo
                search_paths.append(maven_repo)
        
        # Gradle cache (~/.gradle/caches/modules-2/files-2.1)
        gradle_cache = Path.home() / ".gradle" / "caches" / "modules-2" / "files-2.1"
        if gradle_cache.exists():
            if group_path:
                group_id_full = group_id if ':' in package_name else None
                if group_id_full:
                    package_path = gradle_cache / group_id_full / artifact_id
                    if package_path.exists():
                        # Find the latest version directory
                        version_dirs = [d for d in package_path.iterdir() if d.is_dir()]
                        if version_dirs:
                            latest_version = sorted(version_dirs, key=lambda x: x.name)[-1]
                            # Gradle stores files in hash subdirectories
                            hash_dirs = [d for d in latest_version.iterdir() if d.is_dir()]
                            if hash_dirs:
                                return str(hash_dirs[0].resolve())
            else:
                search_paths.append(gradle_cache)
        
        # If group_path wasn't provided or not found, search in the cache directories
        if not group_path or search_paths:
            for base_path in search_paths:
                for jar_file in base_path.rglob(f"*{artifact_id}*.jar"):
                    return str(jar_file.parent.resolve())
        
        # Check local lib directories
        local_lib_paths = [
            Path("./lib"),
            Path("./libs"),
            Path("/usr/local/lib/java"),
            Path("/opt/java/lib"),
        ]
        
        for lib_path in local_lib_paths:
            if not lib_path.exists():
                continue
            
            # Look for JAR files matching the artifact name
            for jar_file in lib_path.glob(f"*{artifact_id}*.jar"):
                return str(jar_file.resolve())
        
        return None
    except Exception as e:
        debug_log(f"Error getting Java package path for {package_name}: {e}")
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
        "java": _get_java_package_path,
        "c": _get_c_package_path,
    }
    finder = finders.get(language)
    if finder:
        return finder(package_name)
    return None
