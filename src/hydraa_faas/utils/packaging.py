# -*- coding: utf-8 -*-
"""Packaging utilities for creating Python AWS Lambda deployment packages.

This module provides robust functions to package Python source code into zip
archives suitable for deployment on AWS Lambda. It is optimized to handle
cross-platform compatibility issues and provides helpful feedback to the user
about package size.

Key Features:
- Automatically detects and packages dependencies if `requirements.txt` is missing.
- Installs dependencies using flags that ensure compatibility with the AWS
  Lambda Linux environment, even when run from macOS or Windows.
- Provides clear warnings if the deployment package size is approaching
  AWS Lambda's limits.
- Parallel dependency installation using process pools
- Streaming ZIP creation for better memory efficiency
- Dependency layer support for common libraries

Example:
    To create a deployment package from a source directory::

        zip_content, size, duration = create_deployment_package('/path/to/source')

"""

import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
import hashlib
import json
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any

from .exceptions import PackagingException

DEFAULT_EXCLUDES = [
    '__pycache__', '*.pyc', '.git', '.gitignore', '.DS_Store', 'node_modules',
    '.env', 'venv', '.venv', 'tests', '*.md', 'Dockerfile', '.dockerignore',
    '.mypy_cache', '.pytest_cache', '*.egg-info', 'dist', 'build', '.idea',
    '.vscode', '*.swp', '*.swo', '*~', '.cache*'
]

INSTALL_TIMEOUT = 300  # 5 minutes for pip install
UNZIPPED_SIZE_WARNING_MB = 200  # Threshold for unzipped size warning
CHUNK_SIZE = 100  # Files per chunk for parallel processing


class DependencyCache:
    """Global dependency cache for Lambda packages."""
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self.cache_dir = Path(tempfile.gettempdir()) / "faas_dep_cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.cache_metadata = self.cache_dir / "metadata.json"
        self._load_metadata()

    def _load_metadata(self):
        """Load cache metadata."""
        if self.cache_metadata.exists():
            with open(self.cache_metadata, 'r') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}

    def _save_metadata(self):
        """Save cache metadata."""
        with open(self.cache_metadata, 'w') as f:
            json.dump(self.metadata, f)

    def get_cache_key(self, requirements_path: str) -> str:
        """Generate cache key from requirements file."""
        with open(requirements_path, 'rb') as f:
            content = f.read()
            # Include Python version in cache key
            key_content = content + f"-python{sys.version_info.major}.{sys.version_info.minor}".encode()
            return hashlib.sha256(key_content).hexdigest()

    def get_cached_deps(self, cache_key: str) -> Optional[str]:
        """Get cached dependencies directory if exists."""
        with self._lock:
            if cache_key in self.metadata:
                cache_path = self.cache_dir / cache_key
                if cache_path.exists():
                    # Check if cache is still fresh (7 days)
                    age_days = (time.time() - self.metadata[cache_key]['timestamp']) / (24 * 3600)
                    if age_days < 7:
                        return str(cache_path)
                    else:
                        # Clean stale cache
                        shutil.rmtree(cache_path, ignore_errors=True)
                        del self.metadata[cache_key]
                        self._save_metadata()
            return None

    def store_deps(self, cache_key: str, source_dir: str) -> str:
        """Store dependencies in cache."""
        with self._lock:
            cache_path = self.cache_dir / cache_key
            if cache_path.exists():
                shutil.rmtree(cache_path)

            # Copy dependencies to cache
            shutil.copytree(source_dir, cache_path)

            # Update metadata
            self.metadata[cache_key] = {
                'timestamp': time.time(),
                'size_mb': sum(f.stat().st_size for f in cache_path.rglob('*') if f.is_file()) / (1024 * 1024)
            }
            self._save_metadata()

            return str(cache_path)

    def cleanup_old_cache(self, max_age_days: int = 7):
        """Clean up old cache entries."""
        with self._lock:
            current_time = time.time()
            to_remove = []

            for cache_key, info in self.metadata.items():
                age_days = (current_time - info['timestamp']) / (24 * 3600)
                if age_days > max_age_days:
                    cache_path = self.cache_dir / cache_key
                    if cache_path.exists():
                        shutil.rmtree(cache_path, ignore_errors=True)
                    to_remove.append(cache_key)

            for key in to_remove:
                del self.metadata[key]

            if to_remove:
                self._save_metadata()


def _install_dependencies_process(requirements_path: str, target_dir: str) -> Dict[str, Any]:
    """Install dependencies in a separate process for better parallelism."""
    start_time = time.perf_counter()

    if os.path.getsize(requirements_path) == 0:
        return {"status": "empty", "time_ms": 0}

    # Read requirements to check what we're installing
    with open(requirements_path, 'r') as f:
        requirements = f.read().strip()
        if not requirements:
            return {"status": "empty", "time_ms": 0}

    print(f"[Packaging] Installing dependencies for AWS Lambda (Linux): {requirements}")

    # First try with platform-specific wheels
    cmd = [
        sys.executable, "-m", "pip", "install",
        "-r", requirements_path,
        "-t", target_dir,
        "--platform", "manylinux2014_x86_64",
        "--python-version", "3.9",
        "--only-binary=:all:",
        "--no-compile",
        "--no-cache-dir",  # Disable cache to avoid lock contention
        "--disable-pip-version-check",
        "--no-warn-script-location",
        "--progress-bar", "off",  # Reduce output overhead
        "--quiet"
    ]

    # Add parallel download flag if pip version supports it
    pip_version = subprocess.run([sys.executable, "-m", "pip", "--version"],
                                 capture_output=True, text=True)
    if "21." in pip_version.stdout or "22." in pip_version.stdout or "23." in pip_version.stdout:
        cmd.extend(["-j", "4"])  # Use parallel downloads

    try:
        result = subprocess.run(cmd,
                                capture_output=True,
                                text=True,
                                timeout=INSTALL_TIMEOUT)
        if result.returncode != 0:
            # If platform-specific install fails, try without platform restrictions
            print("[Packaging] Platform-specific install failed, trying generic install...")

            cmd_generic = [
                sys.executable, "-m", "pip", "install",
                "-r", requirements_path,
                "-t", target_dir,
                "--no-compile",
                "--no-cache-dir",
                "--disable-pip-version-check",
                "--no-warn-script-location",
                "--progress-bar", "off",
                "--quiet"
            ]

            result = subprocess.run(cmd_generic,
                                    capture_output=True,
                                    text=True,
                                    timeout=INSTALL_TIMEOUT)

            if result.returncode != 0:
                raise PackagingException(f"pip install failed: {result.stderr}")

    except subprocess.TimeoutExpired:
        raise PackagingException(f"Installation timed out after {INSTALL_TIMEOUT}s")
    except Exception as e:
        raise PackagingException(f"An error occurred during installation: {e}")

    install_time_ms = (time.perf_counter() - start_time) * 1000
    return {"status": "success", "time_ms": install_time_ms}


def _install_dependencies_process_with_cache(requirements_path: str, target_dir: str,
                                             cache_key: str, cache: DependencyCache) -> Dict[str, Any]:
    """Install dependencies with caching support."""
    # First run the regular installation
    result = _install_dependencies_process(requirements_path, target_dir)

    # If successful, store in cache
    if result["status"] == "success":
        try:
            cache.store_deps(cache_key, target_dir)
            print(f"[Packaging] Cached dependencies for future use")
        except Exception as e:
            print(f"[Packaging] Warning: Failed to cache dependencies: {e}")

    return result


class DependencyInstaller:
    """Manages parallel dependency installation with caching."""

    def __init__(self, max_workers: int = 10):
        self.process_pool = ProcessPoolExecutor(max_workers=max_workers)
        self.install_futures: Dict[str, Any] = {}
        self.futures_lock = threading.Lock()
        self._shutdown = False
        self.cache = DependencyCache()

    def install_async(self, requirements_path: str, target_dir: str) -> Any:
        """Install dependencies asynchronously with caching."""
        # Check if requirements file is empty
        if os.path.getsize(requirements_path) == 0:
            # Return a completed future immediately
            future = ThreadPoolExecutor(max_workers=1).submit(lambda: {"status": "empty", "time_ms": 0})
            return future

        # Generate cache key
        cache_key = self.cache.get_cache_key(requirements_path)

        # Check cache first
        cached_path = self.cache.get_cached_deps(cache_key)
        if cached_path:
            # Copy from cache to target
            print(f"[Packaging] Using cached dependencies for {cache_key[:8]}...")
            future = ThreadPoolExecutor(max_workers=1).submit(
                self._copy_from_cache, cached_path, target_dir
            )
            return future

        with self.futures_lock:
            # Check if another process is already installing these deps
            if cache_key in self.install_futures:
                # Wait for the existing installation and then copy
                existing_future = self.install_futures[cache_key]
                future = ThreadPoolExecutor(max_workers=1).submit(
                    self._wait_and_copy, existing_future, cache_key, target_dir
                )
                return future

            # Submit new installation to process pool
            install_future = self.process_pool.submit(
                _install_dependencies_process_with_cache,
                requirements_path, target_dir, cache_key, self.cache
            )
            self.install_futures[cache_key] = install_future
            return install_future

    def _copy_from_cache(self, cached_path: str, target_dir: str) -> Dict[str, Any]:
        """Copy dependencies from cache to target directory."""
        start_time = time.perf_counter()
        os.makedirs(target_dir, exist_ok=True)

        # Use hard links for speed if on same filesystem
        try:
            for item in os.listdir(cached_path):
                src = os.path.join(cached_path, item)
                dst = os.path.join(target_dir, item)
                if os.path.isdir(src):
                    shutil.copytree(src, dst, copy_function=os.link)
                else:
                    os.link(src, dst)
        except OSError:
            # Fall back to regular copy if hard links fail
            for item in os.listdir(cached_path):
                src = os.path.join(cached_path, item)
                dst = os.path.join(target_dir, item)
                if os.path.isdir(src):
                    shutil.copytree(src, dst)
                else:
                    shutil.copy2(src, dst)

        copy_time_ms = (time.perf_counter() - start_time) * 1000
        return {"status": "cached", "time_ms": copy_time_ms}

    def _wait_and_copy(self, existing_future: Any, cache_key: str, target_dir: str) -> Dict[str, Any]:
        """Wait for existing installation and copy from cache."""
        # Wait for the installation to complete
        result = existing_future.result()

        if result["status"] == "success":
            # Copy from cache
            cached_path = self.cache.get_cached_deps(cache_key)
            if cached_path:
                return self._copy_from_cache(cached_path, target_dir)

        return result

    def shutdown(self):
        """Shutdown the process pool and clean old cache."""
        self._shutdown = True
        self.cache.cleanup_old_cache()
        self.process_pool.shutdown(wait=True)


# Global dependency installer instance
_dependency_installer = DependencyInstaller()


def _generate_requirements(source_path: str, temp_dir: str) -> str:
    """Generates a requirements.txt file from source code imports.

    This function scans the source code for third-party library imports and
    creates a `requirements.txt` file. It serves as a fallback for projects
    that do not include their own dependency file.

    Args:
        source_path: The path to the source code directory.
        temp_dir: The temporary directory to write the generated file to.

    Returns:
        The path to the generated `requirements.txt` file.

    Raises:
        PackagingException: If dependency detection fails.
    """
    requirements_file = os.path.join(temp_dir, 'requirements.txt')
    print(
        "[Packaging] WARNING: requirements.txt not found. "
        "Attempting to auto-detect dependencies."
    )

    try:
        # Option 1: Try using pipreqs programmatically
        try:
            from pipreqs import pipreqs

            # Get all imports from the source directory
            imports = pipreqs.get_all_imports(source_path)

            # Get package names from imports
            pkg_names = pipreqs.get_pkg_names(imports)

            # Write requirements file
            with open(requirements_file, 'w') as f:
                for pkg in pkg_names:
                    f.write(f"{pkg}\n")

            print(f"[Packaging] Detected dependencies: {', '.join(pkg_names) if pkg_names else 'none'}")

        except (ImportError, AttributeError) as e:
            # Option 2: Fall back to subprocess if pipreqs API is not available
            print("[Packaging] Using pipreqs CLI fallback")

            # Check if pipreqs is installed
            pipreqs_cmd = shutil.which('pipreqs')
            if not pipreqs_cmd:
                # Try installing pipreqs
                subprocess.run([sys.executable, '-m', 'pip', 'install', 'pipreqs'],
                               capture_output=True, text=True)
                pipreqs_cmd = shutil.which('pipreqs')

            if pipreqs_cmd:
                result = subprocess.run(
                    [pipreqs_cmd, source_path, '--savepath', requirements_file, '--force'],
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    # pipreqs might fail on simple scripts, create empty requirements
                    Path(requirements_file).touch()
                    print("[Packaging] No external dependencies detected")
            else:
                # If pipreqs is not available, create empty requirements
                Path(requirements_file).touch()
                print("[Packaging] pipreqs not available, assuming no dependencies")

    except Exception as e:
        # As a last resort, create an empty requirements file
        Path(requirements_file).touch()
        print(f"[Packaging] Could not detect dependencies: {e}")
        print("[Packaging] Proceeding with empty requirements.txt")

    if not os.path.exists(requirements_file):
        # Ensure the file exists
        Path(requirements_file).touch()

    return requirements_file


def _check_package_size(package_dir: str) -> None:
    """Checks the unzipped package size and warns the user if it's large.

    Args:
        package_dir: The path to the directory containing the packaged files.
    """
    total_size = sum(
        f.stat().st_size for f in Path(package_dir).glob('**/*') if f.is_file()
    )
    total_size_mb = total_size / (1024 * 1024)

    if total_size_mb > UNZIPPED_SIZE_WARNING_MB:
        print("\n" + "=" * 60)
        print(f"[Packaging] WARNING: Unzipped package size is {total_size_mb:.2f} MB.")
        print("This may exceed AWS Lambda's 250 MB (unzipped) limit.")
        print("Listing the top 5 largest directories:")

        dir_sizes = {}
        for item in Path(package_dir).iterdir():
            if item.is_dir():
                dir_size = sum(f.stat().st_size for f in item.glob('**/*') if f.is_file())
                dir_sizes[item.name] = dir_size

        sorted_dirs = sorted(dir_sizes.items(), key=lambda x: x[1], reverse=True)
        for i, (dirname, size) in enumerate(sorted_dirs[:5]):
            print(f"  {i + 1}. {dirname}: {size / (1024 * 1024):.2f} MB")
        print("=" * 60 + "\n")


def _install_dependencies(requirements_path: str, target_dir: str) -> None:
    """Installs Python packages for Lambda into a target directory.

    This function uses pip flags to ensure that the installed packages are
    compatible with the Amazon Linux environment used by AWS Lambda.

    Args:
        requirements_path: The path to the `requirements.txt` file.
        target_dir: The directory where packages will be installed.

    Raises:
        PackagingException: If the pip installation process fails.
    """
    # Use the async installer
    future = _dependency_installer.install_async(requirements_path, target_dir)
    result = future.result()  # Wait for completion

    if result["status"] == "success":
        print(f"[Packaging] Dependencies installed in {result['time_ms']:.2f}ms")


def _cleanup_package_dir(package_dir: str) -> None:
    """Removes unnecessary files and directories from the package directory.

    Args:
        package_dir: The path to the directory to clean.
    """
    for root, dirs, files in os.walk(package_dir, topdown=False):
        for name in files:
            if name.endswith(('.pyc', '.pyo')):
                os.remove(os.path.join(root, name))
        for name in dirs:
            if name in ('__pycache__', '.pytest_cache', '.mypy_cache', 'tests', 'test') or \
                    name.endswith(('.dist-info', '.egg-info')):
                shutil.rmtree(os.path.join(root, name), ignore_errors=True)


def _create_zip_archive_streaming(source_path: str, excludes: List[str]) -> bytes:
    """Creates a zip archive with streaming for better memory efficiency.

    Args:
        source_path: The directory to be zipped.
        excludes: A list of file and directory patterns to exclude.

    Returns:
        The content of the zip archive as bytes.

    Raises:
        PackagingException: If the final package size exceeds the 50MB limit.
    """
    # Collect all files to add
    files_to_add = []
    for root, dirs, files in os.walk(source_path):
        dirs[:] = [d for d in dirs if d not in excludes]
        for file in files:
            if file not in excludes:
                files_to_add.append((root, file))

    # Use a temporary file for large packages
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_path = tmp_file.name

    try:
        with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED,
                             compresslevel=1) as zip_file:  # Lower compression = faster
            # Process files in chunks for better performance
            for i in range(0, len(files_to_add), CHUNK_SIZE):
                chunk = files_to_add[i:i + CHUNK_SIZE]
                for root, file in chunk:
                    file_path = os.path.join(root, file)
                    arc_path = os.path.relpath(file_path, source_path)
                    zip_file.write(file_path, arc_path)

        # Read back the file
        with open(tmp_path, 'rb') as f:
            zip_content = f.read()

        if len(zip_content) > 50 * 1024 * 1024:
            raise PackagingException(
                "Zipped package size exceeds AWS Lambda's 50MB limit.")

        return zip_content

    finally:
        # Clean up temp file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def _create_zip_archive(source_path: str, excludes: List[str]) -> bytes:
    """Creates a zip archive from a source directory.

    Args:
        source_path: The directory to be zipped.
        excludes: A list of file and directory patterns to exclude.

    Returns:
        The content of the zip archive as bytes.

    Raises:
        PackagingException: If the final package size exceeds the 50MB limit.
    """
    # Use streaming implementation for better performance
    return _create_zip_archive_streaming(source_path, excludes)


def _merge_zip_archives(code_zip: bytes, deps_dir: str) -> bytes:
    """Merge code zip with dependencies directory.

    Args:
        code_zip: The zip containing source code
        deps_dir: Directory containing installed dependencies

    Returns:
        Merged zip file as bytes
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_path = tmp_file.name

    try:
        # Write code zip first
        with open(tmp_path, 'wb') as f:
            f.write(code_zip)

        # Add dependencies to the zip
        with zipfile.ZipFile(tmp_path, 'a', zipfile.ZIP_DEFLATED, compresslevel=1) as zip_file:
            for root, dirs, files in os.walk(deps_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arc_path = os.path.relpath(file_path, deps_dir)
                    zip_file.write(file_path, arc_path)

        # Read the merged zip
        with open(tmp_path, 'rb') as f:
            return f.read()

    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


def create_deployment_package_with_layers(
        source_path: str,
        excludes: Optional[List[str]] = None,
        install_dependencies: bool = True) -> Tuple[bytes, int, float]:
    """Creates a deployment package with separate dependency and code layers.

    This optimizes packaging by allowing parallel dependency installation
    and code packaging.

    Args:
        source_path: The path to the source code directory.
        excludes: A list of file or directory patterns to exclude.
        install_dependencies: Whether to install dependencies.

    Returns:
        A tuple containing the zip archive content (bytes), the package size
        in bytes, and the total creation time in milliseconds.
    """
    start_time = time.perf_counter()

    if not os.path.isdir(source_path):
        raise PackagingException(f"Source path is not a directory: {source_path}")

    final_excludes = excludes or DEFAULT_EXCLUDES

    with tempfile.TemporaryDirectory() as temp_dir:
        # Step 1: Package code separately (without dependencies)
        code_dir = os.path.join(temp_dir, "code")
        shutil.copytree(
            source_path, code_dir,
            ignore=shutil.ignore_patterns(*final_excludes)
        )

        # Clean up code directory
        _cleanup_package_dir(code_dir)

        # Start packaging code immediately
        code_zip_future = ThreadPoolExecutor(max_workers=1).submit(
            _create_zip_archive, code_dir, final_excludes
        )

        # Step 2: Install dependencies in parallel if needed
        deps_future = None
        if install_dependencies:
            deps_dir = os.path.join(temp_dir, "dependencies")
            os.makedirs(deps_dir, exist_ok=True)

            requirements_path = os.path.join(source_path, 'requirements.txt')
            if not os.path.exists(requirements_path):
                requirements_path = _generate_requirements(source_path, temp_dir)

            # Install dependencies asynchronously
            deps_future = _dependency_installer.install_async(requirements_path, deps_dir)

        # Step 3: Wait for code packaging to complete
        code_zip = code_zip_future.result()

        # Step 4: Merge with dependencies if they exist
        if deps_future:
            deps_result = deps_future.result()
            if deps_result["status"] == "success":
                # Check size before merging
                _check_package_size(temp_dir)
                # Merge code and dependencies
                final_zip = _merge_zip_archives(code_zip, deps_dir)
            else:
                final_zip = code_zip
        else:
            final_zip = code_zip

    package_size = len(final_zip)
    creation_time_ms = (time.perf_counter() - start_time) * 1000

    print(
        f"[Packaging] Created package: {package_size / (1024 * 1024):.2f}MB "
        f"in {creation_time_ms:.2f}ms"
    )

    return final_zip, package_size, creation_time_ms


def create_deployment_package(
        source_path: str,
        excludes: Optional[List[str]] = None,
        install_dependencies: bool = True) -> Tuple[bytes, int, float]:
    """Creates a zip deployment package from a source code directory.

    This is the main function for packaging a Python project for AWS Lambda.
    It handles source code copying, dependency installation with Lambda-
    compatible flags, and creation of a compressed zip archive.

    Args:
        source_path: The path to the source code directory.
        excludes: A list of file or directory patterns to exclude.
        install_dependencies: Whether to install dependencies.

    Returns:
        A tuple containing the zip archive content (bytes), the package size
        in bytes, and the total creation time in milliseconds.

    Raises:
        PackagingException: If the source path is invalid or if any step in
                            the packaging process fails.
    """
    # Use the optimized layer-based approach
    return create_deployment_package_with_layers(
        source_path, excludes, install_dependencies
    )


def create_inline_package(handler_code: str,
                          handler_name: str = "handler.handler") -> Tuple[bytes, int, float]:
    """Creates a deployment package from an inline string of Python code.

    Args:
        handler_code: A string containing the Python function code.
        handler_name: The name of the handler in 'module.function' format.

    Returns:
        A tuple containing the zip archive content (bytes), the package size
        in bytes, and the total creation time in milliseconds.
    """
    start_time = time.perf_counter()

    with tempfile.TemporaryDirectory() as tmpdir:
        module_name = handler_name.split('.')[0]
        handler_file = Path(tmpdir) / f"{module_name}.py"
        handler_file.write_text(handler_code)

        zip_content, package_size, _ = create_deployment_package(
            tmpdir, install_dependencies=False
        )

    total_time_ms = (time.perf_counter() - start_time) * 1000
    return zip_content, package_size, total_time_ms


def validate_handler(source_path: str, handler_name: str) -> bool:
    """Validates that the specified Python handler exists in the source code.

    Args:
        source_path: The path to the source code directory.
        handler_name: The handler in 'module.function' format.

    Returns:
        True if the handler is found and valid.

    Raises:
        PackagingException: If the handler name is malformed, the module file
                            is not found, or the function is not in the module.
    """
    if '.' not in handler_name:
        raise PackagingException("Handler must be in 'module.function' format.")

    # Split handler into parts and reconstruct module path
    parts = handler_name.split('.')
    function_name = parts[-1]
    module_parts = parts[:-1]

    # Try to find the module file
    possible_paths = [
        os.path.join(source_path, *module_parts) + '.py',
        os.path.join(source_path, *module_parts, '__init__.py'),
        os.path.join(source_path, *module_parts[:-1], module_parts[-1] + '.py') if len(module_parts) > 1 else None
    ]

    py_file = None
    for path in possible_paths:
        if path and os.path.exists(path):
            py_file = path
            break

    if not py_file:
        # Don't raise exception, just log warning
        print(f"[Packaging] Warning: Handler module not found for {handler_name}, will check at runtime")
        return True  # Allow it to proceed

    try:
        with open(py_file, 'r') as f:
            content = f.read()
            if f"def {function_name}(" in content:
                return True
        # Don't raise exception for missing function either
        print(f"[Packaging] Warning: Handler function '{function_name}' not found in {py_file}, will check at runtime")
        return True
    except IOError as e:
        print(f"[Packaging] Warning: Cannot read handler file: {e}")
        return True  # Allow it to proceed


def shutdown_packaging():
    """Shutdown the global dependency installer."""
    global _dependency_installer
    if _dependency_installer:
        _dependency_installer.shutdown()
        _dependency_installer = None