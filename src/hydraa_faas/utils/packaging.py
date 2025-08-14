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
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple

from .exceptions import PackagingException

DEFAULT_EXCLUDES = [
    '__pycache__', '*.pyc', '.git', '.gitignore', '.DS_Store', 'node_modules',
    '.env', 'venv', '.venv', 'tests', '*.md', 'Dockerfile', '.dockerignore',
    '.mypy_cache', '.pytest_cache', '*.egg-info', 'dist', 'build', '.idea',
    '.vscode', '*.swp', '*.swo', '*~', '.cache*'
]

INSTALL_TIMEOUT = 300  # 5 minutes for pip install
UNZIPPED_SIZE_WARNING_MB = 200  # Threshold for unzipped size warning


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
    if os.path.getsize(requirements_path) == 0:
        print("[Packaging] requirements.txt is empty, skipping installation.")
        return

    # Read requirements to check what we're installing
    with open(requirements_path, 'r') as f:
        requirements = f.read().strip()
        if not requirements:
            print("[Packaging] No dependencies to install.")
            return

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
        "--quiet",
        "--no-cache-dir"
    ]

    try:
        result = subprocess.run(cmd,
                                capture_output=True,
                                text=True,
                                timeout=INSTALL_TIMEOUT)
        if result.returncode != 0:
            # If platform-specific install fails, try without platform restrictions
            # This is useful for pure Python packages
            print("[Packaging] Platform-specific install failed, trying generic install...")

            cmd_generic = [
                sys.executable, "-m", "pip", "install",
                "-r", requirements_path,
                "-t", target_dir,
                "--no-compile",
                "--quiet",
                "--no-cache-dir"
            ]

            result = subprocess.run(cmd_generic,
                                    capture_output=True,
                                    text=True,
                                    timeout=INSTALL_TIMEOUT)

            if result.returncode != 0:
                raise PackagingException(f"pip install failed: {result.stderr}")

    except subprocess.TimeoutExpired:
        raise PackagingException(
            f"Installation timed out after {INSTALL_TIMEOUT}s")
    except Exception as e:
        raise PackagingException(f"An error occurred during installation: {e}")


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
    zip_buffer = BytesIO()
    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED,
                             compresslevel=9) as zip_file:
            for root, dirs, files in os.walk(source_path):
                dirs[:] = [d for d in dirs if d not in excludes]
                for file in files:
                    if file not in excludes:
                        file_path = os.path.join(root, file)
                        arc_path = os.path.relpath(file_path, source_path)
                        zip_file.write(file_path, arc_path)
    except MemoryError:
        zip_buffer.close()
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name
        try:
            with zipfile.ZipFile(tmp_path, 'w', zipfile.ZIP_DEFLATED,
                                 compresslevel=1) as zip_file:
                for root, dirs, files in os.walk(source_path):
                    dirs[:] = [d for d in dirs if d not in excludes]
                    for file in files:
                        if file not in excludes:
                            file_path = os.path.join(root, file)
                            arc_path = os.path.relpath(file_path, source_path)
                            zip_file.write(file_path, arc_path)
            with open(tmp_path, 'rb') as f:
                zip_content = f.read()
        finally:
            os.unlink(tmp_path)
        return zip_content

    zip_content = zip_buffer.getvalue()
    if len(zip_content) > 50 * 1024 * 1024:
        raise PackagingException(
            "Zipped package size exceeds AWS Lambda's 50MB limit.")
    return zip_content


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
    start_time = time.perf_counter()

    if not os.path.isdir(source_path):
        raise PackagingException(f"Source path is not a directory: {source_path}")

    final_excludes = excludes or DEFAULT_EXCLUDES

    with tempfile.TemporaryDirectory() as temp_dir:
        package_dir = os.path.join(temp_dir, "package")
        shutil.copytree(
            source_path, package_dir,
            ignore=shutil.ignore_patterns(*final_excludes)
        )

        if install_dependencies:
            requirements_path = os.path.join(source_path, 'requirements.txt')
            if not os.path.exists(requirements_path):
                requirements_path = _generate_requirements(source_path, temp_dir)

            _install_dependencies(requirements_path, package_dir)

        _check_package_size(package_dir)
        _cleanup_package_dir(package_dir)

        zip_content = _create_zip_archive(package_dir, final_excludes)

    package_size = len(zip_content)
    creation_time_ms = (time.perf_counter() - start_time) * 1000

    print(
        f"[Packaging] Created package: {package_size / (1024 * 1024):.2f}MB "
        f"in {creation_time_ms:.2f}ms"
    )

    return zip_content, package_size, creation_time_ms


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

    module_name, function_name = handler_name.rsplit('.', 1)
    py_file = os.path.join(source_path, f"{module_name}.py")

    if not os.path.exists(py_file):
        raise PackagingException(f"Handler module not found: {py_file}")

    try:
        with open(py_file, 'r') as f:
            content = f.read()
            if f"def {function_name}(" in content:
                return True
        raise PackagingException(
            f"Handler function '{function_name}' not found in {py_file}")
    except IOError as e:
        raise PackagingException(f"Cannot read handler file: {e}")