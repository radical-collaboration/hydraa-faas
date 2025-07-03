"""
Utilities for packaging Lambda functions from source code
"""

import os
import zipfile
import tempfile
import shutil
from io import BytesIO
from .exceptions import PackagingException

DEFAULT_EXCLUDES = [
    '__pycache__',
    '*.pyc',
    '.git',
    '.gitignore',
    '.DS_Store',
    'node_modules',
    '.env',
    'venv',
    '.venv',
    'tests',
    '*.md',
    'Dockerfile',
    '.dockerignore'
]


def create_deployment_package(source_path: str, excludes: list = None, max_memory_size: int = 10*1024*1024):
    """
    Create a ZIP deployment package from source code directory

    Args:
        source_path: Path to source code directory
        excludes: List of patterns to exclude from package
        max_memory_size: Maximum size to keep in memory before using temp file

    Returns:
        bytes: ZIP file content as bytes

    Raises:
        PackagingException: If packaging fails
    """
    if not os.path.exists(source_path):
        raise PackagingException(f"Source path does not exist: {source_path}")

    if not os.path.isdir(source_path):
        raise PackagingException(f"Source path is not a directory: {source_path}")

    excludes = excludes or DEFAULT_EXCLUDES

    # estimate size first to decide memory vs file approach
    estimated_size = estimate_package_size(source_path, excludes)

    try:
        if estimated_size > max_memory_size:
            return _create_package_with_tempfile(source_path, excludes)
        else:
            return _create_package_in_memory(source_path, excludes)
    except Exception as e:
        raise PackagingException(f"Failed to create deployment package: {str(e)}")


def _create_package_in_memory(source_path: str, excludes: list):
    """Create package in memory for smaller packages"""
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        _add_files_to_zip(zip_file, source_path, excludes)

    zip_buffer.seek(0)
    zip_content = zip_buffer.getvalue()

    if len(zip_content) > 50 * 1024 * 1024:  # 50MB limit
        raise PackagingException("Package size exceeds 50MB limit for ZIP deployment")

    return zip_content


def _create_package_with_tempfile(source_path: str, excludes: list):
    """Create package using temporary file for larger packages"""
    with tempfile.NamedTemporaryFile() as tmp_file:
        with zipfile.ZipFile(tmp_file, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            _add_files_to_zip(zip_file, source_path, excludes)

        tmp_file.seek(0)
        zip_content = tmp_file.read()

        if len(zip_content) > 50 * 1024 * 1024:  # 50MB limit
            raise PackagingException("Package size exceeds 50MB limit for ZIP deployment")

        return zip_content


def _add_files_to_zip(zip_file: zipfile.ZipFile, source_path: str, excludes: list):
    """Add files to zip archive"""
    for root, dirs, files in os.walk(source_path):
        # filter out excluded directories
        dirs[:] = [d for d in dirs if not _should_exclude(d, excludes)]

        for file in files:
            if _should_exclude(file, excludes):
                continue

            file_path = os.path.join(root, file)
            # create archive path relative to source_path
            arc_path = os.path.relpath(file_path, source_path)
            zip_file.write(file_path, arc_path)


def _should_exclude(name, excludes):
    """Check if file/directory should be excluded based on patterns"""
    for pattern in excludes:
        if pattern.startswith('*') and name.endswith(pattern[1:]):
            return True
        elif name == pattern:
            return True
    return False


def validate_handler(source_path, handler_name):
    """
    Validate that the specified handler exists in the source code

    Args:
        source_path (str): Path to source code directory
        handler_name (str): Handler in format 'module.function'

    Returns:
        bool: True if handler exists

    Raises:
        PackagingException: If handler validation fails
    """
    if not handler_name or '.' not in handler_name:
        raise PackagingException("Handler must be in format 'module.function'")

    module_name, function_name = handler_name.rsplit('.', 1)

    # check for python handler
    py_file = os.path.join(source_path, f"{module_name}.py")
    if os.path.exists(py_file):
        return _validate_python_handler(py_file, function_name)

    # check for node.js handler
    js_file = os.path.join(source_path, f"{module_name}.js")
    if os.path.exists(js_file):
        return _validate_nodejs_handler(js_file, function_name)

    raise PackagingException(f"Handler module not found: {module_name}")


def _validate_python_handler(file_path, function_name):
    """Validate Python handler function exists"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            if f"def {function_name}(" in content:
                return True
        raise PackagingException(f"Handler function '{function_name}' not found")
    except IOError as e:
        raise PackagingException(f"Cannot read handler file: {str(e)}")


def _validate_nodejs_handler(file_path, function_name):
    """Validate Node.js handler function exists"""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
            # basic check for exports or function declaration
            if f"exports.{function_name}" in content or f"function {function_name}" in content:
                return True
        raise PackagingException(f"Handler function '{function_name}' not found")
    except IOError as e:
        raise PackagingException(f"Cannot read handler file: {str(e)}")


def estimate_package_size(source_path, excludes=None):
    """
    Estimate the size of the deployment package without creating it

    Args:
        source_path (str): Path to source code directory
        excludes (list): List of patterns to exclude

    Returns:
        int: Estimated size in bytes
    """
    excludes = excludes or DEFAULT_EXCLUDES
    total_size = 0

    for root, dirs, files in os.walk(source_path):
        dirs[:] = [d for d in dirs if not _should_exclude(d, excludes)]

        for file in files:
            if not _should_exclude(file, excludes):
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                except OSError:
                    pass  # skip files that cant be read

    return total_size