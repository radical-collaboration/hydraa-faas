"""
Function package validator for FaaS agent.
Validates uploaded function packages for deployment-critical issues.
"""

import zipfile
import ast
import tempfile
import re
from typing import Dict, List, Any

from .models import ValidationResult


class FunctionValidator:
    """Validates function packages for issues that will affect deployment."""

    # Supported runtimes and their required file extensions
    RUNTIME_CONFIGS = {
        'python:3.8': {'extensions': ['.py']},
        'python:3.9': {'extensions': ['.py']},
        'python:3.10': {'extensions': ['.py']},
        'python:3.11': {'extensions': ['.py']},
        'node:16': {'extensions': ['.js']},
        'node:18': {'extensions': ['.js']},
        'node:20': {'extensions': ['.js']},
        'go:1.19': {'extensions': ['.go']},
        'go:1.20': {'extensions': ['.go']},
        'go:1.21': {'extensions': ['.go']},
    }

    # Maximum file sizes (in bytes)
    MAX_ZIP_SIZE = 250 * 1024 * 1024  # 250MB
    MAX_FILE_SIZE = 50 * 1024 * 1024   # 50MB per file

    def __init__(self):
        self.validation_cache = {}

    async def validate_zip(self, zip_file) -> ValidationResult:
        """Validate uploaded zip file for deployment readiness."""
        errors = []
        metadata = {}

        try:
            zip_file.file.seek(0)
            zip_content = await zip_file.read()
            zip_file.file.seek(0)

            # Check file size
            if len(zip_content) > self.MAX_ZIP_SIZE:
                errors.append(f"Zip file too large: {len(zip_content)} bytes (max: {self.MAX_ZIP_SIZE})")
                return ValidationResult(is_valid=False, errors=errors)

            # Validate zip structure and contents
            with tempfile.NamedTemporaryFile() as temp_file:
                temp_file.write(zip_content)
                temp_file.flush()

                try:
                    with zipfile.ZipFile(temp_file.name, 'r') as zf:
                        validation_result = self._validate_zip_contents(zf)
                        errors.extend(validation_result['errors'])
                        metadata.update(validation_result['metadata'])
                except zipfile.BadZipFile:
                    errors.append("Invalid zip file format")

        except Exception as e:
            errors.append(f"Validation error: {str(e)}")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=[],  # Warnings are no longer generated
            metadata=metadata
        )

    def _validate_zip_contents(self, zip_file: zipfile.ZipFile) -> Dict[str, Any]:
        """Validate contents of zip file for deployment-critical issues."""
        errors = []
        metadata = {
            'files': [],
            'total_size': 0,
            'has_handler_function': False,
            'file_count': 0
        }

        file_list = zip_file.namelist()
        metadata['file_count'] = len(file_list)

        # Validate each file
        for file_info in zip_file.filelist:
            if file_info.is_dir():
                continue

            filename = file_info.filename
            file_size = file_info.file_size

            metadata['files'].append({'name': filename, 'size': file_size})
            metadata['total_size'] += file_size

            # Check individual file size
            if file_size > self.MAX_FILE_SIZE:
                errors.append(f"File too large: {filename} ({file_size} bytes)")

            # Check for handler function in Python files
            if filename.endswith('.py'):
                try:
                    file_content = zip_file.read(filename).decode('utf-8')
                    if not metadata['has_handler_function'] and self._has_handler_function(file_content):
                        metadata['has_handler_function'] = True
                except Exception:
                    # Ignore files that can't be read or parsed; focus is on finding a valid handler
                    pass

        # Validate overall project structure
        structure_errors = self._validate_project_structure(file_list, metadata)
        errors.extend(structure_errors)

        return {'errors': errors, 'warnings': [], 'metadata': metadata}

    def _validate_project_structure(self, file_list: List[str], metadata: Dict[str, Any]) -> List[str]:
        """Validate project structure requirements that affect deployment."""
        errors = []
        python_files = [f for f in file_list if f.endswith('.py') and not f.startswith('__pycache__')]

        # For Python projects make sure theres at least one function that looks like a handler.
        if python_files and not metadata['has_handler_function']:
            errors.append("No function named 'handler', 'main', or similar found in any Python files.")

        # Check for excessive file count
        if len(file_list) > 1000:
            errors.append(f"Too many files in package: {len(file_list)} (max: 1000)")

        # Check total uncompressed size
        if metadata['total_size'] > 1024 * 1024 * 1024:  # 1GB
            errors.append(f"Total uncompressed size too large: {metadata['total_size']} bytes")

        return errors

    def _has_handler_function(self, content: str) -> bool:
        """Check if Python file contains a potential handler function."""
        try:
            tree = ast.parse(content)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name in ['handler', 'main', 'lambda_handler', 'app']:
                    return True
        except SyntaxError:
            return False
        return False

    def validate_runtime_compatibility(self, runtime: str, file_list: List[str]) -> ValidationResult:
        """Validate that files are compatible with the specified runtime."""
        errors = []
        if runtime not in self.RUNTIME_CONFIGS:
            errors.append(f"Unsupported runtime: {runtime}")
            return ValidationResult(is_valid=False, errors=errors)

        config = self.RUNTIME_CONFIGS[runtime]
        required_ext = config['extensions']

        # Check for files with the correct extension for the chosen runtime
        has_valid_files = any(any(f.endswith(ext) for ext in required_ext) for f in file_list)
        if not has_valid_files:
            errors.append(f"No files found for runtime {runtime}. Expected files with extensions: {required_ext}")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    def validate_handler_format(self, handler: str) -> ValidationResult:
        """Validate handler format (e.g., 'module:function')."""
        errors = []
        if not handler or ':' not in handler:
            errors.append("Handler must be in the format 'module:function'")
        else:
            module, function = handler.split(':', 1)
            if not module or not function:
                errors.append("Both module and function names are required in the handler string")
            elif not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', module):
                errors.append(f"Invalid module name format in handler: '{module}'")
            elif not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', function):
                errors.append(f"Invalid function name format in handler: '{function}'")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    def get_validation_summary(self, result: ValidationResult) -> Dict[str, Any]:
        """Get a human-readable validation summary."""
        return {
            'valid': result.is_valid,
            'error_count': len(result.errors),
            'errors': result.errors,
            'metadata': result.metadata,
        }
