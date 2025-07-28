"""
Cross-platform utilities for FaaS agent
Handles Docker operations and system detection
"""

import logging
import os
import subprocess
import uuid
import platform
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


def generate_id() -> str:
    """Generate a unique function ID."""
    return f"func-{uuid.uuid4().hex[:8]}"


def detect_platform() -> Dict[str, str]:
    """Detect current platform and return system info"""
    return {
        'system': platform.system(),
        'machine': platform.machine(),
        'platform': platform.platform(),
        'python_version': platform.python_version(),
        'is_windows': os.name == 'nt',
        'is_linux': platform.system() == 'Linux',
        'is_macos': platform.system() == 'Darwin'
    }


class DockerError(Exception):
    """Exception for docker errors."""

    def __init__(self, message: str, command: str = "", exit_code: int = -1):
        self.message = message
        self.command = command
        self.exit_code = exit_code
        super().__init__(f"Docker error: {message}")


class DockerUtils:
    """Cross-platform utility class for docker operations."""

    def __init__(self):
        """Initialize docker utilities with platform detection."""
        self.platform_info = detect_platform()
        self.docker_cmd = self._find_docker_command()
        self.timeout = int(os.getenv('DOCKER_TIMEOUT', '300'))
        self._validate_docker()
        logger.info(f"Docker utilities initialized on {self.platform_info['system']}")

    def _find_docker_command(self) -> str:
        """Find the correct Docker command for the platform"""
        if self.platform_info['is_windows']:
            # On Windows, try different Docker locations
            possible_commands = ['docker.exe', 'docker']
        else:
            # On Linux/macOS
            possible_commands = ['docker']

        for cmd in possible_commands:
            if self._command_exists(cmd):
                return cmd

        raise DockerError("Docker command not found. Please ensure Docker is installed and in PATH.")

    def _command_exists(self, command: str) -> bool:
        """Check if a command exists in PATH"""
        try:
            result = subprocess.run(
                [command, '--version'],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            return False

    def _validate_docker(self):
        """Validate docker availability with platform-specific checks."""
        try:
            result = self._run_command(['version', '--format', '{{.Server.Version}}'])
            server_version = result.stdout.strip()
            logger.info(f"Docker server version: {server_version}")

            # Additional validation for Linux (check if user is in docker group)
            if self.platform_info['is_linux']:
                self._check_linux_permissions()

        except DockerError as e:
            if 'permission denied' in e.message.lower():
                if self.platform_info['is_linux']:
                    raise DockerError(
                        "Permission denied. Run: sudo usermod -aG docker $USER && newgrp docker"
                    )
                else:
                    raise DockerError("Permission denied - ensure Docker Desktop is running")
            elif 'cannot connect' in e.message.lower():
                raise DockerError("Cannot connect to Docker daemon - ensure Docker is running")
            raise

    def _check_linux_permissions(self):
        """Check Docker permissions on Linux"""
        try:
            # Check if user is in docker group
            result = subprocess.run(['groups'], capture_output=True, text=True)
            if 'docker' not in result.stdout:
                logger.warning("User not in docker group - some operations may require sudo")
        except Exception:
            pass  # Non-critical check

    def _run_command(self, args: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
        """Run docker command with cross-platform support."""
        cmd = [self.docker_cmd] + args

        try:
            # Platform-specific adjustments
            if self.platform_info['is_windows']:
                # On Windows, ensure proper shell handling
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                    cwd=cwd,
                    shell=False  # Don't use shell on Windows to avoid cmd.exe issues
                )
            else:
                # Linux/macOS
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=self.timeout,
                    cwd=cwd
                )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"

                # Platform-specific error handling
                if self.platform_info['is_windows'] and 'access is denied' in error_msg.lower():
                    error_msg += " (Ensure Docker Desktop is running and you have admin rights)"
                elif self.platform_info['is_linux'] and 'permission denied' in error_msg.lower():
                    error_msg += " (Run: sudo usermod -aG docker $USER && newgrp docker)"

                raise DockerError(error_msg, ' '.join(cmd), result.returncode)

            return result

        except subprocess.TimeoutExpired:
            raise DockerError(f"Command timed out after {self.timeout}s", ' '.join(cmd))
        except FileNotFoundError:
            raise DockerError(f"Docker command not found: {self.docker_cmd}")

    def build_image(self, build_context: str, image_name: str,
                    dockerfile: str = "Dockerfile", build_args: Optional[Dict[str, str]] = None,
                    no_cache: bool = False) -> Dict[str, Any]:
        """Build docker image with cross-platform support."""
        build_context_path = Path(build_context).resolve()

        if not build_context_path.exists():
            raise DockerError(f"Build context does not exist: {build_context_path}")

        dockerfile_path = build_context_path / dockerfile
        if not dockerfile_path.exists():
            raise DockerError(f"Dockerfile does not exist: {dockerfile_path}")

        # Build command
        cmd_args = ['build']
        if no_cache:
            cmd_args.append('--no-cache')

        if build_args:
            for key, value in build_args.items():
                cmd_args.extend(['--build-arg', f'{key}={value}'])

        # Platform-specific dockerfile handling
        if self.platform_info['is_windows']:
            # Use forward slashes for dockerfile path on Windows
            dockerfile_arg = dockerfile.replace('\\', '/')
        else:
            dockerfile_arg = dockerfile

        cmd_args.extend(['-f', dockerfile_arg, '-t', image_name, '.'])

        logger.info(f"Building Docker image: {image_name}")
        self._run_command(cmd_args, cwd=str(build_context_path))

        return {'image_name': image_name, 'status': 'success'}

    def push_image(self, image_name: str) -> Dict[str, Any]:
        """Push docker image to registry."""
        logger.info(f"Pushing Docker image: {image_name}")
        self._run_command(['push', image_name])
        return {'image_name': image_name, 'status': 'success'}

    def tag_image(self, source_image: str, target_image: str) -> Dict[str, Any]:
        """Tag docker image."""
        logger.info(f"Tagging image {source_image} as {target_image}")
        self._run_command(['tag', source_image, target_image])
        return {'source_image': source_image, 'target_image': target_image, 'status': 'success'}

    def check_image_exists(self, image_name: str) -> bool:
        """Check if docker image exists locally."""
        try:
            self._run_command(['inspect', image_name])
            return True
        except DockerError:
            return False

    def remove_image(self, image_name: str, force: bool = False) -> Dict[str, Any]:
        """Remove docker image."""
        cmd_args = ['rmi']
        if force:
            cmd_args.append('--force')
        cmd_args.append(image_name)

        logger.info(f"Removing Docker image: {image_name}")
        self._run_command(cmd_args)
        return {'image_name': image_name, 'status': 'success'}

    def get_platform_info(self) -> Dict[str, Any]:
        """Get Docker and platform information"""
        try:
            # Get Docker info
            result = self._run_command(['info', '--format', '{{json .}}'])
            docker_info = result.stdout.strip()

            return {
                'platform': self.platform_info,
                'docker_command': self.docker_cmd,
                'docker_info': docker_info,
                'timeout': self.timeout
            }
        except Exception as e:
            return {
                'platform': self.platform_info,
                'docker_command': self.docker_cmd,
                'error': str(e)
            }


class KubernetesUtils:
    """Cross-platform Kubernetes utilities"""

    def __init__(self):
        self.kubectl_cmd = self._find_kubectl_command()
        self.platform_info = detect_platform()

    def _find_kubectl_command(self) -> str:
        """Find kubectl command"""
        if detect_platform()['is_windows']:
            possible_commands = ['kubectl.exe', 'kubectl']
        else:
            possible_commands = ['kubectl']

        for cmd in possible_commands:
            try:
                result = subprocess.run([cmd, 'version', '--client'],
                                        capture_output=True, timeout=10)
                if result.returncode == 0:
                    return cmd
            except (subprocess.TimeoutExpired, FileNotFoundError):
                continue

        raise DockerError("kubectl not found. Please install kubectl.")

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        try:
            result = subprocess.run(
                [self.kubectl_cmd, 'cluster-info'],
                capture_output=True,
                text=True,
                timeout=30
            )

            return {
                'status': 'connected' if result.returncode == 0 else 'disconnected',
                'info': result.stdout if result.returncode == 0 else result.stderr,
                'kubectl_command': self.kubectl_cmd
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'kubectl_command': self.kubectl_cmd
            }
