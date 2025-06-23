"""Docker utilities for container management."""

import os
import subprocess
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path

logger = logging.getLogger(__name__)


class DockerError(Exception):
    """Exception for docker errors."""
    
    def __init__(self, message: str, command: str = "", exit_code: int = -1):
        """Initialize docker error.
        
        Args:
            message: Error message.
            command: Failed command.
            exit_code: Process exit code.
        """
        self.message = message
        self.command = command
        self.exit_code = exit_code
        super().__init__(f"Docker error: {message}")


class DockerUtils:
    """Utility class for docker operations."""
    
    def __init__(self):
        """Initialize docker utilities.
        
        Raises:
            DockerError: If docker is not available.
        """
        self.docker_cmd = os.getenv('DOCKER_CMD', 'docker')
        self.timeout = int(os.getenv('DOCKER_TIMEOUT', '300'))
        self._validate_docker()
        logger.info("Docker utilities initialized")
    
    def _validate_docker(self):
        """Validate docker availability.
        
        Raises:
            DockerError: If docker is not available or running.
        """
        try:
            result = self._run_command(['version', '--format', '{{.Server.Version}}'])
            logger.info(f"Docker server version: {result.stdout.strip()}")
        except DockerError as e:
            if 'permission denied' in e.message.lower():
                raise DockerError("Permission denied - add user to docker group")
            elif 'cannot connect' in e.message.lower():
                raise DockerError("Cannot connect to Docker daemon")
            raise
    
    def _run_command(self, args: List[str], cwd: Optional[str] = None) -> subprocess.CompletedProcess:
        """Run docker command.
        
        Args:
            args: Docker command arguments.
            cwd: Working directory.
            
        Returns:
            Completed process result.
            
        Raises:
            DockerError: If command fails.
        """
        cmd = [self.docker_cmd] + args
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, 
                                  timeout=self.timeout, cwd=cwd)
            
            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                raise DockerError(error_msg, ' '.join(cmd), result.returncode)
            
            return result
            
        except subprocess.TimeoutExpired:
            raise DockerError(f"Command timed out after {self.timeout}s", ' '.join(cmd))
        except FileNotFoundError:
            raise DockerError(f"Docker command not found: {self.docker_cmd}")
    
    def build_image(self, build_context: str, image_name: str, 
                   dockerfile: str = "Dockerfile", build_args: Optional[Dict[str, str]] = None,
                   no_cache: bool = False) -> Dict[str, Any]:
        """Build docker image.
        
        Args:
            build_context: Path to build context.
            image_name: Image name and tag.
            dockerfile: Dockerfile name.
            build_args: Build arguments.
            no_cache: Disable build cache.
            
        Returns:
            Build result with image name and status.
            
        Raises:
            DockerError: If build fails.
        """
        build_context_path = Path(build_context).resolve()
        
        if not build_context_path.exists():
            raise DockerError(f"Build context does not exist: {build_context_path}")
        
        dockerfile_path = build_context_path / dockerfile
        if not dockerfile_path.exists():
            raise DockerError(f"Dockerfile does not exist: {dockerfile_path}")
        
        # build command
        cmd_args = ['build']
        if no_cache:
            cmd_args.append('--no-cache')
        
        if build_args:
            for key, value in build_args.items():
                cmd_args.extend(['--build-arg', f'{key}={value}'])
        
        cmd_args.extend(['-f', dockerfile, '-t', image_name, '.'])
        
        logger.info(f"Building Docker image: {image_name}")
        self._run_command(cmd_args, cwd=str(build_context_path))
        
        return {'image_name': image_name, 'status': 'success'}
    
    def push_image(self, image_name: str) -> Dict[str, Any]:
        """Push docker image to registry.
        
        Args:
            image_name: Image name and tag.
            
        Returns:
            Push result with image name and status.
            
        Raises:
            DockerError: If push fails.
        """
        logger.info(f"Pushing Docker image: {image_name}")
        self._run_command(['push', image_name])
        return {'image_name': image_name, 'status': 'success'}
    
    def tag_image(self, source_image: str, target_image: str) -> Dict[str, Any]:
        """Tag docker image.
        
        Args:
            source_image: Source image name.
            target_image: Target image name.
            
        Returns:
            Tag result with source, target, and status.
            
        Raises:
            DockerError: If tagging fails.
        """
        logger.info(f"Tagging image {source_image} as {target_image}")
        self._run_command(['tag', source_image, target_image])
        return {'source_image': source_image, 'target_image': target_image, 'status': 'success'}
    
    def check_image_exists(self, image_name: str) -> bool:
        """Check if docker image exists locally.
        
        Args:
            image_name: Image name and tag.
            
        Returns:
            True if image exists.
        """
        try:
            self._run_command(['inspect', image_name])
            return True
        except DockerError:
            return False
    
    def remove_image(self, image_name: str, force: bool = False) -> Dict[str, Any]:
        """Remove docker image.
        
        Args:
            image_name: Image name and tag.
            force: Force removal.
            
        Returns:
            Removal result with image name and status.
            
        Raises:
            DockerError: If removal fails.
        """
        cmd_args = ['rmi']
        if force:
            cmd_args.append('--force')
        cmd_args.append(image_name)
        
        logger.info(f"Removing Docker image: {image_name}")
        self._run_command(cmd_args)
        return {'image_name': image_name, 'status': 'success'}