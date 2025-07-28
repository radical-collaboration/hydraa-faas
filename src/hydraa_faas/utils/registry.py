"""
Unified Registry Manager for FaaS Providers
Handles Docker registries including ECR, DockerHub, ACR, and local registries
"""

import os
import json
import base64
import platform
import subprocess
from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass
from enum import Enum

from .exceptions import FaasException


class RegistryType(Enum):
    """Supported registry types"""
    NONE = "none"
    LOCAL = "local"
    DOCKERHUB = "dockerhub"
    ECR = "ecr"
    ACR = "acr"
    CUSTOM = "custom"


@dataclass
class RegistryConfig:
    """Registry configuration"""
    type: RegistryType
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    region: Optional[str] = None
    repository_name: Optional[str] = None
    image_tag_mutability: str = 'MUTABLE'
    scan_on_push: bool = True


class RegistryManager:
    """
    Unified registry manager for all Docker registry types.
    Consolidates ECR helper and other registry management functionality.
    """

    def __init__(self, logger, aws_clients: Optional[Dict[str, Any]] = None):
        """
        Initialize registry manager

        Args:
            logger: Logger instance
            aws_clients: Optional dict with 'ecr' client for AWS operations
        """
        self.logger = logger
        self._registry_configs = {}
        self._aws_clients = aws_clients or {}

    def configure_registry(self, name: str, config: RegistryConfig) -> Dict[str, Any]:
        """
        Configure a registry for use

        Args:
            name: Registry name (used as reference)
            config: Registry configuration

        Returns:
            Registry configuration dict
        """
        self.logger.info(f"Configuring registry: {name} (type: {config.type.value})")

        # store config
        self._registry_configs[name] = config

        # return registry specific configuration
        if config.type == RegistryType.ECR:
            return self._configure_ecr(config)
        elif config.type == RegistryType.DOCKERHUB:
            return self._configure_dockerhub(config)
        elif config.type == RegistryType.ACR:
            return self._configure_acr(config)
        elif config.type == RegistryType.LOCAL:
            return self._configure_local(config)
        elif config.type == RegistryType.NONE:
            return {'type': 'none'}
        else:
            return self._configure_custom(config)

    def login(self, registry_name: str) -> bool:
        """
        Login to a configured registry

        Args:
            registry_name: Name of configured registry

        Returns:
            True if login successful
        """
        config = self._registry_configs.get(registry_name)
        if not config:
            raise ValueError(f"Registry {registry_name} not configured")

        if config.type == RegistryType.ECR:
            return self._login_ecr(config)
        elif config.type == RegistryType.DOCKERHUB:
            return self._login_dockerhub(config)
        elif config.type == RegistryType.ACR:
            return self._login_acr(config)
        elif config.type in [RegistryType.LOCAL, RegistryType.NONE]:
            return True  # No login needed
        else:
            return self._login_custom(config)

    def push(self, image: str, registry_name: str) -> str:
        """
        Push image to registry

        Args:
            image: Image name with tag
            registry_name: Name of configured registry

        Returns:
            Full image URI after push
        """
        config = self._registry_configs.get(registry_name)
        if not config:
            raise ValueError(f"Registry {registry_name} not configured")

        # Tag image for registry
        tagged_image = self._tag_for_registry(image, config)

        # Login if needed
        self.login(registry_name)

        # Push image
        cmd = ['docker', 'push', tagged_image]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise FaasException(f"Docker push failed: {result.stderr}")

        self.logger.info(f"Pushed image: {tagged_image}")
        return tagged_image

    def pull(self, image: str, registry_name: Optional[str] = None) -> None:
        """
        Pull image from registry

        Args:
            image: Image name with tag
            registry_name: Optional registry name if login needed
        """
        if registry_name:
            self.login(registry_name)

        cmd = ['docker', 'pull', image]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0:
            raise FaasException(f"Docker pull failed: {result.stderr}")

        self.logger.info(f"Pulled image: {image}")

    # ECR-specific methods (merged from ECR helper)

    def create_ecr_repository(self, repository_name: str, config: RegistryConfig) -> str:
        """
        Create ECR repository

        Args:
            repository_name: Name of the repository
            config: Registry configuration

        Returns:
            Repository URI
        """
        if 'ecr' not in self._aws_clients:
            raise ValueError("ECR client not provided")

        ecr_client = self._aws_clients['ecr']

        try:
            response = ecr_client.create_repository(
                repositoryName=repository_name,
                imageTagMutability=config.image_tag_mutability,
                imageScanningConfiguration={'scanOnPush': config.scan_on_push}
            )

            repo_uri = response['repository']['repositoryUri']
            self.logger.info(f"Created ECR repository: {repo_uri}")
            return repo_uri

        except ecr_client.exceptions.RepositoryAlreadyExistsException:
            # Repository exists, get its URI
            response = ecr_client.describe_repositories(
                repositoryNames=[repository_name]
            )
            repo_uri = response['repositories'][0]['repositoryUri']
            self.logger.info(f"Using existing ECR repository: {repo_uri}")
            return repo_uri

        except Exception as e:
            raise FaasException(f"Failed to create ECR repository '{repository_name}': {str(e)}")

    def delete_ecr_repository(self, repository_name: str, force: bool = True) -> None:
        """
        Delete ECR repository

        Args:
            repository_name: Name of the repository
            force: Delete even if repository contains images
        """
        if 'ecr' not in self._aws_clients:
            raise ValueError("ECR client not provided")

        ecr_client = self._aws_clients['ecr']

        try:
            ecr_client.delete_repository(
                repositoryName=repository_name,
                force=force
            )
            self.logger.info(f"Deleted ECR repository: {repository_name}")

        except ecr_client.exceptions.RepositoryNotFoundException:
            self.logger.warning(f"ECR repository '{repository_name}' not found, skipping deletion")

        except Exception as e:
            raise FaasException(f"Failed to delete ECR repository '{repository_name}': {str(e)}")

    def build_and_push_image(self, source_path: str, repository_uri: str, image_tag: str = 'latest') -> str:
        """
        Build Docker image and push to registry

        Args:
            source_path: Path to directory containing Dockerfile
            repository_uri: Repository URI (for ECR) or image name
            image_tag: Image tag

        Returns:
            Full image URI with tag
        """
        if not os.path.exists(os.path.join(source_path, 'Dockerfile')):
            raise FaasException(f"Dockerfile not found in {source_path}")

        full_image_uri = f"{repository_uri}:{image_tag}"

        # Build image
        self._docker_build(source_path, full_image_uri)

        # Determine registry type from URI
        registry_name = None
        for name, config in self._registry_configs.items():
            if config.type == RegistryType.ECR and repository_uri.startswith(config.url or ''):
                registry_name = name
                break

        # Push image
        if registry_name:
            self.push(full_image_uri, registry_name)
        else:
            # Direct push without registry config
            cmd = ['docker', 'push', full_image_uri]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise FaasException(f"Docker push failed: {result.stderr}")

        self.logger.info(f"Successfully built and pushed image: {full_image_uri}")
        return full_image_uri

    # Private methods

    def _configure_ecr(self, config: RegistryConfig) -> Dict[str, Any]:
        """Configure ECR registry"""
        if not config.region:
            raise ValueError("ECR requires region")

        if 'ecr' not in self._aws_clients:
            self.logger.warning("ECR client not provided, some operations may fail")

        # Get account ID and construct URL
        import boto3
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()['Account']
        ecr_url = f"{account_id}.dkr.ecr.{config.region}.amazonaws.com"

        config.url = ecr_url

        return {
            'type': 'ecr',
            'url': ecr_url,
            'region': config.region
        }

    def _configure_dockerhub(self, config: RegistryConfig) -> Dict[str, Any]:
        """Configure Docker Hub registry"""
        if not config.username or not config.password:
            raise ValueError("Docker Hub requires username and password")

        return {
            'type': 'dockerhub',
            'url': 'docker.io',
            'username': config.username
        }

    def _configure_acr(self, config: RegistryConfig) -> Dict[str, Any]:
        """Configure Azure Container Registry"""
        if not config.url:
            raise ValueError("ACR requires registry URL")

        if not config.username or not config.password:
            raise ValueError("ACR requires username and password")

        return {
            'type': 'acr',
            'url': config.url,
            'username': config.username
        }

    def _configure_local(self, config: RegistryConfig) -> Dict[str, Any]:
        """Configure local registry"""
        if not config.url:
            config.url = 'localhost:5000'

        return {
            'type': 'local',
            'url': config.url
        }

    def _configure_custom(self, config: RegistryConfig) -> Dict[str, Any]:
        """Configure custom registry"""
        if not config.url:
            raise ValueError("Custom registry requires URL")

        return {
            'type': 'custom',
            'url': config.url,
            'username': config.username
        }

    def _login_ecr(self, config: RegistryConfig) -> bool:
        """Login to ECR"""
        if 'ecr' not in self._aws_clients:
            raise ValueError("ECR client not provided")

        ecr_client = self._aws_clients['ecr']

        try:
            response = ecr_client.get_authorization_token()
            auth_data = response['authorizationData'][0]

            token = auth_data['authorizationToken']
            registry_url = auth_data['proxyEndpoint']

            # Decode token
            decoded_token = base64.b64decode(token).decode('utf-8')
            username, password = decoded_token.split(':', 1)

            # Docker login
            return self._docker_login(username, password, registry_url)

        except Exception as e:
            raise FaasException(f"Failed to get ECR authorization: {str(e)}")

    def _login_dockerhub(self, config: RegistryConfig) -> bool:
        """Login to Docker Hub"""
        return self._docker_login(config.username, config.password, 'docker.io')

    def _login_acr(self, config: RegistryConfig) -> bool:
        """Login to Azure Container Registry"""
        return self._docker_login(config.username, config.password, config.url)

    def _login_custom(self, config: RegistryConfig) -> bool:
        """Login to custom registry"""
        if config.username and config.password:
            return self._docker_login(config.username, config.password, config.url)
        return True

    def _docker_login(self, username: str, password: str, registry_url: str) -> bool:
        """Execute docker login with platform-specific handling"""
        # Platform-specific keychain cleanup for macOS
        if platform.system() == 'Darwin':
            try:
                subprocess.run(
                    ['security', 'delete-internet-password', '-s', registry_url],
                    capture_output=True,
                    check=False
                )
                self.logger.debug(f"Cleared existing macOS keychain entry for {registry_url}")
            except Exception:
                pass  # Non-critical

        # Docker login
        cmd = ['docker', 'login', '--username', username, '--password-stdin', registry_url]

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate(input=password)

        if process.returncode != 0:
            platform_info = f" (Platform: {platform.system()} {platform.release()})"
            raise FaasException(f"Docker login failed{platform_info}: {stderr}")

        self.logger.info("Docker login successful")
        return True

    def _docker_build(self, source_path: str, image_uri: str) -> None:
        """Build Docker image with proper manifest format"""
        # Disable BuildKit for Lambda compatibility
        env = os.environ.copy()
        env['DOCKER_BUILDKIT'] = '0'

        cmd = ['docker', 'build', '--platform', 'linux/amd64', '-t', image_uri, source_path]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise FaasException(f"Docker build failed: {stderr}")

        self.logger.info(f"Docker build completed for {image_uri}")

    def _tag_for_registry(self, image: str, config: RegistryConfig) -> str:
        """Tag image appropriately for the registry"""
        if config.type == RegistryType.ECR:
            # Ensure image has full ECR URI
            if not image.startswith(config.url):
                parts = image.split('/')
                image_name = parts[-1] if len(parts) > 1 else image
                return f"{config.url}/{image_name}"
            return image

        elif config.type == RegistryType.DOCKERHUB:
            # Ensure image has username prefix
            if '/' not in image:
                return f"{config.username}/{image}"
            return image

        elif config.type == RegistryType.ACR:
            # Ensure image has ACR URL prefix
            if not image.startswith(config.url):
                return f"{config.url}/{image}"
            return image

        elif config.type == RegistryType.LOCAL:
            # Ensure image has local registry prefix
            if not image.startswith(config.url):
                return f"{config.url}/{image}"
            return image

        else:
            # Custom registry
            if config.url and not image.startswith(config.url):
                return f"{config.url}/{image}"
            return image

    def get_registry_config(self, registry_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a named registry"""
        config = self._registry_configs.get(registry_name)
        if not config:
            return None

        return {
            'type': config.type.value,
            'url': config.url,
            'username': config.username,
            'region': config.region
        }

    def list_ecr_images(self, repository_name: str) -> List[Dict[str, Any]]:
        """List images in ECR repository"""
        if 'ecr' not in self._aws_clients:
            raise ValueError("ECR client not provided")

        ecr_client = self._aws_clients['ecr']

        try:
            response = ecr_client.list_images(repositoryName=repository_name)
            return response['imageIds']
        except Exception as e:
            raise FaasException(f"Failed to list images in repository '{repository_name}': {str(e)}")

    def ecr_repository_exists(self, repository_name: str) -> bool:
        """Check if ECR repository exists"""
        if 'ecr' not in self._aws_clients:
            raise ValueError("ECR client not provided")

        ecr_client = self._aws_clients['ecr']

        try:
            ecr_client.describe_repositories(repositoryNames=[repository_name])
            return True
        except ecr_client.exceptions.RepositoryNotFoundException:
            return False
        except Exception:
            return False