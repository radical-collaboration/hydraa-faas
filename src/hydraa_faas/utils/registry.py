# -*- coding: utf-8 -*-
"""Registry manager for FaaS providers.

This module provides a simplified and robust interface for interacting with
various Docker container registries, including ECR, Docker Hub, and others.
It leverages the Docker SDK for Python to handle building, pushing, and
authenticating, which eliminates platform-specific complexities and makes
the module more maintainable.

Key Features:
- Uses the official Docker SDK for all Docker operations.
- Simplifies ECR authentication by delegating token management to boto3.
- Tracks detailed metrics for build and push operations from structured SDK output.
- Thread-safe for use in concurrent environments.
- Parallel builds for images with the same base layer.

Example:
    To build and push an image to ECR::

        # (Assuming aws_clients are configured boto3 clients)
        registry_manager = RegistryManager(aws_clients={'ecr': ecr_client})
        ecr_config = RegistryConfig(type=RegistryType.ECR, region='us-east-1')
        registry_manager.configure_registry('my_ecr', ecr_config)

        uri, build_m, push_m = registry_manager.build_and_push_image(
            source_path='/path/to/docker_project',
            repository_uri='123456789012.dkr.ecr.us-east-1.amazonaws.com/my-repo'
        )

"""

import base64
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import docker
from docker.errors import APIError, BuildError

from .exceptions import ECRException, FaasException


class RegistryType(Enum):
    """Supported registry types."""
    NONE = "none"
    LOCAL = "local"
    DOCKERHUB = "dockerhub"
    ECR = "ecr"
    ACR = "acr"
    CUSTOM = "custom"


@dataclass
class RegistryConfig:
    """Configuration for a container registry."""
    type: RegistryType
    url: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    region: Optional[str] = None
    repository_name: Optional[str] = None
    image_tag_mutability: str = 'MUTABLE'
    scan_on_push: bool = True


@dataclass
class BuildMetrics:
    """Metrics captured from a Docker build operation."""
    build_time_ms: float
    image_size_bytes: Optional[int] = None
    layers_count: int = 0
    cache_hits: int = 0


@dataclass
class PushMetrics:
    """Metrics captured from a Docker push operation."""
    push_time_ms: float
    layers_pushed: int = 0
    bytes_pushed: int = 0  # Note: Not easily available from SDK, defaults to 0


class RegistryManager:
    """Manages interactions with various Docker registries using the Docker SDK.

    This class simplifies building and pushing container images by providing a
    unified interface that handles authentication and metric collection for
    different registry providers.

    Attributes:
        logger: An optional logger instance.
        docker_client: An instance of the Docker SDK client.
    """

    def __init__(self, logger: Optional[Any] = None, aws_clients: Optional[Dict[str, Any]] = None,
                 build_workers: int = 20):
        """Initializes the RegistryManager.

        Args:
            logger: An optional logger instance for logging messages.
            aws_clients: An optional dictionary containing boto3 clients,
                         e.g., {'ecr': boto3.client('ecr')}.
            build_workers: Number of workers for parallel builds (default 20).

        Raises:
            FaasException: If the Docker daemon is not running or accessible.
        """
        self.logger = logger
        self._registry_configs: Dict[str, RegistryConfig] = {}
        self._aws_clients = aws_clients or {}
        self._config_lock = threading.Lock()
        self._metrics_lock = threading.Lock()

        try:
            self.docker_client = docker.from_env(timeout=60)
            self.docker_client.ping()
        except Exception as e:
            self._log_error(f"Failed to connect to Docker daemon: {e}")
            raise FaasException("Docker daemon is not running or accessible.")

        # Thread pool for parallel builds
        self.build_executor = ThreadPoolExecutor(max_workers=build_workers, thread_name_prefix="Docker_Build")

        self._last_build_metrics: Optional[BuildMetrics] = None
        self._last_push_metrics: Optional[PushMetrics] = None

        # Cache for ECR tokens (valid for 12 hours)
        self._ecr_token_cache: Dict[str, Dict[str, Any]] = {}
        self._ecr_token_lock = threading.Lock()

    def _log_info(self, message: str):
        """Logs an info message."""
        if self.logger:
            self.logger.info(message)
        else:
            print(f"[RegistryManager] {message}")

    def _log_debug(self, message: str):
        """Logs a debug message."""
        if self.logger:
            self.logger.debug(message)

    def _log_error(self, message: str):
        """Logs an error message."""
        if self.logger:
            self.logger.error(message)
        else:
            print(f"[RegistryManager ERROR] {message}")

    def configure_registry(self, name: str, config: RegistryConfig) -> None:
        """Configures a registry for future use.

        Args:
            name: A friendly name to refer to this registry configuration.
            config: The configuration details for the registry.
        """
        self._log_info(f"Configuring registry: {name} (type: {config.type.value})")
        with self._config_lock:
            self._registry_configs[name] = config

    def login(self, registry_name: str) -> bool:
        """Logs in to a configured registry.

        Args:
            registry_name: The friendly name of the configured registry.

        Returns:
            True if the login was successful.
        """
        with self._config_lock:
            config = self._registry_configs.get(registry_name)

        if not config:
            raise ValueError(f"Registry {registry_name} not configured.")

        if config.type == RegistryType.ECR:
            return self._login_ecr(config)
        elif config.type in [RegistryType.DOCKERHUB, RegistryType.ACR, RegistryType.CUSTOM]:
            if config.username and config.password:
                return self._docker_login(config.username, config.password, config.url)
        return True  # No login needed for LOCAL or NONE

    def push(self, image_uri: str, registry_name: str) -> Tuple[str, PushMetrics]:
        """Tags and pushes an image to a configured registry.

        Args:
            image_uri: The name or URI of the local image to push.
            registry_name: The friendly name of the target registry.

        Returns:
            A tuple containing the full image URI in the remote registry and
            the metrics for the push operation.
        """
        with self._config_lock:
            config = self._registry_configs.get(registry_name)
        if not config:
            raise ValueError(f"Registry {registry_name} not configured.")

        tagged_uri = self._tag_for_registry(image_uri, config)
        if tagged_uri != image_uri:
            self.docker_client.images.get(image_uri).tag(tagged_uri)
            self._log_info(f"Tagged image {image_uri} as {tagged_uri}")

        self.login(registry_name)

        push_start = time.perf_counter()
        layers_pushed = 0
        push_errors = []
        try:
            self._log_info(f"Pushing image: {tagged_uri}")
            push_log_stream = self.docker_client.images.push(tagged_uri, stream=True, decode=True)
            for line in push_log_stream:
                self._log_debug(f"Push output: {line}")
                if 'status' in line and line['status'] == 'Pushed':
                    layers_pushed += 1
                elif 'error' in line or 'errorDetail' in line:
                    error_msg = line.get('error', line.get('errorDetail', {}).get('message', 'Unknown error'))
                    push_errors.append(error_msg)
                    self._log_error(f"Push error: {error_msg}")
        except APIError as e:
            raise FaasException(f"Docker push failed: {e}")

        # Check if push actually succeeded
        if push_errors:
            raise FaasException(f"Docker push failed with errors: {'; '.join(push_errors)}")

        push_time_ms = (time.perf_counter() - push_start) * 1000
        push_metrics = PushMetrics(push_time_ms=push_time_ms, layers_pushed=layers_pushed)

        with self._metrics_lock:
            self._last_push_metrics = push_metrics

        self._log_info(f"Pushed image: {tagged_uri} in {push_time_ms:.1f}ms")
        return tagged_uri, push_metrics

    def build_and_push_image(self, source_path: str, repository_uri: str,
                             image_tag: str = 'latest') -> Tuple[str, BuildMetrics, PushMetrics]:
        """Builds a Docker image and pushes it to a registry.

        Args:
            source_path: The path to the directory containing the Dockerfile.
            repository_uri: The URI of the remote repository.
            image_tag: The tag to apply to the image.

        Returns:
            A tuple containing the full image URI and metrics for the build
            and push operations.
        """
        full_image_uri = f"{repository_uri}:{image_tag}"
        build_metrics = self._docker_build(source_path, full_image_uri)

        registry_name = self._find_registry_by_uri(repository_uri)
        if not registry_name:
            raise FaasException(f"No configured registry found for URI: {repository_uri}")

        _, push_metrics = self.push(full_image_uri, registry_name)

        self._log_info(f"Successfully built and pushed image: {full_image_uri}")
        return full_image_uri, build_metrics, push_metrics

    def build_images_parallel(self, build_requests: List[Dict[str, str]]) -> List[
        Tuple[str, BuildMetrics, Optional[Exception]]]:
        """Build multiple images in parallel, grouped by base image.

        Args:
            build_requests: List of dicts with 'source_path', 'image_uri', and optional 'base_image'

        Returns:
            List of tuples (image_uri, metrics, error) for each build request.
        """
        # Group by base image
        builds_by_base = defaultdict(list)
        for req in build_requests:
            base_image = req.get('base_image', 'default')
            builds_by_base[base_image].append(req)

        results = []
        all_futures = {}

        # Build groups in parallel
        for base_image, group in builds_by_base.items():
            self._log_info(f"Building {len(group)} images with base: {base_image}")

            for req in group:
                future = self.build_executor.submit(
                    self._docker_build,
                    req['source_path'],
                    req['image_uri']
                )
                all_futures[future] = req['image_uri']

        # Collect results
        for future in as_completed(all_futures):
            image_uri = all_futures[future]
            try:
                metrics = future.result()
                results.append((image_uri, metrics, None))
            except Exception as e:
                self._log_error(f"Failed to build {image_uri}: {e}")
                results.append((image_uri, None, e))

        return results

    def create_ecr_repository(self, repository_name: str) -> str:
        """Creates an ECR repository if it doesn't exist.

        Args:
            repository_name: The name of the ECR repository.

        Returns:
            The URI of the repository.
        """
        if 'ecr' not in self._aws_clients:
            raise ECRException("ECR client not provided.")
        ecr_client = self._aws_clients['ecr']

        try:
            response = ecr_client.create_repository(repositoryName=repository_name)
            repo_uri = response['repository']['repositoryUri']
            self._log_info(f"Created ECR repository: {repo_uri}")
            return repo_uri
        except ecr_client.exceptions.RepositoryAlreadyExistsException:
            response = ecr_client.describe_repositories(repositoryNames=[repository_name])
            repo_uri = response['repositories'][0]['repositoryUri']
            self._log_info(f"Using existing ECR repository: {repo_uri}")
            return repo_uri
        except Exception as e:
            raise ECRException(f"Failed to create ECR repository: {e}")

    def _docker_build(self, source_path: str, image_uri: str) -> BuildMetrics:
        """Builds a Docker image using the Docker SDK.

        Args:
            source_path: Path to the build context.
            image_uri: The tag to apply to the built image.

        Returns:
            Metrics for the build operation.
        """
        build_start = time.perf_counter()
        cache_hits = 0
        layers_count = 0
        self._log_info(f"Building Docker image: {image_uri} from {source_path}")

        # For multi-platform builds, we need to use buildx
        # First check if we're on a different architecture than target
        import platform as platform_module
        if platform_module.machine() in ['arm64', 'aarch64']:
            self._log_info("Detected ARM architecture, using buildx for linux/amd64 build")
            # Use subprocess for buildx as Docker SDK doesn't fully support it
            import subprocess
            build_cmd = [
                'docker', 'buildx', 'build',
                '--platform', 'linux/amd64',
                '--load',  # Load into local docker
                '-t', image_uri,
                source_path
            ]

            try:
                result = subprocess.run(build_cmd, capture_output=True, text=True, check=True)
                self._log_debug(f"Buildx output: {result.stdout}")

                # Get the image object for size calculation
                image = self.docker_client.images.get(image_uri)
                image_size_bytes = image.attrs.get('Size')

                build_time_ms = (time.perf_counter() - build_start) * 1000

                metrics = BuildMetrics(
                    build_time_ms=build_time_ms,
                    image_size_bytes=image_size_bytes,
                    layers_count=1,  # Can't easily get from buildx
                    cache_hits=0
                )

                with self._metrics_lock:
                    self._last_build_metrics = metrics
                return metrics

            except subprocess.CalledProcessError as e:
                raise FaasException(f"Docker buildx failed: {e.stderr}")

        # Original build for same architecture
        build_args = {'DOCKER_BUILDKIT': '0'}  # Disable BuildKit for Lambda compatibility

        try:
            image, build_log_stream = self.docker_client.images.build(
                path=source_path,
                tag=image_uri,
                rm=True,
                forcerm=True,
                buildargs=build_args
            )

            for line in build_log_stream:
                if 'stream' in line:
                    log_line = line['stream'].strip()
                    self._log_debug(f"Build output: {log_line}")
                    if "Step" in log_line and "/" in log_line:
                        layers_count += 1
                    if "Using cache" in log_line:
                        cache_hits += 1

            build_time_ms = (time.perf_counter() - build_start) * 1000
            image_size_bytes = image.attrs.get('Size')

            metrics = BuildMetrics(
                build_time_ms=build_time_ms,
                image_size_bytes=image_size_bytes,
                layers_count=layers_count,
                cache_hits=cache_hits
            )
            with self._metrics_lock:
                self._last_build_metrics = metrics
            return metrics

        except BuildError as e:
            error_detail = "\n".join([line.get('error', '') for line in e.build_log if 'error' in line])
            raise FaasException(f"Docker build failed: {error_detail or e.msg}")
        except APIError as e:
            raise FaasException(f"Docker API error during build: {e}")

    def _login_ecr(self, config: RegistryConfig) -> bool:
        """Logs in to Amazon ECR with token caching.

        Args:
            config: The ECR registry configuration.

        Returns:
            True if login is successful.
        """
        if 'ecr' not in self._aws_clients:
            raise ECRException("ECR client not provided for login.")

        ecr_client = self._aws_clients['ecr']

        # Check token cache
        with self._ecr_token_lock:
            cache_key = config.region or 'default'
            cached_token = self._ecr_token_cache.get(cache_key)

            if cached_token:
                # Check if token is still valid (11 hours to be safe)
                if time.time() - cached_token['timestamp'] < (11 * 60 * 60):
                    self._log_info("Using cached ECR token")
                    return self._docker_login_with_token(cached_token)

        try:
            # Get fresh authorization token
            auth_data = ecr_client.get_authorization_token()['authorizationData'][0]
            token = auth_data['authorizationToken']
            registry_url = auth_data['proxyEndpoint']

            # Decode token
            username, password = base64.b64decode(token).decode('utf-8').split(':', 1)

            # Cache token
            with self._ecr_token_lock:
                self._ecr_token_cache[cache_key] = {
                    'username': username,
                    'password': password,
                    'registry': registry_url.replace('https://', '').replace('http://', ''),
                    'timestamp': time.time()
                }

            # Extract registry without https://
            registry = registry_url.replace('https://', '').replace('http://', '')

            # Use Docker SDK login with registry parameter
            self.docker_client.login(
                username=username,
                password=password,
                registry=registry,
                reauth=True  # Force re-authentication
            )
            self._log_info(f"Docker login to {registry} successful.")
            return True
        except Exception as e:
            raise ECRException(f"Failed to get ECR authorization: {e}")

    def _docker_login_with_token(self, token_data: Dict[str, Any]) -> bool:
        """Login with cached token data."""
        try:
            self.docker_client.login(
                username=token_data['username'],
                password=token_data['password'],
                registry=token_data['registry'],
                reauth=True
            )
            return True
        except Exception:
            # Token might be expired, return False to get new one
            return False

    def _docker_login(self, username: str, password: str, registry_url: str) -> bool:
        """Logs in to a Docker registry using the Docker SDK.

        Args:
            username: The username for the registry.
            password: The password for the registry.
            registry_url: The URL of the registry.

        Returns:
            True if login is successful.
        """
        try:
            self.docker_client.login(username=username, password=password, registry=registry_url)
            self._log_info(f"Docker login to {registry_url} successful.")
            return True
        except APIError as e:
            raise FaasException(f"Docker login to {registry_url} failed: {e}")

    def _tag_for_registry(self, image: str, config: RegistryConfig) -> str:
        """Constructs the full image URI for a given registry.

        Args:
            image: The base image name and tag.
            config: The configuration of the target registry.

        Returns:
            The fully qualified image URI.
        """
        if config.type == RegistryType.ECR:
            # For ECR, the image might already have the full URI
            if config.url and config.url in image:
                return image
            # Otherwise construct it
            if config.url and not image.startswith(config.url):
                # Extract just the tag part if it includes repository name
                tag_part = image.split('/')[-1]
                return f"{config.url}/{tag_part}"
        elif config.url and not image.startswith(config.url):
            return f"{config.url.replace('https://', '').replace('http://', '')}/{image.split('/')[-1]}"
        return image

    def _find_registry_by_uri(self, uri: str) -> Optional[str]:
        """Finds a configured registry name by its URI.

        Args:
            uri: The repository URI to search for.

        Returns:
            The friendly name of the matching registry, or None.
        """
        with self._config_lock:
            for name, config in self._registry_configs.items():
                if config.type == RegistryType.ECR:
                    # For ECR, match if the registry URL is in the URI
                    # config.url format: 123456789012.dkr.ecr.us-east-1.amazonaws.com
                    # uri format: 123456789012.dkr.ecr.us-east-1.amazonaws.com/repo-name
                    if config.url and uri.startswith(config.url):
                        return name
                    # Also check with region matching as fallback
                    if '.dkr.ecr.' in uri and config.region and config.region in uri:
                        return name
                elif config.url:
                    # For other registries, match URL prefix
                    url_prefix = config.url.replace('https://', '').replace('http://', '')
                    if uri.startswith(url_prefix):
                        return name
        return None

    def shutdown(self) -> None:
        """Shutdown the registry manager and cleanup resources."""
        self.build_executor.shutdown(wait=True)
        self._log_info("Registry manager shutdown complete")