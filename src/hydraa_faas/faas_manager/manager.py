# -*- coding: utf-8 -*-
"""FaaS Manager - Improved with integrated metrics collection.

This module serves as the central orchestrator for FaaS providers, following
HYDRA's established patterns for service managers with comprehensive metrics.
"""

import os
import json
import queue
import threading
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Union

import radical.utils as ru
from hydraa import Task, proxy
from hydraa.services.caas_manager.utils import misc

from ..utils.exceptions import FaasException, InvocationException
from ..utils.metrics_collector import MetricsCollector
from ..utils import packaging  # Import packaging module for shutdown
from .aws_lambda import AwsLambda
from .custom_faas import NuclioProvider

# Provider constants
LAMBDA = 'lambda'
NUCLIO = 'nuclio'


class FaaSManager:
    """Orchestrates various FaaS providers with integrated metrics collection.

    This manager integrates with HYDRA's ServiceManager and uses existing
    HYDRA components without modification, while providing comprehensive
    metrics collection and analysis.
    """

    def __init__(self,
                 proxy_mgr: proxy,
                 vms: Optional[List[Any]] = None,
                 asynchronous: bool = True,
                 auto_terminate: bool = True,
                 deployment_workers: int = 200,
                 invocation_workers: int = 50,
                 packaging_workers: int = 50,
                 enable_metrics: bool = True,
                 metrics_save_interval: int = 60):
        """Initializes the FaaS Manager with integrated metrics.

        Args:
            proxy_mgr: HYDRA's proxy manager for credentials.
            vms: List of VM definitions for infrastructure.
            asynchronous: If True, tasks run in background.
            auto_terminate: If True, cleanup resources on shutdown.
            deployment_workers: Max workers for deployment operations (default 200).
            invocation_workers: Max workers for invocation operations (default 50).
            packaging_workers: Max workers for packaging operations (default 50).
            enable_metrics: If True, collect performance metrics (default True).
            metrics_save_interval: Interval in seconds to save metrics (default 60).
        """
        self._proxy = proxy_mgr
        self.vms = vms or []
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.deployment_workers = deployment_workers
        self.invocation_workers = invocation_workers
        self.packaging_workers = packaging_workers
        self.enable_metrics = enable_metrics

        # HYDRA ServiceManager expectations
        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self.sandbox = None
        self.logger = None
        self.profiler = None
        self.status = False

        # FaaS specific attributes
        self._providers: Dict[str, Dict[str, Any]] = {}
        self._terminate = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._task_to_function_map: Dict[str, str] = {}

        # Performance optimization: function to provider cache
        self._function_to_provider_cache: Dict[str, str] = {}

        # Thread safety with single RLock
        self._manager_lock = threading.RLock()

        # Separate thread pools for deployments and invocations
        self.deployment_executor = ThreadPoolExecutor(
            max_workers=deployment_workers,
            thread_name_prefix="FaaS_Deploy"
        )

        self.invocation_executor = ThreadPoolExecutor(
            max_workers=invocation_workers,
            thread_name_prefix="FaaS_Invoke"
        )

        # Lazy loading for providers
        self._provider_factories = {}

        # Track pending deployments for async mode
        self._pending_deployments = []

        # Integrated metrics collector
        self.metrics_collector = None
        self._metrics_save_interval = metrics_save_interval

    def start(self, sandbox: str) -> None:
        """Starts the FaaS Manager with metrics collection.

        Called by ServiceManager to initialize the service.

        Args:
            sandbox: The directory path for logs and temporary files.
        """
        if self.status:
            return  # Already started

        # Set up sandbox following HYDRA pattern
        self.sandbox = os.path.join(sandbox, f"faas_{uuid.uuid4().hex[:8]}")
        os.makedirs(self.sandbox, exist_ok=True)

        # Use HYDRA's logger utility
        self.logger = misc.logger(path=os.path.join(self.sandbox, 'faas_manager.log'))

        # Use HYDRA's profiler
        self.profiler = ru.Profiler(name='FaasManager', path=self.sandbox)

        # Initialize metrics collector if enabled
        if self.enable_metrics:
            metrics_dir = os.path.join(self.sandbox, 'metrics')
            self.metrics_collector = MetricsCollector(
                output_dir=metrics_dir,
                save_interval_seconds=self._metrics_save_interval,
                enable_collection=True
            )
            self.logger.trace(f"Metrics collection enabled, saving to {metrics_dir}")

        self.logger.trace(f"Starting FaaS manager in {self.sandbox}")
        self.profiler.prof('faas_start', uid='manager')

        # Initialize provider factories (lazy loading)
        self._initialize_provider_factories()

        # Note: Worker thread is no longer needed for async mode
        # as we submit directly to thread pool

        self.status = True
        self.profiler.prof('faas_started', uid='manager')
        self.logger.trace("FaaS manager started successfully")

    def _initialize_provider_factories(self) -> None:
        """Initialize FaaS provider factories for lazy loading."""
        # Extract infrastructure config from VMs if available
        infra_config = self._get_infrastructure_config()

        # Store factory functions instead of creating instances immediately
        if 'aws' in self._proxy.loaded_providers:
            self._provider_factories['lambda'] = lambda: self._init_lambda_provider(
                infra_config.get('aws', {})
            )

        if self.vms:
            self._provider_factories['nuclio'] = lambda: self._init_nuclio_provider(
                infra_config.get('nuclio', {})
            )

        if not self._provider_factories:
            raise FaasException("No FaaS providers available")

    def _get_provider(self, name: str) -> Dict[str, Any]:
        """Get or create a provider instance (lazy loading)."""
        with self._manager_lock:
            if name not in self._providers and name in self._provider_factories:
                try:
                    provider_info = self._provider_factories[name]()
                    if provider_info:
                        self._providers[name] = provider_info
                        self.logger.trace(f"Initialized provider: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to initialize {name}: {e}")
                    raise

            if name not in self._providers:
                raise FaasException(f"Provider {name} not available")

            return self._providers[name]

    def _get_infrastructure_config(self) -> Dict[str, Dict[str, Any]]:
        """Extract FaaS-relevant configuration from VMs."""
        config = {'aws': {}, 'nuclio': {}}

        if not self.vms:
            return config

        # Extract AWS-specific config from AWS VMs
        aws_vms = [vm for vm in self.vms if hasattr(vm, 'Provider') and vm.Provider == 'aws']
        if aws_vms:
            vm = aws_vms[0]
            if hasattr(vm, 'IamInstanceProfile'):
                config['aws']['iam_role'] = vm.IamInstanceProfile.get('Arn')
            if hasattr(vm, 'input_kwargs'):
                config['aws']['subnet_ids'] = vm.input_kwargs.get('SubnetIds', [])
                config['aws']['security_groups'] = vm.input_kwargs.get('SecurityGroupIds', [])

        # Nuclio will use all VMs
        config['nuclio']['vms'] = self.vms

        return config

    def _init_lambda_provider(self, infra_config: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize AWS Lambda provider."""
        lambda_sandbox = os.path.join(self.sandbox, 'lambda')
        os.makedirs(lambda_sandbox, exist_ok=True)

        provider = AwsLambda(
            sandbox=lambda_sandbox,
            manager_id=uuid.uuid4().hex[:8],
            cred=self._proxy._load_credentials('aws'),
            asynchronous=self.asynchronous,
            auto_terminate=self.auto_terminate,
            log=self.logger,
            resource_config=infra_config,
            profiler=self.profiler,
            deployment_workers=self.deployment_workers,
            invocation_workers=self.invocation_workers,
            packaging_workers=self.packaging_workers,
            enable_metrics=self.enable_metrics
        )
        return {'instance': provider, 'type': 'lambda'}

    def _init_nuclio_provider(self, infra_config: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize Nuclio provider."""
        nuclio_sandbox = os.path.join(self.sandbox, 'nuclio')
        os.makedirs(nuclio_sandbox, exist_ok=True)

        # Determine credential source from VMs
        vm_provider = self.vms[0].Provider if self.vms else 'local'

        provider = NuclioProvider(
            sandbox=nuclio_sandbox,
            manager_id=uuid.uuid4().hex[:8],
            vms=infra_config.get('vms', self.vms),
            cred=self._proxy._load_credentials(vm_provider),
            asynchronous=self.asynchronous,
            auto_terminate=self.auto_terminate,
            log=self.logger,
            resource_config=infra_config,
            profiler=self.profiler,
            deployment_workers=self.deployment_workers,
            invocation_workers=self.invocation_workers,
            enable_metrics=self.enable_metrics
        )
        return {'instance': provider, 'type': 'nuclio'}

    def _process_task(self, task: Task) -> None:
        """Process a single task with metrics tracking."""
        deployment_id = None
        try:
            # Mark task as running using Future interface
            task.set_running_or_notify_cancel()

            # Prepare task for FaaS
            self._prepare_task_for_faas(task)

            # Handle dependencies
            if task.depends_on:
                self._wait_for_dependencies(task)

            # Route to provider
            provider_name = self._get_task_provider(task)
            provider_dict = self._get_provider(provider_name)
            provider = provider_dict['instance']

            # Start deployment metrics
            if self.metrics_collector:
                deployment_type = self._determine_deployment_type(task)
                deployment_id = self.metrics_collector.start_deployment(
                    function_name=task.name or f"task-{task.id}",
                    deployment_type=deployment_type
                )

            # Deploy the function
            if self.enable_metrics:
                self.profiler.prof('deploy_start', uid=task.id)

            function_id = provider.deploy_function(task)

            if self.enable_metrics:
                self.profiler.prof('deploy_end', uid=task.id)

            # Track deployment with thread safety
            with self._manager_lock:
                self._task_to_function_map[task.id] = function_id
                self._function_to_provider_cache[function_id] = provider_name

            # Complete deployment metrics
            if self.metrics_collector and deployment_id:
                self.metrics_collector.complete_deployment(
                    deployment_id,
                    status='success'
                )

            # Set result using Future interface
            task.set_result(function_id)

            # Report success
            msg = f"Deployed function {function_id} for task {task.id}"
            self.outgoing_q.put(msg)

        except Exception as e:
            # Complete deployment metrics with failure
            if self.metrics_collector and deployment_id:
                self.metrics_collector.complete_deployment(
                    deployment_id,
                    status='failed',
                    error_message=str(e)
                )

            # Use Future interface for exceptions
            task.set_exception(e)
            self.logger.error(f"Failed to process task {task.id}: {e}")
            self.outgoing_q.put(f"Task {task.id} failed: {e}")

    def _determine_deployment_type(self, task: Task) -> str:
        """Determine deployment type from task configuration."""
        faas_config = self._extract_faas_config(task)

        # Check if it's a prebuilt image
        if hasattr(task, 'image') and task.image and self._is_full_image_uri(task.image):
            return 'prebuilt-image'

        # Check for source + image = container build
        if faas_config.get('source') and hasattr(task, 'image') and task.image:
            return 'container-build'

        # Check for source only = zip
        if faas_config.get('source'):
            return 'zip'

        # Check for inline code
        if faas_config.get('inline_code'):
            if hasattr(task, 'image') and task.image:
                return 'container-build'
            return 'zip'

        return 'unknown'

    def _is_full_image_uri(self, image: str) -> bool:
        """Check if image is a full URI."""
        if not image:
            return False
        base_images = ['python:3.9', 'python:3.10', 'python:3.11', 'python:3.12']
        if image in base_images:
            return False
        return ('/' in image or
                ':' in image and not image.startswith('python:') or
                '.amazonaws.com' in image or
                '.azurecr.io' in image)

    def _prepare_task_for_faas(self, task: Task) -> None:
        """Adapt HYDRA Task for FaaS deployment."""
        # Don't add default cmd - let it fail if not properly configured

        # Map FaaS concepts to Task attributes
        if not task.memory:
            task.memory = 256  # Default memory

        # Extract and process FaaS config
        faas_config = self._extract_faas_config(task)

        # Store processed FaaS config back in env_var for providers
        if not task.env_var:
            task.env_var = []

        # Store user env vars separately
        user_env_vars = []
        new_env_var = []

        # Process env vars to separate FaaS config from user vars
        for var in list(task.env_var):  # Create a copy to iterate over
            if isinstance(var, str) and '=' in var:
                key, value = var.split('=', 1)
                if not key.startswith('FAAS_'):
                    user_env_vars.append(var)
                    new_env_var.append(var)
                # FAAS_ vars are already in faas_config
            else:
                new_env_var.append(var)

        # Store user env vars for providers to use
        task._user_env_vars = user_env_vars

        # Update task env_var to only contain user vars
        task.env_var = new_env_var

        # Re-add FaaS config to env_var for provider compatibility
        for key, value in faas_config.items():
            task.env_var.append(f"FAAS_{key.upper()}={value}")

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from FAAS_* env vars."""
        faas_config = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        # FaaS configuration
                        config_key = key.replace('FAAS_', '').lower()
                        faas_config[config_key] = value

        # Apply defaults (but not for inline code)
        if 'handler' not in faas_config and 'inline_code' not in faas_config:
            faas_config.setdefault('handler', 'handler.handler')
        faas_config.setdefault('runtime', 'python3.9')
        faas_config.setdefault('timeout', '30')

        return faas_config

    def _get_task_provider(self, task: Task) -> str:
        """Determine which provider should handle the task."""
        # Check FAAS_PROVIDER env var first
        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and var.startswith('FAAS_PROVIDER='):
                    provider = var.split('=', 1)[1].lower()
                    # Ensure provider is available
                    with self._manager_lock:
                        if provider not in self._provider_factories:
                            raise FaasException(f"Requested provider '{provider}' not available")
                    return provider

        # Auto-detect from image URI
        if hasattr(task, 'image') and task.image:
            if '.dkr.ecr.' in task.image and 'lambda' in self._provider_factories:
                return 'lambda'
            elif task.image and 'nuclio' in self._provider_factories:
                return 'nuclio'

        # Default to first available provider
        with self._manager_lock:
            return next(iter(self._provider_factories.keys()))

    def _wait_for_dependencies(self, task: Task) -> None:
        """Wait for task dependencies to complete."""
        for dep_id in task.depends_on:
            # Check if dependency is deployed
            with self._manager_lock:
                if dep_id not in self._task_to_function_map:
                    raise FaasException(f"Dependency {dep_id} not found")

            # In real implementation, would check function status
            self.logger.trace(f"Task {task.id} waiting for dependency {dep_id}")

    def submit(self, tasks: Union[Task, List[Task]]) -> None:
        """Submit tasks for processing with metrics tracking.

        Args:
            tasks: Single task or list of tasks to deploy as functions.
        """
        if not self.status:
            raise FaasException("Manager must be started before submitting tasks")

        if not isinstance(tasks, list):
            tasks = [tasks]

        if self.enable_metrics:
            self.profiler.prof('submit_start', uid='batch')

        if self.asynchronous:
            # For async mode, submit directly to thread pool for true parallelism
            futures = []
            for task in tasks:
                if not isinstance(task, Task):
                    raise TypeError(f"Expected Task object, got {type(task)}")

                future = self.deployment_executor.submit(self._process_task, task)
                futures.append((task, future))

            # Store futures for tracking if needed
            self._pending_deployments.extend(futures)
        else:
            # Synchronous mode - process sequentially
            for task in tasks:
                if not isinstance(task, Task):
                    raise TypeError(f"Expected Task object, got {type(task)}")
                self._process_task(task)

        if self.enable_metrics:
            self.profiler.prof('submit_end', uid='batch')

        self.logger.trace(f"Submitted {len(tasks)} tasks")

    def invoke(self, function_name: str, payload: Any = None,
               provider: Optional[str] = None) -> Any:
        """Invoke a deployed function with metrics tracking.

        Args:
            function_name: Name of the function to invoke.
            payload: JSON-serializable payload.
            provider: Specific provider to use (optional).

        Returns:
            The function response.
        """
        if not self.status:
            raise FaasException("Manager not started")

        invocation_id = None
        try:
            # Start invocation metrics
            if self.metrics_collector:
                payload_size = len(json.dumps(payload)) if payload else 0
                invocation_id = self.metrics_collector.start_invocation(
                    function_name=function_name,
                    payload_size=payload_size
                )

            # Performance optimization: Check cache first
            if not provider:
                with self._manager_lock:
                    provider = self._function_to_provider_cache.get(function_name)

            if provider:
                # Fast path: we know which provider has this function
                provider_dict = self._get_provider(provider)
                result = provider_dict['instance'].invoke_function(
                    function_name, payload
                )
            else:
                # Slow path: search all providers
                found = False
                with self._manager_lock:
                    provider_names = list(self._providers.keys())

                for provider_name in provider_names:
                    try:
                        provider_dict = self._get_provider(provider_name)
                        result = provider_dict['instance'].invoke_function(
                            function_name, payload
                        )
                        # Cache for next time
                        with self._manager_lock:
                            self._function_to_provider_cache[function_name] = provider_name
                        found = True
                        break
                    except InvocationException:
                        continue

                if not found:
                    raise FaasException(f"Function '{function_name}' not found")

            # Complete invocation metrics
            if self.metrics_collector and invocation_id:
                self.metrics_collector.complete_invocation(
                    invocation_id,
                    status='success'
                )

            return result

        except Exception as e:
            # Complete invocation metrics with failure
            if self.metrics_collector and invocation_id:
                self.metrics_collector.complete_invocation(
                    invocation_id,
                    status='failed',
                    error_message=str(e)
                )
            raise

    def invoke_parallel(self, invocations: List[Dict[str, Any]]) -> List[Any]:
        """Invoke multiple functions in parallel with metrics tracking.

        Args:
            invocations: List of dicts with 'function_name', 'payload', and optional 'provider'

        Returns:
            List of results in the same order as invocations.
        """
        if not self.status:
            raise FaasException("Manager not started")

        # Track metrics for all invocations
        invocation_ids = []
        if self.metrics_collector:
            for inv in invocations:
                payload_size = len(json.dumps(inv.get('payload'))) if inv.get('payload') else 0
                inv_id = self.metrics_collector.start_invocation(
                    function_name=inv['function_name'],
                    payload_size=payload_size
                )
                invocation_ids.append(inv_id)

        # Submit all invocations to the invocation thread pool
        futures = []
        for inv in invocations:
            future = self.invocation_executor.submit(
                self.invoke,
                inv['function_name'],
                inv.get('payload'),
                inv.get('provider')
            )
            futures.append(future)

        # Collect results and complete metrics
        results = []
        for i, future in enumerate(futures):
            try:
                result = future.result()
                results.append(result)

                # Complete metrics for successful invocation
                if self.metrics_collector and i < len(invocation_ids):
                    self.metrics_collector.complete_invocation(
                        invocation_ids[i],
                        status='success'
                    )
            except Exception as e:
                results.append({'error': str(e)})

                # Complete metrics for failed invocation
                if self.metrics_collector and i < len(invocation_ids):
                    self.metrics_collector.complete_invocation(
                        invocation_ids[i],
                        status='failed',
                        error_message=str(e)
                    )

        return results

    def get_metrics(self) -> Optional[MetricsCollector]:
        """Get the metrics collector instance.

        Returns:
            The metrics collector if enabled, None otherwise.
        """
        return self.metrics_collector

    def get_deployment_stats(self) -> Any:
        """Get deployment statistics from metrics collector.

        Returns:
            Pandas DataFrame with deployment statistics or None.
        """
        if self.metrics_collector:
            return self.metrics_collector.get_deployment_statistics()
        return None

    def get_invocation_stats(self) -> Any:
        """Get invocation statistics from metrics collector.

        Returns:
            Pandas DataFrame with invocation statistics or None.
        """
        if self.metrics_collector:
            return self.metrics_collector.get_invocation_statistics()
        return None

    def save_metrics(self, filename: str = "faas_metrics.json") -> Optional[str]:
        """Save current metrics to file.

        Args:
            filename: Name of the file to save metrics to.

        Returns:
            Path to saved file or None if metrics disabled.
        """
        if self.metrics_collector:
            return str(self.metrics_collector.save_results(filename))
        return None

    def shutdown(self) -> None:
        """Shutdown the FaaS Manager with proper metrics cleanup."""
        if not self.status:
            return

        self.logger.trace("Shutting down FaaS manager")
        if self.enable_metrics:
            self.profiler.prof('shutdown_start', uid='manager')

        # Signal termination
        self._terminate.set()

        # Wait for pending deployments if any
        for task, future in self._pending_deployments:
            try:
                future.result(timeout=30)
            except Exception as e:
                self.logger.error(f"Error waiting for task {task.id}: {e}")

        # Save final metrics before shutting down providers
        if self.metrics_collector:
            self.logger.trace("Saving final metrics before shutdown")
            final_metrics_path = self.save_metrics("final_metrics.json")
            if final_metrics_path:
                self.logger.trace(f"Final metrics saved to: {final_metrics_path}")

        # Shutdown providers
        shutdown_futures = []
        with self._manager_lock:
            for name, provider_info in self._providers.items():
                self.logger.trace(f"Shutting down provider: {name}")
                future = self.deployment_executor.submit(provider_info['instance'].shutdown)
                shutdown_futures.append(future)

        # Wait for shutdowns
        for future in shutdown_futures:
            try:
                future.result(timeout=60)
            except Exception as e:
                self.logger.error(f"Error during provider shutdown: {e}")

        # Shutdown metrics collector
        if self.metrics_collector:
            self.logger.trace("Shutting down metrics collector")
            self.metrics_collector.shutdown()

        # Cleanup thread pools
        self.deployment_executor.shutdown(wait=True)
        self.invocation_executor.shutdown(wait=True)

        # Shutdown the global packaging dependency installer
        packaging.shutdown_packaging()

        self.status = False

        if self.enable_metrics:
            self.profiler.prof('shutdown_end', uid='manager')

        self.logger.trace("FaaS manager shutdown complete")

    def __call__(self, func: Callable = None, provider: str = '') -> Callable:
        """Decorator for automatic function deployment.

        Args:
            func: Function that returns a Task object.
            provider: Target provider name.

        Returns:
            Wrapper function.
        """
        if func is None:
            return lambda f: self.__call__(f, provider)

        def wrapper(*args, **kwargs):
            task = func(*args, **kwargs)
            if not isinstance(task, Task):
                raise TypeError("Decorated function must return a Task object")

            # Set provider via env var if specified
            if provider and not any(v.startswith('FAAS_PROVIDER=') for v in (task.env_var or [])):
                if not task.env_var:
                    task.env_var = []
                task.env_var.append(f'FAAS_PROVIDER={provider}')

            self.submit(task)
            return task

        return wrapper