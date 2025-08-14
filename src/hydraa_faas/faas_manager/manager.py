# -*- coding: utf-8 -*-
"""FaaS Manager - Refactored to better integrate with HYDRA architecture.

This module serves as the central orchestrator for FaaS providers, following
HYDRA's established patterns for service managers.
"""

import os
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
from .aws_lambda import AwsLambda
from .custom_faas import NuclioProvider

# Provider constants
LAMBDA = 'lambda'
NUCLIO = 'nuclio'


class FaasManager:
    """Orchestrates various FaaS providers following HYDRA patterns.

    This manager integrates with HYDRA's ServiceManager and uses existing
    HYDRA components without modification.
    """

    def __init__(self,
                 proxy_mgr: proxy,
                 vms: Optional[List[Any]] = None,
                 asynchronous: bool = True,
                 auto_terminate: bool = True):
        """Initializes the FaasManager following HYDRA patterns.

        Args:
            proxy_mgr: HYDRA's proxy manager for credentials.
            vms: List of VM definitions for infrastructure.
            asynchronous: If True, tasks run in background.
            auto_terminate: If True, cleanup resources on shutdown.
        """
        self._proxy = proxy_mgr
        self.vms = vms or []
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate

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
        self._function_to_provider: Dict[str, str] = {}

        # Thread safety locks
        self._function_map_lock = threading.RLock()
        self._provider_lock = threading.RLock()

        # Thread pool for provider initialization
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="FaaS_Init")

        # Lazy loading for providers
        self._provider_factories = {}

    def start(self, sandbox: str) -> None:
        """Starts the FaaS Manager following HYDRA's pattern.

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

        self.logger.trace(f"Starting FaaS manager in {self.sandbox}")
        self.profiler.prof('faas_start', uid='manager')

        # Initialize provider factories (lazy loading)
        self._initialize_provider_factories()

        # Start worker thread for async processing
        if self.asynchronous:
            self._worker_thread = threading.Thread(
                target=self._worker,
                name='FaasWorker',
                daemon=True
            )
            self._worker_thread.start()

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
        with self._provider_lock:
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
            profiler=self.profiler
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
            profiler=self.profiler
        )
        return {'instance': provider, 'type': 'nuclio'}

    def _worker(self) -> None:
        """Worker thread for processing tasks asynchronously."""
        self.logger.trace("FaaS worker thread started")

        while not self._terminate.is_set():
            try:
                task = self.incoming_q.get(timeout=1.0)
                if task:
                    self._process_task(task)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Worker error: {e}")

    def _process_task(self, task: Task) -> None:
        """Process a single task."""
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

            # Deploy the function
            self.profiler.prof('deploy_start', uid=task.id)
            function_id = provider.deploy_function(task)
            self.profiler.prof('deploy_end', uid=task.id)

            # Track deployment with thread safety
            with self._function_map_lock:
                self._task_to_function_map[task.id] = function_id
                self._function_to_provider[function_id] = provider_name

            # Set result using Future interface
            task.set_result(function_id)

            # Report success
            msg = f"Deployed function {function_id} for task {task.id}"
            self.outgoing_q.put(msg)

        except Exception as e:
            # Use Future interface for exceptions
            task.set_exception(e)
            self.logger.error(f"Failed to process task {task.id}: {e}")
            self.outgoing_q.put(f"Task {task.id} failed: {e}")

    def _prepare_task_for_faas(self, task: Task) -> None:
        """Adapt HYDRA Task for FaaS deployment."""
        # Use existing Task attributes creatively
        if not hasattr(task, 'cmd') or not task.cmd:
            task.cmd = ['python', '-c', 'print("No function provided")']

        # Map FaaS concepts to Task attributes
        if not task.vcpus:
            task.vcpus = 0.25  # Default 256MB Lambda
        if not task.memory:
            task.memory = 256  # Default memory

        # Extract and separate FaaS config from user env vars
        faas_config = self._extract_faas_config(task)

        # Store processed FaaS config back in env_var for providers
        if not task.env_var:
            task.env_var = []

        # Add back FaaS config
        for key, value in faas_config.items():
            task.env_var.append(f"FAAS_{key.upper()}={value}")

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from FAAS_* env vars."""
        faas_config = {}
        user_env_vars = []

        if task.env_var:
            new_env_var = []
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        # FaaS configuration
                        config_key = key.replace('FAAS_', '').lower()
                        faas_config[config_key] = value
                    else:
                        # User runtime environment variable
                        user_env_vars.append(var)
                        new_env_var.append(var)
                else:
                    new_env_var.append(var)

            # Update task env_var to only contain user vars
            task.env_var = new_env_var

        # Store user env vars for providers to use
        task._user_env_vars = user_env_vars

        # Apply defaults
        faas_config.setdefault('handler', 'handler.handler')
        faas_config.setdefault('runtime', 'python3.9')
        faas_config.setdefault('timeout', '30')
        faas_config.setdefault('source', '')

        return faas_config

    def _get_task_provider(self, task: Task) -> str:
        """Determine which provider should handle the task."""
        # Check FAAS_PROVIDER env var first
        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and var.startswith('FAAS_PROVIDER='):
                    provider = var.split('=', 1)[1].lower()
                    # Ensure provider is available
                    with self._provider_lock:
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
        with self._provider_lock:
            return next(iter(self._provider_factories.keys()))

    def _wait_for_dependencies(self, task: Task) -> None:
        """Wait for task dependencies to complete."""
        for dep_id in task.depends_on:
            # Check if dependency is deployed
            with self._function_map_lock:
                if dep_id not in self._task_to_function_map:
                    raise FaasException(f"Dependency {dep_id} not found")

            # In real implementation, would check function status
            self.logger.trace(f"Task {task.id} waiting for dependency {dep_id}")

    def submit(self, tasks: Union[Task, List[Task]]) -> None:
        """Submit tasks for processing following HYDRA pattern.

        Args:
            tasks: Single task or list of tasks to deploy as functions.
        """
        if not self.status:
            raise FaasException("Manager must be started before submitting tasks")

        if not isinstance(tasks, list):
            tasks = [tasks]

        self.profiler.prof('submit_start', uid='batch')

        for task in tasks:
            # Validate task
            if not isinstance(task, Task):
                raise TypeError(f"Expected Task object, got {type(task)}")

            # Queue for processing
            if self.asynchronous:
                self.incoming_q.put(task)
            else:
                self._process_task(task)

        self.profiler.prof('submit_end', uid='batch')
        self.logger.trace(f"Submitted {len(tasks)} tasks")

    def invoke(self, function_name: str, payload: Any = None,
               provider: Optional[str] = None) -> Any:
        """Invoke a deployed function.

        Args:
            function_name: Name of the function to invoke.
            payload: JSON-serializable payload.
            provider: Specific provider to use (optional).

        Returns:
            The function response.
        """
        if not self.status:
            raise FaasException("Manager not started")

        self.profiler.prof('invoke_start', uid=function_name)

        try:
            if provider:
                provider_name = provider.lower()
                provider_dict = self._get_provider(provider_name)
                result = provider_dict['instance'].invoke_function(
                    function_name, payload
                )
            else:
                # Try to find function in any provider
                with self._function_map_lock:
                    if function_name in self._function_to_provider:
                        provider_name = self._function_to_provider[function_name]
                        provider_dict = self._get_provider(provider_name)
                        result = provider_dict['instance'].invoke_function(
                            function_name, payload
                        )
                    else:
                        # Search all providers
                        found = False
                        with self._provider_lock:
                            for provider_name in list(self._providers.keys()):
                                try:
                                    provider_dict = self._providers[provider_name]
                                    result = provider_dict['instance'].invoke_function(
                                        function_name, payload
                                    )
                                    found = True
                                    break
                                except InvocationException:
                                    continue

                        if not found:
                            raise FaasException(f"Function '{function_name}' not found")

            self.profiler.prof('invoke_end', uid=function_name)
            return result

        except Exception as e:
            self.profiler.prof('invoke_failed', uid=function_name)
            raise

    def shutdown(self) -> None:
        """Shutdown the FaaS Manager following HYDRA pattern."""
        if not self.status:
            return

        self.logger.trace("Shutting down FaaS manager")
        self.profiler.prof('shutdown_start', uid='manager')

        # Signal termination
        self._terminate.set()

        # Wait for worker thread
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=5)

        # Shutdown providers
        shutdown_futures = []
        with self._provider_lock:
            for name, provider_info in self._providers.items():
                self.logger.trace(f"Shutting down provider: {name}")
                future = self.executor.submit(provider_info['instance'].shutdown)
                shutdown_futures.append(future)

        # Wait for shutdowns
        for future in shutdown_futures:
            try:
                future.result(timeout=60)
            except Exception as e:
                self.logger.error(f"Error during provider shutdown: {e}")

        # Cleanup
        self.executor.shutdown(wait=True)
        self.status = False

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