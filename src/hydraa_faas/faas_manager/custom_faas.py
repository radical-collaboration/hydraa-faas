# -*- coding: utf-8 -*-
"""Nuclio provider - Refactored for better HYDRA integration.

This module handles Nuclio FaaS deployments on Kubernetes clusters,
following HYDRA patterns and conventions.
"""

import asyncio
import aiohttp
import json
import os
import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import nuclio
import yaml
from hydraa import Task
from hydraa.services.caas_manager.kubernetes import kubernetes

from ..utils.exceptions import DeploymentException, InvocationException, NuclioException
from ..utils.registry import RegistryManager, RegistryConfig, RegistryType
from ..utils.resource_manager import ResourceManager


class NuclioProvider:
    """Nuclio FaaS provider following HYDRA patterns."""

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 vms: List[Any],
                 cred: Dict[str, Any],
                 asynchronous: bool,
                 auto_terminate: bool,
                 log: Any,
                 resource_config: Dict[str, Any],
                 profiler: Any):
        """Initialize Nuclio provider.

        Args:
            sandbox: Directory for temporary files.
            manager_id: Unique manager identifier.
            vms: List of VM definitions for Kubernetes.
            cred: Credentials for VM provider.
            asynchronous: Whether to process asynchronously.
            auto_terminate: Whether to cleanup on shutdown.
            log: HYDRA logger instance.
            resource_config: Provider configuration.
            profiler: HYDRA profiler instance.
        """
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.vms = vms
        self.logger = log
        self.profiler = profiler
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_config = resource_config

        # Registry and resource management
        self.resource_manager = ResourceManager(logger=self.logger)
        self.registry_manager = RegistryManager(logger=self.logger)

        # Kubernetes cluster (will be initialized)
        self.k8s_cluster: Optional[kubernetes.K8sCluster] = None
        self.dashboard_url: Optional[str] = None
        self.dashboard_port = 8070
        self._port_forward_process = None

        # Function tracking
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._functions_lock = threading.RLock()

        # Processing queue for async mode
        self.incoming_q = queue.Queue()
        self._terminate = threading.Event()

        # Thread pool
        self.executor = ThreadPoolExecutor(
            max_workers=5,
            thread_name_prefix="Nuclio"
        )

        # Initialize Nuclio environment
        self._setup_nuclio()

        # Start worker if async
        if self.asynchronous:
            self.worker_thread = threading.Thread(
                target=self._worker,
                name='NuclioWorker',
                daemon=True
            )
            self.worker_thread.start()

    def _setup_nuclio(self) -> None:
        """Set up Kubernetes and Nuclio following HYDRA patterns."""
        self.logger.trace("Setting up Nuclio environment")
        self.profiler.prof('nuclio_setup_start', uid=self.manager_id)

        try:
            # Initialize Kubernetes cluster using HYDRA's kubernetes module
            self._setup_kubernetes()

            # Install Nuclio with retry logic
            self._install_nuclio(max_retries=3)

            # Setup dashboard access with async check
            self._setup_dashboard_access()

            # Configure registry
            self._configure_registry()

            self.profiler.prof('nuclio_setup_end', uid=self.manager_id)
            self.logger.trace("Nuclio environment ready")

        except Exception as e:
            self.profiler.prof('nuclio_setup_failed', uid=self.manager_id)
            raise DeploymentException(f"Failed to setup Nuclio: {e}")

    def _setup_kubernetes(self) -> None:
        """Initialize Kubernetes cluster using HYDRA patterns."""
        self.logger.trace("Initializing Kubernetes cluster for Nuclio")

        # Use HYDRA's kubernetes module
        self.k8s_cluster = kubernetes.K8sCluster(
            run_id=f"nuclio-{self.manager_id}",
            vms=self.vms,
            sandbox=self.sandbox,
            log=self.logger
        )

        # Bootstrap the cluster
        self.k8s_cluster.bootstrap()

        # Wait for cluster to be ready
        timeout = 30
        start_time = time.time()
        while self.k8s_cluster.status != 'RUNNING':
            if time.time() - start_time > timeout:
                raise DeploymentException("Kubernetes cluster failed to start")
            time.sleep(2)

    def _install_nuclio(self, max_retries: int = 3) -> None:
        """Install Nuclio on the Kubernetes cluster with retry logic."""
        self.logger.trace("Installing Nuclio")

        install_commands = [
            "kubectl create namespace nuclio",
            "kubectl apply -f https://raw.githubusercontent.com/nuclio/nuclio/master/hack/k8s/resources/nuclio-rbac.yaml",
            "kubectl apply -f https://raw.githubusercontent.com/nuclio/nuclio/master/hack/k8s/resources/nuclio.yaml"
        ]

        for attempt in range(max_retries):
            try:
                for cmd in install_commands:
                    try:
                        from hydraa.services.caas_manager.utils.misc import sh_callout
                        out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)
                        if ret and "already exists" not in err:
                            raise DeploymentException(f"Command failed: {cmd}")
                    except Exception as e:
                        if "already exists" not in str(e):
                            raise

                # Wait for Nuclio to be ready
                self._wait_for_nuclio()
                return  # Success

            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Nuclio installation attempt {attempt + 1} failed: {e}")
                    time.sleep(10 * (attempt + 1))  # Exponential backoff
                else:
                    raise DeploymentException(f"Failed to install Nuclio after {max_retries} attempts: {e}")

    def _wait_for_nuclio(self) -> None:
        """Wait for Nuclio components to be ready."""
        self.logger.trace("Waiting for Nuclio to be ready")

        from hydraa.services.caas_manager.utils.misc import sh_callout

        timeout = 120
        start_time = time.time()

        while time.time() - start_time < timeout:
            cmd = "kubectl get pods -n nuclio -o json"
            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

            if not ret:
                try:
                    pods_data = json.loads(out)
                    all_ready = all(
                        pod['status']['phase'] == 'Running'
                        for pod in pods_data.get('items', [])
                    )
                    if all_ready and pods_data.get('items'):
                        self.logger.trace("Nuclio is ready")
                        return
                except json.JSONDecodeError:
                    pass

            time.sleep(5)

        raise DeploymentException("Nuclio failed to become ready")

    async def _check_dashboard_async(self, timeout: int = 30) -> bool:
        """Asynchronously check if Nuclio dashboard is accessible."""
        start_time = time.time()

        async with aiohttp.ClientSession() as session:
            while time.time() - start_time < timeout:
                try:
                    async with session.get(
                            f"http://localhost:{self.dashboard_port}/api/healthz",
                            timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status == 200:
                            return True
                except:
                    pass

                await asyncio.sleep(1)

        return False

    def _setup_dashboard_access(self) -> None:
        """Set up access to Nuclio dashboard with async check."""
        self.logger.trace("Setting up Nuclio dashboard access")

        # Start port forwarding
        from hydraa.services.caas_manager.utils.misc import sh_callout

        cmd = f"kubectl port-forward -n nuclio deployment/nuclio-dashboard {self.dashboard_port}:8070"

        import subprocess
        self._port_forward_process = subprocess.Popen(
            cmd.split(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # Use async check for better performance
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            if loop.run_until_complete(self._check_dashboard_async()):
                self.dashboard_url = f"http://localhost:{self.dashboard_port}"
                self.logger.trace(f"Nuclio dashboard available at {self.dashboard_url}")
            else:
                raise DeploymentException("Nuclio dashboard failed to become accessible")
        finally:
            loop.close()

    def _configure_registry(self) -> None:
        """Configure container registry for Nuclio."""
        reg_config = self.resource_config.get('registry', {})

        if reg_config.get('url'):
            config = RegistryConfig(
                type=RegistryType(reg_config.get('type', 'custom')),
                url=reg_config['url'],
                username=reg_config.get('username'),
                password=reg_config.get('password')
            )
            self.registry_manager.configure_registry('nuclio_registry', config)
            self.logger.trace(f"Configured registry: {config.url}")
        else:
            # Use local registry by default
            self.logger.trace("Using local Docker registry")

    def _worker(self) -> None:
        """Worker thread for async processing."""
        while not self._terminate.is_set():
            try:
                task = self.incoming_q.get(timeout=1)
                if task:
                    self.deploy_function(task)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Nuclio worker error: {e}")

    def deploy_function(self, task: Task) -> str:
        """Deploy a function to Nuclio from a HYDRA Task.

        Args:
            task: HYDRA Task object.

        Returns:
            Function identifier.
        """
        self.profiler.prof('nuclio_deploy_start', uid=str(task.id))

        try:
            # Extract configuration
            faas_config = self._extract_faas_config(task)

            # Validate configuration
            self._validate_faas_config(faas_config)

            # Generate function name
            function_name = self._generate_function_name(task)

            # Build container image
            image_uri = self._build_image(task, function_name, faas_config)

            # Deploy to Nuclio
            self._deploy_to_nuclio(task, function_name, image_uri, faas_config)

            # Track deployment
            with self._functions_lock:
                self._functions[function_name] = {
                    'task_id': str(task.id),
                    'image_uri': image_uri,
                    'original_name': task.name or f"task-{task.id}"
                }

            self.profiler.prof('nuclio_deploy_end', uid=str(task.id))
            self.logger.trace(f"Deployed Nuclio function: {function_name}")

            return function_name

        except Exception as e:
            self.profiler.prof('nuclio_deploy_failed', uid=str(task.id))
            raise DeploymentException(f"Nuclio deployment failed: {e}")

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from Task's env_var."""
        config = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        config[key.replace('FAAS_', '').lower()] = value

        # Defaults
        config.setdefault('handler', 'main:handler')
        config.setdefault('runtime', 'python:3.9')
        config.setdefault('source', '')

        return config

    def _validate_faas_config(self, config: Dict[str, Any]) -> None:
        """Validate FaaS configuration."""
        # Validate handler format
        handler = config.get('handler', '')
        if not handler or ':' not in handler:
            raise DeploymentException(f"Invalid handler format: {handler}. Expected 'module:function'")

        # Validate runtime
        runtime = config.get('runtime', '')
        valid_runtimes = ['python:3.9', 'python:3.10', 'python:3.11', 'golang:1.19']
        if not any(runtime.startswith(r) for r in valid_runtimes):
            raise DeploymentException(f"Invalid runtime: {runtime}")

    def _generate_function_name(self, task: Task) -> str:
        """Generate Nuclio-compatible function name."""
        base_name = task.name or f"task-{task.id}"
        # Kubernetes naming constraints
        clean_name = ''.join(c for c in base_name.lower() if c.isalnum() or c == '-')
        return f"nuclio-{self.manager_id}-{clean_name}"[:63]  # K8s limit

    def _build_image(self, task: Task, function_name: str,
                     faas_config: Dict[str, Any]) -> str:
        """Build container image for Nuclio function."""
        # Check if using pre-built image
        if task.image and not task.image.startswith('python:'):
            return task.image

        # Build from source
        source_path = faas_config.get('source', '')
        if not source_path:
            # Create from inline code if available
            if task.cmd and len(task.cmd) > 2 and task.cmd[0] == 'python':
                source_path = self._create_source_from_task(task)
            else:
                raise DeploymentException("No source code provided")

        # Get registry configuration
        repo_config = self.registry_manager.get_registry_config('nuclio_registry')
        if repo_config and repo_config.get('url'):
            repo_uri = repo_config['url']
        else:
            # Use local registry
            repo_uri = "localhost:5000/nuclio"

        # Build and push image
        image_uri, _, _ = self.registry_manager.build_and_push_image(
            source_path=source_path,
            repository_uri=repo_uri,
            image_tag=function_name
        )

        return image_uri

    def _create_source_from_task(self, task: Task) -> str:
        """Create source directory from task's inline code."""
        source_dir = os.path.join(self.sandbox, f"task_{task.id}_source")
        os.makedirs(source_dir, exist_ok=True)

        # Extract code from task.cmd
        code = task.cmd[2] if len(task.cmd) > 2 else ''

        # Create handler file
        handler_file = os.path.join(source_dir, "handler.py")
        with open(handler_file, 'w') as f:
            f.write(code)

        # Create Dockerfile for Nuclio
        dockerfile = os.path.join(source_dir, "Dockerfile")
        with open(dockerfile, 'w') as f:
            f.write(f"""FROM python:3.9-slim
WORKDIR /app
COPY handler.py .
RUN pip install nuclio-sdk
CMD ["python", "handler.py"]
""")

        return source_dir

    def _deploy_to_nuclio(self, task: Task, function_name: str,
                          image_uri: str, faas_config: Dict[str, Any]) -> None:
        """Deploy function to Nuclio platform."""
        self.logger.trace(f"Deploying to Nuclio: {function_name}")

        # Build Nuclio function spec
        spec = nuclio.ConfigSpec(
            env=[],
            config={
                "spec": {
                    "handler": faas_config.get('handler', 'main:handler'),
                    "runtime": "python:3.9",
                    "image": image_uri,
                    "resources": {
                        "requests": {
                            "cpu": f"{int(task.vcpus * 1000)}m",
                            "memory": f"{task.memory}Mi"
                        },
                        "limits": {
                            "cpu": f"{int(task.vcpus * 1000)}m",
                            "memory": f"{task.memory}Mi"
                        }
                    }
                }
            }
        )

        # Add environment variables
        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var and not var.startswith('FAAS_'):
                    key, value = var.split('=', 1)
                    spec.set_env(key, value)

        # Deploy using Nuclio SDK with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                nuclio.deploy_file(
                    dashboard_url=self.dashboard_url,
                    name=function_name,
                    project='default',
                    spec=spec,
                    tag="latest"
                )
                return  # Success
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"Nuclio deployment attempt {attempt + 1} failed: {e}")
                    time.sleep(5 * (attempt + 1))
                else:
                    raise NuclioException(f"Failed to deploy to Nuclio after {max_retries} attempts: {e}")

    def invoke_function(self, function_name: str, payload: Any = None) -> Dict[str, Any]:
        """Invoke a Nuclio function.

        Args:
            function_name: Function name or identifier.
            payload: JSON-serializable payload.

        Returns:
            Function response.
        """
        self.profiler.prof('nuclio_invoke_start', uid=function_name)

        try:
            # Find full function name
            full_name = None
            with self._functions_lock:
                if function_name in self._functions:
                    full_name = function_name
                else:
                    # Search by original name or task ID
                    for fname, fdata in self._functions.items():
                        if (fdata['task_id'] == function_name or
                                fdata['original_name'] == function_name):
                            full_name = fname
                            break

            if not full_name:
                raise InvocationException(f"Function '{function_name}' not found")

            # Invoke via Nuclio SDK
            response = nuclio.invoke(
                dashboard_url=self.dashboard_url,
                name=full_name,
                body=json.dumps(payload or {})
            )

            result = {
                'statusCode': response.status_code,
                'payload': response.text,
                'headers': dict(response.headers)
            }

            self.profiler.prof('nuclio_invoke_end', uid=function_name)
            return result

        except Exception as e:
            self.profiler.prof('nuclio_invoke_failed', uid=function_name)
            raise InvocationException(f"Failed to invoke '{function_name}': {e}")

    def shutdown(self) -> None:
        """Shutdown provider and cleanup resources."""
        self.logger.trace("Shutting down Nuclio provider")
        self.profiler.prof('nuclio_shutdown_start', uid=self.manager_id)

        # Signal termination
        self._terminate.set()

        # Wait for worker
        if self.asynchronous and hasattr(self, 'worker_thread'):
            self.worker_thread.join(timeout=5)

        # Cleanup functions if auto_terminate
        if self.auto_terminate:
            with self._functions_lock:
                function_names = list(self._functions.keys())

            for name in function_names:
                try:
                    nuclio.delete_function(
                        dashboard_url=self.dashboard_url,
                        name=name
                    )
                    self.logger.trace(f"Deleted Nuclio function: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete {name}: {e}")

            # Shutdown Kubernetes cluster
            if self.k8s_cluster:
                self.k8s_cluster.shutdown()

        # Stop port forwarding
        if self._port_forward_process:
            self._port_forward_process.terminate()
            self._port_forward_process.wait(timeout=5)

        # Shutdown executor
        self.executor.shutdown(wait=True)

        self.profiler.prof('nuclio_shutdown_end', uid=self.manager_id)
        self.logger.trace("Nuclio provider shutdown complete")