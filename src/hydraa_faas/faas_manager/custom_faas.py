# -*- coding: utf-8 -*-
"""Nuclio provider - Simplified resource provisioning.

This module handles Nuclio FaaS deployments on Kubernetes clusters,
with automatic resource detection and required registry specification
for container builds.
"""

import asyncio
import aiohttp
import base64
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

# Python runtime constants
SUPPORTED_PYTHON_RUNTIMES = ['python:3.9', 'python:3.10', 'python:3.11', 'python:3.12']
DEFAULT_PYTHON_RUNTIME = 'python:3.9'
BASE_PYTHON_IMAGES = ['python:3.9', 'python:3.10', 'python:3.11', 'python:3.12']


class NuclioProvider:
    """Nuclio FaaS provider with simplified resource provisioning."""

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 vms: List[Any],
                 cred: Dict[str, Any],
                 asynchronous: bool,
                 auto_terminate: bool,
                 log: Any,
                 resource_config: Dict[str, Any],
                 profiler: Any,
                 deployment_workers: int = 200,
                 invocation_workers: int = 50,
                 enable_metrics: bool = True):
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
            deployment_workers: Max workers for deployment operations.
            invocation_workers: Max workers for invocation operations.
            enable_metrics: Whether to collect performance metrics.
        """
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.vms = vms
        self.logger = log
        self.profiler = profiler
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_config = resource_config
        self.enable_metrics = enable_metrics

        # Registry manager (no default registry setup)
        self.registry_manager = RegistryManager(logger=self.logger)
        self.resource_manager = ResourceManager(logger=self.logger)

        # Kubernetes cluster (from VMs)
        self.k8s_cluster: Optional[kubernetes.K8sCluster] = None
        self.dashboard_url: Optional[str] = None
        self.dashboard_port = 8070
        self._port_forward_process = None

        # Function tracking with single lock
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._provider_lock = threading.RLock()

        # Processing queue for async mode
        self.incoming_q = queue.Queue()
        self._terminate = threading.Event()

        # Separate thread pools for deployments and invocations
        self.deployment_executor = ThreadPoolExecutor(
            max_workers=deployment_workers,
            thread_name_prefix="Nuclio_Deploy"
        )

        self.invocation_executor = ThreadPoolExecutor(
            max_workers=invocation_workers,
            thread_name_prefix="Nuclio_Invoke"
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
        if self.enable_metrics:
            self.profiler.prof('nuclio_setup_start', uid=self.manager_id)

        try:
            # Initialize Kubernetes cluster from VMs
            self._setup_kubernetes()

            # Install Nuclio with retry logic
            self._install_nuclio(max_retries=3)

            # Setup dashboard access with async check
            self._setup_dashboard_access()

            # No default registry configuration - determined per task

            if self.enable_metrics:
                self.profiler.prof('nuclio_setup_end', uid=self.manager_id)
            self.logger.trace("Nuclio environment ready")

        except Exception as e:
            if self.enable_metrics:
                self.profiler.prof('nuclio_setup_failed', uid=self.manager_id)
            raise DeploymentException(f"Failed to setup Nuclio: {e}")

    def _setup_kubernetes(self) -> None:
        """Initialize Kubernetes cluster using HYDRA patterns."""
        self.logger.trace("Initializing Kubernetes cluster for Nuclio")

        # Use HYDRA's kubernetes module with VMs as source of truth
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
        if self.enable_metrics:
            self.profiler.prof('nuclio_deploy_start', uid=str(task.id))

        try:
            # Extract configuration
            faas_config = self._extract_faas_config(task)

            # Generate function name
            function_name = self._generate_function_name(task)

            # Determine deployment type automatically
            deployment_type = self._determine_deployment_type(task, faas_config)
            self.logger.trace(f"Auto-detected deployment type: {deployment_type} for task {task.id}")

            # Deploy based on type
            if deployment_type == 'prebuilt-image':
                # Deploy with prebuilt image
                self._deploy_prebuilt_image(task, function_name, faas_config)
            elif deployment_type == 'source-build':
                # Build from source using nuctl
                self._deploy_from_source(task, function_name, faas_config)
            elif deployment_type == 'inline-code':
                # Deploy inline code
                self._deploy_inline_code(task, function_name, faas_config)
            else:
                raise DeploymentException(f"Unknown deployment type: {deployment_type}")

            # Track deployment
            with self._provider_lock:
                self._functions[function_name] = {
                    'task_id': str(task.id),
                    'original_name': task.name or f"task-{task.id}",
                    'deployment_type': deployment_type
                }

            if self.enable_metrics:
                self.profiler.prof('nuclio_deploy_end', uid=str(task.id))
            self.logger.trace(f"Deployed Nuclio function: {function_name}")

            return function_name

        except Exception as e:
            if self.enable_metrics:
                self.profiler.prof('nuclio_deploy_failed', uid=str(task.id))
            raise DeploymentException(f"Nuclio deployment failed: {e}")

    def deploy_batch_optimized(self, tasks: List[Task]) -> List[str]:
        """Deploy multiple functions in parallel with optimizations."""
        if self.enable_metrics:
            self.profiler.prof('nuclio_batch_start', uid=self.manager_id)

        # Group tasks by deployment type
        tasks_by_type = {}
        for task in tasks:
            faas_config = self._extract_faas_config(task)
            dep_type = self._determine_deployment_type(task, faas_config)
            tasks_by_type.setdefault(dep_type, []).append(task)

        results = []
        all_futures = {}

        # Deploy groups in parallel
        for dep_type, task_group in tasks_by_type.items():
            for task in task_group:
                future = self.deployment_executor.submit(
                    self.deploy_function,
                    task
                )
                all_futures[future] = task

        # Collect results
        for future in as_completed(all_futures):
            task = all_futures[future]
            try:
                function_name = future.result()
                results.append(function_name)
                task.set_result(function_name)
            except Exception as e:
                self.logger.error(f"Failed to deploy task {task.id}: {e}")
                task.set_exception(e)

        if self.enable_metrics:
            self.profiler.prof('nuclio_batch_end', uid=self.manager_id)
        return results

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from Task's env_var."""
        config = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        config[key.replace('FAAS_', '').lower()] = value

        return config

    def _extract_granular_config(self, task: Task) -> Dict[str, Any]:
        """Extract granular Nuclio configuration from env vars."""
        granular = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_') and key not in ['FAAS_PROVIDER', 'FAAS_SOURCE',
                                                               'FAAS_HANDLER', 'FAAS_RUNTIME',
                                                               'FAAS_TIMEOUT', 'FAAS_REGISTRY_URI',
                                                               'FAAS_INLINE_CODE']:
                        # These are granular configs
                        config_key = key.replace('FAAS_', '').lower()
                        try:
                            # Try to parse JSON strings
                            if value.startswith('{') or value.startswith('['):
                                granular[config_key] = json.loads(value)
                            else:
                                granular[config_key] = value
                        except json.JSONDecodeError:
                            granular[config_key] = value

        return granular

    def _extract_nuclio_build_config(self, task: Task) -> Dict[str, Any]:
        """Extract Nuclio-specific build configuration from task."""
        build_config = {}

        # Use cmd for Nuclio build commands
        if hasattr(task, 'cmd') and task.cmd:
            build_config['build_commands'] = task.cmd

        # Extract other Nuclio-specific configs from env vars
        granular_config = self._extract_granular_config(task)
        build_config.update(granular_config)

        return build_config

    def _generate_function_name(self, task: Task) -> str:
        """Generate Nuclio-compatible function name."""
        base_name = task.name or f"task-{task.id}"
        # Kubernetes naming constraints
        clean_name = ''.join(c for c in base_name.lower() if c.isalnum() or c == '-')
        return f"nuclio-{self.manager_id}-{clean_name}"[:63]  # K8s limit

    def _determine_deployment_type(self, task: Task, faas_config: Dict[str, Any]) -> str:
        """Automatically determine deployment type from task attributes."""
        # Check if it's a prebuilt image (full URI)
        if hasattr(task, 'image') and task.image and self._is_full_image_uri(task.image):
            return 'prebuilt-image'

        # Check for source path - Nuclio can build from source
        source_path = faas_config.get('source')
        if source_path:
            return 'source-build'

        # Check for inline code
        inline_code = faas_config.get('inline_code')
        if inline_code:
            return 'inline-code'

        raise DeploymentException(
            "Cannot determine deployment type. Provide either:\n"
            "1. image with full URI (prebuilt-image)\n"
            "2. FAAS_SOURCE=/path/to/code + FAAS_REGISTRY_URI (source-build)\n"
            "3. FAAS_INLINE_CODE='your code' (inline-code)"
        )

    def _is_full_image_uri(self, image: str) -> bool:
        """Check if image is a full URI (not just a base runtime)."""
        # Base Python runtimes
        if image in BASE_PYTHON_IMAGES:
            return False

        # Full URIs contain registry/repo:tag
        return ('/' in image or
                ':' in image and not image.startswith('python:') or
                image.startswith(('docker.io/', 'gcr.io/', 'quay.io/')) or
                '.azurecr.io' in image or
                'localhost:' in image)

    def _deploy_prebuilt_image(self, task: Task, function_name: str, faas_config: Dict[str, Any]) -> None:
        """Deploy a function with a prebuilt container image."""
        self.logger.trace(f"Deploying prebuilt image: {task.image}")

        # Build Nuclio function spec
        spec = self._build_nuclio_spec(task, function_name, faas_config)
        spec.config["spec"]["image"] = task.image

        # Deploy using Nuclio SDK
        self._deploy_with_retries(function_name, spec)

    def _deploy_from_source(self, task: Task, function_name: str, faas_config: Dict[str, Any]) -> None:
        """Deploy a function by building from source code."""
        source_path = faas_config.get('source')
        if not source_path or not os.path.exists(source_path):
            raise DeploymentException(f"Source path does not exist: {source_path}")

        # Registry URI is required for source builds
        registry_uri = faas_config.get('registry_uri')
        if not registry_uri:
            raise DeploymentException(
                "Source build deployment requires 'FAAS_REGISTRY_URI' in env_var. "
                "Example: FAAS_REGISTRY_URI=docker.io/myuser/myrepo or "
                "FAAS_REGISTRY_URI=localhost:5000/nuclio"
            )

        self.logger.trace(f"Building from source: {source_path} to registry: {registry_uri}")

        # Create function.yaml for nuctl build
        function_yaml_path = self._create_function_yaml(task, function_name, faas_config, source_path, registry_uri)

        # Build using nuctl
        self._nuctl_build(function_name, function_yaml_path, registry_uri)

        # Deploy the built function
        self._nuctl_deploy(function_name, function_yaml_path)

    def _deploy_inline_code(self, task: Task, function_name: str, faas_config: Dict[str, Any]) -> None:
        """Deploy a function with inline code."""
        inline_code = faas_config.get('inline_code')
        if not inline_code:
            raise DeploymentException("No inline code found in FAAS_INLINE_CODE")

        self.logger.trace(f"Deploying inline code for function: {function_name}")

        # Build Nuclio function spec
        spec = self._build_nuclio_spec(task, function_name, faas_config)

        # Add inline source code as base64
        source_code = self._prepare_inline_code(inline_code, faas_config)
        encoded_source = base64.b64encode(source_code.encode('utf-8')).decode('utf-8')

        spec.config["spec"]["build"] = {
            "functionSourceCode": encoded_source,
            "codeEntryType": "sourceCode"
        }

        # Use default Python base image if not specified
        if not hasattr(task, 'image') or not task.image:
            spec.config["spec"]["runtime"] = faas_config.get('runtime', DEFAULT_PYTHON_RUNTIME)
        else:
            spec.config["spec"]["runtime"] = f"python:{task.image.split(':')[-1]}"

        # Deploy using Nuclio SDK
        self._deploy_with_retries(function_name, spec)

    def _prepare_inline_code(self, inline_code: str, faas_config: Dict[str, Any]) -> str:
        """Prepare inline code for Nuclio deployment."""
        # Check if code already has a handler function
        if 'def handler' not in inline_code:
            # Wrap the code in a Nuclio handler function
            handler_name = faas_config.get('handler', 'main:handler').split(':')[-1]
            prepared_code = f"""def {handler_name}(context, event):
    # User code starts here
{chr(10).join('    ' + line for line in inline_code.split(chr(10)))}
    # User code ends here
    return 'Success'
"""
        else:
            # Adjust handler signature for Nuclio if needed
            prepared_code = inline_code.replace('def handler(event, context):', 'def handler(context, event):')

        return prepared_code

    def _create_function_yaml(self, task: Task, function_name: str, faas_config: Dict[str, Any],
                              source_path: str, registry_uri: str) -> str:
        """Create function.yaml for nuctl build."""
        yaml_path = os.path.join(self.sandbox, f"{function_name}_function.yaml")

        # Build configuration
        build_config = self._extract_nuclio_build_config(task)

        function_config = {
            "apiVersion": "nuclio.io/v1",
            "kind": "NuclioFunction",
            "metadata": {
                "name": function_name,
                "namespace": "nuclio"
            },
            "spec": {
                "handler": faas_config.get('handler', 'main:handler'),
                "runtime": faas_config.get('runtime', DEFAULT_PYTHON_RUNTIME),
                "build": {
                    "path": source_path,
                    "registry": registry_uri,
                    "noBaseImagesPull": True
                },
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

        # Add base image if specified
        if hasattr(task, 'image') and task.image and task.image in BASE_PYTHON_IMAGES:
            function_config["spec"]["build"]["baseImage"] = task.image

        # Add build commands if specified
        if build_config.get('build_commands'):
            function_config["spec"]["build"]["commands"] = build_config['build_commands']

        # Add environment variables
        if hasattr(task, '_user_env_vars') and task._user_env_vars:
            env_vars = []
            for var in task._user_env_vars:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    env_vars.append({"name": key, "value": value})
            function_config["spec"]["env"] = env_vars

        # Write YAML file
        with open(yaml_path, 'w') as f:
            yaml.dump(function_config, f, default_flow_style=False)

        return yaml_path

    def _nuctl_build(self, function_name: str, function_yaml_path: str, registry_uri: str) -> None:
        """Build function using nuctl."""
        from hydraa.services.caas_manager.utils.misc import sh_callout

        cmd = f"nuctl build {function_name} --path {function_yaml_path} --registry {registry_uri} --platform kube"

        self.logger.trace(f"Building function with nuctl: {cmd}")
        out, err, ret = sh_callout(cmd, shell=True)

        if ret != 0:
            raise NuclioException(f"nuctl build failed: {err}")

        self.logger.trace(f"Successfully built function: {function_name}")

    def _nuctl_deploy(self, function_name: str, function_yaml_path: str) -> None:
        """Deploy function using nuctl."""
        from hydraa.services.caas_manager.utils.misc import sh_callout

        cmd = f"nuctl deploy {function_name} --path {function_yaml_path} --platform kube"

        self.logger.trace(f"Deploying function with nuctl: {cmd}")
        out, err, ret = sh_callout(cmd, shell=True)

        if ret != 0:
            raise NuclioException(f"nuctl deploy failed: {err}")

        self.logger.trace(f"Successfully deployed function: {function_name}")

    def _build_nuclio_spec(self, task: Task, function_name: str, faas_config: Dict[str, Any]) -> nuclio.ConfigSpec:
        """Build base Nuclio configuration spec."""
        # Extract granular configuration
        granular_config = self._extract_granular_config(task)

        # Build Nuclio function spec
        spec = nuclio.ConfigSpec(
            env=[],
            config={
                "spec": {
                    "handler": faas_config.get('handler', 'main:handler'),
                    "runtime": "python:3.9",  # Nuclio format
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

        # Apply granular configurations
        spec_config = spec.config["spec"]

        if 'min_replicas' in granular_config:
            spec_config["minReplicas"] = int(granular_config['min_replicas'])

        if 'max_replicas' in granular_config:
            spec_config["maxReplicas"] = int(granular_config['max_replicas'])

        if 'target_cpu' in granular_config:
            spec_config["targetCPU"] = int(granular_config['target_cpu'])

        # Add environment variables
        if hasattr(task, '_user_env_vars'):
            for var in task._user_env_vars:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    spec.set_env(key, value)

        return spec

    def _deploy_with_retries(self, function_name: str, spec: nuclio.ConfigSpec, max_retries: int = 3) -> None:
        """Deploy function using Nuclio SDK with retry logic."""
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
        """Invoke a Nuclio function using the invocation thread pool.

        Args:
            function_name: Function name or identifier.
            payload: JSON-serializable payload.

        Returns:
            Function response.
        """
        # Submit to invocation thread pool for true parallelism
        future = self.invocation_executor.submit(
            self._invoke_function_internal,
            function_name,
            payload
        )

        # Wait for result (caller can use ThreadPoolExecutor for parallel calls)
        return future.result()

    def _invoke_function_internal(self, function_name: str, payload: Any = None) -> Dict[str, Any]:
        """Internal function to invoke Nuclio - runs in thread pool."""
        try:
            # Fast path: check function cache first
            with self._provider_lock:
                if function_name in self._functions:
                    full_name = function_name
                else:
                    # Search by original name or task ID
                    full_name = None
                    for fname, fdata in self._functions.items():
                        if (fdata['task_id'] == function_name or
                                fdata['original_name'] == function_name):
                            full_name = fname
                            break

            if not full_name:
                raise InvocationException(f"Function '{function_name}' not found")

            # Direct invoke via Nuclio SDK
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

            return result

        except Exception as e:
            raise InvocationException(f"Failed to invoke '{function_name}': {e}")

    def shutdown(self) -> None:
        """Shutdown provider and cleanup resources."""
        self.logger.trace("Shutting down Nuclio provider")
        if self.enable_metrics:
            self.profiler.prof('nuclio_shutdown_start', uid=self.manager_id)

        # Signal termination
        self._terminate.set()

        # Wait for worker
        if self.asynchronous and hasattr(self, 'worker_thread'):
            self.worker_thread.join(timeout=5)

        # Cleanup functions if auto_terminate
        if self.auto_terminate:
            with self._provider_lock:
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

        # Shutdown executors
        self.deployment_executor.shutdown(wait=True)
        self.invocation_executor.shutdown(wait=True)

        if self.enable_metrics:
            self.profiler.prof('nuclio_shutdown_end', uid=self.manager_id)
        self.logger.trace("Nuclio provider shutdown complete")