# -*- coding: utf-8 -*-
"""Nuclio provider - Simplified resource provisioning.

This module handles Nuclio FaaS deployments on Kubernetes clusters,
with automatic resource detection and required registry specification
for container builds.
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

        # Registry manager (no default registry setup)
        self.registry_manager = RegistryManager(logger=self.logger)
        self.resource_manager = ResourceManager(logger=self.logger)

        # Kubernetes cluster (from VMs)
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
            # Initialize Kubernetes cluster from VMs
            self._setup_kubernetes()

            # Install Nuclio with retry logic
            self._install_nuclio(max_retries=3)

            # Setup dashboard access with async check
            self._setup_dashboard_access()

            # No default registry configuration - determined per task

            self.profiler.prof('nuclio_setup_end', uid=self.manager_id)
            self.logger.trace("Nuclio environment ready")

        except Exception as e:
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

            # Generate function name
            function_name = self._generate_function_name(task)

            # Determine deployment type automatically
            deployment_type = self._determine_deployment_type(task, faas_config)
            self.logger.trace(f"Auto-detected deployment type: {deployment_type} for task {task.id}")

            # Build or get container image
            image_uri = self._prepare_image(task, function_name, deployment_type, faas_config)

            # Deploy to Nuclio
            self._deploy_to_nuclio(task, function_name, image_uri, faas_config)

            # Track deployment
            with self._functions_lock:
                self._functions[function_name] = {
                    'task_id': str(task.id),
                    'image_uri': image_uri,
                    'original_name': task.name or f"task-{task.id}",
                    'deployment_type': deployment_type
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
                                                               'FAAS_TIMEOUT', 'FAAS_REGISTRY_URI']:
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

        # Get source path
        source_path = faas_config.get('source')

        # Has source + image = container build
        if source_path and hasattr(task, 'image') and task.image:
            return 'container-build'

        # Check for inline code
        if task.cmd and len(task.cmd) > 2 and task.cmd[0] == 'python' and task.cmd[1] == '-c':
            # Inline code = container build (Nuclio doesn't support zip)
            return 'container-build'

        # Nuclio doesn't support zip deployments
        if source_path and not (hasattr(task, 'image') and task.image):
            raise DeploymentException(
                "Nuclio requires container deployments. Please specify an image for the task."
            )

        raise DeploymentException(
            "Cannot determine deployment type. Provide either:\n"
            "1. image with full URI (prebuilt-image)\n"
            "2. FAAS_SOURCE + image + FAAS_REGISTRY_URI (container-build)\n"
            "3. inline code via cmd (container-build with FAAS_REGISTRY_URI)"
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

    def _prepare_image(self, task: Task, function_name: str,
                       deployment_type: str, faas_config: Dict[str, Any]) -> str:
        """Prepare container image based on deployment type."""
        if deployment_type == 'prebuilt-image':
            # Just use the provided image URI
            self.logger.trace(f"Using prebuilt image: {task.image}")
            return task.image

        elif deployment_type == 'container-build':
            # Container build requires registry_uri
            registry_uri = faas_config.get('registry_uri')
            if not registry_uri:
                raise DeploymentException(
                    "Container build deployment requires 'FAAS_REGISTRY_URI' in env_var. "
                    "Example: FAAS_REGISTRY_URI=docker.io/myuser/myrepo or "
                    "FAAS_REGISTRY_URI=localhost:5000/nuclio for local registry"
                )

            repo_uri = registry_uri
            self.logger.trace(f"Building image to push to: {repo_uri}")

            # Get or create source
            source_path = faas_config.get('source')

            # Use inline code if no source path
            if not source_path and task.cmd and len(task.cmd) > 2:
                source_path = self._create_source_from_inline_code(task, faas_config)

            if not source_path:
                raise DeploymentException("Container build requires source path or inline code")

            # Configure registry if needed (for auth)
            self._configure_registry_if_needed(repo_uri)

            # Build and push image
            image_uri, _, _ = self.registry_manager.build_and_push_image(
                source_path=source_path,
                repository_uri=repo_uri,
                image_tag=function_name
            )
            return image_uri

        else:
            raise DeploymentException(f"Unsupported deployment type: {deployment_type}")

    def _create_source_from_inline_code(self, task: Task, faas_config: Dict[str, Any]) -> str:
        """Create temporary source directory from inline code."""
        if not (task.cmd and len(task.cmd) > 2 and task.cmd[0] == 'python' and task.cmd[1] == '-c'):
            raise DeploymentException("No inline code found in task.cmd")

        code = task.cmd[2]
        source_dir = os.path.join(self.sandbox, f"task_{task.id}_source")
        os.makedirs(source_dir, exist_ok=True)

        # Create handler file for Nuclio
        handler_file = os.path.join(source_dir, "main.py")
        with open(handler_file, 'w') as f:
            # Ensure the code has a handler function for Nuclio
            if 'def handler' not in code:
                # Wrap the code in a handler function
                f.write("def handler(context, event):\n")
                indented_code = '\n'.join(f"    {line}" for line in code.split('\n'))
                f.write(indented_code)
                f.write("\n    return 'Success'\n")
            else:
                # Adjust handler signature for Nuclio if needed
                code = code.replace('def handler(event, context):', 'def handler(context, event):')
                f.write(code)

        # Create Dockerfile for Nuclio
        dockerfile = os.path.join(source_dir, "Dockerfile")
        with open(dockerfile, 'w') as f:
            runtime = task.image if hasattr(task, 'image') and task.image else DEFAULT_PYTHON_RUNTIME
            # Validate runtime
            if runtime not in BASE_PYTHON_IMAGES:
                runtime = DEFAULT_PYTHON_RUNTIME

            f.write(f"""FROM {runtime}
WORKDIR /app
COPY main.py .
RUN pip install nuclio-sdk
CMD ["python", "main.py"]
""")

        return source_dir

    def _configure_registry_if_needed(self, registry_uri: str) -> None:
        """Configure registry authentication if needed."""
        # Parse registry type from URI
        if 'docker.io' in registry_uri or registry_uri.startswith('docker.io/'):
            # Docker Hub - might need auth
            if hasattr(self, 'docker_credentials'):
                config = RegistryConfig(
                    type=RegistryType.DOCKERHUB,
                    url='https://index.docker.io/v1/',
                    username=self.docker_credentials.get('username'),
                    password=self.docker_credentials.get('password')
                )
                self.registry_manager.configure_registry('dockerhub', config)
        elif 'localhost:' in registry_uri:
            # Local registry - no auth needed
            config = RegistryConfig(
                type=RegistryType.LOCAL,
                url=registry_uri.split('/')[0]
            )
            self.registry_manager.configure_registry('local', config)
        else:
            # Custom registry
            config = RegistryConfig(
                type=RegistryType.CUSTOM,
                url=registry_uri.split('/')[0]
            )
            self.registry_manager.configure_registry('custom', config)

    def _deploy_to_nuclio(self, task: Task, function_name: str,
                          image_uri: str, faas_config: Dict[str, Any]) -> None:
        """Deploy function to Nuclio platform."""
        self.logger.trace(f"Deploying to Nuclio: {function_name}")

        # Validate runtime (Python only)
        runtime = faas_config.get('runtime', DEFAULT_PYTHON_RUNTIME)
        if not any(runtime.startswith(f'python:{v}') for v in ['3.9', '3.10', '3.11', '3.12']):
            raise DeploymentException(
                f"Unsupported runtime: {runtime}. "
                f"Only Python runtimes are supported: {SUPPORTED_PYTHON_RUNTIMES}"
            )

        # Extract granular configuration
        granular_config = self._extract_granular_config(task)

        # Build Nuclio function spec
        spec = nuclio.ConfigSpec(
            env=[],
            config={
                "spec": {
                    "handler": faas_config.get('handler', 'main:handler'),
                    "runtime": "python:3.9",  # Nuclio format
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

        # Apply granular configurations
        spec_config = spec.config["spec"]

        if 'min_replicas' in granular_config:
            spec_config["minReplicas"] = int(granular_config['min_replicas'])

        if 'max_replicas' in granular_config:
            spec_config["maxReplicas"] = int(granular_config['max_replicas'])

        if 'target_cpu' in granular_config:
            spec_config["targetCPU"] = int(granular_config['target_cpu'])

        if 'triggers' in granular_config:
            spec_config["triggers"] = granular_config['triggers']

        if 'data_bindings' in granular_config:
            spec_config["dataBindings"] = granular_config['data_bindings']

        if 'service_type' in granular_config:
            spec_config["serviceType"] = granular_config['service_type']

        if 'annotations' in granular_config:
            spec_config["annotations"] = granular_config['annotations']

        if 'labels' in granular_config:
            spec_config["labels"] = granular_config['labels']

        if 'disable_default_http_trigger' in granular_config:
            spec_config["disableDefaultHTTPTrigger"] = granular_config['disable_default_http_trigger'].lower() == 'true'

        if 'scale_to_zero' in granular_config:
            spec_config["scaleToZero"] = granular_config['scale_to_zero'].lower() == 'true'

        if 'node_selector' in granular_config:
            spec_config["nodeSelector"] = granular_config['node_selector']

        if 'priority_class_name' in granular_config:
            spec_config["priorityClassName"] = granular_config['priority_class_name']

        if 'preemption_policy' in granular_config:
            spec_config["preemptionPolicy"] = granular_config['preemption_policy']

        # Add environment variables
        if hasattr(task, '_user_env_vars'):
            for var in task._user_env_vars:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    spec.set_env(key, value)
        elif task.env_var:
            # Fallback if _user_env_vars not set
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