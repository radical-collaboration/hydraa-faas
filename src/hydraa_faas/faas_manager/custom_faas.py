"""
Nuclio Provider for FaaS Manager - Production-ready Version
High-performance implementation with improved connection pooling and custom command support
"""

import os
import sys
import time
import json
import yaml
import httpx
import base64
import tempfile
import threading
import queue
import platform
import subprocess
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Tuple
from dataclasses import dataclass, field
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from enum import Enum
from functools import wraps, lru_cache
import paramiko
from datetime import datetime, timedelta

# import from hydraa
from hydraa import Task, AwsVM, AzureVM, LocalVM
from hydraa.services.caas_manager.kubernetes import kubernetes
from hydraa.services.caas_manager.utils.misc import sh_callout, generate_id, logger
from hydraa.services.caas_manager.utils import ssh

# import our utilities
from ..utils.exceptions import DeploymentException, NuclioException
from ..utils.resource_manager import ResourceManager
from ..utils.registry import RegistryManager, RegistryType, RegistryConfig

# bulk processing configuration
MAX_BULK_SIZE = int(os.environ.get('FAAS_MAX_BULK_SIZE', '10'))
MAX_BULK_TIME = float(os.environ.get('FAAS_MAX_BULK_TIME', '2'))
MIN_BULK_TIME = float(os.environ.get('FAAS_MIN_BULK_TIME', '0.1'))

# performance configuration
MAX_PARALLEL_DEPLOYMENTS = int(os.environ.get('FAAS_MAX_PARALLEL_DEPLOYMENTS', '5'))
CONNECTION_POOL_SIZE = int(os.environ.get('FAAS_CONNECTION_POOL_SIZE', '10'))
SSH_KEEPALIVE_INTERVAL = int(os.environ.get('FAAS_SSH_KEEPALIVE', '30'))
HTTP_TIMEOUT = int(os.environ.get('FAAS_HTTP_TIMEOUT', '30'))
CONNECTION_REUSE_TIMEOUT = int(os.environ.get('FAAS_CONNECTION_REUSE_TIMEOUT', '300'))
GRACEFUL_SHUTDOWN_TIMEOUT = int(os.environ.get('NUCLIO_GRACEFUL_SHUTDOWN_TIMEOUT', '30'))

# retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1
RETRY_BACKOFF = 2

# task states
TASK_STATE_PENDING = 'PENDING'
TASK_STATE_BUILDING = 'BUILDING'
TASK_STATE_DEPLOYING = 'DEPLOYING'
TASK_STATE_DEPLOYED = 'DEPLOYED'
TASK_STATE_FAILED = 'FAILED'


def retry_on_error(max_attempts=MAX_RETRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    """Decorator for exponential backoff retry logic"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except (DeploymentException, NuclioException) as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        wait_time = delay * (backoff ** attempt)
                        time.sleep(wait_time)
                    else:
                        raise
            raise last_exception
        return wrapper
    return decorator


class DeploymentTarget(Enum):
    """Supported deployment targets"""
    MINIKUBE = "minikube"
    EKS = "eks"
    AKS = "aks"


@dataclass
class NuclioConfig:
    """Nuclio deployment configuration"""
    version: str = "1.13.0"
    dashboard_port: int = 30000
    namespace: str = "nuclio"
    registry_config: Optional[RegistryConfig] = None
    enable_metrics: bool = True
    enable_scale_to_zero: bool = True
    shared_repository: bool = True  # use shared repo for all functions


@dataclass
class FunctionConfig:
    """Configuration for a Nuclio function"""
    name: str
    namespace: str
    runtime: str
    handler: str
    image: Optional[str] = None
    source_path: Optional[str] = None
    source_code: Optional[str] = None
    env_vars: Dict[str, str] = field(default_factory=dict)
    labels: Dict[str, str] = field(default_factory=dict)
    min_replicas: int = 0
    max_replicas: int = 10
    resources: Dict[str, Any] = field(default_factory=dict)
    registry_url: Optional[str] = None
    image_pull_secrets: Optional[str] = None
    build_commands: List[str] = field(default_factory=list)
    _original_task: Optional[Task] = None

    @classmethod
    def from_task(cls, task: Task, namespace: str = "nuclio") -> "FunctionConfig":
        """Create FunctionConfig from Hydraa Task"""
        runtime = getattr(task, 'runtime', 'python:3.9')
        if not runtime.startswith(('python', 'node', 'java', 'dotnetcore', 'golang')):
            runtime = 'python:3.9'

        resources = {
            'requests': {
                'cpu': f"{int(task.vcpus * 1000)}m",
                'memory': f"{task.memory}Mi"
            },
            'limits': {
                'cpu': f"{int(task.vcpus * 2000)}m",
                'memory': f"{task.memory * 2}Mi"
            }
        }

        # extract build commands from task
        build_commands = []
        if hasattr(task, 'cmd'):
            if isinstance(task.cmd, str):
                build_commands = [task.cmd]
            else:
                build_commands = list(task.cmd)

        config = cls(
            name=task.name,
            namespace=namespace,
            runtime=runtime,
            handler=getattr(task, 'handler', 'main:handler'),
            image=getattr(task, 'image', None),
            source_path=getattr(task, 'source_path', None),
            source_code=getattr(task, 'handler_code', None),
            env_vars=getattr(task, 'env_vars', {}),
            labels=getattr(task, 'labels', {}),
            resources=resources,
            min_replicas=getattr(task, 'min_replicas', 0),
            max_replicas=getattr(task, 'max_replicas', 10),
            build_commands=build_commands
        )

        # preserve reference to original task
        config._original_task = task

        return config


class ConnectionPool:
    """Manages connection pooling for SSH and HTTP clients with improved reuse"""

    def __init__(self, logger):
        self.logger = logger
        self._ssh_clients = {}
        self._ssh_last_used = {}
        self._http_clients = {}
        self._lock = threading.Lock()
        self._shutdown = threading.Event()
        self._last_keepalive = time.time()

        # start keepalive thread
        self._keepalive_thread = threading.Thread(
            target=self._keepalive_worker,
            daemon=True
        )
        self._keepalive_thread.start()

    def get_ssh_client(self, host: str, port: int = 22, username: str = 'docker',
                       reuse_timeout: int = CONNECTION_REUSE_TIMEOUT) -> paramiko.SSHClient:
        """Get or create SSH client with connection pooling and reuse timeout"""
        key = f"{host}:{port}"

        with self._lock:
            if key in self._ssh_clients:
                client = self._ssh_clients[key]
                last_used = self._ssh_last_used.get(key, 0)

                # check if connection is still valid and within reuse timeout
                if time.time() - last_used < reuse_timeout:
                    try:
                        transport = client.get_transport()
                        if transport and transport.is_active():
                            # update last used time
                            self._ssh_last_used[key] = time.time()
                            return client
                    except:
                        pass

                # connection is stale so remove it
                try:
                    client.close()
                except:
                    pass
                del self._ssh_clients[key]
                if key in self._ssh_last_used:
                    del self._ssh_last_used[key]

            # create new connection
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            try:
                # for minikube we often dont need password
                client.connect(
                    hostname=host,
                    port=port,
                    username=username,
                    timeout=10,
                    allow_agent=True,
                    look_for_keys=True
                )

                # enable keepalive
                transport = client.get_transport()
                transport.set_keepalive(SSH_KEEPALIVE_INTERVAL)

                self._ssh_clients[key] = client
                self._ssh_last_used[key] = time.time()
                self.logger.debug(f"Created SSH connection to {key}")
                return client

            except Exception as e:
                self.logger.error(f"Failed to create SSH connection to {key}: {e}")
                raise

    def get_http_client(self, base_url: str = None, http2: bool = True) -> httpx.Client:
        """Get or create HTTP client with connection pooling"""
        key = base_url or 'default'

        with self._lock:
            if key in self._http_clients:
                client = self._http_clients[key]
                # check if client is still open
                try:
                    if not client.is_closed:
                        return client
                except:
                    pass
                # client is closed so remove it
                del self._http_clients[key]

            # create new client with optimized settings
            client = httpx.Client(
                base_url=base_url,
                http2=http2,
                timeout=httpx.Timeout(HTTP_TIMEOUT),
                limits=httpx.Limits(
                    max_keepalive_connections=CONNECTION_POOL_SIZE,
                    max_connections=CONNECTION_POOL_SIZE * 2,
                    keepalive_expiry=300
                )
            )

            self._http_clients[key] = client
            self.logger.debug(f"Created HTTP client for {key}")
            return client

    def _keepalive_worker(self):
        """Send keepalive packets to maintain SSH connections"""
        while not self._shutdown.is_set():
            time.sleep(SSH_KEEPALIVE_INTERVAL)

            with self._lock:
                current_time = time.time()
                stale_connections = []

                for key, client in list(self._ssh_clients.items()):
                    try:
                        last_used = self._ssh_last_used.get(key, 0)
                        # remove connections not used for over reuse timeout
                        if current_time - last_used > CONNECTION_REUSE_TIMEOUT:
                            stale_connections.append(key)
                            continue

                        transport = client.get_transport()
                        if transport and transport.is_active():
                            transport.send_ignore()
                        else:
                            # remove dead connection
                            stale_connections.append(key)
                    except:
                        # remove failed connection
                        stale_connections.append(key)

                # clean up stale connections
                for key in stale_connections:
                    if key in self._ssh_clients:
                        try:
                            self._ssh_clients[key].close()
                        except:
                            pass
                        del self._ssh_clients[key]
                    if key in self._ssh_last_used:
                        del self._ssh_last_used[key]

    def close_all(self):
        """Close all connections"""
        self._shutdown.set()

        with self._lock:
            for client in self._ssh_clients.values():
                try:
                    client.close()
                except:
                    pass
            self._ssh_clients.clear()
            self._ssh_last_used.clear()

            for client in self._http_clients.values():
                try:
                    client.close()
                except:
                    pass
            self._http_clients.clear()


class BatchProcessor:
    """Handles batch operations for deployments and invocations"""

    def __init__(self, executor: ThreadPoolExecutor, logger):
        self.executor = executor
        self.logger = logger

    def deploy_batch(self, configs: List[FunctionConfig], deploy_func) -> Dict[str, Any]:
        """Deploy multiple functions in parallel"""
        results = {}
        futures = {}

        # submit all deployments
        for config in configs:
            future = self.executor.submit(deploy_func, config)
            futures[future] = config.name

        # collect results
        for future in as_completed(futures):
            func_name = futures[future]
            try:
                result = future.result()
                results[func_name] = {'status': 'success', 'result': result}
                self.logger.info(f"Successfully deployed {func_name}")
            except Exception as e:
                results[func_name] = {'status': 'error', 'error': str(e)}
                self.logger.error(f"Failed to deploy {func_name}: {e}")

        return results

    def invoke_batch(self, invocations: List[Tuple[str, Any]], invoke_func) -> List[Any]:
        """Invoke multiple functions in parallel"""
        results = []
        futures = []

        # submit all invocations
        for func_name, payload in invocations:
            future = self.executor.submit(invoke_func, func_name, payload)
            futures.append(future)

        # collect results in order
        for future in futures:
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({'error': str(e)})

        return results


class NuclioProvider:
    """
    Optimized Nuclio provider with connection pooling and batch operations.

    Features:
    - Connection pooling for SSH (minikube) and HTTP (dashboard API)
    - Batch deployment and invocation support
    - Smart caching of function endpoints
    - Efficient resource management
    - Support for custom build commands
    """

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 vms: List[Union[LocalVM, AwsVM, AzureVM]],
                 cred: Dict[str, Any],
                 asynchronous: bool,
                 auto_terminate: bool,
                 log,
                 resource_manager: Optional[ResourceManager] = None,
                 resource_config: Optional[Dict[str, Any]] = None):
        """Initialize Nuclio provider"""
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.vms = vms
        self.logger = log
        self.cred = cred
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.status = False
        self.resource_config = resource_config or {}

        # validate vms
        self._validate_vms_for_k8s()

        # determine deployment target
        self.deployment_target = self._determine_target_from_vms()

        # parse configuration
        self._setup_configuration()

        # resource management
        self.resource_manager = resource_manager or ResourceManager()

        # registry manager integration through resource manager
        self.registry_manager = self.resource_manager.registry_manager if hasattr(
            self.resource_manager, 'registry_manager'
        ) else RegistryManager(logger=self.logger)

        # connection pooling
        self.connection_pool = ConnectionPool(self.logger)

        # batch processor
        self.executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_DEPLOYMENTS)
        self.batch_processor = BatchProcessor(self.executor, self.logger)

        # caching
        self._endpoint_cache = {}  # function_name to endpoint info
        self._cache_lock = threading.Lock()
        self._cache_ttl = 3600  # 1 hour ttl for cache entries

        # task tracking with thread safe state management
        self._task_id = 0
        self._task_lock = threading.Lock()
        self._deployed_functions = OrderedDict()
        self._functions_book = OrderedDict()
        self._active_deployments = set()

        # queue management
        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self._terminate = threading.Event()
        self._shutdown_complete = threading.Event()

        # kubernetes cluster
        self.k8s_cluster = None
        self.nuctl_path = None
        self.dashboard_url = None

        # generate unique run id
        self.run_id = generate_id(prefix='nuclio', length=8)

        # setup mock servers for localvm
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            self._setup_local_servers()

        # check for existing cluster in resource config
        self._existing_cluster = self.resource_config.get('existing_cluster')

        # start worker thread
        self.worker_thread = threading.Thread(
            target=self._get_work,
            name='NuclioWorker',
            daemon=True
        )

        # check if thread is already started following hydraa pattern
        if not self.worker_thread.is_alive():
            self.worker_thread.start()

        # initialize in background
        self._init_thread = threading.Thread(
            target=self._initialize_resources,
            name='NuclioInit',
            daemon=True
        )
        self._init_thread.start()

    def _validate_vms_for_k8s(self):
        """Validate that VMs are suitable for Kubernetes deployment"""
        if not self.vms:
            raise ValueError("No VMs provided for Nuclio deployment")

        for vm in self.vms:
            if isinstance(vm, AwsVM):
                if vm.LaunchType.upper() not in ['EKS', 'EC2']:
                    raise ValueError(f"AWS VM with launch type {vm.LaunchType} not suitable for Kubernetes")
            elif isinstance(vm, AzureVM):
                if vm.LaunchType.upper() not in ['AKS']:
                    raise ValueError(f"Azure VM with launch type {vm.LaunchType} not suitable for Kubernetes")
            elif isinstance(vm, LocalVM):
                if vm.LaunchType not in ['create', 'join']:
                    raise ValueError(f"Local VM with launch type {vm.LaunchType} not valid")

    def _setup_local_servers(self):
        """Set up mock servers for LocalVM"""
        import psutil
        from openstack.compute.v2 import server as op_server
        from openstack.compute.v2 import flavor as op_flavor

        vcpus = os.cpu_count()
        memory = psutil.virtual_memory().total // (1024 * 1024)

        for vm in self.vms:
            if isinstance(vm, LocalVM):
                vm.Servers = []
                flavor = op_flavor.Flavor(vcpus=vcpus, ram=memory, disk=10)
                server = op_server.Server(
                    status='ACTIVE',
                    flavor=flavor,
                    name='local-hydraa',
                    access_ipv4='127.0.0.1',
                    addresses={'fixed_ip': [{'addr': '127.0.0.1'}]}
                )

                server.remote = ssh.Remote(
                    vm_keys=['', ''],
                    user=None,
                    fip='127.0.0.1',
                    log=self.logger,
                    local=True
                )

                vm.Servers.append(server)
                self.logger.info(f"Set up local server for {vm.VmName}")

    def _determine_target_from_vms(self) -> DeploymentTarget:
        """Determine deployment target from VM types"""
        vm = self.vms[0]
        if isinstance(vm, LocalVM):
            return DeploymentTarget.MINIKUBE
        elif isinstance(vm, AwsVM):
            return DeploymentTarget.EKS
        elif isinstance(vm, AzureVM):
            return DeploymentTarget.AKS
        else:
            raise ValueError(f"Unsupported VM type: {type(vm)}")

    def _setup_configuration(self):
        """Parse and validate configuration"""
        nuclio_config = self.resource_config.get('nuclio', {})
        registry_config_dict = nuclio_config.get('registry', self.cred)

        registry_type = registry_config_dict.get('registry_type', 'none')
        registry_config = None

        if registry_type != 'none':
            registry_config = RegistryConfig(
                type=RegistryType(registry_type),
                url=registry_config_dict.get('registry_url') or registry_config_dict.get('url'),
                username=registry_config_dict.get('registry_username') or registry_config_dict.get('username'),
                password=registry_config_dict.get('registry_password') or registry_config_dict.get('password'),
                region=registry_config_dict.get('registry_region') or registry_config_dict.get('region')
            )

        self.nuclio_config = NuclioConfig(
            version=self.cred.get('nuclio_version', '1.13.0'),
            dashboard_port=self.cred.get('dashboard_port', 30000),
            namespace=self.cred.get('namespace', 'nuclio'),
            registry_config=registry_config,
            enable_metrics=self.cred.get('enable_metrics', True),
            enable_scale_to_zero=self.cred.get('enable_scale_to_zero', True),
            shared_repository=self.cred.get('shared_repository', True)
        )

    def _initialize_resources(self):
        """Initialize Kubernetes cluster and Nuclio"""
        try:
            self.logger.info(f"Initializing Nuclio provider for {self.deployment_target.value}")

            # setup kubernetes cluster
            self._setup_k8s_cluster()

            # setup registry if configured
            if self.nuclio_config.registry_config:
                self._setup_registry()

            # install nuclio on the cluster
            self._install_nuclio()

            # setup nuctl cli
            self._setup_nuctl()

            # setup dashboard access
            self._setup_dashboard_access()

            # deploy gateway function for remote deployments
            if self.deployment_target != DeploymentTarget.MINIKUBE:
                self._deploy_gateway_function()

            # load existing functions
            self._load_existing_functions()

            self.status = True
            self.logger.info("Nuclio provider initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize Nuclio provider: {e}")
            self.status = False
            raise

    def _setup_k8s_cluster(self):
        """Setup Kubernetes cluster"""
        # check if we can reuse existing cluster
        if self._existing_cluster:
            self.logger.info("Using existing Kubernetes cluster from CaaS manager")
            self.k8s_cluster = self._existing_cluster
            return

        self.logger.info(f"Setting up {self.deployment_target.value} cluster")

        try:
            if self.deployment_target == DeploymentTarget.MINIKUBE:
                self.k8s_cluster = kubernetes.K8sCluster(
                    run_id=self.run_id,
                    vms=self.vms,
                    sandbox=self.sandbox,
                    log=self.logger
                )
            elif self.deployment_target == DeploymentTarget.EKS:
                import boto3
                ec2_client = boto3.client('ec2',
                                        aws_access_key_id=self.cred['aws_access_key_id'],
                                        aws_secret_access_key=self.cred['aws_secret_access_key'],
                                        region_name=self.cred.get('region', 'us-east-1'))

                self.k8s_cluster = kubernetes.EKSCluster(
                    run_id=self.run_id,
                    sandbox=self.sandbox,
                    vms=self.vms,
                    ec2=ec2_client,
                    log=self.logger
                )

            # handle join mode for localvm
            if isinstance(self.vms[0], LocalVM) and self.vms[0].LaunchType == 'join':
                self._configure_existing_cluster()
            else:
                self.k8s_cluster.bootstrap()

            # create nuclio namespace
            self.k8s_cluster.create_namespace(self.nuclio_config.namespace)

            self.logger.info(f"Kubernetes cluster ready: {self.k8s_cluster.name}")

        except Exception as e:
            self.logger.error(f"Failed to setup Kubernetes cluster: {e}")
            raise

    def _configure_existing_cluster(self):
        """Configure existing Kubernetes cluster for join mode"""
        self.logger.info("Configuring existing Kubernetes cluster")

        self.k8s_cluster.add_nodes_properity()

        # find kubeconfig
        config_path = None
        if hasattr(self.vms[0], 'KubeConfigPath') and self.vms[0].KubeConfigPath:
            config_path = os.path.expanduser(self.vms[0].KubeConfigPath)
        else:
            possible_paths = [
                os.path.expanduser('~/.kube/config'),
                os.path.expanduser('~/.minikube/config')
            ]
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break

        if not config_path:
            raise ValueError("join mode requires KubeConfigPath or kubeconfig in default location")

        # setup kubeconfig
        kube_dir = os.path.join(self.k8s_cluster.sandbox, '.kube')
        os.makedirs(kube_dir, exist_ok=True)
        sandbox_config = os.path.join(kube_dir, 'config')
        shutil.copy2(config_path, sandbox_config)
        self.k8s_cluster.kube_config = sandbox_config

        from kubernetes import config as k8s_config
        k8s_config.load_kube_config(self.k8s_cluster.kube_config)

        self.k8s_cluster.status = 'Ready'

    def _setup_registry(self):
        """Setup registry with proper authentication"""
        if not self.nuclio_config.registry_config:
            return

        self.registry_manager.configure_registry('nuclio-registry', self.nuclio_config.registry_config)

        # create image pull secret
        self._ensure_image_pull_secret()

        self.logger.info(f"Registry configured: {self.nuclio_config.registry_config.type.value}")

    def _ensure_image_pull_secret(self) -> str:
        """Create or update Kubernetes image pull secret"""
        if not self.nuclio_config.registry_config:
            return None

        registry_config = self.nuclio_config.registry_config
        secret_name = 'nuclio-registry-creds'

        # build docker config
        if registry_config.type == RegistryType.DOCKERHUB:
            server = 'https://index.docker.io/v1/'
        else:
            server = f"https://{registry_config.url}"

        # delete existing secret
        delete_cmd = f"kubectl delete secret {secret_name} -n {self.nuclio_config.namespace} --ignore-not-found"
        sh_callout(delete_cmd, shell=True, kube=self.k8s_cluster)

        # create new secret
        create_cmd = f"kubectl create secret docker-registry {secret_name} "
        create_cmd += f"--docker-server={server} "
        create_cmd += f"--docker-username={registry_config.username} "
        create_cmd += f"--docker-password={registry_config.password} "
        create_cmd += f"--namespace {self.nuclio_config.namespace}"

        out, err, ret = sh_callout(create_cmd, shell=True, kube=self.k8s_cluster)
        if ret:
            self.logger.warning(f"Failed to create pull secret: {err}")
            return None

        self.logger.info(f"Created image pull secret: {secret_name}")
        return secret_name

    def _install_nuclio(self):
        """Install Nuclio on the Kubernetes cluster"""
        self.logger.info("Installing Nuclio on cluster")

        # add helm repo
        helm_cmds = [
            "helm repo add nuclio https://nuclio.github.io/nuclio/charts",
            "helm repo update"
        ]

        for cmd in helm_cmds:
            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)
            if ret and "already exists" not in err:
                raise RuntimeError(f"Helm command failed: {err}")

        # prepare helm values
        values = {
            'controller.enabled': True,
            'dashboard.enabled': True,
            'dashboard.nodePort': self.nuclio_config.dashboard_port,
            'rbac.create': True,
        }

        # configure registry
        if self.nuclio_config.registry_config and self.nuclio_config.registry_config.type != RegistryType.NONE:
            values['registry.pushPullUrl'] = self.nuclio_config.registry_config.url
            values['registry.secretName'] = 'nuclio-registry-creds'

        # create values file
        values_file = f"{self.sandbox}/nuclio-values.yaml"
        with open(values_file, 'w') as f:
            yaml.dump(values, f)

        # install nuclio
        install_cmd = f"""helm upgrade --install nuclio nuclio/nuclio \
            --namespace {self.nuclio_config.namespace} \
            --values {values_file} \
            --wait --timeout 10m"""

        out, err, ret = sh_callout(install_cmd, shell=True, kube=self.k8s_cluster)
        if ret:
            raise RuntimeError(f"Failed to install Nuclio: {err}")

        self.logger.info("Nuclio installed successfully")

    def _setup_nuctl(self):
        """Setup nuctl CLI"""
        existing_nuctl = shutil.which('nuctl')
        if existing_nuctl:
            self.logger.info(f"Using existing nuctl: {existing_nuctl}")
            self.nuctl_path = existing_nuctl
            return

        system = platform.system()
        machine = platform.machine().lower()
        arch = 'amd64' if machine in ['x86_64', 'amd64'] else 'arm64'

        if system == 'Darwin':
            nuctl_url = f"https://github.com/nuclio/nuclio/releases/download/{self.nuclio_config.version}/nuctl-{self.nuclio_config.version}-darwin-{arch}"
        elif system == 'Linux':
            nuctl_url = f"https://github.com/nuclio/nuclio/releases/download/{self.nuclio_config.version}/nuctl-{self.nuclio_config.version}-linux-{arch}"
        else:
            raise RuntimeError(f"Unsupported OS: {system}")

        self.nuctl_path = f"{self.sandbox}/nuctl"

        # download nuctl
        out, err, ret = sh_callout(f"curl -s -L {nuctl_url} -o {self.nuctl_path}", shell=True)
        if ret:
            raise RuntimeError(f"Failed to download nuctl: {err}")

        os.chmod(self.nuctl_path, 0o755)
        self.logger.info(f"nuctl downloaded to: {self.nuctl_path}")

    def _setup_dashboard_access(self):
        """Setup access to Nuclio dashboard"""
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            # for minikube use nodeport directly
            cmd = f"kubectl get svc nuclio-dashboard -n {self.nuclio_config.namespace} -o jsonpath='{{.spec.ports[0].nodePort}}'"
            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)
            if ret == 0:
                port = out.strip()
                self.dashboard_url = f"http://localhost:{port}"
        else:
            # for remote we will use port forward on demand
            self.dashboard_url = "http://localhost:8070"

    def _deploy_gateway_function(self):
        """Deploy the API gateway function for remote deployments"""
        self.logger.info("Deploying API gateway function")

        # import gateway code from utils
        from ..utils.nuclio_gateway import GATEWAY_CODE

        # deploy gateway
        config = FunctionConfig(
            name='nuclio-gateway',
            namespace=self.nuclio_config.namespace,
            runtime='python:3.9',
            handler='gateway:handler',
            source_code=GATEWAY_CODE,
            min_replicas=2,
            max_replicas=10
        )

        try:
            self._deploy_function_internal(config)
            self.logger.info("Gateway function deployed successfully")
        except Exception as e:
            self.logger.warning(f"Failed to deploy gateway: {e}")

    def _load_existing_functions(self):
        """Load existing functions from cluster"""
        try:
            cmd = f"{self.nuctl_path} get functions --namespace {self.nuclio_config.namespace} --output json"
            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

            if ret == 0 and out:
                functions = json.loads(out) or []
                for func in functions:
                    name = func.get('metadata', {}).get('name', '')
                    self._deployed_functions[name] = func
                    # cache endpoint
                    self._cache_function_endpoint(name)

                self.logger.info(f"Loaded {len(functions)} existing functions")
        except Exception as e:
            self.logger.warning(f"Failed to load existing functions: {e}")

    def _get_work(self):
        """Worker thread to process deployment requests with batching"""
        bulk = []

        while not self._terminate.is_set():
            try:
                # wait for initialization
                if not self.status:
                    time.sleep(1)
                    continue

                # collect tasks for bulk processing
                now = time.time()
                timeout_start = now

                while time.time() - now < MAX_BULK_TIME:
                    try:
                        remaining_time = max(0.1, MIN_BULK_TIME - (time.time() - timeout_start))
                        task = self.incoming_q.get(block=True, timeout=remaining_time)
                    except queue.Empty:
                        task = None

                    if task:
                        bulk.append(task)

                    if len(bulk) >= MAX_BULK_SIZE:
                        break

                if bulk:
                    with self._task_lock:
                        self._process_bulk(bulk)
                    bulk = []

            except Exception as e:
                if not self._terminate.is_set():
                    self.logger.error(f"Error in worker thread: {e}")

        self.logger.info("Nuclio worker thread shutting down")

    def _process_bulk(self, tasks: List[Task]):
        """Process a bulk of tasks using batch processor with state management"""
        # convert tasks to configs
        configs = []
        task_map = {}

        for task in tasks:
            # assign id and name with thread safety
            with self._task_lock:
                task.id = self._task_id
                task.name = f'nuclio-function-{self._task_id}'
                self._task_id += 1
                self._functions_book[task.name] = task
                self._active_deployments.add(task.name)

            # update task state
            task.state = TASK_STATE_PENDING

            # create config
            config = FunctionConfig.from_task(task, self.nuclio_config.namespace)

            # set registry config
            if self.nuclio_config.registry_config:
                config.registry_url = self._get_registry_url()
                config.image_pull_secrets = 'nuclio-registry-creds'

            configs.append(config)
            task_map[config.name] = task

        # update all tasks to building state
        for task in tasks:
            task.state = TASK_STATE_BUILDING
            if not task.done():
                task.set_running_or_notify_cancel()

        # process in batch
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            results = self.batch_processor.deploy_batch(configs, self._deploy_local)
        else:
            results = self.batch_processor.deploy_batch(configs, self._deploy_remote)

        # update task states based on results
        for func_name, result in results.items():
            task = task_map[func_name]

            with self._task_lock:
                self._active_deployments.discard(func_name)

            if result['status'] == 'success':
                self._deployed_functions[func_name] = result['result']
                task.state = TASK_STATE_DEPLOYED

                if not task.done():
                    task.set_result({
                        'function_name': func_name,
                        'url': self._get_function_url(func_name),
                        'deployed_at': time.time()
                    })

                self.outgoing_q.put({
                    'function_name': func_name,
                    'state': 'deployed',
                    'message': f'Function {func_name} deployed successfully'
                })
            else:
                task.state = TASK_STATE_FAILED
                if not task.done():
                    task.set_exception(Exception(result['error']))

                self.outgoing_q.put({
                    'function_name': func_name,
                    'state': 'failed',
                    'error': result['error']
                })

    @retry_on_error()
    def _deploy_local(self, config: FunctionConfig) -> Dict[str, Any]:
        """Deploy function locally using nuctl"""
        self.logger.info(f"Deploying {config.name} locally")

        # generate yaml
        yaml_content = self._generate_nuclio_yaml(config)
        yaml_path = f"{self.sandbox}/{config.name}.yaml"

        with open(yaml_path, 'w') as f:
            yaml.dump(yaml_content, f)

        # deploy with nuctl
        cmd = f"{self.nuctl_path} deploy --file {yaml_path} --platform kube --namespace {self.nuclio_config.namespace}"
        out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

        if ret:
            raise DeploymentException(f"Deployment failed: {err}")

        # cache endpoint
        self._cache_function_endpoint(config.name)

        return self._get_function_info(config.name)

    @retry_on_error()
    def _deploy_remote(self, config: FunctionConfig) -> Dict[str, Any]:
        """Deploy function remotely via build plus dashboard API"""
        self.logger.info(f"Deploying {config.name} remotely")

        # build and push if source provided
        if config.source_path or config.source_code:
            image_uri = self._build_and_push_function(config)
        else:
            image_uri = config.image

        if not image_uri:
            raise DeploymentException("No image specified and no source to build")

        # deploy via dashboard api
        return self._deploy_via_dashboard(config, image_uri)

    def _deploy_function_internal(self, config: FunctionConfig) -> Dict[str, Any]:
        """Internal function deployment method"""
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            return self._deploy_local(config)
        else:
            return self._deploy_remote(config)

    @retry_on_error()
    def _build_and_push_function(self, config: FunctionConfig) -> str:
        """Build function with nuctl and push to registry"""
        # prepare build directory
        build_dir = f"{self.sandbox}/build-{config.name}"
        os.makedirs(build_dir, exist_ok=True)

        # write source code if inline
        if config.source_code:
            handler_file = os.path.join(build_dir, "handler.py")
            with open(handler_file, 'w') as f:
                f.write(config.source_code)
            source_path = build_dir
        else:
            source_path = config.source_path

        # build with nuctl
        registry_url = self._get_registry_url()

        cmd = f"{self.nuctl_path} build {config.name}"
        cmd += f" --path {source_path}"
        cmd += f" --handler {config.handler}"
        cmd += f" --runtime {config.runtime}"
        cmd += f" --registry {registry_url}"
        cmd += " --platform local"  # always build locally

        # add default and custom build commands
        build_commands = ['pip install msgpack']  # default
        build_commands.extend(config.build_commands)  # custom commands

        # create a unique command string
        build_command_str = ' && '.join(build_commands)
        cmd += f" --build-command '{build_command_str}'"

        self.logger.info(f"Building function: {cmd}")
        out, err, ret = sh_callout(cmd, shell=True)

        if ret:
            raise DeploymentException(f"Build failed: {err}")

        # extract image name
        if self.nuclio_config.shared_repository:
            image_uri = f"{registry_url}:processor-{config.name}-latest"
        else:
            image_uri = f"{registry_url}/processor-{config.name}:latest"

        self.logger.info(f"Built and pushed image: {image_uri}")
        return image_uri

    @retry_on_error()
    def _deploy_via_dashboard(self, config: FunctionConfig, image_uri: str) -> Dict[str, Any]:
        """Deploy function via Nuclio dashboard API"""
        # prepare deployment spec
        spec = {
            "metadata": {
                "name": config.name,
                "namespace": config.namespace,
                "labels": config.labels
            },
            "spec": {
                "image": image_uri,
                "handler": config.handler,
                "runtime": config.runtime,
                "minReplicas": config.min_replicas,
                "maxReplicas": config.max_replicas,
                "resources": config.resources,
                "env": [{"name": k, "value": v} for k, v in config.env_vars.items()]
            }
        }

        if config.image_pull_secrets:
            spec["spec"]["imagePullSecrets"] = config.image_pull_secrets

        # deploy via dashboard api
        with self._ensure_dashboard_connection() as dashboard_url:
            client = self.connection_pool.get_http_client(dashboard_url)

            response = client.post(
                "/api/functions",
                json=spec,
                headers={"Content-Type": "application/json"}
            )

            if response.status_code not in [200, 201, 202]:
                raise DeploymentException(f"Dashboard API error: {response.text}")

        # wait for deployment
        time.sleep(3)

        # cache endpoint
        self._cache_function_endpoint(config.name)

        return self._get_function_info(config.name)

    def _ensure_dashboard_connection(self):
        """Ensure dashboard is accessible with port forward if needed"""
        class DashboardConnection:
            def __init__(self, provider):
                self.provider = provider
                self.process = None

            def __enter__(self):
                if self.provider.deployment_target == DeploymentTarget.MINIKUBE:
                    return self.provider.dashboard_url
                else:
                    # start port forward
                    cmd = f"kubectl port-forward -n {self.provider.nuclio_config.namespace} svc/nuclio-dashboard 8070:8070"
                    self.process = subprocess.Popen(cmd, shell=True)
                    time.sleep(2)  # wait for port forward
                    return "http://localhost:8070"

            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.process:
                    self.process.terminate()
                    self.process.wait()

        return DashboardConnection(self)

    def _generate_nuclio_yaml(self, config: FunctionConfig) -> Dict[str, Any]:
        """Generate Nuclio function YAML with custom build commands"""
        yaml_content = {
            'apiVersion': 'nuclio.io/v1beta1',
            'kind': 'NuclioFunction',
            'metadata': {
                'name': config.name,
                'namespace': config.namespace,
                'labels': config.labels
            },
            'spec': {
                'runtime': config.runtime,
                'handler': config.handler,
                'resources': config.resources,
                'minReplicas': config.min_replicas,
                'maxReplicas': config.max_replicas,
                'triggers': {
                    'http': {
                        'kind': 'http',
                        'attributes': {'serviceType': 'NodePort'}
                    }
                },
                'env': [{'name': k, 'value': v} for k, v in config.env_vars.items()]
            }
        }

        # build configuration with custom commands
        build_config = {
            'noBaseImagesPull': True,
            'commands': ['pip install msgpack']  # default command
        }

        # add custom build commands
        if config.build_commands:
            build_config['commands'].extend(config.build_commands)

        if config.source_path:
            build_config['path'] = config.source_path
        elif config.source_code:
            build_config['functionSourceCode'] = base64.b64encode(
                config.source_code.encode('utf-8')
            ).decode('utf-8')

        # registry configuration
        if config.registry_url:
            build_config['registry'] = config.registry_url
        else:
            yaml_content['spec']['imagePullPolicy'] = 'Never'

        yaml_content['spec']['build'] = build_config

        return yaml_content

    def _get_registry_url(self) -> str:
        """Get the registry URL for builds"""
        if not self.nuclio_config.registry_config:
            return ""

        config = self.nuclio_config.registry_config

        if self.nuclio_config.shared_repository:
            # for shared repo return the full repository url
            if config.type == RegistryType.ECR:
                return f"{config.url}/nuclio-functions"
            elif config.type == RegistryType.DOCKERHUB:
                return f"{config.url}/nuclio-functions"
            else:
                return f"{config.url}/nuclio-functions"
        else:
            # for individual repos return just the registry url
            return config.url

    def _cache_function_endpoint(self, function_name: str):
        """Cache function endpoint for fast invocation"""
        with self._cache_lock:
            if self.deployment_target == DeploymentTarget.MINIKUBE:
                # get nodeport
                cmd = f"kubectl get svc nuclio-{function_name} -n {self.nuclio_config.namespace} -o jsonpath='{{.spec.ports[0].nodePort}}'"
                out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

                if ret == 0:
                    node_port = out.strip()
                    # get minikube ip
                    ip_cmd = "minikube ip"
                    ip_out, _, _ = sh_callout(ip_cmd, shell=True)
                    minikube_ip = ip_out.strip() if ip_out else 'localhost'

                    self._endpoint_cache[function_name] = {
                        'type': 'nodeport',
                        'host': minikube_ip,
                        'port': node_port,
                        'cached_at': time.time()
                    }
            else:
                # for remote cache the service url
                self._endpoint_cache[function_name] = {
                    'type': 'service',
                    'url': f"http://nuclio-{function_name}.{self.nuclio_config.namespace}.svc.cluster.local:8080",
                    'cached_at': time.time()
                }

    @lru_cache(maxsize=128)
    def _get_cached_endpoint(self, function_name: str) -> Optional[Dict[str, Any]]:
        """Get cached endpoint with TTL check"""
        with self._cache_lock:
            if function_name in self._endpoint_cache:
                endpoint = self._endpoint_cache[function_name]
                # check if cache is still valid
                if time.time() - endpoint['cached_at'] < self._cache_ttl:
                    return endpoint
                else:
                    # cache expired so remove it
                    del self._endpoint_cache[function_name]
        return None

    def _get_function_url(self, function_name: str) -> str:
        """Get function invocation URL"""
        endpoint = self._get_cached_endpoint(function_name)
        if endpoint:
            if endpoint['type'] == 'nodeport':
                return f"http://{endpoint['host']}:{endpoint['port']}"
            else:
                return endpoint['url']

        # fallback
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            return f"http://localhost:30000/{function_name}"
        else:
            return f"/api/gateway/{function_name}"

    def _get_function_info(self, function_name: str) -> Dict[str, Any]:
        """Get function information"""
        cmd = f"{self.nuctl_path} get function {function_name}"
        cmd += f" --namespace {self.nuclio_config.namespace}"
        cmd += " --output json"

        out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)
        if ret:
            self.logger.warning(f"Failed to get function info: {err}")
            return {}

        try:
            functions = json.loads(out)
            return functions[0] if functions else {}
        except json.JSONDecodeError:
            return {}

    @retry_on_error()
    def invoke_function(self, function_name: str, payload: Any = None,
                       invocation_type: str = 'RequestResponse') -> Any:
        """Invoke deployed function with connection pooling"""
        if self.deployment_target == DeploymentTarget.MINIKUBE:
            return self._invoke_minikube_ssh(function_name, payload)
        else:
            return self._invoke_remote(function_name, payload)

    def _invoke_minikube_ssh(self, function_name: str, payload: Any = None) -> Any:
        """Invoke function via SSH for optimal minikube performance"""
        # get cached endpoint or refresh
        endpoint = self._get_cached_endpoint(function_name)

        if not endpoint or endpoint['type'] != 'nodeport':
            # cache miss or wrong type so refresh cache
            self._cache_function_endpoint(function_name)
            endpoint = self._get_cached_endpoint(function_name)

        if not endpoint:
            raise NuclioException(f"Function {function_name} endpoint not found")

        # prepare payload
        data_arg = ""
        if payload:
            if isinstance(payload, dict):
                json_data = json.dumps(payload)
                data_arg = f"-d '{json_data}'"
            else:
                data_arg = f"-d '{payload}'"

        # execute via ssh with connection pooling
        ssh_client = self.connection_pool.get_ssh_client(
            endpoint['host'],
            reuse_timeout=CONNECTION_REUSE_TIMEOUT
        )

        curl_cmd = f"curl -s -X POST http://localhost:{endpoint['port']} {data_arg}"
        if payload and isinstance(payload, dict):
            curl_cmd += " -H 'Content-Type: application/json'"

        stdin, stdout, stderr = ssh_client.exec_command(curl_cmd)
        result = stdout.read().decode('utf-8')
        error = stderr.read().decode('utf-8')

        if error:
            raise NuclioException(f"Invocation error: {error}")

        # try to parse as json
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return result

    def _invoke_remote(self, function_name: str, payload: Any = None) -> Any:
        """Invoke function via gateway for remote deployments"""
        # use gateway
        gateway_url = f"{self._get_nginx_url()}/api/gateway/{function_name}"

        client = self.connection_pool.get_http_client()

        try:
            if payload:
                response = client.post(gateway_url, json=payload)
            else:
                response = client.get(gateway_url)

            response.raise_for_status()

            # try to parse as json
            try:
                return response.json()
            except:
                return response.text

        except Exception as e:
            raise NuclioException(f'Failed to invoke function {function_name}: {str(e)}')

    def _get_nginx_url(self) -> str:
        """Get NGINX ingress URL"""
        cmd = "kubectl get svc -n ingress-nginx ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
        out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

        if ret == 0 and out:
            return f"http://{out.strip()}"
        else:
            raise NuclioException("Could not determine NGINX URL")

    def invoke_batch(self, invocations: List[Tuple[str, Any]]) -> List[Any]:
        """Invoke multiple functions in parallel"""
        return self.batch_processor.invoke_batch(invocations, self.invoke_function)

    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions"""
        try:
            cmd = f"{self.nuctl_path} get functions --namespace {self.nuclio_config.namespace} --output json"
            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)

            if ret == 0 and out:
                functions = json.loads(out) or []
                return [
                    {
                        'name': f.get('metadata', {}).get('name', ''),
                        'state': f.get('status', {}).get('state', 'unknown'),
                        'replicas': f.get('status', {}).get('availableReplicas', 0),
                        'url': self._get_function_url(f.get('metadata', {}).get('name', ''))
                    }
                    for f in functions
                ]
            return []

        except Exception as e:
            self.logger.error(f"Failed to list functions: {e}")
            return []

    @retry_on_error()
    def delete_function(self, function_name: str):
        """Delete a deployed function"""
        try:
            cmd = f"{self.nuctl_path} delete function {function_name}"
            cmd += f" --namespace {self.nuclio_config.namespace}"

            out, err, ret = sh_callout(cmd, shell=True, kube=self.k8s_cluster)
            if ret:
                self.logger.warning(f"Failed to delete function: {err}")

            # update tracking
            with self._task_lock:
                self._deployed_functions.pop(function_name, None)
                for name, task in list(self._functions_book.items()):
                    if task.name == function_name:
                        del self._functions_book[name]
                        break

            # clear cache
            with self._cache_lock:
                self._endpoint_cache.pop(function_name, None)
            self._get_cached_endpoint.cache_clear()

            self.logger.info(f"Deleted function: {function_name}")

        except Exception as e:
            self.logger.error(f"Failed to delete function {function_name}: {e}")
            raise

    def get_tasks(self) -> List[Task]:
        """Get all deployed tasks"""
        with self._task_lock:
            return list(self._functions_book.values())

    def get_resource_status(self) -> Dict[str, Any]:
        """Get status of Nuclio resources"""
        try:
            return {
                'active': self.status,
                'functions_count': len(self._deployed_functions),
                'active_deployments': len(self._active_deployments),
                'deployment_target': self.deployment_target.value,
                'namespace': self.nuclio_config.namespace,
                'registry_configured': bool(self.nuclio_config.registry_config),
                'cluster_status': 'ready' if self.k8s_cluster else 'pending',
                'cached_endpoints': len(self._endpoint_cache),
                'active_connections': {
                    'ssh': len(self.connection_pool._ssh_clients),
                    'http': len(self.connection_pool._http_clients)
                }
            }
        except Exception as e:
            self.logger.error(f"Failed to get resource status: {e}")
            return {'active': False, 'error': str(e)}

    def shutdown(self):
        """Shutdown provider and cleanup resources with graceful shutdown"""
        if not self.status:
            return

        self.logger.info("Shutting down Nuclio provider")
        self._terminate.set()

        # wait for active deployments to complete
        start_time = time.time()
        while self._active_deployments and time.time() - start_time < GRACEFUL_SHUTDOWN_TIMEOUT:
            self.logger.info(f"Waiting for {len(self._active_deployments)} active deployments...")
            time.sleep(1)

        if self._active_deployments:
            self.logger.warning(f"{len(self._active_deployments)} deployments still active after timeout")

        # wait for worker thread
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)

        # close connection pool
        self.connection_pool.close_all()

        # shutdown thread pool
        self.executor.shutdown(wait=True)

        # cleanup functions if auto_terminate
        if self.auto_terminate:
            functions_to_delete = list(self._deployed_functions.keys())
            for function_name in functions_to_delete:
                try:
                    self.delete_function(function_name)
                except Exception as e:
                    self.logger.error(f"Error deleting function {function_name}: {e}")

        # shutdown kubernetes cluster if we created it and not reusing
        if self.k8s_cluster and not self._existing_cluster and not self.cred.get('use_existing', False):
            self.logger.info("Shutting down Kubernetes cluster")
            self.k8s_cluster.shutdown()

        self.status = False
        self._shutdown_complete.set()
        self.logger.info("Nuclio provider shutdown complete")

    @property
    def is_active(self):
        """Check if provider is active"""
        return self.status