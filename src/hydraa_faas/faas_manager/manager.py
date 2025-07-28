"""
FaaS Manager - Production-ready version with improved integration
Coordinates FaaS providers (Lambda, Nuclio) with proper sandbox management
"""

import os
import time
import queue
import threading
from typing import Dict, Any, List, Union, Optional, Callable
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# import from hydraa
from hydraa import Task, proxy
from hydraa.services.caas_manager.utils import misc

# import our providers
from .aws_lambda import AwsLambda
from .custom_faas import NuclioProvider
from ..utils.exceptions import FaasException
from ..utils.resource_manager import ResourceManager

# provider constants
LAMBDA = 'lambda'
NUCLIO = 'nuclio'

# termination signals following hydraa pattern
TERM_SIGNALS = {
    0: "Auto-terminate was set, terminating.",
    1: "No more tasks to process, terminating.",
    2: "User termination requested, terminating.",
    3: "Internal failure detected, terminating."
}

# configuration keeping same pattern as hydraa with string values
TIMEOUT = 0.1
MAX_BULK_SIZE = os.environ.get('FAAS_MAX_BULK_SIZE', '10')
MAX_BULK_TIME = os.environ.get('FAAS_MAX_BULK_TIME', '2')
MIN_BULK_TIME = os.environ.get('FAAS_MIN_BULK_TIME', '0.1')
MAX_PARALLEL_DEPLOYMENTS = int(os.environ.get('FAAS_MAX_PARALLEL_DEPLOYMENTS', '5'))
GRACEFUL_SHUTDOWN_TIMEOUT = int(os.environ.get('FAAS_GRACEFUL_SHUTDOWN_TIMEOUT', '30'))


class FaasManager:
    """
    FaaS Manager orchestrates function-as-a-service providers.

    This manager can work independently of CaaS manager. For providers like Nuclio
    that need Kubernetes, it will use Hydraa's K8s classes directly.
    """

    def __init__(self,
                 proxy_mgr: proxy,
                 vms: List[Any] = None,
                 asynchronous: bool = True,
                 auto_terminate: bool = True,
                 resource_manager: Optional[ResourceManager] = None,
                 resource_config: Optional[Dict[str, Any]] = None,
                 caas_manager: Optional[Any] = None) -> None:
        """
        Initialize FaaS Manager.

        Args:
            proxy_mgr: Hydraa proxy for credential management
            vms: List of VMs for deployments (required for Nuclio, optional for Lambda)
            asynchronous: Run operations asynchronously
            auto_terminate: Auto-terminate resources on shutdown
            resource_manager: Optional ResourceManager instance
            resource_config: Optional resource configuration per provider
            caas_manager: Optional CaaS manager for K8s cluster reuse
        """
        self.sandbox = None
        self.vms = vms or []
        self._proxy = proxy_mgr
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_manager = resource_manager or ResourceManager()
        self.resource_config = resource_config or {}
        self.caas_manager = caas_manager

        # generate unique id using hydraa utility
        self._manager_id = misc.generate_id(prefix='faas-mgr', length=8)

        # provider registry
        self._providers = OrderedDict()
        self._provider_threads = []

        # threading following hydraa pattern
        self._terminate = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutdown_complete = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=10)

        # task tracking
        self._task_count = 0
        self._active_tasks = set()
        self._task_lock = threading.Lock()
        self._pending_tasks = queue.Queue()

        # logger will be set in start
        self.logger = None

        # state
        self._initialized = False

    def start(self, sandbox: str) -> None:
        """
        Start the FaaS Manager and initialize providers.

        Args:
            sandbox: Sandbox directory path from ServiceManager.
        """
        if self._initialized:
            self.logger.info("Manager already started")
            return

        if not sandbox:
            raise ValueError("Sandbox must be provided by ServiceManager")

        # create faas specific sandbox within the provided sandbox
        self.sandbox = os.path.join(sandbox, f"faas_{self._manager_id}")
        os.makedirs(self.sandbox, exist_ok=True)

        # use hydraa logger utility
        self.logger = misc.logger(path=os.path.join(self.sandbox, 'faas_manager.log'))
        self.logger.info(f"Starting FaaS manager {self._manager_id}")

        # initialize providers based on proxy configuration
        self._initialize_providers()

        # start result monitoring threads
        self._start_result_monitors()

        self._initialized = True
        self.logger.info(f'FaaS manager started with ID: {self._manager_id}')

    def _group_vms_by_provider(self) -> Dict[str, List[Any]]:
        """Group VMs by their provider for multi cloud support"""
        vm_groups = defaultdict(list)
        for vm in self.vms:
            vm_groups[vm.Provider].append(vm)
        return dict(vm_groups)

    def _initialize_providers(self) -> None:
        """Initialize providers based on available credentials with parallel initialization"""
        # create futures for parallel provider initialization
        futures = []
        future_to_provider = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            # check aws credentials for lambda
            if 'aws' in self._proxy.loaded_providers:
                future = executor.submit(self._init_lambda_provider)
                futures.append(future)
                future_to_provider[future] = 'lambda'

            # check for nuclio deployment requires vms
            if self.vms:
                # group vms by provider for multi cloud support
                vm_groups = self._group_vms_by_provider()

                for vm_provider, provider_vms in vm_groups.items():
                    if vm_provider in self._proxy.loaded_providers:
                        future = executor.submit(
                            self._init_nuclio_provider,
                            vm_provider,
                            provider_vms
                        )
                        futures.append(future)
                        future_to_provider[future] = f"nuclio-{vm_provider}"

            # wait for all provider initialization to complete
            for future in as_completed(futures):
                provider_name = future_to_provider[future]
                try:
                    provider_info = future.result()
                    if provider_info:
                        self._providers[provider_name] = provider_info
                        self.logger.info(f"Initialized {provider_name} provider")
                except Exception as e:
                    self.logger.error(f"Failed to initialize {provider_name} provider: {e}")

        if not self._providers:
            raise FaasException("No FaaS providers could be initialized")

    def _init_lambda_provider(self) -> Optional[Dict[str, Any]]:
        """Initialize AWS Lambda provider"""
        try:
            cred = self._proxy._load_credentials('aws')

            # get aws specific resource config
            aws_resource_config = self.resource_config.get('aws', {})

            # create provider specific sandbox
            lambda_sandbox = os.path.join(self.sandbox, 'lambda')
            os.makedirs(lambda_sandbox, exist_ok=True)

            provider = AwsLambda(
                sandbox=lambda_sandbox,
                manager_id=self._manager_id,
                cred=cred,
                asynchronous=self.asynchronous,
                auto_terminate=self.auto_terminate,
                log=self.logger,
                resource_manager=self.resource_manager,
                resource_config=aws_resource_config
            )

            return {
                'instance': provider,
                'in_q': provider.incoming_q,
                'out_q': provider.outgoing_q,
                'active': True
            }
        except Exception as e:
            self.logger.error(f"Failed to initialize Lambda provider: {e}")
            return None

    def _init_nuclio_provider(self, vm_provider: str, provider_vms: List[Any]) -> Optional[Dict[str, Any]]:
        """Initialize Nuclio provider for a specific cloud provider"""
        try:
            cred = self._proxy._load_credentials(vm_provider)

            # get provider specific resource config if available
            provider_resource_config = self.resource_config.get(vm_provider, {})

            # check if we can reuse existing k8s cluster from caas
            if self.caas_manager and hasattr(self.caas_manager, 'get_k8s_cluster'):
                existing_cluster = self.caas_manager.get_k8s_cluster(vm_provider)
                if existing_cluster:
                    self.logger.info(f"Reusing existing K8s cluster from CaaS manager")
                    provider_resource_config['existing_cluster'] = existing_cluster

            # create nuclio specific sandbox
            nuclio_sandbox = os.path.join(self.sandbox, f"nuclio-{vm_provider}")
            os.makedirs(nuclio_sandbox, exist_ok=True)

            provider = NuclioProvider(
                sandbox=nuclio_sandbox,
                manager_id=self._manager_id,
                vms=provider_vms,
                cred=cred,
                asynchronous=self.asynchronous,
                auto_terminate=self.auto_terminate,
                log=self.logger,
                resource_manager=self.resource_manager,
                resource_config=provider_resource_config
            )

            return {
                'instance': provider,
                'in_q': provider.incoming_q,
                'out_q': provider.outgoing_q,
                'active': True,
                'vm_provider': vm_provider
            }
        except Exception as e:
            self.logger.error(f"Failed to initialize Nuclio provider for {vm_provider}: {e}")
            return None

    def _start_result_monitors(self) -> None:
        """Start threads to monitor provider results"""
        for provider_name, provider_info in self._providers.items():
            thread = threading.Thread(
                target=self._monitor_provider,
                args=(provider_name, provider_info),
                name=f"{provider_name}-Monitor",
                daemon=True
            )
            thread.start()
            self._provider_threads.append(thread)

    def _monitor_provider(self, provider_name: str, provider_info: Dict[str, Any]) -> None:
        """Monitor a provider output queue for results"""
        out_q = provider_info['out_q']

        while not self._terminate.is_set():
            try:
                msg = out_q.get(block=True, timeout=TIMEOUT)
                if msg:
                    # handle termination signals
                    if isinstance(msg, tuple) and len(msg) == 2:
                        term_sig, provider = msg
                        term_msg = TERM_SIGNALS.get(term_sig, f"Unknown signal: {term_sig}")
                        self.logger.info(f"{term_msg} from {provider}")
                        if not self._terminate.is_set():
                            self.shutdown()

                    # handle regular messages
                    elif isinstance(msg, str):
                        self.logger.info(f'{provider_name} reported: {msg}')

                    # handle structured results
                    elif isinstance(msg, dict):
                        self._handle_result(provider_name, msg)

                    else:
                        self.logger.warning(f'Unexpected message type from {provider_name}: {type(msg)}')

            except queue.Empty:
                continue
            except Exception as e:
                if not self._terminate.is_set():
                    self.logger.error(f'Error monitoring {provider_name}: {e}')

    def _handle_result(self, provider_name: str, result: Dict[str, Any]) -> None:
        """Handle structured result from provider"""
        function_name = result.get('function_name')
        status = result.get('state')

        if function_name and status:
            self.logger.debug(f"Function {function_name} from {provider_name}: {status}")

            # update task tracking
            with self._task_lock:
                if status in ['deployed', 'completed']:
                    self._active_tasks.discard(function_name)
                elif status == 'failed':
                    self._active_tasks.discard(function_name)

    def submit(self, tasks: Union[Task, List[Task]]) -> None:
        """
        Submit tasks for deployment to appropriate providers.
        Now supports parallel submission within each provider.

        Args:
            tasks: Single task or list of tasks to deploy
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        if not isinstance(tasks, list):
            tasks = [tasks]

        if not tasks:
            return

        # group tasks by target provider
        provider_tasks = defaultdict(list)

        for task in tasks:
            if not isinstance(task, Task):
                raise ValueError(f'Task must be of type {Task}')

            # determine provider
            provider_name = getattr(task, 'provider', '').lower()

            # route to appropriate provider
            if provider_name == LAMBDA and LAMBDA in self._providers:
                provider_tasks[LAMBDA].append(task)
            elif provider_name == NUCLIO:
                # find the appropriate nuclio instance
                nuclio_provider = self._find_nuclio_provider(task)
                if nuclio_provider:
                    provider_tasks[nuclio_provider].append(task)
                else:
                    # fallback to first available provider
                    provider_tasks[list(self._providers.keys())[0]].append(task)
            else:
                # default routing based on task properties
                if hasattr(task, 'handler_code') or hasattr(task, 'source_path'):
                    # code based deployment to lambda if available
                    if LAMBDA in self._providers:
                        provider_tasks[LAMBDA].append(task)
                    else:
                        provider_tasks[list(self._providers.keys())[0]].append(task)
                else:
                    # container based deployment to nuclio if available
                    nuclio_provider = self._find_first_nuclio_provider()
                    if nuclio_provider:
                        provider_tasks[nuclio_provider].append(task)
                    else:
                        provider_tasks[list(self._providers.keys())[0]].append(task)

        # submit tasks to providers with optional parallelization
        errors = self._submit_to_providers_parallel(provider_tasks)

        # log any errors that occurred
        if errors:
            for provider_name, provider_errors in errors.items():
                for error in provider_errors:
                    self.logger.error(f"Error in {provider_name}: {error}")

        self.logger.info(f'Submitted {len(tasks)} function(s) for deployment')

    def _find_nuclio_provider(self, task: Task) -> Optional[str]:
        """Find the appropriate Nuclio provider for a task based on VM provider"""
        # check if task has a specific cloud provider hint
        task_cloud_provider = getattr(task, 'cloud_provider', None)

        for provider_name, provider_info in self._providers.items():
            if provider_name.startswith(NUCLIO):
                if task_cloud_provider:
                    # match based on explicit cloud provider
                    if provider_info.get('vm_provider') == task_cloud_provider:
                        return provider_name
                else:
                    # return first available nuclio provider
                    return provider_name

        return None

    def _find_first_nuclio_provider(self) -> Optional[str]:
        """Find the first available Nuclio provider"""
        for provider_name in self._providers.keys():
            if provider_name.startswith(NUCLIO):
                return provider_name
        return None

    def _submit_to_providers_parallel(self, provider_tasks: Dict[str, List[Task]]) -> Dict[str, List[Exception]]:
        """Submit tasks to providers with parallel submission support and error tracking"""
        futures = []
        future_to_info = {}
        errors = defaultdict(list)

        for provider_name, tasks in provider_tasks.items():
            provider_info = self._providers[provider_name]

            # check if parallel submission is enabled
            if MAX_PARALLEL_DEPLOYMENTS > 1 and len(tasks) > 1:
                # submit tasks in parallel batches
                for i in range(0, len(tasks), MAX_PARALLEL_DEPLOYMENTS):
                    batch = tasks[i:i + MAX_PARALLEL_DEPLOYMENTS]
                    future = self.executor.submit(
                        self._submit_task_batch,
                        provider_info,
                        batch,
                        provider_name
                    )
                    futures.append(future)
                    future_to_info[future] = (provider_name, batch)
            else:
                # sequential submission original behavior
                for task in tasks:
                    provider_info['in_q'].put(task)

                    # track task
                    with self._task_lock:
                        self._task_count += 1
                        self._active_tasks.add(getattr(task, 'name', f'task-{self._task_count}'))

        # wait for parallel submissions to complete and collect errors
        for future in as_completed(futures):
            provider_name, batch = future_to_info[future]
            try:
                future.result()
            except Exception as e:
                errors[provider_name].append(e)
                # mark all tasks in the batch as failed
                for task in batch:
                    task.state = 'failed'

        return errors

    def _submit_task_batch(self, provider_info: Dict[str, Any], tasks: List[Task], provider_name: str) -> None:
        """Submit a batch of tasks to a provider"""
        for task in tasks:
            provider_info['in_q'].put(task)

            # track task
            with self._task_lock:
                self._task_count += 1
                self._active_tasks.add(getattr(task, 'name', f'task-{self._task_count}'))

        self.logger.debug(f"Submitted batch of {len(tasks)} tasks to {provider_name}")

    def invoke(self, function_name: str, payload: Any = None,
               provider: str = None) -> Dict[str, Any]:
        """
        Invoke a deployed function.

        Args:
            function_name: Name of the function to invoke
            payload: Payload to send to function
            provider: Specific provider to use (if None, tries all)

        Returns:
            Invocation response
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        last_error = None

        # try specific provider if specified
        if provider:
            provider_lower = provider.lower()

            # handle both simple provider names and instance names like nuclio-aws
            matching_providers = [p for p in self._providers.keys() if p.startswith(provider_lower)]

            for prov_name in matching_providers:
                try:
                    instance = self._providers[prov_name]['instance']
                    return instance.invoke_function(function_name, payload)
                except Exception as e:
                    last_error = e
                    continue

            if last_error:
                raise FaasException(f'Failed to invoke on {provider}: {str(last_error)}')
            else:
                raise FaasException(f'Provider {provider} not available')

        # try all providers
        for prov_name, prov_info in self._providers.items():
            if not prov_info['active']:
                continue

            try:
                instance = prov_info['instance']
                return instance.invoke_function(function_name, payload)
            except Exception as e:
                last_error = e
                continue

        if last_error:
            raise FaasException(f'Function {function_name} not found on any provider: {str(last_error)}')
        else:
            raise FaasException(f'No active providers available')

    def list_functions(self, provider: str = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        List functions across providers.

        Args:
            provider: Specific provider to list from (if None, lists all)

        Returns:
            Dictionary mapping provider names to function lists
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        results = {}

        # determine which providers to query
        if provider:
            # handle both simple provider names and instance names
            providers_to_check = [p for p in self._providers.keys()
                                 if p.startswith(provider.lower())]
        else:
            providers_to_check = list(self._providers.keys())

        # query each provider
        for prov_name in providers_to_check:
            prov_info = self._providers[prov_name]
            if not prov_info['active']:
                results[prov_name] = []
                continue

            try:
                instance = prov_info['instance']
                functions = instance.list_functions()
                results[prov_name] = functions
            except Exception as e:
                self.logger.error(f'Failed to list functions from {prov_name}: {e}')
                results[prov_name] = []

        return results

    def delete_function(self, function_name: str, provider: str = None) -> None:
        """
        Delete a function from provider(s).

        Args:
            function_name: Name of function to delete
            provider: Specific provider (if None, tries all)
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        success_count = 0

        # determine which providers to check
        if provider:
            providers_to_check = [p for p in self._providers.keys()
                                 if p.startswith(provider.lower())]
        else:
            providers_to_check = list(self._providers.keys())

        # try to delete from each provider
        for prov_name in providers_to_check:
            prov_info = self._providers[prov_name]
            if not prov_info['active']:
                continue

            try:
                instance = prov_info['instance']
                instance.delete_function(function_name)
                success_count += 1
                self.logger.info(f'Deleted function {function_name} from {prov_name}')
            except Exception as e:
                self.logger.debug(f'Function {function_name} not found on {prov_name}: {e}')

        if success_count == 0:
            raise FaasException(f'Function {function_name} not found on any provider')

    def get_status(self) -> Dict[str, Any]:
        """Get status of the FaaS manager and providers"""
        status = {
            'manager_id': self._manager_id,
            'initialized': self._initialized,
            'active_tasks': len(self._active_tasks),
            'total_tasks': self._task_count,
            'providers': {}
        }

        for name, info in self._providers.items():
            try:
                instance = info['instance']
                if hasattr(instance, 'get_resource_status'):
                    provider_status = instance.get_resource_status()
                else:
                    provider_status = {'active': info['active']}

                provider_status['queue_size'] = info['in_q'].qsize()
                if 'vm_provider' in info:
                    provider_status['vm_provider'] = info['vm_provider']

                status['providers'][name] = provider_status

            except Exception as e:
                status['providers'][name] = {'error': str(e)}

        return status

    def get_provider_health(self) -> Dict[str, Dict[str, Any]]:
        """Check health of all providers"""
        health = {}
        for name, info in self._providers.items():
            try:
                instance = info['instance']
                health[name] = {
                    'active': info['active'],
                    'queue_size': info['in_q'].qsize(),
                    'responsive': instance.is_active if hasattr(instance, 'is_active') else info['active'],
                    'type': 'lambda' if name.startswith('lambda') else 'nuclio'
                }

                # add provider specific health info
                if hasattr(instance, 'get_resource_status'):
                    resource_status = instance.get_resource_status()
                    health[name].update({
                        'functions_count': resource_status.get('functions_count', 0),
                        'has_resources': resource_status.get('has_iam_role', False) or
                                       resource_status.get('cluster_status') == 'ready'
                    })

            except Exception as e:
                health[name] = {
                    'active': False,
                    'error': str(e),
                    'responsive': False
                }

        return health

    def shutdown(self) -> None:
        """Shutdown FaaS Manager and clean up resources with graceful shutdown"""
        with self._shutdown_lock:
            if self._terminate.is_set():
                return

            self.logger.info("Shutting down FaaS Manager")
            self._terminate.set()

            # stop accepting new work
            for prov_info in self._providers.values():
                prov_info['active'] = False

            # wait for active tasks to complete with timeout
            start_time = time.time()
            while self._active_tasks and time.time() - start_time < GRACEFUL_SHUTDOWN_TIMEOUT:
                self.logger.info(f"Waiting for {len(self._active_tasks)} active tasks...")
                time.sleep(1)

            if self._active_tasks:
                self.logger.warning(f"{len(self._active_tasks)} tasks still active after {GRACEFUL_SHUTDOWN_TIMEOUT}s timeout")

            # shutdown providers with proper cleanup
            shutdown_futures = []
            with ThreadPoolExecutor(max_workers=len(self._providers)) as executor:
                for name, info in self._providers.items():
                    future = executor.submit(self._shutdown_provider, name, info)
                    shutdown_futures.append(future)

                # wait for all providers to shutdown
                for future in as_completed(shutdown_futures, timeout=30):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Error during provider shutdown: {e}")

            # shutdown executor
            self.executor.shutdown(wait=True)

            # save resources
            try:
                self.resource_manager.save_all_resources()
                self.logger.info("Resources saved successfully")
            except Exception as e:
                self.logger.error(f"Failed to save resources: {e}")

            # signal shutdown complete
            self._shutdown_complete.set()
            self.logger.info("FaaS Manager shutdown complete")

    def _shutdown_provider(self, name: str, info: Dict[str, Any]) -> None:
        """Shutdown a single provider with timeout"""
        try:
            self.logger.info(f"Shutting down provider: {name}")
            instance = info['instance']
            if hasattr(instance, 'shutdown'):
                instance.shutdown()
        except Exception as e:
            self.logger.error(f"Error shutting down {name}: {e}")

    def wait_for_shutdown(self, timeout: float = None) -> bool:
        """Wait for shutdown to complete"""
        return self._shutdown_complete.wait(timeout)

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.shutdown()

    def __call__(self, func: Callable = None, provider: str = '') -> Callable:
        """
        Decorator for automatic function deployment.

        Args:
            func: Function to decorate
            provider: Target provider (lambda, nuclio)

        Returns:
            Wrapped function that returns a task
        """
        if func is None:
            return lambda f: self.__call__(f, provider)

        def wrapper(*args, **kwargs):
            task = func(*args, **kwargs)

            if not isinstance(task, Task):
                raise ValueError(f'Function must return object of type {Task}')

            if not task.provider:
                task.provider = provider

            self.submit(task)
            return task

        return wrapper