"""
FaaS Manager - Fixed version with accurate metrics integration
Coordinates FaaS providers with proper timing separation
"""

import os
import time
import queue
import threading
from typing import Dict, Any, List, Union, Optional, Callable, Tuple
from collections import OrderedDict, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

# import from hydraa
from hydraa import Task, proxy
from hydraa.services.caas_manager.utils import misc

# import our providers
from .aws_lambda import AwsLambda
from .custom_faas import NuclioProvider
from ..utils.exceptions import FaasException
from ..utils.resource_manager import ResourceManager
from ..utils.metrics_collector import MetricsCollector, SimpleTimer

# provider constants
LAMBDA = 'lambda'
NUCLIO = 'nuclio'

# termination signals
TERM_SIGNALS = {
    0: "Auto-terminate was set, terminating.",
    1: "No more tasks to process, terminating.",
    2: "User termination requested, terminating.",
    3: "Internal failure detected, terminating."
}

TIMEOUT = 0.01  # Was 0.05 - faster queue checking
MAX_BULK_SIZE = os.environ.get('FAAS_MAX_BULK_SIZE', '1000')  # Was 50
MAX_BULK_TIME = os.environ.get('FAAS_MAX_BULK_TIME', '0.1')  # Was 0.5
MIN_BULK_TIME = os.environ.get('FAAS_MIN_BULK_TIME', '0.001')  # Was 0.01
MAX_PARALLEL_DEPLOYMENTS = int(os.environ.get('FAAS_MAX_PARALLEL_DEPLOYMENTS', '1000'))  # Safe parallel limit
GRACEFUL_SHUTDOWN_TIMEOUT = int(os.environ.get('FAAS_GRACEFUL_SHUTDOWN_TIMEOUT', '30'))  # Reasonable cleanup time

class FaasManager:
    """
    FaaS Manager orchestrates function-as-a-service providers with accurate metrics tracking.
    """

    def __init__(self,
                 proxy_mgr: proxy,
                 vms: List[Any] = None,
                 asynchronous: bool = True,
                 auto_terminate: bool = True,
                 resource_manager: Optional[ResourceManager] = None,
                 resource_config: Optional[Dict[str, Any]] = None,
                 caas_manager: Optional[Any] = None,
                 metrics_collector: Optional[MetricsCollector] = None) -> None:
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
            metrics_collector: Optional MetricsCollector for benchmarking
        """
        self.sandbox = None
        self.vms = vms or []
        self._proxy = proxy_mgr
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_manager = resource_manager or ResourceManager()
        self.resource_config = resource_config or {}
        self.caas_manager = caas_manager
        self.metrics_collector = metrics_collector

        # generate unique id
        self._manager_id = misc.generate_id(prefix='faas-mgr', length=8)

        # provider registry
        self._providers = OrderedDict()
        self._provider_threads = []

        # threading
        self._terminate = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutdown_complete = threading.Event()

        # task tracking
        self._task_count = 0
        self._active_tasks = set()
        self._task_lock = threading.Lock()
        self._pending_tasks = queue.Queue()

        # function provider lookup cache
        self._function_provider_cache = {}
        self._cache_lock = threading.Lock()

        # name mapping
        self._original_name_mapping = {}
        self._deployed_name_mapping = {}
        self._name_mapping_lock = threading.Lock()

        # logger will be set in start
        self.logger = None

        # state
        self._initialized = False
        self._active = True

        # thread pool for parallel provider operations
        self.executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_DEPLOYMENTS)

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

        # create faas specific sandbox
        self.sandbox = os.path.join(sandbox, f"faas_{self._manager_id}")
        os.makedirs(self.sandbox, exist_ok=True)

        # use hydraa logger utility
        self.logger = misc.logger(path=os.path.join(self.sandbox, 'faas_manager.log'))
        self.logger.info(f"Starting FaaS manager {self._manager_id}")

        # initialize providers
        self._initialize_providers()

        # start result monitoring threads
        self._start_result_monitors()

        self._initialized = True
        self.logger.info(f'FaaS manager started with ID: {self._manager_id}')

    def _initialize_providers(self) -> None:
        """Initialize providers based on available credentials"""
        # check aws credentials for lambda
        if 'aws' in self._proxy.loaded_providers:
            try:
                provider_info = self._init_lambda_provider()
                if provider_info:
                    self._providers['lambda'] = provider_info
                    self.logger.info("Initialized lambda provider")
            except Exception as e:
                self.logger.error(f"Failed to initialize lambda provider: {e}")

        # check for nuclio deployment
        if self.vms:
            # group vms by provider
            vm_groups = defaultdict(list)
            for vm in self.vms:
                vm_groups[vm.Provider].append(vm)

            for vm_provider, provider_vms in vm_groups.items():
                if vm_provider in self._proxy.loaded_providers:
                    try:
                        provider_info = self._init_nuclio_provider(vm_provider, provider_vms)
                        if provider_info:
                            provider_name = f"nuclio-{vm_provider}"
                            self._providers[provider_name] = provider_info
                            self.logger.info(f"Initialized {provider_name} provider")
                    except Exception as e:
                        self.logger.error(f"Failed to initialize nuclio-{vm_provider} provider: {e}")

        if not self._providers:
            raise FaasException("No FaaS providers could be initialized")

    def _init_lambda_provider(self) -> Optional[Dict[str, Any]]:
        """Initialize AWS Lambda provider"""
        try:
            # get credentials
            cred = {
                'aws_access_key_id': os.environ.get('AWS_ACCESS_KEY_ID') or os.environ.get('ACCESS_KEY_ID'),
                'aws_secret_access_key': os.environ.get('AWS_SECRET_ACCESS_KEY') or os.environ.get('ACCESS_KEY_SECRET'),
                'region_name': os.environ.get('AWS_DEFAULT_REGION') or os.environ.get('AWS_REGION', 'us-east-1'),
                'auto_setup_resources': True
            }

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
                resource_config=aws_resource_config,
                metrics_collector=self.metrics_collector
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

            # get provider specific resource config
            provider_resource_config = self.resource_config.get(vm_provider, {})

            # check if we can reuse existing k8s cluster
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
                resource_config=provider_resource_config,
                metrics_collector=self.metrics_collector
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
                    # update cache when function is deployed
                    with self._cache_lock:
                        self._function_provider_cache[function_name] = provider_name
                elif status == 'failed':
                    self._active_tasks.discard(function_name)
                    # remove from cache if deployment failed
                    with self._cache_lock:
                        self._function_provider_cache.pop(function_name, None)

    def submit(self, tasks: Union[Task, List[Task]]) -> None:
        """
        Submit tasks for deployment to appropriate providers.

        Args:
            tasks: Single task or list of tasks to deploy
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        if not self._active:
            raise FaasException("Manager is shutting down, not accepting new tasks")

        if not isinstance(tasks, list):
            tasks = [tasks]

        if not tasks:
            return

        # Track submission with metrics if available
        if self.metrics_collector:
            submission_timer = SimpleTimer()
            submission_timer.start()
        else:
            submission_timer = None

        # Sort tasks by dependencies
        sorted_tasks = self._sort_tasks_by_dependencies(tasks)

        # Process tasks in dependency order
        for task_batch in sorted_tasks:
            # Wait for dependencies
            self._wait_for_dependencies(task_batch)

            # Route tasks to providers
            provider_tasks = self._route_tasks_to_providers(task_batch)

            # Submit tasks to providers
            self._submit_to_providers_parallel(provider_tasks)

        self.logger.info(f'Submitted {len(tasks)} function(s) for deployment')

    def _submit_to_providers_parallel(self, provider_tasks: Dict[str, List[Task]]) -> None:
        """Submit tasks to providers in parallel"""
        if len(provider_tasks) == 1:
            # Single provider
            provider_name = list(provider_tasks.keys())[0]
            tasks = provider_tasks[provider_name]
            self._submit_to_single_provider(provider_name, tasks)
        else:
            # Multiple providers
            futures = []
            for provider_name, tasks in provider_tasks.items():
                future = self.executor.submit(self._submit_to_single_provider, provider_name, tasks)
                futures.append(future)

            # Wait for all submissions
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    self.logger.error(f"Error submitting to provider: {e}")

    def _submit_to_single_provider(self, provider_name: str, tasks: List[Task]) -> None:
        """Submit tasks to a single provider"""
        provider_info = self._providers[provider_name]

        # Submit all tasks to the provider's queue
        for task in tasks:
            provider_info['in_q'].put(task)

            # Track task
            with self._task_lock:
                self._task_count += 1
                self._active_tasks.add(getattr(task, 'name', f'task-{self._task_count}'))

        self.logger.debug(f"Submitted {len(tasks)} tasks to {provider_name}")

    def _sort_tasks_by_dependencies(self, tasks: List[Task]) -> List[List[Task]]:
        """Sort tasks by dependencies using topological sort"""
        # Build dependency graph
        task_map = {id(task): task for task in tasks}
        in_degree = {id(task): 0 for task in tasks}
        adjacency = defaultdict(list)

        # Count dependencies
        for task in tasks:
            if hasattr(task, 'depends_on') and task.depends_on:
                for dep in task.depends_on:
                    if isinstance(dep, Task):
                        dep_id = id(dep)
                    elif isinstance(dep, str):
                        # Find task by name
                        dep_task = next((t for t in tasks if t.name == dep), None)
                        if dep_task:
                            dep_id = id(dep_task)
                        else:
                            self.logger.warning(f"Dependency '{dep}' not found in current batch")
                            continue
                    else:
                        continue

                    if dep_id in task_map:
                        adjacency[dep_id].append(id(task))
                        in_degree[id(task)] += 1

        # Topological sort
        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        sorted_batches = []

        while queue:
            current_batch = []
            next_queue = []

            for task_id in queue:
                current_batch.append(task_map[task_id])

                # Reduce in-degree for dependent tasks
                for neighbor in adjacency[task_id]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        next_queue.append(neighbor)

            sorted_batches.append(current_batch)
            queue = next_queue

        # Check for cycles
        total_sorted = sum(len(batch) for batch in sorted_batches)
        if total_sorted != len(tasks):
            raise ValueError("Circular dependency detected in tasks")

        self.logger.info(f"Sorted {len(tasks)} tasks into {len(sorted_batches)} dependency levels")
        return sorted_batches

    def _wait_for_dependencies(self, tasks: List[Task]) -> None:
        """Wait for task dependencies to complete"""
        for task in tasks:
            if hasattr(task, 'depends_on') and task.depends_on:
                for dep in task.depends_on:
                    if isinstance(dep, Task):
                        try:
                            self.logger.debug(f"Task '{task.name}' waiting for dependency '{dep.name}'")
                            dep.result(timeout=300)  # 5 minute timeout
                        except Exception as e:
                            task.set_exception(Exception(f"Dependency '{dep.name}' failed: {str(e)}"))
                            raise

    def _route_tasks_to_providers(self, tasks: List[Task]) -> Dict[str, List[Task]]:
        """Route tasks to appropriate providers"""
        provider_tasks = defaultdict(list)

        for task in tasks:
            if not isinstance(task, Task):
                raise ValueError(f'Task must be of type {Task}')

            # Store original name
            original_name = getattr(task, 'name', None)

            # Ensure unique names
            if not hasattr(task, 'name') or not task.name:
                task.name = f'faas-{self._manager_id}-func-{uuid.uuid4().hex[:8]}'
            elif not task.name.startswith(f'faas-{self._manager_id}'):
                task.name = f'faas-{self._manager_id}-{task.name}'

            # Map names
            if original_name:
                with self._name_mapping_lock:
                    self._original_name_mapping[original_name] = task.name
                    self._deployed_name_mapping[task.name] = original_name
                    self._original_name_mapping[task.name] = task.name

            # Determine provider
            provider_name = getattr(task, 'provider', '').lower()

            # Route to appropriate provider
            if provider_name == LAMBDA and LAMBDA in self._providers:
                provider_tasks[LAMBDA].append(task)
            elif provider_name == NUCLIO:
                nuclio_provider = self._find_nuclio_provider(task)
                if nuclio_provider:
                    provider_tasks[nuclio_provider].append(task)
                else:
                    provider_tasks[list(self._providers.keys())[0]].append(task)
            else:
                # Default routing
                if hasattr(task, 'handler_code') or hasattr(task, 'source_path'):
                    # Code based deployment
                    if LAMBDA in self._providers:
                        provider_tasks[LAMBDA].append(task)
                    else:
                        provider_tasks[list(self._providers.keys())[0]].append(task)
                else:
                    # Container based deployment
                    nuclio_provider = self._find_first_nuclio_provider()
                    if nuclio_provider:
                        provider_tasks[nuclio_provider].append(task)
                    else:
                        provider_tasks[list(self._providers.keys())[0]].append(task)

        return provider_tasks

    def _find_nuclio_provider(self, task: Task) -> Optional[str]:
        """Find the appropriate Nuclio provider for a task"""
        task_cloud_provider = getattr(task, 'cloud_provider', None)

        for provider_name, provider_info in self._providers.items():
            if provider_name.startswith(NUCLIO):
                if task_cloud_provider:
                    if provider_info.get('vm_provider') == task_cloud_provider:
                        return provider_name
                else:
                    return provider_name

        return None

    def _find_first_nuclio_provider(self) -> Optional[str]:
        """Find the first available Nuclio provider"""
        for provider_name in self._providers.keys():
            if provider_name.startswith(NUCLIO):
                return provider_name
        return None

    def invoke(self, function_name: str, payload: Any = None,
               provider: str = None) -> Dict[str, Any]:
        """
        Invoke a deployed function with metrics tracking.

        Args:
            function_name: Name of the function to invoke
            payload: Payload to send to function
            provider: Specific provider to use (if None, tries all)

        Returns:
            Invocation response
        """
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        self.logger.debug(f"Invoking function: {function_name}")

        # Resolve the actual deployed name
        with self._name_mapping_lock:
            actual_function_name = self._original_name_mapping.get(function_name, function_name)
            self.logger.debug(f"Resolved function name: {function_name} -> {actual_function_name}")

        # Start timing for this invocation
        if self.metrics_collector:
            invocation_id, timer = self.metrics_collector.start_invocation(
                function_name=function_name,
                payload_size=len(str(payload)) if payload else 0
            )
        else:
            invocation_id = None
            timer = SimpleTimer()
            timer.start()

        last_error = None

        try:
            # Provider selection
            timer.mark("provider_selection")
            selected_provider = self._select_provider(provider, actual_function_name)

            if not selected_provider:
                raise FaasException(f'No suitable provider found for function {function_name}')

            self.logger.debug(f"Selected provider: {selected_provider}")

            # Pre-invoke overhead
            timer.mark("pre_invoke")

            # Get provider instance
            provider_info = self._providers[selected_provider]
            instance = provider_info['instance']

            # Mark platform API call start
            timer.mark("platform_api_call")

            # Actual invocation (provider handles platform timing)
            result = instance.invoke_function(actual_function_name, payload)

            # Mark post-invoke start
            timer.mark("post_invoke")

            # Ensure result is dict
            if not isinstance(result, dict):
                result = {'response': result}

            # Extract execution metrics from the Lambda provider response
            function_execution_ms = None
            cold_start = False
            memory_used_mb = None

            # Check if the result contains metrics (Lambda provider returns this)
            if '_metrics' in result:
                metrics = result.pop('_metrics')  # Remove from user-visible result
                function_execution_ms = metrics.get('function_execution_ms')
                cold_start = metrics.get('cold_start', False)
                memory_used_mb = metrics.get('memory_used_mb')

            # Complete invocation tracking at manager level
            if self.metrics_collector and invocation_id:
                self.metrics_collector.complete_invocation(
                    invocation_id=invocation_id,
                    status='success',
                    cold_start=cold_start,
                    function_execution_ms=function_execution_ms,
                    memory_used_mb=memory_used_mb
                )

            return result

        except Exception as e:
            last_error = e
            self.logger.error(f"Invocation failed: {e}")
            # Record failed invocation
            if self.metrics_collector and invocation_id:
                self.metrics_collector.complete_invocation(
                    invocation_id=invocation_id,
                    status='failed',
                    error_message=str(e)
                )
            raise

    def _select_provider(self, provider: Optional[str], function_name: str) -> Optional[str]:
        """Select appropriate provider for function invocation"""
        # Check cache first
        with self._cache_lock:
            cached_provider = self._function_provider_cache.get(function_name)
            if cached_provider and cached_provider in self._providers:
                if self._providers[cached_provider]['active']:
                    return cached_provider
                else:
                    del self._function_provider_cache[function_name]

        if provider:
            provider_lower = provider.lower()
            matching_providers = [p for p in self._providers.keys() if p.startswith(provider_lower)]

            for prov_name in matching_providers:
                if self._providers[prov_name]['active']:
                    with self._cache_lock:
                        self._function_provider_cache[function_name] = prov_name
                    return prov_name
            return None
        else:
            # Try all active providers
            for prov_name, prov_info in self._providers.items():
                if prov_info['active']:
                    try:
                        instance = prov_info['instance']
                        functions = instance.list_functions()
                        if any(f['name'] == function_name for f in functions):
                            with self._cache_lock:
                                self._function_provider_cache[function_name] = prov_name
                            return prov_name
                    except:
                        continue
            return None

    def list_functions(self, provider: str = None) -> Dict[str, List[Dict[str, Any]]]:
        """List functions across providers"""
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        results = {}

        # Determine which providers to query
        if provider:
            providers_to_check = [p for p in self._providers.keys()
                                 if p.startswith(provider.lower())]
        else:
            providers_to_check = list(self._providers.keys())

        # Query each provider
        for prov_name in providers_to_check:
            prov_info = self._providers[prov_name]
            if not prov_info['active']:
                results[prov_name] = []
                continue

            try:
                instance = prov_info['instance']
                functions = instance.list_functions()

                # Add original name mapping
                enhanced_functions = []
                for func in functions:
                    func_copy = func.copy()
                    with self._name_mapping_lock:
                        original_name = self._deployed_name_mapping.get(func['name'])
                        if original_name:
                            func_copy['original_name'] = original_name
                    enhanced_functions.append(func_copy)

                results[prov_name] = enhanced_functions
            except Exception as e:
                self.logger.error(f'Failed to list functions from {prov_name}: {e}')
                results[prov_name] = []

        return results

    def delete_function(self, function_name: str, provider: str = None) -> None:
        """Delete a function from provider(s)"""
        if not self._initialized:
            raise FaasException("Manager not started. Call start() first.")

        # Resolve the actual deployed name
        with self._name_mapping_lock:
            actual_function_name = self._original_name_mapping.get(function_name, function_name)
            original_name = self._deployed_name_mapping.get(actual_function_name, function_name)

        success_count = 0

        # Remove from cache
        with self._cache_lock:
            self._function_provider_cache.pop(actual_function_name, None)
            self._function_provider_cache.pop(function_name, None)

        # Determine which providers to check
        if provider:
            providers_to_check = [p for p in self._providers.keys()
                                 if p.startswith(provider.lower())]
        else:
            providers_to_check = list(self._providers.keys())

        # Try to delete from each provider
        for prov_name in providers_to_check:
            prov_info = self._providers[prov_name]
            if not prov_info['active']:
                continue

            try:
                instance = prov_info['instance']
                instance.delete_function(actual_function_name)
                success_count += 1
                self.logger.info(f'Deleted function {actual_function_name} from {prov_name}')
            except Exception as e:
                self.logger.debug(f'Function {actual_function_name} not found on {prov_name}: {e}')

        # Clean up name mappings
        if success_count > 0:
            with self._name_mapping_lock:
                self._original_name_mapping.pop(function_name, None)
                self._original_name_mapping.pop(actual_function_name, None)
                self._deployed_name_mapping.pop(actual_function_name, None)
                if original_name:
                    self._original_name_mapping.pop(original_name, None)

        if success_count == 0:
            raise FaasException(f'Function {function_name} not found on any provider')

    def get_status(self) -> Dict[str, Any]:
        """Get status of the FaaS manager and providers"""
        status = {
            'manager_id': self._manager_id,
            'initialized': self._initialized,
            'active_tasks': len(self._active_tasks),
            'total_tasks': self._task_count,
            'providers': {},
            'cached_functions': len(self._function_provider_cache),
            'name_mappings': len(self._original_name_mapping)
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

        # Add current metrics if available
        if self.metrics_collector:
            status['current_metrics'] = self.metrics_collector.get_current_metrics()

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

                # Add provider specific health info
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

    def get_task_status(self, task_name: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific task"""
        # Resolve the actual deployed name
        with self._name_mapping_lock:
            actual_task_name = self._original_name_mapping.get(task_name, task_name)

        for provider_name, provider_info in self._providers.items():
            instance = provider_info.get('instance')
            if instance and hasattr(instance, '_functions_book'):
                if actual_task_name in instance._functions_book:
                    task = instance._functions_book[actual_task_name]
                    return {
                        'name': actual_task_name,
                        'original_name': task_name,
                        'state': getattr(task, 'state', 'UNKNOWN'),
                        'done': task.done(),
                        'provider': provider_name
                    }
        return None

    def shutdown(self) -> None:
        """Shutdown FaaS Manager and clean up resources"""
        with self._shutdown_lock:
            if self._terminate.is_set():
                return

            self.logger.info("Shutting down FaaS Manager")
            self._terminate.set()

            # Stop accepting new work
            self._active = False
            for prov_info in self._providers.values():
                prov_info['active'] = False

            # Process remaining queue items with timeout
            remaining_start = time.time()
            while True:
                if time.time() - remaining_start > GRACEFUL_SHUTDOWN_TIMEOUT:
                    self.logger.warning("Graceful shutdown timeout reached")
                    break

                # Check if any queues have items
                has_items = False
                for prov_info in self._providers.values():
                    if prov_info['in_q'].qsize() > 0:
                        has_items = True
                        break

                if not has_items:
                    break

                self.logger.info("Processing remaining queue items...")
                time.sleep(0.5)

            # Force fail any remaining tasks
            self._force_fail_remaining_tasks()

            # Wait for active tasks
            start_time = time.time()
            while self._active_tasks and time.time() - start_time < GRACEFUL_SHUTDOWN_TIMEOUT:
                self.logger.info(f"Waiting for {len(self._active_tasks)} active tasks...")
                time.sleep(1)

            if self._active_tasks:
                self.logger.warning(f"{len(self._active_tasks)} tasks still active after timeout")

            # Shutdown thread pool
            self.executor.shutdown(wait=True)

            # Shutdown providers
            for name, info in self._providers.items():
                self._shutdown_provider(name, info)

            # Save resources
            try:
                self.resource_manager.save_all_resources()
                self.logger.info("Resources saved successfully")
            except Exception as e:
                self.logger.error(f"Failed to save resources: {e}")

            # Signal shutdown complete
            self._shutdown_complete.set()
            self.logger.info("FaaS Manager shutdown complete")

    def _force_fail_remaining_tasks(self):
        """Force fail any tasks still in queues"""
        self.logger.info("Force failing any remaining tasks")

        # Check all provider queues
        for provider_name, provider_info in self._providers.items():
            in_q = provider_info['in_q']

            # Drain the queue
            remaining_count = 0
            while True:
                try:
                    task = in_q.get_nowait()
                    remaining_count += 1

                    if hasattr(task, 'set_exception') and not task.done():
                        task.set_exception(Exception("Manager shutdown before task could be processed"))

                except queue.Empty:
                    break

            if remaining_count > 0:
                self.logger.warning(f"Force failed {remaining_count} tasks from {provider_name} queue")

        # Also check tasks in provider instances
        for provider_name, provider_info in self._providers.items():
            instance = provider_info.get('instance')
            if instance and hasattr(instance, '_functions_book'):
                for task_name, task in instance._functions_book.items():
                    if hasattr(task, 'set_exception') and not task.done():
                        task.set_exception(Exception("Manager shutdown before task completion"))
                        self.logger.debug(f"Force failed task {task_name}")

    def _shutdown_provider(self, name: str, info: Dict[str, Any]) -> None:
        """Shutdown a single provider"""
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