"""
FaaS provider manager
"""

import uuid
import queue
import threading
import time
from typing import Callable, List, Union
from hydraa import Task
from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.utils import misc
from aws_lambda import AwsLambda
from exceptions import FaasException

AWS = 'aws'
LOCAL = 'local'
AGENT = 'agent'

PROVIDER_TO_CLASS = {
    AWS: AwsLambda,
    # AGENT: AgentFaas,
}

TERM_SIGNALS = {
    0: "Auto-terminate was set, terminating.",
    1: "No more tasks to process, terminating.",
    2: "User termination requested, terminating.",
    3: "Internal failure detected, terminating."
}

TIMEOUT = 0.1
_id = str(uuid.uuid4())


class MetricsCollector:
    """Collects deployment and performance metrics"""

    def __init__(self):
        self.deployment_times = []
        self.success_rates = {}
        self.resource_usage = {}
        self._lock = threading.Lock()

    def record_deployment(self, task_name, duration, success, memory_used=None):
        """Record deployment metrics"""
        with self._lock:
            self.deployment_times.append(duration)

            if task_name not in self.success_rates:
                self.success_rates[task_name] = {'success': 0, 'total': 0}

            self.success_rates[task_name]['total'] += 1
            if success:
                self.success_rates[task_name]['success'] += 1

            if memory_used:
                self.resource_usage[task_name] = memory_used

    def get_performance_report(self):
        """Get performance metrics report"""
        with self._lock:
            if not self.deployment_times:
                return {'message': 'no deployment metrics available'}

            return {
                'avg_deployment_time': sum(self.deployment_times) / len(self.deployment_times),
                'total_deployments': len(self.deployment_times),
                'success_rates': {
                    name: data['success'] / data['total'] if data['total'] > 0 else 0
                    for name, data in self.success_rates.items()
                },
                'resource_usage': dict(self.resource_usage)
            }


class FaasManager:
    """
    FaaS Manager orchestrates faas providers

    This manager handles provider routing (lambda, agent, etc.), task lifecycle management,
    function invocation coordination, and resource cleanup.
    """

    def __init__(self, proxy_mgr: proxy, asynchronous: bool = True,
                 auto_terminate: bool = True) -> None:
        """
        Initialize FaaS Manager

        Args:
            proxy_mgr: Proxy manager for credential loading
            asynchronous: Run operations asynchronously
            auto_terminate: Auto-terminate resources on shutdown
        """
        self.sandbox = None
        self._proxy = proxy_mgr
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate

        # provider management
        self._registered_managers = {}

        # threading
        self._terminate = threading.Event()
        self.logger = None

        # metrics collection
        self.metrics = MetricsCollector()

    def start(self, sandbox: str):
        """
        Start the FaaS Manager and initialize providers

        Args:
            sandbox: Sandbox directory path
        """
        self.sandbox = sandbox
        self.logger = misc.logger(path=f'{self.sandbox}/faas_manager.log')

        providers = len(self._proxy.loaded_providers)
        print(f'starting faas manager with [{providers}] providers')

        # initialize providers
        for provider in self._proxy.loaded_providers:
            if provider in PROVIDER_TO_CLASS:
                cred = self._proxy._load_credentials(provider)
                faas_class = PROVIDER_TO_CLASS[provider]

                faas_instance = faas_class(
                    sandbox=self.sandbox,
                    manager_id=_id,
                    cred=cred,
                    asynchronous=self.asynchronous,
                    auto_terminate=self.auto_terminate,
                    log=self.logger
                )

                self._registered_managers[provider] = {
                    'class': faas_instance,
                    'run_id': faas_instance.run_id,
                    'in_q': faas_instance.incoming_q,
                    'out_q': faas_instance.outgoing_q
                }

                # set as attribute for easy access
                setattr(self, faas_class.__name__, faas_instance)

                # start result monitoring thread
                self._start_result_monitor(provider)

        print(f'faas manager started with providers: {list(self._registered_managers.keys())}')

    def _start_result_monitor(self, provider):
        """Start thread to monitor provider results"""
        manager_attrs = self._registered_managers[provider]

        result_thread = threading.Thread(
            target=self._get_results,
            name=f"{provider}-FaasManagerResult",
            args=(manager_attrs,)
        )
        result_thread.daemon = True
        result_thread.start()

    def _get_results(self, manager_attrs):
        """Monitor and process messages from provider output queue"""
        manager_queue = manager_attrs['out_q']
        manager_name = manager_attrs['class'].__class__.__name__

        while not self._terminate.is_set():
            try:
                msg = manager_queue.get(block=True, timeout=TIMEOUT)

                if msg:
                    # handle termination signals
                    if isinstance(msg, tuple):
                        term_sig, provider = msg
                        term_msg = TERM_SIGNALS.get(term_sig, f"unknown signal: {term_sig}")
                        print(term_msg)
                        self.shutdown(provider=provider)

                    # handle regular messages
                    elif isinstance(msg, str):
                        self.logger.info(f'{manager_name} reported: {msg}')

                    # handle unexpected messages
                    else:
                        self.logger.error(f'unexpected message type from {manager_name}: {type(msg)}')

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f'error in result monitor for {manager_name}: {str(e)}')

    def __call__(self, func: Callable = None, provider: str = '') -> Callable:
        """
        Decorator for automatic function deployment and invocation

        Args:
            func: Function to decorate
            provider: Target provider (aws, agent, etc.)

        Returns:
            Wrapped function that returns a task

        Example:
            @faas_manager(provider='aws')
            def my_function():
                task = Task()
                task.source_path = '/path/to/code'
                task.handler_name = 'main.handler'
                task.runtime = 'python3.9'
                task.memory = 256
                return task
        """
        if func is None:
            return lambda f: self.__call__(f, provider)

        def wrapper(*args, **kwargs):
            task = func(*args, **kwargs)

            if not isinstance(task, Task):
                raise ValueError(f'function must return object of type {Task}')

            if not task.provider:
                task.provider = provider

            self.submit(task)
            return task

        return wrapper

    def submit(self, tasks: Union[Task, List[Task]]):
        """
        Submit tasks for deployment to appropriate providers

        Args:
            tasks: Single task or list of tasks to deploy
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        if not self._registered_managers:
            raise FaasException('no faas providers registered')

        task_count = 0
        start_time = time.time()

        for task in tasks:
            # validate task
            if not isinstance(task, Task):
                raise ValueError(f'task must be of type {Task}')

            # determine target provider
            task_provider = getattr(task, 'provider', '').lower()

            if task_provider in self._registered_managers:
                manager = self._registered_managers[task_provider]
            else:
                # default to first available provider
                manager = next(iter(self._registered_managers.values()))
                self.logger.warning(f'no manager found for provider "{task_provider}", '
                                    f'using default provider')

            # submit to provider
            manager['in_q'].put(task)
            task_count += 1

        print(f'{task_count} function(s) submitted for deployment')

    def invoke(self, function_name: str, payload: dict = None,
               provider: str = None, invocation_type: str = 'RequestResponse'):
        """
        Invoke a deployed function

        Args:
            function_name: Name of the function to invoke
            payload: Payload to send to function
            provider: Target provider (if None, tries all providers)
            invocation_type: RequestResponse, Event, or DryRun

        Returns:
            dict: Invocation response

        Raises:
            FaasException: If invocation fails
        """
        if provider:
            # invoke on specific provider
            if provider not in self._registered_managers:
                raise FaasException(f'provider "{provider}" not registered')

            manager = self._registered_managers[provider]['class']
            return manager.invoke_function(function_name, payload, invocation_type)

        else:
            # try all providers until one succeeds
            last_error = None
            for provider_name, manager_attrs in self._registered_managers.items():
                try:
                    manager = manager_attrs['class']
                    return manager.invoke_function(function_name, payload, invocation_type)
                except Exception as e:
                    last_error = e
                    continue

            if last_error:
                raise FaasException(f'failed to invoke function on any provider: {str(last_error)}')
            else:
                raise FaasException(f'function "{function_name}" not found on any provider')

    def list_functions(self, provider: str = None):
        """
        List functions across providers

        Args:
            provider: Specific provider to list from (if None, lists all)

        Returns:
            dict: Functions by provider
        """
        all_functions = {}

        providers_to_check = [provider] if provider else self._registered_managers.keys()

        for provider_name in providers_to_check:
            if provider_name in self._registered_managers:
                try:
                    manager = self._registered_managers[provider_name]['class']
                    if hasattr(manager, 'list_functions'):
                        functions = manager.list_functions()
                        all_functions[provider_name] = functions
                    elif hasattr(manager, 'get_functions'):
                        functions = manager.get_functions()
                        all_functions[provider_name] = [{'FunctionName': f.name, 'FunctionArn': f.arn}
                                                        for f in functions]
                except Exception as e:
                    self.logger.error(f'Failed to list functions from {provider_name}: {str(e)}')
                    all_functions[provider_name] = {'error': str(e)}

        return all_functions

    def delete_function(self, function_name: str, provider: str = None):
        """
        Delete a function from provider(s)

        Args:
            function_name: Name of function to delete
            provider: Specific provider (if None, tries all)
        """
        success_count = 0

        providers_to_check = [provider] if provider else self._registered_managers.keys()

        for provider_name in providers_to_check:
            if provider_name in self._registered_managers:
                try:
                    manager = self._registered_managers[provider_name]['class']
                    if hasattr(manager, 'delete_function'):
                        manager.delete_function(function_name)
                        success_count += 1
                        self.logger.info(f'deleted function {function_name} from {provider_name}')
                except Exception as e:
                    self.logger.error(f'failed to delete function {function_name} from {provider_name}: {str(e)}')

        if success_count == 0:
            raise FaasException(f'failed to delete function "{function_name}" from any provider')

        print(f'Function "{function_name}" deleted from {success_count} provider(s)')

    def get_provider_status(self):
        """Get status of all registered providers"""
        status = {}
        for provider_name, manager_attrs in self._registered_managers.items():
            manager = manager_attrs['class']
            status[provider_name] = {
                'active': getattr(manager, 'status', False),
                'run_id': manager_attrs['run_id'],
                'functions_count': len(getattr(manager, '_functions_book', {}))
            }
        return status

    def get_performance_metrics(self):
        """Get deployment performance metrics"""
        return self.metrics.get_performance_report()

    def shutdown(self, provider: str = None):
        """
        Shutdown FaaS Manager and clean up resources

        Args:
            provider: Specific provider to shutdown (if None, shuts down all)
        """
        self._terminate.set()

        if provider:
            # shutdown specific provider
            if provider in self._registered_managers:
                print(f'shutting down faas provider: {provider}')
                manager = self._registered_managers[provider]['class']
                manager.shutdown()
                del self._registered_managers[provider]
            else:
                self.logger.warning(f'provider "{provider}" not found for shutdown')
        else:
            # shutdown all providers
            print('shutting down all faas providers')
            for provider_name, manager_attrs in list(self._registered_managers.items()):
                print(f'terminating provider: {provider_name}')
                try:
                    manager = manager_attrs['class']
                    manager.shutdown()
                except Exception as e:
                    self.logger.error(f'error shutting down {provider_name}: {str(e)}')

            self._registered_managers.clear()
            print('faas manager shutdown complete')