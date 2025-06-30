"""Manager for FaaS providers.
TODO: make docstrings more informative
TODO: look into async io
TODO: create example and unit test
TODO: more exception handling
"""

import os
import uuid
import queue
import asyncio
import threading as mt
import radical.utils as ru
from typing import Callable, List, Union, Dict, Any
from collections import OrderedDict
from hydraa import Task
from hydraa.providers.proxy import proxy
from hydraa.services.caas_manager.utils import misc
from .aws_lambda import AwsLambda
from .agent_faas import AgentFaas

# provider constants
AWS = 'aws'
AGENT = 'agent'
LOCAL = 'local'

PROVIDER_TO_CLASS = {
    AWS: AwsLambda,
    AGENT: AgentFaas,
    LOCAL: AgentFaas,  # local uses agent
}

TERM_SIGNALS = {
    0: "Auto-terminate was set, terminating.",
    1: "No more tasks to process, terminating.",
    2: "User termination requested, terminating.",
    3: "Internal failure detected, terminating."
}

TIMEOUT = 0.1
_id = str(uuid.uuid4())


class FaasManager:
    """Main manager for orchestrating different FaaS providers.

    Handles multiple FaaS providers (aws lambda, local/remote agent) and provides
    a unified interface for deploying and invoking functions.

    Args:
        proxy_mgr: An instance of the proxy manager to load credentials and providers
        providers: List of FaaS provider names to initialize
        asynchronous: Whether to run operations asynchronously
        auto_terminate: Whether to auto-terminate when no more tasks
        agent_config: Configuration for local FaaS agent
    """

    def __init__(self, proxy_mgr: proxy, providers: List[str],
                 asynchronous: bool = True, auto_terminate: bool = True,
                 agent_config: dict = None):
        """Initialize the FaaS manager.

        Args:
            proxy_mgr: An instance of the proxy manager to load credentials.
            providers: List of provider names ('aws', 'agent', 'local').
            asynchronous: Whether to run asynchronously.
            auto_terminate: Whether to auto-terminate.
            agent_config: Configuration for agent based providers.
        """
        self.providers = providers
        self.sandbox = None
        self._proxy = proxy_mgr
        self._terminate = mt.Event()
        self._registered_managers = {}
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.agent_config = agent_config or {}

        # task management
        self._task_id = 0
        self._tasks_book = OrderedDict()
        self._task_lock = mt.Lock()

        # function registry
        self._functions = {}

    def start(self, sandbox):
        """Start the FaaS Manager and initialize all providers.

        Args:
            sandbox: Path to the sandbox directory.
        """
        self.sandbox = sandbox
        self.logger = misc.logger(path=f'{self.sandbox}/faas_manager.log')

        providers_count = len(self.providers)
        print(f'faas session sandbox created: {self.sandbox} with [{providers_count}] providers')

        # initialize each provider
        for provider in self.providers:
            if provider in PROVIDER_TO_CLASS:
                self._initialize_provider(provider)
            else:
                self.logger.warning(f'unknown provider: {provider}')

        # start result processing threads
        for provider_name, manager_attrs in self._registered_managers.items():
            self._start_result_thread(provider_name, manager_attrs)

        self.logger.info('faas manager started successfully')

    def _initialize_provider(self, provider: str):
        """Initialize a specific FaaS provider.

        Args:
            provider: Name of the provider to initialize.
        """
        try:
            if provider == AGENT or provider == LOCAL: # for readability basically
                # local agent doesn't need credentials from proxy
                faas_instance = PROVIDER_TO_CLASS[provider](
                    self.sandbox, _id, self.agent_config,
                    self.asynchronous, self.auto_terminate,
                    self.logger
                )
            else:
                # cloud providers need credentials and profiler
                if provider in self._proxy.loaded_providers:
                    cred = self._proxy._load_credentials(provider)
                    faas_class = PROVIDER_TO_CLASS[provider]

                    faas_instance = faas_class(
                        self.sandbox, _id, cred,
                        self.asynchronous, self.auto_terminate,
                        self.logger, ru.Profiler
                    )
                else:
                    self.logger.error(f'provider {provider} not loaded in proxy')
                    return

            self._registered_managers[provider] = {
                'class': faas_instance,
                'run_id': faas_instance.run_id,
                'in_q': faas_instance.incoming_q,
                'out_q': faas_instance.outgoing_q
            }

            # set as attribute
            setattr(self, f'{provider.title()}faas', faas_instance)

            self.logger.info(f'provider {provider} initialized successfully')

        except Exception as e:
            self.logger.error(f'failed to initialize provider {provider}: {e}')
            raise

    def _start_result_thread(self, provider_name: str, manager_attrs: dict):
        """Start result processing thread for a provider.

        Args:
            provider_name: Name of the provider.
            manager_attrs: Dictionary containing manager attributes.
        """
        result_thread = mt.Thread(
                    target=self._get_results,
                    name=f"{provider_name}-faasManagerResult",
                    args=(manager_attrs,)
                )
        result_thread.daemon = True
        result_thread.start()

    def _get_results(self, manager_attrs):
        """Process results from a provider manager.

        Args:
            manager_attrs: Dictionary containing manager attributes and queues.
        """
        manager_queue = manager_attrs['out_q']
        manager_name = manager_attrs['class'].__class__.__name__

        while not self._terminate.is_set():
            try:
                msg = manager_queue.get(block=True, timeout=TIMEOUT)
                if msg:
                    if isinstance(msg, tuple):
                        # termination signal
                        term_sig, prov = msg
                        term_msg = TERM_SIGNALS.get(term_sig, "unknown termination signal")
                        print(term_msg)
                        self.shutdown(provider=prov)
                    elif isinstance(msg, str):
                        # log message
                        self.logger.info(f'{manager_name} reported: {msg}')
                    else:
                        self.logger.warning(f'unexpected message type: {type(msg)}')

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f'error processing results: {e}')

    def __call__(self, func: Callable = None, provider: str = '') -> Callable:
        """Decorator for automatic function deployment and invocation.

        Args:
            func: Function to decorate.
            provider: Target FaaS provider.

        Returns:
            Decorated function.
        """
        if func is None:
            return lambda f: self.__call__(f, provider)

        def wrapper(*args, **kwargs):
            # deploy function if not already deployed
            func_name = f"{func.__name__}_{uuid.uuid4().hex[:8]}"

            if func_name not in self._functions:
                # create deployment task
                task = Task()
                task.name = func_name
                task.provider = provider or list(self._registered_managers.keys())[0]
                task.handler_function = func
                task.payload = {'args': args, 'kwargs': kwargs}

                self.deploy_function(task)

            # invoke function
            return self.invoke_function(func_name, {'args': args, 'kwargs': kwargs})

        return wrapper

    def deploy_function(self, task: Task):
        """Deploy a function to the specified provider.

        Args:
            task: Task containing function deployment information.
        """
        # validate task has required attributes for function deployment
        if not hasattr(task, 'name'):
            with self._task_lock:
                task.name = f'func-{self._task_id}'
                self._task_id += 1

        # set default attributes if not provided (no lambda task)
        if not hasattr(task, 'handler'):
            task.handler = 'main:handler'
        if not hasattr(task, 'runtime'):
            task.runtime = 'python3.9'
        if not hasattr(task, 'memory'):
            task.memory = 512
        if not hasattr(task, 'timeout'):
            task.timeout = 300

        task_provider = task.provider.lower() if hasattr(task, 'provider') and task.provider else 'agent'

        if task_provider in self._registered_managers:
            manager = self._registered_managers[task_provider]
        else:
            # default to first available manager
            if self._registered_managers:
                manager = next(iter(self._registered_managers.values()))
                self.logger.warning(f'provider {task_provider} not found, using default')
            else:
                raise RuntimeError('no faas managers available')

        # store function reference
        self._functions[task.name] = {
            'provider': task_provider,
            'deployed': False,
            'task': task
        }

        # submit deployment task
        manager['in_q'].put(('deploy', task))

        self.logger.info(f'function {task.name} submitted for deployment')

    def invoke_function(self, func_name: str, payload: dict):
        """Invoke a deployed function.

        Args:
            func_name: Name of the function to invoke.
            payload: Payload to send to the function.

        Returns:
            Function execution result.
        """
        if func_name not in self._functions:
            raise ValueError(f'function {func_name} not found')

        func_info = self._functions[func_name]
        provider = func_info['provider']

        if provider in self._registered_managers:
            manager = self._registered_managers[provider]

            # create invocation task
            task = Task()
            task.name = func_name
            task.payload = payload

            # submit invocation
            manager['in_q'].put(('invoke', task))

            self.logger.info(f'function {func_name} invoked')
            return task  # return future like object
        else:
            raise ValueError(f'provider {provider} not available')

    def list_functions(self, provider: str = None):
        """List deployed functions.

        Args:
            provider: Filter by provider.

        Returns:
            Dictionary of functions by provider.
        """
        if provider:
            if provider in self._registered_managers:
                manager = self._registered_managers[provider]
                manager['in_q'].put(('list', None))
            else:
                raise ValueError(f'provider {provider} not found')
        else:
            # list all functions from local registry
            result = {}
            for func_name, func_info in self._functions.items():
                prov = func_info['provider']
                if prov not in result:
                    result[prov] = []
                result[prov].append({
                    'name': func_name,
                    'deployed': func_info['deployed'],
                    'provider': prov
                })
            return result

    def delete_function(self, func_name: str):
        """Delete a deployed function.

        Args:
            func_name: Name of function to delete.
        """
        if func_name not in self._functions:
            raise ValueError(f'function {func_name} not found')

        func_info = self._functions[func_name]
        provider = func_info['provider']

        if provider in self._registered_managers:
            manager = self._registered_managers[provider]

            # create deletion task
            task = Task()
            task.name = func_name

            manager['in_q'].put(('delete', task))

            # remove from local registry
            del self._functions[func_name]

            self.logger.info(f'function {func_name} deleted')
        else:
            raise ValueError(f'provider {provider} not available')

    def submit(self, tasks: Union[Task, List[Task]]):
        """Submit tasks for function deployment/invocation.

        Args:
            tasks: Task or list of tasks to submit.
        """
        if not isinstance(tasks, list):
            tasks = [tasks]

        for task in tasks:
            if (hasattr(task, 'handler_function') or hasattr(task, 'handler_code') or
                hasattr(task, 'source_directory') or hasattr(task, 'module_code')):
                # this is a deployment task
                self.deploy_function(task)
            else:
                # this is an invocation task
                self.invoke_function(task.name, task.payload or {})

        print(f'{len(tasks)} task(s) submitted to FaaS Manager')

    async def health_check(self, provider: str = None) -> Dict[str, bool]:
        """Check health of FaaS providers.

        Args:
            provider: Specific provider to check, or None for all.

        Returns:
            Dictionary of provider health statuses.
        """
        health_results = {}

        providers_to_check = [provider] if provider else self._registered_managers.keys()

        for prov in providers_to_check:
            if prov in self._registered_managers:
                manager_instance = self._registered_managers[prov]['class']
                if hasattr(manager_instance, 'health_check'):
                    try:
                        health_results[prov] = await manager_instance.health_check()
                    except Exception as e:
                        self.logger.error(f'health check failed for {prov}: {e}')
                        health_results[prov] = False
                else:
                    health_results[prov] = manager_instance.is_active
            else:
                health_results[prov] = False

        return health_results

    def get_metrics(self) -> Dict[str, Any]:
        """Get metrics about deployed functions and usage.

        Returns:
            Dictionary containing various metrics.
        """
        metrics = {
            'total_functions': len(self._functions),
            'functions_by_provider': {},
            'active_providers': len(self._registered_managers),
            'providers': list(self._registered_managers.keys())
        }

        # Count functions by provider
        for func_info in self._functions.values():
            provider = func_info['provider']
            if provider not in metrics['functions_by_provider']:
                metrics['functions_by_provider'][provider] = 0
            metrics['functions_by_provider'][provider] += 1

        return metrics

    def shutdown(self, provider: str = None):
        """Shutdown the FaaS Manager.

        Args:
            provider: Specific provider to shutdown.
        """
        self._terminate.set()

        if provider:
            if provider in self._registered_managers:
                print(f'terminating faas provider {provider}')
                self._registered_managers[provider]['class'].shutdown()
        else:
            print('shutting down all faas providers')
            for provider_name, manager_attrs in self._registered_managers.items():
                print(f'terminating faas provider {provider_name}')
                try:
                    manager_attrs['class'].shutdown()
                except Exception as e:
                    self.logger.error(f'error shutting down {provider_name}: {e}')

        self.logger.info('faas manager shutdown complete')