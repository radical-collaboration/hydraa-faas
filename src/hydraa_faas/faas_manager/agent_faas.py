#!/usr/bin/env python3
"""
Agent FaaS Provider
"""

import os
import time
import uuid
import queue
import asyncio
import threading
import tempfile
import zipfile
import httpx
import shutil
import base64
from typing import Dict, Any, List
from collections import OrderedDict
from hydraa import Task


class AgentFaas:
    """Agent FaaS provider for local and remote function execution."""

    def __init__(self, sandbox: str, manager_id: str, config: Dict[str, Any],
                 asynchronous: bool, auto_terminate: bool, logger):
        """Initialize Agent FaaS provider.

        Args:
            sandbox: Sandbox directory path
            manager_id: Unique manager identifier
            config: Agent configuration dictionary
            asynchronous: Whether to run asynchronously
            auto_terminate: Whether to auto-terminate when done
            logger: Logger instance
        """
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.logger = logger
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.config = config
        self.status = False

        # agent configuration
        self.agent_host = config.get('host', 'localhost')
        self.agent_port = config.get('port', 8000)
        self.agent_url = f"http://{self.agent_host}:{self.agent_port}"

        # task management
        self._task_id = 0
        self._tasks_book = OrderedDict()
        self._deployed_functions = {}

        # threading
        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self._task_lock = threading.Lock()
        self._terminate = threading.Event()

        # unique run identifier
        self.run_id = f"agent-faas.{str(uuid.uuid4())}"

        # create sandbox subdirectory
        self.agent_sandbox = f"{sandbox}/agent-faas.{self.run_id}"
        os.makedirs(self.agent_sandbox, exist_ok=True)

        # http client for agent communication
        self.http_client = None

        # start worker thread
        self._start_worker()

        self.status = True
        self.logger.info(f"Agent FaaS provider initialized: {self.run_id}")

    @property
    def is_active(self):
        """Check if provider is active."""
        return self.status

    def _start_worker(self):
        """Start worker thread to process tasks."""
        self.worker_thread = threading.Thread(
            target=self._run_worker,
            name='AgentFaasWorker',
            daemon=True
        )
        self.worker_thread.start()

        if not self.asynchronous:
            self.result_thread = threading.Thread(
                target=self._wait_tasks,
                name='AgentFaasWatcher',
                daemon=True
            )
            self.result_thread.start()

    def _run_worker(self):
        """Worker thread main loop."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(self._get_work())

    async def _get_work(self):
        """Process incoming tasks from queue."""
        self.http_client = httpx.AsyncClient(timeout=60.0)
        try:
            while not self._terminate.is_set():
                # in real app this loop would process from self.incoming_q
                await asyncio.sleep(0.1)
        finally:
            if self.http_client:
                await self.http_client.aclose()
                self.http_client = None

    async def _process_bulk(self, bulk):
        """Process bulk of tasks."""
        tasks = []
        for operation, task in bulk:
            if operation == 'deploy':
                tasks.append(self._deploy_function(task))
            elif operation == 'invoke':
                tasks.append(self._invoke_function(task.name, task.payload or {}))
            elif operation == 'delete':
                tasks.append(self._delete_function(task))
            elif operation == 'list':
                tasks.append(self._list_functions())
            else:
                self.logger.warning(f'Unknown operation: {operation}')

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _deploy_function(self, task: Task) -> Dict[str, Any]:
        """Deploy function to agent."""
        try:
            # create function package
            package_data = await self._create_function_package(task)

            # deploy to agent
            deploy_payload = {
                'function_name': task.name,
                'handler': getattr(task, 'handler', 'main:handler'),
                'runtime': getattr(task, 'runtime', 'python3.9'),
                'memory': getattr(task, 'memory', 512),
                'timeout': getattr(task, 'timeout', 300),
                'environment_vars': getattr(task, 'environment_vars', {}),
                'package_data': package_data
            }

            response = await self.http_client.post(
                f"{self.agent_url}/deploy",
                json=deploy_payload
            )
            response.raise_for_status()

            result = response.json()

            # store deployment info
            self._deployed_functions[task.name] = {
                'task': task,
                'created_at': time.time(),
                'result': result
            }

            if hasattr(task, 'set_result'):
                task.set_result(result)

            self.outgoing_q.put(f'Function {task.name} deployed successfully')
            return result

        except Exception as e:
            error_msg = f'Failed to deploy function {task.name}: {e}'
            self.logger.error(error_msg)
            if hasattr(task, 'set_exception'):
                task.set_exception(Exception(error_msg))
            raise

    async def _create_function_package(self, task: Task) -> str:
        """Create function package for deployment."""
        package_dir = tempfile.mkdtemp(prefix=f"agent-{task.name}-")
        zip_path = f"{package_dir}.zip"

        try:
            # add source code based on task configuration
            if hasattr(task, 'source_directory') and task.source_directory:
                self.logger.info(f"Adding source directory: {task.source_directory}")
                self._copy_directory(task.source_directory, package_dir)

            elif hasattr(task, 'source_files') and task.source_files:
                self.logger.info(f"Adding source files: {list(task.source_files.keys())}")
                for source_path, target_path in task.source_files.items():
                    target_full_path = os.path.join(package_dir, target_path)
                    os.makedirs(os.path.dirname(target_full_path), exist_ok=True)
                    if os.path.isfile(source_path):
                        shutil.copy2(source_path, target_full_path)
                    else:
                        self._copy_directory(source_path, target_full_path)

            elif hasattr(task, 'module_code') and task.module_code:
                self.logger.info("Adding module code")
                for module_name, code_content in task.module_code.items():
                    # convert module name to file path
                    if '.' in module_name:
                        parts = module_name.split('.')
                        file_path = os.path.join(*parts[:-1], parts[-1] + '.py')
                        # create __init__.py files for packages
                        for i in range(len(parts) - 1):
                            init_dir = os.path.join(package_dir, *parts[:i + 1])
                            os.makedirs(init_dir, exist_ok=True)
                            init_file = os.path.join(init_dir, '__init__.py')
                            if not os.path.exists(init_file):
                                with open(init_file, 'w', encoding='utf-8') as f:
                                    f.write('')
                    else:
                        file_path = module_name + '.py'

                    full_file_path = os.path.join(package_dir, file_path)
                    os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
                    with open(full_file_path, 'w', encoding='utf-8') as f:
                        f.write(code_content)

            elif hasattr(task, 'handler_code') and task.handler_code:
                # single handler code
                main_file = os.path.join(package_dir, 'main.py')
                with open(main_file, 'w', encoding='utf-8') as f:
                    f.write(task.handler_code)

            else:
                # default handler
                main_file = os.path.join(package_dir, 'main.py')
                with open(main_file, 'w', encoding='utf-8') as f:
                    f.write(self._get_default_handler_code())

            # create requirements.txt if specified
            if hasattr(task, 'requirements') and task.requirements:
                req_file = os.path.join(package_dir, 'requirements.txt')
                with open(req_file, 'w', encoding='utf-8') as f:
                    f.write(task.requirements)

            # create zip package
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, _, files in os.walk(package_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, package_dir)
                        zipf.write(file_path, arcname)

            # read zip file as base64
            with open(zip_path, 'rb') as f:
                package_data = base64.b64encode(f.read()).decode('utf-8')

            return package_data

        finally:
            # clean up temp directory and zip file
            shutil.rmtree(package_dir, ignore_errors=True)
            if os.path.exists(zip_path):
                os.unlink(zip_path)

    def _copy_directory(self, src: str, dst: str):
        """Copy directory with exclusions."""
        exclude_patterns = {
            '*.pyc', '__pycache__', '.git', '.pytest_cache',
            '*.egg-info', '.coverage', '.tox', 'venv', '.venv'
        }

        def ignore_patterns(_, files):
            import fnmatch
            ignored = set()
            for pattern in exclude_patterns:
                ignored.update(fnmatch.filter(files, pattern))
            return ignored

        shutil.copytree(src, dst, ignore=ignore_patterns, dirs_exist_ok=True)

    def _get_default_handler_code(self) -> str:
        """Get default handler code."""
        return '''
        import json
        
        def handler(event, context=None):
            """Default handler function."""
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Hello from Agent FaaS Provider',
                    'event': event
                })
            }
        '''

    async def _invoke_function(self, func_name: str, payload: Any) -> Any:
        """Invoke function on agent."""
        try:
            invoke_payload = {'payload': payload}

            response = await self.http_client.post(
                f"{self.agent_url}/invoke/{func_name}",
                json=invoke_payload
            )
            response.raise_for_status()

            result = response.json()

            self.logger.info(f"Function {func_name} invoked successfully")
            self.outgoing_q.put(f'Function {func_name} invoked successfully')
            return result

        except Exception as e:
            self.logger.error(f"Error invoking function {func_name}: {e}")
            self.outgoing_q.put(f'Function {func_name} invocation failed: {str(e)}')
            raise

    async def _delete_function(self, task: Task):
        """Delete function from agent."""
        try:
            func_name = task.name

            response = await self.http_client.delete(
                f"{self.agent_url}/function/{func_name}"
            )
            response.raise_for_status()

            if func_name in self._deployed_functions:
                del self._deployed_functions[func_name]

            self.logger.info(f"Deleted function: {func_name}")
            self.outgoing_q.put(f'Function {func_name} deleted successfully')

            if hasattr(task, 'set_result'):
                task.set_result({
                    'status': 'deleted',
                    'function_name': func_name,
                    'platform': 'agent-faas'
                })

        except Exception as e:
            error_msg = f"Error deleting function {task.name}: {e}"
            self.logger.error(error_msg)
            if hasattr(task, 'set_exception'):
                task.set_exception(Exception(error_msg))
            raise

    async def _list_functions(self) -> List[Dict[str, Any]]:
        """List deployed functions on agent."""
        try:
            response = await self.http_client.get(f"{self.agent_url}/functions")
            response.raise_for_status()

            result = response.json()
            functions = result.get('functions', [])

            # filter functions deployed by this manager
            filtered_functions = []
            for func in functions:
                if func.get('manager_id') == self.manager_id:
                    filtered_functions.append({
                        'id': func['name'],
                        'name': func['name'],
                        'handler': func.get('handler', 'unknown'),
                        'runtime': func.get('runtime', 'unknown'),
                        'status': func.get('status', 'unknown'),
                        'memory': func.get('memory', 0),
                        'timeout': func.get('timeout', 0),
                        'platform': 'agent-faas'
                    })

            self.logger.info(f"Listed {len(filtered_functions)} agent functions")
            self.outgoing_q.put(f'Found {len(filtered_functions)} functions')
            return filtered_functions

        except Exception as e:
            self.logger.error(f"Error listing agent functions: {e}")
            return []

    def _wait_tasks(self):
        """Monitor task completion for synchronous mode."""
        finished = []
        failed, done = 0, 0

        while not self._terminate.is_set():
            try:
                # check for completed tasks
                for task_name, task_info in self._deployed_functions.items():
                    if task_name in finished:
                        continue

                    task = task_info['task']
                    if hasattr(task, 'done') and task.done():
                        if hasattr(task, 'exception') and task.exception():
                            failed += 1
                            self.logger.error(f"Task {task_name} failed: {task.exception()}")
                        else:
                            done += 1
                            self.logger.info(f"Task {task_name} completed")

                        finished.append(task_name)

                # check if all tasks completed and auto terminate
                if (len(finished) == len(self._deployed_functions) and
                        self._deployed_functions and self.auto_terminate):
                    termination_msg = (0, 'agent')
                    self.outgoing_q.put(termination_msg)
                    break

                time.sleep(0.5)

            except Exception as e:
                self.logger.error(f"Error in task monitoring: {e}")
                time.sleep(1)

    async def health_check(self) -> bool:
        """Check if agent is healthy and responsive."""
        try:
            if not self.http_client:
                self.http_client = httpx.AsyncClient(timeout=10.0)

            response = await self.http_client.get(f"{self.agent_url}/health")
            response.raise_for_status()

            result = response.json()
            return result.get('status') == 'healthy'

        except Exception as e:
            self.logger.warning(f"Agent health check failed: {e}")
            return False

    def get_tasks(self) -> List:
        """Get all managed tasks."""
        return [info['task'] for info in self._deployed_functions.values()]

    async def _cleanup_functions(self):
        """Asynchronous cleanup of deployed functions using the instance-level http_client."""
        if self.http_client is None:
            self.logger.warning("HTTP client not available for cleanup, creating temporary one.")
            async with httpx.AsyncClient(timeout=30.0) as client:
                cleanup_tasks = [client.delete(f"{self.agent_url}/function/{func_name}") for func_name in
                                 self._deployed_functions]
                await asyncio.gather(*cleanup_tasks, return_exceptions=True)
            return

        self.logger.info(f"Cleaning up {len(self._deployed_functions)} functions using shared client...")
        cleanup_tasks = []
        for func_name in list(self._deployed_functions.keys()):
            self.logger.info(f"Scheduling cleanup for function: {func_name}")
            cleanup_tasks.append(
                self.http_client.delete(f"{self.agent_url}/function/{func_name}")
            )

        await asyncio.gather(*cleanup_tasks, return_exceptions=True)

    async def shutdown(self):
        """Shutdown provider and cleanup resources."""
        if not self.status:
            return

        self.logger.info("Shutting down Agent FaaS provider")
        self._terminate.set()

        if self.auto_terminate and self._deployed_functions:
            await self._cleanup_functions()

        self.status = False
        self.logger.info("Agent FaaS provider shutdown complete")