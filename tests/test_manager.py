"""Unit tests for FaasManager."""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import queue
import uuid
import os
import tempfile
import shutil
import threading

from hydraa import Task, proxy
from hydraa_faas.faas_manager.manager import FaasManager


class TestFaasManager(unittest.TestCase):
    """Test cases for FaasManager."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for tests
        self.test_dir = tempfile.mkdtemp()

        # Mock proxy manager
        self.mock_proxy = Mock(spec=proxy)
        self.mock_proxy.loaded_providers = ['aws']
        self.mock_proxy._load_credentials = Mock(return_value={
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'region_name': 'us-east-1'
        })

        # Create test VMs
        self.test_vms = []

        # Test manager with mocked dependencies
        self.manager = FaasManager(
            proxy_mgr=self.mock_proxy,
            vms=self.test_vms,
            asynchronous=True,
            auto_terminate=True
        )

        # Mock logger and profiler to avoid initialization issues
        self.manager.logger = Mock()
        self.manager.logger.trace = Mock()
        self.manager.logger.error = Mock()
        self.manager.logger.warning = Mock()

        self.manager.profiler = Mock()
        self.manager.profiler.prof = Mock()

        # Initialize threading event properly
        self.manager._terminate = threading.Event()

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_initialization(self):
        """Test FaasManager initialization."""
        self.assertTrue(self.manager.asynchronous)
        self.assertTrue(self.manager.auto_terminate)
        self.assertIsInstance(self.manager.incoming_q, queue.Queue)
        self.assertIsInstance(self.manager.outgoing_q, queue.Queue)
        self.assertFalse(self.manager.status)
        self.assertEqual(self.manager._provider_factories, {})

    def test_start(self):
        """Test starting the FaaS manager."""
        sandbox = os.path.join(self.test_dir, 'test_sandbox')

        # Mock the logger creation to avoid file system issues
        with patch('hydraa.services.caas_manager.utils.misc.logger') as mock_logger_func:
            mock_logger_func.return_value = Mock()

            # Mock the profiler
            with patch('radical.utils.Profiler') as mock_profiler_class:
                mock_profiler_class.return_value = Mock()

                # Mock threading for worker thread
                with patch('threading.Thread') as mock_thread:
                    mock_thread_instance = Mock()
                    mock_thread.return_value = mock_thread_instance

                    with patch.object(self.manager, '_initialize_provider_factories'):
                        self.manager.start(sandbox)

        self.assertTrue(self.manager.status)
        self.assertIsNotNone(self.manager.logger)
        self.assertIsNotNone(self.manager.profiler)
        self.assertTrue(os.path.exists(self.manager.sandbox))

    def test_extract_faas_config(self):
        """Test extracting FaaS configuration from task."""
        task = Task(
            name='test-task',
            vcpus=1,
            memory=512,
            image='python:3.9',
            env_var=[
                'FAAS_HANDLER=handler.main',
                'FAAS_RUNTIME=python3.9',
                'FAAS_TIMEOUT=30',
                'USER_VAR=value'
            ]
        )

        config = self.manager._extract_faas_config(task)

        self.assertEqual(config['handler'], 'handler.main')
        self.assertEqual(config['runtime'], 'python3.9')
        self.assertEqual(config['timeout'], '30')
        self.assertIn('USER_VAR=value', task._user_env_vars)

    def test_get_task_provider(self):
        """Test determining task provider."""
        # Mock provider factories
        self.manager._provider_factories = {'lambda': Mock(), 'nuclio': Mock()}
        self.manager._provider_lock = threading.RLock()

        # Test explicit provider
        task = Task(
            name='test-task',
            vcpus=1,
            memory=512,
            image='python:3.9',
            env_var=['FAAS_PROVIDER=lambda']
        )
        provider = self.manager._get_task_provider(task)
        self.assertEqual(provider, 'lambda')

        # Test auto-detect from ECR image
        task2 = Task(
            name='test-task2',
            vcpus=1,
            memory=512,
            image='123456789.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest'
        )
        provider2 = self.manager._get_task_provider(task2)
        self.assertEqual(provider2, 'lambda')

    def test_submit_single_task(self):
        """Test submitting a single task."""
        # Ensure manager is started
        self.manager.status = True

        task = Task(
            name='test-task',
            vcpus=1,
            memory=512,
            image='python:3.9',
            cmd=['python', '-c', 'print("hello")']
        )

        # Submit task
        self.manager.submit(task)

        # Verify task was queued
        self.assertFalse(self.manager.incoming_q.empty())
        queued_task = self.manager.incoming_q.get()
        self.assertEqual(queued_task, task)

    def test_submit_multiple_tasks(self):
        """Test submitting multiple tasks."""
        # Ensure manager is started
        self.manager.status = True

        tasks = [
            Task(name=f'task-{i}', vcpus=1, memory=512, image='python:3.9',
                 cmd=['python', '-c', f'print({i})'])
            for i in range(3)
        ]

        self.manager.submit(tasks)

        # Verify all tasks were queued
        for i in range(3):
            self.assertFalse(self.manager.incoming_q.empty())
            task = self.manager.incoming_q.get()
            self.assertEqual(task.name, f'task-{i}')

    @patch('threading.Thread')
    def test_worker_thread_creation(self, mock_thread):
        """Test worker thread is created in async mode."""
        sandbox = os.path.join(self.test_dir, 'test_sandbox')

        # Mock dependencies
        with patch('hydraa.services.caas_manager.utils.misc.logger'):
            with patch('radical.utils.Profiler'):
                with patch.object(self.manager, '_initialize_provider_factories'):
                    self.manager.asynchronous = True
                    self.manager.start(sandbox)

        # Verify worker thread was created
        mock_thread.assert_called()

    def test_invoke_function(self):
        """Test invoking a deployed function."""
        # Ensure manager is properly initialized
        self.manager.status = True

        # Mock provider
        mock_provider = Mock()
        mock_provider.invoke_function.return_value = {'status': 'success', 'result': 42}

        self.manager._providers = {
            'lambda': {'instance': mock_provider, 'type': 'lambda'}
        }
        self.manager._function_to_provider = {'my-function': 'lambda'}
        self.manager._functions_lock = threading.RLock()
        self.manager._provider_lock = threading.RLock()

        # Invoke function
        result = self.manager.invoke('my-function', {'input': 'data'})

        self.assertEqual(result, {'status': 'success', 'result': 42})
        mock_provider.invoke_function.assert_called_once_with('my-function', {'input': 'data'})

    def test_shutdown(self):
        """Test manager shutdown."""
        self.manager.status = True

        # Mock provider
        mock_provider = Mock()
        self.manager._providers = {
            'lambda': {'instance': mock_provider, 'type': 'lambda'}
        }

        # Mock thread
        self.manager._worker_thread = Mock()
        self.manager._worker_thread.is_alive.return_value = True

        # Initialize required attributes
        # Mock the executor to capture the shutdown call
        mock_future = Mock()
        mock_future.result.return_value = None

        self.manager.executor = Mock()
        self.manager.executor.submit.return_value = mock_future
        self.manager.executor.shutdown.return_value = None

        self.manager._provider_lock = threading.RLock()

        # Shutdown
        self.manager.shutdown()

        # Verify shutdown was called
        self.assertTrue(self.manager._terminate.is_set())
        self.manager._worker_thread.join.assert_called_once()

        # Verify provider shutdown was submitted to executor
        self.manager.executor.submit.assert_called()
        # Get the function that was submitted and verify it's the provider's shutdown
        submitted_func = self.manager.executor.submit.call_args[0][0]
        self.assertEqual(submitted_func, mock_provider.shutdown)

        # Verify executor was shutdown
        self.manager.executor.shutdown.assert_called_once_with(wait=True)
        self.assertFalse(self.manager.status)

    def test_decorator_usage(self):
        """Test using manager as decorator."""
        # Ensure manager is properly initialized
        self.manager.status = True

        @self.manager(provider='lambda')
        def create_task():
            return Task(
                name='decorated-task',
                vcpus=1,
                memory=512,
                image='python:3.9',
                cmd=['python', '-c', 'print("decorated")']
            )

        # Call decorated function
        task = create_task()

        # Verify task was queued
        self.assertFalse(self.manager.incoming_q.empty())
        queued_task = self.manager.incoming_q.get()
        self.assertEqual(queued_task.name, 'decorated-task')
        self.assertIn('FAAS_PROVIDER=lambda', queued_task.env_var)


if __name__ == '__main__':
    unittest.main()