"""Unit tests for NuclioProvider (custom_faas)."""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import os
import tempfile
import shutil
import threading
import asyncio

from hydraa import Task
from hydraa_faas.faas_manager.custom_faas import NuclioProvider, DeploymentException, NuclioException


class TestNuclioProvider(unittest.TestCase):
    """Test cases for NuclioProvider."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory
        self.test_dir = tempfile.mkdtemp()

        # Mock VMs
        self.mock_vm = Mock()
        self.mock_vm.Provider = 'local'
        self.vms = [self.mock_vm]

        # Mock credentials
        self.cred = {'username': 'test', 'password': 'test'}

        # Mock logger and profiler
        self.mock_logger = Mock()
        self.mock_logger.trace = Mock()
        self.mock_logger.error = Mock()
        self.mock_logger.warning = Mock()

        self.mock_profiler = Mock()
        self.mock_profiler.prof = Mock()

        # Create provider with mocked dependencies
        with patch('hydraa_faas.faas_manager.custom_faas.kubernetes.K8sCluster'):
            with patch.object(NuclioProvider, '_setup_nuclio'):
                self.provider = NuclioProvider(
                    sandbox=os.path.join(self.test_dir, 'nuclio_sandbox'),
                    manager_id='test-456',
                    vms=self.vms,
                    cred=self.cred,
                    asynchronous=False,
                    auto_terminate=True,
                    log=self.mock_logger,
                    resource_config={},
                    profiler=self.mock_profiler
                )

        # Mock Kubernetes cluster
        self.provider.k8s_cluster = Mock()
        self.provider.k8s_cluster.status = 'RUNNING'

        # Initialize threading components
        self.provider._terminate = threading.Event()
        self.provider._functions_lock = threading.RLock()

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_initialization(self):
        """Test NuclioProvider initialization."""
        self.assertEqual(self.provider.manager_id, 'test-456')
        self.assertTrue(self.provider.auto_terminate)
        self.assertFalse(self.provider.asynchronous)
        self.assertEqual(self.provider.dashboard_port, 8070)

    def test_deploy_prebuilt_image(self):
        """Test deploying a function with prebuilt image."""
        task = Task(
            name='test-prebuilt',
            vcpus=1,
            memory=512,
            image='docker.io/myuser/myfunction:latest',
            env_var=['FAAS_HANDLER=main:handler']
        )

        # Mock methods
        self.provider._extract_faas_config = Mock(return_value={
            'handler': 'main:handler',
            'runtime': 'python:3.9',
            'timeout': '30'
        })
        self.provider._generate_function_name = Mock(return_value='nuclio-test-456-prebuilt')
        self.provider._determine_deployment_type = Mock(return_value='prebuilt-image')
        self.provider._prepare_image = Mock(return_value='docker.io/myuser/myfunction:latest')
        self.provider._deploy_to_nuclio = Mock()

        # Deploy function
        function_name = self.provider.deploy_function(task)

        self.assertEqual(function_name, 'nuclio-test-456-prebuilt')
        self.provider._deploy_to_nuclio.assert_called_once()

    def test_deploy_container_build(self):
        """Test deploying a function that needs container build."""
        task = Task(
            name='test-build',
            vcpus=2,
            memory=1024,
            image='python:3.9',
            env_var=[
                'FAAS_SOURCE=/tmp/source',
                'FAAS_HANDLER=handler.main',
                'FAAS_REGISTRY_URI=localhost:5000/nuclio'
            ]
        )

        # Mock methods
        self.provider._extract_faas_config = Mock(return_value={
            'source': '/tmp/source',
            'handler': 'handler.main',
            'runtime': 'python:3.9',
            'registry_uri': 'localhost:5000/nuclio'
        })
        self.provider._generate_function_name = Mock(return_value='nuclio-test-456-build')
        self.provider._determine_deployment_type = Mock(return_value='container-build')
        self.provider._configure_registry_if_needed = Mock()

        self.provider.registry_manager = Mock()
        self.provider.registry_manager.build_and_push_image.return_value = (
            'localhost:5000/nuclio:nuclio-test-456-build', Mock(), Mock()
        )
        self.provider._deploy_to_nuclio = Mock()

        # Deploy function
        function_name = self.provider.deploy_function(task)

        self.assertEqual(function_name, 'nuclio-test-456-build')
        self.provider.registry_manager.build_and_push_image.assert_called_once()

    def test_determine_deployment_type(self):
        """Test deployment type determination."""
        # Test prebuilt image
        task1 = Task(image='docker.io/user/image:tag')
        self.assertTrue(self.provider._is_full_image_uri('docker.io/user/image:tag'))
        self.assertEqual(
            self.provider._determine_deployment_type(task1, {}),
            'prebuilt-image'
        )

        # Test container build
        task2 = Task(image='python:3.9')
        config2 = {'source': '/tmp/source'}
        self.assertEqual(
            self.provider._determine_deployment_type(task2, config2),
            'container-build'
        )

        # Test error - no image for source
        task3 = Task()
        config3 = {'source': '/tmp/source'}
        with self.assertRaises(DeploymentException):
            self.provider._determine_deployment_type(task3, config3)

    @patch('nuclio.deploy_file')
    def test_deploy_to_nuclio(self, mock_deploy):
        """Test deploying to Nuclio platform."""
        task = Task(
            name='test',
            vcpus=2,
            memory=1024,
            env_var=['USER_VAR=value']
        )
        task._user_env_vars = ['USER_VAR=value']

        # Setup
        self.provider.dashboard_url = 'http://localhost:8070'

        # Mock ConfigSpec to have the expected structure
        with patch('nuclio.ConfigSpec') as mock_config_spec:
            mock_spec_instance = Mock()
            mock_spec_instance.config = {"spec": {}}
            mock_spec_instance.set_env = Mock()
            mock_config_spec.return_value = mock_spec_instance

            # Deploy
            self.provider._deploy_to_nuclio(
                task=task,
                function_name='test-function',
                image_uri='test/image:latest',
                faas_config={'handler': 'main:handler', 'runtime': 'python:3.9'}
            )

        # Verify
        mock_deploy.assert_called_once()
        call_args = mock_deploy.call_args[1]
        self.assertEqual(call_args['dashboard_url'], 'http://localhost:8070')
        self.assertEqual(call_args['name'], 'test-function')

    def test_invoke_function_success(self):
        """Test successful function invocation."""
        # Setup
        self.provider._functions = {
            'test-function': {
                'task_id': '123',
                'original_name': 'test',
                'deployment_type': 'prebuilt-image'
            }
        }
        self.provider.dashboard_url = 'http://localhost:8070'

        # Mock nuclio module's invoke function
        with patch('hydraa_faas.faas_manager.custom_faas.nuclio') as mock_nuclio:
            # Mock response
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.text = '{"result": "success"}'
            mock_response.headers = {'Content-Type': 'application/json'}
            mock_nuclio.invoke.return_value = mock_response

            # Invoke
            result = self.provider.invoke_function('test-function', {'input': 'data'})

        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['payload'], '{"result": "success"}')
        mock_nuclio.invoke.assert_called_once_with(
            dashboard_url='http://localhost:8070',
            name='test-function',
            body='{"input": "data"}'
        )

    def test_create_source_from_inline_code(self):
        """Test creating source directory from inline code."""
        task = Task(
            cmd=['python', '-c', 'def my_function(): return "hello"']
        )
        task.id = 'test-123'

        # Mock file operations
        mock_file = Mock()

        with patch('builtins.open', create=True) as mock_open:
            mock_open.return_value.__enter__.return_value = mock_file
            with patch('os.makedirs'):
                source_path = self.provider._create_source_from_inline_code(
                    task, {'handler': 'main:handler'}
                )

        # Verify file was written
        mock_open.assert_called()
        self.assertTrue(source_path.endswith('_source'))

    @patch('subprocess.Popen')
    def test_setup_dashboard_access(self, mock_popen):
        """Test dashboard access setup."""
        # Mock subprocess for port forwarding
        mock_process = Mock()
        mock_popen.return_value = mock_process

        # Mock asyncio operations more carefully
        with patch('asyncio.new_event_loop') as mock_new_loop:
            with patch('asyncio.set_event_loop') as mock_set_loop:
                # Create a proper mock event loop
                mock_loop = Mock()
                mock_loop.run_until_complete = Mock(return_value=True)
                mock_loop.close = Mock()
                mock_new_loop.return_value = mock_loop

                self.provider._setup_dashboard_access()

        # Verify port forward was started
        mock_popen.assert_called_once()
        self.assertIsNotNone(self.provider._port_forward_process)
        self.assertEqual(self.provider.dashboard_url, 'http://localhost:8070')

    def test_shutdown_cleanup(self):
        """Test provider shutdown with cleanup."""
        # Setup functions to delete
        self.provider._functions = {
            'func1': {'task_id': '1'},
            'func2': {'task_id': '2'}
        }

        # Mock worker thread
        self.provider.worker_thread = Mock()
        self.provider.worker_thread.is_alive.return_value = True
        self.provider.worker_thread.join.return_value = None

        # Mock port forward process
        self.provider._port_forward_process = Mock()

        # Mock k8s cluster
        self.provider.k8s_cluster = Mock()

        # Mock executor
        self.provider.executor = Mock()

        # Mock nuclio module's delete_function
        with patch('hydraa_faas.faas_manager.custom_faas.nuclio') as mock_nuclio:
            mock_nuclio.delete_function = Mock()

            # Shutdown
            self.provider.shutdown()

            # Verify cleanup
            self.assertTrue(self.provider._terminate.is_set())
            self.assertEqual(mock_nuclio.delete_function.call_count, 2)
            mock_nuclio.delete_function.assert_any_call(
                dashboard_url=self.provider.dashboard_url,
                name='func1'
            )
            mock_nuclio.delete_function.assert_any_call(
                dashboard_url=self.provider.dashboard_url,
                name='func2'
            )

        self.provider._port_forward_process.terminate.assert_called_once()
        self.provider.k8s_cluster.shutdown.assert_called_once()
        self.provider.executor.shutdown.assert_called_once_with(wait=True)

    def test_extract_granular_config(self):
        """Test extracting granular Nuclio configuration."""
        task = Task(
            env_var=[
                'FAAS_MIN_REPLICAS=2',
                'FAAS_MAX_REPLICAS=10',
                'FAAS_TARGET_CPU=80',
                'FAAS_SCALE_TO_ZERO=true',
                'FAAS_TRIGGERS={"http": {"maxWorkers": 4}}'
            ]
        )

        config = self.provider._extract_granular_config(task)

        self.assertEqual(config['min_replicas'], '2')
        self.assertEqual(config['max_replicas'], '10')
        self.assertEqual(config['target_cpu'], '80')
        self.assertEqual(config['scale_to_zero'], 'true')
        self.assertEqual(config['triggers'], {'http': {'maxWorkers': 4}})


if __name__ == '__main__':
    unittest.main()