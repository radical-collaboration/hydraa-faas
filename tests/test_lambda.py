"""Unit tests for AwsLambda provider."""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
import json
import os
import tempfile
import shutil
import threading
from botocore.exceptions import ClientError

from hydraa import Task
from hydraa_faas.faas_manager.aws_lambda import AwsLambda, DeploymentException, InvocationException


class TestAwsLambda(unittest.TestCase):
    """Test cases for AwsLambda provider."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory
        self.test_dir = tempfile.mkdtemp()

        # Mock AWS credentials
        self.cred = {
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'region_name': 'us-east-1'
        }

        # Mock logger and profiler
        self.mock_logger = Mock()
        self.mock_logger.trace = Mock()
        self.mock_logger.error = Mock()
        self.mock_logger.warning = Mock()

        self.mock_profiler = Mock()
        self.mock_profiler.prof = Mock()

        # Create provider with mocked AWS client pool
        with patch('hydraa_faas.faas_manager.aws_lambda.AWSClientPool') as mock_pool:
            # Mock the client pool singleton
            mock_pool_instance = Mock()
            mock_pool.return_value = mock_pool_instance

            # Mock AWS clients
            mock_lambda_client = Mock()
            mock_iam_client = Mock()
            mock_ecr_client = Mock()

            mock_pool_instance.get_client.side_effect = lambda service, *args: {
                'lambda': mock_lambda_client,
                'iam': mock_iam_client,
                'ecr': mock_ecr_client
            }[service]

            # Initialize provider
            self.provider = AwsLambda(
                sandbox=os.path.join(self.test_dir, 'lambda_sandbox'),
                manager_id='test-123',
                cred=self.cred,
                asynchronous=False,
                auto_terminate=True,
                log=self.mock_logger,
                resource_config={},
                profiler=self.mock_profiler
            )

            # Set the mocked clients
            self.provider._lambda_client = mock_lambda_client
            self.provider._iam_client = mock_iam_client
            self.provider._ecr_client = mock_ecr_client

        # Initialize threading components
        self.provider._terminate = threading.Event()
        self.provider._functions_lock = threading.RLock()

        # Mock resource and registry managers
        self.provider.resource_manager = Mock()
        self.provider.resource_manager.aws_resources = Mock()
        self.provider.resource_manager.aws_resources.iam_role_arn = None
        self.provider.resource_manager.aws_resources.ecr_repository_uri = None

        self.provider.registry_manager = Mock()

        # Mock executor
        self.provider.executor = Mock()

    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_initialization(self):
        """Test AwsLambda initialization."""
        self.assertEqual(self.provider.region, 'us-east-1')
        self.assertEqual(self.provider.manager_id, 'test-123')
        self.assertTrue(self.provider.auto_terminate)
        self.assertFalse(self.provider.asynchronous)

    def test_deploy_zip_function(self):
        """Test deploying a function from zip."""
        # Create test task
        task = Task(
            name='test-function',
            vcpus=1,
            memory=512,
            image='python:3.9',
            cmd=['python', '-c', 'print("hello")'],
            env_var=[
                'FAAS_SOURCE=/tmp/test_source',
                'FAAS_HANDLER=handler.main',
                'FAAS_RUNTIME=python3.9'
            ]
        )

        # Mock methods
        self.provider._extract_faas_config = Mock(return_value={
            'source': '/tmp/test_source',
            'handler': 'handler.main',
            'runtime': 'python3.9',
            'timeout': '30'
        })
        self.provider._generate_function_name = Mock(return_value='hydra-test-123-function')
        self.provider._determine_deployment_type = Mock(return_value='zip')
        self.provider._create_package = Mock(return_value=b'fake_zip_content')
        self.provider._deploy_to_lambda = Mock(return_value='arn:aws:lambda:us-east-1:123:function:test')

        # Deploy function
        function_name = self.provider.deploy_function(task)

        self.assertEqual(function_name, 'hydra-test-123-function')
        self.provider._deploy_to_lambda.assert_called_once()

        # Verify function was tracked
        self.assertIn('hydra-test-123-function', self.provider._functions)

    def test_deploy_container_function(self):
        """Test deploying a function from container image."""
        task = Task(
            name='test-container',
            vcpus=1,
            memory=1024,
            image='123456789.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest',
            env_var=['FAAS_HANDLER=handler.main']
        )

        # Mock methods
        self.provider._extract_faas_config = Mock(return_value={
            'handler': 'handler.main',
            'runtime': 'python3.9',
            'timeout': '30'
        })
        self.provider._generate_function_name = Mock(return_value='hydra-test-123-container')
        self.provider._determine_deployment_type = Mock(return_value='prebuilt-image')
        self.provider._create_package = Mock(return_value='123456789.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest')
        self.provider._deploy_to_lambda = Mock(return_value='arn:aws:lambda:us-east-1:123:function:container')

        # Deploy function
        function_name = self.provider.deploy_function(task)

        self.assertEqual(function_name, 'hydra-test-123-container')

    def test_determine_deployment_type(self):
        """Test deployment type determination."""
        # Test prebuilt image
        task1 = Task(image='123.dkr.ecr.region.amazonaws.com/repo:tag')
        self.assertEqual(
            self.provider._determine_deployment_type(task1, {}),
            'prebuilt-image'
        )

        # Test zip deployment
        task2 = Task()
        config2 = {'source': '/tmp/source'}
        self.assertEqual(
            self.provider._determine_deployment_type(task2, config2),
            'zip'
        )

        # Test container build
        task3 = Task(image='python:3.9')
        config3 = {'source': '/tmp/source'}
        self.assertEqual(
            self.provider._determine_deployment_type(task3, config3),
            'container-build'
        )

        # Test inline code to zip
        task4 = Task(cmd=['python', '-c', 'print("hello")'])
        config4 = {}
        self.assertEqual(
            self.provider._determine_deployment_type(task4, config4),
            'zip'
        )

    def test_deploy_to_lambda_success(self):
        """Test successful Lambda deployment."""
        task = Task(name='test', vcpus=1, memory=512)
        task._user_env_vars = []

        # Mock successful response
        self.provider._lambda_client.create_function.return_value = {
            'FunctionArn': 'arn:aws:lambda:us-east-1:123:function:test'
        }

        # Mock waiter
        mock_waiter = Mock()
        self.provider._lambda_client.get_waiter.return_value = mock_waiter

        # Mock IAM role
        self.provider._iam_role_arn = 'arn:aws:iam::123:role/lambda-role'

        # Deploy
        arn = self.provider._deploy_to_lambda(
            task=task,
            function_name='test-function',
            package=b'zip_content',
            deployment_type='zip',
            faas_config={'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'}
        )

        self.assertEqual(arn, 'arn:aws:lambda:us-east-1:123:function:test')
        self.provider._lambda_client.create_function.assert_called_once()
        mock_waiter.wait.assert_called_once_with(FunctionName='test-function')

    def test_deploy_to_lambda_update_existing(self):
        """Test updating existing Lambda function."""
        task = Task(name='test', vcpus=1, memory=512)
        task._user_env_vars = []

        # Mock create function to raise ResourceConflictException
        error_response = {'Error': {'Code': 'ResourceConflictException'}}
        self.provider._lambda_client.create_function.side_effect = ClientError(error_response, 'CreateFunction')

        # Mock update methods
        self.provider._lambda_client.update_function_code.return_value = {}
        self.provider._lambda_client.update_function_configuration.return_value = {
            'FunctionArn': 'arn:aws:lambda:us-east-1:123:function:test-updated'
        }

        # Mock waiter
        mock_waiter = Mock()
        self.provider._lambda_client.get_waiter.return_value = mock_waiter

        # Mock IAM role
        self.provider._iam_role_arn = 'arn:aws:iam::123:role/lambda-role'

        # Deploy (should update)
        arn = self.provider._deploy_to_lambda(
            task=task,
            function_name='test-function',
            package=b'zip_content',
            deployment_type='zip',
            faas_config={'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'}
        )

        self.assertEqual(arn, 'arn:aws:lambda:us-east-1:123:function:test-updated')
        self.provider._lambda_client.update_function_code.assert_called_once()
        self.provider._lambda_client.update_function_configuration.assert_called_once()

    def test_invoke_function_success(self):
        """Test successful function invocation."""
        # Setup
        self.provider._functions = {
            'test-function': {
                'arn': 'arn:aws:lambda:us-east-1:123:function:test',
                'task_id': '123',
                'deployment_type': 'zip'
            }
        }

        # Mock response
        mock_payload = Mock()
        mock_payload.read.return_value = b'{"result": "success"}'

        mock_response = {
            'StatusCode': 200,
            'Payload': mock_payload,
            'LogResult': 'base64_logs'
        }
        self.provider._lambda_client.invoke.return_value = mock_response

        # Invoke
        result = self.provider.invoke_function('test-function', {'input': 'data'})

        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['payload'], {'result': 'success'})
        self.provider._lambda_client.invoke.assert_called_once_with(
            FunctionName='test-function',
            Payload='{"input": "data"}',
            LogType='Tail'
        )

    def test_invoke_function_not_found(self):
        """Test invoking non-existent function."""
        self.provider._functions = {}

        with self.assertRaises(InvocationException) as context:
            self.provider.invoke_function('non-existent')

        self.assertIn('not found', str(context.exception))

    def test_create_ecr_repository(self):
        """Test ECR repository creation."""
        # Mock resource manager to return new repo
        self.provider.resource_manager.create_aws_ecr_repository.return_value = \
            '123.dkr.ecr.us-east-1.amazonaws.com/repo'

        # Create repository
        repo_uri = self.provider._create_ecr_repository()

        self.assertEqual(repo_uri, '123.dkr.ecr.us-east-1.amazonaws.com/repo')
        self.provider.resource_manager.create_aws_ecr_repository.assert_called_once()

    def test_shutdown_with_cleanup(self):
        """Test provider shutdown with resource cleanup."""
        # Setup functions to delete
        self.provider._functions = {
            'func1': {'arn': 'arn1'},
            'func2': {'arn': 'arn2'}
        }

        # Since provider is in synchronous mode (asynchronous=False),
        # there should be no worker thread
        self.assertFalse(hasattr(self.provider, 'worker_thread'))

        # Mock executor futures
        mock_future = Mock()
        mock_future.result.return_value = None
        self.provider.executor.submit.return_value = mock_future

        # Shutdown
        self.provider.shutdown()

        # Verify cleanup
        self.assertTrue(self.provider._terminate.is_set())

        # Verify delete functions were submitted
        self.assertEqual(self.provider.executor.submit.call_count, 2)

        # Verify resource cleanup
        self.provider.resource_manager.cleanup_aws_resources.assert_called_once()
        self.provider.resource_manager.save_all_resources.assert_called_once()

        # Verify executor shutdown
        self.provider.executor.shutdown.assert_called_once_with(wait=True)

    def test_shutdown_async_mode(self):
        """Test provider shutdown in async mode with worker thread."""
        # Set provider to async mode
        self.provider.asynchronous = True

        # Mock worker thread
        self.provider.worker_thread = Mock()
        self.provider.worker_thread.is_alive.return_value = True

        # Mock executor
        self.provider.executor.submit.return_value = Mock()

        # Shutdown
        self.provider.shutdown()

        # Verify worker thread was stopped
        self.assertTrue(self.provider._terminate.is_set())
        self.provider.worker_thread.join.assert_called_once_with(timeout=5)


if __name__ == '__main__':
    unittest.main()