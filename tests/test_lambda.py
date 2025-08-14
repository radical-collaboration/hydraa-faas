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
        self.test_dir = tempfile.mkdtemp()
        self.cred = {
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'region_name': 'us-east-1'
        }
        self.mock_logger = Mock()
        self.mock_profiler = Mock()

        # --- FIX: Apply patches using a context manager inside setUp ---
        # This is the correct pattern for unittest. It ensures mocks are active
        # during the initialization of the class under test without changing
        # the method signature of setUp.
        patcher_pool = patch('hydraa_faas.faas_manager.aws_lambda.AWSClientPool')
        patcher_rm = patch('hydraa_faas.faas_manager.aws_lambda.ResourceManager')
        patcher_reg = patch('hydraa_faas.faas_manager.aws_lambda.RegistryManager')

        # Start the patchers and get the mock classes
        mock_pool_cls = patcher_pool.start()
        mock_rm_cls = patcher_rm.start()
        mock_reg_cls = patcher_reg.start()

        # This ensures the patchers are stopped after the test
        self.addCleanup(patcher_pool.stop)
        self.addCleanup(patcher_rm.stop)
        self.addCleanup(patcher_reg.stop)

        # Configure the mock AWS clients
        self.mock_lambda_client = Mock()
        self.mock_iam_client = Mock()
        self.mock_ecr_client = Mock()

        # Configure return values for ALL expected calls to prevent TypeErrors
        self.mock_iam_client.create_role.return_value = {
            'Role': {'Arn': 'arn:aws:iam::123456789012:role/mock-role-from-test'}
        }
        self.mock_iam_client.get_role.side_effect = ClientError({'Error': {'Code': 'NoSuchEntity'}}, 'GetRole')
        self.mock_iam_client.list_attached_role_policies.return_value = {'AttachedPolicies': []}
        self.mock_iam_client.list_role_policies.return_value = {'PolicyNames': []}
        self.mock_iam_client.list_instance_profiles_for_role.return_value = {'InstanceProfiles': []}

        # Configure the mock client pool to return our configured clients
        mock_pool_instance = mock_pool_cls.return_value
        mock_pool_instance.get_client.side_effect = lambda service, *args: {
            'lambda': self.mock_lambda_client,
            'iam': self.mock_iam_client,
            'ecr': self.mock_ecr_client
        }[service]

        # Configure the mock ResourceManager
        self.mock_resource_manager = mock_rm_cls.return_value
        self.mock_resource_manager.create_aws_iam_role.return_value = 'arn:aws:iam::123456789012:role/mock-role-from-test'
        self.mock_resource_manager.aws_resources.iam_role_arn = None # Start with no pre-existing role
        self.mock_resource_manager.aws_resources.ecr_repository_uri = None # Start with no pre-existing repo

        # Initialize the provider, which will now use all our mocks
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

        # Common test setup
        self.provider._terminate = threading.Event()
        self.provider._functions_lock = threading.RLock()
        self.provider.executor = Mock()


    def tearDown(self):
        """Clean up test fixtures."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_initialization(self):
        """Test AwsLambda initialization."""
        self.assertEqual(self.provider.region, 'us-east-1')
        self.assertEqual(self.provider.manager_id, 'test-123')
        self.assertEqual(self.provider._iam_role_arn, 'arn:aws:iam::123456789012:role/mock-role-from-test')

    def test_create_ecr_repository(self):
        """Test ECR repository creation."""
        # Ensure the check for an existing URI returns None to force creation path
        self.mock_resource_manager.aws_resources.ecr_repository_uri = None
        self.mock_resource_manager.create_aws_ecr_repository.return_value = '123.dkr.ecr.us-east-1.amazonaws.com/repo'

        repo_uri = self.provider._create_ecr_repository()

        self.assertEqual(repo_uri, '123.dkr.ecr.us-east-1.amazonaws.com/repo')
        self.mock_resource_manager.create_aws_ecr_repository.assert_called_once()

    def test_shutdown_with_cleanup(self):
        """Test provider shutdown with resource cleanup."""
        self.provider._functions = {'func1': {'arn': 'arn1'}, 'func2': {'arn': 'arn2'}}
        mock_future = Mock()
        mock_future.result.return_value = None
        self.provider.executor.submit.return_value = mock_future

        # Reset the mock to ignore calls made during setUp
        self.mock_resource_manager.save_all_resources.reset_mock()

        # Shutdown
        self.provider.shutdown()

        # Verify cleanup
        self.mock_resource_manager.cleanup_aws_resources.assert_called_once()
        self.mock_resource_manager.save_all_resources.assert_called_once()
        self.provider.executor.shutdown.assert_called_once_with(wait=True)

    def test_shutdown_async_mode(self):
        """Test provider shutdown in async mode with worker thread."""
        self.provider.asynchronous = True
        self.provider.worker_thread = Mock()
        self.provider.worker_thread.is_alive.return_value = True
        self.provider.executor.submit.return_value = Mock()

        self.provider.shutdown()

        self.provider.worker_thread.join.assert_called_once_with(timeout=5)
        self.mock_resource_manager.cleanup_aws_resources.assert_called_once()

    def test_deploy_zip_function(self):
        """Test deploying a function from zip."""
        task = Task(name='test-function', vcpus=1, memory=512, image='python:3.9',
                    cmd=['python', '-c', 'print("hello")'],
                    env_var=['FAAS_SOURCE=/tmp/test_source', 'FAAS_HANDLER=handler.main', 'FAAS_RUNTIME=python3.9'])
        self.provider._extract_faas_config = Mock(return_value={'source': '/tmp/test_source', 'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'})
        self.provider._generate_function_name = Mock(return_value='hydra-test-123-function')
        self.provider._determine_deployment_type = Mock(return_value='zip')
        self.provider._create_package = Mock(return_value=b'fake_zip_content')
        self.provider._deploy_to_lambda = Mock(return_value='arn:aws:lambda:us-east-1:123:function:test')
        function_name = self.provider.deploy_function(task)
        self.assertEqual(function_name, 'hydra-test-123-function')
        self.provider._deploy_to_lambda.assert_called_once()
        self.assertIn('hydra-test-123-function', self.provider._functions)

    def test_deploy_container_function(self):
        """Test deploying a function from container image."""
        task = Task(name='test-container', vcpus=1, memory=1024, image='123456789.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest', env_var=['FAAS_HANDLER=handler.main'])
        self.provider._extract_faas_config = Mock(return_value={'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'})
        self.provider._generate_function_name = Mock(return_value='hydra-test-123-container')
        self.provider._determine_deployment_type = Mock(return_value='prebuilt-image')
        self.provider._create_package = Mock(return_value='123456789.dkr.ecr.us-east-1.amazonaws.com/myrepo:latest')
        self.provider._deploy_to_lambda = Mock(return_value='arn:aws:lambda:us-east-1:123:function:container')
        function_name = self.provider.deploy_function(task)
        self.assertEqual(function_name, 'hydra-test-123-container')

    def test_determine_deployment_type(self):
        """Test deployment type determination."""
        task1 = Task(image='123.dkr.ecr.region.amazonaws.com/repo:tag')
        self.assertEqual(self.provider._determine_deployment_type(task1, {}), 'prebuilt-image')
        task2 = Task()
        config2 = {'source': '/tmp/source'}
        self.assertEqual(self.provider._determine_deployment_type(task2, config2), 'zip')
        task3 = Task(image='python:3.9')
        config3 = {'source': '/tmp/source'}
        self.assertEqual(self.provider._determine_deployment_type(task3, config3), 'container-build')
        task4 = Task(cmd=['python', '-c', 'print("hello")'])
        config4 = {}
        self.assertEqual(self.provider._determine_deployment_type(task4, config4), 'zip')

    def test_deploy_to_lambda_success(self):
        """Test successful Lambda deployment."""
        task = Task(name='test', vcpus=1, memory=512)
        task._user_env_vars = []
        self.mock_lambda_client.create_function.return_value = {'FunctionArn': 'arn:aws:lambda:us-east-1:123:function:test'}
        mock_waiter = Mock()
        self.mock_lambda_client.get_waiter.return_value = mock_waiter
        self.assertIsNotNone(self.provider._iam_role_arn)
        arn = self.provider._deploy_to_lambda(task=task, function_name='test-function', package=b'zip_content', deployment_type='zip', faas_config={'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'})
        self.assertEqual(arn, 'arn:aws:lambda:us-east-1:123:function:test')
        self.mock_lambda_client.create_function.assert_called_once()
        mock_waiter.wait.assert_called_once_with(FunctionName='test-function')

    def test_deploy_to_lambda_update_existing(self):
        """Test updating existing Lambda function."""
        task = Task(name='test', vcpus=1, memory=512)
        task._user_env_vars = []
        error_response = {'Error': {'Code': 'ResourceConflictException'}}
        self.mock_lambda_client.create_function.side_effect = ClientError(error_response, 'CreateFunction')
        self.mock_lambda_client.update_function_code.return_value = {}
        self.mock_lambda_client.update_function_configuration.return_value = {'FunctionArn': 'arn:aws:lambda:us-east-1:123:function:test-updated'}
        mock_waiter = Mock()
        self.mock_lambda_client.get_waiter.return_value = mock_waiter
        self.assertIsNotNone(self.provider._iam_role_arn)
        arn = self.provider._deploy_to_lambda(task=task, function_name='test-function', package=b'zip_content', deployment_type='zip', faas_config={'handler': 'handler.main', 'runtime': 'python3.9', 'timeout': '30'})
        self.assertEqual(arn, 'arn:aws:lambda:us-east-1:123:function:test-updated')
        self.mock_lambda_client.update_function_code.assert_called_once()
        self.mock_lambda_client.update_function_configuration.assert_called_once()

    def test_invoke_function_success(self):
        """Test successful function invocation."""
        self.provider._functions = {'test-function': {'arn': 'arn:aws:lambda:us-east-1:123:function:test', 'task_id': '123', 'deployment_type': 'zip'}}
        mock_payload = Mock()
        mock_payload.read.return_value = b'{"result": "success"}'
        mock_response = {'StatusCode': 200, 'Payload': mock_payload, 'LogResult': 'base64_logs'}
        self.mock_lambda_client.invoke.return_value = mock_response
        result = self.provider.invoke_function('test-function', {'input': 'data'})
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['payload'], {'result': 'success'})
        self.mock_lambda_client.invoke.assert_called_once_with(FunctionName='test-function', Payload='{"input": "data"}', LogType='Tail')

    def test_invoke_function_not_found(self):
        """Test invoking non-existent function."""
        self.provider._functions = {}
        with self.assertRaises(InvocationException) as context:
            self.provider.invoke_function('non-existent')
        self.assertIn('not found', str(context.exception))

if __name__ == '__main__':
    unittest.main()
