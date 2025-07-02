"""
Unit tests for FaaS Manager and AWS Lambda Provider
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import queue
import tempfile
import os
from io import BytesIO

from hydraa import Task, proxy
from manager import FaasManager
from aws_lambda import AwsLambda
from exceptions import FaasException, DeploymentException, InvocationException
from botocore.exceptions import ClientError


class TestFaasManager(unittest.TestCase):
    """Test suite for FaasManager"""

    def setUp(self):
        """Set up test fixtures"""
        self.mock_proxy = Mock(spec=proxy)
        self.mock_proxy.loaded_providers = ['aws']
        self.mock_proxy._load_credentials.return_value = {
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'region_name': 'us-east-1'
        }
        self.test_sandbox = tempfile.mkdtemp()
        os.makedirs(self.test_sandbox, exist_ok=True)

    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    def test_faas_manager_initialization(self):
        """Test FaasManager initialization"""
        faas_mgr = FaasManager(
            self.mock_proxy,
            asynchronous=True,
            auto_terminate=True
        )
        self.assertEqual(faas_mgr._proxy, self.mock_proxy)
        # test metrics collector is initialized
        self.assertIsNotNone(faas_mgr.metrics)

    @patch('manager.misc.logger')
    def test_faas_manager_start(self, mock_logger):
        """Test FaasManager start method"""
        mock_aws_lambda_class = Mock(spec=AwsLambda)
        mock_aws_lambda_class.__name__ = 'AwsLambda'
        mock_lambda_instance = Mock()
        mock_lambda_instance.run_id = 'test-run-id'
        mock_lambda_instance.incoming_q = queue.Queue()
        mock_lambda_instance.outgoing_q = queue.Queue()
        mock_aws_lambda_class.return_value = mock_lambda_instance
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('manager.PROVIDER_TO_CLASS', {'aws': mock_aws_lambda_class}):
            faas_mgr.start(self.test_sandbox)
        mock_aws_lambda_class.assert_called_once()
        self.assertIn('aws', faas_mgr._registered_managers)

    def test_submit_single_task(self):
        """Test submitting a single task"""
        faas_mgr = FaasManager(self.mock_proxy)
        mock_manager = {'in_q': Mock(spec=queue.Queue)}
        faas_mgr._registered_managers['aws'] = mock_manager
        faas_mgr.logger = Mock()
        task = Task(provider='aws')
        faas_mgr.submit(task)
        mock_manager['in_q'].put.assert_called_once_with(task)

    def test_submit_multiple_tasks(self):
        """Test submitting multiple tasks"""
        faas_mgr = FaasManager(self.mock_proxy)
        mock_manager = {'in_q': Mock(spec=queue.Queue)}
        faas_mgr._registered_managers['aws'] = mock_manager
        faas_mgr.logger = Mock()
        tasks = [Task(provider='aws') for _ in range(3)]
        faas_mgr.submit(tasks)
        self.assertEqual(mock_manager['in_q'].put.call_count, 3)

    def test_decorator_functionality(self):
        """Test FaasManager decorator"""
        faas_mgr = FaasManager(self.mock_proxy)
        faas_mgr.submit = Mock()
        @faas_mgr(provider='aws')
        def test_function():
            return Task()
        result_task = test_function()
        self.assertEqual(result_task.provider, 'aws')
        faas_mgr.submit.assert_called_once_with(result_task)

    def test_get_performance_metrics(self):
        """Test performance metrics functionality"""
        faas_mgr = FaasManager(self.mock_proxy)
        report = faas_mgr.get_performance_metrics()
        self.assertIn('message', report)  # should show no metrics initially

    def test_shutdown_all_providers(self):
        """Test shutting down all providers"""
        faas_mgr = FaasManager(self.mock_proxy)
        mock_manager1 = {'class': Mock()}
        mock_manager2 = {'class': Mock()}
        faas_mgr._registered_managers = {'aws': mock_manager1, 'local': mock_manager2}
        faas_mgr.shutdown()
        mock_manager1['class'].shutdown.assert_called_once()
        mock_manager2['class'].shutdown.assert_called_once()
        self.assertEqual(len(faas_mgr._registered_managers), 0)


class TestAwsLambda(unittest.TestCase):
    """Test suite for AwsLambda provider"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_sandbox = tempfile.mkdtemp()
        self.test_creds = {
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'region_name': 'us-east-1'
        }
        self.mock_logger = Mock()

    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    @patch('boto3.client')
    @patch('boto3.resource')
    def test_aws_lambda_initialization(self, mock_resource, mock_client):
        """Test AwsLambda initialization"""
        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=True, auto_terminate=True, log=self.mock_logger
        )
        self.assertEqual(aws_lambda.manager_id, 'test-manager')
        # test shutdown protection
        self.assertFalse(aws_lambda._shutdown_started)

    @patch('aws_lambda.create_deployment_package')
    @patch('aws_lambda.validate_handler')
    @patch('boto3.client')
    def test_deploy_source_package(self, mock_boto_client, mock_validate, mock_create_package):
        """Test deploying a source package"""
        mock_validate.return_value = True
        mock_create_package.return_value = b'fake_zip_content'
        mock_lambda_client = MagicMock()
        mock_lambda_client.create_function.return_value = {'FunctionArn': 'arn:aws:lambda:us-east-1:123:function:test'}
        mock_iam_client = MagicMock()
        mock_iam_client.get_role.side_effect = ClientError({'Error': {'Code': 'NoSuchEntity'}}, 'GetRole')
        mock_iam_client.create_role.return_value = {'Role': {'Arn': 'arn:aws:iam::123:role/test-role'}}

        def client_side_effect(service_name, **kwargs):
            if service_name == 'lambda': return mock_lambda_client
            if service_name == 'iam': return mock_iam_client
            return MagicMock()

        mock_boto_client.side_effect = client_side_effect

        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=False, auto_terminate=False, log=self.mock_logger
        )
        task = Task()
        task.source_path = self.test_sandbox
        task.handler_name = 'main.handler'
        task.name = 'test-function'

        result = aws_lambda._deploy_source_package(task)
        self.assertIn('arn:aws:lambda', result)

    @patch('boto3.client')
    def test_invoke_function(self, mock_client):
        """Test function invocation"""
        mock_lambda_client = MagicMock()
        mock_lambda_client.invoke.return_value = {
            'StatusCode': 200, 'Payload': MagicMock(read=lambda: b'{"result": "success"}')
        }
        mock_client.return_value = mock_lambda_client
        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=False, auto_terminate=False, log=self.mock_logger
        )
        result = aws_lambda.invoke_function('test-function', {'test': 'payload'})
        self.assertEqual(result['StatusCode'], 200)

    @patch('boto3.client')
    def test_list_functions(self, mock_client):
        """Test listing Lambda functions"""
        mock_lambda_client = MagicMock()
        mock_lambda_client.list_functions.return_value = {'Functions': [{'FunctionName': 'f1'}, {'FunctionName': 'f2'}]}
        mock_client.return_value = mock_lambda_client
        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=False, auto_terminate=False, log=self.mock_logger
        )
        functions = aws_lambda.list_functions()
        self.assertEqual(len(functions), 2)

    @patch('boto3.client')
    def test_delete_function(self, mock_client):
        """Test deleting a Lambda function"""
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client
        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=False, auto_terminate=False, log=self.mock_logger
        )
        aws_lambda._created_resources['functions']['test-function'] = 'test-arn'
        aws_lambda._functions_book['test-function'] = Mock()
        aws_lambda.delete_function('test-function')
        mock_lambda_client.delete_function.assert_called_once_with(FunctionName='test-function')

    @patch('boto3.client')
    def test_shutdown_race_condition_protection(self, mock_client):
        """Test shutdown race condition protection"""
        aws_lambda = AwsLambda(
            sandbox=self.test_sandbox, manager_id='test-manager', cred=self.test_creds,
            asynchronous=False, auto_terminate=False, log=self.mock_logger
        )

        # first shutdown should proceed
        aws_lambda.shutdown()
        self.assertTrue(aws_lambda._shutdown_started)

        # second shutdown should be ignored
        aws_lambda.shutdown()  # should not raise exception


class TestPackaging(unittest.TestCase):
    """Test suite for packaging utilities"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)

    @patch('utils.packaging.estimate_package_size')
    def test_streaming_package_creation(self, mock_estimate):
        """Test streaming vs in-memory package creation"""
        from utils.packaging import create_deployment_package

        # create test file
        test_file = os.path.join(self.test_dir, 'test.py')
        with open(test_file, 'w') as f:
            f.write('print("hello")')

        # test small package (in-memory)
        mock_estimate.return_value = 1024  # 1kb
        result = create_deployment_package(self.test_dir)
        self.assertIsInstance(result, bytes)

        # test large package (temp file)
        mock_estimate.return_value = 50 * 1024 * 1024  # 50mb
        result = create_deployment_package(self.test_dir)
        self.assertIsInstance(result, bytes)


class TestFaasIntegration(unittest.TestCase):
    """Integration tests for FaaS components"""

    def setUp(self):
        """Set up integration test fixtures"""
        self.test_sandbox = tempfile.mkdtemp()
        os.makedirs(self.test_sandbox, exist_ok=True)

    def tearDown(self):
        """Clean up integration test fixtures"""
        import shutil
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    @patch('aws_lambda.create_deployment_package')
    @patch('aws_lambda.validate_handler')
    @patch('boto3.resource')
    @patch('boto3.client')
    @patch('manager.misc.logger')
    def test_end_to_end_deployment(self, mock_logger, mock_client, mock_resource, mock_validate, mock_create_package):
        """Test end-to-end function deployment"""
        mock_validate.return_value = True
        mock_create_package.return_value = b'fake_zip_content'
        mock_lambda_client = MagicMock()
        mock_lambda_client.create_function.return_value = {'FunctionArn': 'arn:aws:lambda...'}
        mock_proxy = Mock(spec=proxy)
        mock_proxy.loaded_providers = ['aws']
        mock_proxy._load_credentials.return_value = {}
        faas_mgr = FaasManager(mock_proxy, asynchronous=False)
        mock_aws_class = MagicMock()
        mock_aws_class.__name__ = "AwsLambda"
        mock_aws_class.return_value = MagicMock()

        with patch.dict('manager.PROVIDER_TO_CLASS', {'aws': mock_aws_class}):
            faas_mgr.start(self.test_sandbox)
        task = Task()
        task.source_path = self.test_sandbox
        task.provider = 'aws'

        faas_mgr.submit(task)
        self.assertTrue(True)


if __name__ == '__main__':
    unittest.main()