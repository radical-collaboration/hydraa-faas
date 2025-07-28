"""
unit tests for aws lambda provider
tests the core functionality without hitting real aws services
"""
import unittest
import queue
import time
import json
import threading
from unittest.mock import Mock, MagicMock, patch, call
from botocore.exceptions import ClientError
from concurrent.futures import Future

# mock task class that mimics hydraa's task
class MockTask(Future):
    def __init__(self):
        super().__init__()
        self.handler_code = None
        self.source_path = None
        self.image = None
        self.build_image = False
        self.memory = 256
        self.vcpus = 0.25
        self.name = 'test-task'
        self.handler = 'handler.handler'
        self.runtime = 'python3.9'
        self.env_vars = {}
        self.timeout = 30
        self.provider = 'aws'
        self.launch_type = 'lambda'
        self.id = None
        self.state = None
        self.arn = None
        self.layers = []
        self.vpc_config = None
        self.tags = {}
        self.min_replicas = 0
        self.max_replicas = 10

    def set_running_or_notify_cancel(self):
        """mock method for task state management"""
        pass


class TestAwsLambda(unittest.TestCase):
    """test cases for aws lambda provider"""

    def setUp(self):
        """set up test fixtures before each test"""
        self.mock_creds = {
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            'region_name': 'us-east-1',
            'auto_setup_resources': False  # disable auto setup for tests
        }

        self.mock_logger = Mock()
        self.mock_resource_manager = Mock()
        self.mock_resource_manager.aws_resources.iam_role_arn = 'arn:aws:iam::123456789012:role/test-role'
        self.mock_resource_manager.aws_resources.iam_role_name = 'test-role'
        self.mock_resource_manager.aws_resources.ecr_repository_uri = '123456789012.dkr.ecr.us-east-1.amazonaws.com/test-repo'
        self.mock_resource_manager.aws_resources.ecr_repository_name = 'test-repo'

        # mock registry manager
        self.mock_registry_manager = Mock()
        self.mock_resource_manager.registry_manager = self.mock_registry_manager

    @patch('boto3.client')
    def test_initialization(self, mock_boto_client):
        """test provider initializes correctly with all components"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        # setup boto3 mocks
        mock_lambda_client = Mock()
        mock_iam_client = Mock()
        mock_ecr_client = Mock()

        def client_factory(service_name, **kwargs):
            if service_name == 'lambda':
                return mock_lambda_client
            elif service_name == 'iam':
                return mock_iam_client
            elif service_name == 'ecr':
                return mock_ecr_client

        mock_boto_client.side_effect = client_factory

        # create provider
        provider = AwsLambda(
            sandbox='test_sandbox',
            manager_id='test_id',
            cred=self.mock_creds,
            asynchronous=True,
            auto_terminate=False,
            log=self.mock_logger,
            resource_manager=self.mock_resource_manager
        )

        # verify initialization
        self.assertEqual(provider.region, 'us-east-1')
        self.assertTrue(provider.status)
        self.assertEqual(provider._task_id, 0)
        self.assertEqual(len(provider._deployed_functions), 0)
        self.assertIsNotNone(provider.worker_thread)
        self.assertTrue(provider.worker_thread.is_alive())

        # verify clients were created with correct config
        self.assertEqual(mock_boto_client.call_count, 3)

        # cleanup - important to stop the worker thread
        provider._terminate.set()
        provider.worker_thread.join(timeout=2)

    def test_task_processing_zip_deployment(self):
        """test zip deployment processing directly"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda, LambdaConfig

        # create provider with mocked clients
        provider = self._create_provider_with_mocks()

        # mock successful function creation
        provider._lambda_client.create_function.return_value = {
            'FunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:test-func'
        }

        # create config
        config = LambdaConfig(
            function_name='test-func',
            runtime='python3.9',
            role='test-role-arn',
            handler='handler.handler',
            timeout=30,
            memory_size=256,
            environment={'Variables': {'TEST': 'value'}}
        )

        # test zip deployment directly
        zip_content = b'fake-zip-content'
        result = provider._create_or_update_function(config, zip_content=zip_content)

        # verify zip deployment
        provider._lambda_client.create_function.assert_called_once()
        call_args = provider._lambda_client.create_function.call_args[1]
        self.assertEqual(call_args['FunctionName'], 'test-func')
        self.assertEqual(call_args['Runtime'], 'python3.9')
        self.assertEqual(call_args['Handler'], 'handler.handler')
        self.assertEqual(call_args['Code']['ZipFile'], zip_content)
        self.assertEqual(call_args['MemorySize'], 256)
        self.assertEqual(result, 'arn:aws:lambda:us-east-1:123456789012:function:test-func')

    def test_container_deployment(self):
        """test container-based lambda deployment"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda, LambdaConfig

        # create provider with mocked clients
        provider = self._create_provider_with_mocks()

        # mock registry manager for container builds
        provider.registry_manager.build_and_push_image = Mock(
            return_value='123456789012.dkr.ecr.us-east-1.amazonaws.com/test:latest'
        )

        # mock successful function creation
        provider._lambda_client.create_function.return_value = {
            'FunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:container-func'
        }

        # create config for container deployment
        config = LambdaConfig(
            function_name='container-func',
            runtime='',  # not used for containers
            role='test-role-arn',
            handler='',  # not used for containers
            timeout=30,
            memory_size=256
        )

        # test container deployment directly
        image_uri = '123456789012.dkr.ecr.us-east-1.amazonaws.com/test:latest'
        result = provider._create_or_update_function(config, image_uri=image_uri)

        # verify container deployment
        provider._lambda_client.create_function.assert_called_once()
        call_args = provider._lambda_client.create_function.call_args[1]
        self.assertEqual(call_args['FunctionName'], 'container-func')
        self.assertEqual(call_args['PackageType'], 'Image')
        self.assertEqual(call_args['Code']['ImageUri'], image_uri)
        self.assertEqual(result, 'arn:aws:lambda:us-east-1:123456789012:function:container-func')

    def test_create_or_update_function_conflict_handling(self):
        """test function update when it already exists"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda, LambdaConfig

        # create provider with mocked clients
        provider = self._create_provider_with_mocks()

        # simulate resource conflict (function exists)
        error_response = {'Error': {'Code': 'ResourceConflictException'}}
        provider._lambda_client.create_function.side_effect = ClientError(
            error_response, 'CreateFunction'
        )

        # mock successful update
        provider._lambda_client.update_function_code.return_value = {}
        provider._lambda_client.update_function_configuration.return_value = {
            'FunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:existing-func'
        }

        # create config
        config = LambdaConfig(
            function_name='existing-func',
            runtime='python3.9',
            role='test-role-arn',
            handler='index.handler',
            timeout=60,
            memory_size=512,
            environment={'Variables': {'ENV': 'test'}}
        )

        # test update flow
        result = provider._create_or_update_function(config, zip_content=b'updated-code')

        # verify update was called
        provider._lambda_client.update_function_code.assert_called_once_with(
            FunctionName='existing-func',
            ZipFile=b'updated-code',
            Publish=True
        )
        provider._lambda_client.update_function_configuration.assert_called_once()
        self.assertEqual(result, 'arn:aws:lambda:us-east-1:123456789012:function:existing-func')

    def test_invoke_function_with_pending_retry(self):
        """test function invocation handles pending state with retries"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()

        # simulate pending state then success
        pending_error = {
            'Error': {
                'Code': 'ResourceConflictException',
                'Message': 'The function is currently in the following state: Pending'
            }
        }

        # first two calls fail with pending, third succeeds
        mock_payload = Mock()
        mock_payload.read.return_value = b'{"result": "success"}'

        provider._lambda_client.invoke.side_effect = [
            ClientError(pending_error, 'Invoke'),
            ClientError(pending_error, 'Invoke'),
            {
                'StatusCode': 200,
                'ExecutedVersion': '$LATEST',
                'Payload': mock_payload
            }
        ]

        # invoke with mocked sleep to speed up test
        with patch('time.sleep'):
            result = provider.invoke_function('test-func', {'input': 'data'})

        # verify result
        self.assertEqual(result['StatusCode'], 200)
        self.assertEqual(result['Payload'], {'result': 'success'})
        self.assertEqual(provider._lambda_client.invoke.call_count, 3)

    def test_list_functions(self):
        """test listing all deployed functions"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()

        # mock paginator
        mock_paginator = Mock()
        provider._lambda_client.get_paginator.return_value = mock_paginator

        # mock function list
        mock_paginator.paginate.return_value = [
            {
                'Functions': [
                    {
                        'FunctionName': 'func1',
                        'FunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:func1',
                        'Runtime': 'python3.9',
                        'Handler': 'index.handler',
                        'MemorySize': 256,
                        'Timeout': 30,
                        'State': 'Active',
                        'PackageType': 'Zip'
                    },
                    {
                        'FunctionName': 'func2',
                        'FunctionArn': 'arn:aws:lambda:us-east-1:123456789012:function:func2',
                        'PackageType': 'Image',
                        'State': 'Active'
                    }
                ]
            }
        ]

        # list functions
        functions = provider.list_functions()

        # verify
        self.assertEqual(len(functions), 2)
        self.assertEqual(functions[0]['name'], 'func1')
        self.assertEqual(functions[0]['runtime'], 'python3.9')
        self.assertEqual(functions[1]['package_type'], 'Image')

    def test_delete_function(self):
        """test function deletion and cleanup"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()

        # add function to tracking
        provider._deployed_functions['test-func'] = {
            'arn': 'arn:aws:lambda:us-east-1:123456789012:function:test-func',
            'created_at': time.time()
        }

        # create a task in functions book
        task = MockTask()
        task.name = 'test-func'
        provider._functions_book['test-func'] = task

        # delete function
        provider.delete_function('test-func')

        # verify deletion
        provider._lambda_client.delete_function.assert_called_once_with(
            FunctionName='test-func'
        )
        self.assertNotIn('test-func', provider._deployed_functions)
        self.assertNotIn('test-func', provider._functions_book)

    def test_shutdown_with_auto_terminate(self):
        """test provider shutdown cleans up resources when auto_terminate is true"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()
        provider.auto_terminate = True

        # add some deployed functions
        provider._deployed_functions = {
            'func1': {'arn': 'arn1', 'created_at': time.time()},
            'func2': {'arn': 'arn2', 'created_at': time.time()}
        }

        # add active deployment to test graceful shutdown
        provider._active_deployments.add('func3')

        # mock cleanup methods
        provider._cleanup_iam_role = Mock()

        # shutdown
        with patch('time.sleep'):  # speed up the test
            provider.shutdown()

        # verify termination
        self.assertTrue(provider._terminate.is_set())
        self.assertFalse(provider.status)

        # verify functions were deleted
        self.assertEqual(provider._lambda_client.delete_function.call_count, 2)

        # verify iam cleanup was attempted
        provider._cleanup_iam_role.assert_called_once()

        # verify resource manager save was called
        self.mock_resource_manager.save_all_resources.assert_called_once()

    def test_error_handling_in_deployment(self):
        """test proper error handling during deployment failures"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()

        # simulate deployment failure
        provider._lambda_client.create_function.side_effect = Exception("Deployment failed")

        # create task
        task = MockTask()
        task.handler_code = "def handler(): pass"

        # process task
        provider._process_task(task)

        # verify task marked as failed
        self.assertEqual(task.state, 'FAILED')
        self.assertNotIn(task.name, provider._deployed_functions)

        # verify error was sent to outgoing queue
        error_msg = provider.outgoing_q.get(timeout=1)
        self.assertEqual(error_msg['state'], 'failed')
        self.assertIn('error', error_msg)

    def test_memory_normalization(self):
        """test memory values are normalized to lambda constraints"""
        from hydraa_faas.faas_manager.aws_lambda import AwsLambda

        provider = self._create_provider_with_mocks()

        # test various memory values
        test_cases = [
            (64, 128),      # below minimum
            (256, 256),     # valid value
            (15000, 10240), # above maximum
            (512.5, 512),   # float to int
        ]

        for input_memory, expected in test_cases:
            result = provider._normalize_memory(input_memory)
            self.assertEqual(result, expected)

    def _create_provider_with_mocks(self):
        """helper to create a provider with all clients mocked"""
        with patch('boto3.client'):
            from hydraa_faas.faas_manager.aws_lambda import AwsLambda

            provider = AwsLambda(
                sandbox='test_sandbox',
                manager_id='test_id',
                cred=self.mock_creds,
                asynchronous=True,
                auto_terminate=False,
                log=self.mock_logger,
                resource_manager=self.mock_resource_manager
            )

            # replace clients with mocks
            provider._lambda_client = Mock()
            provider._iam_client = Mock()
            provider._ecr_client = Mock()
            provider.registry_manager = Mock()

            # stop worker thread for cleaner testing
            provider._terminate.set()
            if provider.worker_thread and provider.worker_thread.is_alive():
                provider.worker_thread.join(timeout=1)
            provider._terminate.clear()

            return provider


if __name__ == '__main__':
    unittest.main()