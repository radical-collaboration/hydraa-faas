"""
Unit tests for AwsLambda.
"""

import unittest
import tempfile
import os
import shutil
import json
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from hydraa import Task
from src.hydraa_faas.faas_manager.aws_lambda import AwsLambda, DeploymentException, InvocationException

class TestAwsLambda(unittest.TestCase):
    """Test suite for AwsLambda provider."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_sandbox = tempfile.mkdtemp()
        self.test_creds = {
            'aws_access_key_id': 'test_access_key',
            'aws_secret_access_key': 'test_secret_key',
            'region_name': 'us-east-1'
        }
        self.mock_logger = Mock()

        # boto3 clients mock
        self.mock_lambda_client = MagicMock()
        self.mock_iam_client = MagicMock()
        self.mock_ecr_client = MagicMock()
        self.mock_logs_client = MagicMock()

        self.boto_patcher = patch('boto3.client')
        self.mock_boto_client = self.boto_patcher.start()
        self.mock_boto_client.side_effect = self._get_mock_client

    def tearDown(self):
        """Clean up test fixtures."""
        self.boto_patcher.stop()
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    def _get_mock_client(self, service_name, **kwargs):
        """Return the correct mock client based on service name."""
        if service_name == 'lambda':
            return self.mock_lambda_client
        if service_name == 'iam':
            return self.mock_iam_client
        if service_name == 'ecr':
            return self.mock_ecr_client
        if service_name == 'logs':
            return self.mock_logs_client
        return MagicMock()

    def _create_aws_lambda_instance(self, asynchronous=False, auto_terminate=False):
        """Helper to create an instance of AwsLambda for tests."""
        # stop the worker thread from starting automatically
        with patch('threading.Thread.start'):
            instance = AwsLambda(
                sandbox=self.test_sandbox,
                manager_id='test-manager',
                cred=self.test_creds,
                asynchronous=asynchronous,
                auto_terminate=auto_terminate,
                log=self.mock_logger
            )
        return instance

    # corrected patch paths
    @patch('src.hydraa_faas.faas_manager.aws_lambda.create_deployment_package', return_value=b'zip')
    @patch('src.hydraa_faas.faas_manager.aws_lambda.validate_handler', return_value=True)
    def test_deploy_source_package_creates_new_function(self, mock_validate, mock_create_pkg):
        """Test deploying a source package for a new function."""
        # iam role does not exist, will be created
        self.mock_iam_client.get_role.side_effect = ClientError({'Error': {'Code': 'NoSuchEntity'}}, 'GetRole')
        self.mock_iam_client.create_role.return_value = {'Role': {'Arn': 'arn:iam::role'}}
        # lambda function does not exist, will be created
        self.mock_lambda_client.create_function.return_value = {'FunctionArn': 'arn:lambda::func'}

        aws_lambda = self._create_aws_lambda_instance()

        task = Task()
        task.source_path = self.test_sandbox
        task.handler_name = 'main.handler'
        task.name = 'test-func'

        result_arn = aws_lambda._deploy_source_package(task)

        self.mock_iam_client.create_role.assert_called_once()
        self.mock_lambda_client.create_function.assert_called_once()
        self.assertEqual(result_arn, 'arn:lambda::func')

    @patch('src.hydraa_faas.faas_manager.aws_lambda.create_deployment_package', return_value=b'zip_new')
    @patch('src.hydraa_faas.faas_manager.aws_lambda.validate_handler', return_value=True)
    def test_deploy_source_package_updates_existing_function(self, mock_validate, mock_create_pkg):
        """Test deploying a source package updates an existing function."""
        # iam role exists
        self.mock_iam_client.get_role.return_value = {'Role': {'Arn': 'arn:iam::role'}}
        # lambda function exists, create_function will raise ResourceConflictException
        self.mock_lambda_client.create_function.side_effect = ClientError(
            {'Error': {'Code': 'ResourceConflictException'}}, 'CreateFunction')
        self.mock_lambda_client.update_function_code.return_value = {'FunctionArn': 'arn:lambda::func_updated'}

        aws_lambda = self._create_aws_lambda_instance()

        task = Task()
        task.source_path = self.test_sandbox
        task.handler_name = 'main.handler'
        task.name = 'test-func'

        result_arn = aws_lambda._deploy_source_package(task)

        self.mock_lambda_client.update_function_code.assert_called_once()
        self.assertEqual(result_arn, 'arn:lambda::func_updated')

    @patch('src.hydraa_faas.faas_manager.aws_lambda.ECRHelper')
    def test_deploy_container_image(self, MockECRHelper):
        """Test deploying a function from a container image."""
        mock_ecr_helper_instance = MockECRHelper.return_value
        mock_ecr_helper_instance.build_and_push_image.return_value = 'my-repo/my-image:latest'
        self.mock_iam_client.get_role.return_value = {'Role': {'Arn': 'arn:iam::role'}}
        self.mock_lambda_client.create_function.return_value = {'FunctionArn': 'arn:lambda::container-func'}

        aws_lambda = self._create_aws_lambda_instance()
        # mock the ecr helper thats created in the constructor
        aws_lambda.ecr_helper = mock_ecr_helper_instance

        task = Task()
        task.source_path = self.test_sandbox
        task.name = 'test-container-func'
        task.image = 'my-repo/my-image:latest'

        # set deployment method for unit test clarity
        with patch.object(aws_lambda, '_determine_deployment_method', return_value='build_image'):
            result_arn = aws_lambda._deploy_container_image(task)

        mock_ecr_helper_instance.build_and_push_image.assert_called_once()
        self.mock_lambda_client.create_function.assert_called_with(
            FunctionName='test-container-func',
            Role='arn:iam::role',
            Code={'ImageUri': 'my-repo/my-image:latest'},
            PackageType='Image',
            Description=unittest.mock.ANY,
            Timeout=unittest.mock.ANY,
            MemorySize=unittest.mock.ANY,
            Publish=True
        )
        self.assertEqual(result_arn, 'arn:lambda::container-func')

    def test_invoke_function_success(self):
        """Test successful function invocation."""
        self.mock_lambda_client.invoke.return_value = {
            'StatusCode': 200,
            'Payload': MagicMock(read=lambda: b'{"result": "success"}')
        }
        aws_lambda = self._create_aws_lambda_instance()
        result = aws_lambda.invoke_function('test-func', {'key': 'val'})

        self.assertEqual(result['StatusCode'], 200)
        self.assertEqual(json.loads(result['Payload'])['result'], 'success')
        self.mock_lambda_client.invoke.assert_called_once_with(
            FunctionName='test-func', InvocationType='RequestResponse', Payload='{"key": "val"}'
        )

    def test_invoke_function_failure_raises_exception(self):
        """Test a failed invocation raises InvocationException."""
        self.mock_lambda_client.invoke.side_effect = ClientError({'Error': {'Code': 'ResourceNotFoundException'}},
                                                                 'Invoke')
        aws_lambda = self._create_aws_lambda_instance()
        with self.assertRaises(InvocationException):
            aws_lambda.invoke_function('non-existent-func')

    def test_safe_delete_iam_role_with_retry(self):
        """Test that IAM role deletion retries on DeleteConflict."""
        aws_lambda = self._create_aws_lambda_instance()
        aws_lambda.cleanup_retry_delay = 0.01  # speed up test
        self.mock_iam_client.list_attached_role_policies.return_value = {'AttachedPolicies': []}
        self.mock_iam_client.delete_role.side_effect = [
            ClientError({'Error': {'Code': 'DeleteConflict'}}, 'DeleteRole'),
            {}  # Success on second attempt
        ]

        result = aws_lambda._safe_delete_iam_role('test-role')
        self.assertTrue(result)
        self.assertEqual(self.mock_iam_client.delete_role.call_count, 2)

    def test_cleanup_in_order(self):
        """Test that cleanup functions are called in the correct order."""
        aws_lambda = self._create_aws_lambda_instance()

        with patch.object(aws_lambda, '_cleanup_lambda_functions') as mock_clean_lambda, \
                patch.object(aws_lambda, '_cleanup_ecr_repositories') as mock_clean_ecr, \
                patch.object(aws_lambda, '_cleanup_iam_roles') as mock_clean_iam, \
                patch.object(aws_lambda, '_cleanup_log_groups') as mock_clean_logs, \
                patch.object(aws_lambda, '_cleanup_sandbox') as mock_clean_sandbox:
            aws_lambda._cleanup_in_order()

            mock_clean_lambda.assert_called_once()
            mock_clean_ecr.assert_called_once()
            mock_clean_iam.assert_called_once()
            mock_clean_logs.assert_called_once()
            mock_clean_sandbox.assert_called_once()


if __name__ == '__main__':
    unittest.main()