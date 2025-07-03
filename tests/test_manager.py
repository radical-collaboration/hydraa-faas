"""
Unit tests for FaasManager.
"""

import unittest
import queue
import tempfile
import os
import shutil
from unittest.mock import Mock, patch, MagicMock
from hydraa import Task
from hydraa.providers.proxy import proxy
from src.hydraa_faas.faas_manager.manager import FaasManager, MetricsCollector
from src.hydraa_faas.faas_manager.aws_lambda import AwsLambda
from src.hydraa_faas.faas_manager.agent_faas import AgentFaas
from src.hydraa_faas.utils.exceptions import FaasException

class TestFaasManager(unittest.TestCase):
    """Test suite for FaasManager."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_proxy = Mock(spec=proxy)
        self.mock_proxy.loaded_providers = ['aws', 'agent']
        self.mock_proxy._load_credentials.side_effect = self._get_creds
        self.test_sandbox = tempfile.mkdtemp()
        os.makedirs(self.test_sandbox, exist_ok=True)

        # mock provider classes
        self.mock_aws_lambda_class = MagicMock(spec=AwsLambda)
        self.mock_aws_lambda_class.__name__ = 'AwsLambda'
        self.mock_aws_instance = self._create_mock_provider_instance('aws-run-id')
        self.mock_aws_lambda_class.return_value = self.mock_aws_instance

        self.mock_agent_faas_class = MagicMock(spec=AgentFaas)
        self.mock_agent_faas_class.__name__ = 'AgentFaas'
        self.mock_agent_instance = self._create_mock_provider_instance('agent-run-id')
        self.mock_agent_faas_class.return_value = self.mock_agent_instance

        self.provider_map = {
            'aws': self.mock_aws_lambda_class,
            'agent': self.mock_agent_faas_class
        }

    def tearDown(self):
        """Clean up."""
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    def _get_creds(self, provider):
        if provider == 'aws':
            return {'aws_access_key_id': 'test', 'aws_secret_access_key': 'test', 'region_name': 'us-east-1'}
        if provider == 'agent':
            return {'host': 'localhost', 'port': 8000}
        return {}

    def _create_mock_provider_instance(self, run_id):
        """Creates a mock provider instance with necessary attributes."""
        mock_instance = MagicMock()
        mock_instance.run_id = run_id
        mock_instance.incoming_q = queue.Queue()
        mock_instance.outgoing_q = queue.Queue()
        mock_instance.status = True
        mock_instance._functions_book = {}
        return mock_instance

    def test_faas_manager_initialization(self):
        """Test FaasManager initialization."""
        faas_mgr = FaasManager(self.mock_proxy, asynchronous=True, auto_terminate=True)
        self.assertEqual(faas_mgr._proxy, self.mock_proxy)
        self.assertTrue(faas_mgr.asynchronous)
        self.assertTrue(faas_mgr.auto_terminate)
        self.assertIsNotNone(faas_mgr.metrics)
        self.assertIsInstance(faas_mgr.metrics, MetricsCollector)

    def test_faas_manager_start(self):
        """Test FaasManager start method correctly initializes providers."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        # assert aws provider was initialized
        self.mock_aws_lambda_class.assert_called_once()
        self.assertIn('aws', faas_mgr._registered_managers)
        self.assertEqual(faas_mgr._registered_managers['aws']['class'], self.mock_aws_instance)

        # assert agent provider was initialized
        self.mock_agent_faas_class.assert_called_once()
        self.assertIn('agent', faas_mgr._registered_managers)
        self.assertEqual(faas_mgr._registered_managers['agent']['class'], self.mock_agent_instance)

    def test_submit_task_to_correct_provider(self):
        """Test submitting a task is routed to the correct provider queue."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        aws_task = Task(provider='aws')
        agent_task = Task(provider='agent')

        faas_mgr.submit(aws_task)
        faas_mgr.submit(agent_task)

        self.assertEqual(self.mock_aws_instance.incoming_q.get(), aws_task)
        self.assertEqual(self.mock_agent_instance.incoming_q.get(), agent_task)
        self.assertTrue(self.mock_aws_instance.incoming_q.empty())
        self.assertTrue(self.mock_agent_instance.incoming_q.empty())

    def test_submit_task_with_no_provider_defaults_to_first(self):
        """Test submitting a task with no provider defaults to the first registered one."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        default_task = Task()
        faas_mgr.submit(default_task)

        # aws should be the first provider based on the setup
        self.assertEqual(self.mock_aws_instance.incoming_q.get(), default_task)
        self.assertTrue(self.mock_agent_instance.incoming_q.empty())

    def test_submit_invalid_task_raises_error(self):
        """Test submitting an object that is not a Task raises ValueError."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)
        with self.assertRaises(ValueError):
            faas_mgr.submit("not a task")

    def test_decorator_functionality(self):
        """Test the FaaSManager decorator correctly submits the task."""
        faas_mgr = FaasManager(self.mock_proxy)
        faas_mgr.submit = Mock() # mock submit method to check if its called

        @faas_mgr(provider='aws')
        def test_function():
            return Task()

        result_task = test_function()
        self.assertEqual(result_task.provider, 'aws')
        faas_mgr.submit.assert_called_once_with(result_task)

    def test_invoke_routes_to_correct_provider(self):
        """Test invoke calls the correct provider's invoke_function."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        self.mock_aws_instance.invoke_function.return_value = {'result': 'from aws'}
        self.mock_agent_instance.invoke_function.return_value = {'result': 'from agent'}

        # invoke on aws
        response_aws = faas_mgr.invoke('my-func', provider='aws')
        self.mock_aws_instance.invoke_function.assert_called_once_with('my-func', None, 'RequestResponse')
        self.mock_agent_instance.invoke_function.assert_not_called()
        self.assertEqual(response_aws, {'result': 'from aws'})

        # invoke on agent
        response_agent = faas_mgr.invoke('my-func', provider='agent')
        self.mock_agent_instance.invoke_function.assert_called_once_with('my-func', None, 'RequestResponse')
        self.assertEqual(response_agent, {'result': 'from agent'})

    def test_invoke_with_no_provider_tries_all(self):
        """Test invoke without a provider tries all registered providers until success."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        # first provider (aws) fails, second (agent) succeeds
        self.mock_aws_instance.invoke_function.side_effect = FaasException("Not found on AWS")
        self.mock_agent_instance.invoke_function.return_value = {'result': 'found on agent'}

        response = faas_mgr.invoke('my-func')

        self.mock_aws_instance.invoke_function.assert_called_once()
        self.mock_agent_instance.invoke_function.assert_called_once()
        self.assertEqual(response, {'result': 'found on agent'})

    def test_shutdown_all_providers(self):
        """Test shutting down all providers."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        faas_mgr.shutdown()

        self.mock_aws_instance.shutdown.assert_called_once()
        self.mock_agent_instance.shutdown.assert_called_once()
        self.assertEqual(len(faas_mgr._registered_managers), 0)

    def test_shutdown_specific_provider(self):
        """Test shutting down a single, specific provider."""
        faas_mgr = FaasManager(self.mock_proxy)
        with patch.dict('src.hydraa_faas.faas_manager.manager.PROVIDER_TO_CLASS', self.provider_map):
            faas_mgr.start(self.test_sandbox)

        faas_mgr.shutdown(provider='aws')

        self.mock_aws_instance.shutdown.assert_called_once()
        self.mock_agent_instance.shutdown.assert_not_called()
        self.assertNotIn('aws', faas_mgr._registered_managers)
        self.assertIn('agent', faas_mgr._registered_managers)

if __name__ == '__main__':
    unittest.main()