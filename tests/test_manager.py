import unittest
import os
import shutil
import tempfile
import uuid
from unittest.mock import Mock, patch, MagicMock

# Mock external dependencies before imports
mock_ru = Mock()
mock_proxy = Mock()
mock_misc = Mock()

modules = {
    'radical.utils': mock_ru,
    'hydraa.providers.proxy': mock_proxy,
    'hydraa.services.caas_manager.utils': mock_misc,
}

with patch.dict('sys.modules', modules):
    from faas_manager import FaasManager, AWS, AGENT
    from aws_lambda_faas import AwsLambda
    from agent_faas import AgentFaas

# Mock the hydraa Task class
class MockTask:
    def __init__(self, name=None, provider=None, handler_code=None, payload=None):
        self.name = name or f"task-{uuid.uuid4().hex[:4]}"
        self.provider = provider
        self.handler_code = handler_code
        self.payload = payload

class TestFaasManager(unittest.TestCase):

    def setUp(self):
        self.sandbox = tempfile.mkdtemp()
        
        # Mock the proxy manager
        self.proxy_mgr = Mock()
        self.proxy_mgr.loaded_providers = [AWS]
        self.proxy_mgr._load_credentials.return_value = {'aws_access_key_id': 'test'}

        # Patch the provider classes
        self.aws_faas_patch = patch('faas_manager.AwsLambda', spec=AwsLambda)
        self.agent_faas_patch = patch('faas_manager.AgentFaas', spec=AgentFaas)
        self.MockAwsFaas = self.aws_faas_patch.start()
        self.MockAgentFaas = self.agent_faas_patch.start()
        
        # Mock instances of providers
        self.mock_aws_instance = self.MockAwsFaas.return_value
        self.mock_aws_instance.run_id = 'aws-run-id'
        self.mock_aws_instance.incoming_q = Mock()
        self.mock_aws_instance.outgoing_q = Mock()
        
        self.mock_agent_instance = self.MockAgentFaas.return_value
        self.mock_agent_instance.run_id = 'agent-run-id'
        self.mock_agent_instance.incoming_q = Mock()
        self.mock_agent_instance.outgoing_q = Mock()

        self.faas_manager = FaasManager(
            proxy_mgr=self.proxy_mgr,
            providers=[AWS, AGENT],
            asynchronous=True,
            auto_terminate=False
        )

    def tearDown(self):
        self.aws_faas_patch.stop()
        self.agent_faas_patch.stop()
        self.faas_manager.shutdown()
        shutil.rmtree(self.sandbox)

    def test_initialization(self):
        self.assertIn(AWS, self.faas_manager.providers)
        self.assertIn(AGENT, self.faas_manager.providers)
        self.assertIsInstance(self.faas_manager, FaasManager)

    def test_start(self):
        self.faas_manager.start(self.sandbox)
        
        # Verify AWS provider initialization
        self.proxy_mgr._load_credentials.assert_called_with(AWS)
        self.MockAwsFaas.assert_called_once()
        self.assertIn(AWS, self.faas_manager._registered_managers)
        self.assertEqual(self.faas_manager.AwsFaas, self.mock_aws_instance)
        
        # Verify Agent provider initialization
        self.MockAgentFaas.assert_called_once()
        self.assertIn(AGENT, self.faas_manager._registered_managers)
        self.assertEqual(self.faas_manager.AgentFaas, self.mock_agent_instance)
        
        self.assertIsNotNone(self.faas_manager.logger)

    def test_deploy_function(self):
        self.faas_manager.start(self.sandbox)
        task = MockTask(provider=AGENT, handler_code="def f(): pass")
        
        self.faas_manager.deploy_function(task)
        
        self.assertIn(task.name, self.faas_manager._functions)
        self.assertEqual(self.faas_manager._functions[task.name]['provider'], AGENT)
        self.mock_agent_instance.incoming_q.put.assert_called_with(('deploy', task))

    def test_invoke_function(self):
        self.faas_manager.start(self.sandbox)
        func_name = 'my-aws-func'
        payload = {'x': 1}
        
        # First, mock a deployed function
        self.faas_manager._functions[func_name] = {'provider': AWS, 'deployed': True, 'task': Mock()}
        
        self.faas_manager.invoke_function(func_name, payload)
        
        # Check that the invocation task was put on the correct queue
        self.mock_aws_instance.incoming_q.put.assert_called_once()
        call_args = self.mock_aws_instance.incoming_q.put.call_args[0]
        self.assertEqual(call_args[0], 'invoke')
        self.assertEqual(call_args[1].name, func_name)
        self.assertEqual(call_args[1].payload, payload)
        
    def test_delete_function(self):
        self.faas_manager.start(self.sandbox)
        func_name = 'my-aws-func'
        # First, mock a deployed function
        self.faas_manager._functions[func_name] = {'provider': AWS, 'deployed': True, 'task': Mock()}
        self.faas_manager.delete_function(func_name)
        self.assertNotIn(func_name,self.faas_manager._functions)
    
    def test_submit_deployment_task(self):
        self.faas_manager.start(self.sandbox)
        deploy_task = MockTask(provider=AGENT, handler_code="def f(): pass")
        self.faas_manager.submit(deploy_task)
        self.mock_agent_instance.incoming_q.put.assert_called_with(('deploy', deploy_task))

    def test_submit_invocation_task(self):
        self.faas_manager.start(self.sandbox)
        func_name = 'my-aws-func'
        # First, mock a deployed function so invocation is possible
        self.faas_manager._functions[func_name] = {'provider': AWS, 'deployed': True, 'task': Mock()}
        invoke_task = MockTask(name=func_name, payload={'data':1})
        self.faas_manager.submit(invoke_task)
        self.mock_aws_instance.incoming_q.put.assert_called_once()
        
    def test_shutdown(self):
        self.faas_manager.start(self.sandbox)
        self.faas_manager.shutdown()
        self.mock_aws_instance.shutdown.assert_called_once()
        self.mock_agent_instance.shutdown.assert_called_once()

if __name__ == '__main__':
    unittest.main()
