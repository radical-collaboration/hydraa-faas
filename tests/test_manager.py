import sys
import types
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock, ANY

# ─── Stub out 'hydraa.Task' to satisfy manager import ───
mod_hydraa = types.ModuleType('hydraa')
class _StubTask:
    def __init__(self, *args, **kwargs): pass
mod_hydraa.Task = _StubTask
sys.modules['hydraa'] = mod_hydraa
mod_hydraa.__path__ = []

# ─── Stub out 'hydraa.providers.proxy.proxy' for type annotation ───
pkg_providers = types.ModuleType('hydraa.providers')
sys.modules['hydraa.providers'] = pkg_providers
pkg_providers.__path__ = []
proxy_mod = types.ModuleType('hydraa.providers.proxy')
proxy_mod.proxy = type('ProxyType', (), {})
sys.modules['hydraa.providers.proxy'] = proxy_mod

# ─── Stub out the hydraa.services.caas_manager.utils.misc dependency ───
sys.modules['hydraa.services'] = types.ModuleType('hydraa.services')
sys.modules['hydraa.services'].__path__ = []
sys.modules['hydraa.services.caas_manager'] = types.ModuleType('hydraa.services.caas_manager')
sys.modules['hydraa.services.caas_manager'].__path__ = []
sys.modules['hydraa.services.caas_manager.utils'] = types.ModuleType('hydraa.services.caas_manager.utils')
sys.modules['hydraa.services.caas_manager.utils'].__path__ = []
misc_mod = types.ModuleType('hydraa.services.caas_manager.utils.misc')
class _DummyLogger:
    def info(self, *args, **kwargs): pass
    def error(self, *args, **kwargs): pass
misc_mod.get_logger = lambda name=None: _DummyLogger()
misc_mod.logger = lambda **kwargs: _DummyLogger()
sys.modules['hydraa.services.caas_manager.utils.misc'] = misc_mod

# ─── Stub out 'hydraa.services.cost_manager.aws_cost' to satisfy aws_lambda import ───
sys.modules['hydraa.services.cost_manager'] = types.ModuleType('hydraa.services.cost_manager')
sys.modules['hydraa.services.cost_manager'].__path__ = []
cost_mod = types.ModuleType('hydraa.services.cost_manager.aws_cost')
class _DummyAwsCost:
    def __init__(self, *args, **kwargs): pass
cost_mod.AwsCost = _DummyAwsCost
sys.modules['hydraa.services.cost_manager.aws_cost'] = cost_mod

# ─── Now import the manager under test ───
from src.hydraa_faas.faas_manager import manager
from src.hydraa_faas.faas_manager.manager import FaasManager, AWS, AGENT

class MockTask:
    def __init__(self, name=None, provider=None, handler_code=None, payload=None):
        self.name = name or "mock-task"
        self.provider = provider
        self.handler_code = handler_code
        self.payload = payload

class TestFaasManager(unittest.TestCase):

    def setUp(self):
        # Patch provider classes on manager
        aws_p = patch.object(manager, 'AwsLambda')
        agt_p = patch.object(manager, 'AgentFaas')

        self.MockAwsLambda = aws_p.start()
        self.MockAgentFaas = agt_p.start()
        self.addCleanup(aws_p.stop)
        self.addCleanup(agt_p.stop)

        # Configure AwsLambda mock
        self.mock_aws = MagicMock()
        self.mock_aws.run_id     = 'mock-aws-run-id'
        self.mock_aws.incoming_q = MagicMock()
        self.mock_aws.outgoing_q = MagicMock()
        self.MockAwsLambda.return_value = self.mock_aws

        # Configure AgentFaas mock
        self.mock_agent = MagicMock()
        self.mock_agent.run_id     = 'mock-agent-run-id'
        self.mock_agent.incoming_q = MagicMock()
        self.mock_agent.outgoing_q = MagicMock()
        self.MockAgentFaas.return_value = self.mock_agent

        # Override provider mapping
        manager.PROVIDER_TO_CLASS[AWS]   = manager.AwsLambda
        manager.PROVIDER_TO_CLASS[AGENT] = manager.AgentFaas

        # Initialize and start
        self.sandbox = tempfile.mkdtemp()
        self.proxy = MagicMock()
        self.proxy.loaded_providers = [AWS, AGENT]
        self.proxy._load_credentials.return_value = {'aws_access_key_id': 'test'}

        self.fm = FaasManager(
            proxy_mgr=self.proxy,
            providers=[AWS, AGENT],
            asynchronous=True,
            auto_terminate=False
        )
        self.fm.start(self.sandbox)

    def tearDown(self):
        self.fm.shutdown()
        shutil.rmtree(self.sandbox)

    def test_initialization(self):
        aws_info   = self.fm._registered_managers[AWS]
        agent_info = self.fm._registered_managers[AGENT]
        self.assertEqual(aws_info['run_id'],   'mock-aws-run-id')
        self.assertEqual(agent_info['run_id'], 'mock-agent-run-id')

    def test_deploy_function_agent(self):
        t = MockTask(provider=AGENT)
        self.fm.deploy_function(t)
        self.mock_agent.incoming_q.put.assert_called_once_with(('deploy', t))

    def test_invoke_function_aws(self):
        t = MockTask(provider=AWS)
        self.fm.deploy_function(t)
        # reset the initial deploy call
        self.mock_aws.incoming_q.put.reset_mock()
        self.fm.invoke_function(t.name, {'x': 1})
        self.mock_aws.incoming_q.put.assert_called_once()
        call_type, call_task = self.mock_aws.incoming_q.put.call_args[0][0]
        self.assertEqual(call_type, 'invoke')
        self.assertEqual(call_task.name,   t.name)
        self.assertEqual(call_task.payload, {'x': 1})

    def test_delete_function_aws(self):
        t = MockTask(provider=AWS)
        self.fm.deploy_function(t)
        # reset the initial deploy call
        self.mock_aws.incoming_q.put.reset_mock()
        self.fm.delete_function(t.name)
        self.mock_aws.incoming_q.put.assert_called_once()
        call_type, call_task = self.mock_aws.incoming_q.put.call_args[0][0]
        self.assertEqual(call_type, 'delete')
        self.assertEqual(call_task.name, t.name)

if __name__ == '__main__':
    unittest.main()
