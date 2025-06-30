import sys
import types
import unittest
import tempfile
import shutil
from unittest.mock import patch, MagicMock

mod_hydraa = types.ModuleType('hydraa')
class _StubTask:
    def __init__(self, *args, **kwargs): pass
mod_hydraa.Task = _StubTask
sys.modules['hydraa'] = mod_hydraa
mod_hydraa.__path__ = []


pkg_providers = types.ModuleType('hydraa.providers')
sys.modules['hydraa.providers'] = pkg_providers
pkg_providers.__path__ = []
proxy_mod = types.ModuleType('hydraa.providers.proxy')
proxy_mod.proxy = type('ProxyType', (), {})
sys.modules['hydraa.providers.proxy'] = proxy_mod

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
        aws_p = patch.object(manager, 'AwsLambda')
        agt_p = patch.object(manager, 'AgentFaas')
        self.MockAwsLambda = aws_p.start()
        self.MockAgentFaas = agt_p.start()
        self.addCleanup(aws_p.stop)
        self.addCleanup(agt_p.stop)

        self.mock_aws = MagicMock()
        self.mock_aws.run_id = 'mock-aws-run-id'
        self.mock_aws.incoming_q = MagicMock()
        self.mock_aws.outgoing_q = MagicMock()
        self.mock_aws.shutdown = MagicMock()
        self.MockAwsLambda.return_value = self.mock_aws

        self.mock_agent = MagicMock()
        self.mock_agent.run_id = 'mock-agent-run-id'
        self.mock_agent.incoming_q = MagicMock()
        self.mock_agent.outgoing_q = MagicMock()
        self.mock_agent.shutdown = MagicMock()
        self.MockAgentFaas.return_value = self.mock_agent

        manager.PROVIDER_TO_CLASS[AWS] = manager.AwsLambda
        manager.PROVIDER_TO_CLASS[AGENT] = manager.AgentFaas

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
        try:
            self.fm.shutdown()
        except Exception:
            pass
        shutil.rmtree(self.sandbox)

    def test_initialization(self):
        aws_info = self.fm._registered_managers[AWS]
        agent_info = self.fm._registered_managers[AGENT]
        self.assertEqual(aws_info['run_id'], 'mock-aws-run-id')
        self.assertEqual(agent_info['run_id'], 'mock-agent-run-id')

    def test_submit_and_invoke_defaults(self):
        t = MockTask(provider=AWS)
        self.fm.submit(t)
        self.mock_aws.incoming_q.put.assert_called_with(('deploy', t))

    def test_deploy_function_agent(self):
        t = MockTask(provider=AGENT)
        self.fm.deploy_function(t)
        self.mock_agent.incoming_q.put.assert_called_once_with(('deploy', t))

    def test_invoke_function_aws(self):
        t = MockTask(provider=AWS)
        self.fm.deploy_function(t)
        self.mock_aws.incoming_q.put.reset_mock()
        self.fm.invoke_function(t.name, {'x': 1})
        self.mock_aws.incoming_q.put.assert_called_once()
        op, task_arg = self.mock_aws.incoming_q.put.call_args[0][0]
        self.assertEqual(op, 'invoke')
        self.assertEqual(task_arg.name, t.name)
        self.assertEqual(task_arg.payload, {'x': 1})

    def test_delete_function_aws(self):
        t = MockTask(provider=AWS)
        self.fm.deploy_function(t)
        self.mock_aws.incoming_q.put.reset_mock()
        self.fm.delete_function(t.name)
        self.mock_aws.incoming_q.put.assert_called_once()
        op, task_arg = self.mock_aws.incoming_q.put.call_args[0][0]
        self.assertEqual(op, 'delete')
        self.assertEqual(task_arg.name, t.name)

    def test_list_functions_and_shutdown(self):
        t1 = MockTask(provider=AWS, name='one')
        t2 = MockTask(provider=AGENT, name='two')
        self.fm.deploy_function(t1)
        self.fm.deploy_function(t2)
        result = self.fm.list_functions()
        self.assertIn(AWS, result)
        self.assertIn(AGENT, result)
        names_aws = [f['name'] for f in result[AWS]]
        names_agent = [f['name'] for f in result[AGENT]]
        self.assertIn('one', names_aws)
        self.assertIn('two', names_agent)
        self.fm.shutdown()
        self.mock_aws.shutdown.assert_called_once()
        self.mock_agent.shutdown.assert_called_once()

    def test_delete_nonexistent_raises(self):
        with self.assertRaises(ValueError):
            self.fm.delete_function('nope')

    def test_provider_not_loaded_skipped(self):
        proxy2 = MagicMock()
        proxy2.loaded_providers = [AGENT]
        proxy2._load_credentials.return_value = {'aws_access_key_id': 'test'}
        fm2 = FaasManager(proxy_mgr=proxy2, providers=[AWS, AGENT], asynchronous=True, auto_terminate=False)
        fm2.start(self.sandbox)
        self.assertNotIn(AWS, fm2._registered_managers)
        self.assertIn(AGENT, fm2._registered_managers)
        fm2.shutdown()

if __name__ == '__main__':
    unittest.main()
