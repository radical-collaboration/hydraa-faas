import unittest
import sys
import shutil
import tempfile
import queue
import threading
import time
from unittest.mock import patch, MagicMock

# mock all dependencies
mocks = [
    'hydraa', 'hydraa.providers', 'hydraa.providers.proxy', 'hydraa.services',
    'hydraa.services.caas_manager', 'hydraa.services.caas_manager.utils',
    'hydraa.services.caas_manager.utils.misc', 'radical', 'radical.pilot',
    'radical.utils', 'boto3', 'botocore', 'botocore.exceptions', 'chi',
    'chi.lease', 'blazarclient', 'blazarclient.exception', 'aws_lambda', 'agent_faas'
]
for mock_name in mocks:
    sys.modules[mock_name] = MagicMock()

from src.hydraa_faas.faas_manager import manager

class MockTask:
    def __init__(self):
        self.name = None
        self.provider = None
        self.handler = None
        self.payload = None
        self.id = None
    
    def _verify(self):
        pass

class MockProvider:
    def __init__(self, *args, **kwargs):
        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self.run_id = f'{self.__class__.__name__}_run_123'
        self.shutdown = MagicMock()

class TestFaasManager(unittest.TestCase):

    def setUp(self):
        """set up test environment"""
        self.test_dir = tempfile.mkdtemp()
        
        # mock proxy manager
        self.mock_proxy_mgr = MagicMock()
        self.mock_proxy_mgr.loaded_providers = ['aws', 'agent', 'local']
        self.mock_proxy_mgr._load_credentials.return_value = {'key': 'secret'}
        
        # replace provider classes
        self.original_provider_to_class = manager.PROVIDER_TO_CLASS.copy()
        manager.PROVIDER_TO_CLASS['aws'] = MockProvider
        manager.PROVIDER_TO_CLASS['agent'] = MockProvider
        manager.PROVIDER_TO_CLASS['local'] = MockProvider
        manager.Task = MockTask

    def tearDown(self):
        """clean up test environment"""
        shutil.rmtree(self.test_dir)
        manager.PROVIDER_TO_CLASS = self.original_provider_to_class

    @patch('threading.Thread')
    def test_basic_lifecycle(self, mock_thread):
        """test initialization, start, and basic shutdown"""
        mock_thread.return_value = MagicMock()
        
        # initialization
        faas_manager = manager.FaasManager(
            self.mock_proxy_mgr, 
            providers=['aws', 'agent']
        )
        
        self.assertIsNotNone(faas_manager)
        self.assertEqual(faas_manager.providers, ['aws', 'agent'])
        self.assertEqual(len(faas_manager._registered_managers), 0)
        
        # start
        with patch('manager.misc.logger') as mock_logger:
            mock_logger.return_value = MagicMock()
            faas_manager.start(self.test_dir)
        
        self.assertEqual(len(faas_manager._registered_managers), 2)
        self.assertIn('aws', faas_manager._registered_managers)
        self.assertIn('agent', faas_manager._registered_managers)
        
        # shutdown
        faas_manager.shutdown()
        self.assertTrue(faas_manager._terminate.is_set())

    @patch('threading.Thread')
    def test_function_operations(self, mock_thread):
        """test deploy, invoke, and delete operations"""
        mock_thread.return_value = MagicMock()
        
        faas_manager = manager.FaasManager(
            self.mock_proxy_mgr, 
            providers=['aws']
        )
        
        with patch('manager.misc.logger') as mock_logger:
            mock_logger.return_value = MagicMock()
            faas_manager.start(self.test_dir)
        
        aws_manager = faas_manager._registered_managers['aws']
        queue_obj = aws_manager['in_q']
        
        # test deploy
        deploy_task = MockTask()
        deploy_task.provider = 'aws'
        deploy_task.handler = lambda x: x
        deploy_task.name = 'test_func'
        
        faas_manager.deploy_function(deploy_task)
        
        self.assertFalse(queue_obj.empty())
        cmd, task_sent = queue_obj.get()
        
        self.assertEqual(cmd, 'deploy')
        self.assertTrue(task_sent.name.startswith('func-'))
        self.assertIn(task_sent.name, faas_manager._functions)
        
        # test invoke
        func_name = task_sent.name
        payload = {'data': 'test'}
        faas_manager.invoke_function(func_name, payload)
        
        self.assertFalse(queue_obj.empty())
        cmd, invoke_task = queue_obj.get()
        
        self.assertEqual(cmd, 'invoke')
        self.assertEqual(invoke_task.name, func_name)
        self.assertEqual(invoke_task.payload, payload)
        
        # test delete
        faas_manager.delete_function(func_name)
        
        self.assertFalse(queue_obj.empty())
        cmd, delete_task = queue_obj.get()
        
        self.assertEqual(cmd, 'delete')
        self.assertEqual(delete_task.name, func_name)
        self.assertNotIn(func_name, faas_manager._functions)
        
        faas_manager.shutdown()

    def test_resource_cleanup(self):
        """test thread cleanup and graceful shutdown"""
        faas_manager = manager.FaasManager(
            self.mock_proxy_mgr, 
            providers=['aws', 'agent']
        )
        
        with patch('manager.misc.logger') as mock_logger:
            mock_logger.return_value = MagicMock()
            faas_manager.start(self.test_dir)
            
            # verify threads are created and running
            result_threads = [t for t in threading.enumerate() 
                            if 'FaasManagerResult' in t.name]
            self.assertGreater(len(result_threads), 0, "no result threads found")
            
            for thread in result_threads:
                self.assertTrue(thread.is_alive())
                self.assertTrue(thread.daemon)
            
            # test graceful shutdown
            faas_manager.shutdown()
            time.sleep(0.5)  # give threads time to cleanup
            
            # verify cleanup
            self.assertTrue(faas_manager._terminate.is_set())
            
            # verify threads are cleaned up
            remaining_threads = [t for t in threading.enumerate() 
                               if 'FaasManagerResult' in t.name and t.is_alive()]
            
            if remaining_threads:
                time.sleep(1.0)  # extra time for daemon threads
                remaining_threads = [t for t in threading.enumerate() 
                                   if 'FaasManagerResult' in t.name and t.is_alive()]
            
            self.assertEqual(len(remaining_threads), 0, 
                           f"threads still running: {[t.name for t in remaining_threads]}")

    def test_provider_management(self):
        """test individual and bulk provider shutdown"""
        faas_manager = manager.FaasManager(
            self.mock_proxy_mgr, 
            providers=['aws', 'agent']
        )
        
        with patch('manager.misc.logger') as mock_logger:
            mock_logger.return_value = MagicMock()
            faas_manager.start(self.test_dir)
            
            # get provider instances
            aws_provider = faas_manager._registered_managers['aws']['class']
            agent_provider = faas_manager._registered_managers['agent']['class']
            
            # test selective shutdown
            faas_manager.shutdown(provider='aws')
            aws_provider.shutdown.assert_called_once()
            agent_provider.shutdown.assert_not_called()
            
            # test full shutdown
            aws_provider.shutdown.reset_mock()
            faas_manager.shutdown()
            agent_provider.shutdown.assert_called_once()

    def test_error_handling(self):
        """test graceful error handling during shutdown"""
        faas_manager = manager.FaasManager(
            self.mock_proxy_mgr, 
            providers=['aws']
        )
        
        with patch('manager.misc.logger') as mock_logger:
            mock_logger.return_value = MagicMock()
            faas_manager.start(self.test_dir)
            
            # make provider throw exception during shutdown
            aws_provider = faas_manager._registered_managers['aws']['class']
            aws_provider.shutdown.side_effect = Exception("shutdown failed")
            
            # should handle exception gracefully
            try:
                faas_manager.shutdown()
            except Exception as e:
                self.fail(f"shutdown should handle errors gracefully: {e}")
            
            self.assertTrue(faas_manager._terminate.is_set())


if __name__ == '__main__':
    unittest.main()