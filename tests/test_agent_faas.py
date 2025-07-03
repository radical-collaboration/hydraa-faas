"""
Unit test for AgentFaas.
"""

import unittest
import tempfile
import os
import shutil
import zipfile
import base64
from unittest.mock import patch, MagicMock, AsyncMock
from hydraa import Task
from src.hydraa_faas.faas_manager.agent_faas import AgentFaas

# have to use IsolatedAsyncioTestCase for async method testing
class TestAgentFaas(unittest.IsolatedAsyncioTestCase):
    """Test suite for AgentFaas provider."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_sandbox = tempfile.mkdtemp()
        self.mock_logger = MagicMock()
        self.config = {'host': '127.0.0.1', 'port': 8888}

        # stop worker threads from starting during tests
        self.thread_patcher = patch('threading.Thread.start')
        self.thread_patcher.start()

        self.agent = AgentFaas(
            sandbox=self.test_sandbox,
            manager_id='test-manager',
            config=self.config,
            asynchronous=True,
            auto_terminate=False,
            logger=self.mock_logger
        )

        # mock the async http client for the instance
        self.mock_http_client = AsyncMock()
        self.agent.http_client = self.mock_http_client

    def tearDown(self):
        """Clean up test fixtures."""
        self.thread_patcher.stop()
        shutil.rmtree(self.test_sandbox, ignore_errors=True)

    async def test_initialization(self):
        """Test AgentFaas provider initialization."""
        self.assertEqual(self.agent.agent_url, "http://127.0.0.1:8888")
        self.assertTrue(self.agent.status)
        self.assertIn('agent-faas', self.agent.agent_sandbox)
        self.assertTrue(os.path.exists(self.agent.agent_sandbox))

    async def test_health_check_success(self):
        """Test a successful agent health check."""
        self.mock_http_client.get.return_value = MagicMock(
            status_code=200,
            json=lambda: {'status': 'healthy'}
        )
        self.mock_http_client.get.return_value.raise_for_status = MagicMock()

        is_healthy = await self.agent.health_check()
        self.assertTrue(is_healthy)
        self.mock_http_client.get.assert_called_once_with(f"{self.agent.agent_url}/health")

    async def test_health_check_failure(self):
        """Test a failed agent health check."""
        self.mock_http_client.get.side_effect = Exception("Connection failed")
        is_healthy = await self.agent.health_check()
        self.assertFalse(is_healthy)

    async def test_create_package_from_handler_code(self):
        """Test creating a zip package from inline handler code."""
        task = Task()
        task.name="test"
        task.handler_code="def handler(e, c): return 1"

        package_data = await self.agent._create_function_package(task)

        # decode and inspect the zip file
        zip_bytes = base64.b64decode(package_data)
        zip_file_path = os.path.join(self.test_sandbox, "test.zip")
        with open(zip_file_path, 'wb') as f:
            f.write(zip_bytes)

        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            self.assertIn('main.py', zf.namelist())
            self.assertEqual(zf.read('main.py').decode(), "def handler(e, c): return 1")

    async def test_create_package_from_source_directory(self):
        """Test creating a zip package from a source directory."""
        source_dir = tempfile.mkdtemp()
        with open(os.path.join(source_dir, "app.py"), "w") as f:
            f.write("import utils\n")
        os.makedirs(os.path.join(source_dir, "utils"))
        with open(os.path.join(source_dir, "utils", "helpers.py"), "w") as f:
            f.write("def helper(): pass\n")

        task = Task()
        task.name="test"
        task.source_directory=source_dir

        package_data = await self.agent._create_function_package(task)

        zip_bytes = base64.b64decode(package_data)
        zip_file_path = os.path.join(self.test_sandbox, "test.zip")
        with open(zip_file_path, 'wb') as f:
            f.write(zip_bytes)

        with zipfile.ZipFile(zip_file_path, 'r') as zf:
            self.assertIn('app.py', zf.namelist())
            self.assertIn('utils/helpers.py', zf.namelist())

    async def test_deploy_function(self):
        """Test deploying a function to the agent."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'status': 'deployed', 'name': 'test-func'}
        mock_response.raise_for_status = MagicMock()
        self.mock_http_client.post.return_value = mock_response

        task = Task()
        task.name = 'test-func'
        task.handler_code = 'pass'
        task.set_result = MagicMock()

        result = await self.agent._deploy_function(task)

        self.assertEqual(result['status'], 'deployed')
        self.mock_http_client.post.assert_called_once()
        self.assertIn('test-func', self.agent._deployed_functions)
        task.set_result.assert_called_once_with(result)

    async def test_invoke_function(self):
        """Test invoking a function on the agent."""
        mock_response = MagicMock()
        mock_response.json.return_value = {'result': 'success'}
        mock_response.raise_for_status = MagicMock()
        self.mock_http_client.post.return_value = mock_response

        result = await self.agent._invoke_function('test-func', {'key': 'value'})
        self.assertEqual(result['result'], 'success')
        self.mock_http_client.post.assert_called_once_with(
            f"{self.agent.agent_url}/invoke/test-func",
            json={'payload': {'key': 'value'}}
        )

    async def test_delete_function(self):
        """Test deleting a function from the agent."""
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        self.mock_http_client.delete.return_value = mock_response

        # pre populate the function
        self.agent._deployed_functions['test-func'] = {}

        task = Task()
        task.name = 'test-func'
        task.set_result = MagicMock()

        await self.agent._delete_function(task)
        self.mock_http_client.delete.assert_called_once_with(f"{self.agent.agent_url}/function/test-func")
        self.assertNotIn('test-func', self.agent._deployed_functions)
        task.set_result.assert_called_once()

    async def test_shutdown_with_auto_terminate(self):
        """Test shutdown cleans up functions when auto_terminate is True."""
        # setup agent for this specific test
        self.agent.auto_terminate = True
        self.agent._deployed_functions['func-to-clean'] = {}

        # mock successful deletion on the client we are watching
        self.mock_http_client.delete.return_value = MagicMock(status_code=200)

        # call the async shutdown method
        await self.agent.shutdown()

        # assert that the mocked clients delete method was called
        self.mock_http_client.delete.assert_called_once_with(
            f"{self.agent.agent_url}/function/func-to-clean"
        )
        self.assertFalse(self.agent.status)


if __name__ == '__main__':
    unittest.main()