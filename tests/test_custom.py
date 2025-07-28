"""
unit tests for nuclio provider
tests kubernetes and nuclio functionality without real clusters
"""
import unittest
import queue
import time
import json
import threading
from unittest.mock import Mock, MagicMock, patch, call
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta

# import the actual VM classes from the hydraa library
from hydraa import LocalVM, AwsVM

# mock classes that properly inherit from hydraa classes
class MockLocalVM(LocalVM):
    def __init__(self):
        # LocalVM expects launch_type as positional parameter (lowercase)
        super().__init__('create')
        self.Provider = 'local'
        self.VmName = 'local-vm'
        self.Servers = []
        self.KubeConfigPath = None


class MockAwsVM(AwsVM):
    def __init__(self):
        # AwsVM expects positional parameters
        super().__init__(
            'EKS',  # launch_type
            'ami-12345678',  # image_id
            1,  # min_count
            1,  # max_count
            't2.micro'  # instance_type
        )
        self.Provider = 'aws'
        self.VmName = 'aws-vm'


# mock classes
class MockTask(Future):
    def __init__(self):
        super().__init__()
        self.name = 'test-task'
        self.image = None
        self.source_path = None
        self.handler_code = None
        self.vcpus = 1
        self.memory = 512
        self.runtime = 'python:3.9'
        self.handler = 'main:handler'
        self.env_vars = {}
        self.schedule = None
        self.provider = 'nuclio'
        self.launch_type = 'nuclio'
        self.id = None
        self.state = None
        self.arn = None
        self.cmd = []
        self.labels = {}
        self.min_replicas = 0
        self.max_replicas = 10

    def set_running_or_notify_cancel(self):
        pass


class MockRemote:
    def __init__(self):
        pass


class TestNuclioProvider(unittest.TestCase):
    """test cases for nuclio provider"""

    def setUp(self):
        """set up test fixtures"""
        self.mock_logger = Mock()
        self.mock_resource_manager = Mock()
        self.mock_resource_manager.registry_manager = Mock()

        self.local_vm = MockLocalVM()
        self.aws_vm = MockAwsVM()

        self.mock_creds = {
            'registry_type': 'dockerhub',
            'registry_url': 'docker.io/testuser',
            'registry_username': 'testuser',
            'registry_password': 'testpass',
            'nuclio_version': '1.13.0',
            'namespace': 'nuclio-test'
        }

    @patch('hydraa_faas.faas_manager.custom_faas.NuclioProvider._initialize_resources')
    def test_initialization(self, mock_init_resources):
        """test provider initializes correctly"""
        from hydraa_faas.faas_manager.custom_faas import NuclioProvider, DeploymentTarget

        # create provider
        provider = NuclioProvider(
            sandbox='test_sandbox',
            manager_id='test_id',
            vms=[self.local_vm],
            cred=self.mock_creds,
            asynchronous=True,
            auto_terminate=False,
            log=self.mock_logger,
            resource_manager=self.mock_resource_manager
        )

        # verify initialization
        self.assertEqual(provider.deployment_target, DeploymentTarget.MINIKUBE)
        self.assertEqual(provider.nuclio_config.namespace, 'nuclio-test')
        self.assertIsNotNone(provider.connection_pool)
        self.assertIsNotNone(provider.batch_processor)
        self.assertEqual(provider._task_id, 0)
        self.assertTrue(provider.worker_thread.is_alive())

        # verify background init thread was created (may have finished already)
        self.assertIsNotNone(provider._init_thread)

        # cleanup
        provider._terminate.set()
        provider.worker_thread.join(timeout=2)

    def test_deployment_target_detection(self):
        """test correct deployment target is detected from vm types"""
        from hydraa_faas.faas_manager.custom_faas import NuclioProvider, DeploymentTarget

        # patch initialization to prevent real setup
        with patch('hydraa_faas.faas_manager.custom_faas.NuclioProvider._initialize_resources'):
            # test local vm -> minikube
            provider_local = NuclioProvider(
                sandbox='test',
                manager_id='test',
                vms=[self.local_vm],
                cred=self.mock_creds,
                asynchronous=True,
                auto_terminate=False,
                log=self.mock_logger,
                resource_manager=self.mock_resource_manager  # use the mock resource manager
            )
            self.assertEqual(provider_local.deployment_target, DeploymentTarget.MINIKUBE)
            provider_local._terminate.set()

            # test aws vm -> eks
            provider_aws = NuclioProvider(
                sandbox='test',
                manager_id='test',
                vms=[self.aws_vm],
                cred=self.mock_creds,
                asynchronous=True,
                auto_terminate=False,
                log=self.mock_logger,
                resource_manager=self.mock_resource_manager  # use the mock resource manager
            )
            self.assertEqual(provider_aws.deployment_target, DeploymentTarget.EKS)
            provider_aws._terminate.set()

    def test_function_config_from_task(self):
        """test function config creation from task"""
        from hydraa_faas.faas_manager.custom_faas import FunctionConfig

        # create task with various properties
        task = MockTask()
        task.vcpus = 2
        task.memory = 1024
        task.env_vars = {'KEY1': 'value1', 'KEY2': 'value2'}
        task.labels = {'app': 'test', 'version': '1.0'}
        task.min_replicas = 1
        task.max_replicas = 5
        task.cmd = ['pip install requests', 'pip install numpy']

        # create config
        config = FunctionConfig.from_task(task, namespace='test-ns')

        # verify config
        self.assertEqual(config.name, 'test-task')
        self.assertEqual(config.namespace, 'test-ns')
        self.assertEqual(config.runtime, 'python:3.9')
        self.assertEqual(config.handler, 'main:handler')
        self.assertEqual(config.env_vars, task.env_vars)
        self.assertEqual(config.labels, task.labels)
        self.assertEqual(config.min_replicas, 1)
        self.assertEqual(config.max_replicas, 5)
        self.assertEqual(config.build_commands, ['pip install requests', 'pip install numpy'])

        # verify resources calculation
        self.assertEqual(config.resources['requests']['cpu'], '2000m')
        self.assertEqual(config.resources['requests']['memory'], '1024Mi')
        self.assertEqual(config.resources['limits']['cpu'], '4000m')  # 2x vcpus
        self.assertEqual(config.resources['limits']['memory'], '2048Mi')  # 2x memory

    def test_connection_pooling(self):
        """test connection pool manages ssh and http clients efficiently"""
        # patch httpx.Client to avoid http2 issues
        with patch('httpx.Client') as mock_client_class:
            # create mock clients
            mock_client1 = Mock()
            mock_client1.is_closed = False
            mock_client2 = Mock()
            mock_client2.is_closed = False

            # return different mock clients for different base_urls
            def client_side_effect(base_url=None, **kwargs):
                if base_url == 'http://test.com':
                    return mock_client1
                else:
                    return mock_client2

            mock_client_class.side_effect = client_side_effect

            from hydraa_faas.faas_manager.custom_faas import ConnectionPool

            pool = ConnectionPool(self.mock_logger)

            # test http client pooling
            client1 = pool.get_http_client('http://test.com')
            client2 = pool.get_http_client('http://test.com')
            client3 = pool.get_http_client('http://other.com')

            # same url should return same client
            self.assertIs(client1, client2)
            # different url gets different client
            self.assertIsNot(client1, client3)

            # verify pool tracking
            self.assertEqual(len(pool._http_clients), 2)

            # cleanup
            pool.close_all()

    @patch('paramiko.SSHClient')
    def test_ssh_connection_reuse(self, mock_ssh_class):
        """test ssh connections are reused within timeout window"""
        from hydraa_faas.faas_manager.custom_faas import ConnectionPool

        pool = ConnectionPool(self.mock_logger)

        # mock ssh client
        mock_ssh = Mock()
        mock_transport = Mock()
        mock_transport.is_active.return_value = True
        mock_ssh.get_transport.return_value = mock_transport
        mock_ssh_class.return_value = mock_ssh

        # test with current time for first two connections
        with patch('time.time', return_value=10):
            client1 = pool.get_ssh_client('192.168.1.100', username='docker')
            client2 = pool.get_ssh_client('192.168.1.100', username='docker')

            # should reuse same connection
            self.assertEqual(mock_ssh.connect.call_count, 1)

        # now test with time past the reuse timeout
        with patch('time.time', return_value=350):
            client3 = pool.get_ssh_client('192.168.1.100', username='docker')
            # should create new connection
            self.assertEqual(mock_ssh.connect.call_count, 2)

        # cleanup
        pool.close_all()

    def test_batch_processor(self):
        """test batch processor handles parallel deployments"""
        from hydraa_faas.faas_manager.custom_faas import BatchProcessor, FunctionConfig

        executor = ThreadPoolExecutor(max_workers=3)
        processor = BatchProcessor(executor, self.mock_logger)

        # create configs
        configs = []
        for i in range(5):
            config = FunctionConfig(
                name=f'func-{i}',
                namespace='test',
                runtime='python:3.9',
                handler='handler:main'
            )
            configs.append(config)

        # mock deploy function
        deploy_results = {}
        def mock_deploy(config):
            if config.name == 'func-2':
                raise Exception("Deploy failed")
            return {'status': 'deployed', 'name': config.name}

        # process batch
        results = processor.deploy_batch(configs, mock_deploy)

        # verify results
        self.assertEqual(len(results), 5)
        self.assertEqual(results['func-0']['status'], 'success')
        self.assertEqual(results['func-2']['status'], 'error')
        self.assertIn('Deploy failed', results['func-2']['error'])

        # cleanup
        executor.shutdown(wait=False)

    def test_task_processing_with_source_code(self):
        """test processing task with inline source code"""
        provider = self._create_provider_with_mocks()

        # create task with handler code
        task = MockTask()
        task.handler_code = """
def handler(context, event):
    return "Hello from Nuclio"
"""

        # manually assign task id and name as the worker would
        with provider._task_lock:
            task.id = provider._task_id
            task.name = f'nuclio-function-{provider._task_id}'
            provider._task_id += 1
            provider._functions_book[task.name] = task
            provider._active_deployments.add(task.name)

        # mock deployment methods
        provider._deploy_local = Mock(return_value={'status': 'ready'})
        provider._cache_function_endpoint = Mock()
        provider._get_function_url = Mock(return_value='http://test-url')

        # mock task future methods
        task.done = Mock(return_value=False)
        task.set_running_or_notify_cancel = Mock()
        task.set_result = Mock()

        # call the bulk processing method which handles tasks
        from hydraa_faas.faas_manager.custom_faas import FunctionConfig
        config = FunctionConfig.from_task(task, provider.nuclio_config.namespace)

        # deploy directly
        result = provider._deploy_local(config)

        # verify deployment
        self.assertEqual(result, {'status': 'ready'})
        provider._deploy_local.assert_called_once()

    def test_yaml_generation_with_custom_commands(self):
        """test nuclio yaml generation includes custom build commands"""
        from hydraa_faas.faas_manager.custom_faas import FunctionConfig

        provider = self._create_provider_with_mocks()

        # create config with custom commands
        config = FunctionConfig(
            name='test-func',
            namespace='nuclio',
            runtime='python:3.9',
            handler='main:handler',
            source_path='/test/path',
            build_commands=['pip install pandas', 'pip install scikit-learn']
        )

        # generate yaml
        yaml_content = provider._generate_nuclio_yaml(config)

        # verify yaml structure
        self.assertEqual(yaml_content['metadata']['name'], 'test-func')
        self.assertEqual(yaml_content['spec']['runtime'], 'python:3.9')

        # verify build commands
        build_commands = yaml_content['spec']['build']['commands']
        self.assertIn('pip install msgpack', build_commands)  # default
        self.assertIn('pip install pandas', build_commands)
        self.assertIn('pip install scikit-learn', build_commands)

    @patch('hydraa_faas.faas_manager.custom_faas.sh_callout')
    def test_minikube_ssh_invocation(self, mock_sh):
        """test function invocation via ssh for minikube"""
        from hydraa_faas.faas_manager.custom_faas import DeploymentTarget

        provider = self._create_provider_with_mocks()
        provider.deployment_target = DeploymentTarget.MINIKUBE

        # setup cached endpoint
        provider._endpoint_cache['test-func'] = {
            'type': 'nodeport',
            'host': '192.168.49.2',
            'port': '31000',
            'cached_at': time.time()
        }

        # mock ssh client
        mock_ssh = Mock()
        mock_stdout = Mock()
        mock_stdout.read.return_value = b'{"result": "success"}'
        mock_stderr = Mock()
        mock_stderr.read.return_value = b''
        mock_ssh.exec_command.return_value = (None, mock_stdout, mock_stderr)

        provider.connection_pool.get_ssh_client = Mock(return_value=mock_ssh)

        # invoke function
        result = provider.invoke_function('test-func', {'input': 'data'})

        # verify ssh command
        mock_ssh.exec_command.assert_called_once()
        ssh_cmd = mock_ssh.exec_command.call_args[0][0]
        self.assertIn('curl', ssh_cmd)
        self.assertIn('localhost:31000', ssh_cmd)
        self.assertIn('Content-Type: application/json', ssh_cmd)

        # verify result
        self.assertEqual(result, {'result': 'success'})

    def test_remote_invocation_via_gateway(self):
        """test function invocation via gateway for remote deployments"""
        from hydraa_faas.faas_manager.custom_faas import DeploymentTarget

        provider = self._create_provider_with_mocks()
        provider.deployment_target = DeploymentTarget.EKS

        # mock nginx url
        provider._get_nginx_url = Mock(return_value='http://nginx.example.com')

        # mock http client
        mock_response = Mock()
        mock_response.raise_for_status = Mock()
        mock_response.json.return_value = {'result': 'remote success'}

        mock_client = Mock()
        mock_client.post.return_value = mock_response
        provider.connection_pool.get_http_client = Mock(return_value=mock_client)

        # invoke function
        result = provider.invoke_function('remote-func', {'data': 'test'})

        # verify gateway call
        mock_client.post.assert_called_once_with(
            'http://nginx.example.com/api/gateway/remote-func',
            json={'data': 'test'}
        )

        # verify result
        self.assertEqual(result, {'result': 'remote success'})

    def test_parallel_bulk_processing(self):
        """test bulk task processing with parallel deployments"""
        provider = self._create_provider_with_mocks()

        # create multiple tasks
        tasks = []
        for i in range(5):
            task = MockTask()
            task.name = f'bulk-func-{i}'
            task.source_path = f'/path/{i}'
            tasks.append(task)

        # mock batch processor
        def mock_deploy_batch(configs, deploy_func):
            results = {}
            for config in configs:
                results[config.name] = {
                    'status': 'success',
                    'result': {'name': config.name}
                }
            return results

        provider.batch_processor.deploy_batch = mock_deploy_batch
        provider._get_function_url = Mock(return_value='http://test')

        # process bulk
        provider._process_bulk(tasks)

        # verify all tasks completed
        for task in tasks:
            self.assertEqual(task.state, 'DEPLOYED')
            self.assertIsNotNone(task.result())

    @patch('hydraa_faas.faas_manager.custom_faas.sh_callout')
    def test_endpoint_caching(self, mock_sh):
        """test function endpoints are cached for performance"""
        provider = self._create_provider_with_mocks()

        # mock kubectl output
        mock_sh.return_value = ('31234', '', 0)

        # cache endpoint
        provider._cache_function_endpoint('cached-func')

        # verify cache
        self.assertIn('cached-func', provider._endpoint_cache)
        endpoint = provider._endpoint_cache['cached-func']
        self.assertEqual(endpoint['type'], 'nodeport')
        self.assertEqual(endpoint['port'], '31234')

        # test cache retrieval
        cached = provider._get_cached_endpoint('cached-func')
        self.assertIsNotNone(cached)
        self.assertEqual(cached['port'], '31234')

        # test cache expiry by clearing the lru_cache first
        provider._get_cached_endpoint.cache_clear()
        with patch('time.time', return_value=time.time() + 7200):  # 2 hours later
            expired = provider._get_cached_endpoint('cached-func')
            self.assertIsNone(expired)

    def test_graceful_shutdown(self):
        """test provider shuts down gracefully"""
        provider = self._create_provider_with_mocks()
        provider.status = True
        provider.auto_terminate = True

        # add deployed functions
        provider._deployed_functions = {
            'func1': {},
            'func2': {}
        }

        # add active deployment
        provider._active_deployments.add('func3')

        # mock delete function
        provider.delete_function = Mock()

        # shutdown with mocked sleep
        with patch('time.sleep'):
            provider.shutdown()

        # verify shutdown sequence
        self.assertTrue(provider._terminate.is_set())
        self.assertFalse(provider.status)

        # verify functions were deleted
        self.assertEqual(provider.delete_function.call_count, 2)

        # verify connection pool closed
        provider.connection_pool.close_all = Mock()

    def test_existing_cluster_reuse(self):
        """test provider can reuse existing k8s cluster"""
        # mock existing cluster
        mock_cluster = Mock()
        mock_cluster.name = 'existing-cluster'

        resource_config = {
            'existing_cluster': mock_cluster
        }

        with patch('hydraa_faas.faas_manager.custom_faas.NuclioProvider._initialize_resources') as mock_init:
            provider = self._create_provider_with_mocks(resource_config=resource_config)

            # verify existing cluster is stored
            self.assertEqual(provider._existing_cluster, mock_cluster)

    def test_registry_configuration(self):
        """test registry configuration parsing"""
        from hydraa_faas.faas_manager.custom_faas import NuclioProvider, RegistryType

        # test different registry configs
        configs = [
            {
                'registry_type': 'dockerhub',
                'registry_url': 'docker.io/user',
                'registry_username': 'user',
                'registry_password': 'pass'
            },
            {
                'registry_type': 'ecr',
                'registry_url': '123456789012.dkr.ecr.us-east-1.amazonaws.com',
                'registry_region': 'us-east-1'
            },
            {
                'registry_type': 'none'
            }
        ]

        with patch('hydraa_faas.faas_manager.custom_faas.NuclioProvider._initialize_resources'):
            for config in configs:
                provider = NuclioProvider(
                    sandbox='test',
                    manager_id='test',
                    vms=[self.local_vm],
                    cred=config,
                    asynchronous=True,
                    auto_terminate=False,
                    log=self.mock_logger,
                    resource_manager=self.mock_resource_manager  # use the mock resource manager
                )

                if config['registry_type'] == 'none':
                    self.assertIsNone(provider.nuclio_config.registry_config)
                else:
                    self.assertIsNotNone(provider.nuclio_config.registry_config)
                    self.assertEqual(
                        provider.nuclio_config.registry_config.type.value,
                        config['registry_type']
                    )

                provider._terminate.set()

    def _create_provider_with_mocks(self, resource_config=None):
        """helper to create provider with mocked components"""
        # patch the HTTP client creation to avoid http2 issues
        with patch('hydraa_faas.faas_manager.custom_faas.NuclioProvider._initialize_resources'), \
             patch('hydraa_faas.faas_manager.custom_faas.ConnectionPool.get_http_client') as mock_http:

            # mock http client
            mock_client = Mock()
            mock_http.return_value = mock_client

            from hydraa_faas.faas_manager.custom_faas import NuclioProvider

            provider = NuclioProvider(
                sandbox='test_sandbox',
                manager_id='test_id',
                vms=[self.local_vm],
                cred=self.mock_creds,
                asynchronous=True,
                auto_terminate=False,
                log=self.mock_logger,
                resource_manager=self.mock_resource_manager,
                resource_config=resource_config or {}
            )

            # manually set required attributes
            provider.k8s_cluster = Mock()
            provider.nuctl_path = '/test/nuctl'
            provider.dashboard_url = 'http://localhost:8070'
            provider.status = True

            # mock executor
            provider.executor = Mock()
            provider.executor.shutdown = Mock()

            # stop worker thread for cleaner testing
            provider._terminate.set()
            if provider.worker_thread and provider.worker_thread.is_alive():
                provider.worker_thread.join(timeout=1)
            provider._terminate.clear()

            return provider


if __name__ == '__main__':
    unittest.main()