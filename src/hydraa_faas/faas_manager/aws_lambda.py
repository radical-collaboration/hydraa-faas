# -*- coding: utf-8 -*-
"""AWS Lambda provider - Refactored for better HYDRA integration.

This module handles AWS Lambda deployments while following HYDRA patterns
and using existing Task attributes effectively.
"""

import json
import os
import queue
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from hydraa import Task

from ..utils.exceptions import DeploymentException, InvocationException
from ..utils.packaging import create_deployment_package
from ..utils.registry import RegistryManager, RegistryConfig, RegistryType
from ..utils.resource_manager import ResourceManager

# Constants
LAMBDA_MAX_TIMEOUT = 900  # 15 minutes
LAMBDA_MIN_MEMORY = 128
LAMBDA_MAX_MEMORY = 10240
DEFAULT_PYTHON_RUNTIME = 'python3.9'


class AWSClientPool:
    """Singleton pool for AWS clients with connection reuse."""
    _instance = None
    _clients = {}
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @lru_cache(maxsize=32)
    def get_client(self, service: str, region: str, access_key: str, secret_key: str):
        """Get or create a cached client."""
        key = f"{service}:{region}:{access_key[:8]}"

        with self._lock:
            if key not in self._clients:
                config = Config(
                    region_name=region,
                    retries={'max_attempts': 5, 'mode': 'adaptive'},
                    max_pool_connections=50  # Increase connection pool
                )

                self._clients[key] = boto3.client(
                    service,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=config
                )

            return self._clients[key]


class AwsLambda:
    """AWS Lambda provider following HYDRA patterns."""

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 cred: Dict[str, Any],
                 asynchronous: bool,
                 auto_terminate: bool,
                 log: Any,
                 resource_config: Dict[str, Any],
                 profiler: Any):
        """Initialize AWS Lambda provider.

        Args:
            sandbox: Directory for temporary files.
            manager_id: Unique manager identifier.
            cred: AWS credentials dictionary.
            asynchronous: Whether to process asynchronously.
            auto_terminate: Whether to cleanup on shutdown.
            log: HYDRA logger instance.
            resource_config: Provider configuration.
            profiler: HYDRA profiler instance.
        """
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.logger = log
        self.profiler = profiler
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_config = resource_config

        # AWS clients using connection pool
        self.region = cred['region_name']
        client_pool = AWSClientPool()

        self._lambda_client = client_pool.get_client(
            'lambda',
            self.region,
            cred['aws_access_key_id'],
            cred['aws_secret_access_key']
        )

        self._iam_client = client_pool.get_client(
            'iam',
            self.region,
            cred['aws_access_key_id'],
            cred['aws_secret_access_key']
        )

        self._ecr_client = client_pool.get_client(
            'ecr',
            self.region,
            cred['aws_access_key_id'],
            cred['aws_secret_access_key']
        )

        # Resource management
        self.resource_manager = ResourceManager(logger=self.logger)
        self.registry_manager = RegistryManager(
            logger=self.logger,
            aws_clients={'ecr': self._ecr_client}
        )

        # Function tracking
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._functions_lock = threading.RLock()

        # Processing queue for async mode
        self.incoming_q = queue.Queue()
        self._terminate = threading.Event()

        # Thread pool for parallel deployments
        self.executor = ThreadPoolExecutor(
            max_workers=10,
            thread_name_prefix="Lambda"
        )

        # Initialize resources
        self._setup_resources()

        # Start worker if async
        if self.asynchronous:
            self.worker_thread = threading.Thread(
                target=self._worker,
                name='LambdaWorker',
                daemon=True
            )
            self.worker_thread.start()

    def _setup_resources(self) -> None:
        """Set up AWS resources with HYDRA patterns."""
        self.logger.trace("Setting up AWS Lambda resources")
        self.profiler.prof('lambda_setup_start', uid=self.manager_id)

        # Create IAM role if not provided
        if not self.resource_config.get('iam_role'):
            role_name = f"hydra-lambda-role-{self.manager_id}"
            role_arn = self.resource_manager.create_aws_iam_role(
                role_name,
                self._iam_client
            )
            self.resource_config['iam_role'] = role_arn

        # Setup ECR if container support enabled
        if self.resource_config.get('enable_container_support', False):
            repo_name = f"hydra-faas-{self.manager_id}"
            repo_uri = self.resource_manager.create_aws_ecr_repository(
                repo_name,
                self._ecr_client
            )
            ecr_config = RegistryConfig(
                type=RegistryType.ECR,
                region=self.region
            )
            self.registry_manager.configure_registry('aws_ecr', ecr_config)

        self.profiler.prof('lambda_setup_end', uid=self.manager_id)
        self.logger.trace("AWS Lambda resources ready")

    def _worker(self) -> None:
        """Worker thread for async processing."""
        while not self._terminate.is_set():
            try:
                tasks = self.incoming_q.get(timeout=1)
                if isinstance(tasks, list):
                    self.deploy_batch_optimized(tasks)
                else:
                    self.deploy_function(tasks)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Lambda worker error: {e}")

    def deploy_function(self, task: Task) -> str:
        """Deploy a single function from a HYDRA Task.

        Args:
            task: HYDRA Task object.

        Returns:
            Function identifier (ARN).
        """
        self.profiler.prof('lambda_deploy_start', uid=str(task.id))

        try:
            # Extract FaaS configuration from Task
            faas_config = self._extract_faas_config(task)

            # Validate configuration
            self._validate_faas_config(faas_config, self._determine_deployment_type(task))

            # Generate unique function name
            function_name = self._generate_function_name(task)

            # Determine deployment type
            deployment_type = self._determine_deployment_type(task)

            # Create deployment package
            package_content = self._create_package(task, deployment_type)

            # Deploy to Lambda
            function_arn = self._deploy_to_lambda(
                task,
                function_name,
                package_content,
                deployment_type,
                faas_config
            )

            # Track deployment
            with self._functions_lock:
                self._functions[function_name] = {
                    'arn': function_arn,
                    'task_id': str(task.id),
                    'deployment_type': deployment_type
                }

            self.profiler.prof('lambda_deploy_end', uid=str(task.id))
            self.logger.trace(f"Deployed Lambda function: {function_name}")

            return function_name

        except Exception as e:
            self.profiler.prof('lambda_deploy_failed', uid=str(task.id))
            raise DeploymentException(f"Lambda deployment failed: {e}")

    def deploy_batch_optimized(self, tasks: List[Task]) -> List[str]:
        """Deploy multiple functions in parallel with optimizations."""
        self.profiler.prof('lambda_batch_start', uid=self.manager_id)

        # Group tasks by deployment type for optimization
        tasks_by_type = {}
        for task in tasks:
            dep_type = self._determine_deployment_type(task)
            tasks_by_type.setdefault(dep_type, []).append(task)

        results = []
        all_futures = {}

        # Process each group with type-specific optimizations
        for dep_type, task_group in tasks_by_type.items():
            if dep_type == 'zip':
                # Parallelize packaging
                with ThreadPoolExecutor(max_workers=10) as package_executor:
                    package_futures = {}
                    for task in task_group:
                        future = package_executor.submit(self._prepare_zip_deployment, task)
                        package_futures[future] = task

                    # Deploy as packages complete
                    for future in as_completed(package_futures):
                        task = package_futures[future]
                        try:
                            function_name, package_content, faas_config = future.result()
                            deploy_future = self.executor.submit(
                                self._deploy_to_lambda,
                                task, function_name, package_content, dep_type, faas_config
                            )
                            all_futures[deploy_future] = (task, function_name)
                        except Exception as e:
                            task.set_exception(e)

            elif dep_type == 'container':
                # Batch prepare container deployments
                deployment_configs = []
                for task in task_group:
                    try:
                        config = self._prepare_container_deployment(task)
                        deployment_configs.append((task, config))
                    except Exception as e:
                        task.set_exception(e)

                # Deploy containers in parallel
                for task, (function_name, image_uri, faas_config) in deployment_configs:
                    future = self.executor.submit(
                        self._deploy_to_lambda,
                        task, function_name, image_uri, dep_type, faas_config
                    )
                    all_futures[future] = (task, function_name)

        # Collect results
        for future in as_completed(all_futures):
            task, function_name = all_futures[future]
            try:
                function_arn = future.result()
                # Track deployment
                with self._functions_lock:
                    self._functions[function_name] = {
                        'arn': function_arn,
                        'task_id': str(task.id),
                        'deployment_type': self._determine_deployment_type(task)
                    }
                results.append(function_name)
                task.set_result(function_name)
            except Exception as e:
                self.logger.error(f"Failed to deploy task {task.id}: {e}")
                task.set_exception(e)

        self.profiler.prof('lambda_batch_end', uid=self.manager_id)
        return results

    def _prepare_zip_deployment(self, task: Task) -> tuple:
        """Prepare a zip deployment package."""
        faas_config = self._extract_faas_config(task)
        self._validate_faas_config(faas_config, 'zip')
        function_name = self._generate_function_name(task)
        package_content = self._create_package(task, 'zip')
        return function_name, package_content, faas_config

    def _prepare_container_deployment(self, task: Task) -> tuple:
        """Prepare a container deployment."""
        faas_config = self._extract_faas_config(task)
        self._validate_faas_config(faas_config, 'container')
        function_name = self._generate_function_name(task)
        image_uri = self._create_package(task, 'container')
        return function_name, image_uri, faas_config

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from Task's env_var."""
        config = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        config[key.replace('FAAS_', '').lower()] = value

        # Defaults
        config.setdefault('handler', 'handler.handler')
        config.setdefault('runtime', DEFAULT_PYTHON_RUNTIME)
        config.setdefault('timeout', '30')
        config.setdefault('source', '')

        return config

    def _validate_faas_config(self, config: Dict[str, Any], deployment_type: str) -> None:
        """Validate FaaS configuration before deployment."""
        # Validate handler format
        handler = config.get('handler', '')
        if deployment_type == 'zip' and (not handler or '.' not in handler):
            raise DeploymentException(f"Invalid handler format: {handler}. Expected 'module.function'")

        # Validate runtime
        runtime = config.get('runtime', '')
        valid_runtimes = ['python3.9', 'python3.10', 'python3.11', 'python3.12']
        if deployment_type == 'zip' and runtime not in valid_runtimes:
            raise DeploymentException(f"Invalid runtime: {runtime}. Valid options: {valid_runtimes}")

        # Validate timeout
        timeout = int(config.get('timeout', 30))
        if timeout < 1 or timeout > LAMBDA_MAX_TIMEOUT:
            raise DeploymentException(f"Invalid timeout: {timeout}. Must be between 1 and {LAMBDA_MAX_TIMEOUT}")

        # Validate source path for source-based deployments
        if deployment_type in ['zip', 'source-to-image']:
            source = config.get('source', '')
            if source and not os.path.exists(source):
                raise DeploymentException(f"Source path does not exist: {source}")

    def _generate_function_name(self, task: Task) -> str:
        """Generate Lambda-compatible function name."""
        base_name = task.name or f"task-{task.id}"
        # Lambda naming constraints
        clean_name = ''.join(c for c in base_name if c.isalnum() or c in '-_')
        return f"hydra-{self.manager_id}-{clean_name}"[:64]  # Lambda limit

    def _determine_deployment_type(self, task: Task) -> str:
        """Determine deployment type from Task attributes."""
        if task.image and task.image != 'python:3.9':
            return 'container'
        return 'zip'

    def _create_package(self, task: Task, deployment_type: str) -> Union[bytes, str]:
        """Create deployment package based on type."""
        faas_config = self._extract_faas_config(task)

        if deployment_type == 'container':
            # Use task.image for container deployments
            if task.image.startswith('ecr:'):
                # Pre-built image
                return task.image.replace('ecr:', '')
            else:
                # Build from source
                source_path = faas_config.get('source', '')
                if not source_path:
                    raise DeploymentException("Container deployment requires source_path")

                repo_uri = self.resource_manager.aws_resources.ecr_repository_uri
                if not repo_uri:
                    raise DeploymentException("ECR repository not configured")

                image_uri, _, _ = self.registry_manager.build_and_push_image(
                    source_path=source_path,
                    repository_uri=repo_uri,
                    image_tag=task.name
                )
                return image_uri
        else:
            # ZIP deployment
            if task.cmd and len(task.cmd) > 2:
                # Inline code from task.cmd
                code = task.cmd[2] if task.cmd[0] == 'python' and task.cmd[1] == '-c' else ''
                if code:
                    # Create temporary source file
                    source_dir = os.path.join(self.sandbox, f"task_{task.id}")
                    os.makedirs(source_dir, exist_ok=True)

                    handler_file = os.path.join(source_dir, "handler.py")
                    with open(handler_file, 'w') as f:
                        f.write(code)

                    zip_content, _, _ = create_deployment_package(source_dir)
                    return zip_content

            # External source
            source_path = faas_config.get('source', '')
            if source_path:
                zip_content, _, _ = create_deployment_package(source_path)
                return zip_content

            raise DeploymentException("No source code provided for deployment")

    def _deploy_to_lambda(self, task: Task, function_name: str,
                          package: Union[bytes, str], deployment_type: str,
                          faas_config: Dict[str, Any]) -> str:
        """Deploy package to AWS Lambda."""
        # Use Task's vcpus and memory for Lambda configuration
        memory = max(
            min(int(task.memory or 256), LAMBDA_MAX_MEMORY),
            LAMBDA_MIN_MEMORY
        )

        timeout = min(
            int(faas_config.get('timeout', 30)),
            LAMBDA_MAX_TIMEOUT
        )

        params = {
            'FunctionName': function_name,
            'Role': self.resource_config.get('iam_role'),
            'Timeout': timeout,
            'MemorySize': memory,
            'Environment': {
                'Variables': self._get_environment_variables(task)
            }
        }

        if deployment_type == 'container':
            params.update({
                'PackageType': 'Image',
                'Code': {'ImageUri': package}
            })
        else:
            params.update({
                'Runtime': faas_config.get('runtime', DEFAULT_PYTHON_RUNTIME),
                'Handler': faas_config.get('handler', 'handler.handler'),
                'Code': {'ZipFile': package}
            })

        # Add VPC config if available
        if self.resource_config.get('subnet_ids'):
            params['VpcConfig'] = {
                'SubnetIds': self.resource_config['subnet_ids'],
                'SecurityGroupIds': self.resource_config.get('security_groups', [])
            }

        try:
            response = self._lambda_client.create_function(**params)

            # Wait for function to be active
            waiter = self._lambda_client.get_waiter('function_active_v2')
            waiter.wait(FunctionName=function_name)

            return response['FunctionArn']

        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                # Update existing function
                return self._update_function(function_name, package, deployment_type, params)
            else:
                raise

    def _update_function(self, function_name: str, package: Union[bytes, str],
                         deployment_type: str, params: Dict[str, Any]) -> str:
        """Update existing Lambda function."""
        self.logger.trace(f"Updating existing function: {function_name}")

        # Update code
        if deployment_type == 'container':
            self._lambda_client.update_function_code(
                FunctionName=function_name,
                ImageUri=package
            )
        else:
            self._lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=package
            )

        # Wait for update
        waiter = self._lambda_client.get_waiter('function_updated_v2')
        waiter.wait(FunctionName=function_name)

        # Update configuration
        config_params = {
            k: v for k, v in params.items()
            if k not in ['Code', 'PackageType', 'FunctionName']
        }
        response = self._lambda_client.update_function_configuration(
            FunctionName=function_name,
            **config_params
        )

        # Wait for configuration update
        waiter = self._lambda_client.get_waiter('function_updated_v2')
        waiter.wait(FunctionName=function_name)

        return response['FunctionArn']

    def _get_environment_variables(self, task: Task) -> Dict[str, str]:
        """Extract environment variables from Task."""
        env_vars = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if not key.startswith('FAAS_'):  # Skip FaaS config
                        env_vars[key] = value

        return env_vars

    def invoke_function(self, function_name: str, payload: Any = None) -> Dict[str, Any]:
        """Invoke a Lambda function.

        Args:
            function_name: Function name or identifier.
            payload: JSON-serializable payload.

        Returns:
            Function response.
        """
        self.profiler.prof('lambda_invoke_start', uid=function_name)

        try:
            # Find full function name if needed
            full_name = None
            with self._functions_lock:
                if function_name in self._functions:
                    full_name = function_name
                else:
                    # Search by task ID or partial name
                    for fname, fdata in self._functions.items():
                        if (fdata['task_id'] == function_name or
                                function_name in fname):
                            full_name = fname
                            break

            if not full_name:
                raise InvocationException(f"Function '{function_name}' not found")

            # Invoke
            response = self._lambda_client.invoke(
                FunctionName=full_name,
                Payload=json.dumps(payload or {}),
                LogType='Tail'
            )

            # Parse response
            response_payload = json.loads(
                response['Payload'].read().decode('utf-8')
            )

            self.profiler.prof('lambda_invoke_end', uid=function_name)

            return {
                'statusCode': response['StatusCode'],
                'payload': response_payload,
                'logs': response.get('LogResult', '')
            }

        except Exception as e:
            self.profiler.prof('lambda_invoke_failed', uid=function_name)
            raise InvocationException(f"Failed to invoke '{function_name}': {e}")

    def shutdown(self) -> None:
        """Shutdown provider and cleanup resources."""
        self.logger.trace("Shutting down AWS Lambda provider")
        self.profiler.prof('lambda_shutdown_start', uid=self.manager_id)

        # Signal termination
        self._terminate.set()

        # Wait for worker
        if self.asynchronous and hasattr(self, 'worker_thread'):
            self.worker_thread.join(timeout=5)

        # Cleanup functions if auto_terminate
        if self.auto_terminate:
            with self._functions_lock:
                function_names = list(self._functions.keys())

            cleanup_futures = []
            for name in function_names:
                future = self.executor.submit(
                    self._delete_function,
                    name
                )
                cleanup_futures.append((name, future))

            for name, future in cleanup_futures:
                try:
                    future.result(timeout=30)
                    self.logger.trace(f"Deleted function: {name}")
                except Exception as e:
                    self.logger.error(f"Failed to delete {name}: {e}")

            # Cleanup AWS resources
            self.resource_manager.cleanup_aws_resources(
                self._iam_client,
                self._ecr_client
            )

        # Shutdown executor
        self.executor.shutdown(wait=True)

        self.profiler.prof('lambda_shutdown_end', uid=self.manager_id)
        self.logger.trace("AWS Lambda provider shutdown complete")

    def _delete_function(self, function_name: str) -> None:
        """Delete a Lambda function."""
        try:
            self._lambda_client.delete_function(FunctionName=function_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise