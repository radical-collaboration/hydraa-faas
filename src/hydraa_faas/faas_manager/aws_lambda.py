# -*- coding: utf-8 -*-
"""AWS Lambda provider - Simplified resource provisioning.

This module handles AWS Lambda deployments with automatic resource detection
and simplified provisioning following HYDRA patterns.
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
from ..utils.packaging import create_deployment_package, validate_handler
from ..utils.registry import RegistryManager, RegistryConfig, RegistryType
from ..utils.resource_manager import ResourceManager

# Constants
LAMBDA_MAX_TIMEOUT = 900  # 15 minutes
LAMBDA_MIN_MEMORY = 128
LAMBDA_MAX_MEMORY = 10240
SUPPORTED_PYTHON_RUNTIMES = ['python3.9', 'python3.10', 'python3.11', 'python3.12']
DEFAULT_PYTHON_RUNTIME = 'python3.9'
BASE_PYTHON_IMAGES = ['python:3.9', 'python:3.10', 'python:3.11', 'python:3.12']


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
        """Get or create a cached client with higher connection pool."""
        key = f"{service}:{region}:{access_key[:8]}"

        with self._lock:
            if key not in self._clients:
                config = Config(
                    region_name=region,
                    retries={'max_attempts': 5, 'mode': 'adaptive'},
                    max_pool_connections=1000  # Increased for high concurrency
                )

                self._clients[key] = boto3.client(
                    service,
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=config
                )

            return self._clients[key]


class AwsLambda:
    """AWS Lambda provider with simplified resource provisioning."""

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 cred: Dict[str, Any],
                 asynchronous: bool,
                 auto_terminate: bool,
                 log: Any,
                 resource_config: Dict[str, Any],
                 profiler: Any,
                 deployment_workers: int = 200,
                 invocation_workers: int = 50,
                 enable_metrics: bool = True):
        """Initialize AWS Lambda provider.

        Args:
            sandbox: Directory for temporary files.
            manager_id: Unique manager identifier.
            cred: AWS credentials dictionary.
            asynchronous: Whether to process asynchronously.
            auto_terminate: Whether to cleanup on shutdown.
            log: HYDRA logger instance.
            resource_config: Provider configuration (used for VPC only).
            profiler: HYDRA profiler instance.
            deployment_workers: Max workers for deployment operations.
            invocation_workers: Max workers for invocation operations.
            enable_metrics: Whether to collect performance metrics.
        """
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.logger = log
        self.profiler = profiler
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.resource_config = resource_config
        self.enable_metrics = enable_metrics

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

        # Function tracking with single lock
        self._functions: Dict[str, Dict[str, Any]] = {}
        self._provider_lock = threading.RLock()

        # Processing queue for async mode
        self.incoming_q = queue.Queue()
        self._terminate = threading.Event()

        # Separate thread pools for deployments and invocations
        self.deployment_executor = ThreadPoolExecutor(
            max_workers=deployment_workers,
            thread_name_prefix="Lambda_Deploy"
        )

        self.invocation_executor = ThreadPoolExecutor(
            max_workers=invocation_workers,
            thread_name_prefix="Lambda_Invoke"
        )

        # IAM role (always created/reused, never user-provided)
        self._iam_role_arn = None

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
        """Set up AWS resources - only IAM role at startup."""
        self.logger.trace("Setting up AWS Lambda resources")
        if self.enable_metrics:
            self.profiler.prof('lambda_setup_start', uid=self.manager_id)

        # Always create/reuse IAM role (no user input)
        self._setup_iam_role()

        # ECR is created on-demand during deployment if needed

        if self.enable_metrics:
            self.profiler.prof('lambda_setup_end', uid=self.manager_id)
        self.logger.trace("AWS Lambda resources ready")

    def _setup_iam_role(self) -> None:
        """Create or reuse IAM role for Lambda execution."""
        # Check for existing role from previous sessions
        if self.resource_manager.aws_resources.iam_role_arn:
            # Validate it still exists
            try:
                role_name = self.resource_manager.aws_resources.iam_role_name
                self._iam_client.get_role(RoleName=role_name)
                self._iam_role_arn = self.resource_manager.aws_resources.iam_role_arn
                self.logger.trace(f"Reusing existing IAM role: {role_name}")
                return
            except ClientError:
                self.logger.trace("Previous IAM role not found, creating new one")

        # Create new role
        role_name = f"hydra-lambda-role-{self.manager_id}"
        self._iam_role_arn = self.resource_manager.create_aws_iam_role(
            role_name,
            self._iam_client
        )
        self.resource_manager.save_all_resources()

    def _create_ecr_repository(self) -> str:
        """Create ECR repository on-demand for container builds."""
        # Check if already created
        if self.resource_manager.aws_resources.ecr_repository_uri:
            repo_uri = self.resource_manager.aws_resources.ecr_repository_uri
            # Extract registry URL from repo URI
            registry_url = repo_uri.split('/')[0]

            # Configure registry manager for existing repo
            ecr_config = RegistryConfig(
                type=RegistryType.ECR,
                region=self.region,
                url=registry_url
            )
            self.registry_manager.configure_registry('aws_ecr', ecr_config)
            return repo_uri

        # Create new repository
        repo_name = f"hydra-faas-{self.manager_id}"
        repo_uri = self.resource_manager.create_aws_ecr_repository(
            repo_name,
            self._ecr_client
        )

        # Extract registry URL from repo URI
        registry_url = repo_uri.split('/')[0]

        # Configure registry manager with proper URL
        ecr_config = RegistryConfig(
            type=RegistryType.ECR,
            region=self.region,
            url=registry_url
        )
        self.registry_manager.configure_registry('aws_ecr', ecr_config)

        self.resource_manager.save_all_resources()
        return repo_uri

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
            Function identifier (name).
        """
        if self.enable_metrics:
            self.profiler.prof('lambda_deploy_start', uid=str(task.id))

        try:
            # Extract FaaS configuration from Task
            faas_config = self._extract_faas_config(task)

            # Generate unique function name
            function_name = self._generate_function_name(task)

            # Determine deployment type automatically
            deployment_type = self._determine_deployment_type(task, faas_config)
            self.logger.trace(f"Auto-detected deployment type: {deployment_type} for task {task.id}")

            # Create deployment package based on type
            package_content = self._create_package(task, deployment_type, faas_config)

            # Deploy to Lambda
            function_arn = self._deploy_to_lambda(
                task,
                function_name,
                package_content,
                deployment_type,
                faas_config
            )

            # Track deployment with thread safety
            with self._provider_lock:
                self._functions[function_name] = {
                    'arn': function_arn,
                    'task_id': str(task.id),
                    'deployment_type': deployment_type
                }

            if self.enable_metrics:
                self.profiler.prof('lambda_deploy_end', uid=str(task.id))
            self.logger.trace(f"Deployed Lambda function: {function_name}")

            return function_name

        except Exception as e:
            if self.enable_metrics:
                self.profiler.prof('lambda_deploy_failed', uid=str(task.id))
            raise DeploymentException(f"Lambda deployment failed: {e}")

    def deploy_batch_optimized(self, tasks: List[Task]) -> List[str]:
        """Deploy multiple functions in parallel with optimizations."""
        if self.enable_metrics:
            self.profiler.prof('lambda_batch_start', uid=self.manager_id)

        # Group tasks by deployment type and base image for optimization
        tasks_by_type_and_image = {}
        for task in tasks:
            faas_config = self._extract_faas_config(task)
            dep_type = self._determine_deployment_type(task, faas_config)

            # For container builds, group by base image
            if dep_type == 'container-build' and hasattr(task, 'image'):
                base_image = task.image
            else:
                base_image = 'none'

            key = (dep_type, base_image)
            tasks_by_type_and_image.setdefault(key, []).append((task, faas_config))

        results = []
        all_futures = {}

        # Process each group with type-specific optimizations
        for (dep_type, base_image), task_group in tasks_by_type_and_image.items():
            if dep_type == 'zip':
                # Parallelize packaging
                for task, faas_config in task_group:
                    future = self.deployment_executor.submit(
                        self._deploy_single_function,
                        task, faas_config, dep_type
                    )
                    all_futures[future] = task

            elif dep_type in ['container-build', 'prebuilt-image']:
                # Process container deployments - group by base image for cache efficiency
                if dep_type == 'container-build' and base_image != 'none':
                    # Build all containers with same base image in parallel
                    build_futures = []
                    for task, faas_config in task_group:
                        future = self.deployment_executor.submit(
                            self._prepare_container_deployment,
                            task, faas_config, dep_type
                        )
                        build_futures.append((future, task, faas_config))

                    # Deploy as builds complete
                    for future, task, faas_config in build_futures:
                        try:
                            function_name, package = future.result()
                            deploy_future = self.deployment_executor.submit(
                                self._deploy_to_lambda,
                                task, function_name, package, dep_type, faas_config
                            )
                            all_futures[deploy_future] = task
                        except Exception as e:
                            task.set_exception(e)
                else:
                    # Process individually
                    for task, faas_config in task_group:
                        future = self.deployment_executor.submit(
                            self._deploy_single_function,
                            task, faas_config, dep_type
                        )
                        all_futures[future] = task

        # Collect results
        for future in as_completed(all_futures):
            task = all_futures[future]
            try:
                function_name = future.result()
                results.append(function_name)
                task.set_result(function_name)
            except Exception as e:
                self.logger.error(f"Failed to deploy task {task.id}: {e}")
                task.set_exception(e)

        if self.enable_metrics:
            self.profiler.prof('lambda_batch_end', uid=self.manager_id)
        return results

    def _deploy_single_function(self, task: Task, faas_config: Dict[str, Any], dep_type: str) -> str:
        """Deploy a single function - helper for parallel deployment."""
        function_name = self._generate_function_name(task)
        package = self._create_package(task, dep_type, faas_config)

        function_arn = self._deploy_to_lambda(
            task, function_name, package, dep_type, faas_config
        )

        # Track deployment
        with self._provider_lock:
            self._functions[function_name] = {
                'arn': function_arn,
                'task_id': str(task.id),
                'deployment_type': dep_type
            }

        return function_name

    def _prepare_container_deployment(self, task: Task, faas_config: Dict[str, Any], dep_type: str) -> tuple:
        """Prepare container deployment - separate from actual deployment."""
        function_name = self._generate_function_name(task)
        package_content = self._create_package(task, dep_type, faas_config)
        return function_name, package_content

    def _extract_faas_config(self, task: Task) -> Dict[str, Any]:
        """Extract FaaS configuration from Task's env_var."""
        config = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_'):
                        config[key.replace('FAAS_', '').lower()] = value

        return config

    def _extract_granular_config(self, task: Task) -> Dict[str, Any]:
        """Extract granular Lambda configuration from env vars."""
        granular = {}

        if task.env_var:
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if key.startswith('FAAS_') and key not in ['FAAS_PROVIDER', 'FAAS_SOURCE',
                                                               'FAAS_HANDLER', 'FAAS_RUNTIME',
                                                               'FAAS_TIMEOUT', 'FAAS_REGISTRY_URI',
                                                               'FAAS_INLINE_CODE']:
                        # These are granular configs
                        config_key = key.replace('FAAS_', '').lower()
                        try:
                            # Try to parse JSON strings
                            if value.startswith('{') or value.startswith('['):
                                granular[config_key] = json.loads(value)
                            else:
                                granular[config_key] = value
                        except json.JSONDecodeError:
                            granular[config_key] = value

        return granular

    def _generate_function_name(self, task: Task) -> str:
        """Generate Lambda-compatible function name."""
        base_name = task.name or f"task-{task.id}"
        # Lambda naming constraints
        clean_name = ''.join(c for c in base_name if c.isalnum() or c in '-_')
        return f"hydra-{self.manager_id}-{clean_name}"[:64]  # Lambda limit

    def _determine_deployment_type(self, task: Task, faas_config: Dict[str, Any]) -> str:
        """Automatically determine deployment type from task attributes."""
        # Check if it's a prebuilt image (full URI)
        if hasattr(task, 'image') and task.image and self._is_full_image_uri(task.image):
            return 'prebuilt-image'

        # Get source path
        source_path = faas_config.get('source')

        # Has source + image = container build
        if source_path and hasattr(task, 'image') and task.image:
            return 'container-build'

        # Has source only = zip
        if source_path:
            return 'zip'

        # Check for inline code
        inline_code = faas_config.get('inline_code')
        if inline_code:
            # Inline code + image = container build
            if hasattr(task, 'image') and task.image:
                return 'container-build'
            # Inline code only = zip
            return 'zip'

        raise DeploymentException(
            "Cannot determine deployment type. Provide either:\n"
            "1. image with full URI (prebuilt-image)\n"
            "2. FAAS_SOURCE=/path/to/code (zip)\n"
            "3. FAAS_SOURCE + image (container-build)\n"
            "4. FAAS_INLINE_CODE='your code' (zip or container-build)"
        )

    def _is_full_image_uri(self, image: str) -> bool:
        """Check if image is a full URI (not just a base runtime)."""
        # Base Python runtimes
        if image in BASE_PYTHON_IMAGES:
            return False

        # Full URIs contain registry/repo:tag or at least a slash
        return ('/' in image or
                image.startswith(('http://', 'https://')) or
                '.dkr.ecr.' in image or
                '.amazonaws.com' in image)

    def _create_package(self, task: Task, deployment_type: str, faas_config: Dict[str, Any]) -> Union[bytes, str]:
        """Create deployment package based on type."""
        if deployment_type == 'prebuilt-image':
            # Just return the image URI, no building needed
            image_uri = task.image
            if image_uri.startswith('ecr:'):
                image_uri = image_uri.replace('ecr:', '')
            self.logger.trace(f"Using prebuilt image: {image_uri}")
            return image_uri

        elif deployment_type == 'container-build':
            # Need to build and push to registry
            source_path = faas_config.get('source')

            # Check if task specifies custom registry
            registry_uri = faas_config.get('registry_uri')
            if registry_uri:
                repo_uri = registry_uri
                self.logger.trace(f"Using custom registry: {repo_uri}")
            else:
                # Create ECR repository on-demand
                repo_uri = self._create_ecr_repository()
                self.logger.trace(f"Using auto-created ECR: {repo_uri}")

            # Use inline code if no source path
            if not source_path:
                inline_code = faas_config.get('inline_code')
                if inline_code:
                    source_path = self._create_source_from_inline_code(task, faas_config)
                else:
                    raise DeploymentException("Container build requires source path or inline code")

            if not source_path or not os.path.exists(source_path):
                raise DeploymentException(f"Source path does not exist: {source_path}")

            # Build and push image
            image_uri, _, _ = self.registry_manager.build_and_push_image(
                source_path=source_path,
                repository_uri=repo_uri,
                image_tag=self._generate_function_name(task)
            )
            return image_uri

        else:  # zip deployment
            source_path = faas_config.get('source')

            # Check for inline code
            if not source_path:
                inline_code = faas_config.get('inline_code')
                if inline_code:
                    source_path = self._create_source_from_inline_code(task, faas_config)
                else:
                    raise DeploymentException("Zip deployment requires FAAS_SOURCE path or FAAS_INLINE_CODE")

            # Validate source path exists
            if not os.path.exists(source_path):
                raise DeploymentException(f"Source path does not exist: {source_path}")

            # Validate handler exists if specified
            handler = faas_config.get('handler', 'handler.handler')
            try:
                validate_handler(source_path, handler)
            except Exception as e:
                self.logger.warning(f"Handler validation failed: {e}")
                # Continue anyway - the handler might be generated during packaging

            # Create zip package
            zip_content, _, _ = create_deployment_package(source_path)
            return zip_content

    def _create_source_from_inline_code(self, task: Task, faas_config: Dict[str, Any]) -> str:
        """Create temporary source directory from inline code."""
        inline_code = faas_config.get('inline_code')
        if not inline_code:
            raise DeploymentException("No inline code found in FAAS_INLINE_CODE")

        source_dir = os.path.join(self.sandbox, f"task_{task.id}_source")
        os.makedirs(source_dir, exist_ok=True)

        # Create handler file
        handler_file = os.path.join(source_dir, "handler.py")
        with open(handler_file, 'w') as f:
            # Ensure the code has a handler function
            if 'def handler' not in inline_code:
                # Wrap the code in a handler function
                f.write("def handler(event, context):\n")
                indented_code = '\n'.join(f"    {line}" for line in inline_code.split('\n'))
                f.write(indented_code)
                f.write("\n    return {'statusCode': 200, 'body': 'Success'}\n")
            else:
                f.write(inline_code)

        # Create Dockerfile if container build
        if self._determine_deployment_type(task, faas_config) == 'container-build':
            dockerfile = os.path.join(source_dir, "Dockerfile")
            with open(dockerfile, 'w') as f:
                f.write(f"""FROM public.ecr.aws/lambda/python:3.9
COPY handler.py ${{LAMBDA_TASK_ROOT}}
CMD ["handler.handler"]
""")

        return source_dir

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

        # Validate runtime (Python only)
        runtime = faas_config.get('runtime', DEFAULT_PYTHON_RUNTIME)
        if runtime not in SUPPORTED_PYTHON_RUNTIMES:
            raise DeploymentException(
                f"Unsupported runtime: {runtime}. "
                f"Only Python runtimes are supported: {SUPPORTED_PYTHON_RUNTIMES}"
            )

        # Extract granular configuration
        granular_config = self._extract_granular_config(task)

        params = {
            'FunctionName': function_name,
            'Role': self._iam_role_arn,
            'Timeout': timeout,
            'MemorySize': memory,
            'Environment': {
                'Variables': self._get_environment_variables(task)
            }
        }

        # Apply granular configurations
        if 'ephemeral_storage' in granular_config:
            params['EphemeralStorage'] = {'Size': int(granular_config['ephemeral_storage'])}

        if 'reserved_concurrent_executions' in granular_config:
            params['ReservedConcurrentExecutions'] = int(granular_config['reserved_concurrent_executions'])

        if 'layers' in granular_config:
            params['Layers'] = granular_config['layers'].split(',') if isinstance(granular_config['layers'], str) else \
                granular_config['layers']

        if 'dead_letter_queue' in granular_config:
            params['DeadLetterConfig'] = {'TargetArn': granular_config['dead_letter_queue']}

        if 'tracing_config' in granular_config:
            params['TracingConfig'] = {'Mode': granular_config['tracing_config']}

        if deployment_type in ['container-build', 'prebuilt-image']:
            params.update({
                'PackageType': 'Image',
                'Code': {'ImageUri': package}
            })
        else:  # zip
            params.update({
                'Runtime': runtime,
                'Handler': faas_config.get('handler', 'handler.handler'),
                'Code': {'ZipFile': package}
            })

        # Add VPC config if available from resource_config or VMs
        vpc_config = self._get_vpc_config()
        if vpc_config:
            params['VpcConfig'] = vpc_config

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

    def _get_vpc_config(self) -> Optional[Dict[str, Any]]:
        """Extract VPC configuration from resource_config (from VMs)."""
        if self.resource_config.get('subnet_ids'):
            return {
                'SubnetIds': self.resource_config['subnet_ids'],
                'SecurityGroupIds': self.resource_config.get('security_groups', [])
            }
        return None

    def _update_function(self, function_name: str, package: Union[bytes, str],
                         deployment_type: str, params: Dict[str, Any]) -> str:
        """Update existing Lambda function."""
        self.logger.trace(f"Updating existing function: {function_name}")

        # Update code
        if deployment_type in ['container-build', 'prebuilt-image']:
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

        # Use the stored user env vars (non-FAAS_*)
        if hasattr(task, '_user_env_vars'):
            for var in task._user_env_vars:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    env_vars[key] = value
        elif task.env_var:
            # Fallback if _user_env_vars not set
            for var in task.env_var:
                if isinstance(var, str) and '=' in var:
                    key, value = var.split('=', 1)
                    if not key.startswith('FAAS_'):
                        env_vars[key] = value

        return env_vars

    def invoke_function(self, function_name: str, payload: Any = None) -> Dict[str, Any]:
        """Invoke a Lambda function using the invocation thread pool.

        Args:
            function_name: Function name or identifier.
            payload: JSON-serializable payload.

        Returns:
            Function response.
        """
        # Submit to invocation thread pool for true parallelism
        future = self.invocation_executor.submit(
            self._invoke_function_internal,
            function_name,
            payload
        )

        # Wait for result (caller can use ThreadPoolExecutor for parallel calls)
        return future.result()

    def _invoke_function_internal(self, function_name: str, payload: Any = None) -> Dict[str, Any]:
        """Internal function to invoke Lambda - runs in thread pool."""
        try:
            # Fast path: check if we have the full function name
            with self._provider_lock:
                if function_name in self._functions:
                    full_name = function_name
                else:
                    # Search by task ID or partial name
                    full_name = None
                    for fname, fdata in self._functions.items():
                        if (fdata['task_id'] == function_name or
                                function_name in fname):
                            full_name = fname
                            break

            if not full_name:
                raise InvocationException(f"Function '{function_name}' not found")

            # Direct invoke without extra processing
            response = self._lambda_client.invoke(
                FunctionName=full_name,
                Payload=json.dumps(payload or {})
            )

            # Minimal response processing
            response_payload = json.loads(
                response['Payload'].read().decode('utf-8')
            )

            return {
                'statusCode': response['StatusCode'],
                'payload': response_payload
            }

        except Exception as e:
            raise InvocationException(f"Failed to invoke '{function_name}': {e}")

    def shutdown(self) -> None:
        """Shutdown provider and cleanup resources."""
        self.logger.trace("Shutting down AWS Lambda provider")
        if self.enable_metrics:
            self.profiler.prof('lambda_shutdown_start', uid=self.manager_id)

        # Signal termination
        self._terminate.set()

        # Wait for worker
        if self.asynchronous and hasattr(self, 'worker_thread'):
            self.worker_thread.join(timeout=5)

        # Cleanup functions if auto_terminate
        if self.auto_terminate:
            with self._provider_lock:
                function_names = list(self._functions.keys())

            cleanup_futures = []
            for name in function_names:
                future = self.deployment_executor.submit(
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

        # Save resource state
        self.resource_manager.save_all_resources()

        # Shutdown executors
        self.deployment_executor.shutdown(wait=True)
        self.invocation_executor.shutdown(wait=True)

        if self.enable_metrics:
            self.profiler.prof('lambda_shutdown_end', uid=self.manager_id)
        self.logger.trace("AWS Lambda provider shutdown complete")

    def _delete_function(self, function_name: str) -> None:
        """Delete a Lambda function."""
        try:
            self._lambda_client.delete_function(FunctionName=function_name)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise