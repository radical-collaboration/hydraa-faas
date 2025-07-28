"""
AWS Lambda Provider - Production-ready version with improved state management
Handles Lambda function deployments with clean error handling and minimal complexity
"""

import os
import json
import time
import queue
import threading
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

# import from hydraa
from hydraa import Task
from hydraa.services.caas_manager.utils.misc import generate_id

# import our utilities
from ..utils.exceptions import DeploymentException, InvocationException
from ..utils.packaging import create_deployment_package
from ..utils.resource_manager import ResourceManager
from ..utils.registry import RegistryManager, RegistryConfig, RegistryType

# constants
LAMBDA_MAX_TIMEOUT = 900  # 15 minutes
LAMBDA_MIN_MEMORY = 128
LAMBDA_MAX_MEMORY = 10240
LAMBDA_MAX_ZIP_SIZE = 50 * 1024 * 1024  # 50mb
DEFAULT_PYTHON_RUNTIME = 'python3.9'

# retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1
RETRY_BACKOFF = 2

# bulk processing configuration following hydraa pattern with strings
MAX_BULK_SIZE = os.environ.get('FAAS_MAX_BULK_SIZE', '10')
MAX_BULK_TIME = os.environ.get('FAAS_MAX_BULK_TIME', '2')
MIN_BULK_TIME = os.environ.get('FAAS_MIN_BULK_TIME', '0.1')

# task states
TASK_STATE_PENDING = 'PENDING'
TASK_STATE_BUILDING = 'BUILDING'
TASK_STATE_DEPLOYING = 'DEPLOYING'
TASK_STATE_DEPLOYED = 'DEPLOYED'
TASK_STATE_FAILED = 'FAILED'

# graceful shutdown timeout
GRACEFUL_SHUTDOWN_TIMEOUT = int(os.environ.get('LAMBDA_GRACEFUL_SHUTDOWN_TIMEOUT', '30'))


def retry_on_error(max_attempts=MAX_RETRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    """Decorator for exponential backoff retry logic"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        # dont retry on certain errors
                        error_code = e.response['Error']['Code']
                        if error_code in ['ResourceConflictException', 'InvalidParameterValueException']:
                            raise

                        wait_time = delay * (backoff ** attempt)
                        time.sleep(wait_time)
                    else:
                        raise
            raise last_exception
        return wrapper
    return decorator


@dataclass
class LambdaConfig:
    """Lambda function configuration"""
    function_name: str
    runtime: str
    role: str
    handler: str
    timeout: int
    memory_size: int
    environment: Optional[Dict[str, str]] = None
    vpc_config: Optional[Dict[str, Any]] = None
    layers: Optional[List[str]] = None
    tags: Optional[Dict[str, str]] = None


class AwsLambda:
    """
    AWS Lambda provider for FaaS Manager.

    Simplified implementation focusing on core Lambda operations
    with clean error handling and resource management.
    """

    def __init__(self,
                 sandbox: str,
                 manager_id: str,
                 cred: dict,
                 asynchronous: bool,
                 auto_terminate: bool,
                 log,
                 resource_manager: Optional[ResourceManager] = None,
                 resource_config: Optional[Dict[str, Any]] = None):
        """Initialize AWS Lambda provider"""
        self.sandbox = sandbox
        self.manager_id = manager_id
        self.logger = log
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self.status = False

        # validate credentials
        self._validate_credentials(cred)
        self.region = cred['region_name']

        # resource manager
        self.resource_manager = resource_manager or ResourceManager()

        # registry manager integration through resource manager
        self.registry_manager = self.resource_manager.registry_manager if hasattr(
            self.resource_manager, 'registry_manager'
        ) else RegistryManager(self.logger)

        # generate unique run id
        self.run_id = generate_id(prefix='lambda', length=8)

        # aws clients with connection pooling
        config = Config(
            region_name=self.region,
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50
        )

        self._lambda_client = boto3.client(
            'lambda',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            config=config
        )

        self._iam_client = boto3.client(
            'iam',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            config=config
        )

        self._ecr_client = boto3.client(
            'ecr',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            config=config
        )

        # configure registry manager with ecr client
        self.registry_manager._aws_clients = {'ecr': self._ecr_client}

        # task tracking with thread safe state management
        self._task_id = 0
        self._task_lock = threading.Lock()
        self._deployed_functions = OrderedDict()
        self._functions_book = OrderedDict()
        self._active_deployments = set()

        # queue management following hydraa pattern
        self.incoming_q = queue.Queue()
        self.outgoing_q = queue.Queue()
        self.internal_q = queue.Queue()
        self._terminate = threading.Event()
        self._shutdown_complete = threading.Event()

        # thread pool for parallel operations
        self.executor = ThreadPoolExecutor(max_workers=5)

        # setup resources if configured
        if cred.get('auto_setup_resources', True):
            self._setup_resources()

        # start worker thread
        self.worker_thread = threading.Thread(
            target=self._get_work,
            name='LambdaWorker',
            daemon=True
        )

        # check if thread is already started following hydraa pattern
        if not self.worker_thread.is_alive():
            self.worker_thread.start()

        self.status = True
        self.logger.info(f"AWS Lambda provider initialized: {self.manager_id}")

    def _validate_credentials(self, cred: dict):
        """Validate AWS credentials"""
        required = ['aws_access_key_id', 'aws_secret_access_key', 'region_name']
        missing = [field for field in required if not cred.get(field)]
        if missing:
            raise ValueError(f"Missing required AWS credentials: {missing}")

    def _setup_resources(self):
        """Setup or load AWS resources"""
        try:
            # try to load existing resources first
            if self.resource_manager.aws_resources.iam_role_arn:
                self.logger.info("Using existing AWS resources")
                return

            # create new resources
            self.logger.info("Setting up AWS resources")

            role_name = generate_id(prefix='faas-lambda-role', length=8)
            repo_name = generate_id(prefix='faas-functions', length=8)

            resources = self.resource_manager.setup_aws_resources(
                iam_client=self._iam_client,
                ecr_client=self._ecr_client,
                region=self.region,
                role_name=role_name,
                repo_name=repo_name,
                reuse_existing=True
            )

            self.logger.info(f"AWS resources ready: IAM role={resources.iam_role_name}, "
                           f"ECR repo={resources.ecr_repository_name}")

        except Exception as e:
            self.logger.warning(f"Resource setup error: {e}. Will create resources on demand.")

    def _get_work(self):
        """Worker thread to process deployment requests following hydraa bulk pattern"""
        bulk = list()
        # convert strings to proper types following hydraa pattern
        max_bulk_size = int(MAX_BULK_SIZE)
        max_bulk_time = float(MAX_BULK_TIME)
        min_bulk_time = float(MIN_BULK_TIME)

        while not self._terminate.is_set():
            now = time.time()  # time of last submission
            timeout_start = now

            # collect tasks for min bulk time
            while time.time() - now < max_bulk_time:
                try:
                    # add absolute timeout to prevent infinite waiting
                    remaining_time = max(0.1, min_bulk_time - (time.time() - timeout_start))
                    task = self.incoming_q.get(block=True, timeout=remaining_time)
                except queue.Empty:
                    task = None

                if task:
                    bulk.append(task)

                if len(bulk) >= max_bulk_size:
                    break

            if bulk:
                # use lock pattern consistent with hydraa managers
                with self._task_lock:
                    self._process_bulk(bulk)
                bulk = list()

        # signal that worker thread has completed
        self.logger.info("Lambda worker thread shutting down")

    def _process_bulk(self, tasks: List[Task]):
        """Process a bulk of tasks"""
        for task in tasks:
            try:
                self._process_task(task)
            except Exception as e:
                self.logger.error(f"Failed to process task: {e}")
                with self._task_lock:
                    task.state = TASK_STATE_FAILED
                    if not task.done():
                        task.set_exception(e)

    def _process_task(self, task: Task):
        """Process a single deployment task with proper state management"""
        try:
            # assign task id and name with thread safety
            with self._task_lock:
                task.id = self._task_id
                task.name = f'lambda-function-{self._task_id}'
                self._task_id += 1
                self._functions_book[task.name] = task
                self._active_deployments.add(task.name)

            # update task state
            task.state = TASK_STATE_PENDING
            self.logger.info(f"Deploying Lambda function: {task.name}")

            # set running state before deployment
            task.state = TASK_STATE_BUILDING
            if not task.done():
                task.set_running_or_notify_cancel()

            # deploy based on task type
            task.state = TASK_STATE_DEPLOYING
            if hasattr(task, 'image') and task.image:
                function_arn = self._deploy_container_function(task)
            else:
                function_arn = self._deploy_zip_function(task)

            # update tracking with thread safety
            with self._task_lock:
                task.arn = function_arn
                self._deployed_functions[task.name] = {
                    'arn': function_arn,
                    'created_at': time.time()
                }
                self._active_deployments.discard(task.name)

            # complete the task
            task.state = TASK_STATE_DEPLOYED
            result = {
                'function_name': task.name,
                'function_arn': function_arn,
                'deployed_at': time.time()
            }

            # set result only if task hasnt been cancelled
            if not task.done():
                task.set_result(result)

            # send status update
            self.outgoing_q.put({
                'function_name': task.name,
                'state': 'deployed',
                'message': f'Function {task.name} deployed successfully'
            })
            self.logger.info(f'Function {task.name} deployed: {function_arn}')

        except Exception as e:
            self.logger.error(f'Failed to deploy function: {e}')
            with self._task_lock:
                self._active_deployments.discard(task.name)
                task.state = TASK_STATE_FAILED
                if not task.done():
                    task.set_exception(e)

            self.outgoing_q.put({
                'function_name': task.name,
                'state': 'failed',
                'error': str(e)
            })

    @retry_on_error()
    def _deploy_zip_function(self, task: Task) -> str:
        """Deploy Lambda function from ZIP package"""
        self.logger.info(f'Deploying {task.name} from zip package')

        # create deployment package
        if hasattr(task, 'source_path') and task.source_path:
            zip_content = create_deployment_package(task.source_path)
        elif hasattr(task, 'handler_code') and task.handler_code:
            zip_content = self._create_inline_package(task)
        else:
            raise ValueError("Task must specify source_path or handler_code")

        # validate package size
        if len(zip_content) > LAMBDA_MAX_ZIP_SIZE:
            raise ValueError(f"Deployment package too large: {len(zip_content)} bytes")

        # get or create iam role
        role_arn = self._ensure_iam_role()

        # prepare function configuration
        config = LambdaConfig(
            function_name=task.name,
            runtime=getattr(task, 'runtime', DEFAULT_PYTHON_RUNTIME),
            role=role_arn,
            handler=getattr(task, 'handler', 'handler.handler'),
            timeout=min(getattr(task, 'timeout', 30), LAMBDA_MAX_TIMEOUT),
            memory_size=self._normalize_memory(task.memory),
            environment={'Variables': getattr(task, 'env_vars', {})},
            vpc_config=getattr(task, 'vpc_config', None),
            layers=getattr(task, 'layers', [])[:5],  # max 5 layers
            tags=getattr(task, 'tags', {})
        )

        # create or update function
        return self._create_or_update_function(config, zip_content=zip_content)

    @retry_on_error()
    def _deploy_container_function(self, task: Task) -> str:
        """Deploy Lambda function from container image"""
        self.logger.info(f'Deploying {task.name} from container image')

        # get or create iam role
        role_arn = self._ensure_iam_role()

        # handle image uri
        if hasattr(task, 'build_image') and task.build_image:
            image_uri = self._build_and_push_image(task)
        else:
            image_uri = task.image

        # prepare function configuration
        config = LambdaConfig(
            function_name=task.name,
            runtime='',  # not used for container images
            role=role_arn,
            handler='',  # not used for container images
            timeout=min(getattr(task, 'timeout', 30), LAMBDA_MAX_TIMEOUT),
            memory_size=self._normalize_memory(task.memory),
            environment={'Variables': getattr(task, 'env_vars', {})},
            vpc_config=getattr(task, 'vpc_config', None),
            tags=getattr(task, 'tags', {})
        )

        # create or update function
        return self._create_or_update_function(config, image_uri=image_uri)

    @retry_on_error()
    def _create_or_update_function(self, config: LambdaConfig,
                                   zip_content: bytes = None,
                                   image_uri: str = None) -> str:
        """Create or update Lambda function"""
        try:
            # try to create function
            params = {
                'FunctionName': config.function_name,
                'Role': config.role,
                'Timeout': config.timeout,
                'MemorySize': config.memory_size,
                'Publish': True
            }

            if zip_content:
                params.update({
                    'Runtime': config.runtime,
                    'Handler': config.handler,
                    'Code': {'ZipFile': zip_content}
                })
            else:
                params.update({
                    'Code': {'ImageUri': image_uri},
                    'PackageType': 'Image'
                })

            # add optional parameters
            if config.environment:
                params['Environment'] = config.environment
            if config.vpc_config:
                params['VpcConfig'] = config.vpc_config
            if config.layers:
                params['Layers'] = config.layers
            if config.tags:
                params['Tags'] = config.tags

            response = self._lambda_client.create_function(**params)
            return response['FunctionArn']

        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                # function exists so update it
                self.logger.info(f"Function {config.function_name} exists, updating...")

                # update code
                if zip_content:
                    self._lambda_client.update_function_code(
                        FunctionName=config.function_name,
                        ZipFile=zip_content,
                        Publish=True
                    )
                else:
                    self._lambda_client.update_function_code(
                        FunctionName=config.function_name,
                        ImageUri=image_uri,
                        Publish=True
                    )

                # update configuration
                update_params = {
                    'FunctionName': config.function_name,
                    'Role': config.role,
                    'Timeout': config.timeout,
                    'MemorySize': config.memory_size
                }

                if config.runtime:  # only for zip functions
                    update_params['Runtime'] = config.runtime
                    update_params['Handler'] = config.handler

                if config.environment:
                    update_params['Environment'] = config.environment
                if config.vpc_config:
                    update_params['VpcConfig'] = config.vpc_config
                if config.layers:
                    update_params['Layers'] = config.layers

                response = self._lambda_client.update_function_configuration(**update_params)
                return response['FunctionArn']
            else:
                raise

    def _ensure_iam_role(self) -> str:
        """Ensure IAM role exists for Lambda execution"""
        # check if we have a saved role
        if self.resource_manager.aws_resources.iam_role_arn:
            return self.resource_manager.aws_resources.iam_role_arn

        # create new role
        role_name = generate_id(prefix='faas-lambda-role', length=8)
        return self.resource_manager.create_aws_iam_role(
            role_name=role_name,
            iam_client=self._iam_client,
            reuse_existing=True
        )

    def _build_and_push_image(self, task: Task) -> str:
        """Build and push container image to ECR"""
        # ensure ecr repository exists
        repo_name = f"faas-{task.name}"
        if self.resource_manager.aws_resources.ecr_repository_uri:
            repo_uri = self.resource_manager.aws_resources.ecr_repository_uri
        else:
            repo_uri = self.resource_manager.create_aws_ecr_repository(
                repo_name=repo_name,
                ecr_client=self._ecr_client,
                region=self.region,
                reuse_existing=True
            )

        # configure ecr registry
        ecr_config = RegistryConfig(
            type=RegistryType.ECR,
            region=self.region
        )
        self.registry_manager.configure_registry('ecr', ecr_config)

        # build and push image
        image_tag = f"{task.name}-{int(time.time())}"
        return self.registry_manager.build_and_push_image(
            source_path=task.source_path,
            repository_uri=repo_uri,
            image_tag=image_tag
        )

    def _create_inline_package(self, task: Task) -> bytes:
        """Create deployment package from inline code"""
        import tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # write handler code
            handler_file = Path(tmpdir) / "handler.py"
            handler_file.write_text(task.handler_code)

            # create deployment package
            return create_deployment_package(tmpdir)

    def _normalize_memory(self, memory: int) -> int:
        """Normalize memory to Lambda constraints"""
        memory = max(memory, LAMBDA_MIN_MEMORY)
        memory = min(memory, LAMBDA_MAX_MEMORY)
        return int(memory)

    @retry_on_error()
    def invoke_function(self, function_name: str, payload: Any = None,
                        invocation_type: str = 'RequestResponse') -> Dict[str, Any]:
        """Invoke Lambda function with smart retry for Pending state"""
        # prepare payload
        payload_json = json.dumps(payload) if payload else '{}'

        # retry configuration for pending state
        max_attempts = 10
        base_delay = 1  # start with 1 second
        max_delay = 10  # cap at 10 seconds

        last_exception = None

        for attempt in range(max_attempts):
            try:
                # try to invoke the function
                response = self._lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType=invocation_type,
                    Payload=payload_json
                )

                # parse response
                result = {
                    'StatusCode': response['StatusCode'],
                    'ExecutedVersion': response.get('ExecutedVersion'),
                    'Payload': response['Payload'].read().decode('utf-8') if 'Payload' in response else None
                }

                if result['Payload']:
                    try:
                        result['Payload'] = json.loads(result['Payload'])
                    except json.JSONDecodeError:
                        pass  # keep as string

                self.logger.info(f'Invoked function {function_name} successfully')
                return result

            except ClientError as e:
                error_code = e.response['Error']['Code']
                error_message = e.response['Error']['Message']

                # check if its a pending state error
                if (error_code == 'ResourceConflictException' and
                        'The function is currently in the following state: Pending' in error_message):

                    if attempt < max_attempts - 1:
                        # calculate delay with exponential backoff
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        self.logger.info(
                            f'Function {function_name} is pending, retrying in {delay}s... (attempt {attempt + 1}/{max_attempts})')
                        time.sleep(delay)
                        continue
                    else:
                        # max attempts reached
                        raise InvocationException(
                            f'Function {function_name} did not become active after {max_attempts} attempts')
                else:
                    # not a pending state error so raise immediately
                    raise InvocationException(f'Failed to invoke function {function_name}: {str(e)}')

            except Exception as e:
                # any other exception raise immediately
                raise InvocationException(f'Unexpected error invoking function {function_name}: {str(e)}')

        # this should never be reached but just in case
        if last_exception:
            raise last_exception
        else:
            raise InvocationException(f'Failed to invoke function {function_name} after {max_attempts} attempts')

    def list_functions(self) -> List[Dict[str, Any]]:
        """List all Lambda functions"""
        try:
            functions = []
            paginator = self._lambda_client.get_paginator('list_functions')

            for page in paginator.paginate():
                for func in page['Functions']:
                    functions.append({
                        'name': func['FunctionName'],
                        'arn': func['FunctionArn'],
                        'runtime': func.get('Runtime', 'container'),
                        'handler': func.get('Handler', 'unknown'),
                        'memory': func.get('MemorySize', 0),
                        'timeout': func.get('Timeout', 0),
                        'state': func.get('State', 'unknown'),
                        'package_type': func.get('PackageType', 'Zip')
                    })

            return functions

        except ClientError as e:
            self.logger.error(f'Failed to list functions: {str(e)}')
            return []

    @retry_on_error()
    def delete_function(self, function_name: str):
        """Delete a Lambda function"""
        try:
            self._lambda_client.delete_function(FunctionName=function_name)

            # remove from tracking
            with self._task_lock:
                self._deployed_functions.pop(function_name, None)
                for name, task in list(self._functions_book.items()):
                    if task.name == function_name:
                        del self._functions_book[name]
                        break

            self.logger.info(f'Deleted function: {function_name}')

        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise DeploymentException(f'Failed to delete function {function_name}: {str(e)}')

    def get_tasks(self) -> List[Task]:
        """Get all deployed tasks"""
        with self._task_lock:
            return list(self._functions_book.values())

    def get_resource_status(self) -> Dict[str, Any]:
        """Get status of AWS resources"""
        try:
            return {
                'active': self.status,
                'functions_count': len(self._deployed_functions),
                'active_deployments': len(self._active_deployments),
                'has_iam_role': bool(self.resource_manager.aws_resources.iam_role_arn),
                'has_ecr_repo': bool(self.resource_manager.aws_resources.ecr_repository_uri),
                'region': self.region
            }
        except Exception as e:
            self.logger.error(f"Failed to get resource status: {e}")
            return {'active': False, 'error': str(e)}

    def shutdown(self):
        """Shutdown provider and cleanup resources with graceful shutdown"""
        if not self.status:
            return

        self.logger.info("Shutting down AWS Lambda provider")

        # stop accepting new work
        self._terminate.set()

        # wait for active deployments to complete
        start_time = time.time()
        while self._active_deployments and time.time() - start_time < GRACEFUL_SHUTDOWN_TIMEOUT:
            self.logger.info(f"Waiting for {len(self._active_deployments)} active deployments...")
            time.sleep(1)

        if self._active_deployments:
            self.logger.warning(f"{len(self._active_deployments)} deployments still active after timeout")

        # wait for worker thread
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)

        # shutdown executor
        self.executor.shutdown(wait=True)

        # cleanup functions if auto_terminate
        if self.auto_terminate:
            self.logger.info("Auto-terminating deployed functions")
            functions_to_delete = list(self._deployed_functions.keys())
            for function_name in functions_to_delete:
                try:
                    self.delete_function(function_name)
                except Exception as e:
                    self.logger.error(f"Error deleting function {function_name}: {e}")

            # cleanup iam role if created
            if self.resource_manager.aws_resources.iam_role_name:
                try:
                    self._cleanup_iam_role()
                except Exception as e:
                    self.logger.error(f"Failed to cleanup IAM role: {e}")

            # cleanup ecr repository if created
            if self.resource_manager.aws_resources.ecr_repository_name:
                try:
                    self.registry_manager.delete_ecr_repository(
                        self.resource_manager.aws_resources.ecr_repository_name
                    )
                except Exception as e:
                    self.logger.error(f"Failed to cleanup ECR repository: {e}")

        # save resources
        try:
            self.resource_manager.save_all_resources()
        except Exception as e:
            self.logger.error(f"Failed to save resources: {e}")

        self.status = False
        self._shutdown_complete.set()
        self.logger.info("AWS Lambda provider shutdown complete")

    def _cleanup_iam_role(self):
        """Clean up IAM role and attached policies"""
        role_name = self.resource_manager.aws_resources.iam_role_name

        if not role_name:
            return

        try:
            # detach policies
            policies = [
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
                'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
            ]

            for policy_arn in policies:
                try:
                    self._iam_client.detach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy_arn
                    )
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        self.logger.warning(f"Error detaching policy {policy_arn}: {e}")

            # delete role
            self._iam_client.delete_role(RoleName=role_name)
            self.logger.info(f"Deleted IAM role: {role_name}")

            # clear from resource manager
            self.resource_manager.aws_resources.iam_role_name = None
            self.resource_manager.aws_resources.iam_role_arn = None

        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                raise

    @property
    def is_active(self):
        """Check if provider is active"""
        return self.status