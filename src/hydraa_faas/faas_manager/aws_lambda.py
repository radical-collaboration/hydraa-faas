"""
AWS Lambda provider
"""

import os
import json
import time
import uuid
import queue
import shutil
import signal
import threading
from collections import OrderedDict
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.exceptions import ClientError
from ..utils.packaging import create_deployment_package, validate_handler
from ..utils.ecr_helper import ECRHelper
from ..utils.exceptions import *

# aws lambda constants
LAMBDA_MAX_TIMEOUT = 900  # 15 minutes
LAMBDA_MIN_MEMORY = 128
LAMBDA_MAX_MEMORY = 10240
LAMBDA_MAX_ZIP_SIZE = 50 * 1024 * 1024  # 50MB

DEFAULT_PYTHON_RUNTIME = 'python3.9'
DEFAULT_TIMEOUT = 30
DEFAULT_MEMORY = 128


class AwsLambda:
    """
    AWS Lambda provider for FaaS Manager
    """

    def __init__(self, sandbox: str, manager_id: str, cred: dict, asynchronous: bool, auto_terminate: bool, log):
        """
        Initialize AWS Lambda provider

        Args:
            sandbox: Sandbox directory path
            manager_id: Unique manager identifier
            cred: AWS credentials
            asynchronous: Run operations asynchronously
            auto_terminate: Auto-terminate resources on shutdown
            log: Logging object
        """
        # lambda specific initialization
        self.manager_id = manager_id
        self.logger = log
        self.status = False
        self.asynchronous = asynchronous
        self.auto_terminate = auto_terminate
        self._region_name = cred['region_name']

        # aws clients
        self._lambda_client = self._create_lambda_client(cred)
        self._ecr_client = self._create_ecr_client(cred)
        self._iam_client = self._create_iam_client(cred)
        self._logs_client = self._create_logs_client(cred)

        # ecr helper
        self.ecr_helper = ECRHelper(self._ecr_client, cred['region_name'], self.logger)

        # threading and queues
        self.incoming_q = queue.Queue()  # tasks to deploy
        self.outgoing_q = queue.Queue()  # results back to manager
        self.internal_q = queue.Queue()  # internal messages

        self._task_lock = threading.Lock()
        self._terminate = threading.Event()
        self._shutdown_lock = threading.Lock()
        self._shutdown_started = False

        # function and resource tracking
        self._task_id = 0
        self._functions_book = OrderedDict()  # task_name: task
        # todo: add size limit and lru eviction for _functions_book to prevent memory leak
        self._created_resources = {
            'functions': {},  # function_name: function_arn
            'ecr_repos': {},  # repo_name: repo_uri
            'iam_roles': {},  # role_name: role_arn
            'log_groups': {}  # function_name: log_group_name
        }

        # cleanup configuration
        self.cleanup_timeout = 300  # 5 minutes max cleanup time
        self.cleanup_retry_attempts = 3
        self.cleanup_retry_delay = 5  # seconds

        # setup sandbox
        self.run_id = f'lambda.{str(uuid.uuid4())}'
        self.sandbox = f'{sandbox}/aws_lambda.{self.run_id}'
        os.makedirs(self.sandbox, exist_ok=True)

        # start worker thread
        self.start_thread = threading.Thread(target=self.start, name='AwsLambda')
        self.start_thread.daemon = True

        if not self.start_thread.is_alive():
            self.start_thread.start()

        self.status = True

    @contextmanager
    def _cleanup_timeout_context(self, timeout=60):
        """Context manager to handle cleanup timeouts"""
        def timeout_handler(signum, frame):
            raise TimeoutError(f"cleanup operation timed out after {timeout}s")

        # set up timeout handler
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout)

        try:
            yield
        except TimeoutError:
            self.logger.error(f"cleanup operation timed out after {timeout}s")
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

    def _safe_delete_iam_role(self, role_name):
        """Safely delete IAM role"""
        for attempt in range(self.cleanup_retry_attempts):
            try:
                # first list and detach all attached policies
                try:
                    attached_policies = self._iam_client.list_attached_role_policies(RoleName=role_name)

                    for policy in attached_policies.get('AttachedPolicies', []):
                        try:
                            self._iam_client.detach_role_policy(
                                RoleName=role_name,
                                PolicyArn=policy['PolicyArn']
                            )
                            self.logger.trace(f"detached policy {policy['PolicyArn']} from role {role_name}")
                        except ClientError as e:
                            if e.response['Error']['Code'] != 'NoSuchEntity':
                                self.logger.warning(f"error detaching policy: {e}")

                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        raise

                # wait for aws to do policy detachment
                time.sleep(2)

                # now delete the role
                self._iam_client.delete_role(RoleName=role_name)
                self.logger.trace(f"successfully deleted iam role {role_name}")
                return True

            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'DeleteConflict' and attempt < self.cleanup_retry_attempts - 1:
                    self.logger.warning(f"role {role_name} still in use, retrying in {self.cleanup_retry_delay}s (attempt {attempt + 1})")
                    time.sleep(self.cleanup_retry_delay)
                elif error_code == 'NoSuchEntity':
                    self.logger.trace(f"iam role {role_name} already deleted")
                    return True
                else:
                    self.logger.error(f"failed to delete iam role {role_name}: {e}")
                    return False
            except Exception as e:
                self.logger.error(f"unexpected error deleting iam role {role_name}: {e}")
                return False

        return False

    def _wait_for_function_completion(self, function_name, timeout=60):
        """Wait for function invocations to complete before deletion"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            try:
                # check if function still exists and is not being invoked
                response = self._lambda_client.get_function(FunctionName=function_name)

                # can just wait a reasonable time
                # in the future i can check cloudwatch metrics for concurrent executions
                time.sleep(5)
                break

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    # function already deleted
                    break
                self.logger.warning(f"error checking function {function_name} status: {e}")
                break
            except Exception as e:
                self.logger.warning(f"error checking function {function_name} status: {e}")
                break

    def _cleanup_lambda_functions(self):
        """Clean up Lambda functions with proper error handling"""
        self.logger.trace("starting Lambda functions cleanup")

        for function_name in list(self._created_resources['functions'].keys()):
            try:
                with self._cleanup_timeout_context(60):
                    # wait for any running invocations to complete
                    self._wait_for_function_completion(function_name, timeout=30)

                    # delete the function
                    self._lambda_client.delete_function(FunctionName=function_name)
                    self.logger.trace(f"deleted Lambda function {function_name}")

                    # remove from tracking
                    del self._created_resources['functions'][function_name]

            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    self.logger.trace(f"lambda function {function_name} already deleted")
                else:
                    self.logger.error(f"failed to delete Lambda function {function_name}: {e}")
            except Exception as e:
                self.logger.error(f"unexpected error deleting Lambda function {function_name}: {e}")

    def _cleanup_ecr_repositories(self):
        """Clean up ECR repositories with proper error handling"""
        self.logger.trace("starting ecr repositories cleanup")

        for repo_name in list(self._created_resources['ecr_repos'].keys()):
            try:
                with self._cleanup_timeout_context(60):
                    self.ecr_helper.delete_repository(repo_name, force=True)
                    self.logger.trace(f"deleted ecr repository {repo_name}")

                    # remove from tracking
                    del self._created_resources['ecr_repos'][repo_name]

            except Exception as e:
                self.logger.error(f"failed to delete ecr repository {repo_name}: {e}")

    def _cleanup_iam_roles(self):
        """Clean up IAM roles with proper error handling"""
        self.logger.trace("starting iam roles cleanup")

        for role_name in list(self._created_resources['iam_roles'].keys()):
            # only delete roles we created (with hydraa prefix)
            if role_name.startswith('hydraa-'):
                try:
                    with self._cleanup_timeout_context(120):  # iam operations are slow
                        if self._safe_delete_iam_role(role_name):
                            del self._created_resources['iam_roles'][role_name]
                except Exception as e:
                    self.logger.error(f"unexpected error during IAM role cleanup for {role_name}: {e}")

    def _cleanup_log_groups(self):
        """Clean up CloudWatch log groups"""
        self.logger.trace("starting cloudwatch log groups cleanup")

        for function_name in list(self._created_resources['log_groups'].keys()):
            try:
                with self._cleanup_timeout_context(30):
                    log_group_name = f"/aws/lambda/{function_name}"

                    try:
                        self._logs_client.delete_log_group(logGroupName=log_group_name)
                        self.logger.trace(f"deleted log group {log_group_name}")
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'ResourceNotFoundException':
                            self.logger.warning(f"failed to delete log group {log_group_name}: {e}")

                    # remove from tracking
                    del self._created_resources['log_groups'][function_name]

            except Exception as e:
                self.logger.error(f"error deleting log group for {function_name}: {e}")

    def _cleanup_sandbox(self):
        """Clean up the sandbox directory"""
        try:
            if os.path.exists(self.sandbox):
                shutil.rmtree(self.sandbox)
                self.logger.trace(f"Cleaned up sandbox directory: {self.sandbox}")
        except Exception as e:
            self.logger.error(f"Failed to cleanup sandbox {self.sandbox}: {e}")

    def _cleanup_in_order(self):
        """Clean up resources in the correct dependency order"""
        cleanup_steps = [
            ("Lambda Functions", self._cleanup_lambda_functions),
            ("ECR Repositories", self._cleanup_ecr_repositories),
            ("IAM Roles", self._cleanup_iam_roles),
            ("CloudWatch Log Groups", self._cleanup_log_groups),
            ("Sandbox Directory", self._cleanup_sandbox)
        ]

        for step_name, cleanup_func in cleanup_steps:
            try:
                self.logger.trace(f"Cleaning up {step_name}...")
                cleanup_func()
            except Exception as e:
                self.logger.error(f"Error during {step_name} cleanup: {e}")
                # continue with other cleanup steps

    def _create_lambda_client(self, cred):
        """Create AWS Lambda client"""
        return boto3.client(
            'lambda',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            region_name=cred['region_name']
        )

    def _create_iam_client(self, cred):
        """Create AWS IAM client"""
        return boto3.client(
            'iam',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            region_name=cred['region_name']
        )

    def _create_ecr_client(self, cred):
        """Create AWS ECR client"""
        return boto3.client(
            'ecr',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            region_name=cred['region_name']
        )

    def _create_logs_client(self, cred):
        """Create AWS CloudWatch Logs client"""
        return boto3.client(
            'logs',
            aws_access_key_id=cred['aws_access_key_id'],
            aws_secret_access_key=cred['aws_secret_access_key'],
            region_name=cred['region_name']
        )

    def start(self):
        """Start the Lambda provider"""
        if self.status:
            self.logger.trace('Lambda provider already started')
            return self.run_id

        self.logger.trace(f"Starting Lambda provider {self.run_id}")

        # start work processing
        self._get_work()

        self.status = True
        self.logger.trace(f"Lambda provider {self.run_id} is running")

    def _get_work(self):
        """Process deployment requests from queue"""
        bulk = []
        max_bulk_size = int(os.environ.get('MAX_BULK_SIZE', 32))
        max_bulk_time = float(os.environ.get('MAX_BULK_TIME', 2.0))
        min_bulk_time = float(os.environ.get('MIN_BULK_TIME', 0.1))

        while not self._terminate.is_set():
            now = time.time()

            # collect tasks for bulk processing
            while time.time() - now < max_bulk_time:
                try:
                    task = self.incoming_q.get(block=True, timeout=min_bulk_time)
                except queue.Empty:
                    task = None

                if task:
                    bulk.append(task)

                if len(bulk) >= max_bulk_size:
                    break

            if bulk:
                with self._task_lock:
                    self._process_bulk_deployment_parallel(bulk)
                bulk = []

    def _process_bulk_deployment_parallel(self, tasks):
        """Process multiple function deployments with parallel container builds"""
        # separate container and source tasks
        container_tasks = []
        source_tasks = []

        for task in tasks:
            deployment_method = self._determine_deployment_method(task)
            if deployment_method in ['build_image', 'existing_image']:
                container_tasks.append(task)
            else:
                source_tasks.append(task)

        # process container builds in parallel (max 3 concurrent builds)
        container_futures = {}
        if container_tasks:
            with ThreadPoolExecutor(max_workers=3) as executor:
                for task in container_tasks:
                    task.id = self._task_id
                    task.name = f'hydraa-function-{self._task_id}'
                    self._task_id += 1

                    future = executor.submit(self._deploy_container_with_cleanup, task)
                    container_futures[future] = task

                # process source packages sequentially while containers build
                for task in source_tasks:
                    try:
                        task.id = self._task_id
                        task.name = f'hydraa-function-{self._task_id}'
                        function_arn = self._deploy_source_package(task)

                        task.arn = function_arn
                        self._functions_book[task.name] = task
                        self._created_resources['functions'][task.name] = function_arn
                        task.set_result(f'Function {task.name} deployed successfully')
                        self.outgoing_q.put(f'Function {task.name} deployed: {function_arn}')
                        self.logger.trace(f'Function {task.name} deployed successfully')

                        self._task_id += 1
                    except Exception as e:
                        self.logger.error(f'Failed to deploy function for task {task.name}: {str(e)}')
                        task.set_exception(e)
                        self.outgoing_q.put(f'Failed to deploy {task.name}: {str(e)}')

                # wait for container builds to complete
                for future in as_completed(container_futures):
                    task = container_futures[future]
                    try:
                        function_arn = future.result()
                        task.arn = function_arn
                        self._functions_book[task.name] = task
                        self._created_resources['functions'][task.name] = function_arn
                        task.set_result(f'Function {task.name} deployed successfully')
                        self.outgoing_q.put(f'Function {task.name} deployed: {function_arn}')
                        self.logger.trace(f'Container function {task.name} deployed successfully')
                    except Exception as e:
                        self.logger.error(f'Failed to deploy container function for task {task.name}: {str(e)}')
                        task.set_exception(e)
                        self.outgoing_q.put(f'Failed to deploy {task.name}: {str(e)}')
        else:
            # no container tasks, process source tasks normally
            for task in source_tasks:
                try:
                    task.id = self._task_id
                    task.name = f'hydraa-function-{self._task_id}'
                    function_arn = self._deploy_source_package(task)

                    task.arn = function_arn
                    self._functions_book[task.name] = task
                    self._created_resources['functions'][task.name] = function_arn
                    task.set_result(f'Function {task.name} deployed successfully')
                    self.outgoing_q.put(f'Function {task.name} deployed: {function_arn}')
                    self.logger.trace(f'Function {task.name} deployed successfully')

                    self._task_id += 1
                except Exception as e:
                    self.logger.error(f'Failed to deploy function for task {task.name}: {str(e)}')
                    task.set_exception(e)
                    self.outgoing_q.put(f'Failed to deploy {task.name}: {str(e)}')

    def _deploy_container_with_cleanup(self, task):
        """Deploy container image with proper cleanup on failure"""
        repo_name = f"hydraa-{task.name.lower()}"
        created_repo = False

        try:
            # create ecr repository if needed
            if getattr(task, 'create_ecr_repo', True):
                repo_uri = self.ecr_helper.create_repository(repo_name)
                self._created_resources['ecr_repos'][repo_name] = repo_uri
                created_repo = True

            return self._deploy_container_image(task)

        except Exception as e:
            # cleanup partial resources on failure
            if created_repo and repo_name in self._created_resources['ecr_repos']:
                try:
                    self.ecr_helper.delete_repository(repo_name, force=True)
                    del self._created_resources['ecr_repos'][repo_name]
                    self.logger.trace(f"cleaned up failed ecr repo {repo_name}")
                except Exception as cleanup_e:
                    self.logger.warning(f"failed to cleanup ecr repo {repo_name}: {cleanup_e}")

            raise DeploymentException(f"Container deployment failed: {e}")

    def _determine_deployment_method(self, task):
        # check for source path with dockerfile first
        if hasattr(task, 'source_path') and os.path.exists(task.source_path):
            dockerfile_path = os.path.join(task.source_path, 'Dockerfile')
            if os.path.exists(dockerfile_path):
                return 'build_image'
            else:
                return 'source_package'

        # check for existing container image
        if hasattr(task, 'image') and task.image:
            return 'existing_image'

        raise DeploymentException(
            "Task must specify either 'image' (for container deployment) "
            "or 'source_path' (for ZIP deployment)"
        )

    def _deploy_source_package(self, task):
        """Deploy Lambda function from source code"""
        self.logger.trace(f'Deploying {task.name} from source package')

        # validate handler
        handler_name = getattr(task, 'handler_name', 'index.handler')
        validate_handler(task.source_path, handler_name)

        # create deployment package
        zip_content = create_deployment_package(task.source_path)

        # create or get execution role
        role_arn = self._get_or_create_execution_role(task)

        # prepare function configuration
        function_config = {
            'FunctionName': task.name,
            'Runtime': getattr(task, 'runtime', DEFAULT_PYTHON_RUNTIME),
            'Role': role_arn,
            'Handler': handler_name,
            'Code': {'ZipFile': zip_content},
            'Description': f'Hydraa function deployed by manager {self.manager_id}',
            'Timeout': min(getattr(task, 'timeout', DEFAULT_TIMEOUT), LAMBDA_MAX_TIMEOUT),
            'MemorySize': max(min(getattr(task, 'memory', DEFAULT_MEMORY), LAMBDA_MAX_MEMORY), LAMBDA_MIN_MEMORY),
            'Publish': True
        }

        # add environment variables if specified
        if hasattr(task, 'env_var') and task.env_var:
            env_vars = {}
            for env in task.env_var:
                if '=' in env:
                    key, value = env.split('=', 1)
                    env_vars[key] = value
            if env_vars:
                function_config['Environment'] = {'Variables': env_vars}

        # add vpc configuration if specified
        if hasattr(task, 'vpc_config') and task.vpc_config:
            function_config['VpcConfig'] = task.vpc_config

        # add layers if specified
        if hasattr(task, 'layers') and task.layers:
            function_config['Layers'] = task.layers

        # create the function
        try:
            response = self._lambda_client.create_function(**function_config)
            function_arn = response['FunctionArn']

            self.logger.trace(f'Created Lambda function: {function_arn}')
            return function_arn

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceConflictException':
                # function already exists, update it
                return self._update_function_code(task.name, zip_content)
            else:
                raise DeploymentException(f'Failed to create function: {str(e)}')

    def _deploy_container_image(self, task):
        """Deploy Lambda function from container image"""
        self.logger.trace(f'Deploying {task.name} from container image')

        # handle ecr repository creation/management
        repo_name = f"hydraa-{task.name.lower()}"

        if getattr(task, 'create_ecr_repo', True):
            repo_uri = self.ecr_helper.create_repository(repo_name)
            self._created_resources['ecr_repos'][repo_name] = repo_uri

        # build and push image if source_path provided
        if hasattr(task, 'source_path') and os.path.exists(task.source_path):
            image_uri = self.ecr_helper.build_and_push_image(
                task.source_path,
                self._created_resources['ecr_repos'][repo_name],
                'latest'
            )
        else:
            # use existing image
            image_uri = task.image

        # create or get execution role
        role_arn = self._get_or_create_execution_role(task)

        # prepare function configuration
        function_config = {
            'FunctionName': task.name,
            'Role': role_arn,
            'Code': {'ImageUri': image_uri},
            'PackageType': 'Image',
            'Description': f'Hydraa container function deployed by manager {self.manager_id}',
            'Timeout': min(getattr(task, 'timeout', DEFAULT_TIMEOUT), LAMBDA_MAX_TIMEOUT),
            'MemorySize': max(min(getattr(task, 'memory', DEFAULT_MEMORY), LAMBDA_MAX_MEMORY), LAMBDA_MIN_MEMORY),
            'Publish': True
        }

        # add environment variables if specified
        if hasattr(task, 'env_var') and task.env_var:
            env_vars = {}
            for env in task.env_var:
                if '=' in env:
                    key, value = env.split('=', 1)
                    env_vars[key] = value
            if env_vars:
                function_config['Environment'] = {'Variables': env_vars}

        # create the function
        try:
            response = self._lambda_client.create_function(**function_config)
            function_arn = response['FunctionArn']

            self.logger.trace(f'Created Lambda container function: {function_arn}')
            return function_arn

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ResourceConflictException':
                # function already exists, update it
                return self._update_function_image(task.name, image_uri)
            else:
                raise DeploymentException(f'Failed to create container function: {str(e)}')

    def _get_or_create_execution_role(self, task):
        """Get or create IAM execution role for Lambda function"""
        role_name = f'hydraa-lambda-role-{self.manager_id}'

        # always try to get existing role first
        try:
            response = self._iam_client.get_role(RoleName=role_name)
            role_arn = response['Role']['Arn']
            self._created_resources['iam_roles'][role_name] = role_arn
            self.logger.trace(f'Found existing IAM role: {role_arn}')

            # make sure required policies are attached
            try:
                self._iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'EntityAlreadyExists':
                    self.logger.warning(f"Could not attach policy: {e}")

            return role_arn

        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                # some other error occurred
                self.logger.warning(f'Error checking existing role: {e}')

        # create new role
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            ]
        }

        try:
            # create the role
            response = self._iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f'Execution role for Hydraa Lambda functions (manager {self.manager_id})'
            )
            role_arn = response['Role']['Arn']

            # attach basic lambda execution policy
            self._iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
            )

            # if vpc access is needed, attach vpc execution policy
            if hasattr(task, 'vpc_config') and task.vpc_config:
                self._iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn='arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
                )

            self._created_resources['iam_roles'][role_name] = role_arn
            self.logger.trace(f'Created IAM role for Lambda: {role_arn}')

            # wait for role propagation with intelligent polling
            self._wait_for_role_propagation(role_arn)

            return role_arn

        except ClientError as e:
            raise DeploymentException(f'Failed to create IAM role: {str(e)}')

    # TODO: figure out a better way to do this
    def _wait_for_role_propagation(self, role_arn, max_wait=60):
        """Wait for IAM role to propagate with intelligent polling"""
        for attempt in range(max_wait):
            try:
                # test role by trying to assume it
                # in practice, we just wait incrementally as role setting is eventually consistent
                time.sleep(min(attempt * 0.5, 3))  # exponential backoff up to 3s

                # simple check if we can still get the role
                self._iam_client.get_role(RoleName=role_arn.split('/')[-1])

                # after 5 attempts (roughly 7.5 seconds), assume it's ready, fix this
                if attempt >= 5:
                    break

            except ClientError:
                # if role check fails, continue waiting
                continue

        self.logger.trace(f"waited {attempt + 1} attempts for role propagation")

    def _update_function_code(self, function_name, zip_content):
        """Update existing function with new ZIP code"""
        try:
            response = self._lambda_client.update_function_code(
                FunctionName=function_name,
                ZipFile=zip_content,
                Publish=True
            )
            function_arn = response['FunctionArn']
            self.logger.trace(f'Updated function code: {function_arn}')
            return function_arn
        except ClientError as e:
            raise DeploymentException(f'Failed to update function code: {str(e)}')

    def _update_function_image(self, function_name, image_uri):
        """Update existing function with new container image"""
        try:
            response = self._lambda_client.update_function_code(
                FunctionName=function_name,
                ImageUri=image_uri,
                Publish=True
            )
            function_arn = response['FunctionArn']
            self.logger.trace(f'Updated function image: {function_arn}')
            return function_arn
        except ClientError as e:
            raise DeploymentException(f'Failed to update function image: {str(e)}')

    def invoke_function(self, function_name, payload=None, invocation_type='RequestResponse'):
        """
        Invoke Lambda function

        Args:
            function_name (str): Name of the function to invoke
            payload (dict): Payload to send to function
            invocation_type (str): RequestResponse, Event, or DryRun

        Returns:
            dict: Invocation response

        Raises:
            InvocationException: If invocation fails
        """
        try:
            # prepare payload
            payload_json = json.dumps(payload) if payload else '{}'

            # invoke function
            response = self._lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=payload_json
            )

            # parse response
            result = {
                'StatusCode': response['StatusCode'],
                'ExecutedVersion': response.get('ExecutedVersion'),
                'LogResult': response.get('LogResult'),
                'Payload': response['Payload'].read().decode('utf-8') if 'Payload' in response else None
            }

            self.logger.trace(f'Invoked function {function_name} successfully')
            return result

        except ClientError as e:
            raise InvocationException(f'Failed to invoke function {function_name}: {str(e)}')

    def get_functions(self):
        """Get list of tracked functions"""
        return list(self._functions_book.values())

    def list_functions(self):
        """List all Lambda functions in the account"""
        try:
            response = self._lambda_client.list_functions()
            return response['Functions']
        except ClientError as e:
            self.logger.error(f'Failed to list functions: {str(e)}')
            return []

    def delete_function(self, function_name):
        """Delete a Lambda function"""
        try:
            self._lambda_client.delete_function(FunctionName=function_name)

            # remove from tracking
            if function_name in self._created_resources['functions']:
                del self._created_resources['functions'][function_name]
            if function_name in self._functions_book:
                del self._functions_book[function_name]

            self.logger.trace(f'Deleted function: {function_name}')

        except ClientError as e:
            self.logger.error(f'Failed to delete function {function_name}: {str(e)}')

    def shutdown(self):
        """Clean up all Lambda resources with comprehensive cleanup"""
        with self._shutdown_lock:
            if self._shutdown_started:
                self.logger.warning("Cleanup already in progress")
                return
            self._shutdown_started = True

        if not self.status:
            self.logger.trace("Provider already shut down")
            return

        self.logger.trace("Lambda provider termination started")
        self._terminate.set()

        # use overall cleanup timeout
        try:
            with self._cleanup_timeout_context(self.cleanup_timeout):
                self._cleanup_in_order()
        except Exception as e:
            self.logger.error(f"Error during comprehensive cleanup: {e}")

        self.status = False
        self.logger.trace("Lambda provider shutdown complete")