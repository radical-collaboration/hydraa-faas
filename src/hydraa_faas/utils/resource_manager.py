# -*- coding: utf-8 -*-
"""Handles the lifecycle and persistence of cloud resources for FaaS deployments.

This module provides a simplified, thread-safe manager for creating and
deleting cloud resources, such as IAM roles and ECR repositories, that are
required by FaaS providers. It persists the state of these resources to disk,
allowing them to be reused across sessions.

The resource creation logic is idempotent, using a try-except pattern to
gracefully handle cases where resources already exist, which makes it both
efficient and robust.

Example:
    To create and manage AWS resources::

        # (Assuming boto3 clients are created)
        res_manager = ResourceManager()
        res_manager.create_aws_iam_role(
            role_name='my-faas-lambda-role',
            iam_client=iam_client
        )
        res_manager.save_all_resources()

"""

import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from .exceptions import ECRException, IAMException


@dataclass
class AWSResources:
    """A data class to hold identifiers for AWS resources."""
    iam_role_arn: Optional[str] = None
    iam_role_name: Optional[str] = None
    ecr_repository_uri: Optional[str] = None
    ecr_repository_name: Optional[str] = None
    region: str = "us-east-1"
    created_at: Optional[str] = None


@dataclass
class LocalResources:
    """A data class to hold identifiers for local resources."""
    registry_url: Optional[str] = None
    config_path: Optional[str] = None
    created_at: Optional[str] = None


class ResourceManager:
    """Manages the lifecycle of cloud resources with persistence.

    This class is responsible for creating, deleting, and persisting the state
    of cloud resources needed for FaaS deployments. It is designed to be
    thread-safe.

    Attributes:
        aws_resources: A dataclass instance holding AWS resource information.
        local_resources: A dataclass instance holding local resource info.
    """

    def __init__(self, config_dir: Optional[str] = None, logger: Optional[Any] = None):
        """Initializes the ResourceManager.

        Args:
            config_dir: The directory to store resource configuration files.
                        Defaults to `~/.faas`.
            logger: An optional logger instance for logging messages.
        """
        self.config_dir = Path(config_dir or os.path.expanduser("~/.faas"))
        self.config_dir.mkdir(exist_ok=True)
        self.logger = logger

        self.aws_resources_file = self.config_dir / "aws_resources.json"
        self.local_resources_file = self.config_dir / "local_resources.json"

        self.aws_resources = AWSResources()
        self.local_resources = LocalResources()

        self._aws_lock = threading.RLock()
        self._local_lock = threading.RLock()

        self.load_all_resources()

    def _log_info(self, message: str) -> None:
        """Logs an info message."""
        if self.logger:
            self.logger.info(message)
        else:
            print(f"[ResourceManager] {message}")

    def _log_error(self, message: str) -> None:
        """Logs an error message."""
        if self.logger:
            self.logger.error(message)
        else:
            print(f"[ResourceManager ERROR] {message}")

    def _save_resources(self, file_path: Path, data: Any) -> None:
        """Atomically saves a resource's state to a JSON file.

        Args:
            file_path: The path to the file where the data will be saved.
            data: The dataclass instance to save.
        """
        temp_file = file_path.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(asdict(data), f, indent=2)
        temp_file.replace(file_path)

    def _load_resources(self, file_path: Path, data_class: Any) -> Any:
        """Loads a resource's state from a JSON file.

        Args:
            file_path: The path to the file to load.
            data_class: The dataclass type to instantiate.

        Returns:
            An instance of the provided dataclass with the loaded data.
        """
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                return data_class(**data)
            except (json.JSONDecodeError, TypeError) as e:
                self._log_error(f"Failed to load resources from {file_path}: {e}")
        return data_class()

    def create_aws_iam_role(self, role_name: str, iam_client: Any) -> str:
        """Creates or gets an IAM role for AWS Lambda execution.

        This method attempts to create the role and gracefully handles the
        case where it already exists, making it idempotent.

        Args:
            role_name: The name for the IAM role.
            iam_client: An initialized boto3 IAM client.

        Returns:
            The ARN of the IAM role.

        Raises:
            IAMException: If role creation fails for reasons other than
                          the role already existing.
        """
        with self._aws_lock:
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }]
            }
            try:
                response = iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(trust_policy),
                    Description='Execution role for FaaS Lambda functions'
                )
                role_arn = response['Role']['Arn']
                self._log_info(f"Created new IAM role: {role_name}")
                # Wait for IAM propagation
                time.sleep(10)
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityAlreadyExists':
                    self._log_info(f"Using existing IAM role: {role_name}")
                    response = iam_client.get_role(RoleName=role_name)
                    role_arn = response['Role']['Arn']
                else:
                    raise IAMException(f"Failed to create IAM role: {e}")

            # Attach required policies
            policies = [
                'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
                'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
            ]
            for policy_arn in policies:
                try:
                    iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                except ClientError as e:
                    if e.response['Error']['Code'] != 'EntityAlreadyExists':
                        self._log_error(f"Failed to attach policy {policy_arn}: {e}")

            self.aws_resources.iam_role_arn = role_arn
            self.aws_resources.iam_role_name = role_name
            return role_arn

    def create_aws_ecr_repository(self, repo_name: str, ecr_client: Any) -> str:
        """Creates or gets an ECR repository with proper permissions.

        Args:
            repo_name: The name for the ECR repository.
            ecr_client: An initialized boto3 ECR client.

        Returns:
            The URI of the ECR repository.

        Raises:
            ECRException: If repository creation fails for reasons other than
                          the repository already existing.
        """
        with self._aws_lock:
            try:
                response = ecr_client.create_repository(
                    repositoryName=repo_name,
                    imageTagMutability='MUTABLE'
                )
                repo_uri = response['repository']['repositoryUri']
                self._log_info(f"Created ECR repository: {repo_name}")

                # Set repository policy to allow Lambda service
                self._set_ecr_repository_policy(repo_name, ecr_client)

            except ClientError as e:
                if e.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
                    self._log_info(f"Using existing ECR repository: {repo_name}")
                    response = ecr_client.describe_repositories(repositoryNames=[repo_name])
                    repo_uri = response['repositories'][0]['repositoryUri']

                    # Ensure policy is set even for existing repo
                    self._set_ecr_repository_policy(repo_name, ecr_client)
                else:
                    raise ECRException(f"Failed to create ECR repository: {e}")

            self.aws_resources.ecr_repository_uri = repo_uri
            self.aws_resources.ecr_repository_name = repo_name
            return repo_uri

    def _set_ecr_repository_policy(self, repo_name: str, ecr_client: Any) -> None:
        """Set ECR repository policy to allow Lambda access.

        Args:
            repo_name: The ECR repository name.
            ecr_client: An initialized boto3 ECR client.
        """
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "LambdaECRImageRetrievalPolicy",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "lambda.amazonaws.com"
                    },
                    "Action": [
                        "ecr:BatchGetImage",
                        "ecr:GetDownloadUrlForLayer"
                    ]
                }
            ]
        }

        try:
            ecr_client.set_repository_policy(
                repositoryName=repo_name,
                policyText=json.dumps(policy)
            )
            self._log_info(f"Set ECR repository policy for Lambda access")
        except ClientError as e:
            # Policy might already exist
            if e.response['Error']['Code'] != 'RepositoryPolicyNotFoundException':
                self._log_error(f"Failed to set repository policy: {e}")

    def cleanup_aws_resources(self, iam_client: Any, ecr_client: Any) -> None:
        """Cleans up all managed AWS resources with robust error handling.

        Args:
            iam_client: An initialized boto3 IAM client.
            ecr_client: An initialized boto3 ECR client.
        """
        with self._aws_lock:
            # Clean up IAM role with comprehensive error handling
            if self.aws_resources.iam_role_name:
                role_name = self.aws_resources.iam_role_name
                try:
                    # First, list and detach all managed policies
                    try:
                        policies = iam_client.list_attached_role_policies(RoleName=role_name)
                        for policy in policies['AttachedPolicies']:
                            try:
                                iam_client.detach_role_policy(
                                    RoleName=role_name,
                                    PolicyArn=policy['PolicyArn']
                                )
                                self._log_info(f"Detached policy {policy['PolicyArn']} from role {role_name}")
                            except ClientError as e:
                                if e.response['Error']['Code'] != 'NoSuchEntity':
                                    self._log_error(f"Failed to detach policy: {e}")
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'NoSuchEntity':
                            self._log_error(f"Failed to list attached policies: {e}")

                    # Also check for inline policies
                    try:
                        inline_policies = iam_client.list_role_policies(RoleName=role_name)
                        for policy_name in inline_policies.get('PolicyNames', []):
                            try:
                                iam_client.delete_role_policy(
                                    RoleName=role_name,
                                    PolicyName=policy_name
                                )
                                self._log_info(f"Deleted inline policy {policy_name} from role {role_name}")
                            except ClientError as e:
                                if e.response['Error']['Code'] != 'NoSuchEntity':
                                    self._log_error(f"Failed to delete inline policy: {e}")
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'NoSuchEntity':
                            self._log_error(f"Failed to list inline policies: {e}")

                    # Check for instance profiles using this role
                    try:
                        instance_profiles = iam_client.list_instance_profiles_for_role(RoleName=role_name)
                        for profile in instance_profiles.get('InstanceProfiles', []):
                            try:
                                iam_client.remove_role_from_instance_profile(
                                    InstanceProfileName=profile['InstanceProfileName'],
                                    RoleName=role_name
                                )
                                self._log_info(f"Removed role from instance profile {profile['InstanceProfileName']}")
                            except ClientError as e:
                                if e.response['Error']['Code'] != 'NoSuchEntity':
                                    self._log_error(f"Failed to remove role from instance profile: {e}")
                    except ClientError as e:
                        if e.response['Error']['Code'] != 'NoSuchEntity':
                            self._log_error(f"Failed to list instance profiles: {e}")

                    # Now delete the role
                    iam_client.delete_role(RoleName=role_name)
                    self._log_info(f"Deleted IAM role: {role_name}")

                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        self._log_error(f"Failed to delete IAM role {role_name}: {e}")

            # Clean up ECR repository
            if self.aws_resources.ecr_repository_name:
                repo_name = self.aws_resources.ecr_repository_name
                try:
                    ecr_client.delete_repository(repositoryName=repo_name, force=True)
                    self._log_info(f"Deleted ECR repository: {repo_name}")
                except ClientError as e:
                    if e.response['Error']['Code'] != 'RepositoryNotFoundException':
                        self._log_error(f"Failed to delete ECR repo {repo_name}: {e}")

            self.clear_aws_resources()

    def load_all_resources(self) -> None:
        """Loads all resource configurations from disk."""
        with self._aws_lock:
            self.aws_resources = self._load_resources(self.aws_resources_file, AWSResources)
        with self._local_lock:
            self.local_resources = self._load_resources(self.local_resources_file, LocalResources)

    def save_all_resources(self) -> None:
        """Saves all current resource configurations to disk."""
        with self._aws_lock:
            if self.aws_resources.iam_role_arn or self.aws_resources.ecr_repository_uri:
                self._save_resources(self.aws_resources_file, self.aws_resources)
        with self._local_lock:
            if self.local_resources.registry_url:
                self._save_resources(self.local_resources_file, self.local_resources)

    def clear_aws_resources(self) -> None:
        """Clears the AWS resource state and deletes the config file."""
        with self._aws_lock:
            self.aws_resources = AWSResources()
            if self.aws_resources_file.exists():
                self.aws_resources_file.unlink()