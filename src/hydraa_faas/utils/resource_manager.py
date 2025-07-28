"""
Handles creation and persistence of cloud resources for Lambda deployments.
"""

import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from botocore.exceptions import ClientError


@dataclass
class AWSResources:
    """AWS resource identifiers"""
    iam_role_arn: Optional[str] = None
    iam_role_name: Optional[str] = None
    ecr_repository_uri: Optional[str] = None
    ecr_repository_name: Optional[str] = None
    region: str = "us-east-1"
    created_at: Optional[str] = None


@dataclass
class LocalResources:
    """Local deployment resource identifiers"""
    registry_url: Optional[str] = None
    config_path: Optional[str] = None
    created_at: Optional[str] = None


class ResourceManager:
    """
    Manages cloud resources for FaaS deployments with persistence.

    Simplified version focusing on essential resource management:
    - AWS IAM roles and ECR repositories
    - Local resources for development
    - Simple JSON persistence
    - Basic retry logic
    - Integrated registry manager
    """

    def __init__(self, config_dir: Optional[str] = None):
        """
        Initialize ResourceManager

        Args:
            config_dir: Directory to store resource configs (default: ~/.faas)
        """
        self.config_dir = Path(config_dir or os.path.expanduser("~/.faas"))
        self.config_dir.mkdir(exist_ok=True)

        # Resource files
        self.aws_resources_file = self.config_dir / "aws_resources.json"
        self.local_resources_file = self.config_dir / "local_resources.json"

        # Resource containers
        self.aws_resources = AWSResources()
        self.local_resources = LocalResources()

        # Registry manager integration
        self._registry_manager = None

        # Load existing resources
        self.load_all_resources()

    @property
    def registry_manager(self):
        """Get or create registry manager instance"""
        if self._registry_manager is None:
            from ..registry import RegistryManager
            self._registry_manager = RegistryManager(logger=None)
        return self._registry_manager

    @registry_manager.setter
    def registry_manager(self, value):
        """Set registry manager instance"""
        self._registry_manager = value


    def setup_aws_resources(self,
                           iam_client: Any,
                           ecr_client: Any,
                           region: str,
                           role_name: str,
                           repo_name: str,
                           reuse_existing: bool = True) -> AWSResources:
        """
        Setup all AWS resources needed for Lambda deployments

        Args:
            iam_client: Boto3 IAM client
            ecr_client: Boto3 ECR client
            region: AWS region
            role_name: Name for IAM role
            repo_name: Name for ECR repository
            reuse_existing: Reuse existing resources if found

        Returns:
            AWSResources object with all identifiers
        """
        # Create IAM role
        role_arn = self.create_aws_iam_role(role_name, iam_client, reuse_existing)

        # Create ECR repository
        repo_uri = self.create_aws_ecr_repository(repo_name, ecr_client, region, reuse_existing)

        # Update resources
        self.aws_resources.iam_role_arn = role_arn
        self.aws_resources.iam_role_name = role_name
        self.aws_resources.ecr_repository_uri = repo_uri
        self.aws_resources.ecr_repository_name = repo_name
        self.aws_resources.region = region
        self.aws_resources.created_at = time.strftime('%Y-%m-%d %H:%M:%S')

        # Save to disk
        self.save_aws_resources()

        return self.aws_resources

    def create_aws_iam_role(self,
                           role_name: str,
                           iam_client: Any,
                           reuse_existing: bool = True) -> str:
        """
        Create or get IAM role for Lambda execution

        Args:
            role_name: Name for the IAM role
            iam_client: Boto3 IAM client
            reuse_existing: If True, reuse existing role

        Returns:
            IAM role ARN
        """
        # Check saved role first
        if reuse_existing and self.aws_resources.iam_role_arn:
            try:
                iam_client.get_role(RoleName=self.aws_resources.iam_role_name)
                return self.aws_resources.iam_role_arn
            except ClientError:
                pass

        # Check if role exists
        try:
            response = iam_client.get_role(RoleName=role_name)
            role_arn = response['Role']['Arn']

            # Ensure policies are attached
            self._attach_lambda_policies(iam_client, role_name)

            return role_arn

        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchEntity':
                raise

        # Create new role
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }]
        }

        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Execution role for FaaS Lambda functions'
        )

        role_arn = response['Role']['Arn']

        # Attach policies
        self._attach_lambda_policies(iam_client, role_name)

        # Wait for propagation
        time.sleep(5)

        return role_arn

    def _attach_lambda_policies(self, iam_client: Any, role_name: str):
        """Attach necessary policies to Lambda execution role"""
        policies = [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
            'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole'
        ]

        for policy_arn in policies:
            try:
                iam_client.attach_role_policy(
                    RoleName=role_name,
                    PolicyArn=policy_arn
                )
            except ClientError as e:
                if e.response['Error']['Code'] != 'EntityAlreadyExists':
                    raise

    def create_aws_ecr_repository(self,
                                 repo_name: str,
                                 ecr_client: Any,
                                 region: str,
                                 reuse_existing: bool = True) -> str:
        """
        Create or get ECR repository

        Args:
            repo_name: Name for the ECR repository
            ecr_client: Boto3 ECR client
            region: AWS region
            reuse_existing: If True, reuse existing repository

        Returns:
            ECR repository URI
        """
        # Check saved repository first
        if reuse_existing and self.aws_resources.ecr_repository_uri:
            try:
                ecr_client.describe_repositories(
                    repositoryNames=[self.aws_resources.ecr_repository_name]
                )
                return self.aws_resources.ecr_repository_uri
            except ClientError:
                pass

        # Try to create repository
        try:
            response = ecr_client.create_repository(
                repositoryName=repo_name,
                imageTagMutability='MUTABLE',
                imageScanningConfiguration={'scanOnPush': True}
            )
            return response['repository']['repositoryUri']

        except ClientError as e:
            if e.response['Error']['Code'] == 'RepositoryAlreadyExistsException':
                # Get existing repository URI
                response = ecr_client.describe_repositories(
                    repositoryNames=[repo_name]
                )
                return response['repositories'][0]['repositoryUri']
            else:
                raise


    def setup_local_resources(self,
                            registry_url: str = "localhost:5000",
                            config_path: Optional[str] = None) -> LocalResources:
        """
        Setup local resources for development

        Args:
            registry_url: URL of local Docker registry
            config_path: Path to local config

        Returns:
            LocalResources object
        """
        self.local_resources.registry_url = registry_url
        self.local_resources.config_path = config_path
        self.local_resources.created_at = time.strftime('%Y-%m-%d %H:%M:%S')

        self.save_local_resources()
        return self.local_resources


    def save_aws_resources(self):
        """Save AWS resources to file"""
        with open(self.aws_resources_file, 'w') as f:
            json.dump(asdict(self.aws_resources), f, indent=2)

    def save_local_resources(self):
        """Save local resources to file"""
        with open(self.local_resources_file, 'w') as f:
            json.dump(asdict(self.local_resources), f, indent=2)

    def load_aws_resources(self) -> bool:
        """Load AWS resources from file"""
        if self.aws_resources_file.exists():
            try:
                with open(self.aws_resources_file, 'r') as f:
                    data = json.load(f)
                self.aws_resources = AWSResources(**data)
                return True
            except Exception:
                self.aws_resources = AWSResources()
        return False

    def load_local_resources(self) -> bool:
        """Load local resources from file"""
        if self.local_resources_file.exists():
            try:
                with open(self.local_resources_file, 'r') as f:
                    data = json.load(f)
                self.local_resources = LocalResources(**data)
                return True
            except Exception:
                self.local_resources = LocalResources()
        return False

    def load_all_resources(self):
        """Load all saved resources"""
        self.load_aws_resources()
        self.load_local_resources()

    def save_all_resources(self):
        """Save all resources"""
        if self.aws_resources.iam_role_arn or self.aws_resources.ecr_repository_uri:
            self.save_aws_resources()
        if self.local_resources.registry_url:
            self.save_local_resources()

    def clear_aws_resources(self):
        """Clear AWS resources"""
        self.aws_resources = AWSResources()
        if self.aws_resources_file.exists():
            self.aws_resources_file.unlink()

    def clear_local_resources(self):
        """Clear local resources"""
        self.local_resources = LocalResources()
        if self.local_resources_file.exists():
            self.local_resources_file.unlink()

    def get_status(self) -> Dict[str, Any]:
        """Get status of all saved resources"""
        return {
            "config_directory": str(self.config_dir),
            "aws": {
                "loaded": bool(self.aws_resources.iam_role_arn),
                "iam_role": self.aws_resources.iam_role_name,
                "ecr_repository": self.aws_resources.ecr_repository_name,
                "region": self.aws_resources.region,
                "created_at": self.aws_resources.created_at
            },
            "local": {
                "loaded": bool(self.local_resources.registry_url),
                "registry_url": self.local_resources.registry_url,
                "created_at": self.local_resources.created_at
            }
        }