"""
ECR (Elastic Container Registry) helper utilities
"""

import os
import json
import base64
import subprocess
from exceptions import ECRException


class ECRHelper:
    """Helper class for ECR operations"""

    def __init__(self, ecr_client, region_name, logger):
        self.ecr_client = ecr_client
        self.region_name = region_name
        self.logger = logger

    def create_repository(self, repository_name:str, image_tag_mutability:str='MUTABLE'):
        """
        Create ECR repository

        Args:
            repository_name: Name of the repository
            image_tag_mutability: MUTABLE or IMMUTABLE

        Returns:
            str: Repository URI

        Raises:
            ECRException: If repository creation fails
        """
        try:
            response = self.ecr_client.create_repository(
                repositoryName=repository_name,
                imageTagMutability=image_tag_mutability,
                imageScanningConfiguration={'scanOnPush': True}
            )

            repo_uri = response['repository']['repositoryUri']
            self.logger.trace(f"Created ECR repository: {repo_uri}")
            return repo_uri

        except self.ecr_client.exceptions.RepositoryAlreadyExistsException:
            # repository exists, get its uri
            response = self.ecr_client.describe_repositories(
                repositoryNames=[repository_name]
            )
            repo_uri = response['repositories'][0]['repositoryUri']
            self.logger.trace(f"Using existing ECR repository: {repo_uri}")
            return repo_uri

        except Exception as e:
            raise ECRException(f"Failed to create ECR repository '{repository_name}': {str(e)}")

    def delete_repository(self, repository_name:str, force:bool=True):
        """
        Delete ECR repository

        Args:
            repository_name: Name of the repository
            force: Delete even if repository contains images

        Raises:
            ECRException: If repository deletion fails
        """
        try:
            self.ecr_client.delete_repository(
                repositoryName=repository_name,
                force=force
            )
            self.logger.trace(f"Deleted ECR repository: {repository_name}")

        except self.ecr_client.exceptions.RepositoryNotFoundException:
            self.logger.trace(f"ECR repository '{repository_name}' not found, skipping deletion")

        except Exception as e:
            raise ECRException(f"Failed to delete ECR repository '{repository_name}': {str(e)}")

    def get_authorization_token(self):
        """
        Get Docker authorization token for ECR

        Returns:
            tuple: (username, password, registry_url)

        Raises:
            ECRException: If authorization fails
        """
        try:
            response = self.ecr_client.get_authorization_token()
            auth_data = response['authorizationData'][0]

            token = auth_data['authorizationToken']
            registry_url = auth_data['proxyEndpoint']

            # decode the base64 token
            decoded_token = base64.b64decode(token).decode('utf-8')
            username, password = decoded_token.split(':', 1)

            return username, password, registry_url

        except Exception as e:
            raise ECRException(f"Failed to get ECR authorization token: {str(e)}")

    def build_and_push_image(self, source_path, repo_uri, image_tag='latest'):
        """
        Build Docker image and push to ECR

        Args:
            source_path (str): Path to directory containing Dockerfile
            repo_uri (str): ECR repository URI
            image_tag (str): Image tag

        Returns:
            str: Full image URI with tag

        Raises:
            ECRException: If build or push fails
        """
        if not os.path.exists(os.path.join(source_path, 'Dockerfile')):
            raise ECRException(f"Dockerfile not found in {source_path}")

        full_image_uri = f"{repo_uri}:{image_tag}"

        try:
            # get ecr login credentials
            username, password, registry_url = self.get_authorization_token()

            # docker login to ecr
            self._docker_login(username, password, registry_url)

            # build the image
            self._docker_build(source_path, full_image_uri)

            # push the image
            self._docker_push(full_image_uri)

            self.logger.trace(f"Successfully built and pushed image: {full_image_uri}")
            return full_image_uri

        except Exception as e:
            raise ECRException(f"Failed to build and push image: {str(e)}")

    def _docker_login(self, username, password, registry_url):
        """Login to Docker registry with keychain cleanup"""
        import subprocess

        # Clear existing keychain entry to avoid conflicts
        try:
            subprocess.run(['security', 'delete-internet-password', '-s', registry_url],
                           capture_output=True, check=False)
            self.logger.trace(f"Cleared existing keychain entry for {registry_url}")
        except:
            pass  # Ignore if entry doesn't exist

        # Now do the normal docker login
        cmd = ['docker', 'login', '--username', username, '--password-stdin', registry_url]

        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate(input=password)

        if process.returncode != 0:
            raise ECRException(f"Docker login failed: {stderr}")

        self.logger.trace("Docker login successful")

    def _docker_build(self, source_path, image_uri):
        """Build Docker image with Docker manifest format for Lambda"""
        # Set environment to disable BuildKit (forces Docker manifest format)
        env = os.environ.copy()
        env['DOCKER_BUILDKIT'] = '0'  # This is the key fix!

        cmd = ['docker', 'build', '--platform', 'linux/amd64', '-t', image_uri, source_path]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env  # Forces compatible manifest format
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise ECRException(f"Docker build failed: {stderr}")

        self.logger.trace(f"Docker build completed for {image_uri} with Lambda-compatible format")

    def _docker_push(self, image_uri):
        """Push Docker image to registry"""
        cmd = ['docker', 'push', image_uri]

        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        stdout, stderr = process.communicate()

        if process.returncode != 0:
            raise ECRException(f"Docker push failed: {stderr}")

        self.logger.trace(f"Docker push completed for {image_uri}")

    def list_images(self, repository_name):
        """
        List images in ECR repository

        Args:
            repository_name (str): Name of the repository

        Returns:
            list: List of image details
        """
        try:
            response = self.ecr_client.list_images(repositoryName=repository_name)
            return response['imageIds']

        except Exception as e:
            raise ECRException(f"Failed to list images in repository '{repository_name}': {str(e)}")

    def repository_exists(self, repository_name):
        """
        Check if ECR repository exists

        Args:
            repository_name (str): Name of the repository

        Returns:
            bool: True if repository exists
        """
        try:
            self.ecr_client.describe_repositories(repositoryNames=[repository_name])
            return True
        except self.ecr_client.exceptions.RepositoryNotFoundException:
            return False
        except Exception:
            return False