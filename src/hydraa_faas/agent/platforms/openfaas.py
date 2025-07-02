"""
OpenFaaS Platform Adapter
"""

import os
import json
import time
import logging
from typing import Dict, Any, List, Optional
import requests
from requests.auth import HTTPBasicAuth

from .base import FaasPlatform, PlatformError, PlatformConfig, create_deployment_spec

logger = logging.getLogger(__name__)


class OpenFaaSPlatform(FaasPlatform):
    """
    This adapter communicates with OpenFaaS through the REST API gateway
    """
    
    def __init__(self):
        """Initialize OpenFaaS platform adapter"""
        self.config = PlatformConfig('OPENFAAS')
        self._validate_config()
        
        # get config
        self.gateway_url = self.config.get_config('GATEWAY', 'http://localhost:8080', required=True)
        self.username = self.config.get_config('USERNAME')
        self.password = self.config.get_config('PASSWORD')
        self.token = self.config.get_config('TOKEN')
        self.timeout = self.config.get_int_config('TIMEOUT', 30)
        
        # ensure gateway URL doesn't end with slash
        self.gateway_url = self.gateway_url.rstrip('/')
        
        # setup authentication
        self.auth = None
        self.headers = {'Content-Type': 'application/json'}
        
        if self.token:
            self.headers['Authorization'] = f'Bearer {self.token}'
            logger.info("Using bearer token authentication for OpenFaaS")
        elif self.username and self.password:
            self.auth = HTTPBasicAuth(self.username, self.password)
            logger.info("Using basic authentication for OpenFaaS")
        else:
            logger.info("No authentication configured for OpenFaaS")
        
        logger.info(f"OpenFaaS adapter initialized with gateway: {self.gateway_url}")
    
    def _validate_config(self):
        """Validate OpenFaaS configuration"""
        # gateway URL is required
        if not self.config.get_config('GATEWAY'):
            # Use default if not provided
            pass
        
        # if username is provided, password must also be provided
        username = self.config.get_config('USERNAME')
        password = self.config.get_config('PASSWORD')
        token = self.config.get_config('TOKEN')
        
        if username and not password and not token:
            raise ValueError("OPENFAAS_PASSWORD or OPENFAAS_TOKEN is required when OPENFAAS_USERNAME is provided")
    
    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """
        Make authenticated HTTP request to OpenFaaS gateway
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (without gateway URL prefix)
            **kwargs: Additional arguments to pass to requests
            
        Returns:
            Response object
            
        Raises:
            PlatformError: If request fails
        """
        url = f"{self.gateway_url}{endpoint}"
        
        # merge headers and auth
        headers = self.headers.copy()
        if 'headers' in kwargs:
            headers.update(kwargs.pop('headers'))
        
        kwargs['headers'] = headers
        kwargs['timeout'] = kwargs.get('timeout', self.timeout)
        
        if self.auth:
            kwargs['auth'] = self.auth
        
        try:
            logger.debug(f"Making {method} request to {url}")
            response = requests.request(method, url, **kwargs)
            logger.debug(f"Response status: {response.status_code}")
            
            return response
            
        except requests.exceptions.RequestException as e:
            raise PlatformError(
                message=f"HTTP request failed: {str(e)}",
                platform="openfaas",
                operation=method.lower(),
                details={'url': url, 'error': str(e)}
            )
    
    def deploy(self, func_id: str, image: str) -> Dict[str, Any]:
        """
        Deploy function to OpenFaaS
        
        Args:
            func_id: Function identifier
            image: Docker image name
            
        Returns:
            Deployment result information
        """
        workflow = os.getenv('FAAS_WORKFLOW', 'local')
        deployment_spec = create_deployment_spec(func_id, image, workflow)
        
        # create OpenFaaS deployment payload
        payload = {
            'service': func_id,
            'image': image,
            'fProcess': 'python3 handler.py',
            'labels': deployment_spec['labels'],
            'annotations': deployment_spec['annotations'],
            'constraints': [],
            'secrets': [],
            'envVars': {
                'fprocess': 'python3 handler.py',
                'write_debug': 'true'
            }
        }
        
        # set image pull policy based on workflow
        if workflow == 'local':
            payload['annotations']['com.openfaas.scale.zero'] = 'false'
            # for local development, we don't want OpenFaaS to try pulling the image
            logger.info(f"Deploying {func_id} with local workflow (no image pull)")
        else:
            logger.info(f"Deploying {func_id} with registry workflow")
        
        try:
            # deploy function
            logger.info(f"Deploying function {func_id} to OpenFaaS")
            response = self._make_request('POST', '/system/functions', json=payload)
            
            if response.status_code == 200:
                logger.info(f"Function {func_id} deployed successfully")
                return {
                    'status': 'deployed',
                    'platform': 'openfaas',
                    'endpoint': f"{self.gateway_url}/function/{func_id}",
                    'details': deployment_spec
                }
            elif response.status_code == 409:
                # function already exists, try to update
                logger.info(f"Function {func_id} already exists, attempting update")
                return self._update_function(func_id, payload)
            else:
                error_msg = self._extract_error_message(response)
                raise PlatformError(
                    message=f"Deployment failed: {error_msg}",
                    platform="openfaas",
                    operation="deploy",
                    details={
                        'status_code': response.status_code,
                        'response': response.text,
                        'payload': payload
                    }
                )
                
        except PlatformError:
            raise
        except Exception as e:
            raise PlatformError(
                message=f"Unexpected deployment error: {str(e)}",
                platform="openfaas",
                operation="deploy",
                details={'error': str(e)}
            )
    
    def _update_function(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update existing function"""
        response = self._make_request('PUT', '/system/functions', json=payload)
        
        if response.status_code == 200:
            logger.info(f"Function {func_id} updated successfully")
            return {
                'status': 'updated',
                'platform': 'openfaas',
                'endpoint': f"{self.gateway_url}/function/{func_id}"
            }
        else:
            error_msg = self._extract_error_message(response)
            raise PlatformError(
                message=f"Update failed: {error_msg}",
                platform="openfaas",
                operation="update",
                details={
                    'status_code': response.status_code,
                    'response': response.text
                }
            )
    
    def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invoke function on OpenFaaS
        
        Args:
            func_id: Function identifier
            payload: Function input data
            
        Returns:
            Function execution result
        """
        try:
            # convert payload to JSON string
            if isinstance(payload, dict):
                data = json.dumps(payload)
            else:
                data = str(payload)
            
            logger.info(f"Invoking function {func_id}")
            start_time = time.time()
            
            # invoke function
            response = self._make_request(
                'POST',
                f'/function/{func_id}',
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            
            execution_time = time.time() - start_time
            
            if response.status_code == 200:
                try:
                    result = response.json()
                except json.JSONDecodeError:
                    result = response.text
                
                logger.info(f"Function {func_id} executed successfully in {execution_time:.2f}s")
                return {
                    'result': result,
                    'status': 'success',
                    'execution_time': execution_time,
                    'platform': 'openfaas'
                }
            else:
                error_msg = self._extract_error_message(response)
                raise PlatformError(
                    message=f"Function invocation failed: {error_msg}",
                    platform="openfaas",
                    operation="invoke",
                    details={
                        'status_code': response.status_code,
                        'response': response.text,
                        'execution_time': execution_time
                    }
                )
                
        except PlatformError:
            raise
        except Exception as e:
            raise PlatformError(
                message=f"Unexpected invocation error: {str(e)}",
                platform="openfaas",
                operation="invoke",
                details={'error': str(e)}
            )
    
    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions"""
        try:
            logger.info("Listing OpenFaaS functions")
            response = self._make_request('GET', '/system/functions')
            
            if response.status_code == 200:
                functions = response.json()
                
                # filter functions deployed by this middleware
                middleware_functions = []
                for func in functions:
                    labels = func.get('labels', {})
                    if labels.get('faas-middleware') == 'true':
                        middleware_functions.append({
                            'id': func['name'],
                            'image': func['image'],
                            'replicas': func.get('replicas', 0),
                            'available_replicas': func.get('availableReplicas', 0),
                            'labels': labels,
                            'platform': 'openfaas'
                        })
                
                return middleware_functions
            else:
                error_msg = self._extract_error_message(response)
                raise PlatformError(
                    message=f"Failed to list functions: {error_msg}",
                    platform="openfaas",
                    operation="list",
                    details={'status_code': response.status_code}
                )
                
        except PlatformError:
            raise
        except Exception as e:
            raise PlatformError(
                message=f"Unexpected error listing functions: {str(e)}",
                platform="openfaas",
                operation="list",
                details={'error': str(e)}
            )
    
    def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed function"""
        try:
            logger.info(f"Deleting function {func_id}")
            response = self._make_request('DELETE', '/system/functions', json={'functionName': func_id})
            
            if response.status_code == 200:
                logger.info(f"Function {func_id} deleted successfully")
                return {
                    'status': 'deleted',
                    'platform': 'openfaas',
                    'function_id': func_id
                }
            else:
                error_msg = self._extract_error_message(response)
                raise PlatformError(
                    message=f"Failed to delete function: {error_msg}",
                    platform="openfaas",
                    operation="delete",
                    details={'status_code': response.status_code}
                )
                
        except PlatformError:
            raise
        except Exception as e:
            raise PlatformError(
                message=f"Unexpected error deleting function: {str(e)}",
                platform="openfaas",
                operation="delete",
                details={'error': str(e)}
            )
    
    def get_function_logs(self, func_id: str, lines: int = 100) -> List[str]:
        """Get function logs"""
        try:
            logger.info(f"Getting logs for function {func_id}")
            params = {'name': func_id, 'lines': lines}
            response = self._make_request('GET', '/system/logs', params=params)
            
            if response.status_code == 200:
                # openfaas returns logs as plain text
                log_lines = response.text.strip().split('\n') if response.text.strip() else []
                return log_lines
            else:
                error_msg = self._extract_error_message(response)
                raise PlatformError(
                    message=f"Failed to get logs: {error_msg}",
                    platform="openfaas",
                    operation="logs",
                    details={'status_code': response.status_code}
                )
                
        except PlatformError:
            raise
        except Exception as e:
            raise PlatformError(
                message=f"Unexpected error getting logs: {str(e)}",
                platform="openfaas",
                operation="logs",
                details={'error': str(e)}
            )
    
    def _extract_error_message(self, response: requests.Response) -> str:
        """Extract error message from response"""
        try:
            error_data = response.json()
            return error_data.get('message', error_data.get('error', response.text))
        except json.JSONDecodeError:
            return response.text or f"HTTP {response.status_code}"