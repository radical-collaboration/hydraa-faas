"""Knative platform adapter"""

import os
import json
import time
import subprocess
import requests
from typing import Dict, Any, List

from .base import FaasPlatform, PlatformError, create_deployment_spec


class KnativePlatform(FaasPlatform):
    """Knative platform adapter implementation"""
    
    def __init__(self):
        self.domain = os.getenv('KNATIVE_DOMAIN', '192.168.49.2.nip.io')
        self.namespace = os.getenv('KNATIVE_NAMESPACE', 'default')
        self.timeout = int(os.getenv('KNATIVE_TIMEOUT', '300'))
    
    def _run_command(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run kn CLI command"""
        cmd = ['kn'] + args
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        except subprocess.TimeoutExpired:
            raise PlatformError(f"Command timed out: {' '.join(cmd)}")
        except Exception as e:
            raise PlatformError(f"Command failed: {e}")
    
    def _service_exists(self, func_id: str) -> bool:
        """Check if Knative service exists"""
        result = self._run_command(['service', 'describe', func_id, '--namespace', self.namespace])
        return result.returncode == 0
    
    def deploy(self, func_id: str, image: str) -> Dict[str, Any]:
        """Deploy function to Knative"""
        workflow = os.getenv('FAAS_WORKFLOW', 'local')
        deployment_spec = create_deployment_spec(func_id, image, workflow)
        
        # tag image with dev.local prefix for local workflow
        if workflow == 'local' and not image.startswith('dev.local/'):
            local_image = f"dev.local/{image}"
            try:
                subprocess.run(['docker', 'tag', image, local_image], check=True)
                image = local_image
            except subprocess.CalledProcessError:
                pass  # use original image if tagging fails
        
        # build deployment command
        if self._service_exists(func_id):
            cmd_args = ['service', 'update', func_id, '--image', image]
        else:
            cmd_args = [
                'service', 'create', func_id, '--image', image,
                '--port', '8080', '--env', 'fprocess=python3 handler.py'
            ]
        
        # add common arguments
        cmd_args.extend([
            '--namespace', self.namespace,
            '--pull-policy', 'Never' if workflow == 'local' else 'IfNotPresent'
        ])
        
        # add labels and annotations
        for key, value in deployment_spec['labels'].items():
            cmd_args.extend(['--label', f'{key}={value}'])
        
        for key, value in deployment_spec['annotations'].items():
            cmd_args.extend(['--annotation', f'{key}={value}'])
        
        result = self._run_command(cmd_args)
        
        if result.returncode != 0:
            raise PlatformError(f"Deployment failed: {result.stderr}")
        
        service_url = f"http://{func_id}.{self.namespace}.{self.domain}"
        
        return {
            'status': 'deployed',
            'platform': 'knative',
            'endpoint': service_url,
            'details': deployment_spec
        }
    
    def _get_service_url(self, func_id: str) -> str:
        """Get local accessible URL for the service"""
        try:
            # get kourier nodePort
            result = subprocess.run([
                'kubectl', 'get', 'svc', 'kourier',
                '-n', 'kourier-system',
                '-o', 'jsonpath={.spec.ports[0].nodePort}'
            ], capture_output=True, text=True, timeout=10)
            
            if result.returncode == 0 and result.stdout.strip():
                node_port = result.stdout.strip()
                
                # get minikube IP
                minikube_result = subprocess.run(['minikube', 'ip'], 
                                               capture_output=True, text=True, timeout=10)
                
                if minikube_result.returncode == 0:
                    ip = minikube_result.stdout.strip()
                    return f"http://{ip}:{node_port}"
        except:
            pass
        
        # fallback to default
        return f"http://{func_id}.{self.namespace}.{self.domain}"
    
    def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke function on Knative"""
        service_url = self._get_service_url(func_id)
        
        # prepare request
        headers = {
            'Content-Type': 'application/json',
            'Host': f"{func_id}.{self.namespace}.{self.domain}"
        }
        data = json.dumps(payload)
        
        start_time = time.time()
        try:
            response = requests.post(service_url, data=data, headers=headers, timeout=self.timeout)
            execution_time = time.time() - start_time
            
            if response.status_code == 200:
                try:
                    result = response.json()
                except json.JSONDecodeError:
                    result = response.text
                
                return {
                    'result': result,
                    'status': 'success',
                    'execution_time': execution_time,
                    'platform': 'knative'
                }
            else:
                raise PlatformError(f"HTTP {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            raise PlatformError(f"Request failed: {e}")
    
    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed Knative services"""
        result = self._run_command([
            'service', 'list', '--namespace', self.namespace, '--output', 'json'
        ])
        
        if result.returncode != 0:
            raise PlatformError(f"Failed to list services: {result.stderr}")
        
        services_data = json.loads(result.stdout)
        services = services_data.get('items', [])
        
        # filter services deployed by this middleware
        middleware_functions = []
        for service in services:
            labels = service.get('metadata', {}).get('labels', {})
            if labels.get('faas-middleware') == 'true':
                status = service.get('status', {})
                conditions = status.get('conditions', [])
                ready_condition = next((c for c in conditions if c.get('type') == 'Ready'), {})
                
                middleware_functions.append({
                    'id': service['metadata']['name'],
                    'url': status.get('url'),
                    'ready': ready_condition.get('status') == 'True',
                    'platform': 'knative'
                })
        
        return middleware_functions
    
    def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed Knative service"""
        result = self._run_command([
            'service', 'delete', func_id,
            '--namespace', self.namespace,
            '--wait'
        ])
        
        if result.returncode != 0:
            raise PlatformError(f"Failed to delete service: {result.stderr}")
        
        return {
            'status': 'deleted',
            'platform': 'knative',
            'function_id': func_id
        }