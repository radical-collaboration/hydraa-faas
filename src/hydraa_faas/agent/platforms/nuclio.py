"""Nuclio platform adapter"""

import os
import json
import time
import subprocess
import tempfile
import requests
import yaml
from typing import Dict, Any, List

from .base import FaasPlatform, PlatformError


class NuclioPlatform(FaasPlatform):
    """Nuclio platform adapter implementation"""

    def __init__(self):
        self.namespace = os.getenv('NUCLIO_NAMESPACE', 'nuclio')
        self.platform = os.getenv('NUCLIO_PLATFORM_KIND', 'kube')
        self.timeout = int(os.getenv('NUCLIO_TIMEOUT', '60'))
        self.dashboard_url = os.getenv('NUCLIO_DASHBOARD_URL', 'http://localhost')
    
    def _run_command(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run nuctl CLI command"""
        cmd = ['nuctl'] + args + ['--platform', self.platform, '--namespace', self.namespace]
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        except subprocess.TimeoutExpired:
            raise PlatformError(f"Command timed out: {' '.join(cmd)}")
        except Exception as e:
            raise PlatformError(f"Command failed: {e}")
    
    def _create_function_config(self, func_id: str, image: str) -> Dict[str, Any]:
        """Create nuclio function configuration"""
        workflow = os.getenv('FAAS_WORKFLOW', 'local')
        
        return {
            'metadata': {
                'name': func_id,
                'namespace': self.namespace,
                'labels': {'faas-middleware': 'true'}
            },
            'spec': {
                'runtime': 'python:3.11',
                'handler': 'handler:handler',
                'image': image,
                'imagePullPolicy': 'Never' if workflow == 'local' else 'IfNotPresent',
                'triggers': {
                    'http': {
                        'kind': 'http',
                        'maxWorkers': 1,
                        'attributes': {'port': 8080}
                    }
                }
            }
        }
    
    def deploy(self, func_id: str, image: str) -> Dict[str, Any]:
        """Deploy function to nuclio"""
        config = self._create_function_config(func_id, image)
        workflow = os.getenv('FAAS_WORKFLOW', 'local')
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config, f, default_flow_style=False)
            config_file = f.name
        
        try:
            cmd_args = ['deploy', func_id, '--file', config_file]
            if workflow == 'local':
                cmd_args.append('--no-pull')
            
            result = self._run_command(cmd_args)
            
            if result.returncode != 0 and "already exists" not in result.stderr.lower():
                raise PlatformError(f"Deployment failed: {result.stderr}")
            
            function_url = self._get_function_url(func_id)
            return {
                'status': 'deployed',
                'platform': 'nuclio',
                'endpoint': function_url
            }
        finally:
            os.unlink(config_file)
    
    def _get_function_url(self, func_id: str) -> str:
        """Get function URL for local access"""
        try:
            # need to use port forwarding for the invokation
            result = self._run_command(['get', 'function', func_id, '--output', 'json'])
            
            if result.returncode == 0:
                function_data = json.loads(result.stdout)
                internal_urls = function_data.get('status', {}).get('internalInvocationUrls', [])
                
                if internal_urls:
                    # Extract service name from internal URL
                    # Format: nuclio-FUNCTION_NAME.nuclio.svc.cluster.local:8080
                    internal_url = internal_urls[0]
                    service_name = internal_url.split('.')[0]  # nuclio-hello-nuclio
                    
                    # Return a URL that indicates port forwarding is needed
                    return f"http://localhost:8080"  # This assumes port forwarding to the service
        except:
            pass
        
        return f"http://localhost:8080"
    
    def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke function on Nuclio with automatic port forwarding"""
        import subprocess
        import time
        
        service_name = f"nuclio-{func_id}"
        local_port = 8080
        
        # start port forwarding
        port_forward_cmd = [
            'kubectl', 'port-forward', '-n', self.namespace,
            f'svc/{service_name}', f'{local_port}:8080'
        ]
        
        port_forward_process = None
        try:
            # start port forwarding in background
            port_forward_process = subprocess.Popen(
                port_forward_cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE
            )
            
            # wait for port forwarding to establish
            time.sleep(2)
            
            # make the request
            headers = {'Content-Type': 'application/json'}
            data = json.dumps(payload)
            
            start_time = time.time()
            response = requests.post(f"http://localhost:{local_port}", 
                                   data=data, headers=headers, timeout=self.timeout)
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
                    'platform': 'nuclio'
                }
            else:
                raise PlatformError(f"HTTP {response.status_code}: {response.text}")
        
        except requests.exceptions.RequestException as e:
            raise PlatformError(f"Request failed: {e}")
        finally:
            # clean up port forwarding
            if port_forward_process:
                port_forward_process.terminate()
                try:
                    port_forward_process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    port_forward_process.kill()
    
    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed Nuclio functions"""
        result = self._run_command(['get', 'functions', '--output', 'json'])
        
        if result.returncode != 0:
            raise PlatformError(f"Failed to list functions: {result.stderr}")
        
        try:
            functions_data = json.loads(result.stdout)
        except json.JSONDecodeError:
            return []
        
        if isinstance(functions_data, dict):
            functions_data = [functions_data]
        
        functions = []
        for func in functions_data:
            metadata = func.get('metadata', {})
            labels = metadata.get('labels', {})
            
            if labels.get('faas-middleware') == 'true':
                functions.append({
                    'id': metadata.get('name'),
                    'state': func.get('status', {}).get('state', 'unknown'),
                    'platform': 'nuclio'
                })
        
        return functions
    
    def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed Nuclio function"""
        result = self._run_command(['delete', 'function', func_id])
        
        if result.returncode != 0:
            raise PlatformError(f"Failed to delete: {result.stderr}")
        
        return {
            'status': 'deleted', 
            'platform': 'nuclio', 
            'function_id': func_id
        }