"""Nuclio platform adapter"""

import os
import json
import time
import asyncio
import subprocess
import tempfile
import zipfile
import httpx
import yaml
import logging
from typing import Dict, Any, List, IO
from pathlib import Path

from .base import BasePlatform, PlatformError
from ..utils.misc import DockerUtils, DockerError

logger = logging.getLogger(__name__)


class NuclioPlatform(BasePlatform):
    """Nuclio platform adapter implementation"""

    def __init__(self, platform_config: Dict[str, Any]):
        super().__init__(platform_config)
        self.namespace = platform_config.get('namespace', 'nuclio')
        self.platform = 'kube'
        self.timeout = int(os.getenv('NUCLIO_TIMEOUT', '60'))
        self.docker_utils = DockerUtils()

        # Check if we're in local workflow
        self.container_registry = platform_config.get('container_registry', 'local')
        self.is_local = self.container_registry in ['local', '', 'localhost']

    async def _run_command(self, args: List[str]) -> subprocess.CompletedProcess:
        """Run nuctl CLI command"""
        cmd = ['nuctl'] + args + ['--platform', self.platform, '--namespace', self.namespace]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)

            return type('MockResult', (), {
                'returncode': proc.returncode,
                'stdout': stdout.decode(),
                'stderr': stderr.decode()
            })()
        except asyncio.TimeoutError:
            raise PlatformError(f"Command timed out: {' '.join(cmd)}")
        except Exception as e:
            raise PlatformError(f"Command failed: {e}")

    def _create_function_config(self, func_id: str, image: str, handler: str = "handler:handler") -> Dict[str, Any]:
        """Create nuclio function configuration"""
        return {
            'apiVersion': 'nuclio.io/v1beta1',
            'kind': 'NuclioFunction',
            'metadata': {
                'name': func_id,
                'namespace': self.namespace,
                'labels': {'faas-middleware': 'true'}
            },
            'spec': {
                'runtime': 'python:3.9',
                'handler': handler,
                'image': image,
                'imagePullPolicy': 'Never' if self.is_local else 'IfNotPresent',
                'triggers': {
                    'default-http': {
                        'kind': 'http',
                        'maxWorkers': 1,
                        'attributes': {
                            'serviceType': 'ClusterIP',
                            'port': 8080
                        }
                    }
                },
                'resources': {
                    'requests': {
                        'cpu': '100m',
                        'memory': '128Mi'
                    },
                    'limits': {
                        'cpu': '1000m',
                        'memory': '512Mi'
                    }
                }
            }
        }

    async def _build_image(self, func_name: str, code_stream: IO[bytes], handler: str) -> str:
        """Build Docker image from zip file"""
        if self.is_local:
            image_name = f"nuclio-{func_name}:latest"
        else:
            image_name = f"{self.container_registry}/nuclio-{func_name}:latest"

        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract zip file
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                code_stream.seek(0)
                temp_zip.write(code_stream.read())
                temp_zip.flush()

                with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                os.unlink(temp_zip.name)

            # Create Nuclio-compatible handler wrapper
            handler_wrapper = f'''
import sys
import os
sys.path.insert(0, '/opt/nuclio')

# Import the original handler
{handler.split(':')[0] if ':' in handler else 'handler'}

def handler(context, event):
    """Nuclio handler wrapper"""
    try:
        # Get the actual handler function
        handler_module = "{handler.split(':')[0] if ':' in handler else 'handler'}"
        handler_func = "{handler.split(':')[1] if ':' in handler else 'handler'}"
        
        import importlib
        module = importlib.import_module(handler_module)
        func = getattr(module, handler_func)
        
        # Call the function
        result = func(context, event)
        return result
    except Exception as e:
        context.logger.error(f"Handler error: {{e}}")
        raise e
'''

            # Write the wrapper
            with open(os.path.join(temp_dir, 'nuclio_handler.py'), 'w') as f:
                f.write(handler_wrapper)

            # Create Dockerfile
            dockerfile_content = '''FROM python:3.9-slim

WORKDIR /opt/nuclio

# Copy all files
COPY . .

# Install dependencies if requirements.txt exists
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

# Install nuclio SDK
RUN pip install nuclio-sdk

# Set handler
ENV NUCLIO_HANDLER=nuclio_handler:handler

CMD ["python", "-m", "nuclio"]
'''

            with open(os.path.join(temp_dir, 'Dockerfile'), 'w') as f:
                f.write(dockerfile_content)

            # Build image
            logger.info(f"Building image: {image_name}")
            await asyncio.to_thread(self.docker_utils.build_image, temp_dir, image_name)

            # Push image if not local
            if not self.is_local:
                logger.info(f"Pushing image: {image_name}")
                await asyncio.to_thread(self.docker_utils.push_image, image_name)

            return image_name

    async def deploy(self, func_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> Dict[str, Any]:
        """Deploy function to nuclio"""
        start_time = time.time()

        try:
            # Build image
            image_name = await self._build_image(func_name, code_stream, handler)

            # Create function config
            config = self._create_function_config(func_name, image_name, handler)

            # Deploy using kubectl (more reliable than nuctl for complex configs)
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
                yaml.dump(config, f, default_flow_style=False)
                config_file = f.name

            try:
                # Apply the configuration
                cmd = ['kubectl', 'apply', '-f', config_file]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()

                if proc.returncode != 0:
                    raise PlatformError(f"Deployment failed: {stderr.decode()}")

                # Wait for function to be ready
                await self._wait_for_function_ready(func_name)

                deployment_time = time.time() - start_time

                return {
                    'status': 'success',
                    'name': func_name,
                    'platform': 'nuclio',
                    'deployment_time': deployment_time,
                    'details': {
                        'image': image_name,
                        'namespace': self.namespace
                    }
                }

            finally:
                os.unlink(config_file)

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise PlatformError(f"Deployment failed: {e}")

    async def _wait_for_function_ready(self, func_name: str, max_wait: int = 120):
        """Wait for function to be ready"""
        start_time = time.time()

        while time.time() - start_time < max_wait:
            try:
                result = await self._run_command(['get', 'function', func_name, '--output', 'json'])

                if result.returncode == 0:
                    function_data = json.loads(result.stdout)
                    state = function_data.get('status', {}).get('state', '')

                    if state == 'ready':
                        logger.info(f"Function {func_name} is ready")
                        return
                    elif state in ['error', 'unhealthy']:
                        raise PlatformError(f"Function {func_name} failed to deploy: {state}")

            except Exception as e:
                logger.warning(f"Error checking function status: {e}")

            await asyncio.sleep(5)

        raise PlatformError(f"Function {func_name} did not become ready within {max_wait} seconds")

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke function on Nuclio with port forwarding"""
        start_time = time.time()

        try:
            # Get function service name
            service_name = f"nuclio-{func_id}"

            # Use a unique port to avoid conflicts
            import random
            local_port = random.randint(8081, 8999)

            # Start port forwarding
            pf_cmd = ['kubectl', 'port-forward', '-n', self.namespace,
                     f'svc/{service_name}', f'{local_port}:8080']

            pf_proc = await asyncio.create_subprocess_exec(
                *pf_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # Wait for port forwarding to establish
            await asyncio.sleep(3)

            try:
                # Make the request
                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.post(
                        f"http://localhost:{local_port}",
                        json=payload,
                        headers={'Content-Type': 'application/json'}
                    )

                    if response.status_code == 200:
                        try:
                            result = response.json()
                        except:
                            result = response.text

                        invocation_time = time.time() - start_time

                        return {
                            'status': 'success',
                            'function_id': func_id,
                            'platform': 'nuclio',
                            'function_response': result,
                            'invocation_time': invocation_time
                        }
                    else:
                        raise PlatformError(f"HTTP {response.status_code}: {response.text}")

            finally:
                # Clean up port forwarding
                pf_proc.terminate()
                try:
                    await asyncio.wait_for(pf_proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    pf_proc.kill()

        except Exception as e:
            logger.error(f"Invocation failed: {e}")
            raise PlatformError(f"Invocation failed: {e}")

    async def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed Nuclio functions"""
        try:
            result = await self._run_command(['get', 'functions', '--output', 'json'])

            if result.returncode != 0:
                logger.warning(f"Failed to list functions: {result.stderr}")
                return []

            try:
                functions_data = json.loads(result.stdout)
            except json.JSONDecodeError:
                return []

            # Handle both single function and list of functions
            if isinstance(functions_data, dict):
                functions_data = [functions_data]

            functions = []
            for func in functions_data:
                metadata = func.get('metadata', {})
                labels = metadata.get('labels', {})

                # Only return functions managed by our middleware
                if labels.get('faas-middleware') == 'true':
                    functions.append({
                        'id': metadata.get('name'),
                        'state': func.get('status', {}).get('state', 'unknown'),
                        'platform': 'nuclio',
                        'namespace': metadata.get('namespace', self.namespace)
                    })

            return functions

        except Exception as e:
            logger.error(f"Error listing functions: {e}")
            return []

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed Nuclio function"""
        try:
            result = await self._run_command(['delete', 'function', func_id])

            if result.returncode != 0:
                # Don't raise error if function doesn't exist
                if "not found" in result.stderr.lower():
                    logger.warning(f"Function {func_id} not found")
                else:
                    logger.error(f"Failed to delete function: {result.stderr}")

            return {
                'status': 'success',
                'platform': 'nuclio',
                'function_id': func_id,
                'message': f"Function {func_id} deleted successfully"
            }

        except Exception as e:
            logger.error(f"Error deleting function: {e}")
            raise PlatformError(f"Failed to delete function: {e}")