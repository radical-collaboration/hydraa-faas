"""
Cross-platform Nuclio platform adapter for FaaS agent
Handles source and image-based deployments for Minikube vs GKE.
"""

import os
import json
import time
import asyncio
import tempfile
import zipfile
import httpx
import yaml
import logging
import platform
from typing import Dict, Any, List, IO, Optional
from pathlib import Path

from .base import BasePlatform, PlatformError
from ..utils.misc import DockerUtils, DockerError

logger = logging.getLogger(__name__)


class NuclioPlatform(BasePlatform):
    """Environment-aware Nuclio platform adapter with proper architecture support"""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.nuclio_config = self.config.get('nuclio', {})
        self.k8s_config = self.config.get('kubernetes_target', {})
        self.namespace = self.nuclio_config.get('namespace', 'nuclio')
        self.registry = self.nuclio_config.get('container_registry', 'local')
        self.cluster_type = self.k8s_config.get('type', 'minikube')
        self.cleanup_before_deploy = self.nuclio_config.get('cleanup_before_deploy', True)
        self.platform = 'kube'
        self.system = platform.system().lower()
        self.machine = platform.machine().lower()
        self.arch = 'arm64' if self.machine in ['arm64', 'aarch64'] else 'amd64'
        self.base_image = self._get_base_image()
        logger.info(f"Nuclio initialized for '{self.cluster_type}' on {self.system}/{self.arch}")

    def _get_base_image(self) -> str:
        nuclio_version = "1.13.24"
        if self.system == 'darwin' and self.arch == 'arm64':
            return f"quay.io/nuclio/handler-builder-python-onbuild:{nuclio_version}-arm64"
        elif self.system == 'darwin' and self.arch == 'amd64':
            return f"quay.io/nuclio/handler-builder-python-onbuild:{nuclio_version}-amd64"
        elif self.system == 'linux' and self.arch == 'arm64':
            return f"quay.io/nuclio/handler-builder-python-onbuild:{nuclio_version}-arm64"
        else:
            return f"quay.io/nuclio/handler-builder-python-onbuild:{nuclio_version}-amd64"

    def _create_function_config(self, func_id: str, handler: str, runtime: str,
                               image_name: Optional[str] = None,
                               dynamic_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create Nuclio function configuration with dynamic overrides."""
        if ':' not in handler:
            raise PlatformError(f"Invalid handler format: {handler}. Expected 'module:function'")

        module_name, function_name = handler.split(':', 1)

        # Base configuration
        config = {
            'apiVersion': 'nuclio.io/v1beta1',
            'kind': 'NuclioFunction',
            'metadata': {
                'name': func_id,
                'namespace': self.namespace,
                'labels': {
                    'faas-agent': 'true'  # Mark functions deployed by agent
                }
            },
            'spec': {
                'handler': f'{module_name}:{function_name}',
                'runtime': runtime,
            }
        }

        # Apply dynamic configuration if provided
        if dynamic_config:
            # Use dynamic registry if provided
            registry = dynamic_config.get('registry', self.registry)

            if image_name:
                config['spec']['image'] = image_name
            else:
                config['spec']['build'] = {
                    'baseImageName': self.base_image,
                    'registry': registry
                }
                # Add build commands if provided
                if dynamic_config.get('build'):
                    config['spec']['build'].update(dynamic_config['build'])

            # Apply resources
            if dynamic_config.get('resources'):
                config['spec']['resources'] = dynamic_config['resources']

            # Apply scaling
            if 'minReplicas' in dynamic_config:
                config['spec']['minReplicas'] = dynamic_config['minReplicas']
            if 'maxReplicas' in dynamic_config:
                config['spec']['maxReplicas'] = dynamic_config['maxReplicas']

            # Apply environment variables
            if dynamic_config.get('env'):
                config['spec']['env'] = dynamic_config['env']

            # Apply labels
            if dynamic_config.get('labels'):
                config['metadata']['labels'].update(dynamic_config['labels'])

            # Apply triggers
            if dynamic_config.get('triggers'):
                config['spec']['triggers'] = dynamic_config['triggers']
        else:
            # Use defaults from config file
            if image_name:
                config['spec']['image'] = image_name
            else:
                config['spec']['build'] = {'baseImageName': self.base_image, 'registry': self.registry}

            if defaults := self.nuclio_config.get('function_defaults', {}):
                config['metadata'].update(defaults.get('metadata', {}))
                self._merge_spec_defaults(config['spec'], defaults.get('spec', {}))

        return config

    def _merge_spec_defaults(self, spec: Dict[str, Any], defaults: Dict[str, Any]):
        """Merge default configuration values, handling build commands correctly."""
        for key, value in defaults.items():
            if key in ['handler', 'runtime', 'image']: continue
            if key == 'build' and 'build' in spec:
                spec['build'].update(value)
            elif key not in spec:
                spec[key] = value

    async def deploy(self, func_name: str, handler: str, runtime: str,
                    code_stream: Optional[IO[bytes]] = None,
                    image_name: Optional[str] = None,
                    dynamic_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Deploy function with optional dynamic configuration."""
        start_time = time.time()
        deploy_type = "image" if image_name else "source"
        logger.info(f"Starting {deploy_type} deployment of {func_name} for '{self.cluster_type}' cluster")

        # Log dynamic config if provided
        if dynamic_config:
            logger.info(f"Using dynamic configuration: {json.dumps(dynamic_config, indent=2)}")

        try:
            config = self._create_function_config(func_name, handler, runtime, image_name, dynamic_config)

            with tempfile.TemporaryDirectory() as temp_dir:
                if code_stream:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                        code_stream.seek(0)
                        temp_zip.write(code_stream.read())
                        temp_zip.flush()
                        with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
                            zip_ref.extractall(temp_dir)
                        os.unlink(temp_zip.name)

                config_file = Path(temp_dir) / 'function.yaml'
                with open(config_file, 'w') as f:
                    yaml.dump(config, f)

                if self.cleanup_before_deploy:
                    await self._cleanup_existing_function(func_name)

                # Use the registry from dynamic config if provided
                registry = dynamic_config.get('registry', self.registry) if dynamic_config else self.registry

                cmd = ['nuctl', 'deploy', func_name, '--file', str(config_file),
                       '--platform', self.platform, '--namespace', self.namespace, '--verbose']
                if code_stream:
                    cmd.extend(['--path', temp_dir, '--registry', registry])

                proc = await asyncio.create_subprocess_exec(*cmd,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=temp_dir)
                stdout, stderr = await proc.communicate()

                if proc.returncode != 0:
                    raise PlatformError(f"Deployment failed: {stderr.decode()}")

                await self._wait_for_function_ready(func_name)
                endpoint_url = await self._get_function_external_url(func_name)

                return {
                    'status': 'success', 'name': func_name, 'platform': 'nuclio',
                    'deployment_time': (time.time() - start_time), 'namespace': self.namespace,
                    'handler': handler,
                    'image': image_name or config.get('spec', {}).get('build', {}).get('image'),
                    'endpoint_url': endpoint_url,
                    'dynamic_config': dynamic_config
                }
        except Exception as e:
            logger.error(f"Deployment failed for {func_name}: {e}")
            await self._log_debugging_info(func_name)
            raise PlatformError(f"Deployment failed: {e}")

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        FIXED: Invoke a function using the efficient internal Kubernetes service URL.
        This method does NOT require an Ingress to be configured for the function.
        """
        start_time = time.time()

        # Construct the internal Kubernetes DNS name for the service
        # Format: <service-name>.<namespace>.svc.cluster.local
        service_name = f"nuclio-{func_id}"
        internal_url = f"http://{service_name}.{self.namespace}.svc.cluster.local:8080"

        logger.info(f"Invoking function via internal cluster URL: {internal_url}")

        try:
            timeout = httpx.Timeout(60.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                response = await client.post(internal_url, json=payload, headers={'Content-Type': 'application/json'})

            invocation_time = time.time() - start_time
            result = response.json() if 'application/json' in response.headers.get('content-type', '') else response.text

            if response.status_code < 300:
                return {
                    'status': 'success', 'function_id': func_id, 'platform': 'nuclio',
                    'function_response': result, 'invocation_time': invocation_time,
                    'status_code': response.status_code, 'endpoint_url': internal_url
                }
            else:
                raise PlatformError(f"HTTP {response.status_code}: {response.text}", details={'url': internal_url})

        except httpx.ConnectError as e:
            logger.error(f"Internal invocation failed for {func_id}: Could not connect to service. Is it ready? Error: {e}")
            raise PlatformError(f"Internal invocation failed: Could not connect to the function's service at {internal_url}")
        except Exception as e:
            logger.error(f"Invocation failed for {func_id}: {e}")
            raise PlatformError(f"Invocation failed: {e}")

    async def _get_function_external_url(self, func_name: str) -> Optional[str]:
        """
        Get the function's EXTERNAL URL, if available (for user information).
        This is separate from the internal URL used for invocation.
        """
        if self.cluster_type == 'gke':
            ingress_ip = await self._get_ingress_ip()
            if not ingress_ip:
                return None
            # This assumes a convention where the ingress path is the same as the function name.
            return f"http://{ingress_ip}/{func_name}"
        else: # Minikube logic for NodePort
            service_name = f"nuclio-{func_name}"
            cmd = ['kubectl', 'get', 'svc', service_name, '-n', self.namespace, '-o', 'jsonpath={.spec.type}::{.spec.ports[0].nodePort}']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode == 0 and stdout:
                parts = stdout.decode().strip().split('::')
                if parts[0] == 'NodePort' and len(parts) > 1 and parts[1]:
                    minikube_ip = await self._get_minikube_ip()
                    return f"http://{minikube_ip}:{parts[1]}"
            return None # No external URL found

    # --- Other helper and interface methods remain unchanged ---
    async def _cleanup_existing_function(self, func_name: str):
        try:
            cmd = ['nuctl', 'delete', 'function', func_name, '--platform', self.platform, '--namespace', self.namespace]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await proc.communicate()
            logger.info(f"Cleaned up existing function {func_name}")
            await asyncio.sleep(3)
        except Exception as e:
            logger.warning(f"Error during cleanup (may not exist): {e}")

    async def health_check(self) -> bool:
        try:
            proc = await asyncio.create_subprocess_exec('nuctl', 'version', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await proc.communicate()
            if proc.returncode != 0: return False
            proc = await asyncio.create_subprocess_exec('kubectl', 'get', 'namespace', self.namespace, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            _, stderr = await proc.communicate()
            if proc.returncode != 0: return False
            proc = await asyncio.create_subprocess_exec('kubectl', 'get', 'pods', '-n', self.namespace, '-l', 'nuclio.io/app=controller', '--field-selector=status.phase=Running', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode != 0 or 'Running' not in stdout.decode(): return False
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def _wait_for_function_ready(self, func_name: str, max_wait: int = 300):
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                cmd = ['kubectl', 'get', 'nucliofunction', func_name, '-n', self.namespace, '-o', 'json']
                proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                stdout, _ = await proc.communicate()
                if proc.returncode == 0:
                    status = json.loads(stdout).get('status', {})
                    state = status.get('state', 'unknown')
                    if state == 'ready':
                        logger.info(f"Function {func_name} is ready!")
                        return
                    elif state in ['error', 'unhealthy']:
                        raise PlatformError(f"Function {func_name} failed: {state} - {status.get('message', '')}")
            except Exception as e:
                logger.warning(f"Error checking function status: {e}")
            await asyncio.sleep(5)
        raise PlatformError(f"Function {func_name} did not become ready within {max_wait} seconds")

    async def _get_minikube_ip(self) -> Optional[str]:
        proc = await asyncio.create_subprocess_exec('minikube', 'ip', stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0: return None
        return stdout.decode().strip()

    async def _get_ingress_ip(self) -> Optional[str]:
        ingress_config = self.k8s_config.get('ingress', {})
        ns = ingress_config.get('namespace', 'ingress-nginx')
        service = ingress_config.get('service_name', 'ingress-nginx-controller')
        cmd = ['kubectl', 'get', 'svc', service, '-n', ns, '-o', 'jsonpath={.status.loadBalancer.ingress[0].ip}']
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0: return None
        return stdout.decode().strip()

    async def _log_debugging_info(self, func_name: str):
        try:
            logger.info("=== DEBUGGING INFORMATION ===")
            cmd = ['kubectl', 'get', 'nucliofunction', func_name, '-n', self.namespace, '-o', 'yaml']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode == 0: logger.info(f"Function YAML:\n{stdout.decode()}")
            cmd = ['kubectl', 'get', 'events', '-n', self.namespace, '--field-selector', f'involvedObject.name={func_name}', '--sort-by=.lastTimestamp']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode == 0: logger.info(f"Recent events:\n{stdout.decode()}")
            cmd = ['kubectl', 'get', 'pods', '-n', self.namespace, '-l', f'nuclio.io/function-name={func_name}']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode == 0: logger.info(f"Function pods:\n{stdout.decode()}")
        except Exception as e:
            logger.error(f"Could not get debugging info: {e}")

    async def list_functions(self) -> List[Dict[str, Any]]:
        try:
            cmd = ['kubectl', 'get', 'nucliofunction', '-n', self.namespace, '-o', 'json']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode != 0: return []
            items = json.loads(stdout.decode()).get('items', [])
            functions = []
            for func in items:
                metadata = func.get('metadata', {})
                if metadata.get('labels', {}).get('faas-agent') == 'true':
                    name = metadata.get('name')
                    functions.append({
                        'id': name, 'name': name, 'namespace': metadata.get('namespace'),
                        'platform': 'nuclio', 'state': func.get('status', {}).get('state', 'unknown'),
                        'handler': func.get('spec', {}).get('handler'),
                        'created': metadata.get('creationTimestamp'),
                        'url': await self._get_function_external_url(name)
                    })
            return functions
        except Exception as e:
            logger.error(f"Error listing functions: {e}")
            return []

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        try:
            cmd = ['nuctl', 'delete', 'function', func_id, '--platform', self.platform, '--namespace', self.namespace]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            await proc.communicate()
            return {'status': 'success', 'platform': 'nuclio', 'function_id': func_id, 'message': f"Function {func_id} deleted"}
        except Exception as e:
            raise PlatformError(f"Failed to delete function: {e}")

    async def get_metrics(self) -> Dict[str, Any]:
        try:
            functions = await self.list_functions()
            status_counts = {}
            for func in functions:
                status = func.get('state', 'unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            return {
                'platform': 'nuclio', 'function_count': len(functions),
                'status_distribution': status_counts, 'registry': self.registry,
                'architecture': f"{self.system}/{self.arch}"
            }
        except Exception as e:
            return {'error': str(e)}

    async def get_platform_info(self) -> Dict[str, Any]:
        try:
            info = {
                'platform': 'nuclio', 'namespace': self.namespace,
                'registry': self.registry, 'base_image': self.base_image,
                'cluster_type': self.cluster_type, 'system': self.system,
                'architecture': self.arch
            }
            cmd = ['nuctl', 'version']
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
            stdout, _ = await proc.communicate()
            if proc.returncode == 0:
                info['nuctl_version'] = stdout.decode().strip()
            return info
        except Exception as e:
            return {'error': str(e)}