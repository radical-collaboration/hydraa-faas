"""
Knative platform adapter for FaaS agent
Handles deployment, invocation, and management of Knative services
"""

import os
import time
import asyncio
import tempfile
import zipfile
import json
import httpx
import logging
from typing import Dict, Any, List, IO, Optional
from .base import BasePlatform, PlatformError
from ..utils.misc import DockerUtils, DockerError

logger = logging.getLogger(__name__)


class KnativePlatform(BasePlatform):
    """Optimized Knative platform implementation"""

    RUNTIME_TO_BASE_IMAGE = {
        "python:3.8": "python:3.8-slim",
        "python:3.9": "python:3.9-slim",
        "python:3.10": "python:3.10-slim",
        "python:3.11": "python:3.11-slim",
        "node:16": "node:16-alpine",
        "node:18": "node:18-alpine",
        "node:20": "node:20-alpine",
        "go:1.19": "golang:1.19-alpine",
        "go:1.20": "golang:1.20-alpine",
        "go:1.21": "golang:1.21-alpine"
    }

    def __init__(self, platform_config: Dict[str, Any]):
        super().__init__(platform_config)
        self.docker_utils = DockerUtils()
        self.namespace = platform_config.get('namespace', 'default')
        self.registry = platform_config.get('container_registry', 'local')
        self.image_cache = {}

    def _is_local_registry(self) -> bool:
        """Check if using local registry"""
        local_registries = ["", "local", "dev.local", "localhost", "minikube"]
        return self.registry.lower() in local_registries or self.registry.startswith("localhost:")

    async def health_check(self) -> bool:
        """Check if Knative platform is healthy"""
        try:
            # check if kn cli is available
            proc = await asyncio.create_subprocess_exec(
                'kn', 'version',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                logger.error(f"kn CLI not available: {stderr.decode()}")
                return False

            # check if kubectl can connect to cluster
            proc = await asyncio.create_subprocess_exec(
                'kubectl', 'get', 'namespace', self.namespace,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                logger.error(f"Cannot access namespace {self.namespace}: {stderr.decode()}")
                return False

            # check knative serving components
            proc = await asyncio.create_subprocess_exec(
                'kubectl', 'get', 'pods', '-n', 'knative-serving',
                '--field-selector=status.phase=Running',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            running_pods = len([line for line in stdout.decode().split('\n') if 'Running' in line])
            if running_pods < 3:  # Expect at least controller, webhook, activator
                logger.warning(f"Only {running_pods} Knative pods running")
                return False

            return True

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    async def _build_and_push_image(self, image_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> None:
        """Build and optionally push Docker image"""
        # Check cache first
        cache_key = f"{image_name}_{handler}_{runtime}"
        if cache_key in self.image_cache:
            logger.info(f"Using cached image: {image_name}")
            return

        # Normalize runtime
        if "python" in runtime and ":" not in runtime:
            runtime = runtime.replace("python", "python:")

        base_image = self.RUNTIME_TO_BASE_IMAGE.get(runtime)
        if not base_image:
            raise PlatformError(f"Unsupported runtime: '{runtime}'")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Extract code
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                code_stream.seek(0)
                temp_zip.write(code_stream.read())
                temp_zip.flush()

                with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                os.unlink(temp_zip.name)

            # Create optimized server wrapper
            server_wrapper = self._create_server_wrapper(handler, runtime)
            with open(os.path.join(temp_dir, "server.py"), "w") as f:
                f.write(server_wrapper)

            # Create optimized Dockerfile
            dockerfile_content = self._create_dockerfile(base_image, runtime)
            with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
                f.write(dockerfile_content)

            # Build image
            logger.info(f"Building image: {image_name}")
            await asyncio.to_thread(self.docker_utils.build_image, temp_dir, image_name)

            # Push if not local registry
            if not self._is_local_registry():
                logger.info(f"Pushing image: {image_name}")
                await asyncio.to_thread(self.docker_utils.push_image, image_name)

            # Cache successful build
            self.image_cache[cache_key] = time.time()

    def _create_server_wrapper(self, handler: str, runtime: str) -> str:
        """Create optimized HTTP server wrapper"""
        if runtime.startswith('python'):
            return f'''
import sys
import os
import json
import importlib
from flask import Flask, request, jsonify
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Import handler
try:
    module_name, func_name = "{handler}".split(":")
    handler_module = importlib.import_module(module_name)
    handler_func = getattr(handler_module, func_name)
    logger.info(f"Successfully imported {{module_name}}:{{func_name}}")
except Exception as e:
    logger.error(f"Failed to import handler: {{e}}")
    handler_func = None

@app.route("/", methods=["GET", "POST", "PUT", "DELETE"])
def handle_request():
    if not handler_func:
        return jsonify({{"error": "Handler not available"}}), 500
    
    try:
        # Prepare event data
        event_data = {{
            "method": request.method,
            "path": request.path,
            "headers": dict(request.headers),
            "query": dict(request.args),
            "body": request.get_json() if request.is_json else request.data.decode() if request.data else None
        }}
        
        # Create mock context
        class MockContext:
            def __init__(self):
                self.logger = logger
        
        # Call handler
        result = handler_func(MockContext(), event_data)
        
        # Handle different response formats
        if isinstance(result, dict):
            if "statusCode" in result:
                return jsonify(result.get("body", result)), result.get("statusCode", 200)
            return jsonify(result)
        elif isinstance(result, str):
            return result
        else:
            return jsonify({{"result": result}})
            
    except Exception as e:
        logger.error(f"Handler error: {{e}}")
        return jsonify({{"error": str(e)}}), 500

@app.route("/health")
def health():
    return jsonify({{"status": "healthy", "handler": "{handler}"}})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
'''
        elif runtime.startswith('node'):
            return f'''
const express = require('express');
const app = express();
const port = process.env.PORT || 8080;

app.use(express.json());

// Import handler
let handler;
try {{
    const handlerPath = './{handler.split(":")[0]}';
    const handlerModule = require(handlerPath);
    handler = handlerModule['{handler.split(":")[1]}'];
    console.log('Handler imported successfully');
}} catch (e) {{
    console.error('Failed to import handler:', e);
}}

app.all('/', async (req, res) => {{
    if (!handler) {{
        return res.status(500).json({{ error: 'Handler not available' }});
    }}
    
    try {{
        const event = {{
            method: req.method,
            path: req.path,
            headers: req.headers,
            query: req.query,
            body: req.body
        }};
        
        const context = {{ logger: console }};
        const result = await handler(context, event);
        
        if (result && typeof result === 'object' && result.statusCode) {{
            res.status(result.statusCode).json(result.body || result);
        }} else {{
            res.json(result);
        }}
    }} catch (e) {{
        console.error('Handler error:', e);
        res.status(500).json({{ error: e.message }});
    }}
}});

app.get('/health', (req, res) => {{
    res.json({{ status: 'healthy', handler: '{handler}' }});
}});

app.listen(port, '0.0.0.0', () => {{
    console.log(`Server running on port ${{port}}`);
}});
'''
        else:
            raise PlatformError(f"Server wrapper not implemented for runtime: {runtime}")

    def _create_dockerfile(self, base_image: str, runtime: str) -> str:
        """Create optimized Dockerfile"""
        if runtime.startswith('python'):
            return f'''FROM {base_image}

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt* ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copy application code
COPY . .

# Install Flask for the server wrapper
RUN pip install --no-cache-dir flask gunicorn

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8080/health || exit 1

# Run with gunicorn for production
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--timeout", "60", "server:app"]
'''
        elif runtime.startswith('node'):
            return f'''FROM {base_image}

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production && npm cache clean --force

# Copy application code
COPY . .

# Install express
RUN npm install express

# Create non-root user
RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs
USER nextjs

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:8080/health || exit 1

# Start server
CMD ["node", "server.js"]
'''
        else:
            raise PlatformError(f"Dockerfile not implemented for runtime: {runtime}")

    async def deploy(self, func_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> Dict[str, Any]:
        """Deploy function to Knative"""
        start_time = time.time()

        try:
            # Determine image name
            if self._is_local_registry():
                image_name = f"local/{func_name}:latest"
            else:
                image_name = f"{self.registry}/{func_name}:latest"

            # Build and push image
            await self._build_and_push_image(image_name, handler, runtime, code_stream)

            # Deploy with kn CLI
            cmd = [
                'kn', 'service', 'apply', func_name,
                '--image', image_name,
                '--namespace', self.namespace,
                '--port', '8080',
                '--timeout', '300',
                '--env', f'HANDLER={handler}',
                '--env', f'RUNTIME={runtime}',
                '--annotation', 'autoscaling.knative.dev/minScale=0',
                '--annotation', 'autoscaling.knative.dev/maxScale=10',
                '--limit', 'cpu=1000m,memory=512Mi',
                '--request', 'cpu=100m,memory=128Mi',
                '--wait'
            ]

            logger.info(f"Deploying {func_name} with command: {' '.join(cmd)}")

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown deployment error"
                logger.error(f"Deployment failed: {error_msg}")
                raise PlatformError(f"Deployment failed: {error_msg}")

            # Get service URL
            service_url = await self._get_service_url(func_name)

            deployment_time = time.time() - start_time
            logger.info(f"Successfully deployed {func_name} in {deployment_time:.2f}s")

            return {
                "status": "success",
                "name": func_name,
                "image": image_name,
                "url": service_url,
                "namespace": self.namespace,
                "deployment_time": deployment_time
            }

        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise PlatformError(f"Deployment failed: {e}")

    async def _get_service_url(self, func_name: str) -> Optional[str]:
        """Get Knative service URL"""
        try:
            cmd = ['kn', 'service', 'describe', func_name, '--namespace', self.namespace, '-o', 'url']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0:
                return stdout.decode().strip()

        except Exception as e:
            logger.warning(f"Could not get service URL: {e}")

        return None

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke Knative service using reliable kubectl proxy method"""
        start_time = time.time()

        try:
            # Get current revision name
            revision_name = await self._get_current_revision(func_id)
            if not revision_name:
                raise PlatformError(f"Service {func_id} not found or no ready revision")

            # Use kubectl proxy for reliable access (works on all platforms)
            proxy_port = await self._start_kubectl_proxy()

            try:
                # Construct private service URL (has actual endpoints)
                service_name = f"{revision_name}-private"
                proxy_url = f"http://localhost:{proxy_port}/api/v1/namespaces/{self.namespace}/services/{service_name}:80/proxy"

                logger.info(f"Invoking via kubectl proxy: {proxy_url}")

                # Invoke service via proxy
                timeout = httpx.Timeout(60.0)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(
                        proxy_url,
                        json=payload,
                        headers={'Content-Type': 'application/json'}
                    )
                    response.raise_for_status()

                    invocation_time = time.time() - start_time

                    return {
                        "status": "success",
                        "function_response": response.json() if response.headers.get('content-type', '').startswith('application/json') else response.text,
                        "invocation_time": invocation_time,
                        "status_code": response.status_code
                    }

            finally:
                # Clean up proxy
                await self._stop_kubectl_proxy(proxy_port)

        except httpx.TimeoutException:
            raise PlatformError(f"Invocation timeout for {func_id}")
        except httpx.HTTPStatusError as e:
            raise PlatformError(f"HTTP {e.response.status_code}: {e.response.text}")
        except Exception as e:
            logger.error(f"Invocation failed: {e}")
            raise PlatformError(f"Invocation failed: {e}")

    async def _get_current_revision(self, func_id: str) -> Optional[str]:
        """Get the current ready revision name"""
        try:
            cmd = ['kubectl', 'get', 'ksvc', func_id, '-n', self.namespace, '-o', 'jsonpath={.status.latestReadyRevisionName}']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0 and stdout:
                return stdout.decode().strip()

        except Exception as e:
            logger.warning(f"Could not get current revision: {e}")

        return None

    async def _start_kubectl_proxy(self) -> int:
        """Start kubectl proxy and return the port"""
        import random

        # Try to find an available port
        for attempt in range(5):
            port = random.randint(8000, 8999)
            try:
                # Check if port is available
                import socket
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                result = sock.connect_ex(('localhost', port))
                sock.close()

                if result != 0:  # Port is available
                    # start kubectl proxy
                    proc = await asyncio.create_subprocess_exec(
                        'kubectl', 'proxy', f'--port={port}',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    # wait for proxy to start
                    await asyncio.sleep(2)

                    # verify proxy is running
                    try:
                        async with httpx.AsyncClient(timeout=5.0) as client:
                            response = await client.get(f"http://localhost:{port}/api/v1")
                            if response.status_code == 200:
                                logger.info(f"kubectl proxy started on port {port}")
                                return port
                    except:
                        pass

            except Exception as e:
                logger.warning(f"Failed to start proxy on port {port}: {e}")

        raise PlatformError("Could not start kubectl proxy on any available port")

    async def _stop_kubectl_proxy(self, port: int):
        """Stop kubectl proxy"""
        try:
            # find and kill the proxy process (cross platform)
            if os.name == 'nt':  # Windows
                cmd = ['taskkill', '/F', '/IM', 'kubectl.exe']
            else:  # Linux/macOS
                cmd = ['pkill', '-f', f'kubectl proxy --port={port}']

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            await proc.communicate()

        except Exception as e:
            logger.warning(f"Could not stop kubectl proxy: {e}")

    async def list_functions(self) -> List[Dict[str, Any]]:
        """List Knative services"""
        try:
            cmd = ['kn', 'service', 'list', '--namespace', self.namespace, '-o', 'json']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                logger.error(f"Failed to list services: {stderr.decode()}")
                return []

            result = json.loads(stdout.decode())
            services = result.get("items", [])

            # format services for consistent response
            formatted_services = []
            for service in services:
                metadata = service.get("metadata", {})
                status = service.get("status", {})

                formatted_services.append({
                    "id": metadata.get("name"),
                    "name": metadata.get("name"),
                    "namespace": metadata.get("namespace"),
                    "platform": "knative",
                    "status": self._get_service_status(status),
                    "url": status.get("url"),
                    "created": metadata.get("creationTimestamp"),
                    "ready": status.get("conditions", [{}])[-1].get("status") == "True"
                })

            return formatted_services

        except Exception as e:
            logger.error(f"Error listing functions: {e}")
            return []

    def _get_service_status(self, status: Dict[str, Any]) -> str:
        """Extract service status from Knative status object"""
        conditions = status.get("conditions", [])
        if not conditions:
            return "unknown"

        ready_condition = next((c for c in conditions if c.get("type") == "Ready"), conditions[-1])
        return "ready" if ready_condition.get("status") == "True" else "not_ready"

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete Knative service"""
        try:
            cmd = ['kn', 'service', 'delete', func_id, '--namespace', self.namespace, '--wait']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown deletion error"
                logger.warning(f"Delete command failed: {error_msg}")
                # dont raise error service might not exist

            # clear from image cache
            cache_keys_to_remove = [key for key in self.image_cache.keys() if func_id in key]
            for key in cache_keys_to_remove:
                del self.image_cache[key]

            return {"message": f"Deletion request for '{func_id}' sent"}

        except Exception as e:
            logger.error(f"Error deleting function: {e}")
            return {"message": f"Deletion failed for '{func_id}': {e}"}

    async def get_metrics(self) -> Dict[str, Any]:
        """Get Knative platform metrics"""
        try:
            # get basic cluster metrics
            cmd = ['kubectl', 'top', 'pods', '--namespace', self.namespace, '--no-headers']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            pod_metrics = []
            if proc.returncode == 0:
                for line in stdout.decode().split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 3:
                            pod_metrics.append({
                                'name': parts[0],
                                'cpu': parts[1],
                                'memory': parts[2]
                            })

            # get service count
            services = await self.list_functions()

            return {
                'platform': 'knative',
                'namespace': self.namespace,
                'service_count': len(services),
                'pod_metrics': pod_metrics,
                'image_cache_size': len(self.image_cache),
                'registry': self.registry,
                'is_local_registry': self._is_local_registry()
            }

        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return {'error': str(e)}

    async def get_platform_info(self) -> Dict[str, Any]:
        """Get detailed Knative platform information"""
        try:
            info = {
                'platform': 'knative',
                'namespace': self.namespace,
                'registry': self.registry,
                'local_registry': self._is_local_registry()
            }

            # get Knative version
            cmd = ['kn', 'version']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0:
                info['kn_version'] = stdout.decode().strip()

            # get Knative serving status
            cmd = ['kubectl', 'get', 'pods', '-n', 'knative-serving', '-o', 'json']
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()

            if proc.returncode == 0:
                pods_data = json.loads(stdout.decode())
                knative_pods = []
                for pod in pods_data.get('items', []):
                    knative_pods.append({
                        'name': pod['metadata']['name'],
                        'status': pod['status']['phase'],
                        'ready': all(c['status'] == 'True' for c in pod['status'].get('conditions', []))
                    })
                info['knative_pods'] = knative_pods

            # get supported runtimes
            info['supported_runtimes'] = list(self.RUNTIME_TO_BASE_IMAGE.keys())

            return info

        except Exception as e:
            logger.error(f"Error getting platform info: {e}")
            return {'error': str(e)}