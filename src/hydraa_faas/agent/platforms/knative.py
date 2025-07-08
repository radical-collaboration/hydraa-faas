"""Knative platform adapter using kn CLI with custom function deployment

This implementation builds Docker images from uploaded zip files and deploys them
using the kn CLI for better networking compatibility.

FIXED VERSION - Skips push for local registries
"""

import os
import time
import asyncio
import tempfile
import zipfile
import json
import httpx
import logging
from typing import Dict, Any, List, IO
from .base import BasePlatform, PlatformError
from ..utils.misc import DockerUtils, DockerError

class KnativePlatform(BasePlatform):
    """Knative platform implementation"""

    RUNTIME_TO_BASE_IMAGE = {
        "python:3.9": "python:3.9-slim",
        "python:3.10": "python:3.10-slim",
        "python:3.11": "python:3.11-slim",
        "python:3.8": "python:3.8-slim",
        "python3.9": "python:3.9-slim",
        "python3.10": "python:3.10-slim",
        "python3.11": "python:3.11-slim",
        "python3.8": "python:3.8-slim",
        "node:18": "node:18-alpine",
        "node:20": "node:20-alpine",
        "go:1.21": "golang:1.21-alpine"
    }

    def __init__(self, platform_config: Dict[str, Any]):
        super().__init__(platform_config)
        self.docker_utils = DockerUtils()

    def _is_local_registry(self) -> bool:
        """Check if we're using a local registry that doesn't need push"""
        registry = self.config.get("container_registry", "")
        local_registries = ["", "local", "dev.local", "localhost", "minikube"]
        return registry.lower() in local_registries or registry.startswith("localhost:")

    async def _build_and_push_image(self, image_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> None:
        if "python" in runtime and ":" not in runtime:
            runtime = runtime.replace("python", "python:")
        base_image = self.RUNTIME_TO_BASE_IMAGE.get(runtime)
        if not base_image:
            raise PlatformError(f"unsupported runtime: '{runtime}'.")

        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                code_stream.seek(0)
                temp_zip.write(code_stream.read())
                temp_zip.flush()
                with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
                os.unlink(temp_zip.name)

            # FIXED WSGI wrapper with proper indentation
            wsgi_wrapper = f'''import sys, os, importlib, json
from urllib.parse import parse_qs
sys.path.insert(0, "/app")

def application(environ, start_response):
    try:
        if environ.get("REQUEST_METHOD") == "GET" and environ.get("PATH_INFO", "/") == "/":
            start_response("200 OK", [("Content-Type", "application/json")])
            return [json.dumps({{"status": "healthy"}}).encode("utf-8")]
        
        content_length = int(environ.get("CONTENT_LENGTH", 0))
        if content_length > 0:
            request_body = environ["wsgi.input"].read(content_length).decode("utf-8")
            try:
                event_data = json.loads(request_body)
            except json.JSONDecodeError:
                event_data = {{"body": request_body}}
        else:
            event_data = {{}}
        
        m, f = "{handler}".split(":")
        handler_module = importlib.import_module(m)
        handler_func = getattr(handler_module, f)
        
        result = handler_func(None, event_data)
        
        status_code = result.get("statusCode", 200)
        body = result.get("body", "")
        headers = result.get("headers", {{}})
        
        if isinstance(body, dict):
            body = json.dumps(body)
        elif not isinstance(body, str):
            body = str(body)
        
        response_headers = [("Content-Type", headers.get("Content-Type", "application/json"))]
        for key, value in headers.items():
            if key.lower() != "content-type":
                response_headers.append((key, str(value)))
        
        start_response(f"{{status_code}} OK", response_headers)
        return [body.encode("utf-8")]
        
    except Exception as e:
        import traceback
        error_details = {{
            "error": str(e),
            "error_type": type(e).__name__,
            "traceback": traceback.format_exc()
        }}
        start_response("500 Internal Server Error", [("Content-Type", "application/json")])
        return [json.dumps(error_details).encode("utf-8")]

app = application
'''

            with open(os.path.join(temp_dir, "wsgi_app.py"), "w") as f:
                f.write(wsgi_wrapper)

            dockerfile_content = f'''FROM {base_image}
WORKDIR /app
COPY . .
RUN if [ -f /etc/apt/sources.list ]; then apt-get update && apt-get install -y curl; fi
RUN pip install gunicorn
RUN if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
EXPOSE 8080
CMD ["gunicorn", "-b", "0.0.0.0:8080", "wsgi_app:app"]'''

            with open(os.path.join(temp_dir, "Dockerfile"), "w") as f:
                f.write(dockerfile_content)

            # Always build the image
            await asyncio.to_thread(self.docker_utils.build_image, temp_dir, image_name)

            # Only push if not using local registry
            if not self._is_local_registry():
                logging.info(f"Pushing image to registry: {image_name}")
                await asyncio.to_thread(self.docker_utils.push_image, image_name)
            else:
                logging.info(f"Skipping push for local registry: {image_name}")

    async def deploy(self, func_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> Dict[str, Any]:
        namespace = self.config.get("namespace", "default")
        registry = self.config.get("container_registry", "local")

        # Handle empty registry or local registries
        if self._is_local_registry():
            # For local registries, use a simple image name that doesn't need pushing
            image_destination = f"local/{func_name}:latest"
        else:
            image_destination = f"{registry}/{func_name}:latest"

        await self._build_and_push_image(image_destination, handler, runtime, code_stream)

        # Deploy with kn CLI
        cmd = ["kn", "service", "apply", func_name, "--image", image_destination,
               "--namespace", namespace, "--port", "8080", "--timeout", "300", "--wait"]

        try:
            process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown deployment error"
                logging.error(f"kn service apply failed: {error_msg}")
                raise PlatformError(f"'kn service apply' failed: {error_msg}")

            logging.info(f"Successfully deployed {func_name}")
            return {"status": "success", "name": func_name}

        except FileNotFoundError:
            raise PlatformError("the 'kn' command is not installed.")
        except Exception as e:
            logging.error(f"Deployment failed: {e}")
            raise PlatformError(f"Deployment failed: {e}")

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        namespace = self.config.get("namespace", "default")
        try:
            # First try to get the service URL
            cmd = ["kn", "service", "describe", func_id, "--namespace", namespace, "-o", "url"]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                raise PlatformError(f"Service {func_id} not found: {stderr.decode()}")

            service_url = stdout.decode().strip()
            logging.info(f"Invoking service at URL: {service_url}")

            # Try direct URL invocation first
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(service_url, json=payload)
                response.raise_for_status()
                return {"status": "success", "function_response": response.json()}

        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logging.info(f"Direct URL invocation failed ({e}). Trying kubectl port-forward fallback...")

            # Fallback to port-forward approach
            pf_proc = None
            try:
                pod_name = None
                # Retry logic to find the pod
                for attempt in range(5):
                    pod_cmd = ["kubectl", "get", "pods", "--namespace", namespace,
                             "-l", f"serving.knative.dev/service={func_id}",
                             "-o", "jsonpath={.items[0].metadata.name}"]
                    pod_proc = await asyncio.create_subprocess_exec(*pod_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                    pod_stdout, _ = await pod_proc.communicate()
                    pod_name = pod_stdout.decode().strip()

                    if pod_name and pod_name != "":
                        logging.info(f"Found pod '{pod_name}' for service '{func_id}' on attempt {attempt + 1}.")
                        break

                    logging.warning(f"Attempt {attempt + 1} to find pod for service '{func_id}' failed. Retrying in 2 seconds...")
                    await asyncio.sleep(2)

                if not pod_name or pod_name == "":
                    raise PlatformError(f"No pods found for service '{func_id}' in namespace '{namespace}' after multiple attempts.")

                # Start port-forward
                pf_cmd = ["kubectl", "port-forward", "--namespace", namespace, pod_name, "8080:8080"]
                pf_proc = await asyncio.create_subprocess_exec(*pf_cmd)
                await asyncio.sleep(3)  # Wait for port-forward to establish

                # Invoke via port-forward
                async with httpx.AsyncClient(timeout=60.0) as client:
                    response = await client.post("http://localhost:8080", json=payload)
                    response.raise_for_status()
                    return {"status": "success", "function_response": response.json()}

            except Exception as pf_error:
                logging.error(f"Port-forward invocation failed: {pf_error}", exc_info=True)
                raise PlatformError(f"Port-forward invocation failed: {pf_error}")
            finally:
                if pf_proc:
                    pf_proc.terminate()
                    try:
                        await asyncio.wait_for(pf_proc.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pf_proc.kill()

        except Exception as e:
            logging.error(f"Invocation failed: {e}", exc_info=True)
            raise PlatformError(f"Invocation failed: {e}")

    async def list_functions(self) -> List[Dict[str, Any]]:
        namespace = self.config.get("namespace", "default")
        try:
            cmd = ["kn", "service", "list", "--namespace", namespace, "-o", "json"]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                logging.error(f"Failed to list services: {stderr.decode()}")
                return []

            result = json.loads(stdout.decode())
            return result.get("items", [])

        except Exception as e:
            logging.error(f"Error listing functions: {e}")
            return []

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        namespace = self.config.get("namespace", "default")
        try:
            cmd = ["kn", "service", "delete", func_id, "--namespace", namespace, "--wait"]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()

            if proc.returncode != 0:
                error_msg = stderr.decode() if stderr else "Unknown deletion error"
                logging.warning(f"Delete command failed: {error_msg}")
                # Don't raise error for delete - service might not exist

            return {"message": f"Deletion request for '{func_id}' sent."}

        except Exception as e:
            logging.error(f"Error deleting function: {e}")
            return {"message": f"Deletion failed for '{func_id}': {e}"}