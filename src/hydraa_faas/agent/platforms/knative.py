"""Knative platform adapter using kn CLI with custom function deployment

This implementation builds Docker images from uploaded zip files and deploys them
using the kn CLI for better networking compatibility.
"""

import os
import time
import asyncio
import tempfile
import zipfile
import json
import httpx
from typing import Dict, Any, List, IO
from .base import BasePlatform, PlatformError
from ..utils.misc import DockerUtils, DockerError

class KnativePlatform(BasePlatform):
    """Knative platform implementation"""

    RUNTIME_TO_BASE_IMAGE = {
        "python:3.9": "python:3.9-slim",
        "python:3.10": "python:3.10-slim",
        "python:3.11": "python:3.11-slim",
        "node:18": "node:18-alpine",
        "node:20": "node:20-alpine",
        "go:1.21": "golang:1.21-alpine"
    }

    def __init__(self, platform_config: Dict[str, Any]):
        super().__init__(platform_config)
        self.docker_utils = DockerUtils()

    async def _build_and_push_image(self, image_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> None:
        """
        Builds a container image locally using docker and pushes it to a registry.
        """
        base_image = self.RUNTIME_TO_BASE_IMAGE.get(runtime)
        if not base_image:
            raise PlatformError(
                f"unsupported runtime: '{runtime}'. supported runtimes are: {list(self.RUNTIME_TO_BASE_IMAGE.keys())}")

        with tempfile.TemporaryDirectory() as temp_dir:
            # read the code stream into a temporary file first
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                # reset stream position to beginning
                code_stream.seek(0)
                # copy content to temporary file
                temp_zip.write(code_stream.read())
                temp_zip.flush()

                # extract from the temporary file
                with zipfile.ZipFile(temp_zip.name, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)

                # clean up
                os.unlink(temp_zip.name)

            # create the dockerfile dynamically
            dockerfile_content = f"""
            FROM {base_image}
            WORKDIR /app
            COPY . .
            RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi
            EXPOSE 8080
            ENV PORT=8080
            CMD ["uvicorn", "{handler}", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
            """
            with open(f"{temp_dir}/Dockerfile", "w") as f:
                f.write(dockerfile_content)

            # build and push the image, the calls are async so they are ran in thread
            try:
                await asyncio.to_thread(self.docker_utils.build_image, temp_dir, image_name)
                await asyncio.to_thread(self.docker_utils.push_image, image_name)

            except DockerError as e:
                raise PlatformError(f"failed to build or push image: {e.message}")

    async def deploy(self, func_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> Dict[str, Any]:
        """
        Deploys a function by building a custom Docker image and using kn CLI
        """
        start_time = time.monotonic()
        namespace = self.config.get("namespace", "default")
        registry = self.config.get("container_registry")

        if not registry:
            raise PlatformError("'container_registry' must be specified in the config for knative.")

        image_destination = f"{registry}/{func_name}:latest"

        try:
            # build and push the custom docker image
            await self._build_and_push_image(image_destination, handler, runtime, code_stream)

            # deploy using kn cli
            cmd = [
                "kn", "service", "apply", func_name,
                "--image", image_destination,
                "--namespace", namespace,
                "--port", "8080"
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                raise PlatformError(f"'kn service apply' failed with exit code {process.returncode}.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}")

        except FileNotFoundError:
            raise PlatformError("the 'kn' command is not installed")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred during deployment: {e}")

        duration = time.monotonic() - start_time

        return {
            "status": "success",
            "name": func_name,
            "image": image_destination,
            "platform": 'knative',
            "deployment_duration_seconds": duration,
        }

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invokes a deployed knative service using port-forwarding (more reliable for local development)
        """
        start_time = time.monotonic()
        namespace = self.config.get("namespace", "default")

        try:
            # check if service exists
            cmd = [
                "kn", "service", "describe", func_id,
                "--namespace", namespace,
                "--output", "url"
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode:
                if "not found" in stderr.decode().lower():
                    raise PlatformError(f"knative service '{func_id}' not found.")
                else:
                    raise PlatformError(f"'kn service describe' failed: {stderr.decode()}")

            service_url = stdout.decode().strip()

            # use port-forwarding approach for local access
            get_pod_cmd = [
                "kubectl", "get", "pods",
                "-l", f"serving.knative.dev/service={func_id}",
                "-o", "jsonpath={.items[0].metadata.name}",
                "-n", namespace
            ]

            # check if pod exists
            pod_process = await asyncio.create_subprocess_exec(
                *get_pod_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            pod_stdout, pod_stderr = await pod_process.communicate()
            pod_name = pod_stdout.decode().strip()

            if not pod_name:
                # make a request to trigger scaling will fail but start pod
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        await client.get(service_url)
                except:
                    pass

                # wait for pod to start
                for i in range(15):
                    await asyncio.sleep(1)
                    pod_process = await asyncio.create_subprocess_exec(
                        *get_pod_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    pod_stdout, pod_stderr = await pod_process.communicate()
                    pod_name = pod_stdout.decode().strip()
                    if pod_name:
                        print(f"pod started: {pod_name}")
                        break
                else:
                    raise PlatformError(f"pod failed to start for service {func_id}")
            else:
                print(f"found existing pod: {pod_name}")

            # start port forward to the pod
            port_forward_cmd = [
                "kubectl", "port-forward",
                pod_name, "8080:8080",
                "-n", namespace
            ]

            print(f"Starting port-forward: kubectl port-forward {pod_name} 8080:8080")
            port_forward_process = await asyncio.create_subprocess_exec(
                *port_forward_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # give port forward time to establish
            await asyncio.sleep(3)

            try:
                # invoke via port forward
                async with httpx.AsyncClient(timeout=30.0) as client:
                    print("Invoking via port-forward at localhost:8080")
                    response = await client.post(url="http://localhost:8080/", json=payload)
                    response.raise_for_status()

                    try:
                        function_response_data = response.json()
                    except:
                        function_response_data = {"response": response.text}

            finally:
                # clean up
                try:
                    port_forward_process.terminate()
                    await port_forward_process.wait()
                except:
                    pass

        except FileNotFoundError:
            raise PlatformError("the 'kn' command is not installed")
        except httpx.HTTPStatusError as e:
            raise PlatformError(f"function '{func_id}' returned an error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            raise PlatformError(f"failed to connect to function '{func_id}': {str(e)}")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred during invocation: {str(e)} - {type(e).__name__}")

        duration = time.monotonic() - start_time
        return {
            "status": "success",
            "invocation_duration_seconds": duration,
            "function_response": function_response_data
        }

    async def list_functions(self) -> List[Dict[str, Any]]:
        """
        Lists all deployed knative services using kn CLI
        """
        namespace = self.config.get("namespace", "default")

        try:
            cmd = [
                "kn", "service", "list",
                "--namespace", namespace,
                "--output", "json"
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                raise PlatformError(f"'kn service list' failed: {stderr.decode()}")

            # parse json
            output = stdout.decode().strip()
            if not output:
                return []

            services_data = json.loads(output)

            functions_list = []
            for item in services_data.get("items", []):
                metadata = item.get("metadata", {})
                status = item.get("status", {})

                # check if service is ready
                conditions = status.get("conditions", [])
                ready = any(c.get('status') == 'True' and c.get('type') == 'Ready' for c in conditions)

                functions_list.append({
                    "name": metadata.get("name"),
                    "url": status.get("url"),
                    "ready": ready,
                })

            return functions_list

        except FileNotFoundError:
            raise PlatformError("the 'kn' command is not installed")
        except json.JSONDecodeError as e:
            raise PlatformError(f"Failed to parse kn service list output: {e}")
        except Exception as e:
            raise PlatformError(f"An unexpected error occurred while listing functions: {e}")

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """
        Deletes a specific knative service
        """
        namespace = self.config.get("namespace", "default")

        try:
            cmd = [
                "kn", "service", "delete", func_id,
                "--namespace", namespace,
                "--wait"
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode != 0:
                stderr_text = stderr.decode()
                if "not found" in stderr_text.lower():
                    return {"message": f"knative service '{func_id}' not found, assumed already deleted."}
                else:
                    raise PlatformError(f"'kn service delete' failed: {stderr_text}")

            return {"message": f"knative service '{func_id}' deletion completed successfully."}

        except FileNotFoundError:
            raise PlatformError("the 'kn' command is not installed")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred while deleting function: {e}")