"""
Nuclio platform manager
"""

import json
import os
import time
import asyncio
import tempfile
import yaml
import httpx
import zipfile
from typing import Dict, Any, List, IO
from .base import BasePlatform, PlatformError


class NuclioPlatform(BasePlatform):
    """Nuclio platform manager"""

    def __init__(self, platform_config: Dict[str, Any]):
        """Initialize Nuclio Platform

        Args:
            platform_config: Nuclio configuration
        """
        super().__init__(platform_config)

    async def deploy(self, func_name: str, handler: str, runtime: str, code_stream: IO[bytes]) -> Dict[str, Any]:
        """
        Deploys a function

        Args:
            func_name: Name for deployed function
            handler: Function handler (e.g. main:handler)
            runtime: Language runtime
            code_stream: File object representing the zipped source code

        Returns:
            Dict with deployed status

        Raises:
            PlatformError if deployment fails
        """
        start_time = time.monotonic()
        namespace = self.config.get("namespace", "nuclio")

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                # read the code stream into a temporary file first
                with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                    # reset stream position to beginning
                    code_stream.seek(0)
                    # copy content to temporary file
                    temp_zip.write(code_stream.read())
                    temp_zip.flush()

                    # extract from the temporary file
                    with zipfile.ZipFile(temp_zip.name, 'r') as zf:
                        zf.extractall(temp_dir)

                    # clean up
                    os.unlink(temp_zip.name)

                # create function manifest yaml file
                function_manifest = self.config.get("function_defaults", {}).copy()
                spec = function_manifest.setdefault('spec', {})

                # add build configuration with your registry
                build = spec.setdefault('build', {})
                build['registry'] = self.config.get("container_registry", "docker.io/your-username")
                build['noBaseImagesPull'] = False

                spec['runtime'] = runtime
                spec['handler'] = handler

                with open(f"{temp_dir}/function.yaml", 'w') as f:
                    yaml.dump(function_manifest, f)

                # try to delete any existing function
                print(f"Cleaning up any existing function: {func_name}")
                delete_cmd = [
                    "nuctl", "delete", "function", func_name,
                    "--namespace", namespace
                ]

                delete_process = await asyncio.create_subprocess_exec(
                    *delete_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                await delete_process.communicate()

                # wait longer for cleanup. make 10
                await asyncio.sleep(5)

                # verify function is gone before proceeding
                check_cmd = [
                    "nuctl", "get", "function", func_name,
                    "--namespace", namespace
                ]

                for attempt in range(6):  # try for up to 30 seconds
                    check_process = await asyncio.create_subprocess_exec(
                        *check_cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    await check_process.communicate()

                    if check_process.returncode:
                        print(f"function {func_name} successfully deleted")
                        break
                    else:
                        print(f"function {func_name} still exists, waiting (attempt {attempt + 1}/6)")
                        await asyncio.sleep(5)
                else:
                    print(f"warning: function {func_name} may still exist, proceeding anyway")

                # deploy the function with registry specification
                registry = self.config.get("container_registry")
                cmd = [
                    "nuctl", "deploy", func_name,
                    "--namespace", namespace,
                    "--path", temp_dir
                ]

                # add registry if configured
                if registry:
                    cmd.extend(["--registry", registry])

                process = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                stdout, stderr = await process.communicate()

                if process.returncode:
                    raise PlatformError(f"'nuctl deploy' failed with exit code {process.returncode}.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}")

        except FileNotFoundError:
            raise PlatformError("the 'nuctl' command is not installed")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred during deployment: {e}")

        duration = time.monotonic() - start_time

        return {
            "status": "success",
            "name": func_name,
            "platform": "nuclio",
            "deployment_duration_seconds": duration
        }

    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invokes a deployed function

        Args:
            func_id: The name of the function to invoke
            payload: The JSON payload to send in the request body

        Returns:
            The JSON response from the invoked function

        Raises:
            PlatformError if invocation fails
        """
        start_time = time.monotonic()
        namespace = self.config.get("namespace", "nuclio")

        try:
            # use internal cluster service URL
            internal_url = f"http://{func_id}.{namespace}.svc.cluster.local:8080"

            # make http request to the internal service
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                print(f"Invoking Nuclio function at internal URL: {internal_url}")
                response = await client.post(url=internal_url, json=payload)
                response.raise_for_status()

                # try to parse as JSON
                try:
                    function_response_data = response.json()
                except:
                    function_response_data = {"response": response.text}

        except httpx.HTTPStatusError as e:
            raise PlatformError(f"function '{func_id}' returned an error: {e.response.status_code} - {e.response.text}")
        except httpx.RequestError as e:
            # if internal url fails use forwarding
            print(f"Internal URL failed ({e}), trying port-forward fallback...")

            try:
                # get the service name (usually nuclio-FUNCTION_NAME)
                service_name = f"nuclio-{func_id}"

                # start port forward
                port_forward_cmd = [
                    "kubectl", "port-forward",
                    f"svc/{service_name}", "8080:8080",
                    "-n", namespace
                ]

                print(f"Starting port-forward: {' '.join(port_forward_cmd)}")
                port_forward_process = await asyncio.create_subprocess_exec(
                    *port_forward_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                # give port forward time to establish
                await asyncio.sleep(3)

                try:
                    # try to invoke
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

            except Exception as pf_error:
                raise PlatformError(f"Both internal URL and port-forward failed. Internal: {e}, Port-forward: {pf_error}")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred during invocation: {str(e)}")

        duration = time.monotonic() - start_time
        return {
            "status": "success",
            "invocation_duration_seconds": duration,
            "function_response": function_response_data,
            "platform": "nuclio"
        }

    async def list_functions(self) -> List[Dict[str, Any]]:
        """
        Lists all deployed functions

        Returns:
            List of deployed functions
        """
        namespace = self.config.get("namespace", "nuclio")

        try:
            cmd = [
                "nuctl", "get", "functions",
                "--namespace", namespace,
                "--output", "json"
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode:
                raise PlatformError(f"'nuctl get functions' failed: {stderr.decode()}")

            # parse json
            output = stdout.decode().strip()
            if not output:
                return []

            functions_data = json.loads(output)

            functions_list = []
            # handle both single function and list of functions
            if isinstance(functions_data, dict):
                functions_data = [functions_data]

            for func in functions_data:
                metadata = func.get("metadata", {})
                status = func.get("status", {})

                functions_list.append({
                    "name": metadata.get("name"),
                    "status": status.get("state", "unknown"),
                    "runtime": func.get("spec", {}).get("runtime"),
                    "ready": status.get("state") == "ready"
                })

            return functions_list

        except FileNotFoundError:
            raise PlatformError("the 'nuctl' command is not installed")
        except json.JSONDecodeError as e:
            raise PlatformError(f"Failed to parse nuctl output: {e}")
        except Exception as e:
            raise PlatformError(f"An unexpected error occurred while listing functions: {e}")

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """
        Deletes a specific function

        Args:
            func_id: Function ID to delete

        Returns:
            Dict with deletion status
        """
        namespace = self.config.get("namespace", "nuclio")

        try:
            cmd = [
                "nuctl", "delete", "function", func_id,
                "--namespace", namespace
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await process.communicate()

            if process.returncode:
                stderr_text = stderr.decode()
                if "not found" in stderr_text.lower():
                    return {"message": f"function '{func_id}' was not found, assumed already deleted."}
                else:
                    raise PlatformError(f"'nuctl delete' failed: {stderr_text}")

            return {"message": f"function '{func_id}' deleted successfully."}

        except FileNotFoundError:
            raise PlatformError("the 'nuctl' command is not installed")
        except Exception as e:
            raise PlatformError(f"an unexpected error occurred while deleting function: {e}")