"""
Main fastapi application for faas middleware.

TODO: implement async into platforms
TODO: add/fix docstrings
TODO: add typing and exception handling
"""

import yaml
from typing import Optional, Dict, Any
from .platforms.knative import KnativePlatform
from .platforms.nuclio import NuclioPlatform
from .platforms.base import PlatformError
from .utils import misc
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
import uvicorn
from .utils.models import InvocationRequest, DeleteRequest
app = FastAPI() # start app

PLATFORM_TO_CLASS = {
    'knative': KnativePlatform,
    'nuclio': NuclioPlatform,
}

# read from config.yaml and configure platform adapter
with open('agent/config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

platform = config.get('platform')
platform_manager = PLATFORM_TO_CLASS[platform](config.get('platforms').get(platform))

@app.get("/health")
async def health() -> Dict[str, Any]:
    """
    Health check endpoint.

    Returns:
        JSON response with service status.
    """
    return {"status": "healthy",
            "platform":platform}
@app.post("/deploy")
async def deploy_func(function_name: Optional[str] = Form(None),
                      handler: str = Form("main:handler"),
                      runtime: Optional[str] = Form('python3.9'),
                      code_file: UploadFile = File(...)) -> Dict[str, Any]:

    """
    Receives deployment request and routes
    it to the appropriate platform manager.

    Returns:
        JSON response with the function's result or an error.

    Raises:
        HTTPException when deployment fails.
    """
    try:
        result = await platform_manager.deploy(
            func_name=function_name or misc.generate_id(),
            runtime=runtime,
            handler=handler,
            code_stream=code_file.file)
        return result
    except PlatformError as e:
        raise HTTPException(status_code=500, detail=f"{platform} deployment failed: {e}")

@app.post("/invoke")
async def invoke_function(request_data: InvocationRequest) -> Dict[str, Any]:
    """
    Receives invocation request and routes it to
    the appropriate platform manager.

    Returns:
        JSON response with the function's result or an error.

    Raises:
        HTTPException when invocation fails.
    """
    try:
        func_id = request_data.id
        payload = request_data.payload
        return await platform_manager.invoke(func_id, payload)

    except PlatformError as e:
        raise HTTPException(
            status_code=500,
            detail=f"{platform} invocation failed: {e}"
        )
@app.post("/list")
async def list_functions() -> Dict[str, Any]:
    """
    Lists deployed functions for a platform.

    Returns:
        JSON response with list of functions.

    Raises:
        HTTPException when listing fails.
    """
    try:
        functions_list = await platform_manager.list_functions()
        return {
            "status": "success",
            "platform": platform,
            "data": {"functions": functions_list}
        }
    except PlatformError as e:
        raise HTTPException(status_code=500, detail=f"failed to list functions for {platform}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"an unexpected error occurred: {str(e)}")

@app.post("/delete")
async def delete_function(request_data: DeleteRequest) -> Dict[str, Any]:
    """
    Deletes a deployed function by its ID.

    Args:
        request_data: A JSON body with a required 'id' field.

    Returns:
        JSON response with the deletion result or an error.

    Raises:
        HTTPException when deletion fails.
    """
    try:
        func_id = request_data.id
        result = await platform_manager.delete_function(func_id)
        return {
            "status": "success",
            "platform": platform,
            "data": result
        }
    except PlatformError as e:
        raise HTTPException(status_code=404,
                            detail=f"function not found or could not be deleted on {platform}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"function deletion failed: {e}")

if __name__ == '__main__':
    print(f"starting agent\n\
            platform: {platform}\n")
    uvicorn.run(app, host="0.0.0.0", port=config.get("Port"))