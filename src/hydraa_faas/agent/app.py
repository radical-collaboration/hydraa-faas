"""FastAPI app for FaaS function management."""

import os
import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from io import BytesIO

from fastapi import FastAPI, HTTPException, File, UploadFile, Form
from fastapi.responses import JSONResponse

from .platforms.nuclio import NuclioPlatform
from .platforms.knative import KnativePlatform
from .utils.models import (
    InvocationRequest, DeleteRequest, DeploymentResponse,
    InvocationResponse, ListResponse, DeleteResponse,
    ErrorResponse, HealthResponse
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="FaaS Agent", version="1.0.0")

# Global configuration
CONFIG_FILE = Path("agent/config/config.yaml")
config = {}
platform_adapter = None

def load_config():
    """Load configuration from config.yaml"""
    global config, platform_adapter

    if not CONFIG_FILE.exists():
        raise Exception(f"Configuration file {CONFIG_FILE} not found")

    with open(CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)

    # Initialize platform adapter
    platform_name = config.get('platform', 'nuclio')
    platform_config = config.get('platforms', {}).get(platform_name, {})

    if platform_name == 'nuclio':
        platform_adapter = NuclioPlatform(platform_config)
    elif platform_name == 'knative':
        platform_adapter = KnativePlatform(platform_config)
    else:
        raise Exception(f"Unsupported platform: {platform_name}")

    logger.info(f"Initialized {platform_name} platform adapter")

# Load configuration on startup
try:
    load_config()
except Exception as e:
    logger.error(f"Failed to load configuration: {e}")
    raise

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        platform=config.get('platform', 'unknown'),
        version="1.0.0",
        config_path=str(CONFIG_FILE)
    )

@app.post("/deploy", response_model=DeploymentResponse)
async def deploy_function(
    handler: str = Form(..., description="Handler function (e.g., 'handler:main')"),
    runtime: str = Form(default="python:3.9", description="Runtime environment"),
    name: Optional[str] = Form(default=None, description="Function name (auto-generated if not provided)"),
    code: UploadFile = File(..., description="Zip file containing function code")
):
    """Deploy function from uploaded zip file"""
    try:
        # Generate function name if not provided
        if not name:
            import uuid
            name = f"func-{uuid.uuid4().hex[:8]}"

        # Validate uploaded file
        if not code.filename.endswith('.zip'):
            raise HTTPException(status_code=400, detail="File must be a zip archive")

        # Read uploaded file
        code_content = await code.read()
        code_stream = BytesIO(code_content)

        # Deploy function
        result = await platform_adapter.deploy(name, handler, runtime, code_stream)

        return DeploymentResponse(
            status="success",
            name=name,
            platform=config.get('platform'),
            details=result
        )

    except Exception as e:
        logger.error(f"Deployment failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/invoke", response_model=InvocationResponse)
async def invoke_function(request: InvocationRequest):
    """Invoke deployed function"""
    try:
        result = await platform_adapter.invoke(request.id, request.payload or {})

        return InvocationResponse(
            status="success",
            function_id=request.id,
            platform=config.get('platform'),
            function_response=result.get('function_response'),
            invocation_time=result.get('invocation_time'),
            details=result
        )

    except Exception as e:
        logger.error(f"Invocation failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/functions", response_model=ListResponse)
async def list_functions():
    """List deployed functions"""
    try:
        functions = await platform_adapter.list_functions()

        return ListResponse(
            status="success",
            platform=config.get('platform'),
            count=len(functions),
            data={"functions": functions}
        )

    except Exception as e:
        logger.error(f"Function listing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/delete", response_model=DeleteResponse)
async def delete_function(request: DeleteRequest):
    """Delete deployed function"""
    try:
        result = await platform_adapter.delete_function(request.id)

        return DeleteResponse(
            status="success",
            platform=config.get('platform'),
            function_id=request.id,
            message=result.get('message', f"Function {request.id} deleted"),
            data=result
        )

    except Exception as e:
        logger.error(f"Function deletion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")

    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            status="error",
            error=str(exc),
            platform=config.get('platform')
        ).dict()
    )

if __name__ == "__main__":
    import uvicorn

    print("FaaS Agent starting...")
    print(f"Platform: {config.get('platform')}")
    print(f"Kubernetes Target: {config.get('kubernetes_target', {}).get('type')}")

    port = config.get('Port', 8000)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")