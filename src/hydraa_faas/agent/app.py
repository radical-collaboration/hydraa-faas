"""
AGnet with Nuclio Support Only
Acts as a bridge between FaaS Manager and Nuclio on Kubernetes
"""

import os
import logging
import asyncio
import yaml
import time
import signal
import sys
import tempfile
import json
from pathlib import Path
from typing import Dict, Any, Optional, Coroutine
from io import BytesIO
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, File, UploadFile, Form, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from platforms.nuclio import NuclioPlatform
from utils.models import (
    InvocationRequest, DeleteRequest, DeploymentResponse,
    InvocationResponse, ListResponse, DeleteResponse,
    ErrorResponse, HealthResponse, DeploymentStatusResponse
)
from utils.cache import DeploymentCache
from utils.validator import FunctionValidator
from utils.misc import detect_platform, KubernetesUtils

# Configure initial logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state
platform_adapter = None
config = {}
deployment_cache = None
function_validator = None
k8s_utils = None
agent_environment = "production" # Default environment

async def load_config():
    """Load configuration and initialize Nuclio platform adapter"""
    global config, platform_adapter, deployment_cache, function_validator, k8s_utils, agent_environment

    config_file = Path("agent/config/config.yaml")
    if not config_file.exists():
        raise Exception(f"Configuration file {config_file} not found")

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    k8s_type = config.get('kubernetes_target', {}).get('type', 'minikube')
    if k8s_type == 'gke':
        agent_environment = "production"
        logging.getLogger().setLevel(logging.INFO)
    else:
        agent_environment = "development"
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info(f"Agent running in '{agent_environment}' mode.")

    deployment_cache = DeploymentCache()
    function_validator = FunctionValidator()
    k8s_utils = KubernetesUtils()
    platform_info = detect_platform()
    logger.info(f"Running on {platform_info['system']} ({platform_info['machine']})")
    platform_adapter = NuclioPlatform(config)
    logger.info("Initialized Nuclio platform adapter")

def setup_signal_handlers(app: FastAPI):
    """Setup graceful shutdown handlers"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        sys.exit(0)

    if hasattr(signal, 'SIGTERM'):
        signal.signal(signal.SIGTERM, signal_handler)
    if hasattr(signal, 'SIGINT'):
        signal.signal(signal.SIGINT, signal_handler)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management with graceful shutdown"""
    try:
        await load_config()
        setup_signal_handlers(app)
        logger.info("FaaS Agent started successfully")
    except Exception as e:
        logger.error(f"Failed to initialize agent: {e}")
        raise
    yield
    logger.info("FaaS Agent shutting down...")
    if deployment_cache:
        deployment_cache.clear()
    logger.info("Shutdown complete")

# Initialize FastAPI app
app = FastAPI(
    title="FaaS Agent",
    version="3.0.0",
    description="Production ready Nuclio FaaS agent with dynamic configuration support",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/deploy", response_model=DeploymentResponse)
async def deploy_function(
    background_tasks: BackgroundTasks,
    handler: str = Form(..., description="Handler function (e.g., 'main:handler')"),
    runtime: str = Form(..., description="Runtime environment for the function (e.g., 'python:3.9')"),
    name: Optional[str] = Form(None, description="Function name (auto-generated if not provided)"),
    code: Optional[UploadFile] = File(None, description="Zip file with source code for a source-based deployment."),
    image_name: Optional[str] = Form(None, description="Full name of a pre-built container image for an image-based deployment."),

    # Dynamic configuration options
    registry: Optional[str] = Form(None, description="Container registry to use (overrides default)"),
    cpu_limit: Optional[str] = Form("200m", description="CPU limit (e.g., '200m', '1')"),
    memory_limit: Optional[str] = Form("256Mi", description="Memory limit (e.g., '256Mi', '1Gi')"),
    cpu_request: Optional[str] = Form("25m", description="CPU request"),
    memory_request: Optional[str] = Form("64Mi", description="Memory request"),
    min_replicas: Optional[int] = Form(1, description="Minimum number of replicas"),
    max_replicas: Optional[int] = Form(1, description="Maximum number of replicas"),
    env_vars: Optional[str] = Form(None, description="Environment variables as JSON string"),
    labels: Optional[str] = Form(None, description="Labels as JSON string"),
    build_commands: Optional[str] = Form(None, description="Additional build commands as JSON array"),
    service_type: Optional[str] = Form("ClusterIP", description="Service type: ClusterIP, NodePort, or LoadBalancer"),

    async_deploy: bool = Form(default=False, description="Deploy asynchronously")
):
    """
    Deploy a function with dynamic configuration options.

    Static configurations (namespace, platform, etc.) come from config file.
    Dynamic configurations (registry, resources, etc.) can be specified per deployment.
    """
    start_time = time.time()

    # Validate deployment type
    is_source_deploy = code is not None
    is_image_deploy = bool(image_name and image_name.strip())

    if not (is_source_deploy ^ is_image_deploy):
        raise HTTPException(
            status_code=400,
            detail="Deployment request must include either 'code' (for source) or 'image_name' (for image), but not both."
        )

    try:
        if not name:
            import uuid
            name = f"func-{uuid.uuid4().hex[:8]}"

        # Parse JSON fields
        parsed_env_vars = {}
        parsed_labels = {}
        parsed_build_commands = []

        if env_vars:
            try:
                parsed_env_vars = json.loads(env_vars)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON in env_vars")

        if labels:
            try:
                parsed_labels = json.loads(labels)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON in labels")

        if build_commands:
            try:
                parsed_build_commands = json.loads(build_commands)
            except json.JSONDecodeError:
                raise HTTPException(status_code=400, detail="Invalid JSON in build_commands")

        # Create dynamic config
        dynamic_config = {
            "registry": registry or platform_adapter.registry,
            "resources": {
                "limits": {
                    "cpu": cpu_limit,
                    "memory": memory_limit
                },
                "requests": {
                    "cpu": cpu_request,
                    "memory": memory_request
                }
            },
            "minReplicas": min_replicas,
            "maxReplicas": max_replicas,
            "env": [{"name": k, "value": v} for k, v in parsed_env_vars.items()],
            "labels": parsed_labels,
            "build": {
                "commands": parsed_build_commands
            } if parsed_build_commands else None,
            "triggers": {
                "http-trigger": {
                    "kind": "http",
                    "attributes": {
                        "serviceType": service_type
                    }
                }
            }
        }

        code_content = None
        code_stream = None
        if is_source_deploy:
            code_content = await code.read()
            code_stream = BytesIO(code_content)

        # Check cache with dynamic config included
        cache_key = deployment_cache.generate_key(
            name=name, handler=handler, runtime=runtime,
            code_content=code_content, image_name=image_name,
            dynamic_config=json.dumps(dynamic_config, sort_keys=True)
        )

        if cached_result := deployment_cache.get(cache_key):
            logger.info(f"Found cached deployment for {name}")
            return DeploymentResponse(
                status="success", name=name, platform="nuclio",
                deployment_time=(time.time() - start_time), cached=True, **cached_result
            )

        # Validate source code if provided
        if is_source_deploy:
            class MockUploadFile:
                def __init__(self, content): self.file = BytesIO(content)
                async def read(self): return self.file.getvalue()
                async def seek(self, pos): self.file.seek(pos)

            validation_result = await function_validator.validate_zip(MockUploadFile(code_content))
            if not validation_result.is_valid:
                raise HTTPException(status_code=400, detail=f"Validation failed: {validation_result.errors}")

        # Create deployment task with dynamic config
        deployment_task = platform_adapter.deploy(
            func_name=name, handler=handler, runtime=runtime,
            code_stream=code_stream, image_name=image_name,
            dynamic_config=dynamic_config
        )

        if async_deploy:
            deployment_cache.set_status(name, "deploying")
            background_tasks.add_task(deploy_function_async, name, cache_key, deployment_task)
            return DeploymentResponse(
                status="deploying", name=name, platform="nuclio",
                deployment_time=(time.time() - start_time), async_deployment=True
            )
        else:
            result = await deployment_task
            deployment_cache.set(cache_key, result)
            deployment_cache.set_status(name, "ready")
            return DeploymentResponse(
                status="success", name=name, platform="nuclio",
                deployment_time=(time.time() - start_time), details=result
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Deployment failed: {e}", exc_info=(agent_environment == 'development'))
        if name:
            deployment_cache.set_status(name, "failed", str(e))
        raise HTTPException(status_code=500, detail=str(e))

async def deploy_function_async(name: str, cache_key: str, deployment_task: Coroutine):
    """Background task for async deployment"""
    try:
        result = await deployment_task
        deployment_cache.set(cache_key, result)
        deployment_cache.set_status(name, "ready")
        logger.info(f"Async deployment completed for {name}")
    except Exception as e:
        logger.error(f"Async deployment failed for {name}: {e}", exc_info=(agent_environment == 'development'))
        deployment_cache.set_status(name, "failed", str(e))

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check with platform detection"""
    try:
        platform_healthy = await platform_adapter.health_check()
        platform_info = detect_platform()
        k8s_info = k8s_utils.get_cluster_info()

        return HealthResponse(
            status="healthy" if platform_healthy else "degraded",
            platform="nuclio",
            version="3.0.0",
            platform_health=platform_healthy,
            cache_size=deployment_cache.size() if deployment_cache else 0,
            system_info={
                'environment': agent_environment,
                'platform': platform_info,
                'kubernetes': k8s_info
            }
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            platform="nuclio",
            version="3.0.0",
            platform_health=False,
            error=str(e)
        )

@app.get("/deploy/{function_name}/status", response_model=DeploymentStatusResponse)
async def get_deployment_status(function_name: str):
    """Get deployment status for async deployments"""
    status_info = deployment_cache.get_status(function_name)
    if not status_info:
        raise HTTPException(status_code=404, detail="Function not found")

    return DeploymentStatusResponse(
        function_name=function_name,
        status=status_info['status'],
        platform="nuclio",
        created_at=status_info.get('created_at'),
        updated_at=status_info.get('updated_at'),
        error=status_info.get('error')
    )

@app.post("/invoke", response_model=InvocationResponse)
async def invoke_function(request: InvocationRequest):
    """Invoke deployed function with retries"""
    start_time = time.time()
    logger.debug(f"Invocation request for function '{request.id}'")
    try:
        status_info = deployment_cache.get_status(request.id)
        if not status_info:
            raise HTTPException(status_code=404, detail="Function not found")
        if status_info['status'] != 'ready':
            raise HTTPException(
                status_code=400,
                detail=f"Function not ready. Status: {status_info['status']}"
            )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = await platform_adapter.invoke(request.id, request.payload or {})
                return InvocationResponse(
                    status="success",
                    function_id=request.id,
                    platform="nuclio",
                    function_response=result.get('function_response'),
                    invocation_time=time.time() - start_time,
                    attempt=attempt + 1,
                    details=result
                )
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                logger.warning(f"Invocation attempt {attempt + 1} failed: {e}", exc_info=(agent_environment == 'development'))
                await asyncio.sleep(0.5 * (2 ** attempt))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Invocation failed: {e}", exc_info=(agent_environment == 'development'))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/functions", response_model=ListResponse)
async def list_functions():
    """List deployed functions with enhanced information"""
    try:
        platform_functions = await platform_adapter.list_functions()
        enhanced_functions = []
        for func in platform_functions:
            func_name = func.get('id') or func.get('name')
            if func_name:
                status_info = deployment_cache.get_status(func_name)
                if status_info:
                    func.update({
                        'agent_status': status_info['status'],
                        'created_at': status_info.get('created_at'),
                        'updated_at': status_info.get('updated_at')
                    })
            enhanced_functions.append(func)

        return ListResponse(
            status="success",
            platform="nuclio",
            count=len(enhanced_functions),
            data={"functions": enhanced_functions}
        )
    except Exception as e:
        logger.error(f"Function listing failed: {e}", exc_info=(agent_environment == 'development'))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/delete", response_model=DeleteResponse)
async def delete_function(request: DeleteRequest):
    """Delete deployed function and clean cache"""
    try:
        result = await platform_adapter.delete_function(request.id)
        deployment_cache.remove_function(request.id)
        return DeleteResponse(
            status="success",
            platform="nuclio",
            function_id=request.id,
            message=result.get('message', f"Function {request.id} deleted"),
            data=result
        )
    except Exception as e:
        logger.error(f"Function deletion failed: {e}", exc_info=(agent_environment == 'development'))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics")
async def get_metrics():
    """Get agent and platform metrics"""
    try:
        platform_metrics = await platform_adapter.get_metrics()
        agent_metrics = {
            "agent": {
                "cache_stats": deployment_cache.stats(),
                "uptime": time.time() - app.state.start_time if hasattr(app.state, 'start_time') else 0
            },
            "platform": platform_metrics
        }
        return {"status": "success", "metrics": agent_metrics}
    except Exception as e:
        logger.error(f"Metrics collection failed: {e}", exc_info=(agent_environment == 'development'))
        return {"status": "error", "error": str(e)}

@app.get("/platform/info")
async def get_platform_info():
    """Get detailed Nuclio platform information"""
    try:
        platform_info = await platform_adapter.get_platform_info()
        return {
            "status": "success",
            "platform": "nuclio",
            "config": config,
            "info": platform_info
        }
    except Exception as e:
        logger.error(f"Platform info failed: {e}", exc_info=(agent_environment == 'development'))
        raise HTTPException(status_code=500, detail=str(e))

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler with detailed logging"""
    logger.error(f"Unhandled exception on {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            status="error",
            error=str(exc),
            platform="nuclio",
            path=str(request.url)
        ).dict()
    )

if __name__ == "__main__":
    import uvicorn
    app.state.start_time = time.time()
    print("FaaS Agent starting...")
    print("Platform: Nuclio")
    print("Version: 3.0.0")
    uvicorn.run(
        "agent.app:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    )