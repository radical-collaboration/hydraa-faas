"""
Enhanced Pydantic models for optimized FaaS agent API
"""

from pydantic import BaseModel, Field, validator
from typing import Any, Dict, Optional, Union, List
from datetime import datetime
import json


class InvocationRequest(BaseModel):
    """Request model for function invocation"""
    id: str = Field(..., description="Function ID/name to invoke")
    payload: Optional[Union[Dict[str, Any], str, int, float, bool, list]] = Field(
        default=None,
        description="Payload to send to the function"
    )
    timeout: Optional[int] = Field(default=30, description="Invocation timeout in seconds")

    @validator('id')
    def validate_function_id(cls, v):
        if not v or not v.strip():
            raise ValueError('Function ID cannot be empty')
        return v.strip()

    @validator('payload', pre=True)
    def validate_payload(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                return v
        return v


class DeleteRequest(BaseModel):
    """Request model for function deletion"""
    id: str = Field(..., description="Function ID/name to delete")
    force: bool = Field(default=False, description="Force deletion even if function is in use")

    @validator('id')
    def validate_function_id(cls, v):
        if not v or not v.strip():
            raise ValueError('Function ID cannot be empty')
        return v.strip()


class DeploymentResponse(BaseModel):
    """Response model for deployment operations"""
    status: str = Field(..., description="Status of the operation")
    name: str = Field(..., description="Name of the deployed function")
    platform: Optional[str] = Field(None, description="Target platform")
    deployment_time: Optional[float] = Field(None, description="Deployment duration in seconds")
    cached: bool = Field(default=False, description="Whether result was served from cache")
    async_deployment: bool = Field(default=False, description="Whether deployment is async")
    endpoint_url: Optional[str] = Field(None, description="Function invocation URL")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional deployment details")


class DeploymentStatusResponse(BaseModel):
    """Response model for deployment status queries"""
    function_name: str = Field(..., description="Name of the function")
    status: str = Field(..., description="Current deployment status")
    platform: str = Field(..., description="Target platform")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    error: Optional[str] = Field(None, description="Error message if failed")


class InvocationResponse(BaseModel):
    """Response model for invocation operations"""
    status: str = Field(..., description="Status of the operation")
    function_id: str = Field(..., description="ID of the invoked function")
    platform: Optional[str] = Field(None, description="Platform where function was invoked")
    function_response: Optional[Any] = Field(None, description="Response from the function")
    invocation_time: Optional[float] = Field(None, description="Invocation duration in seconds")
    attempt: Optional[int] = Field(None, description="Attempt number (for retries)")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional invocation details")


class ListResponse(BaseModel):
    """Response model for list operations"""
    status: str = Field(..., description="Status of the operation")
    platform: str = Field(..., description="Platform being queried")
    count: int = Field(..., description="Number of functions found")
    data: Dict[str, Any] = Field(..., description="Function list data")


class DeleteResponse(BaseModel):
    """Response model for delete operations"""
    status: str = Field(..., description="Status of the operation")
    platform: str = Field(..., description="Platform where function was deleted")
    function_id: str = Field(..., description="ID of the deleted function")
    message: str = Field(..., description="Deletion confirmation message")
    data: Optional[Dict[str, Any]] = Field(None, description="Additional deletion details")


class ErrorResponse(BaseModel):
    """Response model for error cases"""
    status: str = Field(default="error", description="Status indicating error")
    error: str = Field(..., description="Error message")
    platform: Optional[str] = Field(None, description="Platform where error occurred")
    function_id: Optional[str] = Field(None, description="Function ID related to error")
    error_type: Optional[str] = Field(None, description="Type of error")
    path: Optional[str] = Field(None, description="Request path where error occurred")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(..., description="Health status")
    platform: str = Field(..., description="Configured platform")
    version: str = Field(..., description="Application version")
    platform_health: bool = Field(..., description="Platform health status")
    cache_size: int = Field(default=0, description="Number of cached deployments")
    system_info: Optional[Dict[str, Any]] = Field(None, description="System and platform information")
    error: Optional[str] = Field(None, description="Error message if unhealthy")


class ValidationResult(BaseModel):
    """Result of function package validation"""
    is_valid: bool = Field(..., description="Whether validation passed")
    errors: List[str] = Field(default_factory=list, description="Validation errors")
    warnings: List[str] = Field(default_factory=list, description="Validation warnings")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Extracted metadata")


class MetricsResponse(BaseModel):
    """Response model for metrics"""
    status: str = Field(..., description="Status of metrics collection")
    platform: str = Field(..., description="Platform name")
    agent_metrics: Dict[str, Any] = Field(..., description="Agent-specific metrics")
    platform_metrics: Dict[str, Any] = Field(..., description="Platform-specific metrics")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Metrics timestamp")
