"""
Enhanced Pydantic models for FaaS agent API with better validation
"""

from pydantic import BaseModel, Field, validator
from typing import Any, Dict, Optional, Union
import json


class InvocationRequest(BaseModel):
    """Request model for function invocation"""
    id: str = Field(..., description="Function ID/name to invoke")
    payload: Optional[Union[Dict[str, Any], str, int, float, bool, list]] = Field(
        default=None,
        description="Payload to send to the function"
    )

    @validator('id')
    def validate_function_id(cls, v):
        if not v or not v.strip():
            raise ValueError('Function ID cannot be empty')
        return v.strip()

    @validator('payload', pre=True)
    def validate_payload(cls, v):
        # Handle string payloads that might be JSON
        if isinstance(v, str):
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                # Return as string if not valid JSON
                return v
        return v


class DeleteRequest(BaseModel):
    """Request model for function deletion"""
    id: str = Field(..., description="Function ID/name to delete")

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
    details: Optional[Dict[str, Any]] = Field(None, description="Additional deployment details")


class InvocationResponse(BaseModel):
    """Response model for invocation operations"""
    status: str = Field(..., description="Status of the operation")
    function_id: str = Field(..., description="ID of the invoked function")
    platform: Optional[str] = Field(None, description="Platform where function was invoked")
    function_response: Optional[Any] = Field(None, description="Response from the function")
    invocation_time: Optional[float] = Field(None, description="Invocation duration in seconds")
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
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")


class HealthResponse(BaseModel):
    """Response model for health check"""
    status: str = Field(..., description="Health status")
    platform: str = Field(..., description="Configured platform")
    version: str = Field(..., description="Application version")
    config_path: Optional[str] = Field(None, description="Configuration file path")