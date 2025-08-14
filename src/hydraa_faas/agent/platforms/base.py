"""Simplified base platform module for Nuclio-focused FaaS agent.

This module defines the platform interface for FaaS platforms,
simplified and optimized for primary Nuclio usage.
"""

from abc import ABC, abstractmethod
from fastapi import File
from typing import Dict, Any, List, Optional, IO

class BasePlatform(ABC):
    """Abstract base class for FaaS platforms.

    Defines the core interface that all FaaS platforms must implement.
    Simplified for primary Nuclio usage but maintains extensibility.
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize platform with configuration.

        Args:
            config: Platform-specific configuration dictionary
        """
        self.config = config

    @abstractmethod
    async def deploy(self,
                     func_name: str,
                     handler: str,
                     runtime: str,
                     code_stream: Optional[IO[bytes]] = None,
                     image_name: Optional[str] = None,
                     dynamic_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Deploy a function to the platform from source or a pre-built image.

        Args:
            func_name: Unique function identifier
            handler: Function entry point (e.g., 'module:function')
            runtime: Runtime environment (e.g., 'python:3.9')
            code_stream: Function source code as a file-like object (for source deployment)
            image_name: Full name of a pre-built container image (for image deployment)
            dynamic_config: Dynamic configuration overrides for this deployment

        Returns:
            Dictionary containing deployment results.

        Raises:
            PlatformError: If deployment fails
        """
        pass

    @abstractmethod
    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke a deployed function.

        Args:
            func_id: Function identifier
            payload: Input data for the function

        Returns:
            Dictionary containing invocation results including:
            - status: Invocation status
            - function_response: Response from the function
            - invocation_time: Execution time in seconds
            - platform-specific details

        Raises:
            PlatformError: If invocation fails
        """
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the platform is healthy and accessible.

        Returns:
            True if platform is healthy, False otherwise
        """
        pass

    async def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions.

        Returns:
            List of dictionaries containing function information:
            - id: Function identifier
            - name: Function name
            - platform: Platform name
            - status/state: Function status
            - Additional platform-specific fields

        Raises:
            PlatformError: If listing fails
        """
        return []

    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed function.

        Args:
            func_id: Function identifier

        Returns:
            Dictionary containing deletion results:
            - status: Deletion status
            - message: Confirmation message
            - platform-specific details

        Raises:
            PlatformError: If deletion fails
        """
        return {
            "status": "success",
            "message": f"Function {func_id} deletion requested"
        }

    async def get_metrics(self) -> Dict[str, Any]:
        """Get platform-specific metrics.

        Returns:
            Dictionary containing platform metrics and statistics
        """
        return {"platform": "unknown", "metrics": {}}

    async def get_platform_info(self) -> Dict[str, Any]:
        """Get detailed platform information.

        Returns:
            Dictionary containing platform details:
            - platform: Platform name
            - version: Platform version (if available)
            - capabilities: Supported features
            - configuration: Current platform configuration
        """
        return {
            "platform": "unknown",
            "config": self.config
        }


class PlatformError(Exception):
    """Exception for platform-specific errors.

    Used to wrap and standardize errors from different FaaS platforms.
    """

    def __init__(self, message: str, platform: str = "unknown",
                 operation: str = "unknown", details: Optional[Dict[str, Any]] = None):
        """Initialize platform error.

        Args:
            message: Error description
            platform: Platform where error occurred
            operation: Operation that failed
            details: Additional error context
        """
        self.message = message
        self.platform = platform
        self.operation = operation
        self.details = details or {}
        super().__init__(message)

    def to_dict(self) -> Dict[str, Any]:
        """Convert error to dictionary for API responses."""
        return {
            "error": self.message,
            "platform": self.platform,
            "operation": self.operation,
            "details": self.details
        }