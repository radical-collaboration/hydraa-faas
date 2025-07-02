"""Base platform module.

This module defines platform-agnostic interface for interacting with faas platforms.
"""

from abc import ABC, abstractmethod
from fastapi import File
from typing import Dict, Any, List, Optional

class BasePlatform(ABC):
    """Abstract base class for faas platforms.

    This class templates common faas platform capabilities
    including list, delete, deployment, and invocation methods.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config


    @abstractmethod
    async def deploy(self, func_name: str, handler: str, runtime:str, code_stream: File) -> Dict[str, Any]:
        """Deploys a function.

        Args:
            func_name: Function identifier.
            handler: Function handler.
            code_stream: Source code.

        Returns:
            Deployment result.
        """
        pass
    
    @abstractmethod
    async def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invokes a deployed function.
        
        Args:
            func_id: Function identifier.
            payload: Data to send to function.
            
        Returns:
            Function execution result.
        """
        pass
    
    async def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions.
        
        Returns:
            List of function information.
            
        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError("Function listing not implemented")
    
    async def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete deployed function.
        
        Args:
            func_id: Function identifier.
            
        Returns:
            Deletion result.
            
        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError("Function deletion not implemented")


class PlatformError(Exception):
    """Exception for platform errors."""
    pass
