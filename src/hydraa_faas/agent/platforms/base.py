"""Knative platform base classes."""

import os
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List


class FaasPlatform(ABC):
    """Base class for knative platform operations."""
    
    @abstractmethod
    def deploy(self, func_id: str, image: str) -> Dict[str, Any]:
        """Deploy function to knative.
        
        Args:
            func_id: Function identifier.
            image: Docker image name.
            
        Returns:
            Deployment result.
        """
        pass
    
    @abstractmethod
    def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke deployed function.
        
        Args:
            func_id: Function identifier.
            payload: Data to send to function.
            
        Returns:
            Function execution result.
        """
        pass
    
    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions.
        
        Returns:
            List of function information.
            
        Raises:
            NotImplementedError: If not implemented by subclass.
        """
        raise NotImplementedError("Function listing not implemented")
    
    def delete_function(self, func_id: str) -> Dict[str, Any]:
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
    """Exception for knative platform errors."""
    pass


def create_deployment_spec(func_id: str, image: str, workflow: str) -> Dict[str, Any]:
    """Create deployment specification for knative.
    
    Args:
        func_id: Function identifier.
        image: Docker image name.
        workflow: Deployment workflow type.
        
    Returns:
        Deployment specification dictionary.
    """
    return {
        'name': func_id,
        'image': image,
        'imagePullPolicy': "Never" if workflow == "local" else "IfNotPresent",
        'labels': {
            'faas-middleware': 'true',
            'workflow': workflow
        },
        'annotations': {
            'deployment.timestamp': str(int(time.time()))
        }
    }