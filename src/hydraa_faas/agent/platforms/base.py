"""FaaS platform abstract base class"""

import os
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List


class FaasPlatform(ABC):
    """Abstract base class for FaaS platform adapters"""
    
    @abstractmethod
    def deploy(self, func_id: str, image: str) -> Dict[str, Any]:
        """Deploy a function to the FaaS platform"""
        pass
    
    @abstractmethod
    def invoke(self, func_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Invoke a deployed function"""
        pass
    
    def list_functions(self) -> List[Dict[str, Any]]:
        """List all deployed functions"""
        raise NotImplementedError("Function listing not implemented")
    
    def delete_function(self, func_id: str) -> Dict[str, Any]:
        """Delete a deployed function"""
        raise NotImplementedError("Function deletion not implemented")


class PlatformError(Exception):
    """Custom exception for platform errors"""
    pass


def create_deployment_spec(func_id: str, image: str, workflow: str) -> Dict[str, Any]:
    """Create a standardized deployment specification"""
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