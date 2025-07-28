"""
Custom exceptions for the FaaS Manager
"""

class FaasException(Exception):
    """Base exception for FaaS operations"""
    pass

class DeploymentException(FaasException):
    """Exception raised during function deployment"""
    pass

class InvocationException(FaasException):
    """Exception raised during function invocation"""
    pass

class PackagingException(FaasException):
    """Exception raised during source code packaging"""
    pass

class ECRException(FaasException):
    """Exception raised during ECR operations"""
    pass

class CostTrackingException(FaasException):
    """Exception raised during cost tracking operations"""
    pass

# Add this new class
class NuclioException(DeploymentException): # Or inherit directly from FaasException if preferred
    """Exception raised during Nuclio-specific operations/deployments"""
    pass