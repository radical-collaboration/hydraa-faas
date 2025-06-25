"""
AWS lambda faas provider
"""

import boto3
import botocore
from botocore.exceptions import ClientError, BotoCoreError
from hydraa import Task
from hydraa.services.cost_manager.aws_cost import AwsCost


class AwsLambda(AwsCost):
    """
    AWS lambda provider for serverless function execution with cost tracking.
    
    This class inherits hydras AwsCost to provide cost management
    for lambda functions including request pricing and compute time costs.
    """
    
    