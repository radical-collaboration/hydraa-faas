import boto3
import json
import threading
import queue
import radical.utils as ru
# import aws cost from hydra

class AwsFaas:
    """
    Manages direct aws lambda function interactions for the FaasManager.
    This class is used for tasks where provider is 'aws'.
    """
