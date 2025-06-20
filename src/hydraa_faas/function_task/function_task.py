from hydraa.cloud_task.task import Task
from typing import Optional, Dict, Any

class FunctionTask(Task):
    """
    Represents a FaasS task that can be used for aws lambda or the agent

    The provider attribute determines the target
        'aws': for direct interaction with aws lambda
        'agent': for interaction for faas middleware 
    """
    def __init__(self,
                 # both platfroms
                 function_name: str,
                 payload: Dict[str, Any],
                 
                 
                 provider: str,

                 # agent
                 agent_url: Optional[str] = None,

                 # lambda
                 handler: Optional[str] = None,
                 runtime: Optional[str] = None,
                 zip_file_path: Optional[str] = None,
                 role_arn: Optional[str] = None,
                 timeout: int = 30,
                 memory_size: int = 128,
                 **kwargs):
        """
        Initializes a new FaasTask

        Args:
            function_name (str): The name/ID of the function.
            payload (Dict[str, Any]): The input data for the function.
            provider (str): The target backend, either 'aws' or 'agent'.
            agent_url (Optional[str]): The base URL of the middleware agent. Required if provider is 'agent'.
            handler (Optional[str]): The function handler (for AWS Lambda creation).
            runtime (Optional[str]): The function runtime (for AWS Lambda creation).
            zip_file_path (Optional[str]): Path to the deployment package (for AWS Lambda creation).
            role_arn (Optional[str]): The IAM role ARN (for AWS Lambda creation).
            timeout (int): Function execution timeout in seconds.
            memory_size (int): Memory allocation in MB.
        """
        # pass provider to the parent class constructor
        super().__init__(provider=provider, **kwargs)

        # both platfroms
        self.function_name = function_name
        self.payload = payload

        # agent
        self.agent_url = agent_url

        # lambda
        self.handler = handler
        self.runtime = runtime
        self.zip_file_path = zip_file_path
        self.role_arn = role_arn
        self.timeout = timeout
        self.memory_size = memory_size

        self._verify()

    def _verify(self):
        """
        Verifies that the required attributes for the specified provider are present
        """
        if not self.provider or self.provider not in ['aws', 'agent']:
            raise ValueError("A valid provider ('aws' or 'agent') is required")

        if self.provider == 'agent':
            if not self.agent_url:
                raise ValueError("'agent_url' is required when provider is 'agent'")
        
        if not self.function_name or self.payload is None:
             raise ValueError("'function_name' and 'payload' are required for all FaaS tasks")