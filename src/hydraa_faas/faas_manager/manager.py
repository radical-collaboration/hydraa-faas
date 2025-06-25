import threading as mt
import radical.utils as ru
from hydraa.providers.proxy import proxy
from .aws_lambda import AwsFaas
from .agent_faas import AgentFaas

PROVIDER_TO_CLASS = {
    'aws': AwsFaas,
    'agent': AgentFaas,
}

class FaasManager:
    """
    The FaasManager class orchestrates FaaS task execution by using
    the appropriate provider (e.g., AwsFaas, AgentFaas)
    """
