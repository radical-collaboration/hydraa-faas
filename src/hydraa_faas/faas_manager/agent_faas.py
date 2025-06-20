import requests
import threading
import queue
import radical.utils as ru

class AgentFaas:
    """
    Manages interactions with a faas middleware agent.
    This class is used for tasks where provider is 'agent'. 
    """
