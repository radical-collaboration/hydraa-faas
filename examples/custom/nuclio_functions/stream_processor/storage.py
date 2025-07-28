import json
import uuid
from datetime import datetime

class StorageHandler:
    """
    handles storage of processed results
    in production, this would integrate with s3, dynamodb, etc.
    """
    def __init__(self):
        self.storage_type = 'memory'  # simulated storage
        self.storage = {}
    
    def store_results(self, data, worker_id):
        """
        store processing results
        """
        # generate unique key
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        key = f'results/{worker_id}/{timestamp}_{unique_id}.json'
        
        # simulate storage
        self.storage[key] = {
            'data': data,
            'stored_at': datetime.utcnow().isoformat(),
            'worker_id': worker_id
        }
        
        return key
    
    def retrieve_results(self, key):
        """
        retrieve stored results
        """
        return self.storage.get(key)
    
    def list_results(self, prefix=None):
        """
        list stored results
        """
        if prefix:
            return [k for k in self.storage.keys() if k.startswith(prefix)]
        return list(self.storage.keys())
