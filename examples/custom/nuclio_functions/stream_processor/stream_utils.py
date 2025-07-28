from datetime import datetime
from collections import defaultdict

class EventProcessor:
    """
    processes individual stream events
    """
    def __init__(self, logger):
        self.logger = logger
        self.event_types = ['click', 'view', 'purchase', 'order', 'payment']
    
    def process_event(self, event):
        """
        process a single event
        """
        # validate event
        if not isinstance(event, dict):
            return {'error': 'invalid event format'}
        
        # extract fields
        event_id = event.get('id', 'unknown')
        event_type = event.get('type', 'unknown')
        timestamp = event.get('timestamp', datetime.utcnow().isoformat())
        
        # enrich event
        processed = {
            'id': event_id,
            'type': event_type,
            'timestamp': timestamp,
            'processed_at': datetime.utcnow().isoformat(),
            'valid': event_type in self.event_types
        }
        
        # add type-specific processing
        if event_type == 'purchase' or event_type == 'order':
            processed['amount'] = event.get('amount', 0)
            processed['currency'] = event.get('currency', 'USD')
        
        if event_type == 'click':
            processed['target'] = event.get('target', 'unknown')
            processed['source'] = event.get('source', 'direct')
        
        # add metadata
        for key, value in event.items():
            if key not in ['id', 'type', 'timestamp']:
                processed[f'meta_{key}'] = value
        
        return processed

class EventAggregator:
    """
    aggregates processed events
    """
    def aggregate(self, events):
        """
        create summary statistics
        """
        if not events:
            return {}
        
        # count by type
        type_counts = defaultdict(int)
        valid_count = 0
        total_amount = 0.0
        
        for event in events:
            if isinstance(event, dict):
                event_type = event.get('type', 'unknown')
                type_counts[event_type] += 1
                
                if event.get('valid', False):
                    valid_count += 1
                
                if 'amount' in event:
                    total_amount += float(event['amount'])
        
        # create summary
        summary = {
            'total_events': len(events),
            'valid_events': valid_count,
            'invalid_events': len(events) - valid_count,
            'event_types': dict(type_counts),
            'total_amount': round(total_amount, 2)
        }
        
        return summary
