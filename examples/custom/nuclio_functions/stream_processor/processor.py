import json
import os
from datetime import datetime
from stream_utils import EventProcessor, EventAggregator
from storage import StorageHandler

def process_stream(context, event):
    """
    nuclio handler for stream processing
    """
    try:
        # get configuration
        worker_id = os.environ.get('WORKER_ID', '0')
        batch_size = int(os.environ.get('BATCH_SIZE', '100'))
        
        # initialize components
        processor = EventProcessor(context.logger)
        aggregator = EventAggregator()
        storage = StorageHandler()
        
        # parse request
        body = event.body
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        
        data = json.loads(body) if body else {}
        
        # handle different trigger types
        trigger_kind = event.trigger.kind
        
        if trigger_kind == 'http':
            # process direct http request
            events = data.get('events', [])
            context.logger.info(f'processing {len(events)} events via http')
        elif trigger_kind == 'kafka':
            # process kafka message
            events = [data]
            context.logger.info(f'processing kafka message')
        elif trigger_kind == 'kinesis':
            # process kinesis record
            events = [data]
            context.logger.info(f'processing kinesis record')
        else:
            events = [data]
        
        if not events:
            return json.dumps({
                'error': 'no events to process'
            }, status_code=400)
        
        # process events
        processed_events = []
        for evt in events:
            processed = processor.process_event(evt)
            processed_events.append(processed)
        
        # aggregate results
        summary = aggregator.aggregate(processed_events)
        
        # store results if configured
        if os.environ.get('ENABLE_STORAGE', 'false').lower() == 'true':
            storage_key = storage.store_results(summary, worker_id)
            summary['storage_key'] = storage_key
        
        # prepare response
        response = {
            'worker_id': worker_id,
            'processed_count': len(processed_events),
            'timestamp': datetime.utcnow().isoformat(),
            'trigger': trigger_kind,
            'summary': summary,
            'sample_events': processed_events[:5]  # return sample
        }
        
        return json.dumps(response)
        
    except Exception as e:
        context.logger.error(f'stream processing error: {str(e)}')
        return json.dumps({
            'error': f'processing failed: {str(e)}'
        }, status_code=500)

