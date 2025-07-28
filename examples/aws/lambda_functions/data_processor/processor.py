import json
import base64
from datetime import datetime
from data_utils import DataValidator, DataTransformer
from aggregator import DataAggregator

def process_handler(event, context):
    """
    data processing lambda function for etl operations
    """
    try:
        # extract data from event
        data_source = event.get('source', 'direct')
        
        if data_source == 's3':
            # handle s3 event
            records = event.get('Records', [])
            data = extract_s3_data(records)
        else:
            # handle direct invocation
            data = event.get('data', [])
        
        if not data:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'no data provided'})
            }
        
        # validate data
        validator = DataValidator()
        valid_records, invalid_records = validator.validate_batch(data)
        
        if not valid_records:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'no valid records found',
                    'invalid_count': len(invalid_records)
                })
            }
        
        # transform data
        transformer = DataTransformer()
        transformed_data = transformer.transform_batch(valid_records)
        
        # aggregate data
        aggregator = DataAggregator()
        summary = aggregator.generate_summary(transformed_data)
        
        # prepare response
        result = {
            'processed_at': datetime.utcnow().isoformat(),
            'total_records': len(data),
            'valid_records': len(valid_records),
            'invalid_records': len(invalid_records),
            'summary': summary,
            'sample_output': transformed_data[:5]  # return sample
        }
        
        return {
            'statusCode': 200,
            'body': json.dumps(result, default=str)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'processing failed: {str(e)}'})
        }

def extract_s3_data(records):
    """
    extract data from s3 event records
    """
    data = []
    for record in records:
        # simplified s3 data extraction
        if 's3' in record:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            # in real implementation i would fetch and parse s3 object
            data.append({
                'source': 's3',
                'bucket': bucket,
                'key': key,
                'timestamp': record['eventTime']
            })
    return data

