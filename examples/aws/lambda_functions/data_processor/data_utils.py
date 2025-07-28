from datetime import datetime

class DataValidator:
    """
    validates incoming data records
    """
    def __init__(self):
        self.required_fields = ['id', 'value', 'timestamp']
    
    def validate_record(self, record):
        """
        validate single record
        """
        if not isinstance(record, dict):
            return False, 'record must be dictionary'
        
        for field in self.required_fields:
            if field not in record:
                return False, f'missing required field: {field}'
        
        # validate types
        if not isinstance(record.get('id'), (str, int)):
            return False, 'id must be string or integer'
        
        if not isinstance(record.get('value'), (int, float)):
            return False, 'value must be numeric'
        
        return True, None
    
    def validate_batch(self, records):
        """
        validate batch of records
        """
        valid_records = []
        invalid_records = []
        
        for i, record in enumerate(records):
            is_valid, error = self.validate_record(record)
            if is_valid:
                valid_records.append(record)
            else:
                invalid_records.append({
                    'index': i,
                    'error': error,
                    'record': record
                })
        
        return valid_records, invalid_records

class DataTransformer:
    """
    transforms and enriches data records
    """
    def transform_record(self, record):
        """
        transform single record
        """
        transformed = {
            'id': str(record['id']),
            'value': float(record['value']),
            'timestamp': record['timestamp'],
            'processed_timestamp': datetime.utcnow().isoformat()
        }
        
        # add derived fields
        transformed['value_squared'] = transformed['value'] ** 2
        transformed['value_category'] = self.categorize_value(transformed['value'])
        
        # add metadata if present
        if 'metadata' in record:
            transformed['metadata'] = record['metadata']
        
        return transformed
    
    def categorize_value(self, value):
        """
        categorize numeric value
        """
        if value < 0:
            return 'negative'
        elif value < 10:
            return 'low'
        elif value < 100:
            return 'medium'
        else:
            return 'high'
    
    def transform_batch(self, records):
        """
        transform batch of records
        """
        return [self.transform_record(record) for record in records]

