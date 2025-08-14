import json
import re

def preprocess_text(text):
    """
    basic text preprocessing
    """
    # remove special characters
    text = re.sub(r'[^a-zA-Z0-9\s]', '', text)
    # normalize whitespace
    text = ' '.join(text.split())
    return text.strip()

def format_response(status_code, body):
    """
    format lambda response
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps(body)
    }
