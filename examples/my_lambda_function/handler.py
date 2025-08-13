
import requests
import json

def handler(event, context):
    # A simple function that makes an API call and returns a message
    try:
        response = requests.get('https://httpbin.org/get', timeout=5)
        response.raise_for_status() # Raise an exception for bad status codes

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully called the test API!',
                'origin_ip': response.json().get('origin')
            })
        }
    except requests.RequestException as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
