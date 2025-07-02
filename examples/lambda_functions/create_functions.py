#!/usr/bin/env python3
"""
Lambda project directory creator that makes example functions for hydraa FaaS manager examples,
this script to create all 10 Lambda function directories with proper files.

Usage:
    cd faas_manager/examples/lambda_functions
    python create_project.py
"""

import os
import json
from pathlib import Path


def create_function_main_py(function_id):
    """Create main.py for a specific function"""
    memory_mb = 256 + (function_id * 64)
    processing_type = "even_squares" if function_id % 2 == 0 else "odd_doubles"
    operation = "item ** 2" if function_id % 2 == 0 else "item * 2"

    content = f'''import json
import os
import time
from datetime import datetime

def handler(event, context):
    """
    Lambda function {function_id} handler for Hydraa FaaS Manager
    """

    # function specific configuration
    function_id = "{function_id}"
    memory_mb = {memory_mb}

    # get environment variables
    env = os.environ.get('ENV', 'production')

    # process event data
    message = event.get('message', 'Hello from Lambda!')
    iteration = event.get('iteration', 0)
    test_data = event.get('test_data', [])

    # simulate processing based on function ID
    processing_time = 0.1 + (int(function_id) * 0.02)

    # do some computation
    start_time = time.time()

    # function specific work
    result_data = []
    for i, item in enumerate(test_data):
        result_data.append({operation})  # function {function_id} {"squares" if function_id % 2 == 0 else "doubles"} values

    # simulate the processing time
    time.sleep(max(0, processing_time - (time.time() - start_time)))

    actual_duration = time.time() - start_time

    # create response
    response = {{
        'statusCode': 200,
        'function_info': {{
            'function_id': function_id,
            'memory_mb': memory_mb,
            'environment': env,
            'timestamp': datetime.utcnow().isoformat(),
            'iteration': iteration
        }},
        'processing': {{
            'input_message': message,
            'input_data_count': len(test_data),
            'output_data_count': len(result_data),
            'processing_time_seconds': round(actual_duration, 3)
        }},
        'results': {{
            'processed_data': result_data,
            'sum_input': sum(test_data) if test_data else 0,
            'sum_output': sum(result_data) if result_data else 0,
            'function_type': '{processing_type}'
        }}
    }}

    # log for cloudwatch
    print(f"function {{function_id}} completed iteration {{iteration}}")
    print(f"processed {{len(test_data)}} items in {{actual_duration:.3f}}s")

    return {{
        'statusCode': 200,
        'headers': {{
            'Content-Type': 'application/json',
            'X-Function-ID': function_id
        }},
        'body': json.dumps(response, indent=2)
    }}

# for local testing
if __name__ == "__main__":
    # test with sample data
    test_event = {{
        'message': 'test message for function {function_id}',
        'iteration': 0,
        'test_data': [1, 2, 3, 4, 5]
    }}

    class MockContext:
        function_name = 'hydraa-function-{function_id}'
        memory_limit_in_mb = '{memory_mb}'
        remaining_time_in_millis = lambda: 30000

    os.environ['FUNCTION_ID'] = '{function_id}'
    os.environ['ENV'] = 'test'

    result = handler(test_event, MockContext())
    print("Local test result:")
    print(result['body'])
'''
    return content


def create_requirements_txt():
    """Create requirements.txt for Lambda functions"""
    return """
    numpy==1.24.3
    pandas==2.0.3 
    requests==2.31.0
    """


def create_config_json(function_id):
    """Create config.json for a function"""
    config = {
        "function_id": function_id,
        "memory_mb": 256 + (function_id * 64),
        "timeout_seconds": 30,
        "description": f"hydraa lambda function {function_id} - batch deployment example",
        "tags": {
            "project": "hydraa-faas",
            "environment": "demo",
            "function_group": "batch_deployment"
        }
    }
    return json.dumps(config, indent=2)


def create_shared_utils():
    """Create shared utilities module"""
    content = '''"""
shared utilities for lambda functions
"""

import json
import time
from datetime import datetime

def log_execution(function_id, event, duration, result):
    """Standard logging format for all functions"""
    log_entry = {
        'timestamp': datetime.utcnow().isoformat(),
        'function_id': function_id,
        'duration_seconds': duration,
        'event_size': len(json.dumps(event)),
        'result_size': len(json.dumps(result)) if result else 0
    }
    print(f"EXECUTION_LOG: {json.dumps(log_entry)}")

def validate_event(event, required_fields=None):
    """Validate incoming event structure"""
    if required_fields is None:
        required_fields = ['message']

    missing_fields = [field for field in required_fields if field not in event]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")

    return True

def format_response(status_code, data, headers=None):
    """Standard response format"""
    if headers is None:
        headers = {'Content-Type': 'application/json'}

    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps(data, indent=2) if isinstance(data, (dict, list)) else str(data)
    }
'''
    return content


def main():
    """Create the complete Lambda project structure"""
    print("creating lambda project for Hydraa FaaS Manager example")

    # create function directories and files
    for i in range(10):
        function_dir = Path(f"function_{i}")
        function_dir.mkdir(exist_ok=True)

        # create main.py
        main_py = function_dir / "main.py"
        main_py.write_text(create_function_main_py(i))

        # create requirements.txt
        req_txt = function_dir / "requirements.txt"
        req_txt.write_text(create_requirements_txt())

        # create config.json
        config_json = function_dir / "config.json"
        config_json.write_text(create_config_json(i))

        print(f"created function_{i}")

    # create shared utilities
    utils_dir = Path("shared_utils")
    utils_dir.mkdir(exist_ok=True)
    utils_init = utils_dir / "__init__.py"
    utils_init.write_text(create_shared_utils())
    print("created shared_utils")


    print(f"lambda project created successfully")
    print(f"directory: {Path.cwd()}")
    print(f"files created:")
    print(f"   10 function directories (function_0 to function_9)")
    print(f"   30 function files (main.py, requirements.txt, config.json)")
    print(f"   1 shared utilities module")
    print(f"   1 README file")

if __name__ == "__main__":
    main()