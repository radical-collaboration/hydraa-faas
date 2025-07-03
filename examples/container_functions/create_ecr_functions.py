#!/usr/bin/env python3
"""
Container lambda project directory creator for hydraa faas manager ecr examples.
creates 5 containerized lambda functions with dockerfiles.

Usage:
    cd faas_manager/examples/container_functions
    python create_container_project.py
"""

import os
import json
from pathlib import Path


def create_function_app_py(function_id):
    """Create app.py for a specific container function"""
    memory_mb = 512 + (function_id * 256)
    processing_type = "image_process" if function_id % 2 == 0 else "data_transform"

    content = f'''import json
import os
import time
import sys
from datetime import datetime

def lambda_handler(event, context):
    """
    Container lambda function {function_id} handler for hydraa faas manager
    """

    # container specific configuration
    function_id = "{function_id}"
    memory_mb = {memory_mb}
    container_version = os.environ.get('CONTAINER_VERSION', '1.0.0')

    # get environment variables
    env = os.environ.get('ENV', 'production')
    function_type = os.environ.get('FUNCTION_TYPE', '{processing_type}')

    # process event data
    message = event.get('message', 'hello from container!')
    iteration = event.get('iteration', 0)
    container_test = event.get('container_test', False)
    data = event.get('data', {{}})

    # simulate container-specific processing
    processing_time = 0.2 + (int(function_id) * 0.05)
    start_time = time.time()

    # container function specific work
    input_array = data.get('input_array', [1, 2, 3, 4, 5])
    process_type = data.get('process_type', 'default')

    # different processing based on function id
    if int(function_id) % 2 == 0:
        # even functions do image-like processing
        result_data = [x * x + function_id for x in input_array]
        operation = "square_plus_id"
    else:
        # odd functions do data transformation
        result_data = [x * 3 - function_id for x in input_array]
        operation = "triple_minus_id"

    # simulate processing time
    time.sleep(max(0, processing_time - (time.time() - start_time)))
    actual_duration = time.time() - start_time

    # container runtime info
    runtime_info = {{
        'python_version': sys.version,
        'container_runtime': True,
        'memory_limit': memory_mb,
        'function_id': function_id
    }}

    # create response
    response = {{
        'statusCode': 200,
        'container_info': {{
            'function_id': function_id,
            'memory_mb': memory_mb,
            'container_version': container_version,
            'environment': env,
            'timestamp': datetime.utcnow().isoformat(),
            'iteration': iteration,
            'function_type': function_type
        }},
        'processing': {{
            'input_message': message,
            'input_data': input_array,
            'output_data': result_data,
            'operation': operation,
            'processing_time_seconds': round(actual_duration, 3),
            'process_type': process_type
        }},
        'runtime': runtime_info,
        'results': {{
            'sum_input': sum(input_array),
            'sum_output': sum(result_data),
            'container_test_mode': container_test,
            'data_transformation': operation
        }}
    }}

    # log for cloudwatch
    print(f"container function {{function_id}} completed iteration {{iteration}}")
    print(f"processed {{len(input_array)}} items in {{actual_duration:.3f}}s using {{operation}}")

    return {{
        'statusCode': 200,
        'headers': {{
            'Content-Type': 'application/json',
            'X-Container-Function-ID': function_id,
            'X-Container-Version': container_version
        }},
        'body': json.dumps(response)
    }}

# for local testing outside container
if __name__ == "__main__":
    # test with sample container data
    test_event = {{
        'message': 'test message for container function {function_id}',
        'iteration': 0,
        'container_test': True,
        'data': {{
            'input_array': [1, 2, 3, 4, 5],
            'process_type': 'container_processing'
        }}
    }}

    class MockContext:
        function_name = 'hydraa-container-function-{function_id}'
        memory_limit_in_mb = '{memory_mb}'
        remaining_time_in_millis = lambda: 60000

    os.environ['FUNCTION_ID'] = '{function_id}'
    os.environ['ENV'] = 'test'
    os.environ['CONTAINER_VERSION'] = '1.0.{function_id}'

    result = lambda_handler(test_event, MockContext())
    print("local container test result:")
    print(result['body'])
'''
    return content


def create_dockerfile(function_id):
    """Create dockerfile for container function"""
    content = f'''# use aws lambda python base image
FROM public.ecr.aws/lambda/python:3.9

# copy requirements and install dependencies
COPY requirements.txt ${{LAMBDA_TASK_ROOT}}
RUN pip install -r requirements.txt --target ${{LAMBDA_TASK_ROOT}}

# copy function code
COPY app.py ${{LAMBDA_TASK_ROOT}}
COPY config.json ${{LAMBDA_TASK_ROOT}}

# copy any additional files
COPY utils/ ${{LAMBDA_TASK_ROOT}}/utils/

# set environment variables
ENV FUNCTION_ID="{function_id}"
ENV CONTAINER_VERSION="1.0.{function_id}"
ENV PYTHONPATH="${{LAMBDA_TASK_ROOT}}"

# set the cmd to your handler
CMD ["app.lambda_handler"]
'''
    return content


def create_requirements_txt():
    """create requirements.txt for container functions"""
    return """numpy
pandas
requests
pillow
boto3
"""


def create_config_json(function_id):
    """Create config.json for container function"""
    config = {
        "function_id": function_id,
        "memory_mb": 512 + (function_id * 256),
        "timeout_seconds": 60,
        "container_config": {
            "base_image": "public.ecr.aws/lambda/python:3.9",
            "working_directory": "/var/task",
            "entry_point": "app.lambda_handler"
        },
        "description": f"hydraa container lambda function {function_id} - ecr deployment example",
        "tags": {
            "project": "hydraa-faas",
            "environment": "demo",
            "deployment_type": "container",
            "function_group": "ecr_deployment"
        }
    }
    return json.dumps(config, indent=2)


def create_utils_helper():
    """Create utils helper for container functions"""
    content = '''"""
Container-specific utilities for lambda functions
"""

import json
import os
import time
from datetime import datetime

def get_container_info():
    """Get container runtime information"""
    return {
        'function_id': os.environ.get('FUNCTION_ID', 'unknown'),
        'container_version': os.environ.get('CONTAINER_VERSION', '1.0.0'),
        'lambda_task_root': os.environ.get('LAMBDA_TASK_ROOT', '/var/task'),
        'aws_lambda_runtime_api': os.environ.get('AWS_LAMBDA_RUNTIME_API'),
        'aws_region': os.environ.get('AWS_REGION', 'us-east-1')
    }

def log_container_execution(function_id, event, duration, result):
    """Container-specific logging format"""
    container_info = get_container_info()

    log_entry = {
        'timestamp': datetime.utcnow().isoformat(),
        'function_id': function_id,
        'container_version': container_info['container_version'],
        'duration_seconds': duration,
        'event_size': len(json.dumps(event)),
        'result_size': len(json.dumps(result)) if result else 0,
        'runtime_type': 'container'
    }
    print(f"container_execution_log: {json.dumps(log_entry)}")

def validate_container_event(event, required_fields=None):
    """Validate container event structure"""
    if required_fields is None:
        required_fields = ['message', 'data']

    missing_fields = [field for field in required_fields if field not in event]
    if missing_fields:
        raise ValueError(f"missing required container fields: {missing_fields}")

    return True

def format_container_response(status_code, data, headers=None):
    """Container-specific response format"""
    if headers is None:
        headers = {
            'Content-Type': 'application/json',
            'X-Container-Runtime': 'true'
        }

    container_info = get_container_info()
    headers['X-Function-ID'] = container_info['function_id']
    headers['X-Container-Version'] = container_info['container_version']

    return {
        'statusCode': status_code,
        'headers': headers,
        'body': json.dumps(data) if isinstance(data, (dict, list)) else str(data)
    }
'''
    return content


def create_dockerignore():
    """Create .dockerignore file"""
    content = '''__pycache__/
*.pyc
*.pyo
*.pyd
.git/
.gitignore
.dockerignore
README.md
.env
.venv/
venv/
tests/
*.md
.DS_Store
'''
    return content


def main():
    """Create the complete container project structure"""
    print("creating container lambda project for hydraa faas manager ecr example")

    # create container function directories and files
    for i in range(5):
        function_dir = Path(f"function_{i}")
        function_dir.mkdir(exist_ok=True)

        # create dockerfile
        dockerfile = function_dir / "Dockerfile"
        dockerfile.write_text(create_dockerfile(i))

        # create app.py
        app_py = function_dir / "app.py"
        app_py.write_text(create_function_app_py(i))

        # create requirements.txt
        req_txt = function_dir / "requirements.txt"
        req_txt.write_text(create_requirements_txt())

        # create config.json
        config_json = function_dir / "config.json"
        config_json.write_text(create_config_json(i))

        # create .dockerignore
        dockerignore = function_dir / ".dockerignore"
        dockerignore.write_text(create_dockerignore())

        # create utils directory
        utils_dir = function_dir / "utils"
        utils_dir.mkdir(exist_ok=True)
        utils_init = utils_dir / "__init__.py"
        utils_init.write_text(create_utils_helper())

        print(f"created container function_{i}")


    print(f"container lambda project created successfully")
    print(f"directory: {Path.cwd()}")
    print(f"files created:")
    print(f"   5 container function directories (function_0 to function_4)")
    print(f"   30 container files (dockerfile, app.py, requirements.txt, config.json, .dockerignore, utils)")

if __name__ == "__main__":
    main()