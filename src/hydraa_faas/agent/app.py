#!/usr/bin/env python3
"""Flask app for knative function management."""

import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, request
from jinja2 import Environment, FileSystemLoader

from platforms.knative import KnativePlatform
from utils.docker_utils import DockerUtils

# initialize flask app
app = Flask(__name__)

# global configuration
CONFIG = {
    'FAAS_WORKFLOW': os.getenv('FAAS_WORKFLOW', 'local'),
    'RADICAL_FAAS_REGISTRY': os.getenv('RADICAL_FAAS_REGISTRY'),
}

# initialize platform adapter and utilities
platform_adapter = KnativePlatform()
docker_utils = DockerUtils()

# initialize jinja2 template environment
template_dir = Path(__file__).parent / 'templates'
jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))


def generate_function_id() -> str:
    """Generate a unique function ID.

    Returns:
        Unique identifier prefixed with 'func-'.
    """
    return f"func-{uuid.uuid4().hex[:8]}"


def get_image_name(func_id: str) -> str:
    """Generate docker image name based on workflow.

    Args:
        func_id: Function identifier.

    Returns:
        Docker image name.
    """
    if CONFIG['FAAS_WORKFLOW'] == 'local':
        return f"{func_id}:latest"
    else:
        registry = CONFIG['RADICAL_FAAS_REGISTRY'].rstrip('/')
        return f"{registry}/{func_id}:latest"


def create_function_package(func_id: str, handler_code: str,
                          requirements: Optional[str] = None) -> str:
    """Create function package with dockerfile and handler.

    Args:
        func_id: Function identifier.
        handler_code: Python code for the function.
        requirements: Python package requirements.

    Returns:
        Path to temporary directory with package.

    Raises:
        Exception: If file creation fails.
    """
    package_dir = tempfile.mkdtemp(prefix=f"faas-{func_id}-")
    package_path = Path(package_dir)

    try:
        # set template parameters
        template_params = {
            'PYTHON_VERSION': '3.11',
            'REQUIREMENTS': requirements or '',
        }

        # generate dockerfile from template
        dockerfile_template = jinja_env.get_template('Dockerfile.j2')
        dockerfile_content = dockerfile_template.render(**template_params)

        # create knative handler
        handler_template = jinja_env.get_template('knative_handler.py.j2')
        handler_content = handler_template.render(handler_code=handler_code)

        # write files
        (package_path / 'Dockerfile').write_text(dockerfile_content)
        (package_path / 'handler.py').write_text(handler_content)

        if requirements:
            (package_path / 'requirements.txt').write_text(requirements)

        return package_dir

    except Exception as e:
        shutil.rmtree(package_dir, ignore_errors=True)
        raise e


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint.

    Returns:
        JSON response with service status.
    """
    return jsonify({
        'status': 'healthy',
        'platform': 'knative',
        'workflow': CONFIG['FAAS_WORKFLOW']
    })


@app.route('/deploy', methods=['POST'])
def deploy_function():
    """Deploy function to knative.

    Returns:
        JSON response with deployment result or error.
    """
    try:
        data = request.get_json()
        if not data or not data.get('handler'):
            return jsonify({'error': 'handler field is required'}), 400

        # extract parameters
        func_id = data.get('id') or generate_function_id()
        handler_code = data['handler']
        requirements = data.get('requirements')

        # create and build function package
        package_dir = create_function_package(func_id, handler_code,
                                              requirements)

        try:
            image_name = get_image_name(func_id)

            # build and optionally push docker image
            docker_utils.build_image(package_dir, image_name)

            if CONFIG['FAAS_WORKFLOW'] == 'registry':
                docker_utils.push_image(image_name)

            # deploy to knative
            deployment_result = platform_adapter.deploy(func_id, image_name)

            response = {
                'id': func_id,
                'image': image_name,
                **deployment_result
            }

            return jsonify(response), 200

        finally:
            shutil.rmtree(package_dir, ignore_errors=True)

    except Exception as e:
        return jsonify({'error': f"deployment failed: {str(e)}"}), 500


@app.route('/invoke', methods=['POST'])
def invoke_function():
    """Invoke deployed function.

    Returns:
        JSON response with function result or error.
    """
    try:
        data = request.get_json()
        if not data or not data.get('id'):
            return jsonify({'error': 'id field is required'}), 400

        func_id = data['id']
        payload = data.get('payload', {})

        result = platform_adapter.invoke(func_id, payload)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': f"function invocation failed: {str(e)}"}), 500


@app.route('/functions', methods=['GET'])
def list_functions():
    """List deployed functions.

    Returns:
        JSON response with list of functions or error.
    """
    try:
        functions = platform_adapter.list_functions()
        return jsonify({'functions': functions}), 200
    except Exception as e:
        return jsonify({'error': f"function listing failed: {str(e)}"}), 500


@app.route('/delete', methods=['POST'])
def delete_function():
    """Delete deployed function.

    Returns:
        JSON response with deletion result or error.
    """
    try:
        data = request.get_json()
        if not data or not data.get('id'):
            return jsonify({'error': 'id field is required'}), 400

        func_id = data['id']
        result = platform_adapter.delete_function(func_id)
        return jsonify(result), 200

    except Exception as e:
        return jsonify({'error': f"function deletion failed: {str(e)}"}), 500


if __name__ == '__main__':
    print("FaaS middleware agent starting")
    print(f"platform: knative")
    print(f"workflow: {CONFIG['FAAS_WORKFLOW']}")

    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)