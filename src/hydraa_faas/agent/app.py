#!/usr/bin/env python3
"""
FaaS middleware agent main application
"""

import os
import json
import uuid
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Any, Optional

from flask import Flask, request, jsonify
from jinja2 import Environment, FileSystemLoader

from platforms.base import FaasPlatform
from platforms.knative import KnativePlatform
from platforms.nuclio import NuclioPlatform
from utils.docker_utils import DockerUtils

# initialize Flask app
app = Flask(__name__)

# global configuration
CONFIG = {
    'FAAS_WORKFLOW': os.getenv('FAAS_WORKFLOW', 'local'),
    'RADICAL_FAAS_PLATFORM': os.getenv('RADICAL_FAAS_PLATFORM', 'knative'),
    'RADICAL_FAAS_REGISTRY': os.getenv('RADICAL_FAAS_REGISTRY'),
}

# initialize platform adapter and utilities
def get_platform_adapter() -> FaasPlatform:
    """Factory function to create the appropriate platform adapter"""
    platform_name = CONFIG['RADICAL_FAAS_PLATFORM']
    return KnativePlatform() if platform_name == 'knative' else NuclioPlatform()

platform_adapter = get_platform_adapter()
docker_utils = DockerUtils()

# initialize Jinja2 template environment
template_dir = Path(__file__).parent / 'templates'
jinja_env = Environment(loader=FileSystemLoader(str(template_dir)))

# utility functions
def generate_function_id() -> str:
    """Generate a unique function ID"""
    return f"func-{uuid.uuid4().hex[:8]}"

def get_image_name(func_id: str) -> str:
    """Generate the appropriate Docker image name based on workflow"""
    if CONFIG['FAAS_WORKFLOW'] == 'local':
        return f"{func_id}:latest"
    else:
        registry = CONFIG['RADICAL_FAAS_REGISTRY'].rstrip('/')
        return f"{registry}/{func_id}:latest"



def create_function_package(func_id: str, handler_code: str, 
                          requirements: Optional[str] = None) -> str:
    """Create a complete function package with dockerfile and handler code"""
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
        
        # create the appropriate handler based on platform
        platform = CONFIG['RADICAL_FAAS_PLATFORM']
        if platform == 'nuclio':
            handler_template = jinja_env.get_template('nuclio_handler.py.j2')
        else:  # knative
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

# API
@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'platform': CONFIG['RADICAL_FAAS_PLATFORM'],
        'workflow': CONFIG['FAAS_WORKFLOW']
    })

@app.route('/deploy', methods=['POST'])
def deploy_function():
    """Deploy a function to the configured FaaS platform"""
    try:
        data = request.get_json()
        if not data or not data.get('handler'):
            return jsonify({'error': 'handler field is required'}), 400
        
        # extract parameters
        func_id = data.get('id') or generate_function_id()
        handler_code = data['handler']
        requirements = data.get('requirements')
        
        # create and build function package
        package_dir = create_function_package(func_id, handler_code, requirements)
        
        try:
            image_name = get_image_name(func_id)
            
            # build and optionally push docker image (this is for registry workflow)
            docker_utils.build_image(package_dir, image_name)
            
            if CONFIG['FAAS_WORKFLOW'] == 'registry':
                docker_utils.push_image(image_name)
            
            # deploy to faas platform
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
        return jsonify({'error': f"Deployment failed: {str(e)}"}), 500

@app.route('/invoke', methods=['POST'])
def invoke_function():
    """Invoke a deployed function"""
    try:
        data = request.get_json()
        if not data or not data.get('id'):
            return jsonify({'error': 'id field is required'}), 400
        
        func_id = data['id']
        payload = data.get('payload', {})
        
        result = platform_adapter.invoke(func_id, payload)
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': f"Function invocation failed: {str(e)}"}), 500

@app.route('/functions', methods=['GET'])
def list_functions():
    """List deployed functions"""
    try:
        functions = platform_adapter.list_functions()
        return jsonify({'functions': functions}), 200
    except Exception as e:
        return jsonify({'error': f"Function listing failed: {str(e)}"}), 500

@app.route('/delete', methods=['POST'])
def delete_function():
    """Delete a deployed function"""
    try:
        data = request.get_json()
        if not data or not data.get('id'):
            return jsonify({'error': 'id field is required'}), 400
        
        func_id = data['id']
        result = platform_adapter.delete_function(func_id)
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': f"Function deletion failed: {str(e)}"}), 500

if __name__ == '__main__':
    print(f"FaaS Middleware Agent Starting")
    print(f"Platform: {CONFIG['RADICAL_FAAS_PLATFORM']}")
    print(f"Workflow: {CONFIG['FAAS_WORKFLOW']}")
    
    port = int(os.getenv('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)