# -*- coding: utf-8 -*-
"""A simple example of deploying a zip-based function to AWS Lambda.

This script demonstrates the end-to-end workflow for using the FaaS Manager
to deploy and invoke a Python function on AWS Lambda.

Prerequisites:
    - Your AWS credentials must be configured in your environment. The simplest
      way is to set the following environment variables:
        export AWS_ACCESS_KEY_ID="YOUR_KEY"
        export AWS_SECRET_ACCESS_KEY="YOUR_SECRET"
        export AWS_REGION="us-east-1" # or your preferred region
    - The `hydraa` library and the FaaS manager modules must be installed.

"""

import os
import shutil
import time
from pathlib import Path

# Import the necessary components from hydraa and the FaaS manager
from hydraa import Task, proxy
from hydraa.services import ServiceManager
from hydraa_faas.faas_manager.manager import FaasManager  # Assuming the manager is in a package

# --- 1. Set up a temporary project directory ---
# For a real project, this would be your actual source code directory.
# Here, we create it on the fly to make the example self-contained.
project_dir = Path("./my_lambda_function")
project_dir.mkdir(exist_ok=True)

try:
    # a. Create the Python handler file
    handler_code = """
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
"""
    (project_dir / "handler.py").write_text(handler_code)

    # b. Create a requirements.txt file for dependencies
    (project_dir / "requirements.txt").write_text("requests==2.28.1\n")

    print(f"Created temporary project directory: {project_dir.resolve()}")

    # --- 2. Initialize the Hydraa Proxy Manager for AWS ---
    # This loads the AWS credentials from your environment.
    print("\nInitializing proxy manager for AWS...")
    provider_mgr = proxy(['aws'])

    # --- 3. Initialize the FaaS Manager ---
    # For a pure AWS Lambda deployment, we don't need to define any VMs.
    # We just pass an empty list.
    print("Initializing FaaS Manager...")
    faas_mgr = FaasManager(
        proxy_mgr=provider_mgr,
        vms=[],  # No VMs needed for Lambda zip deployments
        asynchronous=False,  # Set to False to wait for deployments to complete
        auto_terminate=True  # Automatically clean up resources on shutdown
    )

    # --- 4. Initialize and Start the Service Manager ---
    # The ServiceManager orchestrates the lifecycle of other managers.
    print("Initializing and starting the Service Manager...")
    service_mgr = ServiceManager([faas_mgr])
    # The 'start_services' method creates a sandbox and starts the managers.
    service_mgr.start_services()

    # --- 5. Define a Task for Deployment ---
    # This task tells the FaaS manager what to deploy.
    print("\nDefining the deployment task...")
    deployment_task = Task(name="my-simple-api-caller")
    # Set attributes after instantiation, as they are not in the constructor
    deployment_task.provider = "lambda"
    deployment_task.source_path = str(project_dir.resolve())
    deployment_task.handler = "handler.handler"

    # --- 6. Submit the Task for Deployment ---
    print("Submitting the task for deployment to AWS Lambda...")
    faas_mgr.submit(deployment_task)

    # Because we set asynchronous=False, the script will wait here until
    # the deployment is complete.

    print("\nDeployment complete!")
    time.sleep(10)  # Give Lambda a moment to fully initialize the function

    # --- 7. Invoke the Deployed Function ---
    print("Invoking the deployed function...")
    try:
        # The invocation payload can be any JSON-serializable dictionary
        payload = {"key": "value"}
        response = faas_mgr.invoke(
            function_name="my-simple-api-caller",  # Use the original task name
            payload=payload
        )
        print("\n--- Invocation Response ---")
        print(response)
        print("---------------------------\n")
    except Exception as e:
        print(f"An error occurred during invocation: {e}")

finally:
    # --- 8. Shut Down the Service Manager ---
    # This is a crucial step. If auto_terminate=True, this will delete the
    # IAM role and the Lambda function from your AWS account.
    if 'service_mgr' in locals():
        print("Shutting down the Service Manager and cleaning up resources...")
        service_mgr.shutdown_services()
        print("Shutdown complete.")

    # Clean up the temporary project directory
    if project_dir.exists():
        shutil.rmtree(project_dir)
        print(f"Removed temporary project directory: {project_dir.resolve()}")

