import os
import json
import time
from hydraa import Task, proxy
from hydraa_faas.faas_manager.manager import FaasManager
from hydraa.services.service_manager import ServiceManager

# Enable more detailed logging
import logging

logging.basicConfig(level=logging.DEBUG)

# Initialize components
proxy_mgr = proxy(['aws'])
faas_manager = FaasManager(proxy_mgr, auto_terminate=False)

service_mgr = ServiceManager([faas_manager])
service_mgr.start_services()

# Create a test source directory with a Dockerfile
test_source = '/tmp/container_test'
os.makedirs(test_source, exist_ok=True)

# Create handler.py
with open(f'{test_source}/handler.py', 'w') as f:
    f.write("""
import json
import os

def handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Hello from container!',
            'container_id': os.environ.get('HOSTNAME', 'unknown'),
            'event': event,
            'python_version': os.sys.version
        })
    }
""")

# Create requirements.txt
with open(f'{test_source}/requirements.txt', 'w') as f:
    f.write("requests==2.31.0\n")

# Create Dockerfile for Lambda
with open(f'{test_source}/Dockerfile', 'w') as f:
    f.write("""FROM public.ecr.aws/lambda/python:3.9

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY handler.py ./

CMD ["handler.handler"]
""")

# Let's check the registry configuration after ECR creation
lambda_provider = faas_manager._get_provider('lambda')['instance']

# Check if ECR repo exists or create it
print("\n🔍 Checking ECR repository...")
repo_uri = lambda_provider._create_ecr_repository()
print(f"ECR Repository URI: {repo_uri}")

# Check registry configurations
print("\n🔍 Registry configurations:")
for name, config in lambda_provider.registry_manager._registry_configs.items():
    print(f"  {name}: {config}")

# Now let's manually test the build and push
print("\n🏗️ Testing manual build and push...")
try:
    image_uri, build_metrics, push_metrics = lambda_provider.registry_manager.build_and_push_image(
        source_path=test_source,
        repository_uri=repo_uri,
        image_tag='test-manual'
    )
    print(f"✅ Successfully built and pushed: {image_uri}")
    print(f"   Build time: {build_metrics.build_time_ms:.2f}ms")
    print(f"   Push time: {push_metrics.push_time_ms:.2f}ms")
except Exception as e:
    print(f"❌ Manual build/push failed: {e}")
    import traceback

    traceback.print_exc()

# Now try the full deployment
print("\n🚀 Testing full deployment...")
task = Task(
    name='container-hello',
    vcpus=0.5,
    memory=512,
    image='python:3.9',
    env_var=[
        'FAAS_PROVIDER=lambda',
        f'FAAS_SOURCE={test_source}',
        'FAAS_HANDLER=handler.handler',
        'FAAS_TIMEOUT=30',
        'CONTAINER_ENV=production'
    ]
)

try:
    faas_manager.submit(task)
    function_name = task.result()
    print(f"✅ Deployed: {function_name}")

    # Test invocation
    print("\n📊 Testing container function...")
    response = faas_manager.invoke(function_name, {'test': 'data'})
    print(f"Response: {json.dumps(response['payload'], indent=2)}")
except Exception as e:
    print(f"❌ Deployment failed: {e}")
    import traceback

    traceback.print_exc()

# Check what images exist locally
print("\n🐳 Local Docker images:")
import docker

docker_client = docker.from_env()
for image in docker_client.images.list():
    for tag in image.tags:
        if 'hydra' in tag or 'ecr' in tag:
            print(f"  {tag}")

# Cleanup
input("\nPress Enter to cleanup...")
service_mgr.shutdown_services()