# Local FaaS Agent (radical faas)

A lightweight agent that acts as an intermediate between a user and Knative serverless platform. Scripts and platform adapters for OpenFaaS CE (limited for developer testing) and Nuclio are provided for future work.

### Knative
- **Description**: kubernetes based serverless platform with auto scaling
- **Use Cases**: cloud native applications, microservices, event driven architectures
- **Features**: auto scaling to zero, traffic splitting, canary deployments

## Architecture

The FaaS agent uses **adapters** to provide platform-agnostic function deployment and management. The architecture consists of three main layers:

```
             ┌─────────────────────────┐
             │           User          │
             └─────────────┬───────────┘
                     HTTP REST API
┌─────────────────────────▼────────────────────────────────┐
│                   Flask API Gateway                      │
│                     (app.py)                             │
├─────────────────────────┬────────────────────────────────┤
│                 Platform Adapters                        │
│                    (base.py)                             │
├─────────────────────────┼────────────────────────────────┤
│     Knative Adapter     │         Docker Utils           │
│     (knative.py)        │      (docker_utils.py)         │
└─────────────────────────┼────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼──────┐ ┌────────▼────────┐ ┌──────▼───────┐
│   Knative    │ │    Docker       │ │   Container  │
│   Serving    │ │    Engine       │ │   Registry   │
└──────────────┘ └─────────────────┘ └──────────────┘
```

## Setup

```bash
git clone https://github.com/radical-collaboration/hydraa_faas
cd hydraa_faas/agent

# create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  

# install dependencies
pip install -r requirements.txt
```

### Infrastructure setup

#### Linux

```bash
# make scripts executable
chmod +x scripts/*.sh

# install and start docker (if not already running)
./scripts/install_docker.sh

# apply the group changes immediately (can log out the ec2 and back in)
newgrp docker

# install and start minikube
./scripts/install_minikube.sh

# install Knative
./scripts/install_knative.sh
```

#### MacOS

```bash
# make scripts executable
chmod +x scripts/*.sh

# install docker desktop (if not already installed)
./scripts/install_docker.sh

# install minikube and tools
./scripts/install_minikube.sh

# install knative
./scripts/install_knative.sh
```

### Configure environment

#### Linux
```bash
# configure docker to use minikubes daemon
eval $(minikube docker-env)

# add current user to docker group
sudo usermod -aG docker $USER

# apply the group changes immediately
newgrp docker
```

#### MacOS
```bash
# configure docker to use minikubes daemon
eval $(minikube docker-env)
```

### Start agent

```bash
python3 app.py
```

The agent will be available at `http://localhost:5000`

### Test setup

```bash
# health check
curl http://localhost:5000/health

# deploy a test function
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{"id": "hello-world", "handler": "def handle(req):\n    return \"Hello, World!\""}'

# invoke the function
curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{"id": "hello-world", "payload": {}}'
```

## Configuration (environment variables)

#### General configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `FAAS_WORKFLOW` | deployment workflow (`local` or `registry`) | `local` | no |
| `RADICAL_FAAS_PLATFORM` | target platform (only `knative` supported) | `knative` | no |
| `RADICAL_FAAS_REGISTRY` | container registry URL | - | Yes (if `registry` workflow) |
| `DOCKER_REPO_PREFIX` | docker repository prefix | `library` | no |
| `PORT` | flask server port | `5000` | no |
| `FLASK_DEBUG` | enable debug mode | `false` | no |

#### Knative configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `KNATIVE_DOMAIN` | knative domain for function URLs | `example.com` | Yes |
| `KNATIVE_NAMESPACE` | kubernetes namespace | `default` | no |
| `KNATIVE_TIMEOUT` | command timeout (seconds) | `60` | no |
| `KNATIVE_CLI_PATH` | path to kn CLI binary | `kn` | no |

#### Docker configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DOCKER_CMD` | docker command name | `docker` | no |
| `DOCKER_TIMEOUT` | docker operation timeout | `300` | no |

### Configuration examples

#### Local
```bash
export FAAS_WORKFLOW=local
export RADICAL_FAAS_PLATFORM=knative
export KNATIVE_DOMAIN=$(minikube ip).nip.io
export KNATIVE_NAMESPACE=default
export FLASK_DEBUG=true
```

#### Production (knative with registry)
```bash
export FAAS_WORKFLOW=registry
export RADICAL_FAAS_PLATFORM=knative
export RADICAL_FAAS_REGISTRY=your-registry.com/your-namespace
export KNATIVE_DOMAIN=your-production-domain.com
export KNATIVE_NAMESPACE=production
```

## API

### Health check

**GET** `/health`

Check agent health and configuration status.

**Response:**
```json
{
  "status": "healthy",
  "platform": "knative",
  "workflow": "local"
}
```

### Deploy function

**POST** `/deploy`

Deploy a Python function to Knative.

**Request Body:**
```json
{
  "id": "function-name",           // optional (will be generated if not provided)
  "handler": "def handle(req):\n    return f'Hello {req}'",  // required
  "requirements": "requests==2.31.0\nnumpy==1.24.0",       // optional
  "params": {                      // optional dockerfile template parameters
    "PYTHON_VERSION": "3.11"
  }
}
```

**Response:**
```json
{
  "id": "function-name",
  "image": "library/function-name:latest",
  "status": "deployed",
  "platform": "knative",
  "endpoint": "http://function-name.default.192.168.49.2.nip.io"
}
```

**Status Codes:**
- `200`: Function deployed successfully
- `400`: Invalid request body or missing required fields
- `500`: Deployment failed

### Invoke function

**POST** `/invoke`

Invoke a deployed function with optional payload.

**Request Body:**
```json
{
  "id": "function-name",           // required
  "payload": {                     // optional function input data
    "name": "World",
    "message": "Hello from FaaS!"
  }
}
```

**Response:**
```json
{
  "result": "Hello World",        // function return value
  "status": "success",
  "execution_time": 0.25,         // execution time in seconds
  "platform": "knative"
}
```

**Status Codes:**
- `200`: Function executed successfully
- `400`: Invalid request body or missing function ID
- `500`: Function execution failed

### List functions

**GET** `/functions`

List all deployed functions

**Response:**
```json
{
  "functions": [
    {
      "id": "function-name",
      "image": "library/function-name:latest",
      "ready": true,
      "platform": "knative"
    }
  ]
}
```

**Status Codes:**
- `200`: Functions listed successfully
- `500`: Listing failed

## Examples

### Basic function

```bash
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "id": "hello-world",
    "handler": "def handle(req):\n    return \"Hello, World!\""
  }'

curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "id": "hello-world",
    "payload": {}
  }'
```

### Function with dependencies

```bash
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "id": "web-scraper",
    "handler": "import requests\ndef handle(req):\n    response = requests.get(req[\"url\"])\n    return {\"status\": response.status_code, \"length\": len(response.text)}",
    "requirements": "requests==2.31.0"
  }'

curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "id": "web-scraper",
    "payload": {
      "url": "https://httpbin.org/json"
    }
  }'
```

### Data processing function

```bash
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "id": "data-processor",
    "handler": "import json\ndef handle(req):\n    data = json.loads(req) if isinstance(req, str) else req\n    numbers = data.get(\"numbers\", [])\n    return {\"sum\": sum(numbers), \"average\": sum(numbers)/len(numbers) if numbers else 0}",
    "requirements": ""
  }'

curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "id": "data-processor",
    "payload": {
      "numbers": [1, 2, 3, 4, 5]
    }
  }'
```

### JSON processing

```bash
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{
    "id": "json-transformer",
    "handler": "import json\ndef handle(req):\n    data = req if isinstance(req, dict) else json.loads(req)\n    transformed = {}\n    for key, value in data.items():\n        if isinstance(value, str):\n            transformed[key.upper()] = value.lower()\n        elif isinstance(value, (int, float)):\n            transformed[key.upper()] = value * 2\n        else:\n            transformed[key.upper()] = value\n    return transformed"
  }'

curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{
    "id": "json-transformer",
    "payload": {
      "name": "John Doe",
      "age": 30,
      "score": 85.5,
      "active": true
    }
  }'
```

### Testing

```bash
# basic health check
curl http://localhost:5000/health

# test function deployment and invocation
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{"handler": "def handle(req): return \"test\""}' \
&& curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{"id": "func-XXXXXXXX", "payload": {}}'
```

## Troubleshooting

### Platform-Specific Issues

#### MacOS

**Issue**: Connection timeout to minikube IP
```
Connection to 192.168.49.2 timed out. (connect timeout=300)
```

**Solutions**:
1. **Use minikube tunnel** (recommended):
   ```bash
   # In a separate terminal:
   minikube tunnel
   ```

2. **Restart Docker Desktop**:
   ```bash
   # Stop and start Docker Desktop
   # Then restart minikube
   minikube stop && minikube start
   ```

3. **Check Docker Desktop settings**:
   - Ensure Docker Desktop is running
   - Verify Kubernetes is enabled in Docker Desktop settings

#### Linux

**Error**: `permission denied while trying to connect to the docker daemon socket`

**Solution**:
```bash
# add user to docker group
sudo usermod -aG docker $USER

# logout and login again, or use
newgrp docker

# verify docker access
docker run hello-world
```

### General issues

#### Minikube docker environment not set
**Error**: images not found during deployment type of errors

**Solution**:
```bash
# check if environment is set
echo $DOCKER_HOST

# if empty, run
eval $(minikube docker-env)

# verify it's set
echo $DOCKER_HOST
```

#### Function deployment fails
**Error**: deployment related errors

**Debugging Steps**:
```bash
# check platform logs (knative)
kubectl logs -n knative-serving -l app=controller --tail=50

# check function-specific logs
kubectl logs -n default -l serving.knative.dev/service=FUNCTION_NAME
```

#### Image pull errors
**Error**: `ImagePullBackOff` or something very close to this

**Solutions**:
- **Local**: ensure `eval $(minikube docker-env)` is executed
- **Registry**: verify registry credentials and image names
- **Check image exists**: `docker images | grep FUNCTION_NAME`

#### Connection timeouts
**Error**: timeout errors during function invocation

**Solutions**:
```bash
# increase timeout values
export KNATIVE_TIMEOUT=120

# check if services are ready
kubectl get ksvc
```

#### Port conflicts
**Error**: Port already in use

**Solution**:
```bash
# change agent port
export PORT=5001

# or kill existing processes (be careful as you can kill something important)
lsof -ti:5000 | xargs kill -9
```