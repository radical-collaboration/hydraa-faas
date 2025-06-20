# Local FaaS Agent (radical faas)

A light weight agent that acts as an intermediare between a user and one of the following FaaS platfroms 

### Nuclio
- **Description**: high performance serverless platform for data processing
- **Use cases**: real time data processing, IoT applications, ML inference
- **Features**: GPU support, data binding, high throughput processing

### Knative
- **Description**: kubernetes based serverless platform with auto scaling
- **Use Cases**: cloud native applications, microservices, event driven architectures
- **Features**: auto scaling to zero, traffic splitting, canary deployments

## Architecture

The FaaS agent uses **adapters** to provide platform agnostic function deployment and management. The architecture consists of three main layers

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
│     Knative Adapter     │         Nuclio Adapter         │
│     (knative.py)        │         (nuclio.py)            │
└─────────────────────────┼────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼──────┐ ┌────────▼────────┐ ┌──────▼───────┐
│   Knative    │ │     Nuclio      │ │    Docker    │
│   Serving    │ │    Functions    │ │   Registry   │
└──────────────┘ └─────────────────┘ └──────────────┘
```


## Setup

```bash
git clone https://github.com/radical-collaboration/hydraa_faas
cd hydraa_faas/agent

# Create and activate virtual environment

python3 -m venv venv
source venv/bin/activate  

# Install dependencies
pip install -r requirements.txt
```

### Infrastructure setup

```bash
# Make scripts executable
chmod +x scripts/*.sh

# Install and start docker (if not already running)
./scripts/install_docker.sh

# Apply the group changes immediately (or you can log out the ec2 and back in)
newgrp docker

# Install and start minikube
./scripts/install_minikube.sh

# Install your preferred platform
./scripts/install_knative.sh
#or
./scripts/install_nuclio.sh
```

### Configure environment

```bash
# Configure docker to use minikubes daemon
eval $(minikube docker-env)

# Add current user to docker group
sudo usermod -aG docker $USER

# Apply the group changes immediately
newgrp docker

# Create .env file for knative (or you can just export them manually from the configuration examples below this)
cat > .env << EOF
FAAS_WORKFLOW=local
RADICAL_FAAS_PLATFORM=knative
KNATIVE_DOMAIN=$(minikube ip).nip.io
KNATIVE_NAMESPACE=default
FLASK_DEBUG=true
PORT=5000
EOF

# Load environment variables
export $(cat .env | xargs)
```

### Start agent

```bash
python3 app.py
```

The agent will be available at `http://localhost:5000`

### Test setup

```bash
# Health check
curl http://localhost:5000/health

# Deploy a test function
curl -X POST http://localhost:5000/deploy \
  -H "Content-Type: application/json" \
  -d '{"id": "hello-world", "handler": "def handle(req):\n    return \"Hello, World!\""}'

# Invoke the function
curl -X POST http://localhost:5000/invoke \
  -H "Content-Type: application/json" \
  -d '{"id": "hello-world", "payload": {}}'
```

## Configuration (environment variables)

#### Core configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `FAAS_WORKFLOW` | deployment workflow (`local` or `registry`) | `local` | no |
| `RADICAL_FAAS_PLATFORM` | target platform (`knative`, `nuclio`) | `knative` | no |
| `RADICAL_FAAS_REGISTRY` | container registry URL | - | Yes (if `registry` workflow) |
| `DOCKER_REPO_PREFIX` | docker repository prefix | `library` | no |
| `PORT` | flask server port | `5000` | no |
| `FLASK_DEBUG` | enable debug mode | `false` | no |

#### Knative configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `KNATIVE_DOMAIN` | kative domain for function URLs | `example.com` | Yes |
| `KNATIVE_NAMESPACE` | kubernetes namespace | `default` | no |
| `KNATIVE_TIMEOUT` | command timeout (seconds) | `60` | no |
| `KNATIVE_CLI_PATH` | path to kn CLI binary | `kn` | no |

#### Nuclio configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `NUCLIO_NAMESPACE` | nuclio namespace | `nuclio` | no |
| `NUCLIO_PLATFORM_KIND` | platform type (`kube` or `local`) | `kube` | no |
| `NUCLIO_TIMEOUT` | command timeout (seconds) | `60` | no |
| `NUCLIO_CLI_PATH` | path to nuctl CLI binary | `nuctl` | no |
| `NUCLIO_DASHBOARD_URL` | nuclio dashboard URL | - | no |

#### Docker configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DOCKER_CMD` | docker command name | `docker` | no |
| `DOCKER_TIMEOUT` | docker operation timeout | `300` | no |

### Configuration examples

#### Local (knative)
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

#### Local (nuclio)
```bash
export FAAS_WORKFLOW=local
export RADICAL_FAAS_PLATFORM=nuclio
export NUCLIO_NAMESPACE=nuclio
export NUCLIO_PLATFORM_KIND=kube
export FLASK_DEBUG=true
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

Deploy a Python function to the configured FaaS platform.

**Request Body:**
```json
{
  "id": "function-name",           // optional (will be generated if not providede)
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

List all deployed functions (if supported by platform)

**Response:**
```json
{
  "functions": [
    {
      "id": "function-name",
      "image": "library/function-name:latest",
      "ready": true,               // knative only
      "state": "ready",            // nuclio only
      "platform": "knative"
    }
  ]
}
```

**Status Codes:**
- `200`: Functions listed successfully
- `501`: Function listing not supported by platform
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

### Data processing functione

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

Errors i encountered while testing

#### Docker permission denied
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

# check platform logs (nuclio)
kubectl logs -n nuclio -l app=nuclio-controller --tail=50

# check function-specific logs
kubectl logs -n default -l serving.knative.dev/service=FUNCTION_NAME
```

#### Image pull errors
**Error**: `ImagePullBackOff` or somthing very close to this

**Solutions**:
- **Local**: ensure `eval $(minikube docker-env)` is executed
- **Registry**: verify registry credentials and image names
- **Check image exists**: `docker images | grep FUNCTION_NAME`

#### Connection timeouts
**Error**: timeout errors during function invocation

**Solutions**:
```bash
# increase timeout values (usually not the best fix but it helped once)
export KNATIVE_TIMEOUT=120
export NUCLIO_TIMEOUT=120

# check if services are ready
kubectl get ksvc  # knative
nuctl get functions  # nuclio
```

#### Port conflicts
**Error**: Port already in use

**Solution**:
```bash
# change agent port
export PORT=5001

# or kill existing processes (becareful as you can kill somthing important, i killed airdrop on my mac for example)
lsof -ti:5000 | xargs kill -9
```