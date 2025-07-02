# FaaS Agent

A lightweight platform agnostic agent that provides a unifies REST API for deploying and managing serverless functions on different FaaS platforms.

This agent is designed to be configured once to a target FaaS platform (like knative or nuclio) running on a specific kubernetes environment.

## Workflow

This agent follows a two phase workflow
1. setup: a user first prepares a kubernetes environment (e.g. minikube, aws eks) and deploys the desired faas platform onto it (e.g. nuclio)
2. operation: the user configures the agent through the `config.yaml` to point to the prepared environment and then starts the agent. The agent then provides an api to manage functions on that environment.

## Quick Start

### Prerequisites
* `git`
* `python 3.9`+  and `pip`
* a running docker daemon

### Environment Setup

Scripts are provided in the `scripts/` directory to help set up a complete local development environment using minikube.

```bash
# give the scripts execute permissions
chmod +x scripts/*.sh

# install docker (if not already installed)
./scripts/install_docker.sh

# install minikube, kubectl, helm, and start a cluster
./scripts/install_minikube.sh

# choose one desired platform to deploy on minikube
./scripts/install_knative.sh
# or
./scripts/install_nuclio.sh

docker login
sudo usermod -aG docker $USER
newgrp docker
eval $(minikube docker-env)
```
for instructions on setting up other environments like Kind or AWS EKS see the detailed tutorial in `examples/`

### Configure the Agent

Edit the `agent/config.yaml` file to match the environment you just set up. This is the only file you need to modify. You must specify the `platform`, `kubernetes_target`, and platform specific settings like `container_registry` etc.

example `config.yaml` for a local minikube setup with knative:

```yaml
platform: "knative" # or "nuclio"

kubernetes_target:
  type: "minikube"
  context_name: "minikube"

platforms:
  knative:
    container_registry: "docker.io/your-docker-hub-username"
    namespace: "default"
```

### Run the agent

From the root of the project directory 

```bash
# create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate

# install dependencies
pip install -r requirements.txt

# start the agent using uvicorn
uvicorn agent.app:app --reload
```
the agent API will be available at http://localhost:8000.

## API Reference
The agent provides the following endpoints:
* `GET /health`: health check
* `POST /deploy`: deploys a function from a zipped source file
* `POST /invoke`: invokes a deployed function
* `POST /list`: lists all deployed functions
* `POST /delete`: deletes a deployed function

for detailed request/response formats and runnable examples see `examples/`