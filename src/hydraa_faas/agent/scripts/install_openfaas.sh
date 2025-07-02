#!/bin/bash
set -euo pipefail
# To verify that openfaas has started, run:

#   kubectl -n openfaas get deployments -l "release=openfaas, app=openfaas"

# To retrieve the admin password, run:

#   echo $(kubectl -n openfaas get secret basic-auth -o jsonpath="{.data.basic-auth-password}" | base64 --decode)
# =======================================================================
# = OpenFaaS has been installed.                                        =
# =======================================================================

# # Get the faas-cli
# curl -SLsf https://cli.openfaas.com | sudo sh

# # Forward the gateway to your machine
# kubectl rollout status -n openfaas deploy/gateway
# kubectl port-forward -n openfaas svc/gateway 8080:8080 &

# # If basic auth is enabled, you can now log into your gateway:
# PASSWORD=$(kubectl get secret -n openfaas basic-auth -o jsonpath="{.data.basic-auth-password}" | base64 --decode; echo)
# echo -n $PASSWORD | faas-cli login --username admin --password-stdin

# faas-cli store deploy figlet
# faas-cli list

# # For Raspberry Pi
# faas-cli store list \
# #  --platform armhf

# faas-cli store deploy figlet \
# #  --platform armhf

# # Find out more at:
# # https://github.com/openfaas/faas

main() {
    # dependency check
    if ! minikube status &>/dev/null; then
       echo "minikube is not running, please run the install_minikube.sh script first"
       exit 1 # Added exit here as it's a critical dependency
    fi
    
    # install arkade
    if ! command -v arkade &>/dev/null; then
        echo "installing arkade"
        curl -sLS https://get.arkade.dev | sudo sh
    else
        echo "arkade is already installed"
    fi

    # install openfaas cli & kubectl

    curl -sSL https://cli.openfaas.com | sudo -E sh
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    
    curl -L -s https://dl.k8s.io/release/stable.txt | xargs -I {} curl -LO "https://dl.k8s.io/release/{}/bin/linux/amd64/kubectl.sha256"
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl kubectl.sha256

    # install openfaas itself
    if ! kubectl get deployment -n openfaas gateway &>/dev/null; then
        echo "deploying OpenFaaS with arkade"
        arkade install openfaas
    else
        echo "openfaas is already deployed"
    fi
    
    echo "rolling out gateway"
    kubectl rollout status -n openfaas deploy/gateway --timeout=5m

    # configure docker environment for minikube
    echo "configuring docker environment to use minikubes daemon"
    eval $(minikube docker-env)
    if [ $? -ne 0 ]; then
        echo "error: failed to configure docker environment for minikube"
        exit 1
    fi
    echo "docker environment configured"

    # login to docker hub via minikubes docker daemon
    echo "enter your docker hub username:"
    read DOCKER_USERNAME
    echo -n "please enter your docker hub password:"
    read -s DOCKER_PASSWORD
    echo ""
    echo "logging docker daemon (in minikube) into docker hub"
    echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
    if [ $? -ne 0 ]; then
        echo "error: docker login failed"
        exit 1
    fi
    echo "docker daemon in minikube logged into docker hub"

    echo "starting port-forward in background"
    kubectl port-forward -n openfaas svc/gateway 8080:8080 &
    PF_PID=$!

    echo "waiting for local port 8080 to come up"
    until bash -c '</dev/tcp/127.0.0.1/8080' 2>/dev/null; do
        sleep 1
    done

    echo "logging into openfaas CLI"
    PASSWORD=$(kubectl get secret -n openfaas basic-auth \
    -o jsonpath="{.data.basic-auth-password}" | base64 --decode)
    echo -n "$PASSWORD" | faas-cli login \
    --username admin --password-stdin \
    --gateway http://127.0.0.1:8080

    echo "setting environment variables for app.py in current shell session"
    

    export OPENFAAS_USERNAME="admin"
    export OPENFAAS_PASSWORD="$PASSWORD"
    export RADICAL_FAAS_REGISTRY="localhost:5000"
    export RADICAL_FAAS_PLATFORM="openfaas"
    
    export OPENFAAS_GATEWAY="http://$(minikube ip):31112"

    echo "openfaas is ready (port-forward PID=$PF_PID), run app in this shell"
}

main