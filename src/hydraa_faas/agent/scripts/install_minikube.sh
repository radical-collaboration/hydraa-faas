#!/bin/bash
set -euo pipefail

# check docker first
if ! docker info &>/dev/null; then
   echo "docker is not running, start docker and rerun script again" >&2
   exit 1
fi

echo "installing kubectl, helm, and minikube..."

if [ "$(uname)" == "Darwin" ]; then # macos
    echo "detected macos"
    
    # check if homebrew is installed
    if ! command -v brew &>/dev/null; then
        echo "Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
    fi
    
    # install tools via homebrew
    brew install kubectl helm minikube
    
else # linux
    echo "detected linux"
    
    # install kubectl
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl kubectl.sha256

    # install git for helm (if needed)
    if command -v dnf &>/dev/null; then
        sudo dnf install -y git-all
    elif command -v yum &>/dev/null; then
        sudo yum install -y git
    fi

    # install helm
    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
    chmod 700 get_helm.sh
    ./get_helm.sh
    rm get_helm.sh

    # install minikube
    curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
fi

echo "deleting old minikube profiles"
if [ "$(uname)" == "Linux" ]; then
    sudo -E minikube delete --all || true
else
    minikube delete --all || true
fi

echo "starting minikube"
minikube start

echo "minikube installation complete"
