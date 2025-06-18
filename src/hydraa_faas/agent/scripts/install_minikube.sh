#!/bin/bash
set -euo pipefail
if ! docker info &>/dev/null; then
   echo "docker is not running, start docker an rerun script again" >&2
   exit 1
fi

echo "installing the following (kubectl, helm, minikube)"
if [ "$(uname)" == "Darwin" ]; then #mac
    brew install kubectl helm minikube
else #linux
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
    echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
    sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl

    sudo dnf install git-all # needs to be installed for helm script

    curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
    chmod 700 get_helm.sh
    ./get_helm.sh
    rm get_helm.sh

    curl -LO https://github.com/kubernetes/minikube/releases/latest/download/minikube-linux-amd64
    sudo install minikube-linux-amd64 /usr/local/bin/minikube && rm minikube-linux-amd64
fi

echo "deleting old minikube profiles"
if [ "$(uname)" == "Linux" ]; then
    sudo -E minikube delete --all || true # use || true to prevent failure if no profile exists
else
    minikube delete --all || true
fi

echo "starting minikube"
minikube start
echo "minikube installation complete"
