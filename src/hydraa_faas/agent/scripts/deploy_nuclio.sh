#!/bin/bash

set -e

# configuration
MINIKUBE_CPUS=${MINIKUBE_CPUS:-4}
MINIKUBE_MEMORY=${MINIKUBE_MEMORY:-6g}
MINIKUBE_DISK=${MINIKUBE_DISK:-30g}
NUCLIO_DASHBOARD_NODEPORT=${NUCLIO_DASHBOARD_NODEPORT:-30070}

# logging
log() { echo "[$1] $2"; }
info() { log "info" "$1"; }
success() { log "success" "$1"; }
warning() { log "warning" "$1"; }
error() { log "error" "$1"; }

# utilities
command_exists() { command -v "$1" >/dev/null 2>&1; }

# system detection
detect_system() {
    case "$OSTYPE" in
        linux*)
            if [ -f /etc/os-release ]; then
                . /etc/os-release
                OS=$ID; OS_VERSION=$VERSION_ID
            else
                error "cannot detect linux distribution"; exit 1
            fi
            ;;
        darwin*) OS="macos"; OS_VERSION=$(sw_vers -productVersion) ;;
        *) error "unsupported os: $OSTYPE"; exit 1 ;;
    esac

    case $(uname -m) in
        x86_64|amd64) ARCH="amd64" ;;
        arm64|aarch64) ARCH="arm64" ;;
        *) error "unsupported architecture: $(uname -m)"; exit 1 ;;
    esac

    info "detected: $OS $OS_VERSION ($ARCH)"
}

# install homebrew on macos
install_homebrew() {
    [[ "$OS" != "macos" ]] && return
    command_exists brew && { info "homebrew already installed"; return; }

    info "installing homebrew"
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

    if [[ "$ARCH" == "arm64" ]]; then
        eval "$(/opt/homebrew/bin/brew shellenv)"
    else
        eval "$(/usr/local/bin/brew shellenv)"
    fi
    success "homebrew installed"
}

# install docker
install_docker() {
    command_exists docker && { info "docker already installed: $(docker --version)"; return; }

    info "installing docker"
    case $OS in
        "amzn"|"centos"|"rhel"|"rocky"|"almalinux")
            sudo yum update -y
            sudo yum install -y docker --allowerasing
            sudo systemctl start docker && sudo systemctl enable docker
            sudo usermod -aG docker $USER
            ;;
        "ubuntu")
            sudo apt-get update -y && sudo apt-get install -y docker.io
            sudo systemctl start docker && sudo systemctl enable docker
            sudo usermod -aG docker $USER
            ;;
        "macos")
            brew install --cask docker
            warning "start docker desktop and re-run this script"
            exit 0
            ;;
        *) error "unsupported os for docker: $OS"; exit 1 ;;
    esac
    success "docker installed"
}

# apply docker group membership
apply_docker_group() {
    [[ "$OS" == "macos" ]] && return
    groups | grep -q docker && return

    info "applying docker group membership"
    if command_exists sg; then
        info "re-executing with docker privileges"
        exec sg docker "$0 $*"
    else
        warning "run 'newgrp docker' and re-run this script"
        exit 0
    fi
}

# install system dependencies
install_deps() {
    info "installing system dependencies"
    case $OS in
        "amzn"|"centos"|"rhel"|"rocky"|"almalinux")
            sudo yum install -y git curl wget jq tar gzip --allowerasing ;;
        "ubuntu")
            sudo apt-get install -y git curl wget jq tar gzip ;;
        "macos")
            brew install git curl wget jq ;;
    esac
    success "dependencies installed"
}

# install kubectl
install_kubectl() {
    command_exists kubectl && { info "kubectl already installed"; return; }

    info "installing kubectl"
    case $OS in
        "macos") brew install kubectl ;;
        *)
            KUBECTL_VERSION=$(curl -L -s https://dl.k8s.io/release/stable.txt)
            curl -LO "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${ARCH}/kubectl"
            sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
            rm kubectl
            ;;
    esac
    success "kubectl installed"
}

# install helm
install_helm() {
    command_exists helm && { info "helm already installed"; return; }

    info "installing helm"
    case $OS in
        "macos") brew install helm ;;
        *) curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash ;;
    esac
    success "helm installed"
}

# install minikube
install_minikube() {
    command_exists minikube && { info "minikube already installed"; return; }

    info "installing minikube"
    case $OS in
        "macos") brew install minikube ;;
        *)
            if [[ "$ARCH" == "arm64" ]]; then
                curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-arm64
                sudo install minikube-linux-arm64 /usr/local/bin/minikube
                rm minikube-linux-arm64
            else
                curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
                sudo install minikube-linux-amd64 /usr/local/bin/minikube
                rm minikube-linux-amd64
            fi
            ;;
    esac
    success "minikube installed"
}

# install nuctl
install_nuctl() {
    command_exists nuctl && { info "nuctl already installed"; return; }

    info "installing nuctl"
    PLATFORM=$([[ "$OS" == "macos" ]] && echo "darwin" || echo "linux")

    NUCTL_URL=$(curl -s https://api.github.com/repos/nuclio/nuclio/releases/latest | \
                jq -r ".assets[] | select(.name | contains(\"nuctl\") and contains(\"$PLATFORM\") and contains(\"$ARCH\")) | .browser_download_url" | head -1)

    [[ -z "$NUCTL_URL" ]] && NUCTL_URL=$(curl -s https://api.github.com/repos/nuclio/nuclio/releases/latest | \
                jq -r ".assets[] | select(.name | contains(\"nuctl\") and contains(\"$PLATFORM\")) | .browser_download_url" | head -1)

    [[ -z "$NUCTL_URL" ]] && { error "could not find nuctl download url"; exit 1; }

    wget -O nuctl "$NUCTL_URL" || curl -L -o nuctl "$NUCTL_URL"
    chmod +x nuctl && sudo mv nuctl /usr/local/bin/
    success "nuctl installed"
}

# wait for docker to be ready
wait_for_docker() {
    info "waiting for docker"
    local attempt=1
    while ! docker info >/dev/null 2>&1; do
        [[ $attempt -ge 30 ]] && { error "docker not responding after 30 attempts"; exit 1; }
        info "waiting for docker (attempt $attempt/30)"
        sleep 2; ((attempt++))
    done
    success "docker ready"
}

# start minikube
start_minikube() {
    info "starting minikube"
    minikube status >/dev/null 2>&1 && { info "minikube already running"; return; }

    minikube start \
        --kubernetes-version v1.28.3 \
        --cpus $MINIKUBE_CPUS \
        --memory $MINIKUBE_MEMORY \
        --disk-size $MINIKUBE_DISK \
        --driver docker \
        --addons metrics-server

    kubectl wait --for=condition=Ready nodes --all --timeout=300s >/dev/null 2>&1
    success "minikube started"
}

# install nuclio
install_nuclio() {
    info "installing nuclio"
    helm repo add nuclio https://nuclio.github.io/nuclio/charts >/dev/null 2>&1
    helm repo update >/dev/null 2>&1
    kubectl create namespace nuclio --dry-run=client -o yaml | kubectl apply -f - >/dev/null 2>&1

    helm upgrade --install nuclio nuclio/nuclio --namespace nuclio \
        --set dashboard.nodePort=$NUCLIO_DASHBOARD_NODEPORT >/dev/null 2>&1

    info "waiting for nuclio to be ready"
    kubectl wait --for=condition=available --timeout=300s deployment/nuclio-dashboard -n nuclio >/dev/null 2>&1
    kubectl wait --for=condition=available --timeout=300s deployment/nuclio-controller -n nuclio >/dev/null 2>&1
    success "nuclio installed"
}

# main execution
main() {
    info "starting nuclio setup"

    detect_system
    [[ "$OS" == "macos" ]] && install_homebrew
    install_deps
    install_docker
    apply_docker_group
    install_kubectl
    install_helm
    install_minikube
    install_nuctl

    wait_for_docker
    start_minikube
    install_nuclio

    success "setup complete!"
    echo ""
    info "next steps:"
    info "1. eval \$(minikube docker-env)"
    info "2. docker login (if using registry)"
    if [[ "$OS" != "macos" ]]; then
        info "3. newgrp docker (linux only)"
    fi
    info "4. nuctl deploy <function-name> --file function.yaml --platform kube --namespace nuclio"
    echo ""
    info "dashboard: http://$(minikube ip):$NUCLIO_DASHBOARD_NODEPORT"

    [[ "$OS" != "macos" ]] && ! groups | grep -q docker && \
        warning "note: you may need to logout/login for docker group membership"
}

# check if running as root
if [[ $EUID -eq 0 ]]; then
    warning "running as root not recommended"
    read -p "continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

main "$@"