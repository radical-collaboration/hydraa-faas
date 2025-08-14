#!/bin/bash

set -e

# configuration
MINIKUBE_CPUS=${MINIKUBE_CPUS:-2}
MINIKUBE_MEMORY=${MINIKUBE_MEMORY:-6g}
MINIKUBE_DISK=${MINIKUBE_DISK:-20g}
KNATIVE_SERVING_VERSION=${KNATIVE_SERVING_VERSION:-1.11.0}
KOURIER_VERSION=${KOURIER_VERSION:-1.11.0}
KNATIVE_FUNC_NODEPORT=${KNATIVE_FUNC_NODEPORT:-31080}

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

# install kn cli
install_kn() {
    command_exists kn && { info "kn cli already installed"; return; }

    info "installing kn cli"
    PLATFORM=$([[ "$OS" == "macos" ]] && echo "darwin" || echo "linux")

    KN_URL="https://github.com/knative/client/releases/latest/download/kn-${PLATFORM}-${ARCH}"

    wget -O kn "$KN_URL" || curl -L -o kn "$KN_URL"
    chmod +x kn && sudo mv kn /usr/local/bin/
    success "kn cli installed"
}

# install func cli
install_func() {
    command_exists func && { info "func cli already installed"; return; }

    info "installing func cli"
    PLATFORM=$([[ "$OS" == "macos" ]] && echo "darwin" || echo "linux")

    case $OS in
        "macos")
            brew tap knative/client
            brew install func
            ;;
        *)
            FUNC_URL=$(curl -s https://api.github.com/repos/knative/func/releases/latest | \
                       jq -r ".assets[] | select(.name | contains(\"func_${PLATFORM}_${ARCH}\")) | .browser_download_url" | head -1)

            [[ -z "$FUNC_URL" ]] && { error "could not find func download url"; exit 1; }

            wget -O func "$FUNC_URL" || curl -L -o func "$FUNC_URL"
            chmod +x func && sudo mv func /usr/local/bin/
            ;;
    esac
    success "func cli installed"
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

    local driver="docker"
    [[ "$OS" == "macos" ]] && ! docker info >/dev/null 2>&1 && driver="hyperkit"

    minikube start --driver=$driver --cpus=$MINIKUBE_CPUS --memory=$MINIKUBE_MEMORY \
        --disk-size=$MINIKUBE_DISK --container-runtime=docker --bootstrapper=kubeadm

    info "enabling metrics-server addon"
    minikube addons enable metrics-server
    success "minikube started"
}

# install knative serving
install_knative_serving() {
    info "installing knative serving"

    # install serving crds
    kubectl apply -f https://github.com/knative/serving/releases/download/knative-v${KNATIVE_SERVING_VERSION}/serving-crds.yaml >/dev/null 2>&1

    # install serving core
    kubectl apply -f https://github.com/knative/serving/releases/download/knative-v${KNATIVE_SERVING_VERSION}/serving-core.yaml >/dev/null 2>&1

    # install kourier networking layer
    kubectl apply -f https://github.com/knative/net-kourier/releases/download/knative-v${KOURIER_VERSION}/kourier.yaml >/dev/null 2>&1

    # configure knative to use kourier
    kubectl patch configmap/config-network \
        --namespace knative-serving \
        --type merge \
        --patch '{"data":{"ingress-class":"kourier.ingress.networking.knative.dev"}}' >/dev/null 2>&1

    # configure domain to use nip.io for minikube
    local minikube_ip=$(minikube ip)
    kubectl patch configmap/config-domain \
        --namespace knative-serving \
        --type merge \
        --patch "{\"data\":{\"${minikube_ip}.nip.io\":\"\"}}" >/dev/null 2>&1

    info "waiting for knative serving to be ready"
    kubectl wait --for=condition=available --timeout=300s deployment/activator -n knative-serving >/dev/null 2>&1
    kubectl wait --for=condition=available --timeout=300s deployment/autoscaler -n knative-serving >/dev/null 2>&1
    kubectl wait --for=condition=available --timeout=300s deployment/controller -n knative-serving >/dev/null 2>&1
    kubectl wait --for=condition=available --timeout=300s deployment/webhook -n knative-serving >/dev/null 2>&1
    kubectl wait --for=condition=available --timeout=300s deployment/3scale-kourier-gateway -n kourier-system >/dev/null 2>&1

    success "knative serving installed"
}



# setup kourier nodeport for external access
setup_kourier_nodeport() {
    info "setting up kourier nodeport for external access"

    kubectl patch service kourier \
        --namespace kourier-system \
        --type merge \
        --patch "{\"spec\":{\"type\":\"NodePort\",\"ports\":[{\"name\":\"http2\",\"nodePort\":${KNATIVE_FUNC_NODEPORT},\"port\":80,\"protocol\":\"TCP\",\"targetPort\":8080}]}}" >/dev/null 2>&1

    success "kourier nodeport configured on port $KNATIVE_FUNC_NODEPORT"
}

# create environment file
create_environment() {
    local minikube_ip=$(minikube ip)

    cat > knative-env.sh << EOF
#!/bin/bash
# knative environment variables for docker hub deployment (serving only)
export KNATIVE_NAMESPACE=default
export MINIKUBE_IP=$minikube_ip
export KNATIVE_DOMAIN=${minikube_ip}.nip.io
export KOURIER_ENDPOINT=http://$minikube_ip:$KNATIVE_FUNC_NODEPORT

echo "knative environment loaded"
echo "kourier endpoint: \$KOURIER_ENDPOINT"
echo "domain: \$KNATIVE_DOMAIN"
echo "note: use your docker hub registry (docker.io/username) for deployments"
EOF

    info "environment saved to knative-env.sh"
}



# main execution
main() {
    info "starting knative setup for docker hub"

    detect_system
    [[ "$OS" == "macos" ]] && install_homebrew
    install_deps
    install_docker
    apply_docker_group
    install_kubectl
    install_helm
    install_minikube
    install_kn
    install_func

    wait_for_docker
    start_minikube
    install_knative_serving
    setup_kourier_nodeport
    create_environment

    success "setup complete"
    echo ""
    info "next steps:"
    info "1. docker login"
    info "2. source knative-env.sh"
    if [[ "$OS" != "macos" ]]; then
        info "3. newgrp docker (linux only)"
        info "4. func create -l <language> <function-name>"
        info "5. func deploy --registry docker.io/username"
    else
        info "3. func create -l <language> <function-name>"
        info "4. func deploy --registry docker.io/username"
    fi
    echo ""
    info "example deployments:"
    info "# using func cli:"
    info "func create -l go hello-go && cd hello-go"
    info "func deploy --registry docker.io/username"
    echo ""
    info "# using kn cli:"
    info "kn service create hello --image gcr.io/knative-samples/helloworld-go"
    echo ""
    info "# using kubectl:"
    info "kubectl apply -f <your-function>.yaml"
    echo ""
    info "access functions at: http://$(minikube ip):$KNATIVE_FUNC_NODEPORT"
    info "use 'Host: <service-name>.default.$(minikube ip).nip.io' header for routing"

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