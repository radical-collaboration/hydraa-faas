#!/bin/bash
set -euo pipefail

# versions
KNATIVE_VERSION="v1.14.0"
KOURIER_VERSION="v1.14.0"
CERT_MANAGER_VERSION="v1.15.1"

# logging setup
LOG_OUT="$HOME/knative_install.out"
LOG_ERR="$HOME/knative_install.err"
>"$LOG_OUT" && >"$LOG_ERR"

log() { echo "$(date '+%H:%M:%S') - $1" | tee -a "$LOG_OUT"; }
error() { echo "$(date '+%H:%M:%S') - ERROR: $1" | tee -a "$LOG_ERR" >&2; exit 1; }
run() { log "Running: $*" && "$@" >>"$LOG_OUT" 2>>"$LOG_ERR" || error "Failed: $*"; }

install_kn_cli() {
    if command -v kn &>/dev/null; then
        log "kn CLI already installed"
        return 0
    fi
    
    log "installing kn CLI"
    
    if [[ "$OSTYPE" == "darwin"* ]]; then #mac
        if command -v brew &>/dev/null; then
            run brew install knative/client/kn
        else
            log "homebrew not found, downloading kn CLI directly"
            run curl -L "https://github.com/knative/client/releases/download/knative-v1.12.0/kn-darwin-amd64" -o /tmp/kn
            run chmod +x /tmp/kn
            run sudo mv /tmp/kn /usr/local/bin/
        fi
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then #linux
        log "downloading kn CLI for Linux"
        run curl -L "https://github.com/knative/client/releases/download/knative-v1.12.0/kn-linux-amd64" -o /tmp/kn
        run chmod +x /tmp/kn
        run sudo mv /tmp/kn /usr/local/bin/
    fi
    
    # verify installation
    if command -v kn &>/dev/null; then
        log "kn CLI installed successfully"
        log "kn version: $(kn version 2>/dev/null | head -1 || echo 'installed')"
    else
        error "kn CLI installation failed"
    fi
}

main() {
    # check dependencies
    minikube status &>/dev/null || error "minikube not running, run install_minikube.sh first"
    log "minikube dependency met"
    
    # install cert-manager
    log "installing cert-manager"
    run kubectl apply -f "https://github.com/jetstack/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    run kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=300s
    
    # install knative serving
    log "installing knative serving CRDs and core"
    run kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_VERSION}/serving-crds.yaml"
    run kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_VERSION}/serving-core.yaml"
    
    # install kourier networking
    log "installing kourier networking"
    run kubectl apply -f "https://github.com/knative/net-kourier/releases/download/knative-${KOURIER_VERSION}/kourier.yaml"
    
    # configure networking
    log "configuring knative networking"
    run kubectl patch configmap/config-network -n knative-serving --type merge --patch '{"data":{"ingress.class":"kourier.ingress.networking.knative.dev"}}'
    
    # configure domain
    MINIKUBE_IP=$(minikube ip)
    run kubectl patch configmap/config-domain -n knative-serving --type merge --patch "{\"data\":{\"${MINIKUBE_IP}.nip.io\":\"\"}}"
    
    # wait for ready
    log "waiting for pods to be ready"
    run kubectl wait --for=condition=Ready pod --all -n knative-serving --timeout=300s
    run kubectl wait --for=condition=Ready pod --all -n kourier-system --timeout=300s
    
    # install kn CLI
    install_kn_cli
    
    log "knative installation complete"
    log "domain configured - ${MINIKUBE_IP}.nip.io"
    log "test with -  kn service list"
}

main