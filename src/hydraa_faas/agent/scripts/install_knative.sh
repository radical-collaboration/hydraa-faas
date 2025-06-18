#!/bin/bash
#
# install_knative.sh: Deploys the Knative platform.
#
# Prerequisites:
#   - A running Minikube cluster. Run 'install_minikube.sh' first.
#

set -euo pipefail

# --- Configuration ---
KNATIVE_SERVING_VERSION="v1.14.0"
KNATIVE_NET_KOURIER_VERSION="v1.14.0"
CERT_MANAGER_VERSION="v1.15.1"

# --- Logging and Utility Functions ---
LOG_OUT="$HOME/knative_install.out"
LOG_ERR="$HOME/knative_install.err"
>"$LOG_OUT"
>"$LOG_ERR"

log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - INFO: $1" | tee -a "$LOG_OUT"
}

log_error() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - ERROR: $1" | tee -a "$LOG_OUT" | tee -a "$LOG_ERR" >&2
    exit 1
}

run() {
    log_message "Executing: $*"
    if ! "$@" >>"$LOG_OUT" 2>>"$LOG_ERR"; then
        log_error "Command failed: '$*'. Check '$LOG_ERR' for details."
    fi
}

# --- Main Installation Logic ---
main() {
    # Dependency Check
    if ! minikube status &>/dev/null; then
       log_error "Minikube is not running. Please run the install_minikube.sh script first."
    fi
    log_message "Minikube dependency met."
    
    log_message "Deploying Knative Serving..."

    log_message "1/4: Installing cert-manager..."
    run kubectl apply -f "https://github.com/jetstack/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    log_message "Waiting for cert-manager deployments to be ready..."
    run kubectl wait --for=condition=Available deployment --all -n cert-manager --timeout=5m

    log_message "2/4: Installing Knative Serving CRDs..."
    run kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_SERVING_VERSION}/serving-crds.yaml"
    
    log_message "3/4: Installing Knative Serving core..."
    run kubectl apply -f "https://github.com/knative/serving/releases/download/knative-${KNATIVE_SERVING_VERSION}/serving-core.yaml"
    
    log_message "4/4: Installing Kourier networking layer..."
    run kubectl apply -f "https://github.com/knative/net-kourier/releases/download/knative-${KNATIVE_NET_KOURIER_VERSION}/kourier.yaml"
    
    log_message "Configuring Knative to use Kourier..."
    run kubectl patch configmap/config-network -n knative-serving --type merge --patch '{"data":{"ingress.class":"kourier.ingress.networking.knative.dev"}}'

    log_message "Waiting for all Knative and Kourier pods to be ready..."
    run kubectl wait --for=condition=Ready pod --all -n knative-serving --timeout=5m
    run kubectl wait --for=condition=Ready pod --all -n kourier-system --timeout=5m
    
    log_message "--- Knative installation complete. ---"
}

main
