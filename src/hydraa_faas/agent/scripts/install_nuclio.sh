#!/bin/bash
#
# install_nuclio.sh: Deploys the Nuclio platform.
#
# Prerequisites:
#   - A running Minikube cluster. Run 'install_minikube.sh' first.
#

set -euo pipefail

# --- Logging and Utility Functions ---
LOG_OUT="$HOME/nuclio_install.out"
LOG_ERR="$HOME/nuclio_install.err"
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

get_os() {
    case "$(uname -s)" in
        Darwin) echo "darwin" ;;
        Linux) echo "linux" ;;
        *) log_error "Unsupported operating system: $(uname -s)" ;;
    esac
}

# --- Main Installation Logic ---
main() {
    # Dependency Check
    if ! minikube status &>/dev/null; then
       log_error "Minikube is not running. Please run the install_minikube.sh script first."
    fi
    log_message "Minikube dependency met."

    # Install nuctl
    if ! command -v nuctl &>/dev/null; then
        log_message "Installing nuctl CLI..."
        local OS
        OS=$(get_os)
        # Fetch the latest version tag from GitHub API
        local LATEST_VERSION
        LATEST_VERSION=$(curl -s "https://api.github.com/repos/nuclio/nuclio/releases/latest" | grep -Po '"tag_name": "\K.*?(?=")')
        if [ -z "$LATEST_VERSION" ]; then
            log_error "Could not determine latest Nuclio version."
        fi
        log_message "Latest Nuclio version is $LATEST_VERSION"
        local NUCTL_URL="https://github.com/nuclio/nuclio/releases/download/${LATEST_VERSION}/nuctl-${LATEST_VERSION}-${OS}-amd64"
        
        run curl -Lo nuctl "$NUCTL_URL"
        run chmod +x nuctl
        run sudo mv nuctl /usr/local/bin/
    else
        log_message "nuctl is already installed."
    fi

    # Deploy Nuclio using Helm
    log_message "Checking if Nuclio is already installed..."
    if ! helm status nuclio -n nuclio &>/dev/null; then
        log_message "Adding Nuclio Helm repo..."
        run helm repo add nuclio https://nuclio.github.io/nuclio/charts
        run helm repo update
        
        log_message "Deploying Nuclio via Helm..."
        # Create namespace if it doesn't exist
        kubectl create namespace nuclio --dry-run=client -o yaml | kubectl apply -f -
        run helm install nuclio nuclio/nuclio --namespace nuclio --wait
    else
        log_message "Nuclio is already deployed."
    fi

    log_message "Waiting for Nuclio dashboard to be ready..."
    run kubectl rollout status -n nuclio deployment/nuclio-dashboard --timeout=5m
    
    log_message "Port-forwarding Nuclio dashboard to localhost:8070..."
    pkill -f "port-forward -n nuclio svc/nuclio-dashboard" || true
    kubectl -n nuclio port-forward svc/nuclio-dashboard 8070:8070 >/dev/null 2>&1 &
    
    log_message "--- Nuclio installation complete! ---"
    log_message "Nuclio Dashboard is available at http://localhost:8070"
}

main
