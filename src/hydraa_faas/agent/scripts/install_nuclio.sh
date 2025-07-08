#!/bin/bash
set -euo pipefail

# logging setup
LOG_OUT="$HOME/nuclio_install.out"
LOG_ERR="$HOME/nuclio_install.err"
>"$LOG_OUT" && >"$LOG_ERR"

log() { echo "$(date '+%H:%M:%S') - $1" | tee -a "$LOG_OUT"; }
error() { echo "$(date '+%H:%M:%S') - ERROR: $1" | tee -a "$LOG_ERR" >&2; exit 1; }
run() { log "Running: $*" && "$@" >>"$LOG_OUT" 2>>"$LOG_ERR" || error "Failed: $*"; }

get_os() {
    case "$(uname -s)" in
        Darwin) echo "darwin" ;;
        Linux) echo "linux" ;;
    esac
}

install_nuctl() {
    local os=$(get_os)
    local version=$(curl -s "https://api.github.com/repos/nuclio/nuclio/releases/latest" | grep -Po '"tag_name": "\K.*?(?=")')
    [[ -z "$version" ]] && error "could not determine latest nuclio version"
    
    log "installing nuctl $version for $os..."
    local url="https://github.com/nuclio/nuclio/releases/download/${version}/nuctl-${version}-${os}-amd64"
    
    run curl -Lo nuctl "$url"
    run chmod +x nuctl
    run sudo mv nuctl /usr/local/bin/
}

main() {
    # check dependencies
    minikube status &>/dev/null || error "minikube not running, run install_minikube.sh first"
    command -v helm &>/dev/null || error "helm not found, run install_minikube.sh first"
    log "dependencies met"

    # IMPORTANT: Set up minikube docker environment
    log "configuring minikube docker environment"
    eval $(minikube docker-env)

    # install nuctl CLI
    command -v nuctl &>/dev/null && log "nuctl already installed" || install_nuctl

    # check if already installed
    if helm status nuclio -n nuclio &>/dev/null; then
        log "nuclio already deployed, reinstalling for fresh start"
        run helm uninstall nuclio -n nuclio
        sleep 10
    fi

    log "installing nuclio via helm"
    run helm repo add nuclio https://nuclio.github.io/nuclio/charts
    run helm repo update

    # create namespace
    log "creating nuclio namespace"
    if ! kubectl get namespace nuclio &>/dev/null; then
        run kubectl create namespace nuclio
    else
        log "nuclio namespace already exists"
    fi

    run helm install nuclio nuclio/nuclio --namespace nuclio --wait --timeout=300s

    # wait for dashboard
    log "waiting for nuclio dashboard"
    run kubectl rollout status -n nuclio deployment/nuclio-dashboard --timeout=300s

    # setup port forwarding
    log "setting up port forwarding"
    pkill -f "port-forward.*nuclio-dashboard" || true
    kubectl -n nuclio port-forward svc/nuclio-dashboard 8070:8070 >/dev/null 2>&1 &

    log "nuclio installation complete"
    log "dashboard - http://localhost:8070"
    log "IMPORTANT: Remember to run 'eval \$(minikube docker-env)' before deploying functions"
    log "test with - nuctl get functions --namespace nuclio"
}

main