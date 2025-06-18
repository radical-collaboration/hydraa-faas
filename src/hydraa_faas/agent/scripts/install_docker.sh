#!/bin/bash
set -euo pipefail
echo "starting docker installation"



case "$(uname)" in
    "Darwin")
        echo "installing/updating docker via homebrew"
        brew install --cask docker
        
        echo "starting docker desktop"
        open --background /Applications/Docker.app
        echo "waiting for docker daemon"
        until docker info >/dev/null 2>&1; do sleep 2; done
        ;;
    "Linux")
        echo "installing docker for RHEL/CentOS/Amazon Linux"
        sudo yum update -y
        sudo yum install -y docker
        ;;
esac

if [[ "$(uname)" == "Linux" ]]; then
    echo "enabling and starting docker"
    sudo systemctl enable docker
    sudo systemctl start docker
    
    echo "adding current user to the docker group"
    sudo usermod -aG docker "$USER"
    echo "IMPORTANT: log out and log back in for group changes to take full effect"
fi

echo "docker installation and boot up is complete"
