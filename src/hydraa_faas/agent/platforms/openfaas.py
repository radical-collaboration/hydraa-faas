import os
import logging
import requests
from .base import FaasPlatform

log = logging.getLogger(__name__)

# read from env
GATEWAY  = os.environ.get("OPENFAAS_GATEWAY",  "http://127.0.0.1:8080")
USERNAME = os.environ.get("OPENFAAS_USERNAME")
PASSWORD = os.environ.get("OPENFAAS_PASSWORD")

# If both are set, requests will send HTTP Basic Auth.
_AUTH = (USERNAME, PASSWORD) if USERNAME and PASSWORD else None

class OpenFaasPlatform(FaasPlatform):
    def deploy(self, func_id: str, image: str) -> None:
        url = f"{GATEWAY}/system/functions"
        payload = {
            "service":    func_id,
            "image":      image,
            "imagePullPolicy": "IfNotPresent" 
        }
        log.info(f"Deploying '{func_id}' → {image} @ {url}")
        resp = requests.post(url, json=payload, auth=_AUTH)
        
        if resp.status_code not in [200, 201, 202]:
            log.error(f"Deployment failed with status {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        log.info(f"Deployment request for '{func_id}' accepted.")

    def invoke(self, func_id: str, payload: bytes = None) -> bytes:
        url = f"{GATEWAY}/function/{func_id}"
        log.info(f"Invoking '{func_id}' @ {url}")
        resp = requests.post(url, data=payload or b"", auth=_AUTH)
        resp.raise_for_status()
        return resp.content
