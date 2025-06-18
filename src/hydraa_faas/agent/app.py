#!/usr/bin/env python3
import os
import uuid
import logging
import shutil

from flask import Flask, request, jsonify
from jinja2 import Template, TemplateError

from utils.docker_utils import build_image
from platforms.base import FaasPlatform
from platforms.openfaas import OpenFaasPlatform

# —— Configuration via env vars ——  
REGISTRY_URL = os.environ.get("RADICAL_FAAS_REGISTRY", "localhost:5000")
PLATFORM    = os.environ.get("RADICAL_FAAS_PLATFORM", "openfaas").lower()
OPENFAAS    = os.environ.get("OPENFAAS_GATEWAY", "http://127.0.0.1:31112")

# —— Logging setup ——  
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# —— Choose adapter ——  
if PLATFORM == "openfaas":
    adapter: FaasPlatform = OpenFaasPlatform()
else:
    raise NotImplementedError(f"Platform '{PLATFORM}' is not supported.")

# —— Prepare functions dir & load Dockerfile template ——  
try:
    BASE_DIR = os.getenv(
        "RADICAL_FAAS_FUNCTIONS_DIR",
        os.path.join(os.getcwd(), "functions")
    )
    os.makedirs(BASE_DIR, exist_ok=True)

    with open("templates/Dockerfile.j2") as f:
        dockerfile_tpl = Template(f.read())

except FileNotFoundError:
    log.error("Fatal: Could not find templates/Dockerfile.j2. Exiting.")
    exit(1)
except TemplateError as e:
    log.error(f"Fatal: Jinja2 template error: {e}")
    exit(1)

# —— Flask app ——  
app = Flask(__name__)

def save_function_code(func_id: str, code: str) -> str:
    """
    Writes handler.py, requirements.txt, Dockerfile into
    functions/<func_id>/ and returns that path.
    """
    func_dir = os.path.join(BASE_DIR, func_id)
    os.makedirs(func_dir, exist_ok=True)

    # write user code as handler.py
    with open(os.path.join(func_dir, "handler.py"), "w") as f:
        f.write(code)

    # copy global requirements.txt (if present) into func dir
    src_reqs = os.path.join(os.getcwd(), "requirements.txt")
    dst_reqs = os.path.join(func_dir, "requirements.txt")
    if os.path.exists(src_reqs):
        shutil.copy(src_reqs, dst_reqs)
    else:
        open(dst_reqs, "w").close()

    # render Dockerfile
    df_content = dockerfile_tpl.render()
    with open(os.path.join(func_dir, "Dockerfile"), "w") as f:
        f.write(df_content)

    log.info(f"Saved code & Dockerfile to {func_dir}")
    return func_dir

@app.route("/functions", methods=["POST"])
def create_function():
    payload = request.get_json()
    if not payload or not payload.get("code"):
        return jsonify(error="Missing 'code' in JSON body"), 400

    code    = payload["code"]
    func_id = payload.get("func_id", str(uuid.uuid4()))
    log.info(f"Deploy request → ID={func_id}")

    try:
        func_path = save_function_code(func_id, code)
        image     = build_image(func_id, func_path, REGISTRY_URL)
        adapter.deploy(func_id, image)

    except (RuntimeError, NotImplementedError) as e:
        log.error(e)
        return jsonify(error=str(e)), 500

    except Exception as e:
        log.exception("Unexpected error")
        return jsonify(error="Server error"), 500

    return jsonify(func_id=func_id, status="deployed"), 201

@app.route("/functions/<func_id>/invoke", methods=["POST"])
def invoke_function(func_id):
    payload = request.get_data() or b""
    log.info(f"Invoke request → ID={func_id}")

    try:
        result = adapter.invoke(func_id, payload)
    except RuntimeError as e:
        log.error(e)
        return jsonify(error=str(e)), 500
    except Exception:
        log.exception("Unexpected error")
        return jsonify(error="Server error"), 500

    # Proxy raw response
    return result, 200, {"Content-Type": "application/octet-stream"}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    log.info(f"Starting Radical-FaaS agent on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)
