import subprocess
import logging

log = logging.getLogger(__name__)

def build_image(func_id: str, func_path: str, registry: str) -> str:
    """Builds & tags a Docker image, returns its full name."""
    image = f"{registry}/{func_id}:latest"
    log.info(f"Building Docker image: {image}")

    try:
        subprocess.run(
            ["docker", "build", "-t", image, func_path],
            check=True, capture_output=True, text=True
        )
        log.info(f"Built image: {image}")
        return image
    except subprocess.CalledProcessError as e:
        log.error(f"Build failed (code {e.returncode}):\nSTDOUT:{e.stdout}\nSTDERR:{e.stderr}")
        raise RuntimeError(e.stderr)
