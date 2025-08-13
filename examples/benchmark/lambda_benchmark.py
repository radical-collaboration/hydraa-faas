"""
Comprehensive FaaS Manager Benchmarking Suite - Fixed Version
------------------------------------------------------------
This script executes all requested tests with accurate metrics collection.

Tests:
1. Manager Footprint (CPU/Memory usage)
2. Resource Provisioning Time (IAM, ECR, etc.)
3. Deployment Performance (ZIP, Container, Pre-built)
4. Deployment Concurrency (1-128 concurrent deployments)
5. Invocation Concurrency (10-1000 concurrent invocations)
"""

import os
import sys
import time
import json
import argparse
import tempfile
import shutil
import threading
import logging
import signal
import statistics
import subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
import traceback
from datetime import datetime
import uuid
from dataclasses import asdict
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import seaborn as sns

# Add project root to path
try:
    from hydraa import Task, proxy
    from hydraa_faas.faas_manager.manager import FaasManager
    from hydraa_faas.utils.exceptions import FaasException
    from hydraa_faas.utils.metrics_collector import MetricsCollector
    # Import packaging to clear cache
    from hydraa_faas.utils.packaging import clear_package_cache
except ImportError:
    print("Error: Could not import hydraa_faas modules.")
    print("Please ensure the script is run from the project's root directory,")
    print("or that the project is installed as a package.")
    sys.exit(1)

# --- Benchmark Configuration ---
BENCHMARK_OUTPUT_DIR = Path(__file__).parent / "benchmark_results"
BENCHMARK_SANDBOX = Path(__file__).parent / "benchmark_sandbox"

# Test configurations
DEPLOYMENT_CONCURRENCY_LEVELS = [1, 2, 4, 8, 16, 32, 64, 128]
INVOCATION_CONCURRENCY_LEVELS = [10, 100, 1000]
EXPERIMENT_REPEATS = 3

# Thread pool sizes
MAX_DEPLOYMENT_WORKERS = 200
MAX_INVOCATION_WORKERS = 1500

# Function configurations
BENCHMARK_FUNCTION_PATH = Path(__file__).parent / "functions" / "benchmarks"
DYNAMIC_HTML_HANDLER = "webapps_100.dynamic_html_110.python.function.handler"

PREWARM_INVOCATIONS = 0
PLOT_FONTS = {
    'bar': {
        'title_size': 50,
        'title_weight': 'bold',
        'axis_label_size': 40,
        'axis_label_pad': 20,
        'axis_tick_size': 36,
        'x_tick_size': 36,
        'legend_size': 28,
        'legend_title_size': 30,
        'data_label_size': 22,
        'data_label_weight': 'bold',
        'total_annotation_size': 26,
    },

    'box': {
        'title_size': 40,
        'title_weight': 'bold',
        'axis_label_size': 36,
        'axis_label_pad': 20,
        'axis_tick_size': 26,
        'x_tick_size': 26,
        'stats_annotation_size': 20,
    },


    'invocation_bar': {
        'x_tick_size': 28,
    }
}


class BenchmarkSuite:
    """
    Orchestrates the entire benchmark process with precise metrics
    """

    def __init__(self, credentials_path: str, enable_containers: bool):
        self.credentials_path = credentials_path
        self.enable_containers = enable_containers
        self.output_dir = BENCHMARK_OUTPUT_DIR
        self.sandbox = BENCHMARK_SANDBOX
        self.faas_manager: Optional[FaasManager] = None
        self.metrics_collector: Optional[MetricsCollector] = None

        # Prepare directories
        shutil.rmtree(self.output_dir, ignore_errors=True)
        shutil.rmtree(self.sandbox, ignore_errors=True)
        self.output_dir.mkdir(exist_ok=True)
        self.sandbox.mkdir(exist_ok=True)

        self.logger = logging.getLogger(__name__)
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s [%(levelname)s] %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'benchmark.log'),
                logging.StreamHandler()
            ]
        )

        self.benchmark_id = f"faas_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.results: Dict[str, Any] = {
            "benchmark_id": self.benchmark_id,
            "timestamp": datetime.now().isoformat(),
            "config": {
                "repeats": EXPERIMENT_REPEATS,
                "deployment_concurrency": DEPLOYMENT_CONCURRENCY_LEVELS,
                "invocation_concurrency": INVOCATION_CONCURRENCY_LEVELS,
                "enable_containers": enable_containers,
                "benchmark_function": "110.dynamic-html"
            },
            "manager_footprint": {},
            "provisioning_times": {},
            "deployment_performance": {},
            "deployment_concurrency": {},
            "invocation_concurrency": {}
        }

        self.deployed_functions_for_cleanup = set()
        self._validate_benchmark_functions()

    def _create_combined_requirements(self):
        """Create a combined requirements.txt with all dependencies if it doesn't exist"""
        combined_req_path = BENCHMARK_FUNCTION_PATH / 'requirements.txt'

        if combined_req_path.exists():
            self.logger.info(f"Using existing requirements.txt at {combined_req_path}")
            return

        all_requirements = set()

        # Walk through all Python benchmarks and collect requirements
        for root, dirs, files in os.walk(BENCHMARK_FUNCTION_PATH):
            if 'requirements.txt' in files and root != str(BENCHMARK_FUNCTION_PATH):
                req_path = Path(root) / 'requirements.txt'
                with open(req_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            all_requirements.add(line)

        # Write combined requirements
        with open(combined_req_path, 'w') as f:
            f.write("# Combined requirements for all benchmarks\n")
            for req in sorted(all_requirements):
                f.write(f"{req}\n")

        self.logger.info(f"Created combined requirements.txt with {len(all_requirements)} dependencies")

    def _validate_benchmark_functions(self):
        """Validate that benchmark functions exist"""
        if not BENCHMARK_FUNCTION_PATH.exists():
            raise FileNotFoundError(f"Benchmarks directory not found at {BENCHMARK_FUNCTION_PATH}")

        dynamic_html_path = BENCHMARK_FUNCTION_PATH / "webapps_100" / "dynamic_html_110" / "python"
        if not dynamic_html_path.exists():
            raise FileNotFoundError(f"Dynamic HTML function not found at {dynamic_html_path}")

        required_files = ['function.py', 'requirements.txt', 'templates/template.html']
        for file_path in required_files:
            full_path = dynamic_html_path / file_path
            if not full_path.exists():
                raise FileNotFoundError(f"Required file missing: {full_path}")

        # Check if Dockerfile and requirements.txt exist at root level
        dockerfile_path = BENCHMARK_FUNCTION_PATH / 'Dockerfile'
        requirements_path = BENCHMARK_FUNCTION_PATH / 'requirements.txt'

        if not dockerfile_path.exists():
            self.logger.warning(f"Dockerfile not found at {dockerfile_path}")
            self.logger.warning("Container deployments will fail. Please create a Dockerfile.")

        if not requirements_path.exists():
            self.logger.warning(f"requirements.txt not found at {requirements_path}")
            self.logger.warning("Creating a basic requirements.txt file...")
            self._create_combined_requirements()

        self.logger.info(f"Validated benchmarks directory at {BENCHMARK_FUNCTION_PATH}")
        self.logger.info(f"Package will include entire benchmarks directory (~10-20MB)")

    def setup(self):
        """Initializes the environment, FaaS Manager, and resources."""
        self.logger.info("========== Starting Benchmark Setup ==========")

        # Clear package cache to ensure fresh builds
        self.logger.info("Clearing package cache...")
        clear_package_cache()

        self._setup_credentials()
        self.metrics_collector = MetricsCollector(output_dir=str(self.output_dir))

        # Start provisioning timer
        prov_timer = self.metrics_collector.start_provisioning()

        # Remove existing resource config
        resource_config_file = Path(os.path.expanduser("~/.faas/aws_resources.json"))
        if resource_config_file.exists():
            self.logger.info(f"Removing existing resource config: {resource_config_file}")
            resource_config_file.unlink()

        proxy_mgr = proxy(providers=['aws'])

        # Create resource manager with metrics collector
        from hydraa_faas.utils.resource_manager import ResourceManager
        resource_manager = ResourceManager()

        resource_config = {
            'aws': {
                'auto_setup_resources': True,
                'reuse_existing': False,
                'enable_container_support': self.enable_containers
            }
        }

        self.logger.info("Initializing FaaS Manager and provisioning resources...")

        self.faas_manager = FaasManager(
            proxy_mgr=proxy_mgr,
            asynchronous=True,
            auto_terminate=False,
            resource_config=resource_config,
            resource_manager=resource_manager,
            metrics_collector=self.metrics_collector
        )

        # Pass metrics collector to resource manager for IAM timing
        self.faas_manager.resource_manager.metrics_collector = self.metrics_collector

        self.faas_manager.start(sandbox=str(self.sandbox))

        # Wait for manager to be ready
        ready_start = time.perf_counter()
        while not self._is_manager_ready():
            if time.perf_counter() - ready_start > 180:
                raise TimeoutError("FaaS Manager initialization timed out")
            time.sleep(1)

        # Complete provisioning tracking
        self.metrics_collector.complete_provisioning()

        # Extract provisioning metrics
        prov_metrics = self.metrics_collector.provisioning_metrics
        self.results["provisioning_times"] = {
            "total_provisioning_ms": prov_metrics.total_provisioning_ms,
            "iam_role_creation_ms": prov_metrics.iam_role_creation_ms,
            "iam_role_propagation_ms": prov_metrics.iam_role_propagation_ms,
            "iam_policy_attachment_ms": prov_metrics.iam_policy_attachment_ms,
            "ecr_repository_creation_ms": prov_metrics.ecr_repository_creation_ms,
            "resource_validation_ms": prov_metrics.resource_validation_ms,
            "iam_min_ms": prov_metrics.iam_min_ms,
            "iam_max_ms": prov_metrics.iam_max_ms,
            "ecr_min_ms": prov_metrics.ecr_min_ms,
            "ecr_max_ms": prov_metrics.ecr_max_ms
        }

        self.logger.info(f"Provisioning complete:")
        self.logger.info(f"  Total time: {prov_metrics.total_provisioning_ms:.2f}ms")
        self.logger.info(f"  IAM role creation: {prov_metrics.iam_role_creation_ms:.2f}ms")
        self.logger.info(f"  IAM role propagation: {prov_metrics.iam_role_propagation_ms:.2f}ms")
        self.logger.info(f"  IAM policy attachment: {prov_metrics.iam_policy_attachment_ms:.2f}ms")
        if self.enable_containers:
            self.logger.info(f"  ECR repository creation: {prov_metrics.ecr_repository_creation_ms:.2f}ms")
        self.logger.info(f"  IAM min/max: {prov_metrics.iam_min_ms:.2f}ms / {prov_metrics.iam_max_ms:.2f}ms")
        if self.enable_containers:
            self.logger.info(f"  ECR min/max: {prov_metrics.ecr_min_ms:.2f}ms / {prov_metrics.ecr_max_ms:.2f}ms")

        self.logger.info("========== Benchmark Setup Complete ==========")

    def _is_manager_ready(self) -> bool:
        if 'lambda' not in self.faas_manager._providers:
            return False
        return self.faas_manager._providers['lambda']['instance'].is_active

    def _setup_credentials(self):
        """Load and set AWS credentials"""
        with open(self.credentials_path, 'r') as f:
            creds = json.load(f).get('aws', {})

        access_key = creds.get('aws_access_key_id', '')
        secret_key = creds.get('aws_secret_access_key', '')
        region = creds.get('region_name', 'us-east-1')

        os.environ['ACCESS_KEY_ID'] = access_key
        os.environ['AWS_ACCESS_KEY_ID'] = access_key
        os.environ['ACCESS_KEY_SECRET'] = secret_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key
        os.environ['AWS_REGION'] = region
        os.environ['AWS_DEFAULT_REGION'] = region

    def run_all_tests(self):
        """Executes the full suite of benchmark tests."""
        try:
            self.logger.info("========== Starting Deployment Performance Test ==========")
            self._test_deployment_performance()
            self.logger.info("========== Deployment Performance Test Complete ==========\n")

            self.logger.info("========== Starting Deployment Concurrency Test ==========")
            self._test_deployment_concurrency()
            self.logger.info("========== Deployment Concurrency Test Complete ==========\n")

            self.logger.info("========== Starting Invocation Concurrency Test ==========")
            self._test_invocation_concurrency()
            self.logger.info("========== Invocation Concurrency Test Complete ==========\n")
        except Exception as e:
            self.logger.error(f"Critical error during tests: {e}", exc_info=True)
            raise
        finally:
            self.logger.info("========== All Tests Complete ==========")

    def _test_deployment_performance(self):
        """Test three deployment types with proper metrics breakdown"""
        self.logger.info("Testing deployment performance for ZIP, Container, and Pre-built image...")
        results = {"zip": [], "container": [], "prebuilt": []}

        # Test 1: ZIP Deployments
        self.logger.info("\n--- Running ZIP Deployment Tests ---")
        for repeat in range(EXPERIMENT_REPEATS):
            self.logger.info(f"Repetition {repeat + 1}/{EXPERIMENT_REPEATS}")

            # Clear package cache before each deployment
            clear_package_cache()

            try:
                zip_result = self._test_zip_deployment()
                results["zip"].append(zip_result)
                self.logger.info(f"ZIP deployment completed: {zip_result['total_time_ms']:.2f}ms")
            except Exception as e:
                self.logger.error(f"ZIP deployment failed: {e}", exc_info=True)
                results["zip"].append({"error": str(e)})
            time.sleep(5)

        # Test 2: Container Deployments
        if self.enable_containers:
            self.logger.info("\n--- Running Container Build-and-Deploy Tests ---")

            for repeat in range(EXPERIMENT_REPEATS):
                self.logger.info(f"Repetition {repeat + 1}/{EXPERIMENT_REPEATS}")
                try:
                    container_result = self._test_container_deployment()
                    results["container"].append(container_result)
                    self.logger.info(f"Container deployment completed: {container_result['total_time_ms']:.2f}ms")
                    self.logger.info(f"  - Image build time: {container_result.get('image_build_ms', 0):.2f}ms")
                    self.logger.info(f"  - Registry push time: {container_result.get('registry_push_ms', 0):.2f}ms")
                    self.logger.info(f"  - Platform API call: {container_result.get('platform_api_call_ms', 0):.2f}ms")
                except Exception as e:
                    self.logger.error(f"Container deployment failed: {e}", exc_info=True)
                    results["container"].append({"error": str(e)})
                time.sleep(5)

        # Get an image URI directly from ECR for pre-built tests
        image_uri_for_prebuilt = None
        if self.enable_containers:
            try:
                self.logger.info("\n--- Fetching an image URI from ECR for pre-built tests ---")
                image_uri_for_prebuilt = self._get_existing_image_from_ecr()
                if image_uri_for_prebuilt:
                    self.logger.info(f"Found image in ECR for pre-built tests: {image_uri_for_prebuilt}")
                else:
                    self.logger.warning("No images found in ECR repository for pre-built tests")
            except Exception as e:
                self.logger.error(f"Failed to fetch image from ECR: {e}", exc_info=True)

        # Test 3: Pre-built Image Deployments
        if self.enable_containers and image_uri_for_prebuilt:
            self.logger.info(f"\n--- Running Pre-built Image Deployment Tests ---")
            self.logger.info(f"Using image: {image_uri_for_prebuilt}")
            for repeat in range(EXPERIMENT_REPEATS):
                self.logger.info(f"Repetition {repeat + 1}/{EXPERIMENT_REPEATS}")
                try:
                    prebuilt_result = self._test_prebuilt_deployment(image_uri_for_prebuilt)
                    results["prebuilt"].append(prebuilt_result)
                    self.logger.info(f"Pre-built deployment completed: {prebuilt_result['total_time_ms']:.2f}ms")
                except Exception as e:
                    self.logger.error(f"Pre-built deployment failed: {e}", exc_info=True)
                    results["prebuilt"].append({"error": str(e)})
                time.sleep(5)
        elif self.enable_containers:
            self.logger.warning("Skipping pre-built image tests because no suitable image was found in ECR")

        self.results["deployment_performance"] = results

    def _clear_docker_cache(self):
        """Clear Docker build cache to ensure fresh builds"""
        self.logger.info("Clearing Docker build cache...")
        try:
            # Remove all unused images
            result = subprocess.run(['docker', 'image', 'prune', '-f'],
                                  capture_output=True, text=True, check=True)
            self.logger.info(f"Docker image cache cleared: {result.stdout.strip()}")

            # Also remove dangling images
            result = subprocess.run(['docker', 'images', '-f', 'dangling=true', '-q'],
                                  capture_output=True, text=True, check=True)
            dangling_images = result.stdout.strip().split('\n')
            if dangling_images and dangling_images[0]:
                for image_id in dangling_images:
                    try:
                        subprocess.run(['docker', 'rmi', image_id],
                                     capture_output=True, text=True, check=True)
                    except:
                        pass
                self.logger.info(f"Removed {len(dangling_images)} dangling images")

            # Give Docker time to stabilize
            self.logger.info("Waiting for Docker to stabilize...")
            time.sleep(5)

        except subprocess.CalledProcessError as e:
            self.logger.warning(f"Failed to clear Docker cache: {e.stderr}")
        except Exception as e:
            self.logger.warning(f"Error clearing Docker cache: {e}")

    def _test_container_deployment(self) -> Dict[str, Any]:
        """Test container deployment with build metrics"""
        func_name = f"container-test-{uuid.uuid4().hex[:8]}"

        # Clear Docker cache before each deployment
        self._clear_docker_cache()

        # Ensure Dockerfile exists
        dockerfile_path = BENCHMARK_FUNCTION_PATH / 'Dockerfile'
        if not dockerfile_path.exists():
            raise FileNotFoundError(f"Dockerfile not found at {dockerfile_path}. Please create it first.")

        # Ensure requirements.txt exists
        requirements_path = BENCHMARK_FUNCTION_PATH / 'requirements.txt'
        if not requirements_path.exists():
            raise FileNotFoundError(f"requirements.txt not found at {requirements_path}. Please create it first.")

        self.logger.info(f"Using existing Dockerfile and requirements.txt from {BENCHMARK_FUNCTION_PATH}")

        task = Task(name=func_name)
        task.provider = "lambda"
        task.build_image = True
        task.memory = 512
        task.timeout = 30
        task.source_path = str(BENCHMARK_FUNCTION_PATH)

        try:
            self.faas_manager.submit(task)
            task.result(timeout=300)

            # Get deployment metrics
            deployment = self._get_latest_deployment_metrics()
            self.deployed_functions_for_cleanup.add(task.name)

            return {
                "function_name": task.name,
                "total_time_ms": deployment.total_time_ms,
                "task_preparation_ms": deployment.task_preparation_ms,
                "queue_time_ms": deployment.queue_time_ms,
                "image_build_ms": deployment.image_build_ms,
                "registry_push_ms": deployment.registry_push_ms,
                "platform_api_call_ms": deployment.platform_api_call_ms,
                "manager_overhead_ms": deployment.manager_overhead_ms,
                "image_size_mb": deployment.image_size_mb
            }
        except Exception as e:
            self.logger.error(f"Container deployment failed: {e}")
            raise

    def _get_existing_image_from_ecr(self) -> Optional[str]:
        """Get an existing image URI from ECR repository"""
        try:
            lambda_provider = self.faas_manager._providers['lambda']['instance']
            ecr_client = lambda_provider._ecr_client
            repo_name = lambda_provider.resource_manager.aws_resources.ecr_repository_name
            repo_uri = lambda_provider.resource_manager.aws_resources.ecr_repository_uri

            if not repo_name or not repo_uri:
                self.logger.warning("ECR repository information not found")
                return None

            # List images in the repository
            response = ecr_client.list_images(repositoryName=repo_name)

            if response.get('imageIds'):
                # Get the most recent image
                for image in response['imageIds']:
                    if 'imageTag' in image and image['imageTag']:
                        image_tag = image['imageTag']
                        image_uri = f"{repo_uri}:{image_tag}"

                        # Verify the image exists
                        try:
                            ecr_client.describe_images(
                                repositoryName=repo_name,
                                imageIds=[{'imageTag': image_tag}]
                            )
                            self.logger.info(f"Verified image exists: {image_uri}")
                            return image_uri
                        except Exception as verify_error:
                            self.logger.warning(f"Image {image_uri} verification failed: {verify_error}")
                            continue

                self.logger.warning("No valid tagged images found in ECR repository")
                return None
            else:
                self.logger.warning("ECR repository is empty")
                return None

        except Exception as e:
            self.logger.error(f"Error retrieving image from ECR: {e}", exc_info=True)
            return None

    def _test_zip_deployment(self) -> Dict[str, Any]:
        """Test ZIP deployment with accurate metrics"""
        func_name = f"zip-test-{uuid.uuid4().hex[:8]}"
        task = Task(name=func_name)
        task.provider = "lambda"
        task.memory = 512
        task.timeout = 30
        task.runtime = "python3.9"
        task.handler = DYNAMIC_HTML_HANDLER
        task.source_path = str(BENCHMARK_FUNCTION_PATH)

        self.faas_manager.submit(task)
        task.result(timeout=180)

        # Get deployment metrics
        deployment = self._get_latest_deployment_metrics()
        self.deployed_functions_for_cleanup.add(task.name)

        if deployment.package_size_bytes:
            self.logger.info(f"Deployment package size: {deployment.package_size_bytes / (1024*1024):.2f} MB")

        # Log all timing components for debugging
        self.logger.info(f"ZIP Deployment Timing Breakdown:")
        self.logger.info(f"  Task Preparation: {deployment.task_preparation_ms:.2f}ms")
        self.logger.info(f"  Queue Time: {deployment.queue_time_ms:.2f}ms")
        self.logger.info(f"  Package Creation: {deployment.package_creation_ms:.2f}ms")
        self.logger.info(f"  Platform API Call: {deployment.platform_api_call_ms:.2f}ms")
        self.logger.info(f"  Manager Overhead: {deployment.manager_overhead_ms:.2f}ms")
        self.logger.info(f"  Total Time: {deployment.total_time_ms:.2f}ms")

        return {
            "function_name": task.name,
            "total_time_ms": deployment.total_time_ms,
            "task_preparation_ms": deployment.task_preparation_ms,
            "queue_time_ms": deployment.queue_time_ms,
            "package_creation_ms": deployment.package_creation_ms,
            "platform_api_call_ms": deployment.platform_api_call_ms,
            "manager_overhead_ms": deployment.manager_overhead_ms,
            "package_size_bytes": deployment.package_size_bytes
        }

    def _test_prebuilt_deployment(self, image_uri: str) -> Dict[str, Any]:
        """Test pre-built image deployment"""
        func_name = f"prebuilt-test-{uuid.uuid4().hex[:8]}"
        task = Task(name=func_name)
        task.provider = "lambda"
        task.memory = 512
        task.timeout = 30
        task.image = image_uri

        self.faas_manager.submit(task)
        task.result(timeout=180)

        # Get deployment metrics
        deployment = self._get_latest_deployment_metrics()
        self.deployed_functions_for_cleanup.add(task.name)

        return {
            "function_name": task.name,
            "total_time_ms": deployment.total_time_ms,
            "task_preparation_ms": deployment.task_preparation_ms,
            "queue_time_ms": deployment.queue_time_ms,
            "platform_api_call_ms": deployment.platform_api_call_ms,
            "manager_overhead_ms": deployment.manager_overhead_ms
        }

    def _test_deployment_concurrency(self):
        """Test concurrent deployments with accurate metrics"""
        self.logger.info("Testing deployment concurrency...")
        concurrency_results = {}

        for level in DEPLOYMENT_CONCURRENCY_LEVELS:
            self.logger.info(f"\n--- Testing {level} concurrent deployments ---")
            level_results = []

            for repeat in range(EXPERIMENT_REPEATS):
                self.logger.info(f"Repetition {repeat + 1}/{EXPERIMENT_REPEATS}")

                # Clear package cache before each test
                clear_package_cache()

                deployment_times, packaging_times = self._run_concurrent_deployments(level)

                level_results.append({
                    "concurrent_deployments": level,
                    "individual_times_ms": deployment_times,
                    "individual_packaging_ms": packaging_times,
                    "avg_time_ms": statistics.mean(deployment_times) if deployment_times else 0,
                    "avg_packaging_ms": statistics.mean(packaging_times) if packaging_times else 0,
                    "std_dev_ms": statistics.stdev(deployment_times) if len(deployment_times) > 1 else 0,
                    "min_time_ms": min(deployment_times) if deployment_times else 0,
                    "max_time_ms": max(deployment_times) if deployment_times else 0,
                    "success_rate": len(deployment_times) / level * 100
                })

                if repeat < EXPERIMENT_REPEATS - 1:
                    time.sleep(10)

            concurrency_results[str(level)] = level_results

        self.results["deployment_concurrency"] = concurrency_results

    def _run_concurrent_deployments(self, num_deployments: int) -> Tuple[List[float], List[float]]:
        """Run concurrent deployments and collect timing and packaging separately"""
        tasks = []
        for i in range(num_deployments):
            func_name = f"concurrent-{num_deployments}-{i}-{uuid.uuid4().hex[:6]}"
            task = Task(name=func_name)
            task.provider = "lambda"
            task.memory = 256
            task.timeout = 30
            task.runtime = "python3.9"
            task.handler = DYNAMIC_HTML_HANDLER
            task.source_path = str(BENCHMARK_FUNCTION_PATH)
            tasks.append(task)
            self.deployed_functions_for_cleanup.add(task.name)

        self.logger.info(f"Submitting {num_deployments} tasks simultaneously...")

        # Submit all tasks at once
        start_time = time.perf_counter()
        self.faas_manager.submit(tasks)

        # Wait for all tasks to complete
        deployment_times = []
        packaging_times = []

        with ThreadPoolExecutor(max_workers=min(num_deployments, 50)) as executor:
            future_to_task = {executor.submit(task.result, 300): task for task in tasks}

            for future in as_completed(future_to_task):
                task = future_to_task[future]
                try:
                    future.result()
                    # Find deployment metrics for this task
                    deployment = self._find_deployment_by_function_name(task.name)
                    if deployment:
                        deployment_times.append(deployment.total_time_ms)
                        # For packaging time, use only the package creation time
                        packaging_times.append(deployment.package_creation_ms)
                except Exception as e:
                    self.logger.error(f"Task {task.name} failed: {e}")

        self.logger.info(f"Completed {len(deployment_times)}/{num_deployments} deployments.")
        return deployment_times, packaging_times

    def _test_invocation_concurrency(self):
        """Test concurrent invocations with accurate metrics"""
        self.logger.info("Testing invocation concurrency...")

        # Deploy a single function for invocation tests
        self.logger.info("Deploying a single function for invocation tests...")
        inv_func_name = f"invocation-test-{uuid.uuid4().hex[:8]}"
        task = Task(name=inv_func_name)
        task.provider = "lambda"
        task.memory = 1024
        task.timeout = 60
        task.runtime = "python3.9"
        task.handler = DYNAMIC_HTML_HANDLER
        task.source_path = str(BENCHMARK_FUNCTION_PATH)

        self.faas_manager.submit(task)
        result = task.result(timeout=180)
        self.deployed_functions_for_cleanup.add(task.name)

        # Get the actual deployed function name
        actual_function_name = task.name
        self.logger.info(f"Invocation test function deployed successfully: {actual_function_name}")

        self.logger.info("Waiting for function to become fully active...")
        time.sleep(20)

        # Pre-warm the function with a few invocations
        self.logger.info(f"Pre-warming function with {PREWARM_INVOCATIONS} invocations...")
        test_payload = {
            "username": "prewarm_user",
            "random_len": 100
        }

        for i in range(PREWARM_INVOCATIONS):
            try:
                prewarm_result = self.faas_manager.invoke(actual_function_name, payload=test_payload, provider="lambda")
                self.logger.info(f"Pre-warm invocation {i+1} completed")
                time.sleep(1)  # Small delay between pre-warm invocations
            except Exception as e:
                self.logger.warning(f"Pre-warm invocation {i+1} failed: {e}")

        self.logger.info("Pre-warming complete. Starting concurrency tests...")
        time.sleep(5)  # Wait a bit after pre-warming

        invocation_results = {}
        for level in INVOCATION_CONCURRENCY_LEVELS:
            self.logger.info(f"\n--- Testing {level} concurrent invocations ---")
            level_results = []

            for repeat in range(EXPERIMENT_REPEATS):
                self.logger.info(f"Repetition {repeat + 1}/{EXPERIMENT_REPEATS}")
                try:
                    metrics, true_makespan_ms, avg_overhead_ms = self._run_concurrent_invocations_fixed(level, actual_function_name)

                    if metrics:
                        level_results.append({
                            "concurrent_invocations": level,
                            "successful_invocations": len(metrics),
                            "avg_makespan_ms": true_makespan_ms,
                            "avg_overhead_ms": avg_overhead_ms,
                            "success_rate": len(metrics) / level * 100
                        })

                        self.logger.info(f"Results: True makespan: {true_makespan_ms:.2f}ms, "
                                       f"Avg overhead: {avg_overhead_ms:.2f}ms")
                    else:
                        self.logger.error(f"No successful invocations for {level} concurrent calls")
                        level_results.append({
                            "concurrent_invocations": level,
                            "successful_invocations": 0,
                            "avg_makespan_ms": 0,
                            "avg_overhead_ms": 0,
                            "success_rate": 0
                        })
                except Exception as e:
                    self.logger.error(f"Error in invocation test: {e}", exc_info=True)
                    level_results.append({
                        "concurrent_invocations": level,
                        "successful_invocations": 0,
                        "error": str(e)
                    })

                if repeat < EXPERIMENT_REPEATS - 1:
                    time.sleep(5)

            invocation_results[str(level)] = level_results

        self.results["invocation_concurrency"] = invocation_results
        self.logger.info("Invocation concurrency tests completed")

    def _run_concurrent_invocations_fixed(self, num_invocations: int, function_name: str) -> Tuple[List[Dict[str, float]], float, float]:
        """
        Run concurrent invocations and collect metrics with proper makespan calculation

        Returns:
            - List of individual invocation metrics
            - True makespan (first function start to last function end) in ms
            - Average overhead (manager + network) per invocation in ms
        """
        results = []
        function_start_times = []
        function_end_times = []
        lock = threading.Lock()

        def invoke_function(inv_num: int):
            try:
                # Create payload that returns timestamps
                payload = {
                    "username": f"user_{uuid.uuid4().hex[:8]}",
                    "random_len": 100,
                    "return_timestamps": True  # Request timestamps from function
                }

                self.logger.debug(f"Invoking function {function_name} (invocation {inv_num})")

                # Record invocation start time
                invocation_start = time.perf_counter()

                # Invoke function
                result = self.faas_manager.invoke(function_name, payload=payload, provider="lambda")

                # Record invocation end time
                invocation_end = time.perf_counter()

                self.logger.debug(f"Invocation {inv_num} completed")

                # Extract function timing from response
                function_start = None
                function_end = None
                function_execution_ms = None

                if isinstance(result, dict) and 'Payload' in result:
                    payload_data = result['Payload']
                    if isinstance(payload_data, dict):
                        # Get actual function timestamps
                        function_start = payload_data.get('function_start_time')
                        function_end = payload_data.get('function_end_time')
                        function_execution_ms = payload_data.get('execution_time_ms')

                        # Store function timestamps for makespan calculation
                        if function_start and function_end:
                            with lock:
                                function_start_times.append(function_start)
                                function_end_times.append(function_end)
                        else:
                            self.logger.warning(f"No timestamps in response for invocation {inv_num}")

                # Get invocation metrics from metrics collector
                invocation_metric = None
                if self.metrics_collector and self.metrics_collector._completed_invocations:
                    # Find the matching invocation metric
                    for inv in reversed(self.metrics_collector._completed_invocations):
                        if inv.function_name == function_name and inv.start_time >= invocation_start - 1:
                            invocation_metric = inv
                            break

                if invocation_metric:
                    return {
                        "overhead_ms": invocation_metric.manager_overhead_ms,
                        "network_ms": invocation_metric.network_delay_ms,
                        "total_ms": invocation_metric.total_time_ms,
                        "function_start": function_start,
                        "function_end": function_end
                    }
                else:
                    self.logger.warning(f"Could not find metrics for invocation {inv_num}")
                    return None

            except Exception as e:
                self.logger.error(f"Invocation {inv_num} failed: {e}", exc_info=True)
                return None

        # Run invocations concurrently with a small ramp-up
        self.logger.info(f"Starting {num_invocations} concurrent invocations...")

        # Use batched submission for better scaling
        batch_size = 100
        all_futures = []

        with ThreadPoolExecutor(max_workers=min(num_invocations, MAX_INVOCATION_WORKERS)) as executor:
            for batch_start in range(0, num_invocations, batch_size):
                batch_end = min(batch_start + batch_size, num_invocations)
                batch_futures = []

                for i in range(batch_start, batch_end):
                    future = executor.submit(invoke_function, i+1)
                    batch_futures.append(future)

                all_futures.extend(batch_futures)

                # Small delay between batches to avoid overwhelming the system
                if batch_end < num_invocations:
                    time.sleep(0.1)

            # Collect results
            completed = 0
            for future in as_completed(all_futures):
                result = future.result()
                if result:
                    results.append(result)
                completed += 1
                if completed % 100 == 0 or completed == num_invocations:
                    self.logger.info(f"Progress: {completed}/{num_invocations} invocations completed")

        # Calculate true makespan
        true_makespan_ms = 0
        if function_start_times and function_end_times:
            first_start = min(function_start_times)
            last_end = max(function_end_times)
            true_makespan_ms = (last_end - first_start) * 1000
            self.logger.info(f"True makespan calculation: first_start={first_start:.6f}, last_end={last_end:.6f}")
            self.logger.info(f"True makespan: {true_makespan_ms:.2f}ms ({true_makespan_ms/1000:.2f}s)")

            # Debug: Log number of successful timestamp collections
            self.logger.info(f"Collected timestamps from {len(function_start_times)}/{num_invocations} invocations")

            # Sanity check - makespan should be at least 10 seconds (10000ms) since function sleeps for 10s
            if true_makespan_ms < 10000:
                self.logger.warning(f"Makespan seems too low ({true_makespan_ms:.2f}ms) for a 10s function!")
                self.logger.warning("This might indicate the function is not returning proper timestamps")
        else:
            self.logger.warning("No function timestamps available for makespan calculation")
            self.logger.warning("Make sure your benchmark function returns 'function_start_time' and 'function_end_time'")

        # Calculate average overhead per invocation
        avg_overhead_ms = 0
        if results:
            total_overheads = [r['overhead_ms'] + r['network_ms'] for r in results]
            avg_overhead_ms = statistics.mean(total_overheads)

            self.logger.info(f"\nInvocation Summary for {num_invocations} concurrent calls:")
            self.logger.info(f"  Successful: {len(results)}/{num_invocations} ({len(results)/num_invocations*100:.1f}%)")
            self.logger.info(f"  True Makespan: {true_makespan_ms:.2f}ms ({true_makespan_ms/1000:.2f}s)")
            self.logger.info(f"  Avg Manager Overhead: {statistics.mean([r['overhead_ms'] for r in results]):.2f}ms")
            self.logger.info(f"  Avg Network Delay: {statistics.mean([r['network_ms'] for r in results]):.2f}ms")
            self.logger.info(f"  Avg Total Overhead: {avg_overhead_ms:.2f}ms")

        return results, true_makespan_ms, avg_overhead_ms

    def _get_latest_deployment_metrics(self):
        """Get the most recent deployment metrics"""
        if self.metrics_collector and self.metrics_collector._completed_deployments:
            return self.metrics_collector._completed_deployments[-1]
        return None

    def _find_deployment_by_function_name(self, function_name: str):
        """Find deployment metrics by function name"""
        if self.metrics_collector:
            for deployment in reversed(self.metrics_collector._completed_deployments):
                if deployment.function_name == function_name:
                    return deployment
        return None

    def teardown(self):
        """Clean up resources and save results"""
        self.logger.info("========== Starting Benchmark Teardown ==========")

        # Get manager footprint
        if self.metrics_collector:
            footprint = self.metrics_collector.get_manager_footprint()
            self.results["manager_footprint"] = asdict(footprint)
            self.logger.info(f"Manager Footprint: CPU avg={footprint.avg_cpu_percent:.2f}%, "
                           f"Memory avg={footprint.avg_memory_mb:.2f}MB")

            # Include detailed deployment metrics
            self.results["deployments"] = [asdict(d) for d in self.metrics_collector._completed_deployments]

        # Clean up deployed functions
        if self.faas_manager:
            self.logger.info(f"Cleaning up {len(self.deployed_functions_for_cleanup)} functions...")
            with ThreadPoolExecutor(max_workers=20) as executor:
                list(executor.map(self._delete_function_safe, self.deployed_functions_for_cleanup))

            self._cleanup_aws_resources()
            self.logger.info("Shutting down FaaS Manager...")
            self.faas_manager.shutdown()
            self.faas_manager.wait_for_shutdown(timeout=60)

        # Save results
        results_path = self.output_dir / "results.json"
        with open(results_path, 'w') as f:
            json.dump(self.results, f, indent=2)

        if self.metrics_collector:
            self.metrics_collector.save_results("detailed_metrics.json")

        self.logger.info(f"Results saved to {results_path}")

        # Generate plots
        self.logger.info("Generating plots...")
        self._generate_plots()

        self.logger.info("========== Teardown Complete ==========")

    def _delete_function_safe(self, func_name: str):
        """Safely delete a function"""
        try:
            self.faas_manager.delete_function(func_name, 'lambda')
            self.logger.info(f"Deleted function: {func_name}")
        except Exception as e:
            self.logger.debug(f"Could not delete {func_name}: {e}")

    def _cleanup_aws_resources(self):
        """Clean up AWS resources"""
        lambda_provider = self.faas_manager._providers.get('lambda', {}).get('instance')
        if not lambda_provider:
            return

        # Clean up cache bust files
        for cache_file in BENCHMARK_FUNCTION_PATH.glob('.cachebust_*'):
            cache_file.unlink()
            self.logger.info(f"Removed cache bust file: {cache_file}")

        # Clean up ECR repository if enabled
        if self.enable_containers:
            self.logger.info("Cleaning up ECR repository...")
            try:
                repo_name = lambda_provider.resource_manager.aws_resources.ecr_repository_name
                if repo_name:
                    lambda_provider.registry_manager.delete_ecr_repository(repo_name, force=True)
                    self.logger.info(f"Deleted ECR repository: {repo_name}")
            except Exception as e:
                self.logger.warning(f"ECR cleanup error: {e}")

        # Clean up IAM role
        self.logger.info("Cleaning up IAM role...")
        try:
            role_name = lambda_provider.resource_manager.aws_resources.iam_role_name
            if role_name:
                iam_client = lambda_provider._iam_client
                # Detach policies
                for policy in ['arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
                             'arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole']:
                    try:
                        iam_client.detach_role_policy(RoleName=role_name, PolicyArn=policy)
                    except:
                        pass
                # Delete role
                iam_client.delete_role(RoleName=role_name)
                self.logger.info(f"Deleted IAM role: {role_name}")
        except Exception as e:
            self.logger.warning(f"IAM cleanup error: {e}")

    def _generate_plots(self):
        """Generate all required plots"""
        # Set style
        plt.style.use('seaborn-v0_8-whitegrid')

        # Define consistent color palette
        self.color_palette = {
            'primary': '#2E86AB',    # Blue
            'secondary': '#F18F01',  # Orange
            'tertiary': '#C73E1D',   # Red
            'quaternary': '#2F4858', # Dark Blue
            'quinary': '#F6AE2D',    # Yellow
            'senary': '#A23B72'      # Purple
        }

        # Create plots directory
        plots_dir = self.output_dir / "plots"
        plots_dir.mkdir(exist_ok=True)

        # Generate each plot
        self._plot_deployment_overhead_breakdown(plots_dir)
        self._plot_invocation_makespan_overhead(plots_dir)
        self._plot_deployment_concurrency_boxplots(plots_dir)
        self._plot_deployment_packaging_concurrency(plots_dir)
        self._create_summary_text(plots_dir)

    def _setup_plot_style(self, ax):
        """Common style setup for all plots"""
        # Remove background
        ax.set_facecolor('white')
        ax.figure.patch.set_facecolor('white')

        # Keep only grid
        ax.grid(True, axis='y', alpha=0.3, linewidth=1.5)
        ax.set_axisbelow(True)

        # Remove top and right spines
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)

    def _plot_deployment_overhead_breakdown(self, plots_dir: Path):
        """Create stacked bar chart for deployment overhead breakdown (in seconds)"""
        from matplotlib.ticker import MaxNLocator

        deployment_data = self.results.get("deployment_performance", {})

        if not deployment_data:
            self.logger.warning("No deployment performance data found for plotting")
            return

        # Process data in the requested order: zip, prebuilt, container
        deployment_order = ["zip", "prebuilt", "container"]

        # Define overhead components and their display names
        # Task preparation is already merged into package/image creation in metrics_collector
        overhead_components = {
            "package_creation_ms": "Package Creation",
            "image_build_ms": "Image Build",
            "registry_push_ms": "Registry Push",
            "platform_api_call_ms": "Lambda Setup"
        }

        # Colors for each component
        colors = {
            "Package Creation": self.color_palette['tertiary'],
            "Image Build": self.color_palette['quaternary'],
            "Registry Push": self.color_palette['quinary'],
            "Lambda Setup": self.color_palette['senary']
        }

        # Collect average values (convert to seconds)
        plot_data = {component: [] for component in overhead_components.values()}
        labels = []
        deployment_types = []

        for dtype in deployment_order:
            runs = deployment_data.get(dtype, [])
            if not runs or all('error' in run for run in runs):
                continue

            valid_runs = [run for run in runs if 'error' not in run]
            if not valid_runs:
                continue

            deployment_types.append(dtype)
            labels.append(
                dtype.replace("prebuilt", "Pre-built").replace("zip", "ZIP").replace("container", "Container"))

            # Calculate averages in seconds
            for comp_key, comp_label in overhead_components.items():
                values = [run.get(comp_key, 0) / 1000.0 for run in valid_runs]
                avg_value = statistics.mean(values) if values else 0
                plot_data[comp_label].append(avg_value)

        if not deployment_types:
            self.logger.warning("No valid deployment data to plot")
            return

        # Create figure
        fig, ax = plt.subplots(figsize=(25, 19))

        # Create stacked bars
        x = np.arange(len(labels))
        width = 0.6
        bottom = np.zeros(len(labels))

        for component in overhead_components.values():
            values = plot_data[component]
            if any(v > 0 for v in values):
                bar = ax.bar(x, values, width, label=component, bottom=bottom,
                             color=colors.get(component, '#888888'), edgecolor='black', linewidth=2)

                # Add value labels
                for i, (rect, val) in enumerate(zip(bar, values)):
                    if val > 0.01:  # Only show label if > 10ms
                        height = rect.get_height()
                        ax.text(rect.get_x() + rect.get_width() / 2.,
                                bottom[i] + height / 2,
                                f'{val:.3f}s',
                                ha='center', va='center', color='white',
                                fontweight=PLOT_FONTS['bar']['data_label_weight'],
                                fontsize=PLOT_FONTS['bar']['data_label_size'])

                bottom += np.array(values)

        # Customize plot
        ax.set_xlabel('Deployment Type', fontsize=PLOT_FONTS['bar']['axis_label_size'],
                      labelpad=PLOT_FONTS['bar']['axis_label_pad'])
        ax.set_ylabel('Time (seconds)', fontsize=PLOT_FONTS['bar']['axis_label_size'],
                      labelpad=PLOT_FONTS['bar']['axis_label_pad'])
        ax.set_title('Deployment Overhead Breakdown by Type',
                     fontsize=PLOT_FONTS['bar']['title_size'],
                     fontweight=PLOT_FONTS['bar']['title_weight'], pad=30)
        ax.set_xticks(x)
        ax.set_xticklabels(labels, fontsize=PLOT_FONTS['bar']['x_tick_size'])
        ax.tick_params(axis='y', labelsize=PLOT_FONTS['bar']['axis_tick_size'])

        # Add legend at top left (matching invocation plot)
        ax.legend(title='Overhead Component', loc='upper left',
                  fontsize=PLOT_FONTS['bar']['legend_size'],
                  title_fontsize=PLOT_FONTS['bar']['legend_title_size'])

        # Add total time annotations
        for i, total in enumerate(bottom):
            ax.text(i, total + 0.05, f'Total: {total:.3f}s',
                    ha='center', fontsize=PLOT_FONTS['bar']['total_annotation_size'],
                    fontweight='bold')

        # Force y-axis to use integer values
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))

        self._setup_plot_style(ax)
        plt.tight_layout()

        # Save plot
        save_path = plots_dir / "deployment_overhead_breakdown.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()

        self.logger.info(f"Saved deployment overhead breakdown to: {save_path}")
    def _plot_invocation_makespan_overhead(self, plots_dir: Path):
        """Create stacked bar chart for invocation makespan vs overhead (in seconds)"""
        invocation_data = self.results.get("invocation_concurrency", {})

        if not invocation_data:
            self.logger.warning("No invocation concurrency data found for plotting")
            return

        # Process data (convert to seconds)
        concurrency_levels = []
        makespan_values = []  # True makespan
        overhead_values = []  # Average overhead per invocation

        for level_str, runs in sorted(invocation_data.items(), key=lambda x: int(x[0])):
            if not runs:
                continue

            level = int(level_str)
            concurrency_levels.append(level)

            # Calculate averages across runs (in seconds)
            all_makespans = []
            all_overheads = []

            for run in runs:
                if 'avg_makespan_ms' in run and 'avg_overhead_ms' in run:
                    makespan_ms = run['avg_makespan_ms']
                    overhead_ms = run['avg_overhead_ms']

                    all_makespans.append(makespan_ms / 1000.0)
                    all_overheads.append(overhead_ms / 1000.0)

            avg_makespan = statistics.mean(all_makespans) if all_makespans else 0
            avg_overhead = statistics.mean(all_overheads) if all_overheads else 0

            makespan_values.append(avg_makespan)
            overhead_values.append(avg_overhead)

        if not concurrency_levels:
            self.logger.warning("No valid invocation data to plot")
            return

        # Create figure
        fig, ax = plt.subplots(figsize=(25, 19))

        # Create stacked bars
        x = np.arange(len(concurrency_levels))
        width = 0.5

        # Makespan (bottom) in primary color - updated label
        makespan_bars = ax.bar(x, makespan_values, width,
                               label='Function Execution Makespan (each function runtime - 10s)',
                               color=self.color_palette['primary'], edgecolor='black', linewidth=2)

        # Overhead (top) in secondary color
        overhead_bars = ax.bar(x, overhead_values, width, bottom=makespan_values,
                               label='Average Overhead (Manager + Network)',
                               color=self.color_palette['secondary'], edgecolor='black', linewidth=2)

        # Add value labels
        for i, (makespan, overhead) in enumerate(zip(makespan_values, overhead_values)):
            # Makespan label
            if makespan > 0:
                ax.text(i, makespan / 2, f'{makespan:.3f}s',
                        ha='center', va='center', color='white',
                        fontweight=PLOT_FONTS['bar']['data_label_weight'],
                        fontsize=PLOT_FONTS['bar']['data_label_size'])

            # Overhead label
            if overhead > 0:
                ax.text(i, makespan + overhead / 2, f'{overhead:.3f}s',
                        ha='center', va='center', color='white',
                        fontweight=PLOT_FONTS['bar']['data_label_weight'],
                        fontsize=PLOT_FONTS['bar']['data_label_size'])

            # Total time annotation
            total = makespan + overhead
            ax.text(i, total + 0.005, f'Total: {total:.3f}s',
                    ha='center', fontsize=PLOT_FONTS['bar']['data_label_size'],
                    fontweight='bold')

        # Customize plot
        ax.set_xlabel('Concurrent Invocations', fontsize=PLOT_FONTS['bar']['axis_label_size'],
                      labelpad=PLOT_FONTS['bar']['axis_label_pad'])
        ax.set_ylabel('Time (seconds)', fontsize=PLOT_FONTS['bar']['axis_label_size'],
                      labelpad=PLOT_FONTS['bar']['axis_label_pad'])
        ax.set_title('Invocation Latency: Function Execution Makespan vs Average Overhead',
                     fontsize=PLOT_FONTS['bar']['title_size'],
                     fontweight=PLOT_FONTS['bar']['title_weight'], pad=30)
        ax.set_xticks(x)
        ax.set_xticklabels([str(level) for level in concurrency_levels],
                           fontsize=PLOT_FONTS['invocation_bar']['x_tick_size'])
        ax.tick_params(axis='y', labelsize=PLOT_FONTS['bar']['axis_tick_size'])

        # Add legend at top left (same as deployment plot)
        ax.legend(loc='upper left', fontsize=PLOT_FONTS['bar']['legend_size'])

        self._setup_plot_style(ax)
        plt.tight_layout()

        # Save plot
        save_path = plots_dir / "invocation_makespan_overhead.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()

        self.logger.info(f"Saved invocation overhead plot to: {save_path}")

    def _plot_deployment_concurrency_boxplots(self, plots_dir: Path):
        """Create box plots for deployment concurrency (in seconds)"""
        concurrency_data = self.results.get("deployment_concurrency", {})

        if not concurrency_data:
            self.logger.warning("No deployment concurrency data found for plotting")
            return

        # Prepare data for box plots (convert to seconds)
        plot_data = []
        labels = []

        for level_str in sorted(concurrency_data.keys(), key=int):
            runs = concurrency_data[level_str]
            if not runs:
                continue

            # Collect all individual deployment times across runs (in seconds)
            all_times = []
            for run in runs:
                if 'individual_times_ms' in run:
                    all_times.extend([t / 1000.0 for t in run['individual_times_ms']])

            if all_times:
                plot_data.append(all_times)
                labels.append(level_str)

        if not plot_data:
            self.logger.warning("No valid concurrency data to plot")
            return

        # Create figure
        fig, ax = plt.subplots(figsize=(22, 12))

        # Create box plots
        bp = ax.boxplot(plot_data, labels=labels, patch_artist=True,
                        showmeans=True, meanprops=dict(marker='D', markerfacecolor=self.color_palette['tertiary'],
                                                       markersize=14, markeredgecolor='black'),
                        boxprops=dict(linewidth=2),
                        whiskerprops=dict(linewidth=2),
                        capprops=dict(linewidth=2),
                        medianprops=dict(linewidth=3))

        # Color the boxes
        for i, patch in enumerate(bp['boxes']):
            patch.set_facecolor(self.color_palette['primary'])
            patch.set_alpha(0.7)

        # Customize plot
        ax.set_xlabel('Number of Concurrent Deployments', fontsize=PLOT_FONTS['box']['axis_label_size'],
                      labelpad=PLOT_FONTS['box']['axis_label_pad'])
        ax.set_ylabel('Deployment Time (seconds)', fontsize=PLOT_FONTS['box']['axis_label_size'],
                      labelpad=PLOT_FONTS['box']['axis_label_pad'])
        ax.set_title('End-to-End Deployment Time by Concurrency Level',
                     fontsize=PLOT_FONTS['box']['title_size'],
                     fontweight=PLOT_FONTS['box']['title_weight'], pad=30)
        ax.tick_params(axis='x', labelsize=PLOT_FONTS['box']['x_tick_size'])
        ax.tick_params(axis='y', labelsize=PLOT_FONTS['box']['axis_tick_size'])

        # Add statistics annotations positioned above the highest whisker
        for i, data in enumerate(plot_data):
            if data:
                mean = statistics.mean(data)
                median = statistics.median(data)

                # Find the highest point in the boxplot (upper whisker or outlier)
                q75 = np.percentile(data, 75)
                q25 = np.percentile(data, 25)
                iqr = q75 - q25
                upper_whisker = min(max(data), q75 + 1.5 * iqr)

                # Get all outliers above upper whisker
                outliers_above = [d for d in data if d > upper_whisker]
                if outliers_above:
                    highest_point = max(outliers_above)
                else:
                    highest_point = upper_whisker

                # Position text above the highest point with some padding
                y_pos = highest_point + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.05

                ax.text(i + 1, y_pos, f'Mean: {mean:.3f}s\nMedian: {median:.3f}s',
                        ha='center', fontsize=PLOT_FONTS['box']['stats_annotation_size'],
                        bbox=dict(boxstyle="round,pad=0.5", facecolor='white', alpha=0.9))

        self._setup_plot_style(ax)
        plt.tight_layout()

        # Save plot
        save_path = plots_dir / "deployment_concurrency_boxplots.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()

        self.logger.info(f"Saved deployment concurrency boxplots to: {save_path}")

    def _plot_deployment_packaging_concurrency(self, plots_dir: Path):
        """Create box plots for deployment packaging overhead only (in seconds)"""
        concurrency_data = self.results.get("deployment_concurrency", {})

        if not concurrency_data:
            self.logger.warning("No deployment concurrency data found for plotting")
            return

        # Prepare data for box plots (convert to seconds)
        plot_data = []
        labels = []

        for level_str in sorted(concurrency_data.keys(), key=int):
            runs = concurrency_data[level_str]
            if not runs:
                continue

            # Collect all individual packaging times across runs (convert to seconds)
            all_packaging = []
            for run in runs:
                if 'individual_packaging_ms' in run:
                    all_packaging.extend([t / 1000.0 for t in run['individual_packaging_ms']])

            if all_packaging:
                plot_data.append(all_packaging)
                labels.append(level_str)

        if not plot_data:
            self.logger.warning("No valid packaging data to plot")
            return

        # Create figure
        fig, ax = plt.subplots(figsize=(22, 12))

        # Create box plots
        bp = ax.boxplot(plot_data, labels=labels, patch_artist=True,
                        showmeans=True, meanprops=dict(marker='D', markerfacecolor=self.color_palette['tertiary'],
                                                       markersize=14, markeredgecolor='black'),
                        boxprops=dict(linewidth=2),
                        whiskerprops=dict(linewidth=2),
                        capprops=dict(linewidth=2),
                        medianprops=dict(linewidth=3))

        # Color the boxes
        for i, patch in enumerate(bp['boxes']):
            patch.set_facecolor(self.color_palette['secondary'])
            patch.set_alpha(0.7)

        # Customize plot (updated to seconds)
        ax.set_xlabel('Number of Concurrent Deployments', fontsize=PLOT_FONTS['box']['axis_label_size'],
                      labelpad=PLOT_FONTS['box']['axis_label_pad'])
        ax.set_ylabel('Package Creation Time (seconds)', fontsize=PLOT_FONTS['box']['axis_label_size'],
                      labelpad=PLOT_FONTS['box']['axis_label_pad'])
        ax.set_title('Package Creation Time by Concurrency Level',
                     fontsize=PLOT_FONTS['box']['title_size'],
                     fontweight=PLOT_FONTS['box']['title_weight'], pad=30)
        ax.tick_params(axis='x', labelsize=PLOT_FONTS['box']['x_tick_size'])
        ax.tick_params(axis='y', labelsize=PLOT_FONTS['box']['axis_tick_size'])

        # Add statistics annotations positioned above the highest whisker
        for i, data in enumerate(plot_data):
            if data:
                mean = statistics.mean(data)
                median = statistics.median(data)

                # Find the highest point in the boxplot (upper whisker or outlier)
                q75 = np.percentile(data, 75)
                q25 = np.percentile(data, 25)
                iqr = q75 - q25
                upper_whisker = min(max(data), q75 + 1.5 * iqr)

                # Get all outliers above upper whisker
                outliers_above = [d for d in data if d > upper_whisker]
                if outliers_above:
                    highest_point = max(outliers_above)
                else:
                    highest_point = upper_whisker

                # Position text above the highest point with some padding
                y_pos = highest_point + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.05

                ax.text(i + 1, y_pos, f'Mean: {mean:.3f}s\nMedian: {median:.3f}s',
                        ha='center', fontsize=PLOT_FONTS['box']['stats_annotation_size'],
                        bbox=dict(boxstyle="round,pad=0.5", facecolor='white', alpha=0.9))

        self._setup_plot_style(ax)
        plt.tight_layout()

        # Save plot
        save_path = plots_dir / "deployment_packaging_concurrency.png"
        plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()

        self.logger.info(f"Saved deployment packaging concurrency plot to: {save_path}")

    def _create_summary_text(self, plots_dir: Path):
        """Create text summary for manager footprint and provisioning times (with times in seconds)"""
        summary_lines = ["FaaS Manager Benchmark Summary", "=" * 50, ""]

        # Manager Footprint
        footprint = self.results.get("manager_footprint", {})
        if footprint:
            summary_lines.extend([
                "MANAGER FOOTPRINT:",
                f"  Average CPU Utilization: {footprint.get('avg_cpu_percent', 0):.2f}%",
                f"  Average Memory Usage: {footprint.get('avg_memory_mb', 0):.2f} MB",
                "",
            ])

        # Provisioning Times (convert to seconds)
        provisioning = self.results.get("provisioning_times", {})
        if provisioning:
            summary_lines.extend([
                "RESOURCE PROVISIONING TIMES:",
                f"  Total: {provisioning.get('total_provisioning_ms', 0) / 1000.0:.3f} seconds",
                f"  IAM Role Creation: {provisioning.get('iam_role_creation_ms', 0) / 1000.0:.3f} seconds",
                f"  IAM Role Propagation: {provisioning.get('iam_role_propagation_ms', 0) / 1000.0:.3f} seconds",
                f"  IAM Policy Attachment: {provisioning.get('iam_policy_attachment_ms', 0) / 1000.0:.3f} seconds",
            ])
            if self.enable_containers:
                summary_lines.append(
                    f"  ECR Repository Creation: {provisioning.get('ecr_repository_creation_ms', 0) / 1000.0:.3f} seconds")
            summary_lines.extend([
                f"  IAM Min/Max: {provisioning.get('iam_min_ms', 0) / 1000.0:.3f} / {provisioning.get('iam_max_ms', 0) / 1000.0:.3f} seconds",
            ])
            if self.enable_containers:
                summary_lines.append(
                    f"  ECR Min/Max: {provisioning.get('ecr_min_ms', 0) / 1000.0:.3f} / {provisioning.get('ecr_max_ms', 0) / 1000.0:.3f} seconds")
            summary_lines.append("")

        # Save summary
        summary_path = plots_dir / "benchmark_summary.txt"
        with open(summary_path, 'w') as f:
            f.write("\n".join(summary_lines))

        # Also print to console
        print("\n" + "\n".join(summary_lines))
        self.logger.info(f"Summary saved to: {summary_path}")


def generate_plots_from_results(results_file: str, output_dir: str = None):
    """
    Generate plots from an existing results.json file without running benchmarks

    Args:
        results_file: Path to the results.json file
        output_dir: Optional output directory for plots (defaults to same dir as results)
    """
    # Load results
    results_path = Path(results_file)
    if not results_path.exists():
        print(f"Error: Results file not found: {results_file}")
        return

    with open(results_path, 'r') as f:
        results = json.load(f)

    # Determine output directory
    if output_dir:
        output_path = Path(output_dir)
    else:
        output_path = results_path.parent

    # Create a minimal benchmark suite just for plotting
    suite = BenchmarkSuite.__new__(BenchmarkSuite)  # Create without __init__
    suite.results = results
    suite.output_dir = output_path
    suite.enable_containers = results.get("config", {}).get("enable_containers", True)

    # Setup logger
    suite.logger = logging.getLogger("plot_generator")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )

    suite.logger.info(f"Generating plots from: {results_file}")
    suite.logger.info(f"Output directory: {output_path}")

    # Generate plots
    suite._generate_plots()

    suite.logger.info("Plot generation complete!")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="FaaS Manager Comprehensive Benchmark Suite")
    parser.add_argument("-c", "--credentials", help="Path to credentials JSON file")
    parser.add_argument("--no-containers", action="store_true", help="Disable container tests")
    parser.add_argument("--plot-only", help="Generate plots from existing results.json file")
    parser.add_argument("--plot-output", help="Output directory for plots (used with --plot-only)")
    args = parser.parse_args()

    # If plot-only mode, just generate plots and exit
    if args.plot_only:
        generate_plots_from_results(args.plot_only, args.plot_output)
        return

    # Otherwise run the full benchmark
    if not args.credentials:
        parser.error("--credentials is required when running benchmarks")

    # Increase file descriptor limit
    if sys.platform != "win32":
        try:
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            if soft < 4096:
                resource.setrlimit(resource.RLIMIT_NOFILE, (4096, hard))
                logging.info(f"Increased open file limit to 4096")
        except Exception as e:
            logging.warning(f"Could not increase open file limit: {e}")

    suite = BenchmarkSuite(
        credentials_path=args.credentials,
        enable_containers=not args.no_containers
    )

    def signal_handler(sig, frame):
        print("\nInterrupt received, initiating teardown...")
        suite.teardown()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        suite.setup()
        suite.run_all_tests()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        traceback.print_exc()
    finally:
        suite.teardown()


if __name__ == "__main__":
    main()