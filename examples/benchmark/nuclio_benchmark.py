#!/usr/bin/env python3
"""
Nuclio SDK Benchmark - Uses Nuclio Python SDK with NodePort invocation
Deploys through the Dashboard API (uses Kaniko) and invokes via NodePort
"""

import os
import sys
import time
import json
import argparse
import subprocess
import logging
import statistics
import requests
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import uuid
from dataclasses import dataclass, asdict

# Install nuclio SDK if needed
try:
    import nuclio
    import nuclio.triggers
except ImportError:
    print("Installing nuclio SDK...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "nuclio-jupyter"])
    import nuclio
    import nuclio.triggers

# Configuration
BENCHMARK_OUTPUT_DIR = Path(__file__).parent / "nuclio_benchmark_results"
NUCLIO_NAMESPACE = "nuclio"
NUCLIO_DASHBOARD_URL = "http://localhost:8070"

# Get AWS account info for ECR
AWS_ACCOUNT = subprocess.check_output(
    "aws sts get-caller-identity --query Account --output text",
    shell=True
).decode().strip()
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ECR_REGISTRY = f"{AWS_ACCOUNT}.dkr.ecr.{AWS_REGION}.amazonaws.com"

# Test parameters
DEPLOYMENT_CONCURRENCY_LEVELS = [1, 2, 4, 8, 16]
INVOCATION_CONCURRENCY_LEVELS = [10, 100, 500, 1000]
SCALING_LEVELS = [1, 5, 10, 20, 50]
EXPERIMENT_REPEATS = 3


@dataclass
class NodeConfig:
    """EKS node configuration"""
    instance_type: str
    vcpus: int
    memory_gb: int
    count: int
    config_name: str

    def get_total_vcpus(self) -> int:
        return self.vcpus * self.count


# EKS configurations
EKS_NODE_CONFIGS = [
    NodeConfig("t3.medium", 2, 4, 2, "2-node-4vcpu"),
    NodeConfig("t3.xlarge", 4, 16, 2, "2-node-8vcpu"),
    NodeConfig("t3.2xlarge", 8, 32, 2, "2-node-16vcpu")
]


class NuclioSDKBenchmark:
    """Nuclio benchmark using Python SDK"""

    def __init__(self, node_config: NodeConfig):
        self.node_config = node_config

        # Setup directories
        self.output_dir = BENCHMARK_OUTPUT_DIR / node_config.config_name / datetime.now().strftime('%Y%m%d_%H%M%S')
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self.logger = self._setup_logging()

        # Tracking
        self.deployed_functions = []
        self.port_forward_proc = None

        # Get node IP for direct invocation
        self.node_ip = self._get_node_ip()

        # Results
        self.results = {
            "config": asdict(node_config),
            "timestamp": datetime.now().isoformat(),
            "ecr_registry": ECR_REGISTRY,
            "node_ip": self.node_ip,
            "tests": {}
        }

    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger("nuclio_sdk_benchmark")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()

        fh = logging.FileHandler(self.output_dir / 'benchmark.log')
        ch = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger

    def _get_node_ip(self) -> str:
        """Get the IP of one of the nodes"""
        cmd = "kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type==\"InternalIP\")].address}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return "localhost"  # Fallback

    def setup(self):
        """Setup port forwarding to dashboard and push base images"""
        self.logger.info("Setting up port forward to Nuclio dashboard...")

        # Check if already accessible
        try:
            response = requests.get(f"{NUCLIO_DASHBOARD_URL}/api/functions", timeout=2)
            if response.status_code == 200:
                self.logger.info("Dashboard already accessible")
        except:
            # Setup port forward
            self.port_forward_proc = subprocess.Popen(
                f"kubectl port-forward -n {NUCLIO_NAMESPACE} svc/nuclio-dashboard 8070:8070",
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            time.sleep(5)

        # Create ECR repository for function images
        self.logger.info("Creating ECR repository for function images...")
        func_repo = "nuclio-functions"
        create_cmd = f"aws ecr create-repository --repository-name {func_repo} --region {AWS_REGION} 2>/dev/null || true"
        subprocess.run(create_cmd, shell=True)

        # Push required Nuclio base images to ECR
        self.logger.info("Ensuring Nuclio base images are in ECR...")
        self._push_nuclio_base_images()

    def _push_nuclio_base_images(self):
        """Push Nuclio base images to ECR"""
        # Login to ECR
        login_cmd = f"aws ecr get-login-password --region {AWS_REGION} | docker login --username AWS --password-stdin {ECR_REGISTRY}"
        subprocess.run(login_cmd, shell=True, check=True)

        # Required base images for Python functions
        base_images = [
            ("nuclio/handler-builder-python-onbuild:1.15.2-amd64", "nuclio/handler-builder-python-onbuild"),
            ("nuclio/processor-python:1.15.2-amd64", "nuclio/processor-python"),
            ("quay.io/nuclio/handler-builder-python-onbuild:1.15.2-amd64", "nuclio/handler-builder-python-onbuild"),
            ("quay.io/nuclio/processor-python:1.15.2-amd64", "nuclio/processor-python")
        ]

        for source_image, repo_name in base_images:
            # Create ECR repository
            create_cmd = f"aws ecr create-repository --repository-name {repo_name} --region {AWS_REGION} 2>/dev/null || true"
            subprocess.run(create_cmd, shell=True)

            # Check if image already exists in ECR
            tag = source_image.split(":")[-1]
            check_cmd = f"aws ecr describe-images --repository-name {repo_name} --image-ids imageTag={tag} --region {AWS_REGION} 2>/dev/null"
            result = subprocess.run(check_cmd, shell=True, capture_output=True)

            if result.returncode == 0:
                self.logger.info(f"Image {repo_name}:{tag} already exists in ECR")
            else:
                try:
                    self.logger.info(f"Pulling {source_image}...")
                    pull_cmd = f"docker pull {source_image}"
                    result = subprocess.run(pull_cmd, shell=True, capture_output=True, text=True)

                    if result.returncode != 0:
                        # Try without the specific registry prefix
                        alt_image = source_image.replace("quay.io/", "")
                        self.logger.info(f"Trying alternate image: {alt_image}")
                        subprocess.run(f"docker pull {alt_image}", shell=True, check=True)
                        source_image = alt_image

                    # Tag for ECR
                    ecr_image = f"{ECR_REGISTRY}/{repo_name}:{tag}"
                    tag_cmd = f"docker tag {source_image} {ecr_image}"
                    subprocess.run(tag_cmd, shell=True, check=True)

                    # Push to ECR
                    push_cmd = f"docker push {ecr_image}"
                    subprocess.run(push_cmd, shell=True, check=True)

                    self.logger.info(f"Successfully pushed {ecr_image}")
                except Exception as e:
                    self.logger.warning(f"Could not push {source_image}: {e}")

    def run_all_tests(self):
        """Run all benchmark tests"""
        self.logger.info(f"Starting benchmark for {self.node_config.config_name}")

        # Test 1: Single deployment
        self.logger.info("=== Test 1: Single Deployment Performance ===")
        self._test_single_deployment()

        # Test 2: Concurrent deployments
        self.logger.info("=== Test 2: Concurrent Deployments ===")
        self._test_concurrent_deployments()

        # Test 3: Invocation performance
        self.logger.info("=== Test 3: Invocation Performance ===")
        self._test_invocations()

        # Test 4: Scaling
        self.logger.info("=== Test 4: Scaling Test ===")
        self._test_scaling()

        # Save results
        with open(self.output_dir / "results.json", "w") as f:
            json.dump(self.results, f, indent=2)

        self.logger.info(f"Results saved to {self.output_dir}")

    def _deploy_function(self, name: str) -> Tuple[float, int]:
        """Deploy a function and return (deployment_time, node_port)"""
        start_time = time.time()

        # Function code
        function_code = '''
import time
import json
import random

def handler(context, event):
    start_time = time.time()

    # Parse body
    body = {}
    if hasattr(event, 'body'):
        if isinstance(event.body, bytes):
            body = json.loads(event.body.decode('utf-8'))
        elif isinstance(event.body, str):
            body = json.loads(event.body)
        elif isinstance(event.body, dict):
            body = event.body

    # Simulate work
    work_duration = body.get('work_duration', 0.01)
    time.sleep(work_duration)

    # Return response
    return {
        'status': 'ok',
        'function_name': context.name,
        'start_time': start_time,
        'end_time': time.time(),
        'duration': time.time() - start_time,
        'random': random.random()
    }
'''

        try:
            # Configure the function using the simpler API
            spec = nuclio.ConfigSpec() \
                .set_config('spec.build.registry', ECR_REGISTRY) \
                .set_config('spec.minReplicas', 0) \
                .set_config('spec.maxReplicas', 10) \
                .set_config('spec.resources.requests.cpu', '25m') \
                .set_config('spec.resources.requests.memory', '128Mi') \
                .set_config('spec.resources.limits.cpu', '1') \
                .set_config('spec.resources.limits.memory', '512Mi') \
                .set_config('spec.triggers.http.attributes.serviceType', 'NodePort')

            # Add basic HTTP trigger
            spec.add_trigger('http', nuclio.triggers.HttpTrigger())

            # Deploy the function
            self.logger.info(f"Deploying {name}...")
            addr = nuclio.deploy_code(
                code=function_code,
                name=name,
                project='benchmark',
                verbose=True,
                spec=spec,
                dashboard_url=NUCLIO_DASHBOARD_URL
            )

            deployment_time = time.time() - start_time
            self.deployed_functions.append(name)

            # Wait a bit for the service to be fully ready
            time.sleep(5)

            # Get NodePort
            node_port = self._get_function_nodeport(name)

            self.logger.info(f"Deployed {name} in {deployment_time:.2f}s (NodePort: {node_port}, Address: {addr})")

            return deployment_time, node_port

        except Exception as e:
            self.logger.error(f"Failed to deploy {name}: {e}")
            raise

    def _get_function_nodeport(self, name: str) -> int:
        """Get the NodePort for a function"""
        cmd = f"kubectl get svc nuclio-{name} -n {NUCLIO_NAMESPACE} -o jsonpath='{{.spec.ports[?(@.name==\"http\")].nodePort}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())

        # Fallback: try to get any port
        cmd = f"kubectl get svc nuclio-{name} -n {NUCLIO_NAMESPACE} -o jsonpath='{{.spec.ports[0].nodePort}}'"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            return int(result.stdout.strip())

        raise Exception(f"Could not find NodePort for function {name}")

    def _invoke_function_nodeport(self, name: str, node_port: int, payload: Dict = None) -> float:
        """Invoke a function via NodePort and return invocation time"""
        start_time = time.time()

        # Try direct node access first, then localhost with port forward
        urls = [
            f"http://{self.node_ip}:{node_port}",
            f"http://localhost:{node_port}"
        ]

        last_error = None
        for url in urls:
            try:
                response = requests.post(
                    url,
                    json=payload or {},
                    timeout=30
                )

                if response.status_code == 200:
                    invocation_time = time.time() - start_time
                    return invocation_time

            except Exception as e:
                last_error = e
                continue

        raise Exception(f"Failed to invoke function: {last_error}")

    def _test_single_deployment(self):
        """Test single deployment performance"""
        times = []

        for i in range(EXPERIMENT_REPEATS):
            func_name = f"single-deploy-{i}-{uuid.uuid4().hex[:8]}"
            try:
                deployment_time, _ = self._deploy_function(func_name)
                times.append(deployment_time)
            except Exception as e:
                self.logger.error(f"Deployment {i} failed: {e}")

        if times:
            self.results["tests"]["single_deployment"] = {
                "times_seconds": times,
                "avg_seconds": statistics.mean(times),
                "min_seconds": min(times),
                "max_seconds": max(times)
            }

    def _test_concurrent_deployments(self):
        """Test concurrent deployments"""
        results = {}

        for level in DEPLOYMENT_CONCURRENCY_LEVELS:
            if level > 8 and self.node_config.get_total_vcpus() < 8:
                continue

            self.logger.info(f"Testing {level} concurrent deployments")

            all_times = []
            for repeat in range(EXPERIMENT_REPEATS):
                times = []

                with ThreadPoolExecutor(max_workers=level) as executor:
                    futures = []

                    for i in range(level):
                        func_name = f"concurrent-{level}-{repeat}-{i}-{uuid.uuid4().hex[:6]}"
                        future = executor.submit(self._deploy_function, func_name)
                        futures.append(future)

                    for future in as_completed(futures):
                        try:
                            deployment_time, _ = future.result()
                            times.append(deployment_time)
                        except Exception as e:
                            self.logger.error(f"Concurrent deployment failed: {e}")

                if times:
                    all_times.extend(times)

                time.sleep(5)

            if all_times:
                results[str(level)] = {
                    "times_seconds": all_times,
                    "avg_seconds": statistics.mean(all_times),
                    "success_rate": len(all_times) / (level * EXPERIMENT_REPEATS) * 100
                }

        self.results["tests"]["concurrent_deployments"] = results

    def _test_invocations(self):
        """Test invocation performance"""
        # Deploy a test function
        func_name = f"invoke-test-{uuid.uuid4().hex[:8]}"
        _, node_port = self._deploy_function(func_name)
        time.sleep(5)

        # Also setup port forward for this specific function for localhost access
        pf_proc = subprocess.Popen(
            f"kubectl port-forward -n {NUCLIO_NAMESPACE} svc/nuclio-{func_name} {node_port}:{node_port}",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(2)

        results = {}

        for level in INVOCATION_CONCURRENCY_LEVELS:
            self.logger.info(f"Testing {level} concurrent invocations")

            all_times = []
            for repeat in range(EXPERIMENT_REPEATS):
                times = []
                makespan_start = time.time()

                with ThreadPoolExecutor(max_workers=min(level, 200)) as executor:
                    futures = []

                    for i in range(level):
                        payload = {"work_duration": 0.01, "request_id": i}
                        future = executor.submit(self._invoke_function_nodeport, func_name, node_port, payload)
                        futures.append(future)

                    for future in as_completed(futures):
                        try:
                            inv_time = future.result()
                            times.append(inv_time)
                        except Exception as e:
                            self.logger.debug(f"Invocation failed: {e}")

                makespan = time.time() - makespan_start

                if times:
                    all_times.extend(times)
                    self.logger.info(
                        f"  Repeat {repeat + 1}: {len(times)}/{level} successful, makespan: {makespan:.2f}s")

                time.sleep(2)

            if all_times:
                results[str(level)] = {
                    "times_seconds": all_times,
                    "avg_seconds": statistics.mean(all_times),
                    "min_seconds": min(all_times),
                    "max_seconds": max(all_times),
                    "success_rate": len(all_times) / (level * EXPERIMENT_REPEATS) * 100,
                    "requests_per_second": len(all_times) / sum(all_times) if sum(all_times) > 0 else 0
                }

        # Cleanup port forward
        pf_proc.terminate()

        self.results["tests"]["invocations"] = results

    def _test_scaling(self):
        """Test scaling performance"""
        # Deploy test function
        func_name = f"scale-test-{uuid.uuid4().hex[:8]}"
        _, node_port = self._deploy_function(func_name)
        time.sleep(5)

        results = {}

        for replicas in SCALING_LEVELS:
            if replicas > self.node_config.get_total_vcpus() * 5:
                continue

            self.logger.info(f"Scaling to {replicas} replicas")

            # Scale the function
            scale_cmd = f"kubectl scale deploy nuclio-{func_name} -n {NUCLIO_NAMESPACE} --replicas={replicas}"
            subprocess.run(scale_cmd, shell=True, check=True)

            # Wait for scaling
            time.sleep(20)

            # Test throughput with concurrent requests
            test_duration = 30  # seconds
            successful = 0
            failed = 0

            start_time = time.time()

            with ThreadPoolExecutor(max_workers=100) as executor:
                futures = []
                request_id = 0

                while time.time() - start_time < test_duration:
                    payload = {"work_duration": 0.05, "request_id": request_id}
                    future = executor.submit(self._invoke_function_nodeport, func_name, node_port, payload)
                    futures.append(future)
                    request_id += 1
                    time.sleep(0.01)  # 100 requests per second target

                # Collect results
                for future in futures:
                    try:
                        future.result(timeout=5)
                        successful += 1
                    except:
                        failed += 1

            actual_duration = time.time() - start_time
            throughput = successful / actual_duration if actual_duration > 0 else 0

            results[str(replicas)] = {
                "replicas": replicas,
                "duration_seconds": actual_duration,
                "successful_requests": successful,
                "failed_requests": failed,
                "throughput_per_second": throughput,
                "success_rate": successful / (successful + failed) * 100 if (successful + failed) > 0 else 0
            }

            self.logger.info(
                f"  Throughput: {throughput:.2f} req/s, Success rate: {successful / (successful + failed) * 100:.1f}%")

        self.results["tests"]["scaling"] = results

    def cleanup(self):
        """Clean up deployed functions"""
        self.logger.info("Cleaning up...")

        for func in self.deployed_functions:
            try:
                # Use the SDK to delete
                self.logger.info(f"Deleting {func}...")
                # The delete_function API might need the full address
                nuclio.delete_function(func, dashboard_url=NUCLIO_DASHBOARD_URL)
                self.logger.info(f"Deleted {func}")
            except Exception as e:
                # Fallback to kubectl
                try:
                    cmd = f"kubectl delete nucliofunctions.nuclio.io {func} -n {NUCLIO_NAMESPACE}"
                    subprocess.run(cmd, shell=True, check=True)
                    self.logger.info(f"Deleted {func} via kubectl")
                except:
                    self.logger.debug(f"Failed to delete {func}: {e}")

        if self.port_forward_proc:
            self.port_forward_proc.terminate()


def main():
    parser = argparse.ArgumentParser(description="Nuclio SDK Benchmark")
    parser.add_argument("--config", choices=["2-node-4vcpu", "2-node-8vcpu", "2-node-16vcpu"],
                        help="Node configuration to test")
    args = parser.parse_args()

    # Check prerequisites
    # 1. kubectl configured
    result = subprocess.run(["kubectl", "get", "nodes"], capture_output=True)
    if result.returncode != 0:
        print("Error: kubectl not configured or cluster not accessible")
        sys.exit(1)

    # 2. Nuclio installed
    result = subprocess.run(["kubectl", "get", "pods", "-n", "nuclio"], capture_output=True)
    if result.returncode != 0:
        print("Error: Nuclio namespace not found")
        sys.exit(1)

    # 3. AWS credentials for ECR
    if not AWS_ACCOUNT:
        print("Error: AWS credentials not configured")
        sys.exit(1)

    print(f"Using ECR registry: {ECR_REGISTRY}")

    # Select configurations
    if args.config:
        configs = [c for c in EKS_NODE_CONFIGS if c.config_name == args.config]
    else:
        configs = EKS_NODE_CONFIGS[:1]  # Just run first config by default

    for config in configs:
        print(f"\n{'=' * 60}")
        print(f"Running benchmark for: {config.config_name}")
        print(f"Nodes: {config.count} x {config.instance_type} ({config.get_total_vcpus()} vCPUs total)")
        print(f"{'=' * 60}\n")

        benchmark = NuclioSDKBenchmark(config)

        try:
            benchmark.setup()
            benchmark.run_all_tests()
        except Exception as e:
            print(f"ERROR: {e}")
            import traceback
            traceback.print_exc()
        finally:
            benchmark.cleanup()

    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()