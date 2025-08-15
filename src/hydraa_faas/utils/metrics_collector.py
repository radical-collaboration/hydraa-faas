# -*- coding: utf-8 -*-
"""Metrics collector for FaaS manager benchmarking.

This module provides a thread-safe, high-performance metrics collector for
analyzing FaaS platform behavior. It is designed to be both comprehensive and
efficient, using pandas for fast statistical analysis and offering features
like periodic saving to ensure data integrity.

Key Features:
- Simplified state management for tracking deployments and invocations.
- Direct timestamping, removing the need for a separate timer class.
- Efficient statistical analysis using the pandas library.
- Optional periodic saving to prevent data loss during long runs.
- Continuous resource monitoring of the FaaS manager process.
- Automatic sample aggregation to prevent memory growth.
- Lock-free buffering for high-performance metrics collection.

Example:
    To use the collector in a benchmark::

        # Initialize with periodic saving every 60 seconds
        metrics = MetricsCollector(save_interval_seconds=60)

        # Start tracking a deployment
        dep_id = metrics.start_deployment("my-function", "zip")
        # ... perform deployment ...
        metrics.complete_deployment(dep_id, status="success")

        # Start tracking an invocation
        inv_id = metrics.start_invocation("my-function", payload_size=1024)
        # ... perform invocation ...
        metrics.complete_invocation(inv_id, status="success")

        # At the end of the benchmark
        metrics.save_results()
        metrics.shutdown()

"""

import json
import threading
import time
import itertools

from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import psutil


# A powerful data analysis library.
# Install via: pip install pandas


@dataclass
class DeploymentMetrics:
    """Metrics for a single deployment operation."""
    deployment_id: str
    function_name: str
    deployment_type: str
    status: str = "in_progress"

    # Timestamps for key phases
    start_time: float = 0.0
    preparation_end_time: float = 0.0
    platform_api_start_time: float = 0.0
    end_time: float = 0.0

    # Calculated durations
    total_time_ms: float = 0.0
    manager_overhead_ms: float = 0.0
    packaging_overhead_ms: float = 0.0
    platform_api_call_ms: float = 0.0

    # Additional info
    package_size_bytes: Optional[int] = None
    image_size_mb: Optional[float] = None
    error_message: Optional[str] = None


@dataclass
class InvocationMetrics:
    """Metrics for a single function invocation."""
    invocation_id: str
    function_name: str
    status: str = "in_progress"

    # Timestamps for key phases
    start_time: float = 0.0
    provider_selection_end_time: float = 0.0
    pre_invoke_end_time: float = 0.0
    platform_api_end_time: float = 0.0
    end_time: float = 0.0

    # Calculated durations
    total_time_ms: float = 0.0
    manager_overhead_ms: float = 0.0
    makespan_ms: float = 0.0  # Actual function execution time
    network_delay_ms: float = 0.0

    # Platform-reported metrics
    cold_start: bool = False
    memory_used_mb: Optional[int] = None
    payload_size_bytes: int = 0
    error_message: Optional[str] = None


@dataclass
class ResourceSample:
    """A single sample of system resource usage."""
    timestamp: float
    cpu_percent: float
    memory_mb: float


@dataclass
class MetricsUpdate:
    """Represents a buffered metrics update."""
    update_type: str
    data: Dict[str, Any]
    timestamp: float


class MetricsCollector:
    """A thread-safe, high-performance collector for FaaS metrics.

    This class provides a centralized way to track deployment and invocation
    performance, as well as the resource footprint of the manager itself.

    Attributes:
        output_dir: The directory where metrics files will be saved.
    """

    def __init__(self, output_dir: str = "./metrics", save_interval_seconds: Optional[int] = None,
                 enable_collection: bool = True):
        """Initializes the MetricsCollector.

        Args:
            output_dir: The directory to save metrics files.
            save_interval_seconds: If set, metrics will be saved to disk
                                   periodically at this interval.
            enable_collection: If False, all metric collection is disabled.
        """
        self.enable_collection = enable_collection

        if not self.enable_collection:
            # Create minimal structure to avoid errors
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(exist_ok=True)
            self._deployments = {}
            self._invocations = {}
            return

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self._deployments: Dict[str, DeploymentMetrics] = {}
        self._invocations: Dict[str, InvocationMetrics] = {}
        self._deployments_lock = threading.RLock()
        self._invocations_lock = threading.RLock()

        self._resource_samples = deque(maxlen=10000)
        self._samples_lock = threading.Lock()
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitor_stop = threading.Event()
        self._process = psutil.Process()

        self._counter_lock = threading.Lock()
        self._deployment_counter = 0
        self._invocation_counter = 0

        self._save_interval = save_interval_seconds
        self._saver_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()

        # Efficient lock-free metrics buffering using deque
        self._metrics_buffer = deque(maxlen=10000)  # Circular buffer
        self._flush_interval = 1.0  # seconds
        self._flush_thread: Optional[threading.Thread] = None

        # Sample aggregation
        self._aggregation_thread: Optional[threading.Thread] = None
        self._aggregation_interval = 60.0  # seconds

        self._start_resource_monitoring()
        self._start_flush_thread()
        self._start_aggregation_thread()

        if self._save_interval:
            self._start_periodic_saving()

    def start_deployment(self, function_name: str, deployment_type: str) -> str:
        """Starts tracking a new deployment.

        Args:
            function_name: The name of the function being deployed.
            deployment_type: The type of deployment (e.g., 'zip', 'container').

        Returns:
            A unique ID for this deployment operation.
        """
        if not self.enable_collection:
            return f"dep-disabled-{time.time()}"

        with self._counter_lock:
            self._deployment_counter += 1
            deployment_id = f"dep-{self._deployment_counter:06d}"

        deployment = DeploymentMetrics(
            deployment_id=deployment_id,
            function_name=function_name,
            deployment_type=deployment_type,
            start_time=time.perf_counter()
        )

        # Lock-free append to buffer
        update = MetricsUpdate(
            update_type='deployment_start',
            data={'deployment_id': deployment_id, 'deployment': asdict(deployment)},
            timestamp=time.time()
        )
        self._metrics_buffer.append(update)  # Thread-safe append to deque

        return deployment_id

    def complete_deployment(self, deployment_id: str, status: str, **kwargs) -> None:
        """Completes the tracking for a deployment.

        Args:
            deployment_id: The unique ID of the deployment.
            status: The final status ('success' or 'failed').
            **kwargs: Additional metrics (e.g., package_size_bytes, error_message).
        """
        if not self.enable_collection:
            return

        update = MetricsUpdate(
            update_type='deployment_complete',
            data={
                'deployment_id': deployment_id,
                'status': status,
                'end_time': time.perf_counter(),
                'kwargs': kwargs
            },
            timestamp=time.time()
        )
        self._metrics_buffer.append(update)  # Lock-free append

    def start_invocation(self, function_name: str, payload_size: int = 0) -> str:
        """Starts tracking a new function invocation.

        Args:
            function_name: The name of the function being invoked.
            payload_size: The size of the invocation payload in bytes.

        Returns:
            A unique ID for this invocation operation.
        """
        if not self.enable_collection:
            return f"inv-disabled-{time.time()}"

        with self._counter_lock:
            self._invocation_counter += 1
            invocation_id = f"inv-{self._invocation_counter:06d}"

        invocation = InvocationMetrics(
            invocation_id=invocation_id,
            function_name=function_name,
            start_time=time.perf_counter(),
            payload_size_bytes=payload_size
        )

        # Lock-free append to buffer
        update = MetricsUpdate(
            update_type='invocation_start',
            data={'invocation_id': invocation_id, 'invocation': asdict(invocation)},
            timestamp=time.time()
        )
        self._metrics_buffer.append(update)  # Thread-safe append to deque

        return invocation_id

    def complete_invocation(self, invocation_id: str, status: str, **kwargs) -> None:
        """Completes the tracking for an invocation.

        Args:
            invocation_id: The unique ID of the invocation.
            status: The final status ('success' or 'failed').
            **kwargs: Additional metrics (e.g., cold_start, error_message).
        """
        if not self.enable_collection:
            return

        update = MetricsUpdate(
            update_type='invocation_complete',
            data={
                'invocation_id': invocation_id,
                'status': status,
                'end_time': time.perf_counter(),
                'kwargs': kwargs
            },
            timestamp=time.time()
        )
        self._metrics_buffer.append(update)  # Lock-free append

    def _start_flush_thread(self) -> None:
        """Start thread to periodically flush metrics."""

        def flush_worker():
            while not self._shutdown_event.is_set():
                time.sleep(self._flush_interval)
                self._flush_metrics()

        self._flush_thread = threading.Thread(target=flush_worker, daemon=True, name="MetricsFlush")
        self._flush_thread.start()

    def _flush_metrics(self) -> None:
        """Flush buffered metrics updates efficiently."""
        if not self._metrics_buffer:
            return

        # Process all available updates in one batch
        updates_to_process = []
        try:
            while True:
                update = self._metrics_buffer.popleft()
                updates_to_process.append(update)
        except IndexError:
            # Buffer is empty
            pass

        # Process updates in batch
        for update in updates_to_process:
            self._apply_update(update)

    def _apply_update(self, update: MetricsUpdate) -> None:
        """Apply a buffered metrics update."""
        if update.update_type == 'deployment_start':
            with self._deployments_lock:
                deployment = DeploymentMetrics(**update.data['deployment'])
                self._deployments[update.data['deployment_id']] = deployment

        elif update.update_type == 'deployment_complete':
            with self._deployments_lock:
                deployment_id = update.data['deployment_id']
                if deployment_id in self._deployments:
                    deployment = self._deployments[deployment_id]
                    deployment.status = update.data['status']
                    deployment.end_time = update.data['end_time']

                    for key, value in update.data['kwargs'].items():
                        if hasattr(deployment, key):
                            setattr(deployment, key, value)

                    # Calculate final durations
                    deployment.total_time_ms = (deployment.end_time - deployment.start_time) * 1000
                    if deployment.platform_api_start_time > 0:
                        deployment.manager_overhead_ms = (
                                                                 deployment.platform_api_start_time - deployment.start_time) * 1000
                        deployment.platform_api_call_ms = (
                                                                  deployment.end_time - deployment.platform_api_start_time) * 1000

                    if deployment.preparation_end_time > 0:
                        deployment.packaging_overhead_ms = (
                                                                   deployment.preparation_end_time - deployment.start_time) * 1000

        elif update.update_type == 'invocation_start':
            with self._invocations_lock:
                invocation = InvocationMetrics(**update.data['invocation'])
                self._invocations[update.data['invocation_id']] = invocation

        elif update.update_type == 'invocation_complete':
            with self._invocations_lock:
                invocation_id = update.data['invocation_id']
                if invocation_id in self._invocations:
                    invocation = self._invocations[invocation_id]
                    invocation.status = update.data['status']
                    invocation.end_time = update.data['end_time']

                    for key, value in update.data['kwargs'].items():
                        if hasattr(invocation, key):
                            setattr(invocation, key, value)

                    # Calculate final durations
                    invocation.total_time_ms = (invocation.end_time - invocation.start_time) * 1000
                    if invocation.pre_invoke_end_time > 0 and invocation.platform_api_end_time > 0:
                        pre_platform_overhead = (invocation.pre_invoke_end_time - invocation.start_time) * 1000
                        post_platform_overhead = (invocation.end_time - invocation.platform_api_end_time) * 1000
                        invocation.manager_overhead_ms = pre_platform_overhead + post_platform_overhead
                        invocation.makespan_ms = update.data['kwargs'].get('function_execution_ms', 0)

                        platform_call_duration = (
                                                         invocation.platform_api_end_time - invocation.pre_invoke_end_time) * 1000
                        invocation.network_delay_ms = max(0, platform_call_duration - invocation.makespan_ms)

    def _start_aggregation_thread(self) -> None:
        """Start thread to periodically aggregate resource samples."""

        def aggregation_worker():
            while not self._shutdown_event.is_set():
                time.sleep(self._aggregation_interval)
                self._aggregate_samples()

        self._aggregation_thread = threading.Thread(target=aggregation_worker, daemon=True, name="SampleAggregation")
        self._aggregation_thread.start()

    def _aggregate_samples(self) -> None:
        """Aggregate old samples to prevent memory growth."""
        with self._samples_lock:
            if len(self._resource_samples) > 5000:
                # Keep last 1000 samples, aggregate the rest
                samples_to_aggregate = list(itertools.islice(self._resource_samples, 0, 4000))

                if samples_to_aggregate:
                    avg_cpu = sum(s.cpu_percent for s in samples_to_aggregate) / len(samples_to_aggregate)
                    avg_mem = sum(s.memory_mb for s in samples_to_aggregate) / len(samples_to_aggregate)

                    # Store aggregated sample
                    aggregated = ResourceSample(
                        timestamp=samples_to_aggregate[0].timestamp,
                        cpu_percent=avg_cpu,
                        memory_mb=avg_mem
                    )

                    # Remove aggregated samples and add summary
                    for _ in range(len(samples_to_aggregate)):
                        self._resource_samples.popleft()
                    self._resource_samples.appendleft(aggregated)

    def get_deployment_statistics(self) -> pd.DataFrame:
        """Calculates and returns deployment statistics as a pandas DataFrame."""
        if not self.enable_collection:
            return pd.DataFrame()

        # Flush any pending updates first
        self._flush_metrics()

        with self._deployments_lock:
            completed = [d for d in self._deployments.values() if d.status == 'success']
        if not completed:
            return pd.DataFrame()

        df = pd.DataFrame([asdict(d) for d in completed])
        return df.groupby('deployment_type').agg(
            count=('deployment_id', 'count'),
            avg_total_ms=('total_time_ms', 'mean'),
            p90_total_ms=('total_time_ms', lambda x: x.quantile(0.9)),
            avg_overhead_ms=('manager_overhead_ms', 'mean'),
            avg_packaging_ms=('packaging_overhead_ms', 'mean'),
            avg_platform_ms=('platform_api_call_ms', 'mean')
        )

    def get_invocation_statistics(self) -> pd.DataFrame:
        """Calculates and returns invocation statistics as a pandas DataFrame."""
        if not self.enable_collection:
            return pd.DataFrame()

        # Flush any pending updates first
        self._flush_metrics()

        with self._invocations_lock:
            completed = [i for i in self._invocations.values() if i.status == 'success']
        if not completed:
            return pd.DataFrame()

        df = pd.DataFrame([asdict(i) for i in completed])
        cold_start_percentage = (df['cold_start'].sum() / len(df)) * 100

        summary = df.agg(
            count=('invocation_id', 'count'),
            avg_total_ms=('total_time_ms', 'mean'),
            p90_total_ms=('total_time_ms', lambda x: x.quantile(0.9)),
            avg_overhead_ms=('manager_overhead_ms', 'mean'),
            avg_makespan_ms=('makespan_ms', 'mean'),
            avg_network_delay_ms=('network_delay_ms', 'mean')
        ).to_frame().T
        summary['cold_start_percentage'] = cold_start_percentage
        return summary

    def save_results(self, filename: str = "benchmark_results.json") -> Path:
        """Saves all collected metrics to a JSON file.

        Args:
            filename: The name of the file to save the results to.

        Returns:
            The path to the saved file.
        """
        if not self.enable_collection:
            # Create empty results file
            filepath = self.output_dir / filename
            with open(filepath, 'w') as f:
                json.dump({"message": "Metrics collection was disabled"}, f, indent=2)
            return filepath

        # Flush any pending updates first
        self._flush_metrics()

        with self._deployments_lock, self._invocations_lock, self._samples_lock:
            results = {
                "summary": {
                    "deployment_stats": self.get_deployment_statistics().to_dict('index'),
                    "invocation_stats": self.get_invocation_statistics().to_dict('records')[
                        0] if not self.get_invocation_statistics().empty else {}
                },
                "raw_data": {
                    "deployments": [asdict(d) for d in self._deployments.values()],
                    "invocations": [asdict(i) for i in self._invocations.values()],
                    "resource_samples": [asdict(s) for s in self._resource_samples]
                }
            }

        filepath = self.output_dir / filename
        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)
        self._log_info(f"Metrics saved to {filepath}")
        return filepath

    def shutdown(self) -> None:
        """Shuts down the metrics collector and its background threads."""
        if not self.enable_collection:
            return

        self._log_info("Shutting down metrics collector...")

        # Flush any pending updates
        self._flush_metrics()

        # Signal shutdown
        self._shutdown_event.set()
        self._stop_resource_monitoring()

        # Wait for threads to complete
        if self._saver_thread and self._saver_thread.is_alive():
            self._saver_thread.join(timeout=2)
        if self._flush_thread and self._flush_thread.is_alive():
            self._flush_thread.join(timeout=2)
        if self._aggregation_thread and self._aggregation_thread.is_alive():
            self._aggregation_thread.join(timeout=2)

        self._log_info("Metrics collector shutdown complete.")

    def _start_resource_monitoring(self) -> None:
        """Starts the background thread for resource monitoring."""
        if not self.enable_collection:
            return

        self._monitor_stop.clear()
        self._monitor_thread = threading.Thread(target=self._monitor_resources, daemon=True, name="ResourceMonitor")
        self._monitor_thread.start()

    def _stop_resource_monitoring(self) -> None:
        """Stops the resource monitoring thread."""
        if not self.enable_collection:
            return

        self._monitor_stop.set()
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=2)

    def _monitor_resources(self) -> None:
        """The target function for the resource monitoring thread."""
        while not self._monitor_stop.is_set():
            try:
                cpu = self._process.cpu_percent(interval=0.1)
                mem = self._process.memory_info().rss / (1024 * 1024)
                sample = ResourceSample(timestamp=time.time(), cpu_percent=cpu, memory_mb=mem)
                with self._samples_lock:
                    self._resource_samples.append(sample)
                time.sleep(0.5)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break

    def _start_periodic_saving(self) -> None:
        """Starts the background thread for periodically saving metrics."""
        self._saver_thread = threading.Thread(target=self._periodic_save_worker, daemon=True, name="PeriodicSaver")
        self._saver_thread.start()

    def _periodic_save_worker(self) -> None:
        """The target function for the periodic saving thread."""
        self._log_info(f"Periodic saving enabled every {self._save_interval} seconds.")
        while not self._shutdown_event.wait(self._save_interval):
            self.save_results()

    def _log_info(self, message: str):
        """Logs an info message."""
        print(f"[MetricsCollector] {message}")