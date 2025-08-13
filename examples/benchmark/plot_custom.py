#!/usr/bin/env python3
"""
Nuclio FaaS Manager EKS Benchmark - Plotting and Analysis
Generates plots and analysis from benchmark results
"""

import json
import statistics
from pathlib import Path
from typing import Dict, Any, List, Optional
import matplotlib.pyplot as plt
import numpy as np
import argparse
from datetime import datetime

# Plot styling
plt.style.use('seaborn-v0_8-whitegrid')

COLORS = {
    'primary': '#2E86AB',
    'secondary': '#F18F01',
    'tertiary': '#C73E1D',
    'quaternary': '#2F4858',
    'quinary': '#F6AE2D',
    'senary': '#A23B72',
    'scaling': '#1B998B'
}

PLOT_CONFIG = {
    'figure_size': (12, 8),
    'title_size': 16,
    'label_size': 14,
    'tick_size': 12,
    'legend_size': 12,
    'bar_width': 0.6,
    'dpi': 300
}


class NuclioPlotter:
    """Generate plots from Nuclio benchmark results"""

    def __init__(self, results_path: str, output_dir: Optional[str] = None):
        self.results_path = Path(results_path)

        if self.results_path.is_file():
            # Single results file
            self.output_dir = Path(output_dir or self.results_path.parent / "plots")
            self.single_mode = True
            with open(self.results_path, 'r') as f:
                self.results = json.load(f)
        else:
            # Directory with multiple configs
            self.output_dir = Path(output_dir or self.results_path / "plots")
            self.single_mode = False
            self.results = self._load_multi_config_results()

        self.output_dir.mkdir(exist_ok=True)

    def _load_multi_config_results(self) -> Dict:
        """Load results from multiple configurations"""
        all_results_path = self.results_path / "all_results.json"
        if all_results_path.exists():
            with open(all_results_path, 'r') as f:
                return json.load(f)

        # Build from individual directories
        results = {
            "configurations": []
        }

        for config_dir in sorted(self.results_path.iterdir()):
            if config_dir.is_dir() and (config_dir / "results.json").exists():
                with open(config_dir / "results.json", 'r') as f:
                    config_results = json.load(f)
                    results["configurations"].append({
                        "config_name": config_dir.name,
                        "results": config_results
                    })

        return results

    def generate_all_plots(self):
        """Generate all plots based on available data"""
        if self.single_mode:
            self._generate_single_config_plots()
        else:
            self._generate_multi_config_plots()
            self._generate_comparison_plots()

        self._create_summary_report()
        print(f"Plots saved to: {self.output_dir}")

    def _generate_single_config_plots(self):
        """Generate plots for a single configuration"""
        # Deployment performance
        self._plot_deployment_performance(self.results)

        # Deployment concurrency
        self._plot_deployment_concurrency(self.results)

        # Invocation performance
        self._plot_invocation_performance(self.results)

        # Scaling experiment
        self._plot_scaling_experiment(self.results)

    def _generate_multi_config_plots(self):
        """Generate plots for each configuration"""
        for config in self.results.get("configurations", []):
            if "error" in config:
                continue

            config_name = config["config_name"]
            config_dir = self.output_dir / config_name
            config_dir.mkdir(exist_ok=True)

            # Generate plots for this config
            plotter = NuclioPlotter.__new__(NuclioPlotter)
            plotter.results = config["results"]
            plotter.output_dir = config_dir
            plotter._generate_single_config_plots()

    def _plot_deployment_performance(self, results: Dict):
        """Plot deployment performance breakdown"""
        dep_data = results.get("deployment_performance", {})
        if not dep_data:
            return

        # Prepare data
        types = []
        total_times = []
        components = {
            "Package/Build": [],
            "Registry Push": [],
            "Platform API": []
        }

        for dtype in ["source", "container", "prebuilt"]:
            if dtype not in dep_data:
                continue

            runs = [r for r in dep_data[dtype] if "error" not in r]
            if not runs:
                continue

            types.append(dtype.title())

            # Average times
            avg_total = statistics.mean([r["total_ms"] for r in runs]) / 1000
            total_times.append(avg_total)

            if dtype == "source":
                pkg_time = statistics.mean([r.get("package_ms", 0) for r in runs]) / 1000
                components["Package/Build"].append(pkg_time)
                components["Registry Push"].append(0)
            elif dtype == "container":
                build_time = statistics.mean([r.get("build_ms", 0) for r in runs]) / 1000
                push_time = statistics.mean([r.get("push_ms", 0) for r in runs]) / 1000
                components["Package/Build"].append(build_time)
                components["Registry Push"].append(push_time)
            else:
                components["Package/Build"].append(0)
                components["Registry Push"].append(0)

            platform_time = statistics.mean([r.get("platform_ms", 0) for r in runs]) / 1000
            components["Platform API"].append(platform_time)

        if not types:
            return

        # Create plot
        fig, ax = plt.subplots(figsize=PLOT_CONFIG['figure_size'])

        x = np.arange(len(types))
        width = PLOT_CONFIG['bar_width']

        # Stacked bars
        bottom = np.zeros(len(types))
        colors = [COLORS['tertiary'], COLORS['quinary'], COLORS['senary']]

        for i, (label, values) in enumerate(components.items()):
            if any(v > 0 for v in values):
                ax.bar(x, values, width, label=label, bottom=bottom,
                       color=colors[i], edgecolor='black', linewidth=1)
                bottom += values

        # Labels
        ax.set_xlabel('Deployment Type', fontsize=PLOT_CONFIG['label_size'])
        ax.set_ylabel('Time (seconds)', fontsize=PLOT_CONFIG['label_size'])
        ax.set_title('Deployment Performance by Type', fontsize=PLOT_CONFIG['title_size'])
        ax.set_xticks(x)
        ax.set_xticklabels(types)
        ax.legend()

        # Add total time labels
        for i, total in enumerate(total_times):
            ax.text(i, bottom[i] + 0.5, f'{total:.1f}s', ha='center', fontweight='bold')

        plt.tight_layout()
        plt.savefig(self.output_dir / "deployment_performance.png", dpi=PLOT_CONFIG['dpi'])
        plt.close()

    def _plot_deployment_concurrency(self, results: Dict):
        """Plot deployment concurrency performance"""
        conc_data = results.get("deployment_concurrency", {})
        if not conc_data:
            return

        # Prepare data
        levels = []
        avg_times = []
        std_devs = []

        for level_str in sorted(conc_data.keys(), key=int):
            runs = conc_data[level_str]
            if not runs:
                continue

            levels.append(int(level_str))

            # Collect all individual times
            all_times = []
            for run in runs:
                all_times.extend([t / 1000 for t in run.get("times_ms", [])])

            if all_times:
                avg_times.append(statistics.mean(all_times))
                std_devs.append(statistics.stdev(all_times) if len(all_times) > 1 else 0)

        if not levels:
            return

        # Create plot
        fig, ax = plt.subplots(figsize=PLOT_CONFIG['figure_size'])

        ax.errorbar(levels, avg_times, yerr=std_devs, marker='o', markersize=8,
                    linewidth=2, capsize=5, color=COLORS['primary'])

        ax.set_xlabel('Concurrent Deployments', fontsize=PLOT_CONFIG['label_size'])
        ax.set_ylabel('Average Deployment Time (seconds)', fontsize=PLOT_CONFIG['label_size'])
        ax.set_title('Deployment Time vs Concurrency', fontsize=PLOT_CONFIG['title_size'])
        ax.set_xscale('log', base=2)
        ax.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.output_dir / "deployment_concurrency.png", dpi=PLOT_CONFIG['dpi'])
        plt.close()

    def _plot_invocation_performance(self, results: Dict):
        """Plot invocation performance"""
        inv_data = results.get("invocation_concurrency", {})
        if not inv_data:
            return

        # Prepare data
        levels = []
        makespans = []
        overheads = []

        for level_str in sorted(inv_data.keys(), key=int):
            runs = inv_data[level_str]
            if not runs:
                continue

            levels.append(int(level_str))

            # Average metrics
            avg_makespan = statistics.mean([r["makespan_ms"] / 1000 for r in runs
                                            if "makespan_ms" in r])
            avg_overhead = statistics.mean([r["avg_overhead_ms"] for r in runs
                                            if "avg_overhead_ms" in r])

            makespans.append(avg_makespan)
            overheads.append(avg_overhead / 1000)

        if not levels:
            return

        # Create plot
        fig, ax = plt.subplots(figsize=PLOT_CONFIG['figure_size'])

        x = np.arange(len(levels))
        width = PLOT_CONFIG['bar_width'] / 2

        # Side-by-side bars
        ax.bar(x - width / 2, makespans, width, label='Makespan',
               color=COLORS['primary'], edgecolor='black')
        ax.bar(x + width / 2, overheads, width, label='Avg Overhead',
               color=COLORS['secondary'], edgecolor='black')

        ax.set_xlabel('Concurrent Invocations', fontsize=PLOT_CONFIG['label_size'])
        ax.set_ylabel('Time (seconds)', fontsize=PLOT_CONFIG['label_size'])
        ax.set_title('Invocation Performance', fontsize=PLOT_CONFIG['title_size'])
        ax.set_xticks(x)
        ax.set_xticklabels(levels)
        ax.legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / "invocation_performance.png", dpi=PLOT_CONFIG['dpi'])
        plt.close()

    def _plot_scaling_experiment(self, results: Dict):
        """Plot scaling experiment results"""
        scale_data = results.get("scaling_experiment", {})
        if not scale_data:
            return

        # Prepare data
        replicas = []
        throughputs = []
        efficiencies = []

        for rep_str in sorted(scale_data.keys(), key=int):
            runs = scale_data[rep_str]
            if not runs:
                continue

            rep_count = int(rep_str)
            replicas.append(rep_count)

            # Average throughput
            avg_throughput = statistics.mean([r["throughput"] for r in runs
                                              if "throughput" in r])
            throughputs.append(avg_throughput)

        if not replicas or len(replicas) < 2:
            return

        # Calculate efficiency
        base_throughput = throughputs[0]
        for i, (rep, thr) in enumerate(zip(replicas, throughputs)):
            expected = base_throughput * (rep / replicas[0])
            efficiency = (thr / expected * 100) if expected > 0 else 0
            efficiencies.append(efficiency)

        # Create plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Throughput plot
        ax1.plot(replicas, throughputs, marker='o', markersize=8, linewidth=2,
                 color=COLORS['scaling'])
        ax1.set_xlabel('Number of Replicas', fontsize=PLOT_CONFIG['label_size'])
        ax1.set_ylabel('Throughput (req/s)', fontsize=PLOT_CONFIG['label_size'])
        ax1.set_title('Throughput vs Replicas', fontsize=PLOT_CONFIG['title_size'])
        ax1.grid(True, alpha=0.3)

        # Efficiency plot
        ax2.bar(range(len(replicas)), efficiencies, color=COLORS['quaternary'],
                edgecolor='black', alpha=0.7)
        ax2.axhline(y=100, color='red', linestyle='--', label='Perfect Scaling')
        ax2.set_xlabel('Number of Replicas', fontsize=PLOT_CONFIG['label_size'])
        ax2.set_ylabel('Scaling Efficiency (%)', fontsize=PLOT_CONFIG['label_size'])
        ax2.set_title('Scaling Efficiency', fontsize=PLOT_CONFIG['title_size'])
        ax2.set_xticks(range(len(replicas)))
        ax2.set_xticklabels(replicas)
        ax2.set_ylim(0, 120)
        ax2.legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / "scaling_experiment.png", dpi=PLOT_CONFIG['dpi'])
        plt.close()

    def _generate_comparison_plots(self):
        """Generate comparison plots across configurations"""
        configs = self.results.get("configurations", [])
        if len(configs) < 2:
            return

        # Extract data
        config_names = []
        deployment_times = []
        invocation_makespans = []
        max_throughputs = []

        for config in configs:
            if "error" in config:
                continue

            name = config["config_name"]
            results = config["results"]

            # Parse node count from name
            node_count = int(name.split('-')[0])
            vcpu_count = int(name.split('-')[2].replace('vcpu', ''))
            config_names.append(f"{node_count} nodes\n({vcpu_count} vCPUs)")

            # Deployment time (average across all types)
            dep_times = []
            for dtype in ["source", "container", "prebuilt"]:
                runs = results.get("deployment_performance", {}).get(dtype, [])
                runs = [r for r in runs if "error" not in r]
                if runs:
                    dep_times.extend([r["total_ms"] / 1000 for r in runs])

            deployment_times.append(statistics.mean(dep_times) if dep_times else 0)

            # Invocation makespan (1000 concurrent)
            inv_runs = results.get("invocation_concurrency", {}).get("1000", [])
            if inv_runs:
                makespan = statistics.mean([r["makespan_ms"] / 1000 for r in inv_runs])
                invocation_makespans.append(makespan)
            else:
                invocation_makespans.append(0)

            # Max throughput
            max_thr = 0
            for runs in results.get("scaling_experiment", {}).values():
                for run in runs:
                    if "throughput" in run:
                        max_thr = max(max_thr, run["throughput"])
            max_throughputs.append(max_thr)

        # Create comparison plot
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(14, 10))

        x = np.arange(len(config_names))

        # Deployment performance
        ax1.bar(x, deployment_times, color=COLORS['primary'], edgecolor='black')
        ax1.set_ylabel('Avg Deployment Time (s)', fontsize=PLOT_CONFIG['label_size'])
        ax1.set_title('Deployment Performance', fontsize=PLOT_CONFIG['title_size'])
        ax1.set_xticks(x)
        ax1.set_xticklabels(config_names)

        # Invocation performance
        ax2.bar(x, invocation_makespans, color=COLORS['secondary'], edgecolor='black')
        ax2.set_ylabel('Makespan (s)', fontsize=PLOT_CONFIG['label_size'])
        ax2.set_title('1000 Concurrent Invocations', fontsize=PLOT_CONFIG['title_size'])
        ax2.set_xticks(x)
        ax2.set_xticklabels(config_names)

        # Max throughput
        ax3.bar(x, max_throughputs, color=COLORS['tertiary'], edgecolor='black')
        ax3.set_ylabel('Max Throughput (req/s)', fontsize=PLOT_CONFIG['label_size'])
        ax3.set_title('Maximum Throughput', fontsize=PLOT_CONFIG['title_size'])
        ax3.set_xticks(x)
        ax3.set_xticklabels(config_names)

        # Cost efficiency
        vcpus = [4, 8, 16]  # From config names
        efficiency = [t / v if v > 0 else 0 for t, v in zip(max_throughputs, vcpus)]
        ax4.bar(x, efficiency, color=COLORS['scaling'], edgecolor='black', alpha=0.7)
        ax4.set_ylabel('Throughput per vCPU', fontsize=PLOT_CONFIG['label_size'])
        ax4.set_title('Cost Efficiency', fontsize=PLOT_CONFIG['title_size'])
        ax4.set_xticks(x)
        ax4.set_xticklabels(config_names)

        plt.suptitle('EKS Configuration Comparison', fontsize=18, fontweight='bold')
        plt.tight_layout()
        plt.savefig(self.output_dir / "configuration_comparison.png", dpi=PLOT_CONFIG['dpi'])
        plt.close()

    def _create_summary_report(self):
        """Create text summary report"""
        lines = ["Nuclio EKS Benchmark Summary", "=" * 40, ""]

        if self.single_mode:
            # Single config summary
            config = self.results.get("config", {})
            node_config = config.get("node_config", {})

            lines.extend([
                f"Configuration: {node_config.get('config_name', 'Unknown')}",
                f"Total vCPUs: {node_config.get('total_vcpus', 0)}",
                f"Total Memory: {node_config.get('total_memory_gb', 0)} GB",
                ""
            ])

            # Performance metrics
            self._add_performance_summary(lines, self.results)

        else:
            # Multi-config summary
            lines.append("CONFIGURATIONS TESTED:")

            for config in self.results.get("configurations", []):
                if "error" in config:
                    lines.append(f"  - {config['config_name']}: ERROR")
                else:
                    results = config["results"]
                    node_config = results.get("config", {}).get("node_config", {})
                    lines.append(f"  - {config['config_name']}: "
                                 f"{node_config.get('total_vcpus', 0)} vCPUs")

            lines.extend(["", "PERFORMANCE COMPARISON:", ""])

            # Find best performers
            best_deployment = None
            best_throughput = None
            best_efficiency = None

            deployment_times = []
            throughputs = []
            efficiencies = []

            for i, config in enumerate(self.results.get("configurations", [])):
                if "error" in config:
                    continue

                results = config["results"]

                # Deployment time
                dep_times = []
                for runs in results.get("deployment_performance", {}).values():
                    runs = [r for r in runs if "error" not in r and "total_ms" in r]
                    dep_times.extend([r["total_ms"] / 1000 for r in runs])

                if dep_times:
                    avg_dep = statistics.mean(dep_times)
                    deployment_times.append((i, avg_dep))

                # Max throughput
                max_thr = 0
                for runs in results.get("scaling_experiment", {}).values():
                    for run in runs:
                        if "throughput" in run:
                            max_thr = max(max_thr, run["throughput"])

                if max_thr > 0:
                    throughputs.append((i, max_thr))

                    # Efficiency
                    vcpus = results.get("config", {}).get("node_config", {}).get("total_vcpus", 1)
                    efficiencies.append((i, max_thr / vcpus))

            if deployment_times:
                best_deployment = min(deployment_times, key=lambda x: x[1])[0]
            if throughputs:
                best_throughput = max(throughputs, key=lambda x: x[1])[0]
            if efficiencies:
                best_efficiency = max(efficiencies, key=lambda x: x[1])[0]

            configs_list = [c["config_name"] for c in self.results.get("configurations", [])]

            if best_deployment is not None:
                lines.append(f"Fastest Deployment: {configs_list[best_deployment]}")
            if best_throughput is not None:
                lines.append(f"Highest Throughput: {configs_list[best_throughput]}")
            if best_efficiency is not None:
                lines.append(f"Best Cost Efficiency: {configs_list[best_efficiency]}")

        # Save report
        with open(self.output_dir / "summary.txt", 'w') as f:
            f.write("\n".join(lines))

        print("\n".join(lines))

    def _add_performance_summary(self, lines: List[str], results: Dict):
        """Add performance metrics to summary"""
        # Deployment performance
        dep_perf = results.get("deployment_performance", {})
        if dep_perf:
            lines.append("DEPLOYMENT PERFORMANCE:")
            for dtype in ["source", "container", "prebuilt"]:
                if dtype in dep_perf:
                    runs = [r for r in dep_perf[dtype] if "error" not in r]
                    if runs:
                        avg_time = statistics.mean([r["total_ms"] / 1000 for r in runs])
                        lines.append(f"  {dtype.title()}: {avg_time:.2f}s average")
            lines.append("")

        # Scaling performance
        scale_data = results.get("scaling_experiment", {})
        if scale_data:
            max_throughput = 0
            optimal_replicas = 0

            for rep_str, runs in scale_data.items():
                for run in runs:
                    if "throughput" in run and run["throughput"] > max_throughput:
                        max_throughput = run["throughput"]
                        optimal_replicas = int(rep_str)

            if max_throughput > 0:
                lines.extend([
                    "SCALING PERFORMANCE:",
                    f"  Max Throughput: {max_throughput:.0f} req/s",
                    f"  Optimal Replicas: {optimal_replicas}",
                    ""
                ])


def main():
    parser = argparse.ArgumentParser(description="Generate plots from Nuclio benchmark results")
    parser.add_argument("results", help="Path to results.json or results directory")
    parser.add_argument("-o", "--output", help="Output directory for plots")

    args = parser.parse_args()

    plotter = NuclioPlotter(args.results, args.output)
    plotter.generate_all_plots()


if __name__ == "__main__":
    main()