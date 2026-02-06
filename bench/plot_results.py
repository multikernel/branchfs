#!/usr/bin/env python3
"""
Generate paper figures from benchmark results.

Usage:
    python3 plot_results.py bench_results.json --output figures/
"""

import argparse
import json
import sys
from pathlib import Path

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
except ImportError:
    print("matplotlib not installed. Install with: pip install matplotlib")
    sys.exit(1)


def load_results(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def plot_branch_creation(results: list[dict], output_dir: Path):
    """Plot branch creation latency vs base size."""
    data = [r for r in results if r["name"] == "branch_creation"]
    if not data:
        print("No branch_creation data found")
        return

    x = [r["params"]["base_files"] for r in data]
    y = [r["latency_us"] for r in data]

    fig, ax = plt.subplots(figsize=(4, 3))
    ax.plot(x, y, 'o-', markersize=8, linewidth=2, color='#2E86AB')
    ax.set_xscale('log')
    ax.set_xlabel('Base Directory Size (files)', fontsize=10)
    ax.set_ylabel('Branch Creation Latency (μs)', fontsize=10)
    ax.set_title('(a) Branch Creation', fontsize=11)
    ax.grid(True, alpha=0.3)

    # Add horizontal reference line for O(1)
    avg = sum(y) / len(y)
    ax.axhline(y=avg, color='red', linestyle='--', alpha=0.5, label=f'Avg: {avg:.0f} μs')
    ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(output_dir / 'branch_creation.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(output_dir / 'branch_creation.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved branch_creation.pdf")


def plot_commit_latency(results: list[dict], output_dir: Path):
    """Plot commit latency vs modification size."""
    data = [r for r in results if r["name"] == "commit_latency"]
    if not data:
        print("No commit_latency data found")
        return

    x = [r["params"]["modification_kb"] for r in data]
    y = [r["latency_us"] / 1000 for r in data]  # Convert to ms

    fig, ax = plt.subplots(figsize=(4, 3))
    ax.plot(x, y, 's-', markersize=8, linewidth=2, color='#A23B72')
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Modification Size (KB)', fontsize=10)
    ax.set_ylabel('Commit Latency (ms)', fontsize=10)
    ax.set_title('(b) Commit Latency', fontsize=11)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'commit_latency.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(output_dir / 'commit_latency.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved commit_latency.pdf")


def plot_combined_microbench(results: list[dict], output_dir: Path):
    """Plot combined figure for paper (branch creation + commit latency)."""
    creation_data = [r for r in results if r["name"] == "branch_creation"]
    commit_data = [r for r in results if r["name"] == "commit_latency"]

    if not creation_data or not commit_data:
        print("Missing data for combined plot")
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 2.8))

    # (a) Branch creation
    x1 = [r["params"]["base_files"] for r in creation_data]
    y1 = [r["latency_us"] for r in creation_data]
    ax1.plot(x1, y1, 'o-', markersize=6, linewidth=1.5, color='#2E86AB')
    ax1.set_xscale('log')
    ax1.set_xlabel('Base Directory Size (files)', fontsize=9)
    ax1.set_ylabel('Latency (μs)', fontsize=9)
    ax1.set_title('(a) Branch Creation', fontsize=10)
    ax1.grid(True, alpha=0.3)

    # (b) Commit latency
    x2 = [r["params"]["modification_kb"] for r in commit_data]
    y2 = [r["latency_us"] / 1000 for r in commit_data]  # Convert to ms
    ax2.plot(x2, y2, 's-', markersize=6, linewidth=1.5, color='#A23B72')
    ax2.set_xscale('log')
    ax2.set_yscale('log')
    ax2.set_xlabel('Modification Size (KB)', fontsize=9)
    ax2.set_ylabel('Latency (ms)', fontsize=9)
    ax2.set_title('(b) Commit Latency', fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'microbench.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(output_dir / 'microbench.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved microbench.pdf (combined figure)")


def plot_nested_depth(results: list[dict], output_dir: Path):
    """Plot read latency vs branch depth."""
    data = [r for r in results if r["name"] == "nested_read_latency"]
    if not data:
        print("No nested_read_latency data found")
        return

    x = [r["params"]["depth"] for r in data]
    y = [r["latency_us"] for r in data]

    fig, ax = plt.subplots(figsize=(4, 3))
    ax.plot(x, y, '^-', markersize=8, linewidth=2, color='#F18F01')
    ax.set_xlabel('Branch Depth', fontsize=10)
    ax.set_ylabel('Read Latency (μs)', fontsize=10)
    ax.set_title('Read Latency vs Branch Depth', fontsize=11)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / 'nested_depth.pdf', dpi=300, bbox_inches='tight')
    plt.savefig(output_dir / 'nested_depth.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  Saved nested_depth.pdf")


def generate_latex_table(results: list[dict], output_dir: Path):
    """Generate LaTeX table data for paper."""
    agent_data = [r for r in results if r["name"] == "agent_workload"]

    if not agent_data:
        print("No agent_workload data found")
        return

    r = agent_data[0]
    params = r["params"]
    total_ms = r["latency_us"] / 1000

    latex = f"""% Agent workload results
% {params['num_branches']} branches, {params['base_size_mb']} MB base, {params['mod_size_kb']} KB mods
% Total time: {total_ms:.2f} ms
"""

    output_file = output_dir / 'agent_table.txt'
    with open(output_file, 'w') as f:
        f.write(latex)
    print(f"  Saved agent_table.txt")


def main():
    parser = argparse.ArgumentParser(description="Plot benchmark results")
    parser.add_argument("results", help="JSON results file from branchfs_bench.py")
    parser.add_argument("--output", "-o", default=".", help="Output directory")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading results from {args.results}")
    results = load_results(args.results)
    print(f"Found {len(results)} results")

    print("\nGenerating plots...")
    plot_branch_creation(results, output_dir)
    plot_commit_latency(results, output_dir)
    plot_combined_microbench(results, output_dir)
    plot_nested_depth(results, output_dir)
    generate_latex_table(results, output_dir)

    print("\nDone!")


if __name__ == "__main__":
    main()
