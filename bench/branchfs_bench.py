#!/usr/bin/env python3
"""
BranchFS Microbenchmark Suite

Benchmarks:
1. Branch creation latency vs base directory size (O(1) verification)
2. Commit latency vs modification size
3. Abort latency
4. Read/write throughput

This benchmark captures internal daemon timing via log parsing for accurate
measurements without CLI/socket overhead.

Usage:
    python3 branchfs_bench.py [--branchfs PATH] [--output results.json]
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional


@dataclass
class BenchmarkResult:
    name: str
    params: dict
    latency_us: float  # microseconds
    throughput_mbps: Optional[float] = None
    iterations: int = 1


class BranchFSBench:
    def __init__(self, branchfs_bin: str = "branchfs"):
        self.branchfs = branchfs_bin
        self.results: list[BenchmarkResult] = []

    def _run(self, *args, check=True, env=None) -> subprocess.CompletedProcess:
        """Run branchfs command."""
        cmd = [self.branchfs] + list(args)
        return subprocess.run(cmd, check=check, capture_output=True, text=True, env=env)

    def _start_daemon(self, base_dir: Path, storage: Path, mountpoint: Path, log_file: Path):
        """Start branchfs daemon with logging enabled."""
        env = os.environ.copy()
        env["RUST_LOG"] = "debug"

        with open(log_file, "w") as f:
            proc = subprocess.Popen(
                [self.branchfs, "mount", "--base", str(base_dir),
                 "--storage", str(storage), str(mountpoint)],
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT
            )
        time.sleep(1.5)  # Wait for mount
        return proc

    def _stop_daemon(self, mountpoint: Path, storage: Path):
        """Stop daemon and unmount."""
        try:
            self._run("unmount", str(mountpoint), "--storage", str(storage), check=False)
        except:
            pass
        subprocess.run(["fusermount3", "-u", str(mountpoint)],
                      capture_output=True, check=False)
        time.sleep(0.2)

    def _parse_bench_logs(self, log_file: Path, operation: str) -> list[float]:
        """Parse [BENCH] lines from daemon log and extract latencies in microseconds."""
        latencies = []
        pattern = rf"\[BENCH\] {operation}.*\((\d+) us\)"

        with open(log_file) as f:
            for line in f:
                match = re.search(pattern, line)
                if match:
                    latencies.append(float(match.group(1)))
        return latencies

    def _create_base_files(self, base_dir: Path, num_files: int, file_size: int = 1024):
        """Create test files in base directory."""
        base_dir.mkdir(parents=True, exist_ok=True)
        for i in range(num_files):
            subdir = base_dir / f"dir_{i // 100}"
            subdir.mkdir(exist_ok=True)
            file_path = subdir / f"file_{i}.txt"
            file_path.write_bytes(os.urandom(file_size))

    # =========================================================================
    # Benchmark 1: Branch Creation Latency vs Base Size (O(1) verification)
    # =========================================================================
    def bench_branch_creation(self, base_sizes: list[int] = [100, 1000, 10000],
                              iterations: int = 5):
        """Measure internal branch creation time for various base directory sizes."""
        print("\n=== Benchmark: Branch Creation Latency (Internal) ===")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)

            for num_files in base_sizes:
                print(f"  Testing with {num_files} files...")

                base_dir = tmpdir / f"base_{num_files}"
                mountpoint = tmpdir / f"mnt_{num_files}"
                storage = tmpdir / f"storage_{num_files}"
                log_file = tmpdir / f"daemon_{num_files}.log"

                mountpoint.mkdir()
                storage.mkdir()

                # Create base files
                self._create_base_files(base_dir, num_files)

                # Start daemon with logging
                proc = self._start_daemon(base_dir, storage, mountpoint, log_file)

                try:
                    for i in range(iterations):
                        branch_name = f"bench_{i}"
                        self._run("create", branch_name, str(mountpoint),
                                 "--storage", str(storage))
                        self._run("abort", str(mountpoint), "--storage", str(storage),
                                 check=False)

                    # Parse internal timing from logs
                    latencies = self._parse_bench_logs(log_file, "create_branch")
                    if latencies:
                        avg_lat = sum(latencies) / len(latencies)
                        result = BenchmarkResult(
                            name="branch_creation",
                            params={"base_files": num_files},
                            latency_us=avg_lat,
                            iterations=len(latencies)
                        )
                        self.results.append(result)
                        print(f"    {num_files} files: {avg_lat:.1f} us (internal, avg of {len(latencies)})")
                    else:
                        print(f"    {num_files} files: No timing data found in logs")

                finally:
                    self._stop_daemon(mountpoint, storage)
                    proc.wait(timeout=5)

    # =========================================================================
    # Benchmark 2: Commit Latency vs Modification Size
    # =========================================================================
    def bench_commit_latency(self, mod_sizes_kb: list[int] = [1, 10, 100, 1000],
                             iterations: int = 3):
        """Measure internal commit time for various modification sizes."""
        print("\n=== Benchmark: Commit Latency (Internal) ===")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            base_dir = tmpdir / "base"

            # Create modest base (100 files, 1KB each)
            self._create_base_files(base_dir, 100, file_size=1024)

            for mod_size_kb in mod_sizes_kb:
                print(f"  Testing with {mod_size_kb} KB modifications...")

                latencies = []
                for i in range(iterations):
                    mountpoint = tmpdir / f"mnt_{mod_size_kb}_{i}"
                    storage = tmpdir / f"storage_{mod_size_kb}_{i}"
                    log_file = tmpdir / f"daemon_{mod_size_kb}_{i}.log"
                    mountpoint.mkdir()
                    storage.mkdir()

                    proc = self._start_daemon(base_dir, storage, mountpoint, log_file)

                    try:
                        # Create branch
                        self._run("create", "bench", str(mountpoint),
                                 "--storage", str(storage))

                        # Make modifications
                        mod_file = mountpoint / "modification.bin"
                        mod_file.write_bytes(os.urandom(mod_size_kb * 1024))
                        os.sync()
                        time.sleep(0.1)

                        # Commit via ctl file (direct FUSE, triggers internal timing)
                        (mountpoint / ".branchfs_ctl").write_text("commit")
                        time.sleep(0.2)

                        # Parse timing
                        commit_lats = self._parse_bench_logs(log_file, "commit")
                        if commit_lats:
                            latencies.extend(commit_lats)

                    finally:
                        self._stop_daemon(mountpoint, storage)
                        proc.wait(timeout=5)

                if latencies:
                    avg_lat = sum(latencies) / len(latencies)
                    result = BenchmarkResult(
                        name="commit_latency",
                        params={"modification_kb": mod_size_kb},
                        latency_us=avg_lat,
                        iterations=len(latencies)
                    )
                    self.results.append(result)
                    print(f"    {mod_size_kb} KB: {avg_lat:.1f} us ({avg_lat/1000:.2f} ms)")

    # =========================================================================
    # Benchmark 3: Abort Latency
    # =========================================================================
    def bench_abort_latency(self, mod_sizes_kb: list[int] = [1, 100, 1000],
                            iterations: int = 3):
        """Measure internal abort time."""
        print("\n=== Benchmark: Abort Latency (Internal) ===")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            base_dir = tmpdir / "base"
            self._create_base_files(base_dir, 100, file_size=1024)

            for mod_size_kb in mod_sizes_kb:
                print(f"  Testing abort with {mod_size_kb} KB modifications...")

                latencies = []
                for i in range(iterations):
                    mountpoint = tmpdir / f"abort_mnt_{mod_size_kb}_{i}"
                    storage = tmpdir / f"abort_storage_{mod_size_kb}_{i}"
                    log_file = tmpdir / f"abort_daemon_{mod_size_kb}_{i}.log"
                    mountpoint.mkdir()
                    storage.mkdir()

                    proc = self._start_daemon(base_dir, storage, mountpoint, log_file)

                    try:
                        self._run("create", "bench", str(mountpoint),
                                 "--storage", str(storage))

                        mod_file = mountpoint / "modification.bin"
                        mod_file.write_bytes(os.urandom(mod_size_kb * 1024))
                        os.sync()
                        time.sleep(0.1)

                        # Abort via ctl file
                        (mountpoint / ".branchfs_ctl").write_text("abort")
                        time.sleep(0.2)

                        abort_lats = self._parse_bench_logs(log_file, "abort")
                        if abort_lats:
                            latencies.extend(abort_lats)

                    finally:
                        self._stop_daemon(mountpoint, storage)
                        proc.wait(timeout=5)

                if latencies:
                    avg_lat = sum(latencies) / len(latencies)
                    result = BenchmarkResult(
                        name="abort_latency",
                        params={"modification_kb": mod_size_kb},
                        latency_us=avg_lat,
                        iterations=len(latencies)
                    )
                    self.results.append(result)
                    print(f"    {mod_size_kb} KB: {avg_lat:.1f} us ({avg_lat/1000:.2f} ms)")

    # =========================================================================
    # Benchmark 4: Read/Write Throughput
    # =========================================================================
    def bench_throughput(self, file_size_mb: int = 50, block_size_kb: int = 64):
        """Measure read/write throughput on branch."""
        print("\n=== Benchmark: Read/Write Throughput ===")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            base_dir = tmpdir / "base"
            mountpoint = tmpdir / "mnt"
            storage = tmpdir / "storage"
            log_file = tmpdir / "daemon.log"

            base_dir.mkdir()
            mountpoint.mkdir()
            storage.mkdir()

            # Create a test file in base
            test_file_base = base_dir / "testfile.bin"
            test_file_base.write_bytes(os.urandom(file_size_mb * 1024 * 1024))

            proc = self._start_daemon(base_dir, storage, mountpoint, log_file)

            try:
                self._run("create", "bench", str(mountpoint), "--storage", str(storage))

                test_file = mountpoint / "testfile.bin"

                # Sequential read throughput
                start = time.perf_counter()
                with open(test_file, "rb") as f:
                    while f.read(block_size_kb * 1024):
                        pass
                read_time = time.perf_counter() - start
                read_throughput = file_size_mb / read_time

                # Sequential write throughput (new file)
                write_file = mountpoint / "writefile.bin"
                data = os.urandom(block_size_kb * 1024)
                num_blocks = (file_size_mb * 1024) // block_size_kb

                start = time.perf_counter()
                with open(write_file, "wb") as f:
                    for _ in range(num_blocks):
                        f.write(data)
                    f.flush()
                    os.fsync(f.fileno())
                write_time = time.perf_counter() - start
                write_throughput = file_size_mb / write_time

                self.results.append(BenchmarkResult(
                    name="read_throughput",
                    params={"file_size_mb": file_size_mb, "block_size_kb": block_size_kb},
                    latency_us=read_time * 1_000_000,
                    throughput_mbps=read_throughput
                ))
                self.results.append(BenchmarkResult(
                    name="write_throughput",
                    params={"file_size_mb": file_size_mb, "block_size_kb": block_size_kb},
                    latency_us=write_time * 1_000_000,
                    throughput_mbps=write_throughput
                ))

                print(f"  Read:  {read_throughput:.1f} MB/s")
                print(f"  Write: {write_throughput:.1f} MB/s")

            finally:
                self._stop_daemon(mountpoint, storage)
                proc.wait(timeout=5)

    # =========================================================================
    # Benchmark 5: Nested Branch Depth
    # =========================================================================
    def bench_nested_depth(self, depths: list[int] = [1, 2, 4, 8],
                           iterations: int = 50):
        """Measure read latency at various branch depths."""
        print("\n=== Benchmark: Nested Branch Depth ===")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir = Path(tmpdir)
            base_dir = tmpdir / "base"
            base_dir.mkdir()

            # Create test file in base
            test_file_base = base_dir / "testfile.txt"
            test_file_base.write_text("Hello, World!" * 100)

            for depth in depths:
                print(f"  Testing depth {depth}...")

                mountpoint = tmpdir / f"mnt_{depth}"
                storage = tmpdir / f"storage_{depth}"
                log_file = tmpdir / f"daemon_{depth}.log"
                mountpoint.mkdir()
                storage.mkdir()

                proc = self._start_daemon(base_dir, storage, mountpoint, log_file)

                try:
                    # Create nested branches
                    parent = "main"
                    for d in range(depth):
                        branch_name = f"branch_{d}"
                        self._run("create", branch_name, str(mountpoint),
                                 "-p", parent, "--storage", str(storage))
                        parent = branch_name

                    test_file = mountpoint / "testfile.txt"

                    # Warm up
                    for _ in range(10):
                        test_file.read_bytes()

                    # Measure read latency
                    latencies = []
                    for _ in range(iterations):
                        start = time.perf_counter()
                        test_file.read_bytes()
                        lat = (time.perf_counter() - start) * 1_000_000
                        latencies.append(lat)

                    avg_lat = sum(latencies) / len(latencies)
                    self.results.append(BenchmarkResult(
                        name="nested_read_latency",
                        params={"depth": depth},
                        latency_us=avg_lat,
                        iterations=iterations
                    ))
                    print(f"    Depth {depth}: {avg_lat:.1f} us")

                finally:
                    self._stop_daemon(mountpoint, storage)
                    proc.wait(timeout=5)

    def save_results(self, output_path: str):
        """Save results to JSON file."""
        results_dict = [asdict(r) for r in self.results]
        with open(output_path, "w") as f:
            json.dump(results_dict, f, indent=2)
        print(f"\nResults saved to {output_path}")

    def print_summary(self):
        """Print summary table."""
        print("\n" + "=" * 60)
        print("SUMMARY (Internal Timing)")
        print("=" * 60)
        for r in self.results:
            params_str = ", ".join(f"{k}={v}" for k, v in r.params.items())
            if r.throughput_mbps:
                print(f"{r.name} ({params_str}): {r.throughput_mbps:.1f} MB/s")
            else:
                lat_ms = r.latency_us / 1000
                if lat_ms < 1:
                    print(f"{r.name} ({params_str}): {r.latency_us:.1f} us")
                else:
                    print(f"{r.name} ({params_str}): {lat_ms:.2f} ms")

    def generate_latex_tables(self, output_dir: str = "."):
        """Generate LaTeX tables for paper."""
        output_dir = Path(output_dir)

        # Branch creation table
        creation_data = [r for r in self.results if r.name == "branch_creation"]
        if creation_data:
            with open(output_dir / "branch_creation_table.tex", "w") as f:
                f.write("% Auto-generated from benchmark\n")
                f.write("\\begin{tabular}{rr}\n\\toprule\n")
                f.write("Base Size (files) & Creation Latency ($\\mu$s) \\\\\n\\midrule\n")
                for r in creation_data:
                    f.write(f"{r.params['base_files']:,} & {r.latency_us:.0f} \\\\\n")
                f.write("\\bottomrule\n\\end{tabular}\n")
            print(f"  Generated branch_creation_table.tex")

        # Commit latency table
        commit_data = [r for r in self.results if r.name == "commit_latency"]
        if commit_data:
            with open(output_dir / "commit_table.tex", "w") as f:
                f.write("% Auto-generated from benchmark\n")
                f.write("\\begin{tabular}{rr}\n\\toprule\n")
                f.write("Modification Size & Commit Latency (ms) \\\\\n\\midrule\n")
                for r in commit_data:
                    size_kb = r.params['modification_kb']
                    if size_kb >= 1024:
                        size_str = f"{size_kb // 1024} MB"
                    else:
                        size_str = f"{size_kb} KB"
                    f.write(f"{size_str} & {r.latency_us/1000:.1f} \\\\\n")
                f.write("\\bottomrule\n\\end{tabular}\n")
            print(f"  Generated commit_table.tex")


def main():
    parser = argparse.ArgumentParser(description="BranchFS Microbenchmark Suite")
    parser.add_argument("--branchfs", default="branchfs",
                       help="Path to branchfs binary")
    parser.add_argument("--output", "-o", default="bench_results.json",
                       help="Output JSON file")
    parser.add_argument("--quick", action="store_true",
                       help="Run quick benchmarks with smaller parameters")
    parser.add_argument("--bench", choices=["creation", "commit", "abort",
                                            "throughput", "nested", "all"],
                       default="all", help="Which benchmark to run")
    parser.add_argument("--latex", default=None,
                       help="Generate LaTeX tables in specified directory")
    args = parser.parse_args()

    # Check branchfs is available
    if shutil.which(args.branchfs) is None:
        # Try relative path
        branchfs_path = Path(__file__).parent.parent / "target" / "release" / "branchfs"
        if branchfs_path.exists():
            args.branchfs = str(branchfs_path)
        else:
            print(f"Error: branchfs binary not found. Build with 'cargo build --release'")
            sys.exit(1)

    bench = BranchFSBench(args.branchfs)

    if args.quick:
        base_sizes = [100, 1000]
        mod_sizes = [1, 100]
        abort_sizes = [1, 100]
        depths = [1, 4]
        iterations = 2
    else:
        base_sizes = [100, 1000, 10000]
        mod_sizes = [1, 10, 100, 1000]
        abort_sizes = [1, 100, 1000]
        depths = [1, 2, 4, 8]
        iterations = 5

    try:
        if args.bench in ["creation", "all"]:
            bench.bench_branch_creation(base_sizes, iterations=iterations)

        if args.bench in ["commit", "all"]:
            bench.bench_commit_latency(mod_sizes, iterations=min(iterations, 3))

        if args.bench in ["abort", "all"]:
            bench.bench_abort_latency(abort_sizes, iterations=min(iterations, 3))

        if args.bench in ["throughput", "all"]:
            bench.bench_throughput(file_size_mb=20 if args.quick else 50)

        if args.bench in ["nested", "all"]:
            bench.bench_nested_depth(depths, iterations=30 if args.quick else 50)

        bench.print_summary()
        bench.save_results(args.output)

        if args.latex:
            print(f"\nGenerating LaTeX tables in {args.latex}...")
            bench.generate_latex_tables(args.latex)

    except KeyboardInterrupt:
        print("\nInterrupted")
        bench.print_summary()
        bench.save_results(args.output)


if __name__ == "__main__":
    main()
