# BranchFS Benchmarks

Microbenchmark suite for evaluating BranchFS performance. This suite measures **internal operation timing** by parsing daemon logs, providing accurate measurements without CLI/socket overhead.

## Prerequisites

```bash
# Build branchfs
cd ..
cargo build --release

# For plotting (optional)
pip install matplotlib
```

## Running Benchmarks

### Quick Test (Shell Script)

```bash
./quick_bench.sh
```

### Full Benchmark Suite (Python)

```bash
# Run all benchmarks
python3 branchfs_bench.py

# Quick mode (smaller parameters)
python3 branchfs_bench.py --quick

# Specific benchmark
python3 branchfs_bench.py --bench creation   # Branch creation O(1) test
python3 branchfs_bench.py --bench commit     # Commit latency
python3 branchfs_bench.py --bench abort      # Abort latency
python3 branchfs_bench.py --bench throughput # Read/write throughput
python3 branchfs_bench.py --bench nested     # Nested branch depth

# Generate LaTeX tables for paper
python3 branchfs_bench.py --latex ../paper/
```

## Benchmarks

### 1. Branch Creation Latency (O(1) Verification)

Measures internal branch creation time with varying base directory sizes.
Validates that branch creation is O(1) - constant time regardless of base size.

- **Parameters**: Base sizes of 100, 1K, 10K files
- **Expected**: ~300 µs constant latency

### 2. Commit Latency

Measures internal commit time for varying modification sizes.
Validates linear scaling with modification size.

- **Parameters**: Modifications of 1KB, 10KB, 100KB, 1MB
- **Expected**: Linear increase with modification size

### 3. Abort Latency

Measures internal abort time. Abort only removes delta storage.

- **Parameters**: Modifications of 1KB, 100KB, 1MB
- **Expected**: Fast, independent of modification size

### 4. Read/Write Throughput

Measures sequential read/write throughput through FUSE layer.

- **Parameters**: 50MB file, 64KB block size
- **Output**: MB/s throughput

### 5. Nested Branch Depth

Measures read latency at various branch depths.
Tests path resolution overhead.

- **Parameters**: Depths 1, 2, 4, 8
- **Expected**: Minimal increase with depth (metadata cached)

## Internal Timing

The benchmark parses `[BENCH]` log lines from the daemon to measure actual operation time:

```
[BENCH] create_branch 'test': 287.461µs (287 us)
[BENCH] commit 'test': 321.194µs (321 us), 0 deletions, 1 files, 12 bytes
[BENCH] abort 'test': 156.234µs (156 us)
```

This excludes CLI process spawn and socket communication overhead (~200ms), giving accurate internal operation times.

## Output

- `bench_results.json`: Raw benchmark data
- `branch_creation_table.tex`: LaTeX table for paper
- `commit_table.tex`: LaTeX table for paper
