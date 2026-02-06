#!/bin/bash
# Quick BranchFS benchmark script
# Usage: ./quick_bench.sh [branchfs_binary]
#
# This script measures:
# - Branch creation (via daemon socket - includes CLI overhead)
# - Commit latency (via .branchfs_ctl - pure FUSE operation)
# - Read latency

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_BRANCHFS="$SCRIPT_DIR/../target/release/branchfs"
BRANCHFS="${1:-$DEFAULT_BRANCHFS}"

if [ ! -x "$BRANCHFS" ]; then
    echo "Error: branchfs binary not found at $BRANCHFS"
    echo "Build with: cd $(dirname $SCRIPT_DIR) && cargo build --release"
    exit 1
fi

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "Using branchfs: $BRANCHFS"
echo "Temp dir: $TMPDIR"

# Helper to measure time in microseconds using high-res timer
time_us() {
    local start=$(date +%s%N)
    "$@" >/dev/null 2>&1
    local end=$(date +%s%N)
    echo $(( (end - start) / 1000 ))
}

# Helper to time a file write (pure FUSE operation)
time_write_us() {
    local file="$1"
    local content="$2"
    local start=$(date +%s%N)
    echo -n "$content" > "$file"
    local end=$(date +%s%N)
    echo $(( (end - start) / 1000 ))
}

# ============================================================================
# Test 1: Branch Creation Latency (via CLI - includes socket overhead)
# ============================================================================
echo ""
echo "=== Branch Creation Latency (CLI, includes socket overhead) ==="

for num_files in 100 1000 10000; do
    BASE="$TMPDIR/base_$num_files"
    MNT="$TMPDIR/mnt_$num_files"
    STORAGE="$TMPDIR/storage_$num_files"

    mkdir -p "$BASE" "$MNT" "$STORAGE"

    # Create base files
    echo "  Creating $num_files files..."
    for i in $(seq 1 $num_files); do
        dir="$BASE/dir_$((i / 100))"
        mkdir -p "$dir"
        dd if=/dev/urandom of="$dir/file_$i.txt" bs=1024 count=1 2>/dev/null
    done

    # Mount
    $BRANCHFS mount --base "$BASE" --storage "$STORAGE" "$MNT"
    sleep 0.2

    # Measure branch creation (average of 5)
    total=0
    for i in 1 2 3 4 5; do
        lat=$(time_us $BRANCHFS create "branch_$i" "$MNT" --storage "$STORAGE")
        total=$((total + lat))
        $BRANCHFS abort "$MNT" --storage "$STORAGE" 2>/dev/null || true
    done
    avg=$((total / 5))

    echo "  $num_files files: $avg us ($((avg / 1000)) ms) - includes CLI/socket overhead"

    # Cleanup
    $BRANCHFS unmount "$MNT" --storage "$STORAGE" 2>/dev/null || fusermount3 -u "$MNT" 2>/dev/null || true
done

# ============================================================================
# Test 2: Commit Latency (direct .branchfs_ctl write - pure FUSE)
# ============================================================================
echo ""
echo "=== Commit Latency (direct ctl write, pure FUSE) ==="

BASE="$TMPDIR/commit_base"
mkdir -p "$BASE"
for i in $(seq 1 100); do
    dd if=/dev/urandom of="$BASE/file_$i.txt" bs=1024 count=10 2>/dev/null
done

for mod_kb in 1 10 100 1000; do
    MNT="$TMPDIR/commit_mnt_$mod_kb"
    STORAGE="$TMPDIR/commit_storage_$mod_kb"
    mkdir -p "$MNT" "$STORAGE"

    $BRANCHFS mount --base "$BASE" --storage "$STORAGE" "$MNT"
    sleep 0.1

    $BRANCHFS create "bench" "$MNT" --storage "$STORAGE"

    # Make modification
    dd if=/dev/urandom of="$MNT/mod.bin" bs=1024 count=$mod_kb 2>/dev/null
    sync

    # Measure commit via direct ctl file write (pure FUSE operation)
    lat=$(time_write_us "$MNT/.branchfs_ctl" "commit")
    echo "  ${mod_kb} KB modification: $lat us ($((lat / 1000)) ms)"

    # Cleanup
    $BRANCHFS unmount "$MNT" --storage "$STORAGE" 2>/dev/null || fusermount3 -u "$MNT" 2>/dev/null || true
done

# ============================================================================
# Test 3: Abort Latency (direct .branchfs_ctl write - pure FUSE)
# ============================================================================
echo ""
echo "=== Abort Latency (direct ctl write, pure FUSE) ==="

for mod_kb in 1 100 1000; do
    MNT="$TMPDIR/abort_mnt_$mod_kb"
    STORAGE="$TMPDIR/abort_storage_$mod_kb"
    mkdir -p "$MNT" "$STORAGE"

    $BRANCHFS mount --base "$BASE" --storage "$STORAGE" "$MNT"
    sleep 0.1

    $BRANCHFS create "bench" "$MNT" --storage "$STORAGE"

    # Make modification
    dd if=/dev/urandom of="$MNT/mod.bin" bs=1024 count=$mod_kb 2>/dev/null
    sync

    # Measure abort via direct ctl file write
    lat=$(time_write_us "$MNT/.branchfs_ctl" "abort")
    echo "  ${mod_kb} KB to discard: $lat us ($((lat / 1000)) ms)"

    # Cleanup
    $BRANCHFS unmount "$MNT" --storage "$STORAGE" 2>/dev/null || fusermount3 -u "$MNT" 2>/dev/null || true
done

# ============================================================================
# Test 4: Read Latency (file on base, read through branch)
# ============================================================================
echo ""
echo "=== Read Latency ==="

MNT="$TMPDIR/read_mnt"
STORAGE="$TMPDIR/read_storage"
mkdir -p "$MNT" "$STORAGE"

# Create test file
dd if=/dev/urandom of="$BASE/testfile.bin" bs=1M count=1 2>/dev/null

$BRANCHFS mount --base "$BASE" --storage "$STORAGE" "$MNT"
sleep 0.1
$BRANCHFS create "bench" "$MNT" --storage "$STORAGE"

# Warm up
cat "$MNT/testfile.bin" > /dev/null

# Measure
total=0
for i in 1 2 3 4 5; do
    lat=$(time_us cat "$MNT/testfile.bin")
    total=$((total + lat))
done
avg=$((total / 5))
echo "  1MB file read: $avg us ($((avg / 1000)) ms)"

$BRANCHFS unmount "$MNT" --storage "$STORAGE" 2>/dev/null || fusermount3 -u "$MNT" 2>/dev/null || true

# ============================================================================
# Test 5: Switch Branch Latency (direct ctl write)
# ============================================================================
echo ""
echo "=== Switch Branch Latency (direct ctl write, pure FUSE) ==="

MNT="$TMPDIR/switch_mnt"
STORAGE="$TMPDIR/switch_storage"
mkdir -p "$MNT" "$STORAGE"

$BRANCHFS mount --base "$BASE" --storage "$STORAGE" "$MNT"
sleep 0.1

# Create multiple branches
$BRANCHFS create "branch_a" "$MNT" --storage "$STORAGE"
$BRANCHFS create "branch_b" "$MNT" --storage "$STORAGE"

# Measure switch latency
total=0
for i in 1 2 3 4 5; do
    lat=$(time_write_us "$MNT/.branchfs_ctl" "switch:branch_a")
    total=$((total + lat))
    echo -n "switch:branch_b" > "$MNT/.branchfs_ctl"
done
avg=$((total / 5))
echo "  Switch branch: $avg us"

$BRANCHFS unmount "$MNT" --storage "$STORAGE" 2>/dev/null || fusermount3 -u "$MNT" 2>/dev/null || true

echo ""
echo "=== Done ==="
echo ""
echo "Note: Branch creation goes through CLI+socket, so it shows higher latency."
echo "      Commit/abort/switch go directly through FUSE (.branchfs_ctl)."
