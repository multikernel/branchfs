#!/bin/bash
# Test helper functions for branchfs tests
# Source this file in test scripts: source "$(dirname "$0")/test_helper.sh"

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counters
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BRANCHFS="$PROJECT_ROOT/target/release/branchfs"

# Create unique test directories for this test run
TEST_ID="$$_$(date +%s)"
TEST_BASE="/tmp/branchfs_test_base_$TEST_ID"
TEST_STORAGE="/tmp/branchfs_test_storage_$TEST_ID"
TEST_MNT="/tmp/branchfs_test_mnt_$TEST_ID"

# Track if we've set up
SETUP_DONE=0

# Build the project if needed
build_if_needed() {
    if [[ ! -x "$BRANCHFS" ]]; then
        echo -e "${YELLOW}Building branchfs...${NC}"
        (cd "$PROJECT_ROOT" && cargo build --release)
    fi
}

# Set up test environment
setup() {
    build_if_needed

    # Create test directories
    mkdir -p "$TEST_BASE"
    mkdir -p "$TEST_STORAGE"
    mkdir -p "$TEST_MNT"

    # Create some initial files in base
    echo "base content" > "$TEST_BASE/file1.txt"
    echo "another file" > "$TEST_BASE/file2.txt"
    mkdir -p "$TEST_BASE/subdir"
    echo "nested file" > "$TEST_BASE/subdir/nested.txt"

    SETUP_DONE=1
    echo -e "${GREEN}Test environment set up${NC}"
    echo "  BASE:    $TEST_BASE"
    echo "  STORAGE: $TEST_STORAGE"
    echo "  MNT:     $TEST_MNT"
}

# Clean up test environment
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"

    # Try to unmount if mounted
    if mountpoint -q "$TEST_MNT" 2>/dev/null; then
        fusermount3 -u "$TEST_MNT" 2>/dev/null || fusermount -u "$TEST_MNT" 2>/dev/null || true
        sleep 0.5
    fi

    # Kill any daemon that might be running with our storage
    local socket="$TEST_STORAGE/daemon.sock"
    if [[ -S "$socket" ]]; then
        # Send shutdown request
        echo '{"cmd":"shutdown"}' | nc -U "$socket" 2>/dev/null || true
        sleep 0.5
    fi

    # Remove test directories
    rm -rf "$TEST_BASE" 2>/dev/null || true
    rm -rf "$TEST_STORAGE" 2>/dev/null || true
    rm -rf "$TEST_MNT" 2>/dev/null || true

    echo -e "${GREEN}Cleanup complete${NC}"
}

# Ensure cleanup on exit
trap cleanup EXIT

# Mount the filesystem
do_mount() {
    "$BRANCHFS" mount --base "$TEST_BASE" --storage "$TEST_STORAGE" "$TEST_MNT"
    sleep 0.5  # Give FUSE time to initialize
}

# Unmount the filesystem
do_unmount() {
    "$BRANCHFS" unmount "$TEST_MNT" --storage "$TEST_STORAGE"
    sleep 0.3
}

# Create a branch
# Usage: do_create <name> [parent] [switch_flag]
# - name: branch name
# - parent: parent branch (default: main)
# - switch_flag: pass "-s" to switch to the new branch after creation
do_create() {
    local name="$1"
    local parent="${2:-main}"
    local switch_flag="${3:-}"

    if [[ -n "$switch_flag" ]]; then
        "$BRANCHFS" create "$name" "$TEST_MNT" -p "$parent" -s --storage "$TEST_STORAGE"
    else
        "$BRANCHFS" create "$name" "$TEST_MNT" -p "$parent" --storage "$TEST_STORAGE"
    fi
    sleep 0.3
}

# Commit changes
do_commit() {
    "$BRANCHFS" commit "$TEST_MNT" --storage "$TEST_STORAGE"
}

# Abort changes
do_abort() {
    "$BRANCHFS" abort "$TEST_MNT" --storage "$TEST_STORAGE"
}

# List branches
do_list() {
    "$BRANCHFS" list "$TEST_MNT" --storage "$TEST_STORAGE"
}

# Assert that a condition is true
assert() {
    local condition="$1"
    local message="$2"

    TESTS_RUN=$((TESTS_RUN + 1))

    if eval "$condition"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} $message"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} $message"
        echo -e "    ${RED}Condition failed: $condition${NC}"
        return 1
    fi
}

# Assert that two values are equal
assert_eq() {
    local actual="$1"
    local expected="$2"
    local message="$3"

    TESTS_RUN=$((TESTS_RUN + 1))

    if [[ "$actual" == "$expected" ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} $message"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} $message"
        echo -e "    ${RED}Expected: $expected${NC}"
        echo -e "    ${RED}Actual:   $actual${NC}"
        return 1
    fi
}

# Assert that a file exists
assert_file_exists() {
    local file="$1"
    local message="${2:-File $file exists}"
    assert "[[ -f '$file' ]]" "$message"
}

# Assert that a file does not exist
assert_file_not_exists() {
    local file="$1"
    local message="${2:-File $file does not exist}"
    assert "[[ ! -f '$file' ]]" "$message"
}

# Assert that a file contains specific content
assert_file_contains() {
    local file="$1"
    local expected="$2"
    local message="${3:-File $file contains expected content}"

    if [[ -f "$file" ]]; then
        local actual
        actual=$(cat "$file")
        assert_eq "$actual" "$expected" "$message"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} $message"
        echo -e "    ${RED}File does not exist: $file${NC}"
        return 1
    fi
}

# Assert that mount is on a specific branch (check via listing)
assert_branch_exists() {
    local branch="$1"
    local message="${2:-Branch $branch exists}"

    local output
    output=$(do_list 2>&1)
    assert "[[ '$output' == *'$branch'* ]]" "$message"
}

# Assert that a branch does not exist
assert_branch_not_exists() {
    local branch="$1"
    local message="${2:-Branch $branch does not exist}"

    local output
    output=$(do_list 2>&1)
    assert "[[ '$output' != *'$branch'* ]]" "$message"
}

# Print test summary
print_summary() {
    echo ""
    echo "=================================="
    echo "Test Summary"
    echo "=================================="
    echo -e "Total:  $TESTS_RUN"
    echo -e "Passed: ${GREEN}$TESTS_PASSED${NC}"
    echo -e "Failed: ${RED}$TESTS_FAILED${NC}"
    echo "=================================="

    if [[ $TESTS_FAILED -gt 0 ]]; then
        return 1
    fi
    return 0
}

# Run a test function
run_test() {
    local test_name="$1"
    local test_func="$2"

    echo ""
    echo -e "${YELLOW}Running: $test_name${NC}"
    echo "-----------------------------------"

    if $test_func; then
        echo -e "${GREEN}$test_name: PASSED${NC}"
    else
        echo -e "${RED}$test_name: FAILED${NC}"
    fi
}
