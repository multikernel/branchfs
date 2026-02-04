#!/bin/bash
# Test branch name validation

source "$(dirname "$0")/test_helper.sh"

# Helper: assert that branch creation fails with a specific error substring
assert_create_fails() {
    local name="$1"
    local expected_err="$2"
    local message="$3"

    local output
    output=$("$BRANCHFS" create "$name" "$TEST_MNT" -p main --storage "$TEST_STORAGE" 2>&1) && {
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} $message"
        echo -e "    ${RED}Expected failure but command succeeded${NC}"
        return 1
    }

    TESTS_RUN=$((TESTS_RUN + 1))
    if [[ "$output" == *"$expected_err"* ]]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} $message"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} $message"
        echo -e "    ${RED}Expected error containing: $expected_err${NC}"
        echo -e "    ${RED}Actual output: $output${NC}"
        return 1
    fi
}

test_reject_empty_name() {
    setup
    do_mount

    assert_create_fails "" "empty" "Rejects empty branch name"

    do_unmount
}

test_reject_dot_names() {
    setup
    do_mount

    assert_create_fails ".." "not a valid branch name" "Rejects '..' as branch name"

    do_unmount
}

test_reject_slash_in_name() {
    setup
    do_mount

    assert_create_fails "foo/bar" "cannot contain '/'" "Rejects name with '/'"

    do_unmount
}

test_reject_at_prefix() {
    setup
    do_mount

    assert_create_fails "@mybranch" "cannot start with '@'" "Rejects name starting with '@'"

    do_unmount
}

test_valid_names_work() {
    setup
    do_mount

    do_create "feature-1"
    assert_branch_exists "feature-1" "Hyphenated name works"

    do_create "my_branch"
    assert_branch_exists "my_branch" "Underscored name works"

    do_create "release42"
    assert_branch_exists "release42" "Alphanumeric name works"

    do_unmount
}

# Run tests
run_test "Reject Empty Name" test_reject_empty_name
run_test "Reject Dot Names" test_reject_dot_names
run_test "Reject Slash in Name" test_reject_slash_in_name
run_test "Reject @ Prefix" test_reject_at_prefix
run_test "Valid Names Work" test_valid_names_work

print_summary
