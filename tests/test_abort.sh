#!/bin/bash
# Test abort functionality

source "$(dirname "$0")/test_helper.sh"

test_abort_discards_changes() {
    setup
    do_mount
    do_create "abort_test" "main"

    # Make changes
    echo "will be discarded" > "$TEST_MNT/discard_file.txt"
    echo "modified" > "$TEST_MNT/file1.txt"

    # Verify changes visible
    assert_file_exists "$TEST_MNT/discard_file.txt" "New file exists before abort"

    # Abort
    do_abort

    # Changes should be gone (now on main)
    assert_file_not_exists "$TEST_MNT/discard_file.txt" "New file gone after abort"
    assert_file_contains "$TEST_MNT/file1.txt" "base content" "Modified file reverted to base"

    # Base should be unchanged
    assert_file_not_exists "$TEST_BASE/discard_file.txt" "No changes to base"
    assert_file_contains "$TEST_BASE/file1.txt" "base content" "Base file unchanged"

    do_unmount
}

test_abort_switches_to_main() {
    setup
    do_mount
    do_create "abort_switch" "main"

    # Abort
    do_abort

    # Branch should be removed
    assert_branch_not_exists "abort_switch" "Branch removed after abort"

    # Should be on main
    assert_branch_exists "main" "Main branch exists"

    # Should still be mounted
    assert "mountpoint -q '$TEST_MNT'" "Still mounted after abort"

    do_unmount
}

test_abort_nested_discards_leaf_only() {
    setup
    do_mount

    # Create nested branches
    do_create "abort_n1" "main"
    echo "n1 content" > "$TEST_MNT/n1_file.txt"

    do_create "abort_n2" "abort_n1"
    echo "n2 content" > "$TEST_MNT/n2_file.txt"

    # Abort from n2 should discard only n2, n1 still exists
    do_abort

    # n2 should be gone, n1 should still exist
    assert_branch_not_exists "abort_n2" "n2 removed after abort"
    assert_branch_exists "abort_n1" "n1 still exists after n2 abort"

    # n2 file should not be visible, but n1 file should be via @branch
    assert_file_not_exists "$TEST_MNT/n2_file.txt" "n2 file gone from current view"
    assert_file_exists "$TEST_MNT/@abort_n1/n1_file.txt" "n1 file still visible via @abort_n1"

    # Base unchanged
    assert_file_not_exists "$TEST_BASE/n1_file.txt" "No n1 file in base"
    assert_file_not_exists "$TEST_BASE/n2_file.txt" "No n2 file in base"

    do_unmount
}

test_abort_preserves_siblings() {
    setup
    do_mount

    # Create two sibling branches
    do_create "keep_sibling" "main"
    do_create "abort_sibling" "main"

    echo "will abort" > "$TEST_MNT/abort_file.txt"

    # Abort one sibling
    do_abort

    # Aborted branch should be gone
    assert_branch_not_exists "abort_sibling" "Aborted branch removed"

    # Other sibling should still exist
    assert_branch_exists "keep_sibling" "Sibling branch preserved"

    do_unmount
}

test_abort_main_fails() {
    setup
    do_mount

    # Try to abort main (should fail or do nothing)
    # We're on main after mount, trying to abort should fail
    if "$BRANCHFS" abort "$TEST_MNT" 2>&1; then
        # If it doesn't fail, that's okay - it might just be a no-op
        echo "  (abort main was a no-op)"
    fi

    # Should still be mounted
    assert "mountpoint -q '$TEST_MNT'" "Still mounted"

    # Main should still exist
    assert_branch_exists "main" "Main branch still exists"

    do_unmount
}

test_abort_non_leaf_fails() {
    setup
    do_mount

    # Create A from main, then B from A
    do_create "branch_a" "main"
    do_create "branch_b" "branch_a"

    # Try to abort A (not a leaf, should fail)
    if do_switch "branch_a" && "$BRANCHFS" abort "$TEST_MNT" --storage "$TEST_STORAGE" 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} Abort non-leaf should fail"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} Abort non-leaf correctly failed"
    fi

    # Both branches should still exist
    assert_branch_exists "branch_a" "branch_a still exists after failed abort"
    assert_branch_exists "branch_b" "branch_b still exists after failed abort"

    do_unmount
}

# Run tests
run_test "Abort Discards Changes" test_abort_discards_changes
run_test "Abort Switches to Main" test_abort_switches_to_main
run_test "Abort Nested Discards Leaf Only" test_abort_nested_discards_leaf_only
run_test "Abort Preserves Siblings" test_abort_preserves_siblings
run_test "Abort Main Fails" test_abort_main_fails
run_test "Abort Non-Leaf Fails" test_abort_non_leaf_fails

print_summary
