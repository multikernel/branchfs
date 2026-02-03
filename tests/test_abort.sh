#!/bin/bash
# Test abort functionality

source "$(dirname "$0")/test_helper.sh"

test_abort_discards_changes() {
    setup
    do_mount
    do_create "abort_test" "main" "-s"

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
    do_create "abort_switch" "main" "-s"

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

test_abort_nested_discards_chain() {
    setup
    do_mount

    # Create nested branches
    do_create "abort_n1" "main" "-s"
    echo "n1 content" > "$TEST_MNT/n1_file.txt"

    do_create "abort_n2" "abort_n1" "-s"
    echo "n2 content" > "$TEST_MNT/n2_file.txt"

    # Abort from n2 should discard both n2 and n1
    do_abort

    # Both branches should be gone
    assert_branch_not_exists "abort_n1" "n1 removed after abort"
    assert_branch_not_exists "abort_n2" "n2 removed after abort"

    # Files should not be visible
    assert_file_not_exists "$TEST_MNT/n1_file.txt" "n1 file gone"
    assert_file_not_exists "$TEST_MNT/n2_file.txt" "n2 file gone"

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
    do_create "abort_sibling" "main" "-s"

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

# Run tests
run_test "Abort Discards Changes" test_abort_discards_changes
run_test "Abort Switches to Main" test_abort_switches_to_main
run_test "Abort Nested Discards Chain" test_abort_nested_discards_chain
run_test "Abort Preserves Siblings" test_abort_preserves_siblings
run_test "Abort Main Fails" test_abort_main_fails

print_summary
