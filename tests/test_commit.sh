#!/bin/bash
# Test commit functionality

source "$(dirname "$0")/test_helper.sh"

test_commit_new_file() {
    setup
    do_mount
    do_create "commit_new" "main"

    # Create a new file
    echo "committed content" > "$TEST_MNT/committed_file.txt"
    assert_file_not_exists "$TEST_BASE/committed_file.txt" "File not in base before commit"

    # Commit
    do_commit

    # File should now be in base
    assert_file_exists "$TEST_BASE/committed_file.txt" "File exists in base after commit"
    assert_file_contains "$TEST_BASE/committed_file.txt" "committed content" "File has correct content in base"

    # Should still be mounted (on main now)
    assert "mountpoint -q '$TEST_MNT'" "Still mounted after commit"

    # File should still be visible in mount
    assert_file_exists "$TEST_MNT/committed_file.txt" "File visible in mount after commit"

    do_unmount
}

test_commit_modified_file() {
    setup
    do_mount
    do_create "commit_mod" "main"

    # Modify existing file
    echo "modified for commit" > "$TEST_MNT/file1.txt"
    assert_file_contains "$TEST_BASE/file1.txt" "base content" "Base unchanged before commit"

    # Commit
    do_commit

    # Base should have modified content
    assert_file_contains "$TEST_BASE/file1.txt" "modified for commit" "Base has modified content after commit"

    do_unmount
}

test_commit_deleted_file() {
    setup
    do_mount
    do_create "commit_del" "main"

    # Delete a file
    rm "$TEST_MNT/file2.txt"
    assert_file_exists "$TEST_BASE/file2.txt" "Base file exists before commit"

    # Commit
    do_commit

    # Base file should be deleted
    assert_file_not_exists "$TEST_BASE/file2.txt" "Base file deleted after commit"

    do_unmount
}

test_commit_switches_to_main() {
    setup
    do_mount
    do_create "commit_switch" "main"

    echo "branch content" > "$TEST_MNT/branch_file.txt"

    # Commit
    do_commit

    # Branch should no longer exist
    assert_branch_not_exists "commit_switch" "Branch removed after commit"

    # Main should still exist
    assert_branch_exists "main" "Main branch exists"

    # Mount should show committed content (now on main)
    assert_file_exists "$TEST_MNT/branch_file.txt" "Committed file visible on main"

    do_unmount
}

test_commit_nested_branches() {
    setup
    do_mount

    # Create nested branches
    do_create "nest1" "main"
    echo "nest1 content" > "$TEST_MNT/nest1_file.txt"

    do_create "nest2" "nest1"
    echo "nest2 content" > "$TEST_MNT/nest2_file.txt"

    # Verify both files visible
    assert_file_exists "$TEST_MNT/nest1_file.txt" "nest1 file visible"
    assert_file_exists "$TEST_MNT/nest2_file.txt" "nest2 file visible"

    # Commit from nest2 — merges into nest1, NOT to base
    do_commit

    # nest2 should be gone, nest1 should still exist
    assert_branch_not_exists "nest2" "nest2 removed after commit"
    assert_branch_exists "nest1" "nest1 still exists after nest2 commit"

    # Files should NOT be in base yet
    assert_file_not_exists "$TEST_BASE/nest1_file.txt" "nest1 file NOT in base yet"
    assert_file_not_exists "$TEST_BASE/nest2_file.txt" "nest2 file NOT in base yet"

    # But nest2_file should be visible via nest1's @branch path
    assert_file_exists "$TEST_MNT/@nest1/nest2_file.txt" "nest2 file visible via @nest1"
    assert_file_exists "$TEST_MNT/@nest1/nest1_file.txt" "nest1 file visible via @nest1"

    # Now switch to nest1 and commit to base
    do_switch "nest1"
    do_commit

    # Both files should now be in base
    assert_file_exists "$TEST_BASE/nest1_file.txt" "nest1 file in base after nest1 commit"
    assert_file_exists "$TEST_BASE/nest2_file.txt" "nest2 file in base after nest1 commit"

    # nest1 should be gone
    assert_branch_not_exists "nest1" "nest1 removed after commit to base"

    do_unmount
}

test_commit_preserves_siblings() {
    setup
    do_mount

    # Create two sibling branches
    do_create "sibling_a" "main"
    do_create "sibling_b" "main"

    echo "sibling b content" > "$TEST_MNT/sibling_b_file.txt"

    # Commit sibling_b — only sibling_b is removed, sibling_a preserved
    do_commit

    assert_branch_not_exists "sibling_b" "sibling_b removed after commit"
    assert_branch_exists "sibling_a" "sibling_a preserved after sibling commit"

    do_unmount
}

test_commit_non_leaf_fails() {
    setup
    do_mount

    # Create A from main, then B from A
    do_create "branch_a" "main"
    do_create "branch_b" "branch_a"

    # Try to commit A (not a leaf, should fail)
    if do_switch "branch_a" && "$BRANCHFS" commit "$TEST_MNT" --storage "$TEST_STORAGE" 2>/dev/null; then
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "  ${RED}✗${NC} Commit non-leaf should fail"
    else
        TESTS_RUN=$((TESTS_RUN + 1))
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "  ${GREEN}✓${NC} Commit non-leaf correctly failed"
    fi

    # Both branches should still exist
    assert_branch_exists "branch_a" "branch_a still exists after failed commit"
    assert_branch_exists "branch_b" "branch_b still exists after failed commit"

    do_unmount
}

# Run tests
run_test "Commit New File" test_commit_new_file
run_test "Commit Modified File" test_commit_modified_file
run_test "Commit Deleted File" test_commit_deleted_file
run_test "Commit Switches to Main" test_commit_switches_to_main
run_test "Commit Nested Branches" test_commit_nested_branches
run_test "Commit Preserves Siblings" test_commit_preserves_siblings
run_test "Commit Non-Leaf Fails" test_commit_non_leaf_fails

print_summary
