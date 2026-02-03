#!/bin/bash
# Test branch creation functionality

source "$(dirname "$0")/test_helper.sh"

test_create_branch_without_mount() {
    setup
    do_mount

    # Create a branch without mounting
    do_create "feature1"

    # Branch should exist in list
    assert_branch_exists "feature1" "Branch feature1 exists in list"
    assert_branch_exists "main" "Branch main still exists"

    do_unmount
}

test_create_branch_with_switch() {
    setup
    do_mount

    # Create base file visibility check
    assert_file_contains "$TEST_MNT/file1.txt" "base content" "Before branch: base content visible"

    # Create a branch and switch to it
    do_create "feature2" "main"

    # Should still see base files
    assert_file_exists "$TEST_MNT/file1.txt" "After branch: file1.txt still visible"
    assert_file_contains "$TEST_MNT/file1.txt" "base content" "After branch: content unchanged"

    # Write a new file in the branch
    echo "feature content" > "$TEST_MNT/feature_file.txt"
    assert_file_exists "$TEST_MNT/feature_file.txt" "New file created in branch"

    # Branch should exist
    assert_branch_exists "feature2" "Branch feature2 exists"

    do_unmount
}

test_create_nested_branches() {
    setup
    do_mount

    # Create level1 branch
    do_create "level1" "main"
    echo "level1 content" > "$TEST_MNT/level1_file.txt"

    # Create level2 branch from level1
    do_create "level2" "level1"
    echo "level2 content" > "$TEST_MNT/level2_file.txt"

    # Should see files from all levels
    assert_file_exists "$TEST_MNT/file1.txt" "Base file visible at level2"
    assert_file_exists "$TEST_MNT/level1_file.txt" "Level1 file visible at level2"
    assert_file_exists "$TEST_MNT/level2_file.txt" "Level2 file visible"

    # All branches should exist
    assert_branch_exists "main" "main branch exists"
    assert_branch_exists "level1" "level1 branch exists"
    assert_branch_exists "level2" "level2 branch exists"

    do_unmount
}

test_create_sibling_branches() {
    setup
    do_mount

    # Create two sibling branches from main
    do_create "sibling1" "main"
    do_create "sibling2" "main"

    # Both should exist
    assert_branch_exists "sibling1" "sibling1 exists"
    assert_branch_exists "sibling2" "sibling2 exists"

    do_unmount
}

# Run tests
run_test "Create Branch Without Mount" test_create_branch_without_mount
run_test "Create Branch and Auto-Switch" test_create_branch_with_switch
run_test "Create Nested Branches" test_create_nested_branches
run_test "Create Sibling Branches" test_create_sibling_branches

print_summary
