#!/bin/bash
# Test unmount functionality (per-mount isolation: full cleanup on unmount)

source "$(dirname "$0")/test_helper.sh"

test_unmount_main() {
    setup
    do_mount

    # Unmount main
    do_unmount

    # Should be unmounted
    assert "! mountpoint -q '$TEST_MNT'" "Mount point unmounted"

    # With per-mount isolation, mount storage is cleaned up on unmount
    # Remounting creates a fresh mount with only main branch
}

test_unmount_discards_single_branch() {
    setup
    do_mount
    do_create "unmount_test" "main" "-s"

    echo "branch content" > "$TEST_MNT/branch_file.txt"

    # Unmount (should discard the branch)
    do_unmount

    # Should be unmounted
    assert "! mountpoint -q '$TEST_MNT'" "Mount point unmounted"

    # No changes to base
    assert_file_not_exists "$TEST_BASE/branch_file.txt" "No changes to base"
}

test_unmount_cleans_all_branches() {
    setup
    do_mount

    # Create nested branches
    do_create "parent_branch" "main" "-s"
    echo "parent content" > "$TEST_MNT/parent_file.txt"

    do_create "child_branch" "parent_branch" "-s"
    echo "child content" > "$TEST_MNT/child_file.txt"

    # Unmount (per-mount isolation: cleans up ALL branches for this mount)
    do_unmount

    # Should be unmounted
    assert "! mountpoint -q '$TEST_MNT'" "Mount point unmounted"

    # Remount - starts fresh with only main branch
    do_mount

    # Per-mount isolation: all branches are cleaned up, only main exists
    assert_branch_exists "main" "Main branch exists after remount"
    assert_branch_not_exists "parent_branch" "Parent branch cleaned up on unmount"
    assert_branch_not_exists "child_branch" "Child branch cleaned up on unmount"

    # No changes to base (nothing was committed)
    assert_file_not_exists "$TEST_BASE/parent_file.txt" "No parent file in base"
    assert_file_not_exists "$TEST_BASE/child_file.txt" "No child file in base"

    do_unmount
}

test_unmount_cleanup() {
    setup
    do_mount
    do_create "cleanup_test" "main" "-s"

    # Create some files
    echo "test" > "$TEST_MNT/test.txt"

    # Check that mount storage exists before unmount
    local mounts_dir="$TEST_STORAGE/mounts"
    assert "[[ -d '$mounts_dir' ]]" "Mounts directory exists before unmount"

    # Get mount count before unmount
    local mount_count_before
    mount_count_before=$(ls "$mounts_dir" 2>/dev/null | wc -l)
    assert "[[ $mount_count_before -gt 0 ]]" "Mount storage exists before unmount"

    # Unmount
    do_unmount

    # Mount-specific storage should be cleaned up
    # (mounts directory may still exist but should be empty)
    local mount_count_after
    mount_count_after=$(ls "$mounts_dir" 2>/dev/null | wc -l)
    assert "[[ $mount_count_after -eq 0 ]]" "Mount storage cleaned up on unmount"
}

# Run tests
run_test "Unmount Main" test_unmount_main
run_test "Unmount Discards Single Branch" test_unmount_discards_single_branch
run_test "Unmount Cleans All Branches (Per-Mount Isolation)" test_unmount_cleans_all_branches
run_test "Unmount Cleanup" test_unmount_cleanup

print_summary
