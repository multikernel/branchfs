#!/bin/bash
# Test @branch virtual directory feature

source "$(dirname "$0")/test_helper.sh"

test_branch_dir_appears_after_create() {
    setup
    do_mount

    # No @branch dirs initially (only main exists, and @main is not shown)
    assert "[[ ! -d '$TEST_MNT/@feature-a' ]]" "@feature-a does not exist before create"

    # Create a branch
    do_create "feature-a" "main"

    # @feature-a should now be visible
    assert "[[ -d '$TEST_MNT/@feature-a' ]]" "@feature-a dir exists after create"

    do_unmount
}

test_branch_dir_shows_base_files() {
    setup
    do_mount
    do_create "feature-a" "main"

    # @feature-a should show the same base files
    assert_file_exists "$TEST_MNT/@feature-a/file1.txt" "file1.txt visible via @feature-a"
    assert_file_contains "$TEST_MNT/@feature-a/file1.txt" "base content" "file1.txt has base content via @feature-a"
    assert_file_exists "$TEST_MNT/@feature-a/subdir/nested.txt" "Nested file visible via @feature-a"
    assert_file_contains "$TEST_MNT/@feature-a/subdir/nested.txt" "nested file" "Nested file has base content"

    do_unmount
}

test_branch_dir_write_isolation() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Write a file through the @branch path
    echo "branch a content" > "$TEST_MNT/@feature-a/branch_file.txt"

    # File should be visible via @branch
    assert_file_exists "$TEST_MNT/@feature-a/branch_file.txt" "New file visible via @feature-a"
    assert_file_contains "$TEST_MNT/@feature-a/branch_file.txt" "branch a content" "File has correct content via @feature-a"

    # Root view is currently on feature-a (auto-switched), so it sees the file too.
    # Switch root back to main to verify isolation.
    echo "switch:main" > "$TEST_MNT/.branchfs_ctl"
    sleep 0.3

    # Root (now on main) should NOT see the branch file
    assert_file_not_exists "$TEST_MNT/branch_file.txt" "Branch file not visible on root (main)"

    # @feature-a still shows it
    assert_file_exists "$TEST_MNT/@feature-a/branch_file.txt" "Branch file still visible via @feature-a"

    # Base should be unchanged
    assert_file_not_exists "$TEST_BASE/branch_file.txt" "Branch file not in base"

    do_unmount
}

test_branch_dir_modify_cow() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Modify a file through @branch path
    echo "modified via branch dir" > "$TEST_MNT/@feature-a/file1.txt"
    assert_file_contains "$TEST_MNT/@feature-a/file1.txt" "modified via branch dir" "Modified content via @feature-a"

    # Base should be unchanged
    assert_file_contains "$TEST_BASE/file1.txt" "base content" "Base file unchanged after branch write"

    do_unmount
}

test_branch_dir_ctl_file_exists() {
    setup
    do_mount
    do_create "feature-a" "main"

    # .branchfs_ctl should exist inside the @branch dir
    assert_file_exists "$TEST_MNT/@feature-a/.branchfs_ctl" "Branch ctl file exists in @feature-a"

    # Root ctl should also still exist
    assert_file_exists "$TEST_MNT/.branchfs_ctl" "Root ctl file still exists"

    do_unmount
}

test_branch_dir_commit_via_ctl() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Write a file via @branch path
    echo "to be committed" > "$TEST_MNT/@feature-a/committed_via_ctl.txt"
    assert_file_not_exists "$TEST_BASE/committed_via_ctl.txt" "File not in base before commit"

    # Commit via branch ctl
    echo "commit" > "$TEST_MNT/@feature-a/.branchfs_ctl"
    sleep 0.3

    # File should now be in base
    assert_file_exists "$TEST_BASE/committed_via_ctl.txt" "File in base after branch ctl commit"
    assert_file_contains "$TEST_BASE/committed_via_ctl.txt" "to be committed" "File has correct content in base"

    # Branch should be gone
    assert_branch_not_exists "feature-a" "Branch removed after commit via ctl"

    # @feature-a dir should no longer be accessible
    assert "[[ ! -d '$TEST_MNT/@feature-a' ]]" "@feature-a gone after commit"

    do_unmount
}

test_branch_dir_abort_via_ctl() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Write a file via @branch path
    echo "to be discarded" > "$TEST_MNT/@feature-a/discard_file.txt"
    assert_file_exists "$TEST_MNT/@feature-a/discard_file.txt" "File exists before abort"

    # Abort via branch ctl
    echo "abort" > "$TEST_MNT/@feature-a/.branchfs_ctl"
    sleep 0.3

    # Branch should be gone
    assert_branch_not_exists "feature-a" "Branch removed after abort via ctl"

    # Base should be unchanged
    assert_file_not_exists "$TEST_BASE/discard_file.txt" "No changes to base after abort"

    do_unmount
}

test_branch_dir_two_branches_isolated() {
    setup
    do_mount
    do_create "branch-a" "main"
    do_create "branch-b" "main"

    # Write different content to each branch via @branch paths
    echo "content a" > "$TEST_MNT/@branch-a/a_only.txt"
    echo "content b" > "$TEST_MNT/@branch-b/b_only.txt"

    # Each branch sees only its own file
    assert_file_exists "$TEST_MNT/@branch-a/a_only.txt" "a_only.txt visible in @branch-a"
    assert_file_not_exists "$TEST_MNT/@branch-a/b_only.txt" "b_only.txt NOT visible in @branch-a"
    assert_file_exists "$TEST_MNT/@branch-b/b_only.txt" "b_only.txt visible in @branch-b"
    assert_file_not_exists "$TEST_MNT/@branch-b/a_only.txt" "a_only.txt NOT visible in @branch-b"

    # Both branches see base files
    assert_file_exists "$TEST_MNT/@branch-a/file1.txt" "Base file visible in @branch-a"
    assert_file_exists "$TEST_MNT/@branch-b/file1.txt" "Base file visible in @branch-b"

    do_unmount
}

test_branch_dir_nested_child() {
    setup
    do_mount

    # Create parent, then child
    do_create "parent-br" "main"
    do_create "child-br" "parent-br"

    # @child-br should appear as a top-level @branch dir
    assert "[[ -d '$TEST_MNT/@child-br' ]]" "@child-br exists at top level"

    # @child-br should also appear nested under @parent-br
    assert "[[ -d '$TEST_MNT/@parent-br/@child-br' ]]" "@child-br nested under @parent-br"

    # Write to child branch, verify visible both ways
    echo "child content" > "$TEST_MNT/@child-br/child_file.txt"
    assert_file_exists "$TEST_MNT/@child-br/child_file.txt" "child_file.txt via @child-br"

    # Child branch should also see parent's files
    echo "parent content" > "$TEST_MNT/@parent-br/parent_file.txt"

    # Switch root to main so we don't confuse things
    echo "switch:main" > "$TEST_MNT/.branchfs_ctl"
    sleep 0.3

    assert_file_exists "$TEST_MNT/@child-br/parent_file.txt" "Child sees parent's file"

    do_unmount
}

test_branch_dir_bogus_enoent() {
    setup
    do_mount

    # Accessing a nonexistent branch should fail
    assert "[[ ! -d '$TEST_MNT/@nonexistent' ]]" "@nonexistent does not exist"
    assert "[[ ! -f '$TEST_MNT/@nonexistent/file.txt' ]]" "File in @nonexistent not accessible"

    do_unmount
}

test_branch_dir_delete_file() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Verify base file visible through @branch
    assert_file_exists "$TEST_MNT/@feature-a/file1.txt" "file1.txt visible before delete"

    # Delete via @branch path
    rm "$TEST_MNT/@feature-a/file1.txt"

    # File should be gone in branch view
    assert_file_not_exists "$TEST_MNT/@feature-a/file1.txt" "file1.txt gone via @feature-a after delete"

    # Base file should be untouched
    assert_file_exists "$TEST_BASE/file1.txt" "Base file1.txt still exists"

    do_unmount
}

test_branch_dir_mkdir() {
    setup
    do_mount
    do_create "feature-a" "main"

    # Create a directory via @branch path
    mkdir "$TEST_MNT/@feature-a/newdir"
    assert "[[ -d '$TEST_MNT/@feature-a/newdir' ]]" "New directory created via @feature-a"

    # Create a file inside
    echo "in new dir" > "$TEST_MNT/@feature-a/newdir/inner.txt"
    assert_file_exists "$TEST_MNT/@feature-a/newdir/inner.txt" "File in new dir exists"
    assert_file_contains "$TEST_MNT/@feature-a/newdir/inner.txt" "in new dir" "File has correct content"

    # Should not exist in base
    assert "[[ ! -d '$TEST_BASE/newdir' ]]" "New dir not in base"

    do_unmount
}

# Run tests
run_test "@branch Dir Appears After Create" test_branch_dir_appears_after_create
run_test "@branch Dir Shows Base Files" test_branch_dir_shows_base_files
run_test "@branch Dir Write Isolation" test_branch_dir_write_isolation
run_test "@branch Dir Modify COW" test_branch_dir_modify_cow
run_test "@branch Dir Ctl File Exists" test_branch_dir_ctl_file_exists
run_test "@branch Dir Commit via Ctl" test_branch_dir_commit_via_ctl
run_test "@branch Dir Abort via Ctl" test_branch_dir_abort_via_ctl
run_test "@branch Dir Two Branches Isolated" test_branch_dir_two_branches_isolated
run_test "@branch Dir Nested Child" test_branch_dir_nested_child
run_test "@branch Dir Bogus ENOENT" test_branch_dir_bogus_enoent
run_test "@branch Dir Delete File" test_branch_dir_delete_file
run_test "@branch Dir Mkdir" test_branch_dir_mkdir

print_summary
