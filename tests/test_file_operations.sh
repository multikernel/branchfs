#!/bin/bash
# Test file operations (read, write, create, delete)

source "$(dirname "$0")/test_helper.sh"

test_read_base_files() {
    setup
    do_mount

    # Read files from base
    assert_file_contains "$TEST_MNT/file1.txt" "base content" "Can read file1.txt"
    assert_file_contains "$TEST_MNT/file2.txt" "another file" "Can read file2.txt"
    assert_file_contains "$TEST_MNT/subdir/nested.txt" "nested file" "Can read nested file"

    do_unmount
}

test_write_new_file_in_branch() {
    setup
    do_mount
    do_create "write_test" "main"

    # Create a new file
    echo "new file content" > "$TEST_MNT/new_file.txt"
    assert_file_exists "$TEST_MNT/new_file.txt" "New file created"
    assert_file_contains "$TEST_MNT/new_file.txt" "new file content" "New file has correct content"

    # File should NOT exist in base (not committed)
    assert_file_not_exists "$TEST_BASE/new_file.txt" "New file not in base yet"

    do_unmount
}

test_modify_existing_file_cow() {
    setup
    do_mount
    do_create "cow_test" "main"

    # Modify an existing file (triggers copy-on-write)
    echo "modified content" > "$TEST_MNT/file1.txt"
    assert_file_contains "$TEST_MNT/file1.txt" "modified content" "File shows modified content"

    # Base file should be unchanged
    assert_file_contains "$TEST_BASE/file1.txt" "base content" "Base file unchanged"

    do_unmount
}

test_delete_file_in_branch() {
    setup
    do_mount
    do_create "delete_test" "main"

    # File exists initially
    assert_file_exists "$TEST_MNT/file1.txt" "File exists before delete"

    # Delete the file
    rm "$TEST_MNT/file1.txt"
    assert_file_not_exists "$TEST_MNT/file1.txt" "File deleted in branch"

    # Base file should still exist
    assert_file_exists "$TEST_BASE/file1.txt" "Base file still exists"

    do_unmount
}

test_create_directory_in_branch() {
    setup
    do_mount
    do_create "mkdir_test" "main"

    # Create a new directory
    mkdir "$TEST_MNT/newdir"
    assert "[[ -d '$TEST_MNT/newdir' ]]" "New directory created"

    # Create file in new directory
    echo "content" > "$TEST_MNT/newdir/file.txt"
    assert_file_exists "$TEST_MNT/newdir/file.txt" "File in new directory exists"

    # Directory should NOT exist in base
    assert "[[ ! -d '$TEST_BASE/newdir' ]]" "New directory not in base"

    do_unmount
}

test_delete_directory_in_branch() {
    setup
    do_mount
    do_create "rmdir_test" "main"

    # Directory exists initially
    assert "[[ -d '$TEST_MNT/subdir' ]]" "Subdir exists before delete"

    # Delete the directory
    rm -rf "$TEST_MNT/subdir"
    assert "[[ ! -d '$TEST_MNT/subdir' ]]" "Subdir deleted in branch"

    # Base directory should still exist
    assert "[[ -d '$TEST_BASE/subdir' ]]" "Base subdir still exists"

    do_unmount
}

test_append_to_file() {
    setup
    do_mount
    do_create "append_test" "main"

    # Append to existing file
    echo "appended line" >> "$TEST_MNT/file1.txt"

    local content
    content=$(cat "$TEST_MNT/file1.txt")
    assert "[[ '$content' == *'base content'* ]]" "Original content preserved"
    assert "[[ '$content' == *'appended line'* ]]" "Appended content present"

    do_unmount
}

test_create_file_permissions() {
    setup
    do_mount
    do_create "perm_create_test" "main"

    # Create a file — default mode should not be 000
    touch "$TEST_MNT/perm_file.txt"
    local mode
    mode=$(stat -c '%a' "$TEST_MNT/perm_file.txt")
    assert "[[ '$mode' != '000' ]]" "Created file has non-zero permissions ($mode)"

    # Verify owner is current user
    local file_uid
    file_uid=$(stat -c '%u' "$TEST_MNT/perm_file.txt")
    assert_eq "$file_uid" "$(id -u)" "Created file owned by current user"

    do_unmount
}

test_mkdir_permissions() {
    setup
    do_mount
    do_create "perm_mkdir_test" "main"

    mkdir "$TEST_MNT/perm_dir"
    local mode
    mode=$(stat -c '%a' "$TEST_MNT/perm_dir")
    # mkdir should produce a directory with reasonable permissions (not 000)
    assert "[[ '$mode' != '000' ]]" "Created directory has non-zero permissions ($mode)"

    local dir_uid
    dir_uid=$(stat -c '%u' "$TEST_MNT/perm_dir")
    assert_eq "$dir_uid" "$(id -u)" "Created directory owned by current user"

    do_unmount
}

test_chmod_file() {
    setup
    do_mount
    do_create "chmod_test" "main"

    echo "test" > "$TEST_MNT/chmod_file.txt"
    chmod 0755 "$TEST_MNT/chmod_file.txt"
    local mode
    mode=$(stat -c '%a' "$TEST_MNT/chmod_file.txt")
    assert_eq "$mode" "755" "chmod 0755 applied correctly"

    chmod 0600 "$TEST_MNT/chmod_file.txt"
    mode=$(stat -c '%a' "$TEST_MNT/chmod_file.txt")
    assert_eq "$mode" "600" "chmod 0600 applied correctly"

    # Base file should be unaffected
    local base_mode
    base_mode=$(stat -c '%a' "$TEST_BASE/file1.txt")
    assert "[[ '$base_mode' != '755' ]]" "Base file permissions unchanged"

    do_unmount
}

test_chmod_directory() {
    setup
    do_mount
    do_create "chmod_dir_test" "main"

    mkdir "$TEST_MNT/chmod_dir"
    chmod 0700 "$TEST_MNT/chmod_dir"
    local mode
    mode=$(stat -c '%a' "$TEST_MNT/chmod_dir")
    assert_eq "$mode" "700" "chmod 0700 on directory applied correctly"

    do_unmount
}

test_chmod_existing_file_cow() {
    setup
    do_mount
    do_create "chmod_cow_test" "main"

    # chmod on a base file should COW and apply
    chmod 0755 "$TEST_MNT/file1.txt"
    local mode
    mode=$(stat -c '%a' "$TEST_MNT/file1.txt")
    assert_eq "$mode" "755" "chmod on base file applied via COW"

    # Base file should be unchanged
    local base_mode
    base_mode=$(stat -c '%a' "$TEST_BASE/file1.txt")
    assert "[[ '$base_mode' != '755' ]]" "Base file permissions unchanged after COW chmod"

    do_unmount
}

test_synthetic_entry_ownership() {
    setup
    do_mount

    # Root directory should be owned by current user
    local root_uid
    root_uid=$(stat -c '%u' "$TEST_MNT")
    assert_eq "$root_uid" "$(id -u)" "Mount root owned by current user"

    # .branchfs_ctl should be owned by current user
    local ctl_uid
    ctl_uid=$(stat -c '%u' "$TEST_MNT/.branchfs_ctl")
    assert_eq "$ctl_uid" "$(id -u)" "Control file owned by current user"

    # @branch virtual directory
    do_create "owner_test" "main"
    local branch_uid
    branch_uid=$(stat -c '%u' "$TEST_MNT/@owner_test")
    assert_eq "$branch_uid" "$(id -u)" "@branch dir owned by current user"

    do_unmount
}

# Run tests
test_recreate_file_after_delete() {
    setup
    do_mount
    echo "first content" > "$TEST_MNT/recreate.txt"
    assert_file_contains "$TEST_MNT/recreate.txt" "first content" "File created with first content"
    rm "$TEST_MNT/recreate.txt"
    assert_file_not_exists "$TEST_MNT/recreate.txt" "File deleted"
    echo "second content" > "$TEST_MNT/recreate.txt"

    # readdir forces a fresh resolution through resolve_path (unlike stat, which
    # is masked by the kernel's positive dentry cache from create)
    assert "ls '$TEST_MNT' | grep -qx 'recreate.txt'" "File recreated with same name visible in listing"
    assert_file_contains "$TEST_MNT/recreate.txt" "second content" "Recreated file has second content"

    local delta_dir="$TEST_STORAGE/branches/main/files"
    local count
    count=$(ls "$delta_dir/recreate.txt" 2>/dev/null | wc -l)
    assert_eq "$count" "1" "Exactly one delta entry for recreated file (no orphan)"
    do_unmount
}

test_recreate_dir_after_delete() {
    setup
    do_mount
    mkdir "$TEST_MNT/recreate_dir"
    assert "[[ -d '$TEST_MNT/recreate_dir' ]]" "Directory created"
    rmdir "$TEST_MNT/recreate_dir"
    assert "[[ ! -d '$TEST_MNT/recreate_dir' ]]" "Directory deleted"
    mkdir "$TEST_MNT/recreate_dir"
    assert "[[ -d '$TEST_MNT/recreate_dir' ]]" "Directory recreated with same name"
    do_unmount
}

test_recreate_symlink_after_delete() {
    setup
    do_mount
    echo "target" > "$TEST_MNT/recreate_target.txt"
    ln -s recreate_target.txt "$TEST_MNT/recreate_link"
    assert "[[ -L '$TEST_MNT/recreate_link' ]]" "Symlink created"
    rm "$TEST_MNT/recreate_link"
    assert "[[ ! -L '$TEST_MNT/recreate_link' ]]" "Symlink deleted"
    ln -s recreate_target.txt "$TEST_MNT/recreate_link"
    assert "[[ -L '$TEST_MNT/recreate_link' ]]" "Symlink recreated with same name"
    do_unmount
}

test_recreate_file_after_delete_in_branch() {
    setup
    do_mount
    do_create "recreate_b" "main"
    echo "branch first" > "$TEST_MNT/@recreate_b/recreate.txt"
    assert_file_contains "$TEST_MNT/@recreate_b/recreate.txt" "branch first" "Branch file created"
    rm "$TEST_MNT/@recreate_b/recreate.txt"
    assert_file_not_exists "$TEST_MNT/@recreate_b/recreate.txt" "Branch file deleted"
    echo "branch second" > "$TEST_MNT/@recreate_b/recreate.txt"
    assert_file_exists "$TEST_MNT/@recreate_b/recreate.txt" "Branch file recreated with same name"
    assert_file_contains "$TEST_MNT/@recreate_b/recreate.txt" "branch second" "Branch recreated file has second content"
    do_unmount
}

run_test "Read Base Files" test_read_base_files
run_test "Write New File in Branch" test_write_new_file_in_branch
run_test "Modify Existing File (COW)" test_modify_existing_file_cow
run_test "Delete File in Branch" test_delete_file_in_branch
run_test "Create Directory in Branch" test_create_directory_in_branch
run_test "Delete Directory in Branch" test_delete_directory_in_branch
run_test "Append to File" test_append_to_file
run_test "Create File Permissions" test_create_file_permissions
run_test "Mkdir Permissions" test_mkdir_permissions
run_test "Chmod File" test_chmod_file
run_test "Chmod Directory" test_chmod_directory
run_test "Chmod Existing File (COW)" test_chmod_existing_file_cow
run_test "Synthetic Entry Ownership" test_synthetic_entry_ownership
run_test "Recreate File After Delete" test_recreate_file_after_delete
run_test "Recreate Dir After Delete" test_recreate_dir_after_delete
run_test "Recreate Symlink After Delete" test_recreate_symlink_after_delete
run_test "Recreate File After Delete in Branch" test_recreate_file_after_delete_in_branch

print_summary
