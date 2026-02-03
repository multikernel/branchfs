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
    do_create "write_test" "main" "-s"

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
    do_create "cow_test" "main" "-s"

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
    do_create "delete_test" "main" "-s"

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
    do_create "mkdir_test" "main" "-s"

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
    do_create "rmdir_test" "main" "-s"

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
    do_create "append_test" "main" "-s"

    # Append to existing file
    echo "appended line" >> "$TEST_MNT/file1.txt"

    local content
    content=$(cat "$TEST_MNT/file1.txt")
    assert "[[ '$content' == *'base content'* ]]" "Original content preserved"
    assert "[[ '$content' == *'appended line'* ]]" "Appended content present"

    do_unmount
}

# Run tests
run_test "Read Base Files" test_read_base_files
run_test "Write New File in Branch" test_write_new_file_in_branch
run_test "Modify Existing File (COW)" test_modify_existing_file_cow
run_test "Delete File in Branch" test_delete_file_in_branch
run_test "Create Directory in Branch" test_create_directory_in_branch
run_test "Delete Directory in Branch" test_delete_directory_in_branch
run_test "Append to File" test_append_to_file

print_summary
