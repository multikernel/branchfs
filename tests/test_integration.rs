//! Integration tests for core filesystem operations including rename.
//!
//! These tests require FUSE support and are ignored by default.
//! Run with: cargo test --test test_integration -- --ignored

use std::fs;
use std::os::unix::fs as unix_fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;



/// Helper: CREATE a branch. Returns the new branch name.
unsafe fn ioctl_create(fd: i32) -> Result<String, i32> {
    #[cfg(target_os = "macos")]
    {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::io::FromRawFd;
        let name = format!("test-branch-{}", uuid::Uuid::new_v4());
        let mut file = std::fs::File::from_raw_fd(libc::dup(fd));
        let _ = file.seek(SeekFrom::Start(0));
        if let Err(_) = file.write_all(format!("create:{}", name).as_bytes()) {
            return Err(libc::EIO);
        }
        Ok(name)
    }
    #[cfg(not(target_os = "macos"))]
    {
        let mut buf = [0u8; 128];
        let ret = libc::ioctl(fd, branchfs::platform::FS_IOC_BRANCH_CREATE as libc::c_ulong, buf.as_mut_ptr());
        if ret < 0 {
            return Err(unsafe { *libc::__errno_location() });
        }
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        Ok(String::from_utf8_lossy(&buf[..end]).to_string())
    }
}

/// Helper: COMMIT the branch identified by the ctl fd's inode.
unsafe fn ioctl_commit(fd: i32) -> i32 {
    #[cfg(target_os = "macos")]
    {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::io::FromRawFd;
        let mut file = std::fs::File::from_raw_fd(libc::dup(fd));
        let _ = file.seek(SeekFrom::Start(0));
        if let Err(_) = file.write_all(b"commit") {
            return -1;
        }
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        libc::ioctl(fd, branchfs::platform::FS_IOC_BRANCH_COMMIT as libc::c_ulong)
    }
}

/// Helper: ABORT the branch identified by the ctl fd's inode.
unsafe fn ioctl_abort(fd: i32) -> i32 {
    #[cfg(target_os = "macos")]
    {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::io::FromRawFd;
        let mut file = std::fs::File::from_raw_fd(libc::dup(fd));
        let _ = file.seek(SeekFrom::Start(0));
        if let Err(_) = file.write_all(b"abort") {
            return -1;
        }
        0
    }
    #[cfg(not(target_os = "macos"))]
    {
        libc::ioctl(fd, branchfs::platform::FS_IOC_BRANCH_ABORT as libc::c_ulong)
    }
}

struct TestFixture {
    base: PathBuf,
    storage: PathBuf,
    mnt: PathBuf,
    branchfs_bin: PathBuf,
}

impl TestFixture {
    fn new(name: &str) -> Self {
        let id = std::process::id();
        let prefix = format!("/tmp/branchfs_integ_{}_{}", name, id);
        let base = PathBuf::from(format!("{}_base", prefix));
        let storage = PathBuf::from(format!("{}_storage", prefix));
        let mnt = PathBuf::from(format!("{}_mnt", prefix));

        // Clean up leftovers from a previous failed run
        #[cfg(target_os = "linux")]
        let unmount_cmd = "fusermount3";
        #[cfg(not(target_os = "linux"))]
        let unmount_cmd = "umount";

        let _ = Command::new(unmount_cmd)
            .args(["-u", mnt.to_str().unwrap()])
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&base);
        let _ = fs::remove_dir_all(&storage);
        let _ = fs::remove_dir_all(&mnt);

        fs::create_dir_all(&base).expect("create base dir");
        fs::create_dir_all(&storage).expect("create storage dir");
        fs::create_dir_all(&mnt).expect("create mount dir");

        // Seed base directory
        fs::write(base.join("file1.txt"), "base content\n").unwrap();
        fs::write(base.join("file2.txt"), "another file\n").unwrap();
        fs::create_dir_all(base.join("subdir")).unwrap();
        fs::write(base.join("subdir").join("nested.txt"), "nested content\n").unwrap();

        let branchfs_bin = PathBuf::from(env!("CARGO_BIN_EXE_branchfs"));

        Self {
            base,
            storage,
            mnt,
            branchfs_bin,
        }
    }

    fn mount(&self) {
        let status = Command::new(&self.branchfs_bin)
            .args([
                "mount",
                "--base",
                self.base.to_str().unwrap(),
                "--storage",
                self.storage.to_str().unwrap(),
                self.mnt.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("failed to run branchfs mount");

        assert!(status.success(), "mount command failed");
        thread::sleep(Duration::from_millis(500));
        assert!(
            self.mnt.join(".branchfs_ctl").exists(),
            ".branchfs_ctl should exist after mount"
        );
    }

    fn unmount(&self) {
        let _ = Command::new(&self.branchfs_bin)
            .args([
                "unmount",
                self.mnt.to_str().unwrap(),
                "--storage",
                self.storage.to_str().unwrap(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        thread::sleep(Duration::from_millis(300));
    }

    fn open_ctl(&self) -> fs::File {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.mnt.join(".branchfs_ctl"))
            .expect("open .branchfs_ctl")
    }

    fn open_branch_ctl(&self, branch: &str) -> fs::File {
        let path = self.mnt.join(format!("@{}", branch)).join(".branchfs_ctl");
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("open branch ctl for '{}' at {:?}: {}", branch, path, e))
    }

    fn branch_dir(&self, branch: &str) -> PathBuf {
        self.mnt.join(format!("@{}", branch))
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        self.unmount();
        let _ = fs::remove_dir_all(&self.base);
        let _ = fs::remove_dir_all(&self.storage);
        let _ = fs::remove_dir_all(&self.mnt);
    }
}

// ── Rename tests ────────────────────────────────────────────────────

#[test]
#[ignore]
fn test_rename_file_same_dir() {
    let fix = TestFixture::new("rename_same");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Rename a base file within the same directory
    fs::rename(bdir.join("file1.txt"), bdir.join("file1_renamed.txt")).expect("rename should work");

    assert!(!bdir.join("file1.txt").exists(), "old name should be gone");
    assert!(
        bdir.join("file1_renamed.txt").exists(),
        "new name should exist"
    );
    assert_eq!(
        fs::read_to_string(bdir.join("file1_renamed.txt")).unwrap(),
        "base content\n"
    );

    // Base should be untouched
    assert!(fix.base.join("file1.txt").exists());
}

#[test]
#[ignore]
fn test_rename_file_cross_dir() {
    let fix = TestFixture::new("rename_xdir");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Move file into a subdirectory
    fs::rename(
        bdir.join("file1.txt"),
        bdir.join("subdir").join("moved.txt"),
    )
    .expect("cross-dir rename");

    assert!(!bdir.join("file1.txt").exists());
    assert!(bdir.join("subdir").join("moved.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("subdir").join("moved.txt")).unwrap(),
        "base content\n"
    );
}

#[test]
#[ignore]
fn test_rename_overwrite_existing() {
    let fix = TestFixture::new("rename_over");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create a new file, then rename it to overwrite file2.txt
    fs::write(bdir.join("new.txt"), "new content\n").unwrap();
    fs::rename(bdir.join("new.txt"), bdir.join("file2.txt")).expect("rename overwrite");

    assert!(!bdir.join("new.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("file2.txt")).unwrap(),
        "new content\n"
    );
}

#[test]
#[ignore]
fn test_rename_in_branch_then_commit() {
    let fix = TestFixture::new("rename_commit");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    fs::rename(bdir.join("file1.txt"), bdir.join("renamed.txt")).expect("rename");

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Base should reflect the rename
    assert!(
        fix.base.join("renamed.txt").exists(),
        "renamed file should be in base"
    );
    assert_eq!(
        fs::read_to_string(fix.base.join("renamed.txt")).unwrap(),
        "base content\n"
    );
    // The original should be gone from base (tombstone applied)
    assert!(
        !fix.base.join("file1.txt").exists(),
        "original should be deleted from base after commit"
    );
}

#[test]
#[ignore]
fn test_rename_in_branch_then_abort() {
    let fix = TestFixture::new("rename_abort");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    fs::rename(bdir.join("file1.txt"), bdir.join("renamed.txt")).expect("rename");

    // Abort
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_abort(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "abort should succeed");

    // Base should be unchanged
    assert!(fix.base.join("file1.txt").exists());
    assert!(!fix.base.join("renamed.txt").exists());
}

#[test]
#[ignore]
fn test_rename_nonexistent_fails() {
    let fix = TestFixture::new("rename_noent");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    let result = fs::rename(bdir.join("no_such_file.txt"), bdir.join("dest.txt"));
    assert!(result.is_err(), "rename of nonexistent file should fail");
}

// ── General filesystem integration tests ────────────────────────────

#[test]
#[ignore]
fn test_create_read_write_delete() {
    let fix = TestFixture::new("crud");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create
    fs::write(bdir.join("crud.txt"), "hello\n").unwrap();
    assert!(bdir.join("crud.txt").exists());

    // Read
    assert_eq!(
        fs::read_to_string(bdir.join("crud.txt")).unwrap(),
        "hello\n"
    );

    // Update (overwrite)
    fs::write(bdir.join("crud.txt"), "updated\n").unwrap();
    assert_eq!(
        fs::read_to_string(bdir.join("crud.txt")).unwrap(),
        "updated\n"
    );

    // Delete
    fs::remove_file(bdir.join("crud.txt")).unwrap();
    assert!(!bdir.join("crud.txt").exists());
}

#[test]
#[ignore]
fn test_branch_isolation() {
    let fix = TestFixture::new("isolation");
    fix.mount();
    let ctl = fix.open_ctl();

    // Create two sibling branches from main
    let branch_a = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE A");
    let branch_b = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE B");

    let dir_a = fix.branch_dir(&branch_a);
    let dir_b = fix.branch_dir(&branch_b);

    // Write different files in each branch
    fs::write(dir_a.join("only_a.txt"), "A\n").unwrap();
    fs::write(dir_b.join("only_b.txt"), "B\n").unwrap();

    // Each branch should only see its own file, not the sibling's
    assert!(dir_a.join("only_a.txt").exists());
    assert!(!dir_a.join("only_b.txt").exists());

    assert!(dir_b.join("only_b.txt").exists());
    assert!(!dir_b.join("only_a.txt").exists());
}

#[test]
#[ignore]
fn test_nested_branch_inheritance() {
    let fix = TestFixture::new("inherit");
    fix.mount();
    let ctl = fix.open_ctl();

    // Create parent branch, write a file
    let parent = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE parent");
    let pdir = fix.branch_dir(&parent);
    fs::write(pdir.join("parent_file.txt"), "from parent\n").unwrap();

    // Create child branch from parent
    let pctl = fix.open_branch_ctl(&parent);
    let child = unsafe { ioctl_create(pctl.as_raw_fd()) }.expect("CREATE child");
    let cdir = fix.branch_dir(&child);

    // Child should see parent's file and base files
    assert!(
        cdir.join("parent_file.txt").exists(),
        "child should inherit parent's file"
    );
    assert_eq!(
        fs::read_to_string(cdir.join("parent_file.txt")).unwrap(),
        "from parent\n"
    );
    assert!(
        cdir.join("file1.txt").exists(),
        "child should see base files"
    );
}

#[test]
#[ignore]
fn test_mkdir_and_nested_files() {
    let fix = TestFixture::new("mkdir_nested");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create nested directory structure
    fs::create_dir_all(bdir.join("a").join("b").join("c")).unwrap();
    fs::write(
        bdir.join("a").join("b").join("c").join("deep.txt"),
        "deep\n",
    )
    .unwrap();
    fs::write(bdir.join("a").join("top.txt"), "top\n").unwrap();

    assert!(bdir.join("a").join("b").join("c").join("deep.txt").exists());
    assert_eq!(
        fs::read_to_string(bdir.join("a").join("b").join("c").join("deep.txt")).unwrap(),
        "deep\n"
    );
    assert_eq!(
        fs::read_to_string(bdir.join("a").join("top.txt")).unwrap(),
        "top\n"
    );

    // None of this should be in base
    assert!(!fix.base.join("a").exists());
}

// ── Symlink tests ───────────────────────────────────────────────────

#[test]
#[ignore]
fn test_symlink_base_visible() {
    let fix = TestFixture::new("sym_base");

    // Add symlinks to base before mounting
    unix_fs::symlink("file1.txt", fix.base.join("link1")).unwrap();
    unix_fs::symlink("nonexistent", fix.base.join("dangling")).unwrap();

    fix.mount();

    // Symlinks should be visible as symlinks (not followed)
    assert!(
        fix.mnt
            .join("link1")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "base symlink should be a symlink through the mount"
    );
    assert_eq!(
        fs::read_link(fix.mnt.join("link1"))
            .unwrap()
            .to_str()
            .unwrap(),
        "file1.txt"
    );

    // Dangling symlink should be visible too
    assert!(
        fix.mnt
            .join("dangling")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "dangling symlink should be visible"
    );
    assert_eq!(
        fs::read_link(fix.mnt.join("dangling"))
            .unwrap()
            .to_str()
            .unwrap(),
        "nonexistent"
    );

    // Following the valid symlink should work
    assert_eq!(
        fs::read_to_string(fix.mnt.join("link1")).unwrap(),
        "base content\n"
    );
}

#[test]
#[ignore]
fn test_symlink_create_in_branch() {
    let fix = TestFixture::new("sym_create");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create a symlink in the branch
    unix_fs::symlink("file1.txt", bdir.join("new_link")).unwrap();

    assert!(
        bdir.join("new_link")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "created symlink should be a symlink"
    );
    assert_eq!(
        fs::read_link(bdir.join("new_link"))
            .unwrap()
            .to_str()
            .unwrap(),
        "file1.txt"
    );
    assert_eq!(
        fs::read_to_string(bdir.join("new_link")).unwrap(),
        "base content\n"
    );

    // Should NOT be in base
    assert!(!fix.base.join("new_link").exists());
}

#[test]
#[ignore]
fn test_symlink_commit() {
    let fix = TestFixture::new("sym_commit");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Create symlinks in the branch
    unix_fs::symlink("file1.txt", bdir.join("link_a")).unwrap();
    unix_fs::symlink("no_target", bdir.join("link_dangling")).unwrap();

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Symlinks should now exist in base
    assert!(
        fix.base
            .join("link_a")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "committed symlink should be a symlink in base"
    );
    assert_eq!(
        fs::read_link(fix.base.join("link_a"))
            .unwrap()
            .to_str()
            .unwrap(),
        "file1.txt"
    );

    // Dangling symlink should also be committed
    assert!(
        fix.base
            .join("link_dangling")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "committed dangling symlink should be a symlink in base"
    );
    assert_eq!(
        fs::read_link(fix.base.join("link_dangling"))
            .unwrap()
            .to_str()
            .unwrap(),
        "no_target"
    );
}

#[test]
#[ignore]
fn test_symlink_abort() {
    let fix = TestFixture::new("sym_abort");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    unix_fs::symlink("file1.txt", bdir.join("aborted_link")).unwrap();
    assert!(bdir.join("aborted_link").symlink_metadata().is_ok());

    // Abort
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_abort(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "abort should succeed");

    // Should not be in base
    assert!(
        fix.base.join("aborted_link").symlink_metadata().is_err(),
        "aborted symlink should not be in base"
    );
}

#[test]
#[ignore]
fn test_symlink_delete_in_branch() {
    let fix = TestFixture::new("sym_delete");

    // Add a symlink to base
    unix_fs::symlink("file1.txt", fix.base.join("link_del")).unwrap();

    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Symlink should be visible
    assert!(bdir
        .join("link_del")
        .symlink_metadata()
        .unwrap()
        .file_type()
        .is_symlink());

    // Delete it in the branch
    fs::remove_file(bdir.join("link_del")).unwrap();
    assert!(
        bdir.join("link_del").symlink_metadata().is_err(),
        "symlink should be gone in branch"
    );

    // Base should still have it
    assert!(
        fix.base
            .join("link_del")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink(),
        "base symlink should be unaffected"
    );
}

#[test]
#[ignore]
fn test_symlink_isolation_between_branches() {
    let fix = TestFixture::new("sym_isolation");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch_a = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE A");
    let branch_b = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE B");

    let dir_a = fix.branch_dir(&branch_a);
    let dir_b = fix.branch_dir(&branch_b);

    // Create different symlinks in each branch
    unix_fs::symlink("file1.txt", dir_a.join("link_a")).unwrap();
    unix_fs::symlink("file2.txt", dir_b.join("link_b")).unwrap();

    // Each branch should only see its own symlink
    assert!(dir_a.join("link_a").symlink_metadata().is_ok());
    assert!(dir_a.join("link_b").symlink_metadata().is_err());

    assert!(dir_b.join("link_b").symlink_metadata().is_ok());
    assert!(dir_b.join("link_a").symlink_metadata().is_err());
}

// ── Readdir inheritance tests ───────────────────────────────────────
//
// Regression tests for: readdir on branches not showing inherited files.
// collect_readdir_entries() only read from base + one resolved directory,
// missing files in parent branch deltas.

/// Files written through the mount root (stored in main's delta) should
/// appear in a child branch's directory listing, not just via direct access.
#[test]
#[ignore]
fn test_readdir_branch_inherits_main_delta_files() {
    let fix = TestFixture::new("readdir_inherit");
    fix.mount();

    // Write a file through the mount root — goes to main's delta
    fs::write(fix.mnt.join("dynamic.txt"), "written via mount\n").unwrap();

    let ctl = fix.open_ctl();
    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Direct access should work (resolve_path walks the chain)
    assert!(
        bdir.join("dynamic.txt").exists(),
        "branch should see main's file via direct access"
    );

    // readdir should ALSO list it
    let entries: Vec<String> = fs::read_dir(&bdir)
        .expect("read_dir on branch")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&"dynamic.txt".to_string()),
        "readdir should list main's delta file; got: {:?}",
        entries
    );
    // Should also list base files
    assert!(
        entries.contains(&"file1.txt".to_string()),
        "readdir should list base files; got: {:?}",
        entries
    );
    assert!(
        entries.contains(&"file2.txt".to_string()),
        "readdir should list base files; got: {:?}",
        entries
    );
}

/// readdir on a nested (grandchild) branch should list files from the
/// parent branch's delta, the grandparent's delta, and base.
#[test]
#[ignore]
fn test_readdir_nested_branch_inherits_parent_delta() {
    let fix = TestFixture::new("readdir_nested");
    fix.mount();
    let ctl = fix.open_ctl();

    // Create parent branch, write a file
    let parent = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE parent");
    let pdir = fix.branch_dir(&parent);
    fs::write(pdir.join("parent_only.txt"), "from parent\n").unwrap();

    // Create child branch from parent
    let pctl = fix.open_branch_ctl(&parent);
    let child = unsafe { ioctl_create(pctl.as_raw_fd()) }.expect("CREATE child");
    let cdir = fix.branch_dir(&child);

    // readdir on child should list parent's file + base files
    let entries: Vec<String> = fs::read_dir(&cdir)
        .expect("read_dir on child branch")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&"parent_only.txt".to_string()),
        "child readdir should list parent's file; got: {:?}",
        entries
    );
    assert!(
        entries.contains(&"file1.txt".to_string()),
        "child readdir should list base files; got: {:?}",
        entries
    );
}

/// readdir should respect tombstones: a file deleted in a branch should
/// not appear in that branch's directory listing.
#[test]
#[ignore]
fn test_readdir_respects_tombstones() {
    let fix = TestFixture::new("readdir_tomb");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);

    // Delete a base file in the branch
    fs::remove_file(bdir.join("file1.txt")).unwrap();

    let entries: Vec<String> = fs::read_dir(&bdir)
        .expect("read_dir")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        !entries.contains(&"file1.txt".to_string()),
        "deleted file should not appear in readdir; got: {:?}",
        entries
    );
    // Other base files should still be there
    assert!(
        entries.contains(&"file2.txt".to_string()),
        "undeleted file should still appear; got: {:?}",
        entries
    );
}

// ── Commit propagation tests ────────────────────────────────────────
//
// Regression tests for: committed changes not visible at mount root.
// When committing to main, delta files were copied to base but main's
// pre-existing delta was left untouched, overshadowing the updated base.

/// After committing a branch that modified a file originally written
/// through the mount root, reading from the mount root should return
/// the committed version.
#[test]
#[ignore]
fn test_commit_changes_visible_at_mount_root() {
    let fix = TestFixture::new("commit_visible");
    fix.mount();

    // Write a file through the mount root — goes to main's delta
    fs::write(fix.mnt.join("evolve.txt"), "version 1\n").unwrap();
    assert_eq!(
        fs::read_to_string(fix.mnt.join("evolve.txt")).unwrap(),
        "version 1\n"
    );

    // Create a branch and modify the file
    let ctl = fix.open_ctl();
    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);
    fs::write(bdir.join("evolve.txt"), "version 2\n").unwrap();
    assert_eq!(
        fs::read_to_string(bdir.join("evolve.txt")).unwrap(),
        "version 2\n"
    );

    // Mount root still sees version 1 (isolation)
    assert_eq!(
        fs::read_to_string(fix.mnt.join("evolve.txt")).unwrap(),
        "version 1\n"
    );

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Mount root should now see version 2
    assert_eq!(
        fs::read_to_string(fix.mnt.join("evolve.txt")).unwrap(),
        "version 2\n",
        "mount root should show committed content, not stale main delta"
    );
}

/// After committing a branch that deletes a file originally written
/// through the mount root, the file should be gone at the mount root.
#[test]
#[ignore]
fn test_commit_deletion_visible_at_mount_root() {
    let fix = TestFixture::new("commit_delete");
    fix.mount();

    // Write a file through the mount root
    fs::write(fix.mnt.join("doomed.txt"), "will be deleted\n").unwrap();
    assert!(fix.mnt.join("doomed.txt").exists());

    // Create a branch and delete the file
    let ctl = fix.open_ctl();
    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);
    fs::remove_file(bdir.join("doomed.txt")).unwrap();
    assert!(!bdir.join("doomed.txt").exists());

    // Mount root still sees it (isolation)
    assert!(fix.mnt.join("doomed.txt").exists());

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Mount root should no longer see the file
    assert!(
        !fix.mnt.join("doomed.txt").exists(),
        "file deleted in branch should be gone at mount root after commit"
    );
}

/// Modifying a base file (not main's delta) in a branch and committing
/// should update the file visible at the mount root.
#[test]
#[ignore]
fn test_commit_base_file_modification_visible_at_mount_root() {
    let fix = TestFixture::new("commit_base_mod");
    fix.mount();

    // file1.txt is seeded in base (not via FUSE write, so no main delta)
    assert_eq!(
        fs::read_to_string(fix.mnt.join("file1.txt")).unwrap(),
        "base content\n"
    );

    // Create a branch and modify the base file
    let ctl = fix.open_ctl();
    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE");
    let bdir = fix.branch_dir(&branch);
    fs::write(bdir.join("file1.txt"), "updated in branch\n").unwrap();

    // Commit
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit should succeed");

    // Mount root should see the update
    assert_eq!(
        fs::read_to_string(fix.mnt.join("file1.txt")).unwrap(),
        "updated in branch\n",
        "mount root should show committed content"
    );

    // Base should also be updated
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "updated in branch\n",
    );
}
