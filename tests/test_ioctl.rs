//! Integration tests for FS_IOC_BRANCH_* ioctls.
//!
//! These tests require FUSE support and are ignored by default.
//! Run with: cargo test --test test_ioctl -- --ignored

use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

use branchfs::{FS_IOC_BRANCH_ABORT, FS_IOC_BRANCH_COMMIT, FS_IOC_BRANCH_CREATE};

/// Helper: CREATE a branch (parent derived from the ctl fd's inode).
/// Returns the new branch name from the 128-byte output buffer.
unsafe fn ioctl_create(fd: i32) -> Result<String, i32> {
    let mut buf = [0u8; 128];
    let ret = libc::ioctl(fd, FS_IOC_BRANCH_CREATE as libc::c_ulong, buf.as_mut_ptr());
    if ret < 0 {
        return Err(*libc::__errno_location());
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    Ok(String::from_utf8_lossy(&buf[..end]).to_string())
}

/// Helper: COMMIT the branch identified by the ctl fd's inode.
unsafe fn ioctl_commit(fd: i32) -> i32 {
    libc::ioctl(fd, FS_IOC_BRANCH_COMMIT as libc::c_ulong)
}

/// Helper: ABORT the branch identified by the ctl fd's inode.
unsafe fn ioctl_abort(fd: i32) -> i32 {
    libc::ioctl(fd, FS_IOC_BRANCH_ABORT as libc::c_ulong)
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
        let prefix = format!("/tmp/branchfs_ioctl_{}_{}", name, id);
        let base = PathBuf::from(format!("{}_base", prefix));
        let storage = PathBuf::from(format!("{}_storage", prefix));
        let mnt = PathBuf::from(format!("{}_mnt", prefix));

        // Clean up leftovers from a previous failed run
        let _ = Command::new("fusermount3")
            .args(["-u", mnt.to_str().unwrap()])
            .stderr(Stdio::null())
            .status();
        let _ = fs::remove_dir_all(&base);
        let _ = fs::remove_dir_all(&storage);
        let _ = fs::remove_dir_all(&mnt);

        fs::create_dir_all(&base).expect("create base dir");
        fs::create_dir_all(&storage).expect("create storage dir");
        fs::create_dir_all(&mnt).expect("create mount dir");

        // Seed base directory with initial files
        fs::write(base.join("file1.txt"), "base content\n").unwrap();
        fs::write(base.join("file2.txt"), "another file\n").unwrap();

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

    /// Open the root control file; caller must keep the returned `File` alive.
    fn open_ctl(&self) -> fs::File {
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(self.mnt.join(".branchfs_ctl"))
            .expect("open .branchfs_ctl")
    }

    /// Open a per-branch control file at `/@<branch>/.branchfs_ctl`.
    fn open_branch_ctl(&self, branch: &str) -> fs::File {
        let path = self.mnt.join(format!("@{}", branch)).join(".branchfs_ctl");
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap_or_else(|e| panic!("open branch ctl for '{}' at {:?}: {}", branch, path, e))
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

// ── CREATE + COMMIT ─────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_create_and_commit_new_file() {
    let fix = TestFixture::new("create_commit");
    fix.mount();
    let ctl = fix.open_ctl();

    // CREATE a branch from main (root ctl → current mount branch = main)
    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE should succeed");
    assert!(!branch.is_empty(), "branch name should not be empty");

    // Write a new file on the branch via @branch virtual path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("new_file.txt"), "branch content\n").unwrap();
    assert!(branch_dir.join("new_file.txt").exists());

    // Base should NOT have the file yet
    assert!(
        !fix.base.join("new_file.txt").exists(),
        "new file must not appear in base before commit"
    );

    // COMMIT the branch via its per-branch ctl
    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "COMMIT should succeed");

    // File should now be in base
    assert!(
        fix.base.join("new_file.txt").exists(),
        "new file should be in base after commit"
    );
    assert_eq!(
        fs::read_to_string(fix.base.join("new_file.txt")).unwrap(),
        "branch content\n"
    );
}

// ── CREATE + modify existing file + COMMIT ──────────────────────────

#[test]
#[ignore]
fn test_ioctl_modify_existing_and_commit() {
    let fix = TestFixture::new("modify_commit");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE should succeed");

    // Overwrite an existing base file via @branch path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("file1.txt"), "modified\n").unwrap();

    // Base still has the original
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "base content\n"
    );

    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0);

    // Base should reflect the modification
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "modified\n"
    );
}

// ── Replace a base directory with a file, then COMMIT ───────────────
//
// Regression test: committing a branch that turns a base directory into a file
// (at the same path) must succeed and leave a file in base. A previous
// implementation published copies before applying deletions, so
// `rename(tmp_file, base/dir)` hit EISDIR and the commit failed with EIO.
//
// The dir->file delta is produced via `rename` (which clears the destination's
// tombstone), so at commit time there is a delta file over a base directory
// with no tombstone — exercising the directory-conflict handling in stage_copy.

#[test]
#[ignore]
fn test_ioctl_replace_dir_with_file_and_commit() {
    let fix = TestFixture::new("replace_dir_with_file");
    // Seed base with an (empty) directory and a file (fixture seeds only files).
    fs::create_dir_all(fix.base.join("subdir")).unwrap();
    fs::write(fix.base.join("src.txt"), "payload\n").unwrap();
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE should succeed");
    let branch_dir = fix.mnt.join(format!("@{}", branch));

    // Remove the directory, then rename the file onto its (now-free) path.
    fs::remove_dir(branch_dir.join("subdir")).unwrap();
    fs::rename(branch_dir.join("src.txt"), branch_dir.join("subdir")).unwrap();

    // Base still has the original directory and file before commit.
    assert!(fix.base.join("subdir").is_dir());
    assert!(fix.base.join("src.txt").exists());

    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_commit(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "COMMIT should succeed (dir replaced by file)");

    // Base now has a file at that path with the branch's content; the source
    // file (renamed away) is gone.
    let meta = fs::symlink_metadata(fix.base.join("subdir")).unwrap();
    assert!(
        meta.file_type().is_file(),
        "base/subdir should be a file after commit, got {:?}",
        meta.file_type()
    );
    assert_eq!(
        fs::read_to_string(fix.base.join("subdir")).unwrap(),
        "payload\n"
    );
    assert!(
        !fix.base.join("src.txt").exists(),
        "renamed-away source should be gone from base"
    );
}

// ── CREATE + ABORT ──────────────────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_create_and_abort() {
    let fix = TestFixture::new("create_abort");
    fix.mount();
    let ctl = fix.open_ctl();

    let branch = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE should succeed");

    // Write a file and modify another via @branch path
    let branch_dir = fix.mnt.join(format!("@{}", branch));
    fs::write(branch_dir.join("abort_file.txt"), "will be discarded\n").unwrap();
    fs::write(branch_dir.join("file1.txt"), "modified\n").unwrap();

    let bctl = fix.open_branch_ctl(&branch);
    let ret = unsafe { ioctl_abort(bctl.as_raw_fd()) };
    assert_eq!(ret, 0, "ABORT should succeed");

    // Branch dir should be gone after abort
    assert!(
        !branch_dir.join("abort_file.txt").exists(),
        "new file should vanish after abort"
    );
    // Base untouched
    assert!(!fix.base.join("abort_file.txt").exists());
    assert_eq!(
        fs::read_to_string(fix.base.join("file1.txt")).unwrap(),
        "base content\n"
    );
}

// ── Nested CREATE + COMMIT chain ────────────────────────────────────

#[test]
#[ignore]
fn test_ioctl_nested_create_and_commit() {
    let fix = TestFixture::new("nested");
    fix.mount();
    let ctl = fix.open_ctl();

    // First branch (child of main) — CREATE via root ctl
    let branch1 = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("first CREATE should succeed");
    let b1_dir = fix.mnt.join(format!("@{}", branch1));
    fs::write(b1_dir.join("level1.txt"), "level1\n").unwrap();

    // Second branch (child of first) — CREATE via branch1's ctl
    let b1_ctl = fix.open_branch_ctl(&branch1);
    let branch2 =
        unsafe { ioctl_create(b1_ctl.as_raw_fd()) }.expect("second CREATE should succeed");
    let b2_dir = fix.mnt.join(format!("@{}", branch2));
    fs::write(b2_dir.join("level2.txt"), "level2\n").unwrap();

    // Files visible through their respective branch paths
    assert!(b1_dir.join("level1.txt").exists());
    assert!(b2_dir.join("level2.txt").exists());
    // level2 branch should also see level1 (inherited from parent)
    assert!(b2_dir.join("level1.txt").exists());

    // COMMIT level-2 → merges into level-1 (via branch2's ctl)
    let b2_ctl = fix.open_branch_ctl(&branch2);
    let ret = unsafe { ioctl_commit(b2_ctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit level-2 should succeed");

    // Neither file should be in base yet
    assert!(!fix.base.join("level1.txt").exists());
    assert!(!fix.base.join("level2.txt").exists());

    // COMMIT level-1 → merges into main / base (via branch1's ctl)
    let ret = unsafe { ioctl_commit(b1_ctl.as_raw_fd()) };
    assert_eq!(ret, 0, "commit level-1 should succeed");

    // Both files in base now
    assert!(fix.base.join("level1.txt").exists());
    assert!(fix.base.join("level2.txt").exists());
}

// ── COMMIT / ABORT on main should fail ──────────────────────────────

#[test]
#[ignore]
fn test_ioctl_commit_on_main_fails() {
    let fix = TestFixture::new("commit_main");
    fix.mount();
    let ctl = fix.open_ctl();

    // Root ctl resolves to "main" — commit on main should fail
    let ret = unsafe { ioctl_commit(ctl.as_raw_fd()) };
    assert!(ret < 0, "COMMIT on main should fail (got {})", ret);
}

#[test]
#[ignore]
fn test_ioctl_abort_on_main_fails() {
    let fix = TestFixture::new("abort_main");
    fix.mount();
    let ctl = fix.open_ctl();

    // Root ctl resolves to "main" — abort on main should fail
    let ret = unsafe { ioctl_abort(ctl.as_raw_fd()) };
    assert!(ret < 0, "ABORT on main should fail (got {})", ret);
}

// ── Multiple CREATE + ABORT discards child branch ───────────────────

#[test]
#[ignore]
fn test_ioctl_abort_returns_to_parent_branch() {
    let fix = TestFixture::new("abort_parent");
    fix.mount();
    let ctl = fix.open_ctl();

    // CREATE first branch, write a file
    let branch1 = unsafe { ioctl_create(ctl.as_raw_fd()) }.expect("CREATE should succeed");
    let b1_dir = fix.mnt.join(format!("@{}", branch1));
    fs::write(b1_dir.join("parent_file.txt"), "parent\n").unwrap();

    // CREATE second (nested) branch via branch1's ctl, write another file
    let b1_ctl = fix.open_branch_ctl(&branch1);
    let branch2 = unsafe { ioctl_create(b1_ctl.as_raw_fd()) }.expect("CREATE should succeed");
    let b2_dir = fix.mnt.join(format!("@{}", branch2));
    fs::write(b2_dir.join("child_file.txt"), "child\n").unwrap();

    // ABORT second branch — child's data is discarded
    let b2_ctl = fix.open_branch_ctl(&branch2);
    let ret = unsafe { ioctl_abort(b2_ctl.as_raw_fd()) };
    assert_eq!(ret, 0);

    // Parent branch still has its file
    assert!(b1_dir.join("parent_file.txt").exists());

    // COMMIT first branch back to main
    let ret = unsafe { ioctl_commit(b1_ctl.as_raw_fd()) };
    assert_eq!(ret, 0);

    assert!(fix.base.join("parent_file.txt").exists());
    assert!(!fix.base.join("child_file.txt").exists());
}
