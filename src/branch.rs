use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use fuser::Notifier;
use parking_lot::{Mutex, RwLock};

use crate::error::{BranchError, Result};
use crate::inode::ROOT_INO;
use crate::storage;

/// Tracks storage usage across all branches and enforces an optional quota.
/// Only counts delta files (the actual disk cost of branching).
pub struct StorageQuota {
    /// Current total bytes used in the storage directory (all branches' deltas).
    used_bytes: AtomicI64,
    /// Maximum allowed bytes. None = unlimited.
    max_bytes: Option<u64>,
}

impl StorageQuota {
    pub fn new(max_bytes: Option<u64>) -> Self {
        Self {
            used_bytes: AtomicI64::new(0),
            max_bytes,
        }
    }

    /// Walk the storage directory to compute initial usage.
    pub fn scan_usage(storage_path: &Path, max_bytes: Option<u64>) -> Self {
        let quota = Self::new(max_bytes);
        let branches_dir = storage_path.join("branches");
        if branches_dir.exists() {
            let bytes = Self::dir_size(&branches_dir);
            quota.used_bytes.store(bytes as i64, Ordering::Relaxed);
        }
        quota
    }

    fn dir_size(path: &Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                let p = entry.path();
                if p.is_dir() {
                    total += Self::dir_size(&p);
                } else if let Ok(meta) = p.symlink_metadata() {
                    total += meta.len();
                }
            }
        }
        total
    }

    /// Check if `additional` bytes can be allocated. Returns Err(ENOSPC) if not.
    pub fn check(&self, additional: u64) -> std::result::Result<(), i32> {
        if let Some(max) = self.max_bytes {
            let current = self.used_bytes.load(Ordering::Relaxed);
            if current as u64 + additional > max {
                return Err(libc::ENOSPC);
            }
        }
        Ok(())
    }

    /// Record that `bytes` were added to storage.
    pub fn add(&self, bytes: u64) {
        self.used_bytes.fetch_add(bytes as i64, Ordering::Relaxed);
    }

    /// Record that `bytes` were removed from storage.
    pub fn sub(&self, bytes: u64) {
        self.used_bytes.fetch_sub(bytes as i64, Ordering::Relaxed);
    }

    pub fn used(&self) -> u64 {
        self.used_bytes.load(Ordering::Relaxed).max(0) as u64
    }

    pub fn max(&self) -> Option<u64> {
        self.max_bytes
    }
}

/// Remove a file or directory at `path`, following symlinks for the type check.
/// Returns `Ok(())` even if the path doesn't exist; propagates real I/O errors.
fn remove_entry(path: &Path) -> std::io::Result<()> {
    match path.symlink_metadata() {
        Ok(m) if m.file_type().is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// An in-progress merge that prepares all of its destructive work as
/// rollback-able side files, then publishes it in one near-infallible phase.
///
/// Two kinds of work are staged:
/// - **copies** — each source file is copied to a temp sibling of its final
///   destination (`.branchfs-tmp.<name>`); the only error-prone step.
/// - **deletions** — each entry to be removed is renamed aside to a trash
///   sibling (`.branchfs-trash.<name>`) instead of being deleted outright.
///
/// `commit` then renames every copy temp into place and discards the trashed
/// entries. If dropped without `commit` (e.g. an error propagated during
/// staging), every copy temp is removed and every trashed entry is renamed
/// back, leaving the destination exactly as it was — so a failed merge can
/// neither be reported as success nor partially mutate the destination.
///
/// Staging a deletion before staging copies under the same path also clears any
/// type conflict (e.g. replacing a directory with a file): the conflicting
/// entry is moved out of the way before the copy's parent directories are
/// created and before the temp is renamed into place.
///
/// Note: a copy's `copy_entry` creates the destination's parent directories, so
/// a rolled-back merge may leave empty directories behind — this is benign (no
/// data is written or lost).
#[derive(Default)]
struct StagedMerge {
    /// (temp path, final destination) for each copied file.
    copies: Vec<(PathBuf, PathBuf)>,
    /// (trash path, original path) for each deletion staged aside.
    deletes: Vec<(PathBuf, PathBuf)>,
    /// Relative paths copied, returned to the caller on `commit` for bookkeeping.
    merged: Vec<String>,
    /// Total bytes copied, for the `[BENCH]` log line.
    bytes: u64,
    committed: bool,
}

impl StagedMerge {
    fn new() -> Self {
        Self::default()
    }

    /// Stage a deletion: rename `target` aside to a trash sibling so it can be
    /// restored on rollback. No-op if `target` does not exist.
    fn stage_delete(&mut self, target: &Path) -> Result<()> {
        if target.symlink_metadata().is_err() {
            return Ok(()); // nothing to delete
        }
        let trash = commit_side_path(target, "trash");
        // Clear any stale trash left by a previously crashed commit so the
        // rename below cannot fail with EEXIST/ENOTEMPTY.
        let _ = remove_entry(&trash);
        fs::rename(target, &trash)?;
        self.deletes.push((trash, target.to_path_buf()));
        Ok(())
    }

    /// Stage a copy: copy `src` to a temp sibling of its final destination under
    /// `dest_root`. The only error-prone step; no existing destination data is
    /// touched (the temp is published only in `commit`).
    fn stage_copy(&mut self, rel_path: &str, src: &Path, dest_root: &Path) -> Result<()> {
        let dest = dest_root.join(rel_path.trim_start_matches('/'));
        // A file or symlink cannot be renamed over an existing directory. If the
        // destination is currently a directory (a path that changed from dir to
        // file, e.g. via rename, which leaves no tombstone), stage its removal
        // first so the publish rename can place the file. Renaming over an
        // existing file or symlink is fine, so those are left to `commit`.
        if dest
            .symlink_metadata()
            .map(|m| m.file_type().is_dir())
            .unwrap_or(false)
        {
            self.stage_delete(&dest)?;
        }
        let tmp = commit_side_path(&dest, "tmp");
        storage::copy_entry(src, &tmp)?;
        if let Ok(meta) = src.symlink_metadata() {
            self.bytes += meta.len();
        }
        self.copies.push((tmp, dest));
        self.merged.push(rel_path.to_string());
        Ok(())
    }

    /// Publish: rename every staged copy into its final place and discard the
    /// trashed deletions. Returns the relative paths that were merged.
    fn commit(mut self) -> Result<Vec<String>> {
        for (tmp, dest) in &self.copies {
            fs::rename(tmp, dest)?;
        }
        for (trash, _) in &self.deletes {
            remove_entry(trash)?;
        }
        self.committed = true;
        Ok(std::mem::take(&mut self.merged))
    }
}

impl Drop for StagedMerge {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        // Roll back: drop copy temps, restore trashed deletions to their
        // original paths so the destination is left untouched.
        for (tmp, _) in &self.copies {
            let _ = remove_entry(tmp);
        }
        for (trash, original) in &self.deletes {
            let _ = fs::rename(trash, original);
        }
    }
}

/// A side-file path next to `target`: `<dir>/.branchfs-<tag>.<name>`.
/// The side file lives in the same directory as `target`, so renaming between
/// them is an atomic same-filesystem operation. Built from the raw `OsStr`
/// (not a lossy `String`) so distinct non-UTF8 names never collide.
fn commit_side_path(target: &Path, tag: &str) -> PathBuf {
    let mut name = std::ffi::OsString::from(format!(".branchfs-{}.", tag));
    name.push(target.file_name().unwrap_or_default());
    target.with_file_name(name)
}

pub struct Branch {
    pub name: String,
    pub parent: Option<String>,
    pub files_dir: PathBuf,
    pub inherited_dir: PathBuf,
    pub tombstones_file: PathBuf,
    tombstones: RwLock<HashSet<String>>,
    /// Number of stale (removed-from-memory-but-still-on-disk) tombstone entries.
    /// When this exceeds the live set size, we compact the file.
    tombstone_stale: AtomicU64,
    /// How many children have been committed (merged) into this branch.
    pub commit_count: AtomicU64,
    /// Parent's commit_count at the time this branch was forked.
    pub parent_version_at_fork: u64,
}

impl Branch {
    pub fn new(
        name: &str,
        parent: Option<&str>,
        storage_path: &Path,
        parent_version_at_fork: u64,
    ) -> Result<Self> {
        let branch_dir = storage_path.join("branches").join(name);
        let files_dir = branch_dir.join("files");
        let inherited_dir = branch_dir.join("inherited");
        let tombstones_file = branch_dir.join("tombstones");

        fs::create_dir_all(&files_dir)?;
        fs::create_dir_all(&inherited_dir)?;
        if !tombstones_file.exists() {
            File::create(&tombstones_file)?;
        }

        let tombstones = Self::load_tombstones(&tombstones_file)?;

        Ok(Self {
            name: name.to_string(),
            parent: parent.map(|s| s.to_string()),
            files_dir,
            inherited_dir,
            tombstones_file,
            tombstones: RwLock::new(tombstones),
            tombstone_stale: AtomicU64::new(0),
            commit_count: AtomicU64::new(0),
            parent_version_at_fork,
        })
    }

    fn load_tombstones(path: &Path) -> Result<HashSet<String>> {
        let mut set = HashSet::new();
        if path.exists() {
            let file = File::open(path)?;
            for line in BufReader::new(file).lines() {
                set.insert(line?);
            }
        }
        Ok(set)
    }

    pub fn is_deleted(&self, path: &str) -> bool {
        self.tombstones.read().contains(path)
    }

    pub fn add_tombstone(&self, path: &str) -> Result<()> {
        let mut tombstones = self.tombstones.write();
        if tombstones.insert(path.to_string()) {
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(&self.tombstones_file)?;
            writeln!(file, "{}", path)?;
        }
        Ok(())
    }

    pub fn remove_tombstone(&self, path: &str) {
        let mut tombstones = self.tombstones.write();
        if tombstones.remove(path) {
            let stale = self.tombstone_stale.fetch_add(1, Ordering::Relaxed) + 1;
            // Compact when stale entries exceed live set size (at least 16 to avoid
            // thrashing on tiny sets).
            if stale >= tombstones.len().max(16) as u64
                && self.rewrite_tombstones(&tombstones).is_ok()
            {
                self.tombstone_stale.store(0, Ordering::Relaxed);
            }
        }
    }

    /// Rewrite the tombstones file from the in-memory set (caller holds write lock).
    fn rewrite_tombstones(&self, tombstones: &HashSet<String>) -> Result<()> {
        let mut file = File::create(&self.tombstones_file)?;
        for t in tombstones {
            writeln!(file, "{}", t)?;
        }
        Ok(())
    }

    pub fn get_tombstones(&self) -> HashSet<String> {
        self.tombstones.read().clone()
    }

    /// Replace the in-memory tombstone set and rewrite the tombstones file.
    pub fn set_tombstones(&self, new_tombstones: HashSet<String>) -> Result<()> {
        let mut tombstones = self.tombstones.write();
        *tombstones = new_tombstones;
        self.rewrite_tombstones(&tombstones)?;
        self.tombstone_stale.store(0, Ordering::Relaxed);
        Ok(())
    }

    pub fn delta_path(&self, rel_path: &str) -> PathBuf {
        self.files_dir.join(rel_path.trim_start_matches('/'))
    }

    pub fn inherited_path(&self, rel_path: &str) -> PathBuf {
        self.inherited_dir.join(rel_path.trim_start_matches('/'))
    }

    pub fn has_delta(&self, rel_path: &str) -> bool {
        self.delta_path(rel_path).symlink_metadata().is_ok()
    }
}

fn validate_branch_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(BranchError::Invalid("branch name cannot be empty".into()));
    }
    if name == "." || name == ".." {
        return Err(BranchError::Invalid(format!(
            "'{}' is not a valid branch name",
            name
        )));
    }
    if name.contains('/') || name.contains('\0') {
        return Err(BranchError::Invalid(
            "branch name cannot contain '/' or null bytes".into(),
        ));
    }
    if name.starts_with('@') {
        return Err(BranchError::Invalid(
            "branch name cannot start with '@' (reserved for virtual paths)".into(),
        ));
    }
    if name.len() > 255 {
        return Err(BranchError::Invalid(
            "branch name cannot exceed 255 characters".into(),
        ));
    }
    Ok(())
}

pub struct BranchManager {
    pub storage_path: PathBuf,
    pub base_path: PathBuf,
    pub workspace_path: PathBuf,
    branches: RwLock<HashMap<String, Branch>>,
    pub epoch: AtomicU64,
    /// Notifiers for invalidating kernel cache on commit/abort
    /// Maps (branch_name, mountpoint) -> Notifier
    notifiers: Mutex<HashMap<(String, PathBuf), Arc<Notifier>>>,
    /// Track opened file inodes per branch for cache invalidation
    /// Maps branch_name -> Set of inodes
    opened_inodes: Mutex<HashMap<String, HashSet<u64>>>,
    /// Current branch per mount — single source of truth
    mount_branches: RwLock<HashMap<PathBuf, String>>,
    /// Storage quota enforcement
    pub quota: StorageQuota,
}

impl BranchManager {
    pub fn new(
        storage_path: PathBuf,
        base_path: PathBuf,
        workspace_path: PathBuf,
        max_storage: Option<u64>,
    ) -> Result<Self> {
        fs::create_dir_all(&storage_path)?;

        let quota = StorageQuota::scan_usage(&storage_path, max_storage);

        // Always start fresh with just the "main" branch
        let mut branches = HashMap::new();
        let main_branch = Branch::new("main", None, &storage_path, 0)?;
        branches.insert("main".to_string(), main_branch);

        Ok(Self {
            storage_path,
            base_path,
            workspace_path,
            branches: RwLock::new(branches),
            epoch: AtomicU64::new(0),
            notifiers: Mutex::new(HashMap::new()),
            opened_inodes: Mutex::new(HashMap::new()),
            mount_branches: RwLock::new(HashMap::new()),
            quota,
        })
    }

    /// Register a mount's initial branch (called before FUSE spawn).
    pub fn set_mount_branch(&self, mountpoint: &Path, branch: &str) {
        self.mount_branches
            .write()
            .insert(mountpoint.to_path_buf(), branch.to_string());
    }

    /// Read the current branch for a mount.
    pub fn get_mount_branch(&self, mountpoint: &Path) -> Option<String> {
        self.mount_branches.read().get(mountpoint).cloned()
    }

    /// Atomically switch a mount's branch, re-keying the notifier map.
    pub fn switch_mount_branch(&self, mountpoint: &Path, new_branch: &str) {
        // Lock order: mount_branches → notifiers (never reversed)
        let mut mb = self.mount_branches.write();
        let old_branch = mb.insert(mountpoint.to_path_buf(), new_branch.to_string());

        // Re-key notifier from (old, mount) → (new, mount)
        if let Some(old) = old_branch {
            let mut notifiers = self.notifiers.lock();
            if let Some(notifier) = notifiers.remove(&(old.clone(), mountpoint.to_path_buf())) {
                notifiers.insert((new_branch.to_string(), mountpoint.to_path_buf()), notifier);
            }
            log::info!(
                "switch_mount_branch: {:?} '{}' -> '{}'",
                mountpoint,
                old,
                new_branch
            );
        }
    }

    /// Remove a mount's branch tracking and notifier (used on unmount).
    pub fn unregister_mount(&self, mountpoint: &Path) {
        let mut mb = self.mount_branches.write();
        let old_branch = mb.remove(mountpoint);
        if let Some(old) = old_branch {
            self.notifiers
                .lock()
                .remove(&(old, mountpoint.to_path_buf()));
        }
    }

    pub fn create_branch(&self, name: &str, parent: &str) -> Result<()> {
        let start = Instant::now();
        validate_branch_name(name)?;

        let mut branches = self.branches.write();

        if branches.contains_key(name) {
            return Err(BranchError::AlreadyExists(name.to_string()));
        }

        let parent_branch = branches
            .get(parent)
            .ok_or_else(|| BranchError::ParentNotFound(parent.to_string()))?;
        let parent_version = parent_branch.commit_count.load(Ordering::SeqCst);

        let branch = Branch::new(name, Some(parent), &self.storage_path, parent_version)?;
        if let Err(e) = self.snapshot_visible_tree(&branches, parent, &branch.inherited_dir) {
            let _ = fs::remove_dir_all(self.storage_path.join("branches").join(name));
            return Err(e);
        }
        branches.insert(name.to_string(), branch);

        let elapsed = start.elapsed();
        log::debug!(
            "[BENCH] create_branch '{}': {:?} ({} us)",
            name,
            elapsed,
            elapsed.as_micros()
        );

        Ok(())
    }

    pub fn get_branch(&self, _name: &str) -> Option<std::sync::Arc<Branch>> {
        // Note: This is a simplified version. In production, use Arc properly.
        None
    }

    pub fn with_branch<F, R>(&self, name: &str, f: F) -> Result<R>
    where
        F: FnOnce(&Branch) -> Result<R>,
    {
        let branches = self.branches.read();
        let branch = branches
            .get(name)
            .ok_or_else(|| BranchError::NotFound(name.to_string()))?;
        f(branch)
    }

    pub fn get_epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn is_branch_valid(&self, name: &str) -> bool {
        self.branches.read().contains_key(name)
    }

    pub fn list_branches(&self) -> Vec<(String, Option<String>)> {
        self.branches
            .read()
            .iter()
            .map(|(name, branch)| (name.clone(), branch.parent.clone()))
            .collect()
    }

    /// Register a notifier for a mounted branch
    pub fn register_notifier(
        &self,
        branch_name: &str,
        mountpoint: PathBuf,
        notifier: Arc<Notifier>,
    ) {
        self.notifiers
            .lock()
            .insert((branch_name.to_string(), mountpoint), notifier);
    }

    /// Unregister a notifier when unmounting
    pub fn unregister_notifier(&self, branch_name: &str, mountpoint: &Path) {
        self.notifiers
            .lock()
            .remove(&(branch_name.to_string(), mountpoint.to_path_buf()));
    }

    /// Register an opened file inode for cache invalidation tracking
    pub fn register_opened_inode(&self, branch_name: &str, ino: u64) {
        self.opened_inodes
            .lock()
            .entry(branch_name.to_string())
            .or_default()
            .insert(ino);
    }

    /// Invalidate kernel cache for all mounts
    fn invalidate_all_mounts(&self) {
        let notifiers = self.notifiers.lock();
        let opened_inodes = self.opened_inodes.lock();

        for ((branch, mountpoint), notifier) in notifiers.iter() {
            // Invalidate root inode first (directory cache)
            if let Err(e) = notifier.inval_inode(ROOT_INO, 0, -1) {
                log::debug!(
                    "Failed to invalidate root inode for branch '{}' at {:?}: {}",
                    branch,
                    mountpoint,
                    e
                );
            }

            // Invalidate all opened file inodes for this branch
            if let Some(inodes) = opened_inodes.get(branch) {
                for &ino in inodes {
                    if ino != ROOT_INO {
                        if let Err(e) = notifier.inval_inode(ino, 0, -1) {
                            log::debug!(
                                "Failed to invalidate inode {} for branch '{}': {}",
                                ino,
                                branch,
                                e
                            );
                        } else {
                            log::debug!(
                                "Invalidated inode {} for branch '{}' at {:?}",
                                ino,
                                branch,
                                mountpoint
                            );
                        }
                    }
                }
            }

            log::info!(
                "Invalidated cache for branch '{}' at {:?}",
                branch,
                mountpoint
            );
        }
    }

    /// Invalidate kernel cache for specific branches
    pub fn invalidate_branches(&self, branch_names: &[String]) {
        let notifiers = self.notifiers.lock();
        let opened_inodes = self.opened_inodes.lock();

        for ((branch, mountpoint), notifier) in notifiers.iter() {
            if branch_names.contains(branch) {
                // Invalidate root inode
                if let Err(e) = notifier.inval_inode(ROOT_INO, 0, -1) {
                    log::debug!(
                        "Failed to invalidate root inode for branch '{}' at {:?}: {}",
                        branch,
                        mountpoint,
                        e
                    );
                }

                // Invalidate all opened file inodes
                if let Some(inodes) = opened_inodes.get(branch) {
                    for &ino in inodes {
                        if ino != ROOT_INO {
                            if let Err(e) = notifier.inval_inode(ino, 0, -1) {
                                log::debug!(
                                    "Failed to invalidate inode {} for branch '{}': {}",
                                    ino,
                                    branch,
                                    e
                                );
                            }
                        }
                    }
                }

                log::info!(
                    "Invalidated cache for branch '{}' at {:?}",
                    branch,
                    mountpoint
                );
            }
        }
    }

    fn inherited_source_path(&self, branch: &Branch, rel_path: &str) -> PathBuf {
        if branch.parent.is_some() {
            branch.inherited_path(rel_path)
        } else {
            self.base_path.join(rel_path.trim_start_matches('/'))
        }
    }

    fn collect_dir_names_locked(
        &self,
        branches: &HashMap<String, Branch>,
        branch_name: &str,
        rel_path: &str,
    ) -> Result<HashSet<String>> {
        let branch = branches
            .get(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;
        let mut names = HashSet::new();

        let delta_dir = branch.files_dir.join(rel_path.trim_start_matches('/'));
        if let Ok(dir) = fs::read_dir(&delta_dir) {
            for entry in dir.flatten() {
                names.insert(entry.file_name().to_string_lossy().to_string());
            }
        }

        let inherited_dir = self.inherited_source_path(branch, rel_path);
        if let Ok(dir) = fs::read_dir(&inherited_dir) {
            for entry in dir.flatten() {
                names.insert(entry.file_name().to_string_lossy().to_string());
            }
        }

        Ok(names)
    }

    fn resolve_path_locked(
        &self,
        branches: &HashMap<String, Branch>,
        branch_name: &str,
        rel_path: &str,
    ) -> Result<Option<PathBuf>> {
        let branch = branches
            .get(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        if branch.is_deleted(rel_path) {
            return Ok(None);
        }

        if branch.has_delta(rel_path) {
            return Ok(Some(branch.delta_path(rel_path)));
        }

        let inherited = self.inherited_source_path(branch, rel_path);
        if inherited.symlink_metadata().is_ok() {
            Ok(Some(inherited))
        } else {
            Ok(None)
        }
    }

    fn snapshot_visible_tree(
        &self,
        branches: &HashMap<String, Branch>,
        source_branch: &str,
        dst_root: &Path,
    ) -> Result<()> {
        if dst_root.exists() {
            fs::remove_dir_all(dst_root)?;
        }
        fs::create_dir_all(dst_root)?;
        self.snapshot_visible_dir(branches, source_branch, "/", dst_root)
    }

    fn snapshot_visible_dir(
        &self,
        branches: &HashMap<String, Branch>,
        source_branch: &str,
        rel_path: &str,
        dst_root: &Path,
    ) -> Result<()> {
        let mut names: Vec<String> = self
            .collect_dir_names_locked(branches, source_branch, rel_path)?
            .into_iter()
            .collect();
        names.sort();

        for name in names {
            let child_rel = if rel_path == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", rel_path, name)
            };

            let Some(src) = self.resolve_path_locked(branches, source_branch, &child_rel)? else {
                continue;
            };
            let meta = src.symlink_metadata()?;
            let dst = dst_root.join(child_rel.trim_start_matches('/'));

            if meta.file_type().is_dir() {
                fs::create_dir_all(&dst)?;
                fs::set_permissions(&dst, meta.permissions())?;
                self.snapshot_visible_dir(branches, source_branch, &child_rel, dst_root)?;
            } else {
                storage::copy_entry(&src, &dst)?;
            }
        }

        Ok(())
    }

    /// Collect all candidate file/directory names visible in a directory.
    /// Branches resolve against their own deltas plus the frozen inherited
    /// snapshot captured at fork time; main resolves against its delta plus
    /// the live base directory.
    pub fn collect_dir_names(&self, branch_name: &str, rel_path: &str) -> Result<HashSet<String>> {
        let branches = self.branches.read();
        self.collect_dir_names_locked(&branches, branch_name, rel_path)
    }

    pub fn resolve_path(&self, branch_name: &str, rel_path: &str) -> Result<Option<PathBuf>> {
        let branches = self.branches.read();
        self.resolve_path_locked(&branches, branch_name, rel_path)
    }

    /// Returns true if no other branch has `parent == name`.
    fn is_leaf(name: &str, branches: &std::collections::HashMap<String, Branch>) -> bool {
        !branches.values().any(|b| b.parent.as_deref() == Some(name))
    }

    /// Commit a leaf branch into its immediate parent.
    /// Returns the parent branch name on success.
    pub fn commit(&self, branch_name: &str) -> Result<String> {
        let start = Instant::now();
        if branch_name == "main" {
            return Err(BranchError::CannotOperateOnMain);
        }

        let mut branches = self.branches.write();

        let branch = branches
            .get(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        if !Self::is_leaf(branch_name, &branches) {
            return Err(BranchError::NotALeaf(branch_name.to_string()));
        }

        let parent_name = branch
            .parent
            .clone()
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        let child_version_at_fork = branch.parent_version_at_fork;

        // First-wins conflict detection: check that the parent hasn't had
        // another sibling committed since this branch was forked.
        {
            let parent = branches
                .get(&parent_name)
                .ok_or_else(|| BranchError::NotFound(parent_name.to_string()))?;
            let current_parent_version = parent.commit_count.load(Ordering::SeqCst);
            if current_parent_version != child_version_at_fork {
                return Err(BranchError::Conflict(branch_name.to_string()));
            }
        }

        let child_tombstones = branch.get_tombstones();
        let child_files_dir = branch.files_dir.clone();

        if parent_name == "main" {
            // Direct child of main: apply to the base filesystem atomically.
            //
            // Phase 1 (rollback-able): stage tombstone deletions (rename the
            // base entries aside) and copy every delta file to a temp sibling.
            // Deletions are staged before copies so a path whose type changed
            // (e.g. a directory replaced by a file) is cleared before its
            // replacement is staged. No base data is destroyed yet: any error
            // here propagates, `staged` is dropped (temps removed, trashed
            // entries restored), and the branch + its delta are preserved for
            // retry or abort — never a false success or a partial mutation.
            let mut staged = StagedMerge::new();
            for path in &child_tombstones {
                let full_path = self.base_path.join(path.trim_start_matches('/'));
                staged.stage_delete(&full_path)?;
            }
            self.walk_files(&child_files_dir, "", &mut |rel_path, src_path| {
                staged.stage_copy(rel_path, src_path, &self.base_path)
            })?;

            // Phase 2 (near-infallible): publish the copies and discard the
            // trashed deletions.
            let total_bytes = staged.bytes;
            let committed_paths = staged.commit()?;
            let num_files = committed_paths.len() as u64;

            // Remove main's delta for committed/tombstoned paths so base
            // takes precedence.  Without this, main's pre-existing delta
            // (written before branching) would overshadow the updated base.
            if let Some(main_branch) = branches.get("main") {
                let main_files_dir = &main_branch.files_dir;
                for rel_path in committed_paths.iter().chain(&child_tombstones) {
                    let main_delta = main_files_dir.join(rel_path.trim_start_matches('/'));
                    let _ = remove_entry(&main_delta);
                }
            }

            // Increment parent's commit_count (first-wins bookkeeping)
            if let Some(main_branch) = branches.get("main") {
                main_branch.commit_count.fetch_add(1, Ordering::SeqCst);
            }

            // Remove branch
            branches.remove(branch_name);
            let branch_dir = self.storage_path.join("branches").join(branch_name);
            if branch_dir.exists() {
                fs::remove_dir_all(&branch_dir)?;
            }

            self.epoch.fetch_add(1, Ordering::SeqCst);

            drop(branches);
            self.invalidate_all_mounts();

            let elapsed = start.elapsed();
            log::debug!(
                "[BENCH] commit '{}' to base: {:?} ({} us), {} deletions, {} files, {} bytes",
                branch_name,
                elapsed,
                elapsed.as_micros(),
                child_tombstones.len(),
                num_files,
                total_bytes
            );
        } else {
            // Nested branch: merge delta into parent's delta
            let parent = branches
                .get(&parent_name)
                .ok_or_else(|| BranchError::NotFound(parent_name.to_string()))?;

            let parent_files_dir = parent.files_dir.clone();
            let mut parent_tombstones = parent.get_tombstones();

            // Phase 1 (rollback-able): stage the merge into the parent's delta
            // directory. For each child tombstone, stage the deletion of any
            // matching parent-delta entry (renamed aside) and record the
            // tombstone; then copy every child delta file to a temp sibling.
            // Deletions are staged before copies so a type change at a path
            // (e.g. a directory replaced by a file) is cleared first. Any error
            // here drops `staged`, restoring the parent's delta untouched.
            let mut staged = StagedMerge::new();
            for tombstone in &child_tombstones {
                let parent_delta = parent_files_dir.join(tombstone.trim_start_matches('/'));
                staged.stage_delete(&parent_delta)?;
                parent_tombstones.insert(tombstone.clone());
            }
            self.walk_files(&child_files_dir, "", &mut |rel_path, src_path| {
                staged.stage_copy(rel_path, src_path, &parent_files_dir)
            })?;

            // Phase 2 (near-infallible): publish the copies into the parent's
            // delta directory and discard the trashed deletions.
            let copied_paths = staged.commit()?;

            // A path that now has a delta file is no longer deleted.
            for path in &copied_paths {
                parent_tombstones.remove(path);
            }

            // Write updated tombstones to parent
            parent.set_tombstones(parent_tombstones)?;

            // Increment parent's commit_count (first-wins bookkeeping)
            parent.commit_count.fetch_add(1, Ordering::SeqCst);

            // Remove child branch
            branches.remove(branch_name);
            let branch_dir = self.storage_path.join("branches").join(branch_name);
            if branch_dir.exists() {
                fs::remove_dir_all(&branch_dir)?;
            }

            self.epoch.fetch_add(1, Ordering::SeqCst);

            let affected = vec![branch_name.to_string(), parent_name.clone()];
            drop(branches);
            self.invalidate_branches(&affected);

            let elapsed = start.elapsed();
            log::debug!(
                "[BENCH] commit '{}' into parent '{}': {:?} ({} us)",
                branch_name,
                parent_name,
                elapsed,
                elapsed.as_micros(),
            );
        }

        Ok(parent_name)
    }

    /// Abort a leaf branch, discarding only that branch.
    /// Returns the parent branch name on success.
    pub fn abort(&self, branch_name: &str) -> Result<String> {
        let start = Instant::now();
        if branch_name == "main" {
            return Err(BranchError::CannotOperateOnMain);
        }

        let mut branches = self.branches.write();

        let branch = branches
            .get(branch_name)
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        if !Self::is_leaf(branch_name, &branches) {
            return Err(BranchError::NotALeaf(branch_name.to_string()));
        }

        let parent_name = branch
            .parent
            .clone()
            .ok_or_else(|| BranchError::NotFound(branch_name.to_string()))?;

        // Remove only this branch
        branches.remove(branch_name);
        let branch_dir = self.storage_path.join("branches").join(branch_name);
        if branch_dir.exists() {
            fs::remove_dir_all(&branch_dir)?;
        }

        // Invalidate kernel cache for this branch only
        drop(branches);
        self.invalidate_branches(&[branch_name.to_string()]);

        let elapsed = start.elapsed();
        log::debug!(
            "[BENCH] abort '{}': {:?} ({} us)",
            branch_name,
            elapsed,
            elapsed.as_micros()
        );

        Ok(parent_name)
    }

    fn walk_files<F>(&self, dir: &Path, prefix: &str, f: &mut F) -> Result<()>
    where
        F: FnMut(&str, &Path) -> Result<()>,
    {
        if !dir.exists() {
            return Ok(());
        }

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            let rel_path = if prefix.is_empty() {
                format!("/{}", name)
            } else {
                format!("{}/{}", prefix, name)
            };

            let is_dir = path
                .symlink_metadata()
                .map(|m| m.file_type().is_dir())
                .unwrap_or(false);
            if is_dir {
                self.walk_files(&path, &rel_path, f)?;
            } else {
                f(&rel_path, &path)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod staged_merge_tests {
    use super::{commit_side_path, StagedMerge};
    use std::fs;
    use std::path::{Path, PathBuf};

    /// A unique temp directory removed on drop (even if a test panics).
    /// Matches the manual-cleanup style of the integration tests and avoids a
    /// dev-dependency, reusing the `uuid` crate already in `[dependencies]`.
    struct TmpDir(PathBuf);

    impl TmpDir {
        fn new() -> Self {
            let p = std::env::temp_dir().join(format!("branchfs-test-{}", uuid::Uuid::new_v4()));
            fs::create_dir_all(&p).unwrap();
            TmpDir(p)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TmpDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn write(path: &Path, data: &[u8]) {
        fs::write(path, data).unwrap();
    }

    fn tmp_of(dest: &std::path::Path) -> std::path::PathBuf {
        commit_side_path(dest, "tmp")
    }

    fn trash_of(target: &std::path::Path) -> std::path::PathBuf {
        commit_side_path(target, "trash")
    }

    #[test]
    fn commit_publishes_all_staged_files() {
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();
        write(&src.join("a"), b"new-a");
        write(&src.join("b"), b"new-b");

        let mut staged = StagedMerge::new();
        staged.stage_copy("/a", &src.join("a"), &dst).unwrap();
        staged.stage_copy("/b", &src.join("b"), &dst).unwrap();
        let merged = staged.commit().unwrap();

        assert_eq!(merged.len(), 2);
        assert_eq!(fs::read(dst.join("a")).unwrap(), b"new-a");
        assert_eq!(fs::read(dst.join("b")).unwrap(), b"new-b");
        // temps are gone after publish
        assert!(!tmp_of(&dst.join("a")).exists());
        assert!(!tmp_of(&dst.join("b")).exists());
    }

    #[test]
    fn commit_publishes_nested_paths() {
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(src.join("sub")).unwrap();
        fs::create_dir_all(&dst).unwrap();
        write(&src.join("sub/x"), b"new-x");

        let mut staged = StagedMerge::new();
        staged
            .stage_copy("/sub/x", &src.join("sub/x"), &dst)
            .unwrap();
        staged.commit().unwrap();

        assert_eq!(fs::read(dst.join("sub/x")).unwrap(), b"new-x");
        assert!(!tmp_of(&dst.join("sub/x")).exists());
    }

    #[test]
    fn drop_without_commit_removes_temps_and_preserves_dest() {
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();
        write(&src.join("a"), b"new-a");
        write(&dst.join("a"), b"old-a"); // pre-existing destination data

        {
            let mut staged = StagedMerge::new();
            staged.stage_copy("/a", &src.join("a"), &dst).unwrap();
            // temp exists while staged, dest still holds old data
            assert!(tmp_of(&dst.join("a")).exists());
            assert_eq!(fs::read(dst.join("a")).unwrap(), b"old-a");
            // dropped here without commit()
        }

        // temp cleaned up; destination data untouched
        assert!(!tmp_of(&dst.join("a")).exists());
        assert_eq!(fs::read(dst.join("a")).unwrap(), b"old-a");
    }

    #[test]
    fn failed_stage_propagates_and_cleans_up() {
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(&dst).unwrap();
        write(&src.join("a"), b"new-a");
        write(&dst.join("a"), b"old-a");

        let mut staged = StagedMerge::new();
        staged.stage_copy("/a", &src.join("a"), &dst).unwrap();
        // staging a non-existent source makes copy_entry fail
        let err = staged.stage_copy("/missing", &src.join("missing"), &dst);
        assert!(err.is_err());
        drop(staged);

        // first temp removed, original dest data preserved (no partial publish)
        assert!(!tmp_of(&dst.join("a")).exists());
        assert_eq!(fs::read(dst.join("a")).unwrap(), b"old-a");
    }

    #[test]
    fn stage_delete_then_commit_removes_target() {
        let tmp = TmpDir::new();
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&dst).unwrap();
        write(&dst.join("a"), b"old-a");

        let mut staged = StagedMerge::new();
        staged.stage_delete(&dst.join("a")).unwrap();
        // target is moved aside (gone from its path) but not yet destroyed
        assert!(!dst.join("a").exists());
        assert!(trash_of(&dst.join("a")).exists());
        staged.commit().unwrap();

        // target deleted, trash discarded
        assert!(!dst.join("a").exists());
        assert!(!trash_of(&dst.join("a")).exists());
    }

    #[test]
    fn stage_delete_rolled_back_on_drop() {
        let tmp = TmpDir::new();
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&dst).unwrap();
        write(&dst.join("a"), b"old-a");

        {
            let mut staged = StagedMerge::new();
            staged.stage_delete(&dst.join("a")).unwrap();
            assert!(!dst.join("a").exists());
            // dropped without commit()
        }

        // original restored with its data; trash gone
        assert_eq!(fs::read(dst.join("a")).unwrap(), b"old-a");
        assert!(!trash_of(&dst.join("a")).exists());
    }

    #[test]
    fn stage_delete_missing_target_is_noop() {
        let tmp = TmpDir::new();
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&dst).unwrap();

        let mut staged = StagedMerge::new();
        staged.stage_delete(&dst.join("does-not-exist")).unwrap();
        staged.commit().unwrap();
    }

    #[test]
    fn replace_directory_with_file() {
        // Regression for the dir->file commit case: a base directory tombstoned
        // and replaced by a file at the same path. Staging the deletion first
        // clears the directory so the file can be published.
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(dst.join("p")).unwrap();
        write(&dst.join("p/inner"), b"inner"); // base dir has contents
        write(&src.join("p"), b"now-a-file"); // delta file replacing it

        let mut staged = StagedMerge::new();
        staged.stage_delete(&dst.join("p")).unwrap(); // tombstone the dir
        staged.stage_copy("/p", &src.join("p"), &dst).unwrap(); // file at same path
        staged.commit().unwrap();

        let meta = dst.join("p").symlink_metadata().unwrap();
        assert!(meta.file_type().is_file());
        assert_eq!(fs::read(dst.join("p")).unwrap(), b"now-a-file");
    }

    #[test]
    fn stage_copy_replaces_directory_without_explicit_tombstone() {
        // A delta file at a path where the destination is a directory, with no
        // staged deletion (reachable via rename, which clears the tombstone):
        // stage_copy must clear the directory itself so publish can place the file.
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(dst.join("d")).unwrap();
        write(&dst.join("d/inner"), b"inner");
        write(&src.join("d"), b"now-a-file");

        let mut staged = StagedMerge::new();
        staged.stage_copy("/d", &src.join("d"), &dst).unwrap(); // no stage_delete
        staged.commit().unwrap();

        assert!(dst
            .join("d")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_file());
        assert_eq!(fs::read(dst.join("d")).unwrap(), b"now-a-file");
    }

    #[test]
    fn replace_directory_with_file_rolls_back_on_failure() {
        // If a copy fails after the directory deletion was staged, the dropped
        // StagedMerge must restore the original directory and its contents.
        let tmp = TmpDir::new();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::create_dir_all(dst.join("p")).unwrap();
        write(&dst.join("p/inner"), b"inner");

        let mut staged = StagedMerge::new();
        staged.stage_delete(&dst.join("p")).unwrap();
        // copy of a non-existent source fails
        assert!(staged.stage_copy("/p", &src.join("p"), &dst).is_err());
        drop(staged);

        // directory and its contents restored exactly
        assert!(dst
            .join("p")
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_dir());
        assert_eq!(fs::read(dst.join("p/inner")).unwrap(), b"inner");
        assert!(!trash_of(&dst.join("p")).exists());
    }

    #[cfg(unix)]
    #[test]
    fn distinct_non_utf8_names_get_distinct_side_paths() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let dir = std::path::Path::new("/dir");
        let p1 = dir.join(OsStr::from_bytes(b"\xff"));
        let p2 = dir.join(OsStr::from_bytes(b"\xfe"));
        // Lossy conversion would map both to U+FFFD and collide; raw OsStr must not.
        assert_ne!(commit_side_path(&p1, "tmp"), commit_side_path(&p2, "tmp"));
    }
}
