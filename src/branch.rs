use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use fuser::Notifier;
use parking_lot::{Mutex, RwLock};

use crate::error::{BranchError, Result};
use crate::inode::ROOT_INO;
use crate::state::{BranchInfo, BranchState, State};

/// Type alias for collected changes: (deletions, file modifications)
type CollectedChanges = (HashSet<String>, Vec<(String, PathBuf)>);

pub struct Branch {
    pub name: String,
    pub parent: Option<String>,
    pub files_dir: PathBuf,
    pub tombstones_file: PathBuf,
    tombstones: RwLock<HashSet<String>>,
}

impl Branch {
    pub fn new(name: &str, parent: Option<&str>, storage_path: &Path) -> Result<Self> {
        let branch_dir = storage_path.join("branches").join(name);
        let files_dir = branch_dir.join("files");
        let tombstones_file = branch_dir.join("tombstones");

        fs::create_dir_all(&files_dir)?;
        if !tombstones_file.exists() {
            File::create(&tombstones_file)?;
        }

        let tombstones = Self::load_tombstones(&tombstones_file)?;

        Ok(Self {
            name: name.to_string(),
            parent: parent.map(|s| s.to_string()),
            files_dir,
            tombstones_file,
            tombstones: RwLock::new(tombstones),
        })
    }

    pub fn load(name: &str, info: &BranchInfo, storage_path: &Path) -> Result<Self> {
        Self::new(name, info.parent.as_deref(), storage_path)
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
        self.tombstones.write().remove(path);
    }

    pub fn get_tombstones(&self) -> HashSet<String> {
        self.tombstones.read().clone()
    }

    pub fn delta_path(&self, rel_path: &str) -> PathBuf {
        self.files_dir.join(rel_path.trim_start_matches('/'))
    }

    pub fn has_delta(&self, rel_path: &str) -> bool {
        self.delta_path(rel_path).exists()
    }
}

pub struct BranchManager {
    pub storage_path: PathBuf,
    pub base_path: PathBuf,
    pub workspace_path: PathBuf,
    branches: RwLock<std::collections::HashMap<String, Branch>>,
    state_file: PathBuf,
    pub epoch: AtomicU64,
    /// Notifiers for invalidating kernel cache on commit/abort
    /// Maps (branch_name, mountpoint) -> Notifier
    notifiers: Mutex<std::collections::HashMap<(String, PathBuf), Arc<Notifier>>>,
    /// Track opened file inodes per branch for cache invalidation
    /// Maps branch_name -> Set of inodes
    opened_inodes: Mutex<std::collections::HashMap<String, HashSet<u64>>>,
}

impl BranchManager {
    pub fn new(storage_path: PathBuf, base_path: PathBuf, workspace_path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&storage_path)?;
        let state_file = storage_path.join("state.json");

        let state = if state_file.exists() {
            State::load(&state_file)?
        } else {
            let state = State::new(base_path.clone(), workspace_path.clone());
            state.save(&state_file)?;
            state
        };

        let mut branches = std::collections::HashMap::new();
        for (name, info) in &state.branches {
            if info.state == BranchState::Active {
                let branch = Branch::load(name, info, &storage_path)?;
                branches.insert(name.clone(), branch);
            }
        }

        Ok(Self {
            storage_path,
            base_path,
            workspace_path,
            branches: RwLock::new(branches),
            state_file: state_file.clone(),
            epoch: AtomicU64::new(state.epoch),
            notifiers: Mutex::new(std::collections::HashMap::new()),
            opened_inodes: Mutex::new(std::collections::HashMap::new()),
        })
    }

    pub fn create_branch(&self, name: &str, parent: &str) -> Result<()> {
        let mut branches = self.branches.write();

        if branches.contains_key(name) {
            return Err(BranchError::AlreadyExists(name.to_string()));
        }

        if !branches.contains_key(parent) {
            return Err(BranchError::ParentNotFound(parent.to_string()));
        }

        let branch = Branch::new(name, Some(parent), &self.storage_path)?;
        branches.insert(name.to_string(), branch);

        self.save_state(&branches)?;
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

    pub fn resolve_path(&self, branch_name: &str, rel_path: &str) -> Result<Option<PathBuf>> {
        let branches = self.branches.read();

        let mut current = branch_name;
        loop {
            let branch = branches
                .get(current)
                .ok_or_else(|| BranchError::NotFound(current.to_string()))?;

            if branch.is_deleted(rel_path) {
                return Ok(None);
            }

            if branch.has_delta(rel_path) {
                return Ok(Some(branch.delta_path(rel_path)));
            }

            match &branch.parent {
                Some(parent) => current = parent,
                None => break,
            }
        }

        let base = self.base_path.join(rel_path.trim_start_matches('/'));
        if base.exists() {
            Ok(Some(base))
        } else {
            Ok(None)
        }
    }

    pub fn commit(&self, branch_name: &str) -> Result<()> {
        if branch_name == "main" {
            return Err(BranchError::CannotOperateOnMain);
        }

        let mut branches = self.branches.write();

        let chain = self.get_branch_chain(branch_name, &branches)?;
        let (deletions, files) = self.collect_changes(&chain, &branches)?;

        for path in &deletions {
            let full_path = self.base_path.join(path.trim_start_matches('/'));
            if full_path.exists() {
                if full_path.is_dir() {
                    fs::remove_dir_all(&full_path)?;
                } else {
                    fs::remove_file(&full_path)?;
                }
            }
        }

        for (rel_path, src_path) in &files {
            let dest = self.base_path.join(rel_path.trim_start_matches('/'));
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(src_path, &dest)?;
        }

        branches.clear();
        let main_branch = Branch::new("main", None, &self.storage_path)?;
        branches.insert("main".to_string(), main_branch);

        self.epoch.fetch_add(1, Ordering::SeqCst);
        self.save_state(&branches)?;

        // Invalidate kernel cache for all mounts (epoch changed, everything is stale)
        // Must be done after releasing the branches lock to avoid deadlock
        drop(branches);
        self.invalidate_all_mounts();

        Ok(())
    }

    pub fn abort(&self, branch_name: &str) -> Result<()> {
        if branch_name == "main" {
            return Err(BranchError::CannotOperateOnMain);
        }

        let mut branches = self.branches.write();
        let chain = self.get_branch_chain(branch_name, &branches)?;

        // Collect branch names before modifying (for cache invalidation)
        let aborted_branches: Vec<String> =
            chain.iter().filter(|n| *n != "main").cloned().collect();

        for name in &chain {
            if name != "main" {
                branches.remove(name);
                let branch_dir = self.storage_path.join("branches").join(name);
                if branch_dir.exists() {
                    fs::remove_dir_all(&branch_dir)?;
                }
            }
        }

        // Note: abort does NOT increment epoch - only the aborted branch chain
        // becomes invalid, siblings remain valid. The aborted branches are removed
        // from state.json, so is_branch_valid() will return false for them.
        self.save_state(&branches)?;

        // Invalidate kernel cache for aborted branches only
        // Must be done after releasing the branches lock
        drop(branches);
        self.invalidate_branches(&aborted_branches);

        Ok(())
    }

    pub fn abort_single(&self, branch_name: &str) -> Result<()> {
        if branch_name == "main" {
            // Nothing to abort for main
            return Ok(());
        }

        let mut branches = self.branches.write();

        if !branches.contains_key(branch_name) {
            // Branch doesn't exist, nothing to do
            return Ok(());
        }

        // Remove only this branch
        branches.remove(branch_name);
        let branch_dir = self.storage_path.join("branches").join(branch_name);
        if branch_dir.exists() {
            fs::remove_dir_all(&branch_dir)?;
        }

        self.save_state(&branches)?;

        // Invalidate kernel cache for this branch only
        drop(branches);
        self.invalidate_branches(&[branch_name.to_string()]);

        Ok(())
    }

    fn get_branch_chain(
        &self,
        start: &str,
        branches: &std::collections::HashMap<String, Branch>,
    ) -> Result<Vec<String>> {
        let mut chain = Vec::new();
        let mut current = start;

        loop {
            chain.push(current.to_string());
            let branch = branches
                .get(current)
                .ok_or_else(|| BranchError::NotFound(current.to_string()))?;

            match &branch.parent {
                Some(parent) => current = parent,
                None => break,
            }
        }

        Ok(chain)
    }

    fn collect_changes(
        &self,
        chain: &[String],
        branches: &std::collections::HashMap<String, Branch>,
    ) -> Result<CollectedChanges> {
        let mut deletions = HashSet::new();
        let mut files = Vec::new();
        let mut seen_files = HashSet::new();

        for name in chain {
            let branch = branches.get(name).unwrap();

            for path in branch.get_tombstones() {
                deletions.insert(path);
            }

            if branch.files_dir.exists() {
                self.walk_files(&branch.files_dir, "", &mut |rel_path, full_path| {
                    if !seen_files.contains(rel_path) && !deletions.contains(rel_path) {
                        seen_files.insert(rel_path.to_string());
                        files.push((rel_path.to_string(), full_path.to_path_buf()));
                    }
                })?;
            }
        }

        Ok((deletions, files))
    }

    fn walk_files<F>(&self, dir: &Path, prefix: &str, f: &mut F) -> Result<()>
    where
        F: FnMut(&str, &Path),
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

            if path.is_dir() {
                self.walk_files(&path, &rel_path, f)?;
            } else {
                f(&rel_path, &path);
            }
        }

        Ok(())
    }

    fn save_state(&self, branches: &std::collections::HashMap<String, Branch>) -> Result<()> {
        let mut state = State::new(self.base_path.clone(), self.workspace_path.clone());
        state.branches.clear();
        state.epoch = self.epoch.load(Ordering::SeqCst);

        for (name, branch) in branches {
            state.branches.insert(
                name.clone(),
                BranchInfo {
                    parent: branch.parent.clone(),
                    state: BranchState::Active,
                },
            );
        }

        state.save(&self.state_file)?;
        Ok(())
    }
}
