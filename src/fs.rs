use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyIoctl, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;

use crate::branch::BranchManager;
use crate::inode::{InodeManager, ROOT_INO};
use crate::storage;

// Zero TTL forces the kernel to always revalidate with FUSE, ensuring consistent
// behavior after branch switches. This is important for speculative execution
// where branches can change at any time.
const TTL: Duration = Duration::from_secs(0);
const BLOCK_SIZE: u32 = 512;

pub const BRANCHFS_IOC_COMMIT: u32 = 0x4201;
pub const BRANCHFS_IOC_ABORT: u32 = 0x4202;

const CTL_FILE: &str = ".branchfs_ctl";
const CTL_INO: u64 = u64::MAX - 1;

/// Classified path context for an inode path.
enum PathContext {
    /// Virtual `@branch` directory (e.g. `/@feature-a`)
    BranchDir(String),
    /// Per-branch ctl file (e.g. `/@feature-a/.branchfs_ctl`)
    BranchCtl(String),
    /// File/dir inside a branch subtree – (branch_name, relative_path)
    BranchPath(String, String),
    /// Root's control file (`/.branchfs_ctl`)
    RootCtl,
    /// Regular path resolved via root's current branch
    RootPath(String),
}

pub struct BranchFs {
    manager: Arc<BranchManager>,
    inodes: InodeManager,
    branch_name: RwLock<String>,
    current_epoch: AtomicU64,
    /// Per-branch ctl inode numbers: branch_name → ino
    branch_ctl_inodes: RwLock<HashMap<String, u64>>,
    next_ctl_ino: AtomicU64,
}

impl BranchFs {
    pub fn new(manager: Arc<BranchManager>, branch_name: String) -> Self {
        let current_epoch = manager.get_epoch();
        Self {
            manager,
            inodes: InodeManager::new(),
            branch_name: RwLock::new(branch_name),
            current_epoch: AtomicU64::new(current_epoch),
            branch_ctl_inodes: RwLock::new(HashMap::new()),
            // Reserve a range well below CTL_INO (u64::MAX - 1) for branch ctl inodes.
            // Start from u64::MAX - 1_000_000 downward.
            next_ctl_ino: AtomicU64::new(u64::MAX - 1_000_000),
        }
    }

    fn get_branch_name(&self) -> String {
        self.branch_name.read().clone()
    }

    fn is_stale(&self) -> bool {
        let branch_name = self.get_branch_name();
        self.manager.get_epoch() != self.current_epoch.load(Ordering::SeqCst)
            || !self.manager.is_branch_valid(&branch_name)
    }

    /// Switch to a different branch (used after commit/abort to switch to main)
    fn switch_to_branch(&self, new_branch: &str) {
        *self.branch_name.write() = new_branch.to_string();
        self.current_epoch
            .store(self.manager.get_epoch(), Ordering::SeqCst);
        // Clear inode cache since we're on a different branch now
        self.inodes.clear();
    }

    fn resolve(&self, path: &str) -> Option<std::path::PathBuf> {
        self.manager
            .resolve_path(&self.get_branch_name(), path)
            .ok()?
    }

    /// Resolve a path within a specific branch (not the root's current branch).
    fn resolve_for_branch(&self, branch: &str, path: &str) -> Option<std::path::PathBuf> {
        self.manager.resolve_path(branch, path).ok()?
    }

    fn make_attr(&self, ino: u64, path: &Path) -> Option<FileAttr> {
        let meta = std::fs::metadata(path).ok()?;
        let kind = if meta.is_dir() {
            FileType::Directory
        } else if meta.is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        };

        Some(FileAttr {
            ino,
            size: meta.len(),
            blocks: meta.len().div_ceil(BLOCK_SIZE as u64),
            atime: meta.accessed().unwrap_or(UNIX_EPOCH),
            mtime: meta.modified().unwrap_or(UNIX_EPOCH),
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind,
            perm: meta.permissions().mode() as u16,
            nlink: meta.nlink() as u32,
            uid: meta.uid(),
            gid: meta.gid(),
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        })
    }

    fn get_delta_path(&self, rel_path: &str) -> std::path::PathBuf {
        self.manager
            .with_branch(&self.get_branch_name(), |b| Ok(b.delta_path(rel_path)))
            .unwrap()
    }

    fn get_delta_path_for_branch(&self, branch: &str, rel_path: &str) -> std::path::PathBuf {
        self.manager
            .with_branch(branch, |b| Ok(b.delta_path(rel_path)))
            .unwrap()
    }

    fn ensure_cow(&self, rel_path: &str) -> std::io::Result<std::path::PathBuf> {
        self.ensure_cow_for_branch(&self.get_branch_name(), rel_path)
    }

    fn ensure_cow_for_branch(
        &self,
        branch: &str,
        rel_path: &str,
    ) -> std::io::Result<std::path::PathBuf> {
        let delta = self.get_delta_path_for_branch(branch, rel_path);

        if !delta.exists() {
            if let Some(src) = self.resolve_for_branch(branch, rel_path) {
                if src.exists() && src.is_file() {
                    storage::copy_file(&src, &delta)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
            }
        }

        storage::ensure_parent_dirs(&delta).map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(delta)
    }

    /// Return a synthetic directory FileAttr.
    fn synthetic_dir_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    /// Return a synthetic ctl-file FileAttr.
    fn ctl_file_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o600,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    /// Get or create the ctl inode number for a branch.
    fn get_or_create_branch_ctl_ino(&self, branch: &str) -> u64 {
        {
            let map = self.branch_ctl_inodes.read();
            if let Some(&ino) = map.get(branch) {
                return ino;
            }
        }
        let mut map = self.branch_ctl_inodes.write();
        if let Some(&ino) = map.get(branch) {
            return ino;
        }
        let ino = self.next_ctl_ino.fetch_add(1, Ordering::SeqCst);
        map.insert(branch.to_string(), ino);
        ino
    }

    /// Check if an inode number is a branch ctl inode, returning the branch name.
    fn branch_for_ctl_ino(&self, ino: u64) -> Option<String> {
        let map = self.branch_ctl_inodes.read();
        for (name, &i) in map.iter() {
            if i == ino {
                return Some(name.clone());
            }
        }
        None
    }

    /// Classify an inode path into a PathContext.
    fn classify_path(&self, path: &str) -> PathContext {
        if path == "/" {
            return PathContext::RootPath("/".to_string());
        }

        // Paths under /@branch/...
        if path.starts_with("/@") {
            // Strip leading "/@"
            let rest = &path[2..];
            // Find the next '/' if any
            if let Some(slash_pos) = rest.find('/') {
                let branch = &rest[..slash_pos];
                let remainder = &rest[slash_pos..]; // e.g. "/.branchfs_ctl" or "/src/main.rs"

                // Handle nested @child: /@parent/@child/... → recurse as /@child/...
                if remainder.starts_with("/@") {
                    return self.classify_path(remainder);
                }

                if remainder == format!("/{}", CTL_FILE).as_str() {
                    PathContext::BranchCtl(branch.to_string())
                } else {
                    PathContext::BranchPath(branch.to_string(), remainder.to_string())
                }
            } else {
                // Just "/@branch" with no trailing content
                PathContext::BranchDir(rest.to_string())
            }
        } else {
            PathContext::RootPath(path.to_string())
        }
    }

    /// Classify an inode number. Returns None for root and CTL_INO (handled separately).
    fn classify_ino(&self, ino: u64) -> Option<PathContext> {
        if ino == ROOT_INO {
            return Some(PathContext::RootPath("/".to_string()));
        }
        if ino == CTL_INO {
            return Some(PathContext::RootCtl);
        }
        // Check if it's a branch ctl inode
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            return Some(PathContext::BranchCtl(branch));
        }
        let path = self.inodes.get_path(ino)?;
        Some(self.classify_path(&path))
    }

    /// Collect readdir entries for a directory resolved via a specific branch.
    fn collect_branch_readdir_entries(
        &self,
        branch: &str,
        rel_path: &str,
        ino: u64,
    ) -> Vec<(u64, FileType, String)> {
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (ino, FileType::Directory, "..".to_string()),
        ];

        let mut seen = std::collections::HashSet::new();

        // Collect from base directory
        let base_dir = self
            .manager
            .base_path
            .join(rel_path.trim_start_matches('/'));
        if let Ok(dir) = std::fs::read_dir(&base_dir) {
            for entry in dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if seen.insert(name.clone()) {
                    let child_rel = if rel_path == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", rel_path, name)
                    };
                    // Inode path in the global namespace: /@branch/child_rel
                    let inode_path = format!("/@{}{}", branch, child_rel);
                    let is_dir = entry.path().is_dir();
                    let child_ino = self.inodes.get_or_create(&inode_path, is_dir);
                    let kind = if is_dir {
                        FileType::Directory
                    } else {
                        FileType::RegularFile
                    };
                    entries.push((child_ino, kind, name));
                }
            }
        }

        // Collect from branch deltas
        if let Some(resolved) = self.resolve_for_branch(branch, rel_path) {
            if resolved != base_dir {
                if let Ok(dir) = std::fs::read_dir(&resolved) {
                    for entry in dir.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if seen.insert(name.clone()) {
                            let child_rel = if rel_path == "/" {
                                format!("/{}", name)
                            } else {
                                format!("{}/{}", rel_path, name)
                            };
                            let inode_path = format!("/@{}{}", branch, child_rel);
                            let is_dir = entry.path().is_dir();
                            let child_ino = self.inodes.get_or_create(&inode_path, is_dir);
                            let kind = if is_dir {
                                FileType::Directory
                            } else {
                                FileType::RegularFile
                            };
                            entries.push((child_ino, kind, name));
                        }
                    }
                }
            }
        }

        entries
    }
}

impl Filesystem for BranchFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();

        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // === Root-level lookups (parent is /) ===
        if parent_path == "/" {
            // Root ctl file
            if name_str == CTL_FILE {
                reply.entry(&TTL, &self.ctl_file_attr(CTL_INO), 0);
                return;
            }

            // @branch virtual directory
            if let Some(branch) = name_str.strip_prefix('@') {
                if self.manager.is_branch_valid(branch) {
                    let inode_path = format!("/@{}", branch);
                    let ino = self.inodes.get_or_create(&inode_path, true);
                    reply.entry(&TTL, &self.synthetic_dir_attr(ino), 0);
                    return;
                } else {
                    reply.error(libc::ENOENT);
                    return;
                }
            }

            // Regular root child — use current branch
            if self.is_stale() {
                reply.error(libc::ESTALE);
                return;
            }

            let path = format!("/{}", name_str);
            let resolved = match self.resolve(&path) {
                Some(p) => p,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };
            let is_dir = resolved.is_dir();
            let ino = self.inodes.get_or_create(&path, is_dir);
            match self.make_attr(ino, &resolved) {
                Some(attr) => reply.entry(&TTL, &attr, 0),
                None => reply.error(libc::ENOENT),
            }
            return;
        }

        // === Parent is inside an @branch subtree ===
        match self.classify_path(&parent_path) {
            PathContext::BranchDir(ref branch) | PathContext::BranchPath(ref branch, _) => {
                let branch = branch.clone();

                // Determine the relative path within the branch for the parent
                let parent_rel = match self.classify_path(&parent_path) {
                    PathContext::BranchDir(_) => "/".to_string(),
                    PathContext::BranchPath(_, rel) => rel,
                    _ => unreachable!(),
                };

                // Looking up .branchfs_ctl inside a branch dir (only at branch root)
                if parent_rel == "/" && name_str == CTL_FILE {
                    let ctl_ino = self.get_or_create_branch_ctl_ino(&branch);
                    reply.entry(&TTL, &self.ctl_file_attr(ctl_ino), 0);
                    return;
                }

                // Looking up @child inside a branch dir (nested branch)
                if name_str.starts_with('@') {
                    let child_branch = &name_str[1..];
                    // Check if child_branch is a child of this branch
                    let children = self.manager.get_children(&branch);
                    if children.iter().any(|c| c == child_branch) {
                        let inode_path = if parent_rel == "/" {
                            format!("/@{}/@{}", branch, child_branch)
                        } else {
                            // Nested @child only at branch root
                            reply.error(libc::ENOENT);
                            return;
                        };
                        let ino = self.inodes.get_or_create(&inode_path, true);
                        reply.entry(&TTL, &self.synthetic_dir_attr(ino), 0);
                        return;
                    } else {
                        reply.error(libc::ENOENT);
                        return;
                    }
                }

                // Regular file/dir inside branch
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }

                let child_rel = if parent_rel == "/" {
                    format!("/{}", name_str)
                } else {
                    format!("{}/{}", parent_rel, name_str)
                };

                let resolved = match self.resolve_for_branch(&branch, &child_rel) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };

                let inode_path = format!("/@{}{}", branch, child_rel);
                let is_dir = resolved.is_dir();
                let ino = self.inodes.get_or_create(&inode_path, is_dir);
                match self.make_attr(ino, &resolved) {
                    Some(attr) => reply.entry(&TTL, &attr, 0),
                    None => reply.error(libc::ENOENT),
                }
            }
            _ => {
                // Parent is a regular root-path subdir
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                let path = format!("{}/{}", parent_path, name_str);
                let resolved = match self.resolve(&path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                let is_dir = resolved.is_dir();
                let ino = self.inodes.get_or_create(&path, is_dir);
                match self.make_attr(ino, &resolved) {
                    Some(attr) => reply.entry(&TTL, &attr, 0),
                    None => reply.error(libc::ENOENT),
                }
            }
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // Root ctl file
        if ino == CTL_INO {
            reply.attr(&TTL, &self.ctl_file_attr(CTL_INO));
            return;
        }

        // Branch ctl file
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            if self.manager.is_branch_valid(&branch) {
                reply.attr(&TTL, &self.ctl_file_attr(ino));
            } else {
                reply.error(libc::ENOENT);
            }
            return;
        }

        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                if ino == ROOT_INO {
                    reply.attr(&TTL, &self.synthetic_dir_attr(ROOT_INO));
                    return;
                }
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.classify_path(&path) {
            PathContext::BranchDir(ref branch) => {
                if self.manager.is_branch_valid(branch) {
                    reply.attr(&TTL, &self.synthetic_dir_attr(ino));
                } else {
                    reply.error(libc::ENOENT);
                }
            }
            PathContext::BranchCtl(ref branch) => {
                if self.manager.is_branch_valid(branch) {
                    reply.attr(&TTL, &self.ctl_file_attr(ino));
                } else {
                    reply.error(libc::ENOENT);
                }
            }
            PathContext::BranchPath(ref branch, ref rel_path) => {
                if !self.manager.is_branch_valid(branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let resolved = match self.resolve_for_branch(branch, rel_path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                match self.make_attr(ino, &resolved) {
                    Some(attr) => reply.attr(&TTL, &attr),
                    None => reply.error(libc::ENOENT),
                }
            }
            PathContext::RootCtl => {
                reply.attr(&TTL, &self.ctl_file_attr(CTL_INO));
            }
            PathContext::RootPath(ref rp) => {
                if ino != ROOT_INO && self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }
                let resolved = match self.resolve(rp) {
                    Some(p) => p,
                    None => {
                        if ino == ROOT_INO {
                            reply.attr(&TTL, &self.synthetic_dir_attr(ROOT_INO));
                            return;
                        }
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                match self.make_attr(ino, &resolved) {
                    Some(attr) => reply.attr(&TTL, &attr),
                    None => reply.error(libc::ENOENT),
                }
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        match self.classify_ino(ino) {
            Some(PathContext::BranchPath(branch, rel_path)) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let resolved = match self.resolve_for_branch(&branch, &rel_path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                match std::fs::read(&resolved) {
                    Ok(data) => {
                        let start = offset as usize;
                        let end = std::cmp::min(start + size as usize, data.len());
                        if start < data.len() {
                            reply.data(&data[start..end]);
                        } else {
                            reply.data(&[]);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Some(PathContext::BranchDir(_)) | Some(PathContext::BranchCtl(_)) => {
                reply.error(libc::EISDIR);
            }
            _ => {
                // Root path or root ctl
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                let path = match self.inodes.get_path(ino) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };

                let resolved = match self.resolve(&path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };

                match std::fs::read(&resolved) {
                    Ok(data) => {
                        if self.is_stale() {
                            reply.error(libc::ESTALE);
                            return;
                        }
                        let start = offset as usize;
                        let end = std::cmp::min(start + size as usize, data.len());
                        if start < data.len() {
                            reply.data(&data[start..end]);
                        } else {
                            reply.data(&[]);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        // === Root ctl file ===
        if ino == CTL_INO {
            let cmd = String::from_utf8_lossy(data).trim().to_string();
            let cmd_lower = cmd.to_lowercase();
            let branch_name = self.get_branch_name();
            log::info!("Control command: '{}' for branch '{}'", cmd, branch_name);

            // Handle switch command: "switch:branchname"
            if cmd_lower.starts_with("switch:") {
                let new_branch = cmd[7..].trim();
                if new_branch.is_empty() {
                    log::warn!("Empty branch name in switch command");
                    reply.error(libc::EINVAL);
                    return;
                }
                if !self.manager.is_branch_valid(new_branch) {
                    log::warn!("Branch '{}' does not exist", new_branch);
                    reply.error(libc::ENOENT);
                    return;
                }
                self.switch_to_branch(new_branch);
                log::info!("Switched to branch '{}'", new_branch);
                reply.written(data.len() as u32);
                return;
            }

            let result = match cmd_lower.as_str() {
                "commit" => self.manager.commit(&branch_name),
                "abort" => self.manager.abort(&branch_name),
                _ => {
                    log::warn!("Unknown control command: {}", cmd);
                    reply.error(libc::EINVAL);
                    return;
                }
            };

            match result {
                Ok(()) => {
                    // Switch to main branch after successful commit/abort (like DAXFS remount)
                    self.switch_to_branch("main");
                    log::info!("Switched to main branch after {}", cmd_lower);
                    reply.written(data.len() as u32)
                }
                Err(e) => {
                    log::error!("Control command failed: {}", e);
                    reply.error(libc::EIO);
                }
            }
            return;
        }

        // === Per-branch ctl file ===
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            let cmd = String::from_utf8_lossy(data).trim().to_string();
            let cmd_lower = cmd.to_lowercase();
            log::info!(
                "Branch ctl command: '{}' for branch '{}'",
                cmd,
                branch
            );

            let result = match cmd_lower.as_str() {
                "commit" => self.manager.commit(&branch),
                "abort" => self.manager.abort(&branch),
                _ => {
                    log::warn!("Unknown branch ctl command: {}", cmd);
                    reply.error(libc::EINVAL);
                    return;
                }
            };

            match result {
                Ok(()) => {
                    if cmd_lower == "commit" {
                        // Commit clears all branches and increments epoch → clear everything
                        self.inodes.clear();
                        self.current_epoch
                            .store(self.manager.get_epoch(), Ordering::SeqCst);
                        *self.branch_name.write() = "main".to_string();
                    } else {
                        // Abort: clear inodes for the aborted branch prefix
                        self.inodes.clear_prefix(&format!("/@{}", branch));
                        // Also clear any child branches that may have been aborted
                        // (abort removes the whole chain)
                    }
                    log::info!("Branch ctl {} succeeded for '{}'", cmd_lower, branch);
                    reply.written(data.len() as u32)
                }
                Err(e) => {
                    log::error!("Branch ctl command failed: {}", e);
                    reply.error(libc::EIO);
                }
            }
            return;
        }

        // === Branch path write ===
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.classify_path(&path) {
            PathContext::BranchDir(_) | PathContext::BranchCtl(_) => {
                reply.error(libc::EPERM);
            }
            PathContext::BranchPath(branch, rel_path) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let delta = match self.ensure_cow_for_branch(&branch, &rel_path) {
                    Ok(p) => p,
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                };

                use std::io::{Seek, SeekFrom, Write};
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&delta);

                match file {
                    Ok(mut f) => {
                        if f.seek(SeekFrom::Start(offset as u64)).is_err() {
                            reply.error(libc::EIO);
                            return;
                        }
                        match f.write(data) {
                            Ok(n) => reply.written(n as u32),
                            Err(_) => reply.error(libc::EIO),
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            _ => {
                // Root path write (existing logic)
                let delta = match self.ensure_cow(&path) {
                    Ok(p) => p,
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                };

                use std::io::{Seek, SeekFrom, Write};
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&delta);

                match file {
                    Ok(mut f) => {
                        if f.seek(SeekFrom::Start(offset as u64)).is_err() {
                            reply.error(libc::EIO);
                            return;
                        }
                        match f.write(data) {
                            Ok(n) => {
                                if self.is_stale() {
                                    reply.error(libc::ESTALE);
                                    return;
                                }
                                reply.written(n as u32)
                            }
                            Err(_) => reply.error(libc::EIO),
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                if ino == ROOT_INO {
                    // fallback
                    "/".to_string()
                } else {
                    reply.error(libc::ENOENT);
                    return;
                }
            }
        };

        match self.classify_path(&path) {
            PathContext::BranchDir(branch) => {
                // Reading a branch dir root: `.`, `..`, `.branchfs_ctl`, @child dirs, real files
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }

                let mut entries = self.collect_branch_readdir_entries(&branch, "/", ino);

                // Add .branchfs_ctl
                let ctl_ino = self.get_or_create_branch_ctl_ino(&branch);
                entries.push((ctl_ino, FileType::RegularFile, CTL_FILE.to_string()));

                // Add @child virtual dirs for children of this branch
                let children = self.manager.get_children(&branch);
                for child in children {
                    let child_inode_path = format!("/@{}/@{}", branch, child);
                    let child_ino = self.inodes.get_or_create(&child_inode_path, true);
                    entries.push((child_ino, FileType::Directory, format!("@{}", child)));
                }

                for (i, (e_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }
                reply.ok();
            }
            PathContext::BranchPath(branch, rel_path) => {
                // Reading a subdirectory inside a branch
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }

                let entries = self.collect_branch_readdir_entries(&branch, &rel_path, ino);

                for (i, (e_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }
                reply.ok();
            }
            PathContext::RootPath(ref rp) if rp == "/" => {
                // Root directory: existing entries + @branch virtual dirs
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                let mut entries = vec![
                    (ino, FileType::Directory, ".".to_string()),
                    (ino, FileType::Directory, "..".to_string()),
                ];

                let mut seen = std::collections::HashSet::new();

                // Collect from base directory
                let base_dir = self.manager.base_path.clone();
                if let Ok(dir) = std::fs::read_dir(&base_dir) {
                    for entry in dir.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if seen.insert(name.clone()) {
                            let child_path = format!("/{}", name);
                            let is_dir = entry.path().is_dir();
                            let child_ino = self.inodes.get_or_create(&child_path, is_dir);
                            let kind = if is_dir {
                                FileType::Directory
                            } else {
                                FileType::RegularFile
                            };
                            entries.push((child_ino, kind, name));
                        }
                    }
                }

                // Collect from branch deltas (current branch)
                if let Some(resolved) = self.resolve("/") {
                    if resolved != base_dir {
                        if let Ok(dir) = std::fs::read_dir(&resolved) {
                            for entry in dir.flatten() {
                                let name = entry.file_name().to_string_lossy().to_string();
                                if seen.insert(name.clone()) {
                                    let child_path = format!("/{}", name);
                                    let is_dir = entry.path().is_dir();
                                    let child_ino =
                                        self.inodes.get_or_create(&child_path, is_dir);
                                    let kind = if is_dir {
                                        FileType::Directory
                                    } else {
                                        FileType::RegularFile
                                    };
                                    entries.push((child_ino, kind, name));
                                }
                            }
                        }
                    }
                }

                // Add .branchfs_ctl
                entries.push((CTL_INO, FileType::RegularFile, CTL_FILE.to_string()));

                // Add @branch virtual dirs for branches that are children of
                // the root's current branch (i.e. main's children typically)
                // We list ALL non-main branches as @branch dirs at root level.
                let branches = self.manager.list_branches();
                for (bname, _parent) in branches {
                    if bname != "main" {
                        let inode_path = format!("/@{}", bname);
                        let bino = self.inodes.get_or_create(&inode_path, true);
                        entries.push((bino, FileType::Directory, format!("@{}", bname)));
                    }
                }

                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                for (i, (e_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }

                reply.ok();
            }
            PathContext::RootPath(_) => {
                // Non-root subdir via current branch (existing logic)
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                let mut entries = vec![
                    (ino, FileType::Directory, ".".to_string()),
                    (ino, FileType::Directory, "..".to_string()),
                ];

                let mut seen = std::collections::HashSet::new();

                let base_dir = self
                    .manager
                    .base_path
                    .join(path.trim_start_matches('/'));
                if let Ok(dir) = std::fs::read_dir(&base_dir) {
                    for entry in dir.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if seen.insert(name.clone()) {
                            let child_path = format!("{}/{}", path, name);
                            let is_dir = entry.path().is_dir();
                            let child_ino = self.inodes.get_or_create(&child_path, is_dir);
                            let kind = if is_dir {
                                FileType::Directory
                            } else {
                                FileType::RegularFile
                            };
                            entries.push((child_ino, kind, name));
                        }
                    }
                }

                if let Some(resolved) = self.resolve(&path) {
                    if resolved != base_dir {
                        if let Ok(dir) = std::fs::read_dir(&resolved) {
                            for entry in dir.flatten() {
                                let name = entry.file_name().to_string_lossy().to_string();
                                if seen.insert(name.clone()) {
                                    let child_path = format!("{}/{}", path, name);
                                    let is_dir = entry.path().is_dir();
                                    let child_ino =
                                        self.inodes.get_or_create(&child_path, is_dir);
                                    let kind = if is_dir {
                                        FileType::Directory
                                    } else {
                                        FileType::RegularFile
                                    };
                                    entries.push((child_ino, kind, name));
                                }
                            }
                        }
                    }
                }

                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                for (i, (e_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(e_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }

                reply.ok();
            }
            _ => {
                reply.error(libc::ENOTDIR);
            }
        }
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = name.to_string_lossy();

        match self.classify_path(&parent_path) {
            PathContext::BranchDir(branch) => {
                // Creating a file inside a branch root dir
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let rel_path = format!("/{}", name_str);
                let delta = self.get_delta_path_for_branch(&branch, &rel_path);
                if storage::ensure_parent_dirs(&delta).is_err() {
                    reply.error(libc::EIO);
                    return;
                }
                match std::fs::File::create(&delta) {
                    Ok(_) => {
                        let inode_path = format!("/@{}{}", branch, rel_path);
                        let ino = self.inodes.get_or_create(&inode_path, false);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.created(&TTL, &attr, 0, 0, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            PathContext::BranchPath(branch, parent_rel) => {
                // Creating a file inside a branch subdir
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let rel_path = format!("{}/{}", parent_rel, name_str);
                let delta = self.get_delta_path_for_branch(&branch, &rel_path);
                if storage::ensure_parent_dirs(&delta).is_err() {
                    reply.error(libc::EIO);
                    return;
                }
                match std::fs::File::create(&delta) {
                    Ok(_) => {
                        let inode_path = format!("/@{}{}", branch, rel_path);
                        let ino = self.inodes.get_or_create(&inode_path, false);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.created(&TTL, &attr, 0, 0, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            PathContext::BranchCtl(_) | PathContext::RootCtl => {
                reply.error(libc::EPERM);
            }
            PathContext::RootPath(rp) => {
                // Existing root-path logic
                let path = if rp == "/" {
                    format!("/{}", name_str)
                } else {
                    format!("{}/{}", rp, name_str)
                };

                let delta = self.get_delta_path(&path);
                if storage::ensure_parent_dirs(&delta).is_err() {
                    reply.error(libc::EIO);
                    return;
                }

                match std::fs::File::create(&delta) {
                    Ok(_) => {
                        if self.is_stale() {
                            let _ = std::fs::remove_file(&delta);
                            reply.error(libc::ESTALE);
                            return;
                        }
                        let ino = self.inodes.get_or_create(&path, false);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.created(&TTL, &attr, 0, 0, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = name.to_string_lossy();

        match self.classify_path(&parent_path) {
            PathContext::BranchDir(branch) | PathContext::BranchPath(branch, _) => {
                let parent_rel = match self.classify_path(&parent_path) {
                    PathContext::BranchDir(_) => "/".to_string(),
                    PathContext::BranchPath(_, rel) => rel,
                    _ => unreachable!(),
                };

                // Can't unlink @child dirs or .branchfs_ctl
                if name_str.starts_with('@') || *name_str == *CTL_FILE {
                    reply.error(libc::EPERM);
                    return;
                }

                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }

                let rel_path = if parent_rel == "/" {
                    format!("/{}", name_str)
                } else {
                    format!("{}/{}", parent_rel, name_str)
                };

                let branch_clone = branch.clone();
                let result = self.manager.with_branch(&branch_clone, |b| {
                    b.add_tombstone(&rel_path)?;
                    let delta = b.delta_path(&rel_path);
                    if delta.exists() {
                        std::fs::remove_file(&delta)?;
                    }
                    Ok(())
                });

                if result.is_err() {
                    reply.error(libc::EIO);
                    return;
                }

                let inode_path = format!("/@{}{}", branch, rel_path);
                self.inodes.remove(&inode_path);
                reply.ok();
            }
            PathContext::BranchCtl(_) | PathContext::RootCtl => {
                reply.error(libc::EPERM);
            }
            PathContext::RootPath(rp) => {
                // Existing root-path unlink
                let path = if rp == "/" {
                    format!("/{}", name_str)
                } else {
                    format!("{}/{}", rp, name_str)
                };

                let result = self.manager.with_branch(&self.get_branch_name(), |b| {
                    b.add_tombstone(&path)?;
                    let delta = b.delta_path(&path);
                    if delta.exists() {
                        std::fs::remove_file(&delta)?;
                    }
                    Ok(())
                });

                if result.is_err() || self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                self.inodes.remove(&path);
                reply.ok();
            }
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = name.to_string_lossy();

        match self.classify_path(&parent_path) {
            PathContext::BranchDir(branch) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let rel_path = format!("/{}", name_str);
                let delta = self.get_delta_path_for_branch(&branch, &rel_path);
                match std::fs::create_dir_all(&delta) {
                    Ok(_) => {
                        let inode_path = format!("/@{}{}", branch, rel_path);
                        let ino = self.inodes.get_or_create(&inode_path, true);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.entry(&TTL, &attr, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            PathContext::BranchPath(branch, parent_rel) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                let rel_path = format!("{}/{}", parent_rel, name_str);
                let delta = self.get_delta_path_for_branch(&branch, &rel_path);
                match std::fs::create_dir_all(&delta) {
                    Ok(_) => {
                        let inode_path = format!("/@{}{}", branch, rel_path);
                        let ino = self.inodes.get_or_create(&inode_path, true);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.entry(&TTL, &attr, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            PathContext::BranchCtl(_) | PathContext::RootCtl => {
                reply.error(libc::EPERM);
            }
            PathContext::RootPath(rp) => {
                let path = if rp == "/" {
                    format!("/{}", name_str)
                } else {
                    format!("{}/{}", rp, name_str)
                };

                let delta = self.get_delta_path(&path);
                match std::fs::create_dir_all(&delta) {
                    Ok(_) => {
                        if self.is_stale() {
                            let _ = std::fs::remove_dir_all(&delta);
                            reply.error(libc::ESTALE);
                            return;
                        }
                        let ino = self.inodes.get_or_create(&path, true);
                        if let Some(attr) = self.make_attr(ino, &delta) {
                            reply.entry(&TTL, &attr, 0);
                        } else {
                            reply.error(libc::EIO);
                        }
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.unlink(_req, parent, name, reply);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        // Control file is always openable (no epoch check)
        if ino == CTL_INO {
            reply.opened(0, 0);
            return;
        }

        // Branch ctl files are always openable
        if self.branch_for_ctl_ino(ino).is_some() {
            reply.opened(0, 0);
            return;
        }

        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.classify_path(&path) {
            PathContext::BranchDir(_) => {
                reply.opened(0, 0);
            }
            PathContext::BranchCtl(_) => {
                reply.opened(0, 0);
            }
            PathContext::BranchPath(branch, rel_path) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                if self.resolve_for_branch(&branch, &rel_path).is_some() {
                    self.manager.register_opened_inode(&branch, ino);
                    reply.opened(0, 0);
                } else {
                    reply.error(libc::ENOENT);
                }
            }
            _ => {
                // Root path
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }
                if self.resolve(&path).is_some() {
                    self.manager
                        .register_opened_inode(&self.get_branch_name(), ino);
                    reply.opened(0, 0);
                } else {
                    reply.error(libc::ENOENT);
                }
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Handle root ctl file (virtual — not in inode table)
        if ino == CTL_INO {
            reply.attr(&TTL, &self.ctl_file_attr(CTL_INO));
            return;
        }

        // Handle per-branch ctl files (virtual — not in inode table)
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            if self.manager.is_branch_valid(&branch) {
                reply.attr(&TTL, &self.ctl_file_attr(ino));
            } else {
                reply.error(libc::ENOENT);
            }
            return;
        }

        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match self.classify_path(&path) {
            PathContext::BranchDir(_) | PathContext::BranchCtl(_) => {
                reply.error(libc::EPERM);
            }
            PathContext::BranchPath(branch, rel_path) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                if let Some(new_size) = size {
                    if let Ok(delta) = self.ensure_cow_for_branch(&branch, &rel_path) {
                        let file = std::fs::OpenOptions::new().write(true).open(&delta);
                        if let Ok(f) = file {
                            let _ = f.set_len(new_size);
                        }
                    }
                }
                if let Some(resolved) = self.resolve_for_branch(&branch, &rel_path) {
                    if let Some(attr) = self.make_attr(ino, &resolved) {
                        reply.attr(&TTL, &attr);
                        return;
                    }
                }
                reply.error(libc::ENOENT);
            }
            _ => {
                // Root path (existing logic)
                if let Some(new_size) = size {
                    if let Ok(delta) = self.ensure_cow(&path) {
                        let file = std::fs::OpenOptions::new().write(true).open(&delta);
                        if let Ok(f) = file {
                            let _ = f.set_len(new_size);
                        }
                    }
                }

                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                if let Some(resolved) = self.resolve(&path) {
                    if let Some(attr) = self.make_attr(ino, &resolved) {
                        reply.attr(&TTL, &attr);
                        return;
                    }
                }

                reply.error(libc::ENOENT);
            }
        }
    }

    fn ioctl(
        &mut self,
        _req: &Request,
        _ino: u64,
        _fh: u64,
        _flags: u32,
        cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        let branch_name = self.get_branch_name();
        match cmd {
            BRANCHFS_IOC_COMMIT => {
                log::info!("ioctl: COMMIT for branch '{}'", branch_name);
                match self.manager.commit(&branch_name) {
                    Ok(()) => {
                        self.switch_to_branch("main");
                        log::info!("Switched to main branch after commit");
                        reply.ioctl(0, &[])
                    }
                    Err(e) => {
                        log::error!("commit failed: {}", e);
                        reply.error(libc::EIO);
                    }
                }
            }
            BRANCHFS_IOC_ABORT => {
                log::info!("ioctl: ABORT for branch '{}'", branch_name);
                match self.manager.abort(&branch_name) {
                    Ok(()) => {
                        self.switch_to_branch("main");
                        log::info!("Switched to main branch after abort");
                        reply.ioctl(0, &[])
                    }
                    Err(e) => {
                        log::error!("abort failed: {}", e);
                        reply.error(libc::EIO);
                    }
                }
            }
            _ => {
                log::warn!("ioctl: unknown command {}", cmd);
                reply.error(libc::ENOTTY);
            }
        }
    }
}
