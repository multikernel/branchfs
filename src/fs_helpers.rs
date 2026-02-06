use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::time::UNIX_EPOCH;

use fuser::{FileAttr, FileType};

use crate::fs::{BranchFs, BLOCK_SIZE};
use crate::storage;

impl BranchFs {
    pub(crate) fn resolve(&self, path: &str) -> Option<std::path::PathBuf> {
        self.manager
            .resolve_path(&self.get_branch_name(), path)
            .ok()?
    }

    /// Resolve a path within a specific branch (not the root's current branch).
    pub(crate) fn resolve_for_branch(
        &self,
        branch: &str,
        path: &str,
    ) -> Option<std::path::PathBuf> {
        self.manager.resolve_path(branch, path).ok()?
    }

    pub(crate) fn get_delta_path(&self, rel_path: &str) -> std::path::PathBuf {
        self.manager
            .with_branch(&self.get_branch_name(), |b| Ok(b.delta_path(rel_path)))
            .unwrap()
    }

    pub(crate) fn get_delta_path_for_branch(
        &self,
        branch: &str,
        rel_path: &str,
    ) -> std::path::PathBuf {
        self.manager
            .with_branch(branch, |b| Ok(b.delta_path(rel_path)))
            .unwrap()
    }

    pub(crate) fn ensure_cow(&self, rel_path: &str) -> std::io::Result<std::path::PathBuf> {
        self.ensure_cow_for_branch(&self.get_branch_name(), rel_path)
    }

    pub(crate) fn ensure_cow_for_branch(
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

    pub(crate) fn make_attr(&self, ino: u64, path: &Path) -> Option<FileAttr> {
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

    /// Return a synthetic directory FileAttr.
    pub(crate) fn synthetic_dir_attr(&self, ino: u64) -> FileAttr {
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
            uid: self.uid.load(std::sync::atomic::Ordering::Relaxed),
            gid: self.gid.load(std::sync::atomic::Ordering::Relaxed),
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    /// Return a synthetic ctl-file FileAttr.
    pub(crate) fn ctl_file_attr(&self, ino: u64) -> FileAttr {
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
            uid: self.uid.load(std::sync::atomic::Ordering::Relaxed),
            gid: self.gid.load(std::sync::atomic::Ordering::Relaxed),
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    /// Collect readdir entries for a directory resolved via a specific branch.
    ///
    /// `inode_prefix` controls how child inode paths are formed:
    /// - `"/@branch"` for branch subtrees (produces `/@branch/child`)
    /// - `""` for root-level paths (produces `/child`)
    pub(crate) fn collect_readdir_entries(
        &self,
        branch: &str,
        rel_path: &str,
        ino: u64,
        inode_prefix: &str,
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
                    let inode_path = format!("{}{}", inode_prefix, child_rel);
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
                            let inode_path = format!("{}{}", inode_prefix, child_rel);
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
