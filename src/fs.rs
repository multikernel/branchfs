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

pub struct BranchFs {
    manager: Arc<BranchManager>,
    inodes: InodeManager,
    branch_name: RwLock<String>,
    current_epoch: AtomicU64,
}

impl BranchFs {
    pub fn new(manager: Arc<BranchManager>, branch_name: String) -> Self {
        let current_epoch = manager.get_epoch();
        Self {
            manager,
            inodes: InodeManager::new(),
            branch_name: RwLock::new(branch_name),
            current_epoch: AtomicU64::new(current_epoch),
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

    fn ensure_cow(&self, rel_path: &str) -> std::io::Result<std::path::PathBuf> {
        let delta = self.get_delta_path(rel_path);

        if !delta.exists() {
            if let Some(src) = self.resolve(rel_path) {
                if src.exists() && src.is_file() {
                    storage::copy_file(&src, &delta)
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                }
            }
        }

        storage::ensure_parent_dirs(&delta).map_err(|e| std::io::Error::other(e.to_string()))?;

        Ok(delta)
    }
}

impl Filesystem for BranchFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();

        // Control file lookup doesn't need branch validity check
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Handle control file lookup in root (before branch validity check)
        if parent_path == "/" && name_str == CTL_FILE {
            let attr = FileAttr {
                ino: CTL_INO,
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
            };
            reply.entry(&TTL, &attr, 0);
            return;
        }

        // Check branch validity for all other lookups
        if self.is_stale() {
            reply.error(libc::ESTALE);
            return;
        }

        let path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
        };

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

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // Handle control file (no branch validity check)
        if ino == CTL_INO {
            let attr = FileAttr {
                ino: CTL_INO,
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
            };
            reply.attr(&TTL, &attr);
            return;
        }

        // Check branch validity
        if ino != ROOT_INO && self.is_stale() {
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
                if ino == ROOT_INO {
                    let attr = FileAttr {
                        ino: ROOT_INO,
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
                    };
                    reply.attr(&TTL, &attr);
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
        // Handle control file writes (no epoch check - control commands are special)
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

        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

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

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
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

        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (ino, FileType::Directory, "..".to_string()),
        ];

        let mut seen = std::collections::HashSet::new();

        // Collect from base directory first
        let base_dir = self.manager.base_path.join(path.trim_start_matches('/'));
        if let Ok(dir) = std::fs::read_dir(&base_dir) {
            for entry in dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if seen.insert(name.clone()) {
                    let child_path = if path == "/" {
                        format!("/{}", name)
                    } else {
                        format!("{}/{}", path, name)
                    };
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

        // Collect from branch deltas
        if let Some(resolved) = self.resolve(&path) {
            if resolved != base_dir {
                if let Ok(dir) = std::fs::read_dir(&resolved) {
                    for entry in dir.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if seen.insert(name.clone()) {
                            let child_path = if path == "/" {
                                format!("/{}", name)
                            } else {
                                format!("{}/{}", path, name)
                            };
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
            }
        }

        if self.is_stale() {
            reply.error(libc::ESTALE);
            return;
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, &name) {
                break;
            }
        }

        reply.ok();
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
        let path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
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

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = name.to_string_lossy();
        let path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
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
        let path = if parent_path == "/" {
            format!("/{}", name_str)
        } else {
            format!("{}/{}", parent_path, name_str)
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

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.unlink(_req, parent, name, reply);
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        // Control file is always openable (no epoch check)
        if ino == CTL_INO {
            reply.opened(0, 0);
            return;
        }

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

        if self.resolve(&path).is_some() {
            // Register this inode for cache invalidation tracking
            self.manager
                .register_opened_inode(&self.get_branch_name(), ino);
            reply.opened(0, 0);
        } else {
            reply.error(libc::ENOENT);
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
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

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
