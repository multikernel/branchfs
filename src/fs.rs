use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read as IoRead, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyIoctl, ReplyOpen, ReplyStatfs, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;

use crate::branch::BranchManager;
use crate::error::BranchError;
use crate::fs_path::{classify_path, PathContext};
use crate::inode::{InodeManager, ROOT_INO};
use crate::storage;
use crate::platform::{FS_IOC_BRANCH_ABORT, FS_IOC_BRANCH_COMMIT, FS_IOC_BRANCH_CREATE};

// Zero TTL forces the kernel to always revalidate with FUSE, ensuring consistent
// behavior after branch switches. This is important for speculative execution
// where branches can change at any time.
pub(crate) const TTL: Duration = Duration::from_secs(0);
pub(crate) const BLOCK_SIZE: u32 = 512;

pub(crate) const CTL_FILE: &str = ".branchfs_ctl";
pub(crate) const CTL_INO: u64 = u64::MAX - 1;

/// Cached open file descriptor for the most recently read inode.
/// Eliminates per-read resolve_path() (2-3 stat syscalls on non-existent
/// delta paths) and File::open()/close() overhead.  Invalidated on write
/// (COW changes the backing path) and on epoch change (branch switch).
struct OpenFileCache {
    ino: u64,
    epoch: u64,
    file: Option<File>,
}

impl OpenFileCache {
    fn new() -> Self {
        Self {
            ino: 0,
            epoch: 0,
            file: None,
        }
    }

    /// Return a mutable reference to the cached File if it matches.
    fn get(&mut self, ino: u64, epoch: u64) -> Option<&mut File> {
        if self.ino == ino && self.epoch == epoch {
            self.file.as_mut()
        } else {
            None
        }
    }

    /// Replace the cached entry.
    fn insert(&mut self, ino: u64, epoch: u64, file: File) {
        self.ino = ino;
        self.epoch = epoch;
        self.file = Some(file);
    }

    fn invalidate_ino(&mut self, ino: u64) {
        if self.ino == ino {
            self.ino = 0;
            self.file = None;
        }
    }
}

/// Cached open file descriptor for writes (delta files).
/// Same idea as OpenFileCache but opened in write mode.
struct WriteFileCache {
    ino: u64,
    epoch: u64,
    file: Option<File>,
}

impl WriteFileCache {
    fn new() -> Self {
        Self {
            ino: 0,
            epoch: 0,
            file: None,
        }
    }

    fn get(&mut self, ino: u64, epoch: u64) -> Option<&mut File> {
        if self.ino == ino && self.epoch == epoch {
            self.file.as_mut()
        } else {
            None
        }
    }

    fn insert(&mut self, ino: u64, epoch: u64, file: File) {
        self.ino = ino;
        self.epoch = epoch;
        self.file = Some(file);
    }

    fn invalidate_ino(&mut self, ino: u64) {
        if self.ino == ino {
            self.ino = 0;
            self.file = None;
        }
    }
}

pub struct BranchFs {
    pub(crate) manager: Arc<BranchManager>,
    pub(crate) inodes: InodeManager,
    pub(crate) mountpoint: PathBuf,
    pub(crate) current_epoch: AtomicU64,
    /// Per-branch ctl inode numbers: branch_name → ino
    pub(crate) branch_ctl_inodes: RwLock<HashMap<String, u64>>,
    pub(crate) next_ctl_ino: AtomicU64,
    pub(crate) uid: AtomicU32,
    pub(crate) gid: AtomicU32,
    /// Cached open file — avoids re-resolve + re-open on consecutive reads
    /// to the same inode.
    open_cache: OpenFileCache,
    /// Cached write fd — avoids re-open on consecutive writes to the same
    /// delta file (after COW).
    write_cache: WriteFileCache,
    /// Whether FUSE passthrough mode is enabled (--passthrough flag).
    passthrough_enabled: bool,
    /// FUSE passthrough state (fh counter, backing_ids)
    passthrough_state: crate::platform::PassthroughState,
}

impl BranchFs {
    pub fn new(manager: Arc<BranchManager>, mountpoint: PathBuf, passthrough: bool) -> Self {
        let current_epoch = manager.get_epoch();
        Self {
            manager,
            inodes: InodeManager::new(),
            mountpoint,
            current_epoch: AtomicU64::new(current_epoch),
            branch_ctl_inodes: RwLock::new(HashMap::new()),
            // Reserve a range well below CTL_INO (u64::MAX - 1) for branch ctl inodes.
            // Start from u64::MAX - 1_000_000 downward.
            next_ctl_ino: AtomicU64::new(u64::MAX - 1_000_000),
            uid: AtomicU32::new(nix::unistd::getuid().as_raw()),
            gid: AtomicU32::new(nix::unistd::getgid().as_raw()),
            open_cache: OpenFileCache::new(),
            write_cache: WriteFileCache::new(),
            passthrough_enabled: passthrough,
            passthrough_state: crate::platform::PassthroughState::new(),
        }
    }

    pub(crate) fn get_branch_name(&self) -> String {
        self.manager
            .get_mount_branch(&self.mountpoint)
            .unwrap_or_else(|| "main".into())
    }

    pub(crate) fn is_stale(&self) -> bool {
        let branch_name = self.get_branch_name();
        self.manager.get_epoch() != self.current_epoch.load(Ordering::SeqCst)
            || !self.manager.is_branch_valid(&branch_name)
    }

    /// Switch to a different branch (used after commit/abort to switch to parent)
    pub(crate) fn switch_to_branch(&self, new_branch: &str) {
        self.manager
            .switch_mount_branch(&self.mountpoint, new_branch);
        self.current_epoch
            .store(self.manager.get_epoch(), Ordering::SeqCst);
        // Clear inode cache since we're on a different branch now
        self.inodes.clear();
        // Note: read_cache is invalidated automatically via epoch mismatch
    }

    fn apply_setattr(
        delta: &Path,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
    ) {
        use std::os::unix::fs::PermissionsExt;

        if let Some(m) = mode {
            let perm = std::fs::Permissions::from_mode(m);
            let _ = std::fs::set_permissions(delta, perm);
        }
        if uid.is_some() || gid.is_some() {
            let _ = nix::unistd::chown(
                delta,
                uid.map(nix::unistd::Uid::from_raw),
                gid.map(nix::unistd::Gid::from_raw),
            );
        }
        if atime.is_some() || mtime.is_some() {
            let to_timespec = |t: Option<TimeOrNow>| -> nix::sys::time::TimeSpec {
                match t {
                    Some(TimeOrNow::SpecificTime(st)) => {
                        let d = st.duration_since(UNIX_EPOCH).unwrap_or_default();
                        nix::sys::time::TimeSpec::new(d.as_secs() as i64, d.subsec_nanos() as i64)
                    }
                    Some(TimeOrNow::Now) => nix::sys::time::TimeSpec::new(0, libc::UTIME_NOW),
                    None => nix::sys::time::TimeSpec::new(0, libc::UTIME_OMIT),
                }
            };
            let _ = nix::sys::stat::utimensat(
                None,
                delta,
                &to_timespec(atime),
                &to_timespec(mtime),
                nix::sys::stat::UtimensatFlags::FollowSymlink,
            );
        }
    }

    /// Attempt to open a file with FUSE passthrough. Falls back to non-passthrough on failure.
    fn try_open_passthrough(
        &mut self,
        flags: i32,
        branch: &str,
        rel_path: &str,
        resolved: &std::path::Path,
        reply: ReplyOpen,
    ) {
        let is_writable = (flags & libc::O_ACCMODE) != libc::O_RDONLY;

        let backing_path = if is_writable {
            match self.ensure_cow_for_branch(branch, rel_path) {
                Ok(p) => p,
                Err(_) => {
                    reply.opened(0, 0);
                    return;
                }
            }
        } else {
            resolved.to_path_buf()
        };

        let open_result = if is_writable {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&backing_path)
        } else {
            File::open(&backing_path)
        };
        
        match open_result {
            Ok(f) => crate::platform::try_open_passthrough(&mut self.passthrough_state, f, reply),
            Err(_) => reply.opened(0, 0),
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
        Some(classify_path(&path))
    }
}

impl Filesystem for BranchFs {
    fn init(&mut self, req: &Request, config: &mut fuser::KernelConfig) -> Result<(), libc::c_int> {
        // The init request may come from the kernel (uid=0) rather than the
        // mounting user, so only override the process-derived defaults when
        // the request carries a real (non-root) uid.
        if req.uid() != 0 {
            self.uid.store(req.uid(), Ordering::Relaxed);
            self.gid.store(req.gid(), Ordering::Relaxed);
        }

        if self.passthrough_enabled {
            crate::platform::setup_capabilities(config, &mut self.passthrough_enabled);
        }

        Ok(())
    }

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
            let is_dir = resolved
                .symlink_metadata()
                .map(|m| m.is_dir())
                .unwrap_or(false);
            let ino = self.inodes.get_or_create(&path, is_dir);
            match self.make_attr(ino, &resolved) {
                Some(attr) => reply.entry(&TTL, &attr, 0),
                None => reply.error(libc::ENOENT),
            }
            return;
        }

        // === Parent is inside an @branch subtree ===
        let branch_ctx = match classify_path(&parent_path) {
            PathContext::BranchDir(b) => Some((b, "/".to_string())),
            PathContext::BranchPath(b, rel) => Some((b, rel)),
            _ => None,
        };

        if let Some((branch, parent_rel)) = branch_ctx {
            // Looking up .branchfs_ctl inside a branch dir (only at branch root)
            if parent_rel == "/" && name_str == CTL_FILE {
                let ctl_ino = self.get_or_create_branch_ctl_ino(&branch);
                reply.entry(&TTL, &self.ctl_file_attr(ctl_ino), 0);
                return;
            }

            // @-prefixed names inside branch dirs are not valid (flat namespace)
            if name_str.starts_with('@') {
                reply.error(libc::ENOENT);
                return;
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
            let is_dir = resolved
                .symlink_metadata()
                .map(|m| m.is_dir())
                .unwrap_or(false);
            let ino = self.inodes.get_or_create(&inode_path, is_dir);
            match self.make_attr(ino, &resolved) {
                Some(attr) => reply.entry(&TTL, &attr, 0),
                None => reply.error(libc::ENOENT),
            }
        } else {
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
            let is_dir = resolved
                .symlink_metadata()
                .map(|m| m.is_dir())
                .unwrap_or(false);
            let ino = self.inodes.get_or_create(&path, is_dir);
            match self.make_attr(ino, &resolved) {
                Some(attr) => reply.entry(&TTL, &attr, 0),
                None => reply.error(libc::ENOENT),
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

        match classify_path(&path) {
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
        // Reading root ctl file returns the current branch name
        if ino == CTL_INO {
            let branch = self.get_branch_name();
            let bytes = branch.as_bytes();
            let off = offset as usize;
            if off >= bytes.len() {
                reply.data(&[]);
            } else {
                let end = (off + size as usize).min(bytes.len());
                reply.data(&bytes[off..end]);
            }
            return;
        }

        let epoch = self.current_epoch.load(Ordering::SeqCst);

        // Fast path: reuse cached fd for the same inode+epoch (avoids
        // resolve_path's stat() calls and File::open()/close() every time).
        if let Some(file) = self.open_cache.get(ino, epoch) {
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                reply.error(libc::EIO);
                return;
            }
            let mut buf = vec![0u8; size as usize];
            match file.read(&mut buf) {
                Ok(n) => reply.data(&buf[..n]),
                Err(_) => reply.error(libc::EIO),
            }
            return;
        }

        // Slow path: resolve + open, then cache the fd.
        let is_root = match self.classify_ino(ino) {
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
                match File::open(&resolved) {
                    Ok(file) => {
                        self.open_cache.insert(ino, epoch, file);
                        false
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                }
            }
            Some(PathContext::BranchDir(_)) | Some(PathContext::BranchCtl(_)) => {
                reply.error(libc::EISDIR);
                return;
            }
            _ => {
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
                match File::open(&resolved) {
                    Ok(file) => {
                        self.open_cache.insert(ino, epoch, file);
                        true
                    }
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                }
            }
        };

        // Now serve from the just-cached fd
        if let Some(file) = self.open_cache.get(ino, epoch) {
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                reply.error(libc::EIO);
                return;
            }
            let mut buf = vec![0u8; size as usize];
            match file.read(&mut buf) {
                Ok(n) => {
                    if is_root && self.is_stale() {
                        reply.error(libc::ESTALE);
                        return;
                    }
                    reply.data(&buf[..n])
                }
                Err(_) => reply.error(libc::EIO),
            }
        } else {
            reply.error(libc::EIO);
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
        // Invalidate read cache — COW will redirect to delta, so the cached
        // read fd (pointing to base) becomes wrong.
        self.open_cache.invalidate_ino(ino);

        // === Root ctl file ===
        if ino == CTL_INO {
            self.handle_root_ctl_write(data, reply);
            return;
        }

        // === Per-branch ctl file ===
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            self.handle_branch_ctl_write(&branch, data, reply);
            return;
        }

        let epoch = self.current_epoch.load(Ordering::SeqCst);

        // Fast path: reuse cached write fd for consecutive writes
        // to the same inode (after COW is already done).
        if let Some(file) = self.write_cache.get(ino, epoch) {
            use std::io::{Seek, SeekFrom, Write};
            // Check quota for writes that extend the file
            let old_size = file.metadata().map(|m| m.len()).unwrap_or(0);
            let write_end = offset as u64 + data.len() as u64;
            if write_end > old_size && self.manager.quota.check(write_end - old_size).is_err() {
                reply.error(libc::ENOSPC);
                return;
            }
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                reply.error(libc::EIO);
                return;
            }
            match file.write(data) {
                Ok(n) => {
                    let new_end = offset as u64 + n as u64;
                    if new_end > old_size {
                        self.manager.quota.add(new_end - old_size);
                    }
                    reply.written(n as u32)
                }
                Err(_) => reply.error(libc::EIO),
            }
            return;
        }

        // Slow path: resolve, ensure COW, open delta, cache fd.
        let path = match self.inodes.get_path(ino) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let (delta, is_root) = match classify_path(&path) {
            PathContext::BranchDir(_) | PathContext::BranchCtl(_) => {
                reply.error(libc::EPERM);
                return;
            }
            PathContext::BranchPath(branch, rel_path) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                match self.ensure_cow_for_branch(&branch, &rel_path) {
                    Ok(p) => (p, false),
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                }
            }
            _ => match self.ensure_cow(&path) {
                Ok(p) => (p, true),
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            },
        };

        // Open delta for writing and cache the fd
        match std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&delta)
        {
            Ok(file) => {
                self.write_cache.insert(ino, epoch, file);
            }
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        }

        // Serve from the just-cached write fd
        if let Some(file) = self.write_cache.get(ino, epoch) {
            use std::io::{Seek, SeekFrom, Write};
            let old_size = file.metadata().map(|m| m.len()).unwrap_or(0);
            let write_end = offset as u64 + data.len() as u64;
            if write_end > old_size && self.manager.quota.check(write_end - old_size).is_err() {
                reply.error(libc::ENOSPC);
                return;
            }
            if file.seek(SeekFrom::Start(offset as u64)).is_err() {
                reply.error(libc::EIO);
                return;
            }
            match file.write(data) {
                Ok(n) => {
                    if is_root && self.is_stale() {
                        reply.error(libc::ESTALE);
                        return;
                    }
                    let new_end = offset as u64 + n as u64;
                    if new_end > old_size {
                        self.manager.quota.add(new_end - old_size);
                    }
                    reply.written(n as u32)
                }
                Err(_) => reply.error(libc::EIO),
            }
        } else {
            reply.error(libc::EIO);
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

        match classify_path(&path) {
            PathContext::BranchDir(branch) => {
                // Reading a branch dir root: `.`, `..`, `.branchfs_ctl`, @child dirs, real files
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }

                let inode_prefix = format!("/@{}", branch);
                let mut entries = self.collect_readdir_entries(&branch, "/", ino, &inode_prefix);

                // Add .branchfs_ctl
                let ctl_ino = self.get_or_create_branch_ctl_ino(&branch);
                entries.push((ctl_ino, FileType::RegularFile, CTL_FILE.to_string()));

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

                let inode_prefix = format!("/@{}", branch);
                let entries = self.collect_readdir_entries(&branch, &rel_path, ino, &inode_prefix);

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

                let branch_name = self.get_branch_name();
                let mut entries = self.collect_readdir_entries(&branch_name, "/", ino, "");

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
            PathContext::RootPath(ref rp2) => {
                // Non-root subdir via current branch (existing logic)
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }

                let branch_name = self.get_branch_name();
                let entries = self.collect_readdir_entries(&branch_name, rp2, ino, "");

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
        mode: u32,
        umask: u32,
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

        let branch_ctx = match classify_path(&parent_path) {
            PathContext::BranchDir(b) => Some((b, "/".to_string())),
            PathContext::BranchPath(b, rel) => Some((b, rel)),
            _ => None,
        };

        if let Some((branch, parent_rel)) = branch_ctx {
            // Creating a file inside a branch dir
            if !self.manager.is_branch_valid(&branch) {
                reply.error(libc::ENOENT);
                return;
            }
            let rel_path = if parent_rel == "/" {
                format!("/{}", name_str)
            } else {
                format!("{}/{}", parent_rel, name_str)
            };
            let delta = self.get_delta_path_for_branch(&branch, &rel_path);
            if storage::ensure_parent_dirs(&delta).is_err() {
                reply.error(libc::EIO);
                return;
            }
            match std::fs::File::create(&delta) {
                Ok(_) => {
                    use std::os::unix::fs::PermissionsExt;
                    let perm = std::fs::Permissions::from_mode(mode & !umask);
                    let _ = std::fs::set_permissions(&delta, perm);
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
        } else {
            match classify_path(&parent_path) {
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
                            use std::os::unix::fs::PermissionsExt;
                            let perm = std::fs::Permissions::from_mode(mode & !umask);
                            let _ = std::fs::set_permissions(&delta, perm);
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
                _ => {
                    reply.error(libc::ENOENT);
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

        let branch_ctx = match classify_path(&parent_path) {
            PathContext::BranchDir(b) => Some((b, "/".to_string())),
            PathContext::BranchPath(b, rel) => Some((b, rel)),
            _ => None,
        };

        if let Some((branch, parent_rel)) = branch_ctx {
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

            let result = self.manager.with_branch(&branch, |b| {
                b.add_tombstone(&rel_path)?;
                let delta = b.delta_path(&rel_path);
                if delta.exists() {
                    let freed = delta.symlink_metadata().map(|m| m.len()).unwrap_or(0);
                    std::fs::remove_file(&delta)?;
                    self.manager.quota.sub(freed);
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
        } else {
            // Root-path unlink (or EPERM for ctl files)
            match classify_path(&parent_path) {
                PathContext::BranchCtl(_) | PathContext::RootCtl => {
                    reply.error(libc::EPERM);
                }
                PathContext::RootPath(rp) => {
                    let path = if rp == "/" {
                        format!("/{}", name_str)
                    } else {
                        format!("{}/{}", rp, name_str)
                    };

                    let result = self.manager.with_branch(&self.get_branch_name(), |b| {
                        b.add_tombstone(&path)?;
                        let delta = b.delta_path(&path);
                        if delta.exists() {
                            let freed = delta.symlink_metadata().map(|m| m.len()).unwrap_or(0);
                            std::fs::remove_file(&delta)?;
                            self.manager.quota.sub(freed);
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
                _ => {
                    reply.error(libc::ENOENT);
                }
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        self.unlink(_req, parent, name, reply);
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        if let Err(e) = crate::platform::check_rename_flags(_flags) {
            reply.error(e);
            return;
        }

        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };
        let newparent_path = match self.inodes.get_path(newparent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = name.to_string_lossy();
        let newname_str = newname.to_string_lossy();

        // Normalize both parents to (branch, parent_rel, inode_prefix, is_root_path).
        let resolve_parent = |path: &str| -> Option<(String, String, String, bool)> {
            match classify_path(path) {
                PathContext::BranchDir(b) => {
                    Some((b.clone(), "/".into(), format!("/@{}", b), false))
                }
                PathContext::BranchPath(b, rel) => {
                    Some((b.clone(), rel, format!("/@{}", b), false))
                }
                PathContext::RootPath(rp) => Some((String::new(), rp, String::new(), true)),
                _ => None,
            }
        };

        let (src_branch, src_parent_rel, src_prefix, src_is_root) =
            match resolve_parent(&parent_path) {
                Some(t) => t,
                None => {
                    reply.error(libc::EPERM);
                    return;
                }
            };
        let (dst_branch, dst_parent_rel, dst_prefix, dst_is_root) =
            match resolve_parent(&newparent_path) {
                Some(t) => t,
                None => {
                    reply.error(libc::EPERM);
                    return;
                }
            };

        // Both must be the same kind (both root or both same branch)
        if src_is_root != dst_is_root || (!src_is_root && src_branch != dst_branch) {
            reply.error(libc::EXDEV);
            return;
        }

        let branch = if src_is_root {
            self.get_branch_name()
        } else {
            if !self.manager.is_branch_valid(&src_branch) {
                reply.error(libc::ENOENT);
                return;
            }
            src_branch
        };

        let join_rel = |parent_rel: &str, child: &str| -> String {
            if parent_rel == "/" {
                format!("/{}", child)
            } else {
                format!("{}/{}", parent_rel, child)
            }
        };
        let src_rel = join_rel(&src_parent_rel, &name_str);
        let dst_rel = join_rel(&dst_parent_rel, &newname_str);

        // Check source exists
        if self.resolve_for_branch(&branch, &src_rel).is_none() {
            reply.error(libc::ENOENT);
            return;
        }

        // RENAME_NOREPLACE
        if crate::platform::check_rename_noreplace(_flags)
            && self.resolve_for_branch(&branch, &dst_rel).is_some()
        {
            reply.error(libc::EEXIST);
            return;
        }

        // COW source into delta
        let src_delta = match self.ensure_cow_for_branch(&branch, &src_rel) {
            Ok(p) => p,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };

        let dst_delta = self.get_delta_path_for_branch(&branch, &dst_rel);
        if storage::ensure_parent_dirs(&dst_delta).is_err() {
            reply.error(libc::EIO);
            return;
        }

        // If destination already exists, remove its delta so rename can overwrite
        let dst_existed = self.resolve_for_branch(&branch, &dst_rel).is_some();
        if dst_existed {
            let _ = self.manager.with_branch(&branch, |b| {
                let d = b.delta_path(&dst_rel);
                if d.exists() {
                    if d.is_dir() {
                        let _ = std::fs::remove_dir_all(&d);
                    } else {
                        let _ = std::fs::remove_file(&d);
                    }
                }
                Ok(())
            });
        }

        // Move within the delta layer (same filesystem, always succeeds)
        if std::fs::rename(&src_delta, &dst_delta).is_err() {
            reply.error(libc::EIO);
            return;
        }

        // Update tombstones: mark src deleted, revive dst, tombstone old dst
        let result = self.manager.with_branch(&branch, |b| {
            b.add_tombstone(&src_rel)?;
            if dst_existed {
                b.add_tombstone(&dst_rel)?;
            }
            b.remove_tombstone(&dst_rel);
            Ok(())
        });
        if result.is_err() {
            reply.error(libc::EIO);
            return;
        }

        if src_is_root && self.is_stale() {
            reply.error(libc::ESTALE);
            return;
        }

        // Update inode cache
        let src_inode_path = format!("{}{}", src_prefix, src_rel);
        let dst_inode_path = format!("{}{}", dst_prefix, dst_rel);
        let is_dir = dst_delta.is_dir();
        self.inodes.remove(&src_inode_path);
        self.inodes.remove(&dst_inode_path);
        let new_ino = self.inodes.get_or_create(&dst_inode_path, is_dir);
        self.open_cache.invalidate_ino(new_ino);
        self.write_cache.invalidate_ino(new_ino);

        reply.ok();
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

        match classify_path(&path) {
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
                let _resolved = match self.resolve_for_branch(&branch, &rel_path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                self.manager.register_opened_inode(&branch, ino);

                if self.passthrough_enabled {
                    self.try_open_passthrough(_flags, &branch, &rel_path, &_resolved, reply);
                } else {
                    reply.opened(0, 0);
                }
            }
            _ => {
                // Root path
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }
                let _resolved = match self.resolve(&path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                };
                let branch_name = self.get_branch_name();
                self.manager.register_opened_inode(&branch_name, ino);

                if self.passthrough_enabled {
                    self.try_open_passthrough(_flags, &branch_name, &path, &_resolved, reply);
                } else {
                    reply.opened(0, 0);
                }
            }
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if fh != 0 {
            crate::platform::release_passthrough(&mut self.passthrough_state, fh);
        }
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Truncation triggers COW — invalidate cached fds
        if size.is_some() {
            self.open_cache.invalidate_ino(ino);
            self.write_cache.invalidate_ino(ino);
        }

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

        match classify_path(&path) {
            PathContext::BranchDir(_) | PathContext::BranchCtl(_) => {
                reply.error(libc::EPERM);
            }
            PathContext::BranchPath(branch, rel_path) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                if let Some(new_size) = size {
                    match self.ensure_cow_for_branch(&branch, &rel_path) {
                        Ok(delta) => {
                            let file = std::fs::OpenOptions::new().write(true).open(&delta);
                            if let Ok(f) = file {
                                let old_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                                if new_size > old_size
                                    && self.manager.quota.check(new_size - old_size).is_err()
                                {
                                    reply.error(libc::ENOSPC);
                                    return;
                                }
                                let _ = f.set_len(new_size);
                                if new_size > old_size {
                                    self.manager.quota.add(new_size - old_size);
                                } else if old_size > new_size {
                                    self.manager.quota.sub(old_size - new_size);
                                }
                            }
                        }
                        Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                            reply.error(libc::ENOSPC);
                            return;
                        }
                        Err(_) => {}
                    }
                }
                if mode.is_some()
                    || uid.is_some()
                    || gid.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                {
                    if let Ok(delta) = self.ensure_cow_for_branch(&branch, &rel_path) {
                        Self::apply_setattr(&delta, mode, uid, gid, atime, mtime);
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
                    match self.ensure_cow(&path) {
                        Ok(delta) => {
                            let file = std::fs::OpenOptions::new().write(true).open(&delta);
                            if let Ok(f) = file {
                                let old_size = f.metadata().map(|m| m.len()).unwrap_or(0);
                                if new_size > old_size
                                    && self.manager.quota.check(new_size - old_size).is_err()
                                {
                                    reply.error(libc::ENOSPC);
                                    return;
                                }
                                let _ = f.set_len(new_size);
                                if new_size > old_size {
                                    self.manager.quota.add(new_size - old_size);
                                } else if old_size > new_size {
                                    self.manager.quota.sub(old_size - new_size);
                                }
                            }
                        }
                        Err(e) if e.raw_os_error() == Some(libc::ENOSPC) => {
                            reply.error(libc::ENOSPC);
                            return;
                        }
                        Err(_) => {}
                    }
                }
                if mode.is_some()
                    || uid.is_some()
                    || gid.is_some()
                    || atime.is_some()
                    || mtime.is_some()
                {
                    if let Ok(delta) = self.ensure_cow(&path) {
                        Self::apply_setattr(&delta, mode, uid, gid, atime, mtime);
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
        ino: u64,
        _fh: u64,
        _flags: u32,
        cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        // Resolve ino to the branch name this ctl fd refers to.
        let branch_name = if ino == CTL_INO {
            self.get_branch_name()
        } else if let Some(name) = self.branch_for_ctl_ino(ino) {
            name
        } else {
            reply.error(libc::ENOTTY);
            return;
        };

        match cmd {
            FS_IOC_BRANCH_CREATE => {
                let name = format!("branch-{}", uuid::Uuid::new_v4());
                log::info!("ioctl: CREATE branch '{}' from '{}'", name, branch_name);
                match self.manager.create_branch(&name, &branch_name) {
                    Ok(()) => {
                        self.current_epoch
                            .store(self.manager.get_epoch(), Ordering::SeqCst);
                        log::info!("Created branch '{}' (no mount switch)", name);
                        let mut buf = [0u8; 128];
                        let name_bytes = name.as_bytes();
                        let len = name_bytes.len().min(127);
                        buf[..len].copy_from_slice(&name_bytes[..len]);
                        reply.ioctl(0, &buf)
                    }
                    Err(e) => {
                        log::error!("create branch failed: {}", e);
                        reply.error(libc::EIO);
                    }
                }
            }
            FS_IOC_BRANCH_COMMIT => {
                log::info!("ioctl: COMMIT branch '{}'", branch_name);
                match self.manager.commit(&branch_name) {
                    Ok(_parent) => {
                        self.inodes.clear_prefix(&format!("/@{}", branch_name));
                        self.current_epoch
                            .store(self.manager.get_epoch(), Ordering::SeqCst);
                        log::info!("Committed branch '{}' (no mount switch)", branch_name);
                        reply.ioctl(0, &[])
                    }
                    Err(BranchError::Conflict(_)) => {
                        log::warn!("commit conflict for branch '{}'", branch_name);
                        reply.error(libc::ESTALE);
                    }
                    Err(e) => {
                        log::error!("commit failed: {}", e);
                        reply.error(libc::EIO);
                    }
                }
            }
            FS_IOC_BRANCH_ABORT => {
                log::info!("ioctl: ABORT branch '{}'", branch_name);
                match self.manager.abort(&branch_name) {
                    Ok(_parent) => {
                        self.inodes.clear_prefix(&format!("/@{}", branch_name));
                        self.current_epoch
                            .store(self.manager.get_epoch(), Ordering::SeqCst);
                        log::info!("Aborted branch '{}' (no mount switch)", branch_name);
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

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
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

        let branch_ctx = match classify_path(&parent_path) {
            PathContext::BranchDir(b) => Some((b, "/".to_string())),
            PathContext::BranchPath(b, rel) => Some((b, rel)),
            _ => None,
        };

        if let Some((branch, parent_rel)) = branch_ctx {
            if !self.manager.is_branch_valid(&branch) {
                reply.error(libc::ENOENT);
                return;
            }
            let rel_path = if parent_rel == "/" {
                format!("/{}", name_str)
            } else {
                format!("{}/{}", parent_rel, name_str)
            };
            let delta = self.get_delta_path_for_branch(&branch, &rel_path);
            match std::fs::create_dir_all(&delta) {
                Ok(_) => {
                    use std::os::unix::fs::PermissionsExt;
                    let perm = std::fs::Permissions::from_mode(mode & !umask);
                    let _ = std::fs::set_permissions(&delta, perm);
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
        } else {
            match classify_path(&parent_path) {
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
                            use std::os::unix::fs::PermissionsExt;
                            let perm = std::fs::Permissions::from_mode(mode & !umask);
                            let _ = std::fs::set_permissions(&delta, perm);
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
                _ => {
                    reply.error(libc::ENOENT);
                }
            }
        }
    }

    fn readlink(&mut self, _req: &Request, ino: u64, reply: ReplyData) {
        let resolved = match self.classify_ino(ino) {
            Some(PathContext::BranchPath(branch, rel_path)) => {
                if !self.manager.is_branch_valid(&branch) {
                    reply.error(libc::ENOENT);
                    return;
                }
                match self.resolve_for_branch(&branch, &rel_path) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                }
            }
            Some(PathContext::RootPath(ref rp)) => {
                if self.is_stale() {
                    reply.error(libc::ESTALE);
                    return;
                }
                match self.resolve(rp) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::ENOENT);
                        return;
                    }
                }
            }
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        match std::fs::read_link(&resolved) {
            Ok(target) => reply.data(target.as_os_str().as_encoded_bytes()),
            Err(_) => reply.error(libc::EINVAL),
        }
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: i32, reply: ReplyEmpty) {
        crate::platform::handle_access(reply);
    }

    fn symlink(
        &mut self,
        _req: &Request,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: ReplyEntry,
    ) {
        let parent_path = match self.inodes.get_path(parent) {
            Some(p) => p,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        let name_str = link_name.to_string_lossy();

        let branch_ctx = match classify_path(&parent_path) {
            PathContext::BranchDir(b) => Some((b, "/".to_string())),
            PathContext::BranchPath(b, rel) => Some((b, rel)),
            _ => None,
        };

        if let Some((branch, parent_rel)) = branch_ctx {
            if !self.manager.is_branch_valid(&branch) {
                reply.error(libc::ENOENT);
                return;
            }
            let rel_path = if parent_rel == "/" {
                format!("/{}", name_str)
            } else {
                format!("{}/{}", parent_rel, name_str)
            };
            let delta = self.get_delta_path_for_branch(&branch, &rel_path);
            if storage::ensure_parent_dirs(&delta).is_err() {
                reply.error(libc::EIO);
                return;
            }
            match std::os::unix::fs::symlink(target, &delta) {
                Ok(()) => {
                    let inode_path = format!("/@{}{}", branch, rel_path);
                    let ino = self.inodes.get_or_create(&inode_path, false);
                    match self.make_attr(ino, &delta) {
                        Some(attr) => reply.entry(&TTL, &attr, 0),
                        None => reply.error(libc::EIO),
                    }
                }
                Err(_) => reply.error(libc::EIO),
            }
        } else {
            match classify_path(&parent_path) {
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
                    if storage::ensure_parent_dirs(&delta).is_err() {
                        reply.error(libc::EIO);
                        return;
                    }
                    match std::os::unix::fs::symlink(target, &delta) {
                        Ok(()) => {
                            if self.is_stale() {
                                let _ = std::fs::remove_file(&delta);
                                reply.error(libc::ESTALE);
                                return;
                            }
                            let ino = self.inodes.get_or_create(&path, false);
                            match self.make_attr(ino, &delta) {
                                Some(attr) => reply.entry(&TTL, &attr, 0),
                                None => reply.error(libc::EIO),
                            }
                        }
                        Err(_) => reply.error(libc::EIO),
                    }
                }
                _ => {
                    reply.error(libc::ENOENT);
                }
            }
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        let block_size = 4096u32;
        let quota = &self.manager.quota;

        if let Some(max_bytes) = quota.max() {
            let used = quota.used();
            let total_blocks = max_bytes / block_size as u64;
            let used_blocks = used.min(max_bytes) / block_size as u64;
            let avail_blocks = total_blocks.saturating_sub(used_blocks);

            reply.statfs(
                total_blocks, // total blocks
                avail_blocks, // free blocks
                avail_blocks, // available blocks (to unprivileged users)
                0,            // total inodes (0 = unspecified)
                0,            // free inodes
                block_size,   // block size
                255,          // max name length
                block_size,   // fragment size
            );
        } else {
            // No quota — report from the storage filesystem
            let storage_path = &self.manager.storage_path;
            match nix::sys::statvfs::statvfs(storage_path) {
                Ok(stat) => {
                    reply.statfs(
                        stat.blocks().into(),
                        stat.blocks_free().into(),
                        stat.blocks_available().into(),
                        stat.files().into(),
                        stat.files_free().into(),
                        stat.block_size() as u32,
                        stat.name_max() as u32,
                        stat.fragment_size() as u32,
                    );
                }
                Err(_) => {
                    reply.error(libc::EIO);
                }
            }
        }
    }
}
