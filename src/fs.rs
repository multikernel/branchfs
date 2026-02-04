use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyIoctl,
    ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use parking_lot::RwLock;

use crate::branch::BranchManager;
use crate::fs_path::{classify_path, PathContext};
use crate::inode::{InodeManager, ROOT_INO};
use crate::storage;

// Zero TTL forces the kernel to always revalidate with FUSE, ensuring consistent
// behavior after branch switches. This is important for speculative execution
// where branches can change at any time.
pub(crate) const TTL: Duration = Duration::from_secs(0);
pub(crate) const BLOCK_SIZE: u32 = 512;

pub const BRANCHFS_IOC_COMMIT: u32 = 0x4201;
pub const BRANCHFS_IOC_ABORT: u32 = 0x4202;

pub(crate) const CTL_FILE: &str = ".branchfs_ctl";
pub(crate) const CTL_INO: u64 = u64::MAX - 1;

pub struct BranchFs {
    pub(crate) manager: Arc<BranchManager>,
    pub(crate) inodes: InodeManager,
    pub(crate) branch_name: RwLock<String>,
    pub(crate) current_epoch: AtomicU64,
    /// Per-branch ctl inode numbers: branch_name → ino
    pub(crate) branch_ctl_inodes: RwLock<HashMap<String, u64>>,
    pub(crate) next_ctl_ino: AtomicU64,
    pub(crate) uid: AtomicU32,
    pub(crate) gid: AtomicU32,
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
            uid: AtomicU32::new(nix::unistd::getuid().as_raw()),
            gid: AtomicU32::new(nix::unistd::getgid().as_raw()),
        }
    }

    pub(crate) fn get_branch_name(&self) -> String {
        self.branch_name.read().clone()
    }

    pub(crate) fn is_stale(&self) -> bool {
        let branch_name = self.get_branch_name();
        self.manager.get_epoch() != self.current_epoch.load(Ordering::SeqCst)
            || !self.manager.is_branch_valid(&branch_name)
    }

    /// Switch to a different branch (used after commit/abort to switch to main)
    pub(crate) fn switch_to_branch(&self, new_branch: &str) {
        *self.branch_name.write() = new_branch.to_string();
        self.current_epoch
            .store(self.manager.get_epoch(), Ordering::SeqCst);
        // Clear inode cache since we're on a different branch now
        self.inodes.clear();
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
    fn init(
        &mut self,
        req: &Request,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        // The init request may come from the kernel (uid=0) rather than the
        // mounting user, so only override the process-derived defaults when
        // the request carries a real (non-root) uid.
        if req.uid() != 0 {
            self.uid.store(req.uid(), Ordering::Relaxed);
            self.gid.store(req.gid(), Ordering::Relaxed);
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
            let is_dir = resolved.is_dir();
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

            // Looking up @child inside a branch dir (nested branch)
            if let Some(child_branch) = name_str.strip_prefix('@') {
                let children = self.manager.get_children(&branch);
                if parent_rel == "/" && children.iter().any(|c| c == child_branch) {
                    let inode_path = format!("/@{}/@{}", branch, child_branch);
                    let ino = self.inodes.get_or_create(&inode_path, true);
                    reply.entry(&TTL, &self.synthetic_dir_attr(ino), 0);
                } else {
                    reply.error(libc::ENOENT);
                }
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
            let is_dir = resolved.is_dir();
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
            let is_dir = resolved.is_dir();
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
            self.handle_root_ctl_write(data, reply);
            return;
        }

        // === Per-branch ctl file ===
        if let Some(branch) = self.branch_for_ctl_ino(ino) {
            self.handle_branch_ctl_write(&branch, data, reply);
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

        match classify_path(&path) {
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
                match Self::write_to_delta(&delta, offset, data) {
                    Ok(n) => reply.written(n),
                    Err(e) => reply.error(e),
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
                match Self::write_to_delta(&delta, offset, data) {
                    Ok(n) => {
                        if self.is_stale() {
                            reply.error(libc::ESTALE);
                            return;
                        }
                        reply.written(n)
                    }
                    Err(e) => reply.error(e),
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
                _ => {
                    reply.error(libc::ENOENT);
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
                    if let Ok(delta) = self.ensure_cow_for_branch(&branch, &rel_path) {
                        let file = std::fs::OpenOptions::new().write(true).open(&delta);
                        if let Ok(f) = file {
                            let _ = f.set_len(new_size);
                        }
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
                    if let Ok(delta) = self.ensure_cow(&path) {
                        let file = std::fs::OpenOptions::new().write(true).open(&delta);
                        if let Ok(f) = file {
                            let _ = f.set_len(new_size);
                        }
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
}
