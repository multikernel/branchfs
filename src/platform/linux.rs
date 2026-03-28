use fuser::{KernelConfig, MountOption, ReplyEmpty, ReplyOpen};
use std::collections::HashMap;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use fuser::BackingId;

pub const FS_IOC_BRANCH_CREATE: u32 = 0x8080_6200; // _IOR('b', 0, [u8; 128])
pub const FS_IOC_BRANCH_COMMIT: u32 = 0x0000_6201; // _IO ('b', 1)
pub const FS_IOC_BRANCH_ABORT: u32 = 0x0000_6202; // _IO ('b', 2)

pub fn get_mount_options() -> Vec<MountOption> {
    vec![MountOption::DefaultPermissions]
}

pub fn setup_capabilities(config: &mut KernelConfig, passthrough_enabled: &mut bool) {
    if *passthrough_enabled {
        if let Err(e) = config.add_capabilities(fuser::consts::FUSE_PASSTHROUGH) {
            log::warn!(
                "Kernel does not support FUSE_PASSTHROUGH (unsupported bits: {:#x}), disabling",
                e
            );
            *passthrough_enabled = false;
        } else if let Err(e) = config.set_max_stack_depth(2) {
            log::warn!(
                "Failed to set max_stack_depth (max: {}), disabling passthrough",
                e
            );
            *passthrough_enabled = false;
        } else {
            log::info!("FUSE passthrough enabled");
        }
    }
}

pub fn handle_access(reply: ReplyEmpty) {
    reply.error(libc::ENOSYS);
}

pub fn check_rename_flags(flags: u32) -> Result<(), i32> {
    if flags & libc::RENAME_EXCHANGE != 0 {
        return Err(libc::EINVAL);
    }
    Ok(())
}

pub fn check_rename_noreplace(flags: u32) -> bool {
    flags & libc::RENAME_NOREPLACE != 0
}

pub fn ctl_file_size(ino: u64) -> u64 {
    // On Linux we report 256 for CTL_INO to ensure the kernel issues read() calls.
    if ino == crate::inode::CTL_INO { 256 } else { 0 }
}

pub struct PassthroughState {
    pub next_fh: AtomicU64,
    pub backing_ids: HashMap<u64, BackingId>,
}

impl PassthroughState {
    pub fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
            backing_ids: HashMap::new(),
        }
    }
}

pub fn try_open_passthrough(
    state: &mut PassthroughState,
    file: File,
    reply: ReplyOpen,
) {
    let backing_id = match reply.open_backing(&file) {
        Ok(id) => id,
        Err(_) => {
            reply.opened(0, 0);
            return;
        }
    };

    let fh = state.next_fh.fetch_add(1, Ordering::Relaxed);
    reply.opened_passthrough(fh, 0, &backing_id);
    state.backing_ids.insert(fh, backing_id);
}

pub fn release_passthrough(state: &mut PassthroughState, fh: u64) {
    state.backing_ids.remove(&fh);
}
