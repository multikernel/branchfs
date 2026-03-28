use fuser::{KernelConfig, MountOption, ReplyEmpty, ReplyOpen};
use std::fs::File;

pub const FS_IOC_BRANCH_CREATE: u32 = 0x4080_6200; // _IOR('b', 0, [u8; 128])
pub const FS_IOC_BRANCH_COMMIT: u32 = 0x2000_6201; // _IO ('b', 1)
pub const FS_IOC_BRANCH_ABORT: u32 = 0x2000_6202; // _IO ('b', 2)

pub fn get_mount_options() -> Vec<MountOption> {
    vec![
        MountOption::CUSTOM("noappledouble".to_string()),
        MountOption::CUSTOM("volname=branchfs".to_string()),
        MountOption::CUSTOM("defer_permissions".to_string()),
        MountOption::CUSTOM("local".to_string()),
    ]
}

pub fn setup_capabilities(_config: &mut KernelConfig, passthrough_enabled: &mut bool) {
    if *passthrough_enabled {
        log::warn!("FUSE passthrough is only supported on Linux, disabling");
        *passthrough_enabled = false;
    }
}

pub fn handle_access(reply: ReplyEmpty) {
    // macOS macFUSE sometimes requires access() to be implemented when DefaultPermissions
    // is not used, otherwise it may return EPERM. We trust the open() and other calls
    // to handle specific permissions.
    reply.ok();
}

pub fn check_rename_flags(_flags: u32) -> Result<(), i32> {
    // macOS does not use Linux rename flags like RENAME_EXCHANGE
    Ok(())
}

pub fn check_rename_noreplace(_flags: u32) -> bool {
    // RENAME_NOREPLACE is Linux-only, macOS ignores this flag
    false
}

pub fn ctl_file_size(_ino: u64) -> u64 {
    // We set size to 0 on macOS because if we report >0, the kernel performs
    // a Read-Modify-Write cycle on writes to the control file.
    0
}

pub struct PassthroughState {}

impl PassthroughState {
    pub fn new() -> Self {
        Self {}
    }
}

pub fn try_open_passthrough(
    _state: &mut PassthroughState,
    _file: File,
    reply: ReplyOpen,
) {
    // Fallback if we accidentally call this on non-Linux
    reply.opened(0, 0);
}

pub fn release_passthrough(_state: &mut PassthroughState, _fh: u64) {}
