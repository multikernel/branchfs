use std::sync::atomic::Ordering;

use fuser::ReplyWrite;

use crate::error::BranchError;
use crate::fs::BranchFs;

impl BranchFs {
    /// Get or create the ctl inode number for a branch.
    pub(crate) fn get_or_create_branch_ctl_ino(&self, branch: &str) -> u64 {
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
    pub(crate) fn branch_for_ctl_ino(&self, ino: u64) -> Option<String> {
        let map = self.branch_ctl_inodes.read();
        for (name, &i) in map.iter() {
            if i == ino {
                return Some(name.clone());
            }
        }
        None
    }

    /// Handle a write to the root ctl file.
    /// Only supports `switch:<branch_name>` — commit/abort go through
    /// per-branch ctl files (`/@<branch>/.branchfs_ctl`).
    pub(crate) fn handle_root_ctl_write(&mut self, data: &[u8], reply: ReplyWrite) {
        let cmd = String::from_utf8_lossy(data)
            .trim_matches(|c: char| c.is_whitespace() || c == '\0')
            .to_string();
        let cmd_lower = cmd.to_lowercase();
        log::info!("Root ctl command: '{}'", cmd);

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
        } else if cmd_lower == "create" || cmd_lower.starts_with("create:") {
            let name = if cmd_lower.starts_with("create:") {
                cmd[7..].trim().to_string()
            } else {
                format!("branch-{}", uuid::Uuid::new_v4())
            };
            if name.is_empty() {
                reply.error(libc::EINVAL);
                return;
            }
            log::info!("Root ctl: CREATE branch '{}' from current", name);
            let parent = self.get_branch_name();
            match self.manager.create_branch(&name, &parent) {
                Ok(()) => {
                    self.current_epoch
                        .store(self.manager.get_epoch(), Ordering::SeqCst);
                    reply.written(data.len() as u32);
                }
                Err(e) => {
                    log::error!("create branch failed: {}", e);
                    reply.error(libc::EIO);
                }
            }
        } else if cmd_lower == "commit" {
            let branch = self.get_branch_name();
            self.finalize_branch_op(&branch, self.manager.commit(&branch), "commit", data.len() as u32, reply);
        } else if cmd_lower == "abort" {
            let branch = self.get_branch_name();
            self.finalize_branch_op(&branch, self.manager.abort(&branch), "abort", data.len() as u32, reply);
        } else {
            log::warn!(
                "Unknown root ctl command: '{}' (supported: create, commit, abort, switch:name)",
                cmd
            );
            reply.error(libc::EINVAL);
        }
    }

    /// Handle a write to a per-branch ctl file.
    pub(crate) fn handle_branch_ctl_write(&mut self, branch: &str, data: &[u8], reply: ReplyWrite) {
        let cmd = String::from_utf8_lossy(data)
            .trim_matches(|c: char| c.is_whitespace() || c == '\0')
            .to_string();
        let cmd_lower = cmd.to_lowercase();
        log::info!("Branch ctl command: '{}' for branch '{}'", cmd, branch);

        if cmd_lower == "commit" {
            self.finalize_branch_op(branch, self.manager.commit(branch), "commit", data.len() as u32, reply);
        } else if cmd_lower == "abort" {
            self.finalize_branch_op(branch, self.manager.abort(branch), "abort", data.len() as u32, reply);
        } else if cmd_lower == "create" || cmd_lower.starts_with("create:") {
            let name = if cmd_lower.starts_with("create:") {
                cmd[7..].trim().to_string()
            } else {
                format!("branch-{}", uuid::Uuid::new_v4())
            };
            if name.is_empty() {
                reply.error(libc::EINVAL);
                return;
            }
            log::info!("Branch ctl: CREATE branch '{}' from '{}'", name, branch);
            match self.manager.create_branch(&name, branch) {
                Ok(()) => {
                    self.current_epoch
                        .store(self.manager.get_epoch(), Ordering::SeqCst);
                    reply.written(data.len() as u32);
                }
                Err(e) => {
                    log::error!("create branch failed: {}", e);
                    reply.error(libc::EIO);
                }
            }
        } else {
            log::warn!("Unknown branch ctl command: {}", cmd);
            reply.error(libc::EINVAL);
        }
    }

    fn finalize_branch_op(&mut self, branch: &str, result: Result<String, BranchError>, op: &str, written_len: u32, reply: ReplyWrite) {
        match result {
            Ok(parent) => {
                // Clear inodes for the affected branch prefix and update epoch
                self.inodes.clear_prefix(&format!("/@{}", branch));
                self.current_epoch
                    .store(self.manager.get_epoch(), Ordering::SeqCst);
                // Only switch the mount if the operated branch is the current mount branch
                let current = self.get_branch_name();
                if current == branch {
                    self.manager.switch_mount_branch(&self.mountpoint, &parent);
                    log::info!(
                        "Branch ctl {} succeeded for '{}', switched to '{}'",
                        op,
                        branch,
                        parent
                    );
                } else {
                    log::info!(
                        "Branch ctl {} succeeded for '{}' (mount stays on '{}')",
                        op,
                        branch,
                        current
                    );
                }
                reply.written(written_len)
            }
            Err(BranchError::Conflict(_)) => {
                log::warn!("Branch ctl {} conflict for '{}'", op, branch);
                reply.error(libc::ESTALE);
            }
            Err(e) => {
                log::error!("Branch ctl command failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }
}
