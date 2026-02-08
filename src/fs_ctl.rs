use std::sync::atomic::Ordering;

use fuser::ReplyWrite;

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
    pub(crate) fn handle_root_ctl_write(&mut self, data: &[u8], reply: ReplyWrite) {
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
            Ok(parent) => {
                self.switch_to_branch(&parent);
                log::info!("Switched to branch '{}' after {}", parent, cmd_lower);
                reply.written(data.len() as u32)
            }
            Err(e) => {
                log::error!("Control command failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }

    /// Handle a write to a per-branch ctl file.
    pub(crate) fn handle_branch_ctl_write(&mut self, branch: &str, data: &[u8], reply: ReplyWrite) {
        let cmd = String::from_utf8_lossy(data).trim().to_string();
        let cmd_lower = cmd.to_lowercase();
        log::info!("Branch ctl command: '{}' for branch '{}'", cmd, branch);

        let result = match cmd_lower.as_str() {
            "commit" => self.manager.commit(branch),
            "abort" => self.manager.abort(branch),
            _ => {
                log::warn!("Unknown branch ctl command: {}", cmd);
                reply.error(libc::EINVAL);
                return;
            }
        };

        match result {
            Ok(parent) => {
                // Clear inodes for the affected branch prefix and update epoch
                self.inodes.clear_prefix(&format!("/@{}", branch));
                self.current_epoch
                    .store(self.manager.get_epoch(), Ordering::SeqCst);
                *self.branch_name.write() = parent.clone();
                log::info!(
                    "Branch ctl {} succeeded for '{}', switched to '{}'",
                    cmd_lower,
                    branch,
                    parent
                );
                reply.written(data.len() as u32)
            }
            Err(e) => {
                log::error!("Branch ctl command failed: {}", e);
                reply.error(libc::EIO);
            }
        }
    }
}
