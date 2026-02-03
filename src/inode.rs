use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

pub const ROOT_INO: u64 = 1;

#[derive(Debug, Clone)]
pub struct InodeInfo {
    pub ino: u64,
    pub path: String,
    pub is_dir: bool,
}

pub struct InodeManager {
    next_ino: AtomicU64,
    path_to_ino: RwLock<HashMap<String, u64>>,
    ino_to_info: RwLock<HashMap<u64, InodeInfo>>,
}

impl InodeManager {
    pub fn new() -> Self {
        let mut path_to_ino = HashMap::new();
        let mut ino_to_info = HashMap::new();

        path_to_ino.insert("/".to_string(), ROOT_INO);
        ino_to_info.insert(
            ROOT_INO,
            InodeInfo {
                ino: ROOT_INO,
                path: "/".to_string(),
                is_dir: true,
            },
        );

        Self {
            next_ino: AtomicU64::new(ROOT_INO + 1),
            path_to_ino: RwLock::new(path_to_ino),
            ino_to_info: RwLock::new(ino_to_info),
        }
    }

    pub fn get_or_create(&self, path: &str, is_dir: bool) -> u64 {
        {
            let map = self.path_to_ino.read();
            if let Some(&ino) = map.get(path) {
                return ino;
            }
        }

        let mut path_map = self.path_to_ino.write();
        let mut info_map = self.ino_to_info.write();

        if let Some(&ino) = path_map.get(path) {
            return ino;
        }

        let ino = self.next_ino.fetch_add(1, Ordering::SeqCst);
        path_map.insert(path.to_string(), ino);
        info_map.insert(
            ino,
            InodeInfo {
                ino,
                path: path.to_string(),
                is_dir,
            },
        );

        ino
    }

    pub fn get_path(&self, ino: u64) -> Option<String> {
        self.ino_to_info.read().get(&ino).map(|i| i.path.clone())
    }

    pub fn get_ino(&self, path: &str) -> Option<u64> {
        self.path_to_ino.read().get(path).copied()
    }

    pub fn get_info(&self, ino: u64) -> Option<InodeInfo> {
        self.ino_to_info.read().get(&ino).cloned()
    }

    pub fn remove(&self, path: &str) {
        let mut path_map = self.path_to_ino.write();
        let mut info_map = self.ino_to_info.write();

        if let Some(ino) = path_map.remove(path) {
            info_map.remove(&ino);
        }
    }

    pub fn all_inos(&self) -> Vec<u64> {
        self.ino_to_info.read().keys().copied().collect()
    }

    /// Remove all inodes whose path starts with `prefix`
    pub fn clear_prefix(&self, prefix: &str) {
        let mut path_map = self.path_to_ino.write();
        let mut info_map = self.ino_to_info.write();

        let to_remove: Vec<String> = path_map
            .keys()
            .filter(|p| p.starts_with(prefix))
            .cloned()
            .collect();

        for path in to_remove {
            if let Some(ino) = path_map.remove(&path) {
                info_map.remove(&ino);
            }
        }
    }

    /// Clear all inodes except root (used when switching branches)
    pub fn clear(&self) {
        let mut path_map = self.path_to_ino.write();
        let mut info_map = self.ino_to_info.write();

        path_map.clear();
        info_map.clear();

        // Re-add root
        path_map.insert("/".to_string(), ROOT_INO);
        info_map.insert(
            ROOT_INO,
            InodeInfo {
                ino: ROOT_INO,
                path: "/".to_string(),
                is_dir: true,
            },
        );

        // Note: next_ino is not reset - inode numbers should remain unique
        // across the lifetime of the mount to avoid confusion
    }
}

impl Default for InodeManager {
    fn default() -> Self {
        Self::new()
    }
}
