use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fuser::{BackgroundSession, MountOption};
use nix::unistd::{fork, setsid, ForkResult};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::branch::BranchManager;
use crate::error::Result;
use crate::fs::BranchFs;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Mount {
        branch: String,
        mountpoint: String,
    },
    Unmount {
        mountpoint: String,
    },
    Create {
        name: String,
        parent: String,
        mountpoint: String,
    },
    NotifySwitch {
        mountpoint: String,
        branch: String,
    },
    List {
        mountpoint: String,
    },
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl Response {
    pub fn success() -> Self {
        Self {
            ok: true,
            error: None,
            data: None,
        }
    }

    pub fn success_with_data(data: serde_json::Value) -> Self {
        Self {
            ok: true,
            error: None,
            data: Some(data),
        }
    }

    pub fn error(msg: &str) -> Self {
        Self {
            ok: false,
            error: Some(msg.to_string()),
            data: None,
        }
    }
}

/// Per-mount state including the FUSE session, current branch, and isolated branch manager
pub struct MountInfo {
    session: BackgroundSession,
    current_branch: String,
    manager: Arc<BranchManager>,
    mount_storage: PathBuf,
}

pub struct Daemon {
    base_path: PathBuf,
    storage_path: PathBuf,
    mounts: Mutex<HashMap<PathBuf, MountInfo>>,
    socket_path: PathBuf,
    shutdown: AtomicBool,
}

/// Generate a hash-based directory name for a mountpoint
fn mount_hash(mountpoint: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    mountpoint.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

impl Daemon {
    pub fn new(
        base_path: PathBuf,
        storage_path: PathBuf,
        _workspace_path: PathBuf,
    ) -> Result<Self> {
        let socket_path = storage_path.join("daemon.sock");

        // Clean up orphaned mount directories on startup
        let mounts_dir = storage_path.join("mounts");
        if mounts_dir.exists() {
            if let Err(e) = fs::remove_dir_all(&mounts_dir) {
                log::warn!("Failed to clean up orphaned mounts directory: {}", e);
            }
        }

        // Store base_path for later use (simple file, not state.json)
        let base_file = storage_path.join("base_path");
        fs::create_dir_all(&storage_path)?;
        fs::write(&base_file, base_path.to_string_lossy().as_bytes())?;

        Ok(Self {
            base_path,
            storage_path,
            mounts: Mutex::new(HashMap::new()),
            socket_path,
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }

    pub fn spawn_mount(&self, branch_name: &str, mountpoint: &Path) -> Result<()> {
        // Create mount-specific storage directory
        let mount_storage = self
            .storage_path
            .join("mounts")
            .join(mount_hash(mountpoint));
        fs::create_dir_all(&mount_storage)?;

        // Create a new BranchManager for this mount
        let manager = Arc::new(BranchManager::new(
            mount_storage.clone(),
            self.base_path.clone(),
            mountpoint.to_path_buf(),
        )?);

        let fs = BranchFs::new(manager.clone(), branch_name.to_string());
        let options = vec![MountOption::FSName("branchfs".to_string())];

        log::info!(
            "Spawning mount for branch '{}' at {:?} with storage {:?}",
            branch_name,
            mountpoint,
            mount_storage
        );

        let session =
            fuser::spawn_mount2(fs, mountpoint, &options).map_err(crate::error::BranchError::Io)?;

        // Get the notifier for cache invalidation and register it with the manager
        let notifier = Arc::new(session.notifier());
        manager.register_notifier(branch_name, mountpoint.to_path_buf(), notifier);

        let mount_info = MountInfo {
            session,
            current_branch: branch_name.to_string(),
            manager,
            mount_storage,
        };

        self.mounts
            .lock()
            .insert(mountpoint.to_path_buf(), mount_info);

        Ok(())
    }

    pub fn unmount(&self, mountpoint: &Path) -> Result<()> {
        let (should_shutdown, mount_info) = {
            let mut mounts = self.mounts.lock();
            if let Some(info) = mounts.remove(mountpoint) {
                log::info!("Unmounted {:?}", mountpoint);
                // The BackgroundSession drop will handle FUSE cleanup
                (mounts.is_empty(), Some(info))
            } else {
                return Err(crate::error::BranchError::MountNotFound(format!(
                    "{:?}",
                    mountpoint
                )));
            }
        };

        // Clean up mount storage directory (full cleanup on unmount)
        if let Some(info) = mount_info {
            info.manager
                .unregister_notifier(&info.current_branch, mountpoint);
            // Delete the entire mount storage directory
            if info.mount_storage.exists() {
                if let Err(e) = fs::remove_dir_all(&info.mount_storage) {
                    log::warn!(
                        "Failed to clean up mount storage {:?}: {}",
                        info.mount_storage,
                        e
                    );
                } else {
                    log::info!("Cleaned up mount storage {:?}", info.mount_storage);
                }
            }
        }

        if should_shutdown {
            log::info!("All mounts removed, daemon will exit");
            self.shutdown.store(true, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn mount_count(&self) -> usize {
        self.mounts.lock().len()
    }

    pub fn create_branch(&self, name: &str, parent: &str, mountpoint: &Path) -> Result<()> {
        let mounts = self.mounts.lock();
        let mount_info = mounts
            .get(mountpoint)
            .ok_or_else(|| crate::error::BranchError::MountNotFound(format!("{:?}", mountpoint)))?;
        mount_info.manager.create_branch(name, parent)
    }

    pub fn list_branches(&self, mountpoint: &Path) -> Result<Vec<(String, Option<String>)>> {
        let mounts = self.mounts.lock();
        let mount_info = mounts
            .get(mountpoint)
            .ok_or_else(|| crate::error::BranchError::MountNotFound(format!("{:?}", mountpoint)))?;
        Ok(mount_info.manager.list_branches())
    }

    pub fn get_manager(&self, mountpoint: &Path) -> Option<Arc<BranchManager>> {
        self.mounts
            .lock()
            .get(mountpoint)
            .map(|info| info.manager.clone())
    }

    pub fn run(&self) -> Result<()> {
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        let listener =
            UnixListener::bind(&self.socket_path).map_err(crate::error::BranchError::Io)?;

        listener
            .set_nonblocking(true)
            .map_err(crate::error::BranchError::Io)?;

        log::info!("Daemon listening on {:?}", self.socket_path);

        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                log::info!("Shutdown flag set, exiting");
                break;
            }

            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).ok();
                    if let Err(e) = self.handle_client(stream) {
                        log::error!("Client error: {}", e);
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => {
                    log::error!("Accept error: {}", e);
                }
            }
        }

        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path).ok();
        }

        Ok(())
    }

    fn handle_client(&self, mut stream: UnixStream) -> Result<()> {
        let reader = BufReader::new(stream.try_clone()?);

        for line in reader.lines() {
            let line = line?;
            let request: Request = match serde_json::from_str(&line) {
                Ok(req) => req,
                Err(e) => {
                    let resp = Response::error(&format!("Invalid request: {}", e));
                    writeln!(stream, "{}", serde_json::to_string(&resp)?)?;
                    continue;
                }
            };

            let response = self.handle_request(request);

            if let Request::Shutdown = serde_json::from_str(&line).unwrap_or(Request::Shutdown) {
                writeln!(stream, "{}", serde_json::to_string(&response)?)?;
                std::process::exit(0);
            }

            writeln!(stream, "{}", serde_json::to_string(&response)?)?;
        }

        Ok(())
    }

    fn handle_request(&self, request: Request) -> Response {
        match request {
            Request::Mount { branch, mountpoint } => {
                let path = PathBuf::from(&mountpoint);
                if let Err(e) = fs::create_dir_all(&path) {
                    return Response::error(&format!("Failed to create mountpoint: {}", e));
                }
                match self.spawn_mount(&branch, &path) {
                    Ok(()) => Response::success(),
                    Err(e) => Response::error(&format!("{}", e)),
                }
            }
            Request::Unmount { mountpoint } => {
                let path = PathBuf::from(&mountpoint);
                match self.unmount(&path) {
                    Ok(()) => Response::success(),
                    Err(e) => Response::error(&format!("{}", e)),
                }
            }
            Request::Create {
                name,
                parent,
                mountpoint,
            } => {
                let path = PathBuf::from(&mountpoint);
                match self.create_branch(&name, &parent, &path) {
                    Ok(()) => Response::success(),
                    Err(e) => Response::error(&format!("{}", e)),
                }
            }
            Request::NotifySwitch { mountpoint, branch } => {
                let path = PathBuf::from(&mountpoint);
                let mut mounts = self.mounts.lock();
                if let Some(ref mut info) = mounts.get_mut(&path) {
                    // Unregister old notifier
                    info.manager
                        .unregister_notifier(&info.current_branch, &path);
                    // Update tracked branch
                    let old_branch = std::mem::replace(&mut info.current_branch, branch.clone());
                    // Register notifier for new branch
                    let notifier = Arc::new(info.session.notifier());
                    info.manager
                        .register_notifier(&branch, path.clone(), notifier);
                    log::info!(
                        "Mount {:?} switched from '{}' to '{}'",
                        path,
                        old_branch,
                        branch
                    );
                    Response::success()
                } else {
                    Response::error(&format!("Mount not found: {:?}", path))
                }
            }
            Request::List { mountpoint } => {
                let path = PathBuf::from(&mountpoint);
                match self.list_branches(&path) {
                    Ok(branches) => {
                        let branches: Vec<_> = branches
                            .into_iter()
                            .map(|(name, parent)| {
                                serde_json::json!({
                                    "name": name,
                                    "parent": parent
                                })
                            })
                            .collect();
                        Response::success_with_data(serde_json::json!(branches))
                    }
                    Err(e) => Response::error(&format!("{}", e)),
                }
            }
            Request::Shutdown => {
                log::info!("Shutdown requested");
                Response::success()
            }
        }
    }
}

pub fn send_request(socket_path: &Path, request: &Request) -> std::io::Result<Response> {
    let mut stream = UnixStream::connect(socket_path)?;
    let request_str = serde_json::to_string(request)?;
    writeln!(stream, "{}", request_str)?;
    stream.flush()?;

    let mut reader = BufReader::new(stream);
    let mut response_str = String::new();
    reader.read_line(&mut response_str)?;

    let response: Response = serde_json::from_str(&response_str)?;
    Ok(response)
}

pub fn is_daemon_running(socket_path: &Path) -> bool {
    if !socket_path.exists() {
        return false;
    }
    UnixStream::connect(socket_path).is_ok()
}

pub fn start_daemon_background(base_path: &Path, storage_path: &Path) -> std::io::Result<()> {
    let socket_path = storage_path.join("daemon.sock");
    let base_path = base_path.to_path_buf();
    let storage_path = storage_path.to_path_buf();

    // Fork to create daemon process
    match unsafe { fork() } {
        Ok(ForkResult::Parent { .. }) => {
            // Parent: wait for daemon to be ready
            for _ in 0..50 {
                std::thread::sleep(Duration::from_millis(100));
                if is_daemon_running(&socket_path) {
                    return Ok(());
                }
            }
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Daemon failed to start",
            ))
        }
        Ok(ForkResult::Child) => {
            // Child: become daemon
            // Create new session to detach from terminal
            let _ = setsid();

            // Run the daemon (this blocks until shutdown)
            let daemon = match Daemon::new(base_path.clone(), storage_path, base_path) {
                Ok(d) => d,
                Err(e) => {
                    log::error!("Failed to create daemon: {}", e);
                    std::process::exit(1);
                }
            };

            if let Err(e) = daemon.run() {
                log::error!("Daemon error: {}", e);
                std::process::exit(1);
            }
            std::process::exit(0);
        }
        Err(e) => Err(std::io::Error::other(format!("Fork failed: {}", e))),
    }
}

pub fn ensure_daemon(base_path: Option<&Path>, storage_path: &Path) -> std::io::Result<()> {
    let socket_path = storage_path.join("daemon.sock");

    if is_daemon_running(&socket_path) {
        return Ok(());
    }

    let base_path = match base_path {
        Some(p) => p.to_path_buf(),
        None => {
            // Try to load from base_path file
            let base_file = storage_path.join("base_path");
            if base_file.exists() {
                let content = fs::read_to_string(&base_file)?;
                PathBuf::from(content.trim())
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "No daemon running and --base not specified. Use --base on first mount.",
                ));
            }
        }
    };

    start_daemon_background(&base_path, storage_path)
}
