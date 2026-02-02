use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fuser::{BackgroundSession, MountOption};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::branch::BranchManager;
use crate::error::Result;
use crate::fs::BranchFs;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum Request {
    Mount { branch: String, mountpoint: String },
    Unmount { mountpoint: String },
    Create { name: String, parent: String },
    NotifySwitch { mountpoint: String, branch: String },
    List,
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
        Self { ok: true, error: None, data: None }
    }

    pub fn success_with_data(data: serde_json::Value) -> Self {
        Self { ok: true, error: None, data: Some(data) }
    }

    pub fn error(msg: &str) -> Self {
        Self { ok: false, error: Some(msg.to_string()), data: None }
    }
}

pub struct Daemon {
    manager: Arc<BranchManager>,
    mounts: Mutex<HashMap<PathBuf, (BackgroundSession, String)>>,
    socket_path: PathBuf,
    shutdown: AtomicBool,
}

impl Daemon {
    pub fn new(base_path: PathBuf, storage_path: PathBuf, workspace_path: PathBuf) -> Result<Self> {
        let manager = Arc::new(BranchManager::new(storage_path.clone(), base_path, workspace_path)?);
        let socket_path = storage_path.join("daemon.sock");

        Ok(Self {
            manager,
            mounts: Mutex::new(HashMap::new()),
            socket_path,
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn socket_path(&self) -> &PathBuf {
        &self.socket_path
    }

    pub fn manager(&self) -> Arc<BranchManager> {
        self.manager.clone()
    }

    pub fn spawn_mount(&self, branch_name: &str, mountpoint: &PathBuf) -> Result<()> {
        let fs = BranchFs::new(self.manager.clone(), branch_name.to_string());
        let options = vec![MountOption::FSName("branchfs".to_string())];

        log::info!("Spawning mount for branch '{}' at {:?}", branch_name, mountpoint);

        let session = fuser::spawn_mount2(fs, mountpoint, &options)
            .map_err(crate::error::BranchError::Io)?;

        // Get the notifier for cache invalidation and register it with the manager
        let notifier = Arc::new(session.notifier());
        self.manager.register_notifier(branch_name, mountpoint.clone(), notifier);

        self.mounts.lock().insert(mountpoint.clone(), (session, branch_name.to_string()));

        Ok(())
    }

    pub fn unmount(&self, mountpoint: &PathBuf) -> Result<()> {
        let (should_shutdown, branch_name) = {
            let mut mounts = self.mounts.lock();
            if let Some((_, branch_name)) = mounts.remove(mountpoint) {
                log::info!("Unmounted {:?}", mountpoint);
                // The BackgroundSession drop will handle FUSE cleanup
                (mounts.is_empty(), Some(branch_name))
            } else {
                return Err(crate::error::BranchError::NotFound(format!("{:?}", mountpoint)));
            }
        };

        // Unregister the notifier and abort the branch if not main
        // (like DAXFS: unmount discards only the current branch, parent chain remains)
        if let Some(ref branch) = branch_name {
            self.manager.unregister_notifier(branch, mountpoint);
            if branch != "main" {
                log::info!("Aborting single branch '{}' on unmount", branch);
                if let Err(e) = self.manager.abort_single(branch) {
                    log::warn!("Failed to abort branch '{}': {}", branch, e);
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

    pub fn create_branch(&self, name: &str, parent: &str) -> Result<()> {
        self.manager.create_branch(name, parent)
    }

    pub fn list_branches(&self) -> Vec<(String, Option<String>)> {
        self.manager.list_branches()
    }

    pub fn run(&self) -> Result<()> {
        if self.socket_path.exists() {
            std::fs::remove_file(&self.socket_path)?;
        }

        let listener = UnixListener::bind(&self.socket_path)
            .map_err(crate::error::BranchError::Io)?;

        listener.set_nonblocking(true)
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

            if let Request::Shutdown = serde_json::from_str(&line).unwrap_or(Request::List) {
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
                if let Err(e) = std::fs::create_dir_all(&path) {
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
            Request::Create { name, parent } => {
                match self.create_branch(&name, &parent) {
                    Ok(()) => Response::success(),
                    Err(e) => Response::error(&format!("{}", e)),
                }
            }
            Request::NotifySwitch { mountpoint, branch } => {
                let path = PathBuf::from(&mountpoint);
                let mut mounts = self.mounts.lock();
                if let Some((session, ref mut current_branch)) = mounts.get_mut(&path) {
                    // Unregister old notifier
                    self.manager.unregister_notifier(current_branch, &path);
                    // Update tracked branch
                    let old_branch = std::mem::replace(current_branch, branch.clone());
                    // Register notifier for new branch
                    let notifier = Arc::new(session.notifier());
                    self.manager.register_notifier(&branch, path.clone(), notifier);
                    log::info!("Mount {:?} switched from '{}' to '{}'", path, old_branch, branch);
                    Response::success()
                } else {
                    Response::error(&format!("Mount not found: {:?}", path))
                }
            }
            Request::List => {
                let branches: Vec<_> = self.list_branches()
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
            Request::Shutdown => {
                log::info!("Shutdown requested");
                Response::success()
            }
        }
    }
}

pub fn send_request(socket_path: &PathBuf, request: &Request) -> std::io::Result<Response> {
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

pub fn is_daemon_running(socket_path: &PathBuf) -> bool {
    if !socket_path.exists() {
        return false;
    }
    UnixStream::connect(socket_path).is_ok()
}

pub fn start_daemon_background(base_path: &Path, storage_path: &Path) -> std::io::Result<()> {
    let exe = std::env::current_exe()?;
    let socket_path = storage_path.join("daemon.sock");

    // Start daemon in background
    Command::new(&exe)
        .args([
            "daemon",
            "--base", base_path.to_str().unwrap(),
            "--storage", storage_path.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    // Wait for daemon to be ready
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

pub fn ensure_daemon(base_path: Option<&PathBuf>, storage_path: &PathBuf) -> std::io::Result<()> {
    let socket_path = storage_path.join("daemon.sock");

    if is_daemon_running(&socket_path) {
        return Ok(());
    }

    let base_path = match base_path {
        Some(p) => p.clone(),
        None => {
            // Try to load from state file
            let state_file = storage_path.join("state.json");
            if state_file.exists() {
                let state = crate::state::State::load(&state_file)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                state.base_path
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
