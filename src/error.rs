use thiserror::Error;

#[derive(Error, Debug)]
pub enum BranchError {
    #[error("branch not found: {0}")]
    NotFound(String),

    #[error("mountpoint not tracked: {0} (already unmounted or daemon restarted)")]
    MountNotFound(String),

    #[error("branch already exists: {0}")]
    AlreadyExists(String),

    #[error("branch is invalid: {0}")]
    Invalid(String),

    #[error("parent branch not found: {0}")]
    ParentNotFound(String),

    #[error("cannot operate on main branch")]
    CannotOperateOnMain,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("nix error: {0}")]
    Nix(#[from] nix::Error),
}

pub type Result<T> = std::result::Result<T, BranchError>;
