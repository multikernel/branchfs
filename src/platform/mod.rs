#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::*;

#[cfg(not(target_os = "linux"))]
mod macos;
#[cfg(not(target_os = "linux"))]
pub use macos::*;
