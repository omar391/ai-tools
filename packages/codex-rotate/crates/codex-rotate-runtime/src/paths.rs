use std::path::PathBuf;

use anyhow::{Context, Result};
use codex_rotate_core::paths::resolve_paths as resolve_core_paths;

#[derive(Clone, Debug)]
pub struct RuntimePaths {
    pub repo_root: PathBuf,
    pub codex_auth_file: PathBuf,
    pub codex_logs_db_file: PathBuf,
    pub codex_state_db_file: PathBuf,
    pub rotate_home: PathBuf,
    pub watch_state_file: PathBuf,
    pub debug_profile_dir: PathBuf,
    pub daemon_socket: PathBuf,
}

pub fn resolve_paths() -> Result<RuntimePaths> {
    let core = resolve_core_paths()?;
    Ok(RuntimePaths {
        repo_root: core.repo_root,
        codex_auth_file: core.codex_auth_file,
        codex_logs_db_file: core.codex_logs_db_file,
        codex_state_db_file: core.codex_state_db_file,
        rotate_home: core.rotate_home,
        watch_state_file: core.watch_state_file,
        debug_profile_dir: core.profile_dir,
        daemon_socket: core.daemon_socket,
    })
}

pub fn legacy_rotate_app_home() -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(dirs::home_dir)
        .context("Failed to resolve home directory.")?;
    Ok(home.join(".codex-rotate-app"))
}
