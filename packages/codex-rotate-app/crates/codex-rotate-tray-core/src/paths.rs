use std::path::PathBuf;

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct TrayPaths {
    pub codex_auth_file: PathBuf,
    pub codex_logs_db_file: PathBuf,
    pub rotate_app_home: PathBuf,
    pub debug_profile_dir: PathBuf,
    pub session_file: PathBuf,
}

pub fn resolve_paths() -> Result<TrayPaths> {
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    let rotate_app_home = home.join(".codex-rotate-app");
    Ok(TrayPaths {
        codex_auth_file: codex_home.join("auth.json"),
        codex_logs_db_file: codex_home.join("logs_1.sqlite"),
        debug_profile_dir: rotate_app_home.join("profile"),
        session_file: rotate_app_home.join("session.json"),
        rotate_app_home,
    })
}
