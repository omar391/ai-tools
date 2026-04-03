use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct CorePaths {
    pub repo_root: PathBuf,
    pub codex_home: PathBuf,
    pub codex_auth_file: PathBuf,
    pub codex_logs_db_file: PathBuf,
    pub rotate_home: PathBuf,
    pub pool_file: PathBuf,
    pub rotate_app_home: PathBuf,
    pub debug_profile_dir: PathBuf,
    pub session_file: PathBuf,
    pub legacy_cli_entrypoint: PathBuf,
    pub automation_bridge_entrypoint: PathBuf,
    pub bun_bin: String,
}

pub fn resolve_paths() -> Result<CorePaths> {
    let repo_root = repo_root()?;
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    let rotate_home = home.join(".codex-rotate");
    let rotate_app_home = home.join(".codex-rotate-app");
    Ok(CorePaths {
        codex_auth_file: codex_home.join("auth.json"),
        codex_logs_db_file: codex_home.join("logs_1.sqlite"),
        repo_root: repo_root.clone(),
        codex_home,
        rotate_home: rotate_home.clone(),
        pool_file: rotate_home.join("accounts.json"),
        rotate_app_home: rotate_app_home.clone(),
        debug_profile_dir: rotate_app_home.join("profile"),
        session_file: rotate_app_home.join("session.json"),
        legacy_cli_entrypoint: repo_root.join("packages").join("codex-rotate").join("index.ts"),
        automation_bridge_entrypoint: repo_root
            .join("packages")
            .join("codex-rotate")
            .join("automation-bridge.ts"),
        bun_bin: std::env::var("BUN_BIN").unwrap_or_else(|_| "bun".to_string()),
    })
}

fn repo_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .context("Failed to resolve repository root.")
}
