use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct CorePaths {
    pub repo_root: PathBuf,
    pub codex_auth_file: PathBuf,
    pub pool_file: PathBuf,
    pub credentials_file: PathBuf,
    pub account_flow_file: PathBuf,
    pub automation_bridge_entrypoint: PathBuf,
    pub bun_bin: String,
}

pub fn resolve_paths() -> Result<CorePaths> {
    let repo_root = repo_root()?;
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    let rotate_home = std::env::var_os("CODEX_ROTATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex-rotate"));
    let default_account_flow_file = repo_root
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com")
        .join("codex-rotate-account-flow.yaml");
    Ok(CorePaths {
        codex_auth_file: codex_home.join("auth.json"),
        repo_root: repo_root.clone(),
        pool_file: rotate_home.join("accounts.json"),
        credentials_file: rotate_home.join("credentials.json"),
        account_flow_file: std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE")
            .map(PathBuf::from)
            .unwrap_or(default_account_flow_file),
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
