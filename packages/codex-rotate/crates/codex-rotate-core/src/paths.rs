use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct CorePaths {
    pub repo_root: PathBuf,
    pub codex_home: PathBuf,
    pub rotate_home: PathBuf,
    pub codex_auth_file: PathBuf,
    pub codex_logs_db_file: PathBuf,
    pub pool_file: PathBuf,
    pub watch_state_file: PathBuf,
    pub profile_dir: PathBuf,
    pub daemon_socket: PathBuf,
    pub account_flow_file: PathBuf,
    pub asset_root: PathBuf,
    pub automation_bridge_entrypoint: PathBuf,
    pub node_bin: String,
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
        .join("codex-rotate-account-flow-main.yaml");
    let asset_root = std::env::var_os("CODEX_ROTATE_ASSET_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| repo_root.join("packages").join("codex-rotate"));
    let default_automation_bridge_entrypoint = {
        let dist_entrypoint = asset_root.join("dist").join("automation-bridge.js");
        if dist_entrypoint.exists() {
            dist_entrypoint
        } else {
            asset_root.join("automation-bridge.ts")
        }
    };
    let automation_bridge_entrypoint = std::env::var_os("CODEX_ROTATE_AUTOMATION_BRIDGE")
        .map(PathBuf::from)
        .unwrap_or(default_automation_bridge_entrypoint);
    Ok(CorePaths {
        codex_home: codex_home.clone(),
        rotate_home: rotate_home.clone(),
        codex_auth_file: codex_home.join("auth.json"),
        codex_logs_db_file: codex_home.join("logs_1.sqlite"),
        repo_root: repo_root.clone(),
        pool_file: rotate_home.join("accounts.json"),
        watch_state_file: rotate_home.join("watch-state.json"),
        profile_dir: rotate_home.join("profile"),
        daemon_socket: rotate_home.join("daemon.sock"),
        account_flow_file: std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE")
            .map(PathBuf::from)
            .unwrap_or(default_account_flow_file),
        asset_root,
        automation_bridge_entrypoint,
        node_bin: resolve_node_binary(),
    })
}

pub fn legacy_credentials_file() -> Result<PathBuf> {
    Ok(resolve_paths()?.rotate_home.join("credentials.json"))
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

fn resolve_node_binary() -> String {
    let candidates = [
        std::env::var_os("CODEX_ROTATE_NODE_BIN").map(PathBuf::from),
        std::env::var_os("NODE_BIN").map(PathBuf::from),
        std::env::var_os("NODE").map(PathBuf::from),
        find_binary_in_path("node"),
        Some(PathBuf::from("/opt/homebrew/opt/node@22/bin/node")),
        Some(PathBuf::from("/opt/homebrew/bin/node")),
        Some(PathBuf::from("/usr/local/bin/node")),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.is_file() {
            return candidate.to_string_lossy().into_owned();
        }
    }
    "node".to_string()
}

fn find_binary_in_path(binary_name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join(binary_name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::resolve_node_binary;
    use std::fs;

    #[test]
    fn resolve_node_binary_prefers_override() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let expected = tempdir.path().join("node");
        fs::write(&expected, "").expect("write node override");

        let previous_override = std::env::var_os("CODEX_ROTATE_NODE_BIN");
        let previous_node_bin = std::env::var_os("NODE_BIN");
        let previous_node = std::env::var_os("NODE");
        let previous_path = std::env::var_os("PATH");

        unsafe {
            std::env::set_var("CODEX_ROTATE_NODE_BIN", &expected);
            std::env::remove_var("NODE_BIN");
            std::env::remove_var("NODE");
            std::env::remove_var("PATH");
        }

        let resolved = resolve_node_binary();

        match previous_override {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_NODE_BIN", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_NODE_BIN") },
        }
        match previous_node_bin {
            Some(value) => unsafe { std::env::set_var("NODE_BIN", value) },
            None => unsafe { std::env::remove_var("NODE_BIN") },
        }
        match previous_node {
            Some(value) => unsafe { std::env::set_var("NODE", value) },
            None => unsafe { std::env::remove_var("NODE") },
        }
        match previous_path {
            Some(value) => unsafe { std::env::set_var("PATH", value) },
            None => unsafe { std::env::remove_var("PATH") },
        }

        assert_eq!(resolved, expected.to_string_lossy());
    }
}
