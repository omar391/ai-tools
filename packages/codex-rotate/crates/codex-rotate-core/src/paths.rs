use std::fs;
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
    pub fast_browser_script: PathBuf,
    pub asset_root: PathBuf,
    pub automation_bridge_entrypoint: PathBuf,
    pub node_bin: String,
}

const LEGACY_ROTATE_HOME_FILE_PATTERNS: &[&str] = &[
    "codex-login-browser-capture-",
    "fast-browser-",
];
const LEGACY_ROTATE_HOME_DIR_PATTERNS: &[&str] = &["codex-login-browser-shim-"];
const LEGACY_ROTATE_HOME_BIN_FILE_PATTERNS: &[&str] = &["codex-login-managed-"];

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
    let default_fast_browser_script = repo_root
        .parent()
        .map(|parent| {
            parent
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("scripts")
                .join("fast-browser.mjs")
        })
        .unwrap_or_else(|| repo_root.join("fast-browser.mjs"));
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
        fast_browser_script: std::env::var_os("CODEX_ROTATE_FAST_BROWSER_SCRIPT")
            .map(PathBuf::from)
            .unwrap_or(default_fast_browser_script),
        asset_root,
        automation_bridge_entrypoint,
        node_bin: resolve_node_binary(),
    })
}

pub fn legacy_credentials_file() -> Result<PathBuf> {
    Ok(resolve_paths()?.rotate_home.join("credentials.json"))
}

pub fn cleanup_legacy_rotate_home_artifacts(root_dir: &Path) -> Result<()> {
    if !root_dir.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(root_dir)
        .with_context(|| format!("Failed to read rotate home {}.", root_dir.display()))?
    {
        let entry = entry
            .with_context(|| format!("Failed to iterate rotate home {}.", root_dir.display()))?;
        let entry_path = entry.path();
        let file_type = entry.file_type().with_context(|| {
            format!("Failed to inspect rotate home entry {}.", entry_path.display())
        })?;
        let entry_name = entry.file_name();
        let entry_name = entry_name.to_string_lossy();

        if file_type.is_file()
            && LEGACY_ROTATE_HOME_FILE_PATTERNS
                .iter()
                .any(|prefix| entry_name.starts_with(prefix))
        {
            fs::remove_file(&entry_path).with_context(|| {
                format!("Failed to remove legacy rotate-home file {}.", entry_path.display())
            })?;
            continue;
        }

        if file_type.is_dir()
            && LEGACY_ROTATE_HOME_DIR_PATTERNS
                .iter()
                .any(|prefix| entry_name.starts_with(prefix))
        {
            fs::remove_dir_all(&entry_path).with_context(|| {
                format!(
                    "Failed to remove legacy rotate-home directory {}.",
                    entry_path.display()
                )
            })?;
            continue;
        }

        if !file_type.is_dir() || entry_name.as_ref() != "bin" {
            continue;
        }

        for bin_entry in fs::read_dir(&entry_path)
            .with_context(|| format!("Failed to read rotate-home bin {}.", entry_path.display()))?
        {
            let bin_entry = bin_entry.with_context(|| {
                format!("Failed to iterate rotate-home bin {}.", entry_path.display())
            })?;
            let bin_entry_path = bin_entry.path();
            let bin_type = bin_entry.file_type().with_context(|| {
                format!(
                    "Failed to inspect rotate-home bin entry {}.",
                    bin_entry_path.display()
                )
            })?;
            if !bin_type.is_file() {
                continue;
            }
            let bin_name = bin_entry.file_name();
            let bin_name = bin_name.to_string_lossy();
            let should_remove = LEGACY_ROTATE_HOME_BIN_FILE_PATTERNS
                .iter()
                .any(|prefix| bin_name.starts_with(prefix))
                || (bin_name.starts_with("codex-login-")
                    && bin_name.len() > "codex-login-".len()
                    && fs::read_to_string(&bin_entry_path)
                        .map(|contents| !contents.contains("internal managed-login"))
                        .unwrap_or(false));
            if should_remove {
                fs::remove_file(&bin_entry_path).with_context(|| {
                    format!(
                        "Failed to remove legacy rotate-home bin entry {}.",
                        bin_entry_path.display()
                    )
                })?;
            }
        }
    }

    Ok(())
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
    use super::{cleanup_legacy_rotate_home_artifacts, resolve_node_binary};
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

    #[test]
    fn cleanup_legacy_rotate_home_artifacts_removes_obsolete_root_and_bin_artifacts() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let root = tempdir.path();
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::write(root.join("accounts.json"), "{}").expect("write accounts");
        fs::write(root.join("codex-login-browser-capture-1.js"), "legacy")
            .expect("write legacy capture");
        fs::write(root.join("fast-browser-1.json"), "").expect("write legacy json");
        fs::create_dir_all(root.join("codex-login-browser-shim-123"))
            .expect("create legacy shim dir");
        fs::write(bin_dir.join("codex-login-managed-dev-1-deadbeef"), "#!/bin/sh")
            .expect("write legacy managed wrapper");
        fs::write(
            bin_dir.join("codex-login-dev-1-deadbeefcafe"),
            "#!/bin/sh\nexec 'codex' \"$@\"\n",
        )
        .expect("write stale wrapper");
        fs::write(
            bin_dir.join("codex-login-dev-1-123456789abc"),
            "#!/bin/sh\nexec '/tmp/codex-rotate' internal managed-login \"$@\"\n",
        )
        .expect("write current wrapper");

        cleanup_legacy_rotate_home_artifacts(root).expect("cleanup legacy artifacts");

        assert!(root.join("accounts.json").exists());
        assert!(bin_dir.join("codex-login-dev-1-123456789abc").exists());
        assert!(!root.join("codex-login-browser-capture-1.js").exists());
        assert!(!root.join("fast-browser-1.json").exists());
        assert!(!root.join("codex-login-browser-shim-123").exists());
        assert!(!bin_dir.join("codex-login-managed-dev-1-deadbeef").exists());
        assert!(!bin_dir.join("codex-login-dev-1-deadbeefcafe").exists());
    }
}
