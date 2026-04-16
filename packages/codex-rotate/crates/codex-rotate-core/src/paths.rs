use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

#[derive(Clone, Debug)]
pub struct CorePaths {
    pub repo_root: PathBuf,
    pub codex_home: PathBuf,
    pub rotate_home: PathBuf,
    pub generated_bin_dir: PathBuf,
    pub codex_auth_file: PathBuf,
    pub codex_logs_db_file: PathBuf,
    pub codex_state_db_file: PathBuf,
    pub pool_file: PathBuf,
    pub lock_dir: PathBuf,
    pub accounts_lock_file: PathBuf,
    pub watch_state_file: PathBuf,
    pub profile_dir: PathBuf,
    pub daemon_socket: PathBuf,
    pub account_flow_file: PathBuf,
    pub fast_browser_script: PathBuf,
    pub asset_root: PathBuf,
    pub automation_bridge_entrypoint: PathBuf,
    pub node_bin: String,
}

const LEGACY_ROTATE_HOME_FILE_PATTERNS: &[&str] =
    &["codex-login-browser-capture-", "fast-browser-"];
const LEGACY_ROTATE_HOME_DIR_PATTERNS: &[&str] = &["codex-login-browser-shim-"];
const DEFAULT_CODEX_LOGS_DB_FILE: &str = "logs_1.sqlite";
const DEFAULT_CODEX_STATE_DB_FILE: &str = "state_5.sqlite";

pub fn resolve_paths() -> Result<CorePaths> {
    let repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT")
        .map(PathBuf::from)
        .unwrap_or(repo_root()?);
    let home = dirs::home_dir().context("Failed to resolve home directory.")?;
    let codex_home = std::env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    let rotate_home = std::env::var_os("CODEX_ROTATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex-rotate"));
    let lock_dir = rotate_home.join("locks");
    let default_account_flow_file = repo_root
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com")
        .join("codex-rotate-account-flow-main.yaml");
    let default_fast_browser_script = resolve_default_fast_browser_script(&repo_root);
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
        generated_bin_dir: repo_root.join(".codex-rotate").join("bin"),
        codex_auth_file: codex_home.join("auth.json"),
        codex_logs_db_file: resolve_codex_logs_db_file(&codex_home),
        codex_state_db_file: resolve_codex_state_db_file(&codex_home),
        repo_root: repo_root.clone(),
        pool_file: rotate_home.join("accounts.json"),
        lock_dir: lock_dir.clone(),
        accounts_lock_file: lock_dir.join("accounts-json.lock"),
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

fn resolve_default_fast_browser_script(repo_root: &Path) -> PathBuf {
    fast_browser_script_candidates(repo_root, resolve_main_worktree_root(repo_root))
        .into_iter()
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| {
            repo_root
                .parent()
                .map(|parent| {
                    parent
                        .join("ai-rules")
                        .join("skills")
                        .join("fast-browser")
                        .join("scripts")
                        .join("fast-browser.mjs")
                })
                .unwrap_or_else(|| repo_root.join("fast-browser.mjs"))
        })
}

fn fast_browser_script_candidates(
    repo_root: &Path,
    main_worktree_root: Option<PathBuf>,
) -> Vec<PathBuf> {
    let mut candidates = vec![fast_browser_script_under(repo_root)];
    if let Some(main_worktree_root) = main_worktree_root {
        if main_worktree_root != repo_root {
            candidates.push(fast_browser_script_under(&main_worktree_root));
        }
    }
    candidates
}

fn fast_browser_script_under(repo_root: &Path) -> PathBuf {
    repo_root
        .parent()
        .map(|parent| {
            parent
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("scripts")
                .join("fast-browser.mjs")
        })
        .unwrap_or_else(|| repo_root.join("fast-browser.mjs"))
}

pub fn resolve_main_worktree_root(repo_root: &Path) -> Option<PathBuf> {
    let output = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("--path-format=absolute")
        .arg("--git-common-dir")
        .current_dir(repo_root)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let common_dir = PathBuf::from(String::from_utf8(output.stdout).ok()?.trim());
    common_dir.parent().map(Path::to_path_buf)
}

pub fn ensure_main_worktree_operation_allowed(repo_root: &Path, operation: &str) -> Result<()> {
    ensure_main_worktree_operation_allowed_with_root(
        repo_root,
        resolve_main_worktree_root(repo_root).as_deref(),
        operation,
    )
}

fn ensure_main_worktree_operation_allowed_with_root(
    repo_root: &Path,
    main_worktree_root: Option<&Path>,
    operation: &str,
) -> Result<()> {
    let Some(main_worktree_root) = main_worktree_root else {
        return Ok(());
    };
    if main_worktree_root == repo_root {
        return Ok(());
    }
    Err(anyhow!(
        "{operation} is disabled from linked worktrees. Run it from the main worktree {}.",
        main_worktree_root.display()
    ))
}

fn resolve_codex_logs_db_file(codex_home: &Path) -> PathBuf {
    resolve_latest_versioned_db_file(
        codex_home,
        "logs_",
        ".sqlite",
        "logs.sqlite",
        DEFAULT_CODEX_LOGS_DB_FILE,
    )
}

fn resolve_codex_state_db_file(codex_home: &Path) -> PathBuf {
    resolve_latest_versioned_db_file(
        codex_home,
        "state_",
        ".sqlite",
        "state.sqlite",
        DEFAULT_CODEX_STATE_DB_FILE,
    )
}

fn resolve_latest_versioned_db_file(
    codex_home: &Path,
    versioned_prefix: &str,
    versioned_suffix: &str,
    unversioned_name: &str,
    default_name: &str,
) -> PathBuf {
    let mut latest_versioned = None::<(u32, PathBuf)>;
    let mut unversioned = None::<PathBuf>;

    if let Ok(entries) = fs::read_dir(codex_home) {
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }

            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == unversioned_name {
                unversioned = Some(path);
                continue;
            }

            let Some(version) = parse_versioned_db_name(&name, versioned_prefix, versioned_suffix)
            else {
                continue;
            };
            match latest_versioned.as_ref() {
                Some((existing, _)) if *existing >= version => {}
                _ => latest_versioned = Some((version, path)),
            }
        }
    }

    latest_versioned
        .map(|(_, path)| path)
        .or(unversioned)
        .unwrap_or_else(|| codex_home.join(default_name))
}

fn parse_versioned_db_name(name: &str, prefix: &str, suffix_marker: &str) -> Option<u32> {
    let suffix = name.strip_prefix(prefix)?.strip_suffix(suffix_marker)?;
    if suffix.is_empty() || !suffix.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    suffix.parse::<u32>().ok()
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
            format!(
                "Failed to inspect rotate home entry {}.",
                entry_path.display()
            )
        })?;
        let entry_name = entry.file_name();
        let entry_name = entry_name.to_string_lossy();

        if file_type.is_file()
            && LEGACY_ROTATE_HOME_FILE_PATTERNS
                .iter()
                .any(|prefix| entry_name.starts_with(prefix))
        {
            fs::remove_file(&entry_path).with_context(|| {
                format!(
                    "Failed to remove legacy rotate-home file {}.",
                    entry_path.display()
                )
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

        if file_type.is_dir() && entry_name.as_ref() == "bin" {
            fs::remove_dir_all(&entry_path).with_context(|| {
                format!(
                    "Failed to remove obsolete rotate-home bin directory {}.",
                    entry_path.display()
                )
            })?;
        }
    }

    Ok(())
}

fn repo_root() -> Result<PathBuf> {
    if let Some(root) = repo_root_from_current_exe() {
        return Ok(root);
    }

    if let Ok(current_dir) = std::env::current_dir() {
        if let Some(root) = git_repo_root(&current_dir) {
            return Ok(root);
        }
    }

    let compiled_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .context("Failed to resolve repository root.")?;

    if let Some(root) = git_repo_root(&compiled_root) {
        return Ok(root);
    }

    Ok(compiled_root)
}

fn repo_root_from_current_exe() -> Option<PathBuf> {
    let current_exe = std::env::current_exe().ok()?;
    repo_root_from_binary_path(&current_exe)
}

fn repo_root_from_binary_path(binary_path: &Path) -> Option<PathBuf> {
    let profile_dir = binary_path.parent()?;
    let target_dir = profile_dir.parent()?;
    let target_name = target_dir.file_name()?.to_str()?;
    if !matches!(target_name, "target" | ".worktree-target") {
        return None;
    }
    let candidate = target_dir.parent()?.canonicalize().ok()?;
    is_valid_repo_root(&candidate).then_some(candidate)
}

fn is_valid_repo_root(candidate: &Path) -> bool {
    candidate.join("Cargo.toml").is_file()
        && candidate
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-cli")
            .join("Cargo.toml")
            .is_file()
}

fn git_repo_root(start_dir: &Path) -> Option<PathBuf> {
    let output = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .current_dir(start_dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8(output.stdout).ok()?;
    let trimmed = root.trim();
    if trimmed.is_empty() {
        return None;
    }
    PathBuf::from(trimmed).canonicalize().ok()
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
    use super::{
        cleanup_legacy_rotate_home_artifacts, ensure_main_worktree_operation_allowed_with_root,
        fast_browser_script_candidates, resolve_codex_logs_db_file, resolve_codex_state_db_file,
        resolve_node_binary, resolve_paths, DEFAULT_CODEX_LOGS_DB_FILE,
        DEFAULT_CODEX_STATE_DB_FILE,
    };
    use crate::test_support::ENV_MUTEX;
    use std::fs;
    use std::path::{Path, PathBuf};

    #[test]
    fn resolve_node_binary_prefers_override() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
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
    fn resolve_paths_prefers_runtime_repo_root_override() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let repo_root = tempdir.path().join("repo-root");
        fs::create_dir_all(repo_root.join("packages").join("codex-rotate"))
            .expect("create asset root");
        fs::create_dir_all(
            repo_root
                .join(".fast-browser")
                .join("workflows")
                .join("web")
                .join("auth.openai.com"),
        )
        .expect("create workflow dir");

        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        unsafe {
            std::env::set_var("CODEX_ROTATE_REPO_ROOT", &repo_root);
        }

        let resolved = resolve_paths().expect("resolve paths");

        match previous_repo_root {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_REPO_ROOT", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_REPO_ROOT") },
        }

        assert_eq!(resolved.repo_root, repo_root);
        assert_eq!(
            resolved.asset_root,
            repo_root.join("packages").join("codex-rotate")
        );
        assert_eq!(
            resolved.generated_bin_dir,
            repo_root.join(".codex-rotate").join("bin")
        );
        assert_eq!(
            resolved.account_flow_file,
            repo_root
                .join(".fast-browser")
                .join("workflows")
                .join("web")
                .join("auth.openai.com")
                .join("codex-rotate-account-flow-main.yaml")
        );
    }

    #[test]
    fn resolve_paths_prefers_current_git_toplevel_for_worktree_like_cwd() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let repo_root = tempdir.path().join("repo-root");
        let nested_dir = repo_root.join("worktrees").join("branch-1");
        fs::create_dir_all(repo_root.join("packages").join("codex-rotate"))
            .expect("create asset root");
        fs::create_dir_all(
            repo_root
                .join(".fast-browser")
                .join("workflows")
                .join("web")
                .join("auth.openai.com"),
        )
        .expect("create workflow dir");
        fs::create_dir_all(&nested_dir).expect("create nested dir");

        let init = std::process::Command::new("git")
            .arg("init")
            .current_dir(&repo_root)
            .output()
            .expect("git init");
        assert!(init.status.success(), "git init failed: {:?}", init);

        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        let previous_cwd = std::env::current_dir().expect("current dir");
        unsafe {
            std::env::remove_var("CODEX_ROTATE_REPO_ROOT");
        }
        std::env::set_current_dir(&nested_dir).expect("set nested cwd");

        let resolved = resolve_paths().expect("resolve paths");

        std::env::set_current_dir(previous_cwd).expect("restore cwd");
        match previous_repo_root {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_REPO_ROOT", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_REPO_ROOT") },
        }

        assert_eq!(
            resolved.repo_root,
            repo_root.canonicalize().expect("canonical repo root")
        );
        assert_eq!(
            resolved.asset_root,
            repo_root
                .canonicalize()
                .expect("canonical repo root")
                .join("packages")
                .join("codex-rotate")
        );
        assert_eq!(
            resolved.generated_bin_dir,
            repo_root
                .canonicalize()
                .expect("canonical repo root")
                .join(".codex-rotate")
                .join("bin")
        );
    }

    #[test]
    fn repo_root_from_binary_path_prefers_worktree_target_layout() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let repo_root = tempdir.path().join("repo-root");
        let binary_path = repo_root
            .join(".worktree-target")
            .join("release")
            .join("codex-rotate");
        fs::create_dir_all(binary_path.parent().expect("binary parent"))
            .expect("create binary dir");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli"),
        )
        .expect("create cli crate dir");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli")
                .join("Cargo.toml"),
            "",
        )
        .expect("write cli cargo");
        fs::write(&binary_path, "").expect("write binary");

        assert_eq!(
            super::repo_root_from_binary_path(&binary_path),
            Some(repo_root.canonicalize().expect("canonical repo root"))
        );
    }

    #[test]
    fn prefers_highest_versioned_codex_logs_db() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("logs_1.sqlite"), "").unwrap();
        fs::write(temp.path().join("logs_2.sqlite"), "").unwrap();
        fs::write(temp.path().join("logs_12.sqlite"), "").unwrap();

        assert_eq!(
            resolve_codex_logs_db_file(temp.path()),
            temp.path().join("logs_12.sqlite")
        );
    }

    #[test]
    fn falls_back_to_unversioned_codex_logs_db() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("logs.sqlite"), "").unwrap();

        assert_eq!(
            resolve_codex_logs_db_file(temp.path()),
            temp.path().join("logs.sqlite")
        );
    }

    #[test]
    fn falls_back_to_default_codex_logs_db_name_when_none_exist() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            resolve_codex_logs_db_file(temp.path()),
            temp.path().join(DEFAULT_CODEX_LOGS_DB_FILE)
        );
    }

    #[test]
    fn prefers_highest_versioned_codex_state_db() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("state_1.sqlite"), "").unwrap();
        fs::write(temp.path().join("state_3.sqlite"), "").unwrap();
        fs::write(temp.path().join("state_5.sqlite"), "").unwrap();

        assert_eq!(
            resolve_codex_state_db_file(temp.path()),
            temp.path().join("state_5.sqlite")
        );
    }

    #[test]
    fn falls_back_to_unversioned_codex_state_db() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(temp.path().join("state.sqlite"), "").unwrap();

        assert_eq!(
            resolve_codex_state_db_file(temp.path()),
            temp.path().join("state.sqlite")
        );
    }

    #[test]
    fn falls_back_to_default_codex_state_db_name_when_none_exist() {
        let temp = tempfile::tempdir().unwrap();

        assert_eq!(
            resolve_codex_state_db_file(temp.path()),
            temp.path().join(DEFAULT_CODEX_STATE_DB_FILE)
        );
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
        fs::write(
            bin_dir.join("codex-login-managed-dev-1-deadbeef"),
            "#!/bin/sh",
        )
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
        assert!(!bin_dir.exists());
        assert!(!root.join("codex-login-browser-capture-1.js").exists());
        assert!(!root.join("fast-browser-1.json").exists());
        assert!(!root.join("codex-login-browser-shim-123").exists());
    }

    #[test]
    fn fast_browser_script_candidates_include_main_worktree_fallback() {
        let repo_root = Path::new("/Users/omar/.codex/worktrees/e7ac/ai-tools");
        let main_root = PathBuf::from("/Volumes/Projects/business/AstronLab/omar391/ai-tools");

        let candidates = fast_browser_script_candidates(repo_root, Some(main_root.clone()));

        assert_eq!(candidates.len(), 2);
        assert_eq!(
            candidates[0],
            PathBuf::from(
                "/Users/omar/.codex/worktrees/e7ac/ai-rules/skills/fast-browser/scripts/fast-browser.mjs"
            )
        );
        assert_eq!(
            candidates[1],
            main_root
                .parent()
                .expect("main root parent")
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("scripts")
                .join("fast-browser.mjs")
        );
    }

    #[test]
    fn resolve_default_fast_browser_script_prefers_existing_main_worktree_fallback() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let workspace_root = tempdir.path();
        let main_root = workspace_root.join("ai-tools");
        let worktree_root = workspace_root
            .join("worktrees")
            .join("e7ac")
            .join("ai-tools");
        let main_script = workspace_root
            .join("ai-rules")
            .join("skills")
            .join("fast-browser")
            .join("scripts")
            .join("fast-browser.mjs");
        fs::create_dir_all(main_script.parent().expect("script parent")).expect("mkdir script");
        fs::create_dir_all(&main_root).expect("mkdir main root");
        fs::create_dir_all(&worktree_root).expect("mkdir worktree root");
        fs::write(&main_script, "export {}").expect("write script");

        let resolved = fast_browser_script_candidates(&worktree_root, Some(main_root))
            .into_iter()
            .find(|candidate| candidate.is_file())
            .expect("existing fallback");

        assert_eq!(resolved, main_script);
    }

    #[test]
    fn ensure_main_worktree_operation_allowed_rejects_linked_worktrees() {
        let error = ensure_main_worktree_operation_allowed_with_root(
            Path::new("/Users/omar/.codex/worktrees/e7ac/ai-tools"),
            Some(Path::new(
                "/Volumes/Projects/business/AstronLab/omar391/ai-tools",
            )),
            "Fresh account creation",
        )
        .expect_err("linked worktree should be rejected");

        assert!(error
            .to_string()
            .contains("Fresh account creation is disabled from linked worktrees."));
        assert!(error
            .to_string()
            .contains("/Volumes/Projects/business/AstronLab/omar391/ai-tools"));
    }
}
