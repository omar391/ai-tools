use std::env;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;

use crate::paths::resolve_paths;

const VM_BRIDGE_ROOT_ENV: &str = "CODEX_ROTATE_VM_BRIDGE_ROOT";
const UTMCTL_BIN_ENV: &str = "CODEX_ROTATE_UTMCTL_BIN";
const UTMCTL_BUNDLED_PATH: &str = "/Applications/UTM.app/Contents/MacOS/utmctl";
const GUEST_BRIDGE_PLIST_NAME: &str = "com.codexrotate.guest-bridge.plist";

pub fn bootstrap_vm_base(
    mounted_guest_root: &Path,
    bridge_root_override: Option<&Path>,
) -> Result<String> {
    let app_root = normalize_guest_root(mounted_guest_root)?;
    ensure_utmctl_available()?;

    let bridge_root = resolve_bridge_root(&app_root, bridge_root_override);
    let autostart_dir = app_root.join("Library").join("LaunchAgents");
    let bootstrap_stamp = app_root
        .join("Users")
        .join("Shared")
        .join(".codex-rotate-vm-base-sealed");

    ensure_directory_exists(
        &app_root.join("Applications").join("Codex.app"),
        "Codex Desktop",
    )?;
    let codex_cli_path = app_root.join("usr").join("local").join("bin").join("codex");
    ensure_executable_exists(&codex_cli_path, "Codex CLI")?;
    ensure_directory_exists(
        &app_root.join("Applications").join("Google Chrome.app"),
        "Google Chrome",
    )?;
    let node_path = app_root.join("usr").join("local").join("bin").join("node");
    ensure_executable_exists(&node_path, "Node.js")?;

    fs::create_dir_all(&bridge_root)
        .with_context(|| format!("Failed to create {}.", bridge_root.display()))?;
    fs::create_dir_all(&autostart_dir)
        .with_context(|| format!("Failed to create {}.", autostart_dir.display()))?;

    let source_assets = resolve_paths()?
        .repo_root
        .join("packages")
        .join("codex-rotate");
    if !source_assets.is_dir() {
        return Err(anyhow!(
            "codex-rotate package assets not found at {}.",
            source_assets.display()
        ));
    }
    copy_directory_contents(&source_assets, &bridge_root)?;

    let plist_path = autostart_dir.join(GUEST_BRIDGE_PLIST_NAME);
    let plist_contents = render_guest_bridge_launch_agent(&node_path, &bridge_root);
    fs::write(&plist_path, plist_contents)
        .with_context(|| format!("Failed to write {}.", plist_path.display()))?;

    let stamp = format!(
        "sealed_at={}\nbridge_root={}\nnotes=Install or verify Codex Desktop, Codex CLI, Chrome, Node, and run the codex-rotate guest bridge LaunchAgent.\n",
        Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
        bridge_root.display()
    );
    fs::write(&bootstrap_stamp, stamp)
        .with_context(|| format!("Failed to write {}.", bootstrap_stamp.display()))?;

    Ok(format!(
        "codex-rotate VM base bootstrap prepared:\n  guest root: {}\n  bridge root: {}\n  launch agent: {}\n  seal stamp: {}",
        app_root.display(),
        bridge_root.display(),
        plist_path.display(),
        bootstrap_stamp.display()
    ))
}

fn normalize_guest_root(path: &Path) -> Result<PathBuf> {
    let raw = path.as_os_str().to_string_lossy();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(anyhow!(
            "Usage: codex-rotate internal vm-bootstrap <mounted-guest-root> [--bridge-root <path>]"
        ));
    }
    let without_trailing = trimmed.trim_end_matches('/');
    Ok(PathBuf::from(if without_trailing.is_empty() {
        "/".to_string()
    } else {
        without_trailing.to_string()
    }))
}

fn resolve_bridge_root(app_root: &Path, override_root: Option<&Path>) -> PathBuf {
    if let Some(override_root) = override_root {
        return override_root.to_path_buf();
    }
    if let Some(value) = env::var_os(VM_BRIDGE_ROOT_ENV).map(PathBuf::from) {
        return value;
    }
    app_root
        .join("Users")
        .join("Shared")
        .join("codex-rotate-bridge")
}

fn ensure_utmctl_available() -> Result<()> {
    if resolve_utmctl_binary().is_some() {
        return Ok(());
    }
    if Path::new(UTMCTL_BUNDLED_PATH).is_file() {
        return Err(anyhow!(
            "utmctl found in /Applications/UTM.app, but not in PATH. Please add it to your PATH."
        ));
    }

    eprintln!("utmctl not found. Installing UTM via Homebrew...");
    if !command_in_path("brew") {
        return Err(anyhow!("error: Homebrew is required to auto-download UTM."));
    }
    let status = Command::new("brew")
        .args(["install", "--cask", "utm"])
        .status()
        .context("Failed to run brew install --cask utm.")?;
    if !status.success() {
        return Err(anyhow!(
            "brew install --cask utm failed with status {status}."
        ));
    }
    if resolve_utmctl_binary().is_none() {
        return Err(anyhow!(
            "utmctl is still unavailable after attempting to install UTM."
        ));
    }
    Ok(())
}

fn resolve_utmctl_binary() -> Option<PathBuf> {
    if let Some(path) = env::var_os(UTMCTL_BIN_ENV).map(PathBuf::from) {
        if is_executable_file(&path) {
            return Some(path);
        }
    }
    command_in_path("utmctl").then(|| PathBuf::from("utmctl"))
}

fn command_in_path(binary: &str) -> bool {
    env::var_os("PATH")
        .map(|paths| {
            env::split_paths(&paths).any(|dir| {
                let candidate = if dir.as_os_str().is_empty() {
                    PathBuf::from(binary)
                } else {
                    dir.join(binary)
                };
                is_executable_file(&candidate)
            })
        })
        .unwrap_or(false)
}

fn ensure_directory_exists(path: &Path, label: &str) -> Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    Err(anyhow!("error: {label} not found at {}", path.display()))
}

fn ensure_executable_exists(path: &Path, label: &str) -> Result<()> {
    if is_executable_file(path) {
        return Ok(());
    }
    Err(anyhow!("error: {label} not found at {}", path.display()))
}

fn is_executable_file(path: &Path) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        (metadata.permissions().mode() & 0o111) != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

fn copy_directory_contents(source_dir: &Path, destination_dir: &Path) -> Result<()> {
    for entry in fs::read_dir(source_dir)
        .with_context(|| format!("Failed to read directory {}.", source_dir.display()))?
    {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination_dir.join(entry.file_name());
        copy_path_recursive(&source_path, &destination_path)?;
    }
    Ok(())
}

fn copy_path_recursive(source: &Path, destination: &Path) -> Result<()> {
    let metadata =
        fs::metadata(source).with_context(|| format!("Failed to inspect {}.", source.display()))?;
    if metadata.is_dir() {
        fs::create_dir_all(destination)
            .with_context(|| format!("Failed to create {}.", destination.display()))?;
        for entry in fs::read_dir(source)
            .with_context(|| format!("Failed to read directory {}.", source.display()))?
        {
            let entry = entry?;
            copy_path_recursive(&entry.path(), &destination.join(entry.file_name()))?;
        }
        return Ok(());
    }

    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::copy(source, destination).with_context(|| {
        format!(
            "Failed to copy {} to {}.",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn render_guest_bridge_launch_agent(node_path: &Path, bridge_root: &Path) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>com.codexrotate.guest-bridge</string>
    <key>ProgramArguments</key>
    <array>
      <string>{}</string>
      <string>{}/index.js</string>
      <string>guest-bridge</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/Shared/codex-rotate-bridge/guest-bridge.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/Shared/codex-rotate-bridge/guest-bridge.err.log</string>
  </dict>
</plist>
"#,
        node_path.display(),
        bridge_root.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_mutex;
    use codex_rotate_refresh::FilesystemTracker;
    use tempfile::tempdir;

    fn restore_env(name: &str, value: Option<std::ffi::OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
        }
    }

    #[cfg(unix)]
    fn make_executable(path: &Path) {
        let mut permissions = fs::metadata(path).expect("stat executable").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod executable");
    }

    #[cfg(not(unix))]
    fn make_executable(_path: &Path) {}

    #[test]
    fn bootstrap_vm_base_writes_bridge_assets_launch_agent_and_stamp() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let temp = tempdir().expect("tempdir");
        let repo_root = temp.path().join("repo");
        let guest_root = temp.path().join("guest");
        let fake_utmctl = temp.path().join("fake-utmctl");
        let path_guard = FilesystemTracker::new()
            .expect("create filesystem tracker")
            .leak_guard("vm bootstrap filesystem cleanup");

        fs::create_dir_all(repo_root.join("packages").join("codex-rotate")).expect("repo assets");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("index.js"),
            "console.log('bridge');\n",
        )
        .expect("write bridge asset");

        fs::create_dir_all(guest_root.join("Applications").join("Codex.app"))
            .expect("create Codex app");
        fs::create_dir_all(guest_root.join("Applications").join("Google Chrome.app"))
            .expect("create Chrome app");
        fs::create_dir_all(guest_root.join("usr").join("local").join("bin"))
            .expect("create bin dir");
        let codex_bin = guest_root
            .join("usr")
            .join("local")
            .join("bin")
            .join("codex");
        fs::write(&codex_bin, "#!/bin/sh\nexit 0\n").expect("write codex bin");
        make_executable(&codex_bin);
        let node_bin = guest_root
            .join("usr")
            .join("local")
            .join("bin")
            .join("node");
        fs::write(&node_bin, "#!/bin/sh\nexit 0\n").expect("write node bin");
        make_executable(&node_bin);

        fs::write(&fake_utmctl, "#!/bin/sh\nexit 0\n").expect("write utmctl");
        make_executable(&fake_utmctl);

        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        let previous_utmctl_bin = std::env::var_os(UTMCTL_BIN_ENV);
        let previous_bridge_root = std::env::var_os(VM_BRIDGE_ROOT_ENV);
        unsafe {
            std::env::set_var("CODEX_ROTATE_REPO_ROOT", &repo_root);
            std::env::set_var(UTMCTL_BIN_ENV, &fake_utmctl);
            std::env::remove_var(VM_BRIDGE_ROOT_ENV);
        }

        let output = bootstrap_vm_base(&guest_root, None).expect("bootstrap vm base");
        assert!(output.contains("codex-rotate VM base bootstrap prepared"));

        let bridge_root = guest_root
            .join("Users")
            .join("Shared")
            .join("codex-rotate-bridge");
        assert!(bridge_root.join("index.js").is_file());
        let launch_agent = guest_root
            .join("Library")
            .join("LaunchAgents")
            .join(GUEST_BRIDGE_PLIST_NAME);
        assert!(launch_agent.is_file());
        let launch_agent_contents = fs::read_to_string(&launch_agent).expect("read launch agent");
        assert!(launch_agent_contents.contains("guest-bridge"));
        assert!(launch_agent_contents.contains(&bridge_root.display().to_string()));
        let seal_stamp = guest_root
            .join("Users")
            .join("Shared")
            .join(".codex-rotate-vm-base-sealed");
        assert!(seal_stamp.is_file());
        let seal_contents = fs::read_to_string(&seal_stamp).expect("read seal stamp");
        assert!(seal_contents.contains("sealed_at="));
        assert!(seal_contents.contains("bridge_root="));

        path_guard.record_temp_path(&guest_root, "guest root", false);
        path_guard.record_temp_path(&bridge_root, "bridge root", false);
        path_guard.record_temp_path(&launch_agent, "launch agent", false);
        path_guard.record_temp_path(&seal_stamp, "seal stamp", false);

        drop(temp);
        path_guard
            .assert_clean()
            .expect("vm bootstrap artifacts should be removed");

        restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
        restore_env(UTMCTL_BIN_ENV, previous_utmctl_bin);
        restore_env(VM_BRIDGE_ROOT_ENV, previous_bridge_root);
    }

    #[test]
    fn bootstrap_vm_base_requires_codex_desktop_presence() {
        let _env_guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let temp = tempdir().expect("tempdir");
        let repo_root = temp.path().join("repo");
        let guest_root = temp.path().join("guest");
        let fake_utmctl = temp.path().join("fake-utmctl");

        fs::create_dir_all(repo_root.join("packages").join("codex-rotate")).expect("repo assets");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("index.js"),
            "console.log('bridge');\n",
        )
        .expect("write bridge asset");

        fs::create_dir_all(guest_root.join("Applications").join("Google Chrome.app"))
            .expect("create Chrome app");
        fs::create_dir_all(guest_root.join("usr").join("local").join("bin"))
            .expect("create bin dir");
        let codex_bin = guest_root
            .join("usr")
            .join("local")
            .join("bin")
            .join("codex");
        fs::write(&codex_bin, "#!/bin/sh\nexit 0\n").expect("write codex bin");
        make_executable(&codex_bin);
        let node_bin = guest_root
            .join("usr")
            .join("local")
            .join("bin")
            .join("node");
        fs::write(&node_bin, "#!/bin/sh\nexit 0\n").expect("write node bin");
        make_executable(&node_bin);

        fs::write(&fake_utmctl, "#!/bin/sh\nexit 0\n").expect("write utmctl");
        make_executable(&fake_utmctl);

        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        let previous_utmctl_bin = std::env::var_os(UTMCTL_BIN_ENV);
        unsafe {
            std::env::set_var("CODEX_ROTATE_REPO_ROOT", &repo_root);
            std::env::set_var(UTMCTL_BIN_ENV, &fake_utmctl);
        }

        let error = bootstrap_vm_base(&guest_root, None).expect_err("missing Codex app");
        assert!(error.to_string().contains("Codex Desktop not found"));

        restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
        restore_env(UTMCTL_BIN_ENV, previous_utmctl_bin);
    }
}
