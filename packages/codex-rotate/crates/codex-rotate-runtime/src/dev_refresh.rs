use std::fs::{self, OpenOptions};
#[cfg(unix)]
use std::os::raw::c_int;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Context, Result};

use crate::paths::resolve_paths;

const FRESHNESS_TOLERANCE: Duration = Duration::from_secs(1);
#[cfg(target_os = "macos")]
pub const MACOS_TRAY_LAUNCHD_LABEL: &str = "com.astronlab.codex-rotate.tray";
#[cfg(target_os = "macos")]
const MACOS_TRAY_LAUNCHD_LABEL_ENV: &str = "CODEX_ROTATE_TRAY_LAUNCHD_LABEL";
const LOCAL_REFRESH_DISABLE_ENV: &str = "CODEX_ROTATE_DISABLE_LOCAL_REFRESH";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildProfile {
    Debug,
    Release,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalCliBuild {
    pub repo_root: PathBuf,
    pub profile: BuildProfile,
    pub cli_binary: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalTrayBuild {
    pub repo_root: PathBuf,
    pub profile: BuildProfile,
    pub tray_binary: PathBuf,
}

pub fn detect_local_cli_build(cli_binary: &Path) -> Option<LocalCliBuild> {
    let (repo_root, profile) = detect_local_build(
        cli_binary,
        &[
            "packages",
            "codex-rotate",
            "crates",
            "codex-rotate-cli",
            "Cargo.toml",
        ],
    )?;
    Some(LocalCliBuild {
        repo_root,
        profile,
        cli_binary: cli_binary.to_path_buf(),
    })
}

pub fn detect_local_tray_build(tray_binary: &Path) -> Option<LocalTrayBuild> {
    let (repo_root, profile) = detect_local_build(
        tray_binary,
        &["packages", "codex-rotate-app", "src-tauri", "Cargo.toml"],
    )?;
    Some(LocalTrayBuild {
        repo_root,
        profile,
        tray_binary: tray_binary.to_path_buf(),
    })
}

pub fn current_process_local_cli_build() -> Option<LocalCliBuild> {
    std::env::current_exe()
        .ok()
        .and_then(|path| detect_local_cli_build(&path))
}

pub fn current_process_local_tray_build() -> Option<LocalTrayBuild> {
    std::env::current_exe()
        .ok()
        .and_then(|path| detect_local_tray_build(&path))
}

pub fn local_cli_sources_newer_than_binary(build: &LocalCliBuild) -> Result<bool> {
    let binary_modified = file_modified_at(&build.cli_binary)?;
    source_paths_newer_than_binary(binary_modified, tracked_cli_source_paths(&build.repo_root))
}

pub fn local_tray_sources_newer_than_binary(build: &LocalTrayBuild) -> Result<bool> {
    let binary_modified = file_modified_at(&build.tray_binary)?;
    source_paths_newer_than_binary(binary_modified, tracked_tray_source_paths(&build.repo_root))
}

pub fn daemon_socket_is_older_than_binary(daemon_socket: &Path, cli_binary: &Path) -> Result<bool> {
    if !daemon_socket.exists() || !cli_binary.exists() {
        return Ok(false);
    }
    let socket_modified = file_modified_at(daemon_socket)?;
    let binary_modified = file_modified_at(cli_binary)?;
    Ok(is_meaningfully_newer(binary_modified, socket_modified))
}

pub fn rebuild_local_cli(build: &LocalCliBuild) -> Result<()> {
    rebuild_local_binary(
        &build.repo_root,
        build.profile,
        build.repo_root.join("Cargo.toml"),
        "codex-rotate-cli",
    )
}

pub fn rebuild_local_tray(build: &LocalTrayBuild) -> Result<()> {
    rebuild_local_binary(
        &build.repo_root,
        build.profile,
        build
            .repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("Cargo.toml"),
        "codex-rotate-tray",
    )
}

pub fn stop_running_daemons(cli_binary: &Path, daemon_socket: &Path) -> Result<()> {
    let mut pids = daemon_pids_from_lsof(daemon_socket).unwrap_or_default();
    for pid in daemon_pids_from_ps(cli_binary)? {
        if !pids.contains(&pid) {
            pids.push(pid);
        }
    }

    for pid in pids {
        stop_process(pid).with_context(|| format!("Failed to stop daemon pid {}.", pid))?;
    }
    Ok(())
}

pub fn stop_other_local_daemons(
    build: &LocalCliBuild,
    daemon_socket: &Path,
    keep_pid: u32,
) -> Result<()> {
    let Some(binary_name) = build.cli_binary.file_name() else {
        return Ok(());
    };
    let debug_binary = build
        .repo_root
        .join("target")
        .join("debug")
        .join(binary_name);
    let release_binary = build
        .repo_root
        .join("target")
        .join("release")
        .join(binary_name);

    let mut pids = daemon_pids_from_lsof(daemon_socket).unwrap_or_default();
    for binary in [&debug_binary, &release_binary] {
        for pid in daemon_pids_from_ps(binary)? {
            if !pids.contains(&pid) {
                pids.push(pid);
            }
        }
    }

    for pid in pids.into_iter().filter(|pid| *pid != keep_pid) {
        stop_process(pid).with_context(|| format!("Failed to stop daemon pid {}.", pid))?;
    }
    Ok(())
}

pub fn stop_running_trays(tray_binary: &Path) -> Result<()> {
    for pid in tray_pids_from_ps(tray_binary)? {
        stop_process(pid).with_context(|| format!("Failed to stop tray pid {}.", pid))?;
    }
    Ok(())
}

pub fn spawn_detached_process(binary: &Path, args: &[&str]) -> Result<()> {
    let mut command = Command::new(binary);
    command.args(args);
    spawn_detached_command(&mut command)
        .with_context(|| format!("Failed to start {}.", binary.display()))?;
    Ok(())
}

pub fn launch_tray_process(tray_binary: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        let plist_path = write_tray_launch_agent_plist(tray_binary)?;
        bootstrap_tray_launch_agent_after_reset(
            &plist_path,
            "Failed to bootstrap Codex Rotate tray launch agent after reset",
        )?;
        kickstart_tray_launch_agent(&label, "Failed to start Codex Rotate tray launch agent.")?;
        return Ok(());
    }

    #[cfg(not(target_os = "macos"))]
    {
        spawn_detached_process(tray_binary, &[])
    }
}

pub fn ensure_tray_process_registered() -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        let plist_path = tray_launch_agent_plist_path()?;
        if !plist_path.is_file() {
            return Ok(false);
        }

        let label = tray_launchd_label();
        if launchctl_service_is_registered(&label)? {
            return Ok(false);
        }

        bootstrap_tray_launch_agent_after_reset(
            &plist_path,
            "Failed to restore Codex Rotate tray launch agent after reset",
        )?;
        kickstart_tray_launch_agent(
            &label,
            "Failed to kickstart restored Codex Rotate tray launch agent.",
        )?;
        return Ok(true);
    }

    #[cfg(not(target_os = "macos"))]
    {
        Ok(false)
    }
}

pub fn maybe_start_background_release_cli_build(build: &LocalCliBuild) -> Result<bool> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    if build.profile != BuildProfile::Debug {
        return Ok(false);
    }
    maybe_start_background_release_build(
        &build.repo_root.join("Cargo.toml"),
        "codex-rotate-cli",
        &build.cli_binary,
        tracked_cli_source_paths(&build.repo_root),
    )
}

pub fn maybe_start_background_release_tray_build(build: &LocalTrayBuild) -> Result<bool> {
    if local_refresh_disabled() {
        return Ok(false);
    }
    if build.profile != BuildProfile::Debug {
        return Ok(false);
    }
    maybe_start_background_release_build(
        &build
            .repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("Cargo.toml"),
        "codex-rotate-tray",
        &build.tray_binary,
        tracked_tray_source_paths(&build.repo_root),
    )
}

pub fn preferred_release_cli_binary(build: &LocalCliBuild) -> Result<Option<PathBuf>> {
    if local_refresh_disabled() {
        return Ok(None);
    }
    if build.profile != BuildProfile::Debug {
        return Ok(None);
    }
    preferred_release_binary(&build.cli_binary, tracked_cli_source_paths(&build.repo_root))
}

pub fn preferred_release_tray_binary(build: &LocalTrayBuild) -> Result<Option<PathBuf>> {
    if local_refresh_disabled() {
        return Ok(None);
    }
    if build.profile != BuildProfile::Debug {
        return Ok(None);
    }
    preferred_release_binary(&build.tray_binary, tracked_tray_source_paths(&build.repo_root))
}

pub fn schedule_tray_relaunch_process(tray_binary: &Path) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        let plist_path = write_tray_launch_agent_plist(tray_binary)?;
        let script = build_tray_launch_agent_reset_script(&plist_path, &label);
        return spawn_detached_process(Path::new("/bin/sh"), &["-c", script.as_str()]);
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let script = format!("sleep 1; exec {}", shell_single_quote(tray_binary));
        return spawn_detached_process(Path::new("/bin/sh"), &["-c", script.as_str()]);
    }

    #[cfg(not(unix))]
    {
        spawn_detached_process(tray_binary, &[])
    }
}

pub fn clear_tray_service_registration() {
    #[cfg(target_os = "macos")]
    {
        let label = tray_launchd_label();
        if let Ok(plist_path) = tray_launch_agent_plist_path() {
            let _ = launchctl_bootout_plist_quiet(&plist_path);
            let _ = fs::remove_file(plist_path);
        }
        let _ = launchctl_remove_label_quiet(&label);
    }
}

#[cfg(target_os = "macos")]
fn tray_launch_agent_plist_path() -> Result<PathBuf> {
    Ok(resolve_paths()?.rotate_home.join("tray.launchd.plist"))
}

#[cfg(target_os = "macos")]
fn write_tray_launch_agent_plist(tray_binary: &Path) -> Result<PathBuf> {
    let plist_path = tray_launch_agent_plist_path()?;
    if let Some(parent) = plist_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::write(&plist_path, tray_launch_agent_plist_contents(tray_binary))
        .with_context(|| format!("Failed to write {}.", plist_path.display()))?;
    Ok(plist_path)
}

#[cfg(target_os = "macos")]
fn tray_launch_agent_plist_contents(tray_binary: &Path) -> String {
    let label = tray_launchd_label();
    let mut xml = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>"#,
    );
    xml.push_str(&xml_escape(&label));
    xml.push_str(
        r#"</string>
  <key>ProgramArguments</key>
  <array>
    <string>"#,
    );
    xml.push_str(&xml_escape(&tray_binary.display().to_string()));
    xml.push_str(
        r#"</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>ProcessType</key>
  <string>Interactive</string>
"#,
    );
    let env_vars = launch_agent_environment_variables();
    if !env_vars.is_empty() {
        xml.push_str("  <key>EnvironmentVariables</key>\n  <dict>\n");
        for (key, value) in env_vars {
            xml.push_str("    <key>");
            xml.push_str(&xml_escape(&key));
            xml.push_str("</key>\n    <string>");
            xml.push_str(&xml_escape(&value));
            xml.push_str("</string>\n");
        }
        xml.push_str("  </dict>\n");
    }
    xml.push_str("</dict>\n</plist>\n");
    xml
}

#[cfg(target_os = "macos")]
fn launch_agent_environment_variables() -> Vec<(String, String)> {
    [
        "CODEX_ROTATE_HOME",
        "CODEX_ROTATE_CLI_BIN",
        "CODEX_ROTATE_TRAY_BIN",
        "CODEX_ROTATE_DEBUG_PORT",
        "PATH",
    ]
    .iter()
    .filter_map(|key| {
        std::env::var_os(key).map(|value| (key.to_string(), value.to_string_lossy().to_string()))
    })
    .collect()
}

#[cfg(target_os = "macos")]
fn launchctl_bootstrap_plist(plist_path: &Path) -> Result<()> {
    let output = launchctl_output(["bootstrap", &launchctl_user_domain(), &plist_path.display().to_string()])?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("bootstrap", &output, Some(plist_path))
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_kickstart_label(label: &str) -> Result<()> {
    let service = launchctl_service_target(label);
    let output = launchctl_output(["kickstart", "-k", &service])?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("kickstart", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_service_is_registered(label: &str) -> Result<bool> {
    let service = launchctl_service_target(label);
    let output = launchctl_output(["print", &service])?;
    if output.status.success() {
        return Ok(true);
    }
    if launchctl_output_is_absent_service(&output) {
        return Ok(false);
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("print", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn bootstrap_tray_launch_agent_after_reset(plist_path: &Path, message: &str) -> Result<()> {
    let label = tray_launchd_label();
    launchctl_bootout_plist_quiet(plist_path).ok();
    launchctl_remove_label_quiet(&label).ok();
    let mut last_error = None;
    for _ in 0..5 {
        match launchctl_bootstrap_plist(plist_path) {
            Ok(()) => return Ok(()),
            Err(error) => {
                last_error = Some(error);
                std::thread::sleep(Duration::from_millis(250));
                launchctl_bootout_plist_quiet(plist_path).ok();
                launchctl_remove_label_quiet(&label).ok();
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("Failed to bootstrap tray launch agent.")))
        .with_context(|| message.to_string())
}

#[cfg(target_os = "macos")]
fn kickstart_tray_launch_agent(label: &str, message: &str) -> Result<()> {
    launchctl_kickstart_label(label).with_context(|| message.to_string())
}

#[cfg(target_os = "macos")]
fn launchctl_output<const N: usize>(args: [&str; N]) -> Result<Output> {
    Command::new("launchctl")
        .args(args)
        .output()
        .context("Failed to invoke launchctl.")
}

#[cfg(target_os = "macos")]
fn launchctl_bootout_plist_quiet(plist_path: &Path) -> Result<()> {
    let output = Command::new("launchctl")
        .arg("bootout")
        .arg(launchctl_user_domain())
        .arg(plist_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .context("Failed to invoke launchctl bootout.")?;
    if output.status.success() || launchctl_output_is_absent_service(&output) {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("bootout", &output, Some(plist_path))
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_remove_label_quiet(label: &str) -> Result<()> {
    let output = Command::new("launchctl")
        .arg("remove")
        .arg(label)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .context("Failed to invoke launchctl remove.")?;
    if output.status.success() || launchctl_output_is_absent_service(&output) {
        return Ok(());
    }
    Err(anyhow!(
        "{}",
        format_launchctl_failure("remove", &output, None)
    ))
}

#[cfg(target_os = "macos")]
fn launchctl_output_is_absent_service(output: &Output) -> bool {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    [stderr.as_ref(), stdout.as_ref()].iter().any(|text| {
        text.contains("Could not find service")
            || text.contains("No such process")
            || text.contains("service cannot load in requested session")
    })
}

#[cfg(target_os = "macos")]
fn format_launchctl_failure(action: &str, output: &Output, plist_path: Option<&Path>) -> String {
    let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match plist_path {
        Some(path) if !detail.is_empty() => format!(
            "launchctl {action} exited with status {} for {}: {}",
            output.status,
            path.display(),
            detail
        ),
        Some(path) => format!(
            "launchctl {action} exited with status {} for {}.",
            output.status,
            path.display()
        ),
        None if !detail.is_empty() => {
            format!("launchctl {action} exited with status {}: {}", output.status, detail)
        }
        None => format!("launchctl {action} exited with status {}.", output.status),
    }
}

#[cfg(target_os = "macos")]
fn launchctl_user_domain() -> String {
    format!("gui/{}", effective_user_id())
}

#[cfg(target_os = "macos")]
fn launchctl_service_target(label: &str) -> String {
    format!("{}/{}", launchctl_user_domain(), label)
}

#[cfg(target_os = "macos")]
fn tray_launchd_label() -> String {
    std::env::var(MACOS_TRAY_LAUNCHD_LABEL_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| MACOS_TRAY_LAUNCHD_LABEL.to_string())
}

#[cfg(target_os = "macos")]
fn effective_user_id() -> u32 {
    std::env::var("UID")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .or_else(|| {
            Command::new("id")
                .arg("-u")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| {
                    String::from_utf8_lossy(&output.stdout)
                        .trim()
                        .parse::<u32>()
                        .ok()
                })
        })
        .unwrap_or(0)
}

#[cfg(any(unix, target_os = "macos"))]
fn shell_single_quote(path: &Path) -> String {
    shell_single_quote_string(&path.display().to_string())
}

#[cfg(any(unix, target_os = "macos"))]
fn shell_single_quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

#[cfg(target_os = "macos")]
fn build_tray_launch_agent_reset_script(plist_path: &Path, label: &str) -> String {
    format!(
        "sleep 1; \
launchctl bootout {domain} {plist} >/dev/null 2>&1 || true; \
launchctl remove {label} >/dev/null 2>&1 || true; \
i=0; \
while [ $i -lt 5 ]; do \
  launchctl bootstrap {domain} {plist} >/dev/null 2>&1 && \
  launchctl kickstart -k {service} >/dev/null 2>&1 && exit 0; \
  i=$((i + 1)); \
  sleep 1; \
  launchctl bootout {domain} {plist} >/dev/null 2>&1 || true; \
  launchctl remove {label} >/dev/null 2>&1 || true; \
done; \
exit 1",
        domain = shell_single_quote_string(&launchctl_user_domain()),
        plist = shell_single_quote(plist_path),
        label = shell_single_quote_string(label),
        service = shell_single_quote_string(&launchctl_service_target(label)),
    )
}

#[cfg(target_os = "macos")]
fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn tracked_cli_source_paths(repo_root: &Path) -> Vec<PathBuf> {
    vec![
        repo_root.join("Cargo.toml"),
        repo_root.join("Cargo.lock"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("src"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("src"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-cli")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-cli")
            .join("src"),
    ]
}

fn local_refresh_disabled() -> bool {
    std::env::var(LOCAL_REFRESH_DISABLE_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn tracked_tray_source_paths(repo_root: &Path) -> Vec<PathBuf> {
    vec![
        repo_root.join("Cargo.toml"),
        repo_root.join("Cargo.lock"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-core")
            .join("src"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-runtime")
            .join("src"),
        repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("Cargo.toml"),
        repo_root
            .join("packages")
            .join("codex-rotate-app")
            .join("src-tauri")
            .join("src"),
    ]
}

fn release_binary_path(current_binary: &Path) -> PathBuf {
    current_binary
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new(""))
        .join("release")
        .join(
            current_binary
                .file_name()
                .unwrap_or_default(),
        )
}

fn detect_local_build(
    binary: &Path,
    manifest_segments: &[&str],
) -> Option<(PathBuf, BuildProfile)> {
    let profile_dir = binary.parent()?;
    let profile = match profile_dir.file_name()?.to_str()? {
        "debug" => BuildProfile::Debug,
        "release" => BuildProfile::Release,
        _ => return None,
    };
    let target_dir = profile_dir.parent()?;
    if target_dir.file_name()?.to_str()? != "target" {
        return None;
    }
    let repo_root = target_dir.parent()?.to_path_buf();
    if !repo_root.join("Cargo.toml").is_file() || !repo_root.join_iter(manifest_segments).is_file()
    {
        return None;
    }
    Some((repo_root, profile))
}

trait JoinPathExt {
    fn join_iter(&self, segments: &[&str]) -> PathBuf;
}

impl JoinPathExt for PathBuf {
    fn join_iter(&self, segments: &[&str]) -> PathBuf {
        let mut path = self.clone();
        for segment in segments {
            path.push(segment);
        }
        path
    }
}

impl JoinPathExt for Path {
    fn join_iter(&self, segments: &[&str]) -> PathBuf {
        let mut path = self.to_path_buf();
        for segment in segments {
            path.push(segment);
        }
        path
    }
}

fn source_paths_newer_than_binary(
    binary_modified: SystemTime,
    paths: Vec<PathBuf>,
) -> Result<bool> {
    for candidate in paths {
        if path_contains_newer_file(&candidate, binary_modified)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn rebuild_local_binary(
    repo_root: &Path,
    profile: BuildProfile,
    manifest_path: PathBuf,
    package_name: &str,
) -> Result<()> {
    let cargo_binary = resolve_cargo_binary();
    let mut command = Command::new(&cargo_binary);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg("-p")
        .arg(package_name);
    if profile == BuildProfile::Release {
        command.arg("--release");
    }

    let status = command.status().with_context(|| {
        format!(
            "Failed to invoke {} build for {package_name}.",
            cargo_binary.display()
        )
    })?;
    if status.success() {
        return Ok(());
    }

    Err(anyhow!(
        "{} build exited with status {} while rebuilding {} from {}.",
        cargo_binary.display(),
        status,
        package_name,
        repo_root.display()
    ))
}

fn maybe_start_background_release_build(
    manifest_path: &Path,
    package_name: &str,
    current_binary: &Path,
    tracked_paths: Vec<PathBuf>,
) -> Result<bool> {
    let release_binary = release_binary_path(current_binary);
    if binary_is_current(&release_binary, tracked_paths)? {
        clear_stale_release_build_lock(package_name)?;
        return Ok(false);
    }

    let Some(lock_path) = try_acquire_release_build_lock(package_name)? else {
        return Ok(false);
    };

    let cargo_binary = resolve_cargo_binary();
    let mut command = Command::new(&cargo_binary);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path)
        .arg("-p")
        .arg(package_name)
        .arg("--release");
    let pid = spawn_detached_command(&mut command).with_context(|| {
        format!(
            "Failed to invoke {} build --release for {}.",
            cargo_binary.display(),
            package_name
        )
    })?;
    fs::write(&lock_path, pid.to_string()).with_context(|| {
        format!(
            "Failed to record background release build pid in {}.",
            lock_path.display()
        )
    })?;
    Ok(true)
}

fn resolve_cargo_binary() -> PathBuf {
    let candidates = [
        std::env::var_os("CODEX_ROTATE_CARGO_BIN").map(PathBuf::from),
        std::env::var_os("CARGO_BIN").map(PathBuf::from),
        std::env::var_os("CARGO").map(PathBuf::from),
        find_binary_in_path("cargo"),
        dirs::home_dir().map(|home| home.join(".cargo").join("bin").join("cargo")),
        Some(PathBuf::from("/opt/homebrew/bin/cargo")),
        Some(PathBuf::from("/usr/local/bin/cargo")),
    ];
    for candidate in candidates.into_iter().flatten() {
        if candidate.is_file() {
            return candidate;
        }
    }
    PathBuf::from("cargo")
}

fn binary_is_current(binary: &Path, tracked_paths: Vec<PathBuf>) -> Result<bool> {
    if !binary.is_file() {
        return Ok(false);
    }
    let modified = file_modified_at(binary)?;
    Ok(!source_paths_newer_than_binary(modified, tracked_paths)?)
}

fn preferred_release_binary(
    current_binary: &Path,
    tracked_paths: Vec<PathBuf>,
) -> Result<Option<PathBuf>> {
    let release_binary = release_binary_path(current_binary);
    if current_binary == release_binary {
        return Ok(None);
    }
    if binary_is_current(&release_binary, tracked_paths)? {
        return Ok(Some(release_binary));
    }
    Ok(None)
}

fn release_build_lock_path(package_name: &str) -> Result<PathBuf> {
    let paths = resolve_paths()?;
    Ok(paths.rotate_home.join(format!(
        ".release-build-{}.pid",
        package_name.replace(|ch: char| !ch.is_ascii_alphanumeric(), "-")
    )))
}

fn try_acquire_release_build_lock(package_name: &str) -> Result<Option<PathBuf>> {
    let lock_path = release_build_lock_path(package_name)?;
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }

    loop {
        match OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(_) => return Ok(Some(lock_path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if release_build_lock_is_stale(&lock_path)? {
                    fs::remove_file(&lock_path).ok();
                    continue;
                }
                return Ok(None);
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("Failed to create {}.", lock_path.display()))
            }
        }
    }
}

fn clear_stale_release_build_lock(package_name: &str) -> Result<()> {
    let lock_path = release_build_lock_path(package_name)?;
    if lock_path.exists() && release_build_lock_is_stale(&lock_path)? {
        fs::remove_file(&lock_path).ok();
    }
    Ok(())
}

fn release_build_lock_is_stale(lock_path: &Path) -> Result<bool> {
    let pid = fs::read_to_string(lock_path)
        .ok()
        .and_then(|raw| raw.trim().parse::<u32>().ok());
    match pid {
        Some(pid) => Ok(!process_is_running(pid)),
        None => Ok(true),
    }
}

fn process_is_running(process_id: u32) -> bool {
    #[cfg(unix)]
    {
        Command::new("kill")
            .args(["-0", &process_id.to_string()])
            .status()
            .map(|status| status.success())
            .unwrap_or(false)
    }

    #[cfg(windows)]
    {
        Command::new("tasklist")
            .args(["/FI", &format!("PID eq {}", process_id), "/FO", "CSV", "/NH"])
            .output()
            .map(|output| output.status.success() && String::from_utf8_lossy(&output.stdout).contains(&process_id.to_string()))
            .unwrap_or(false)
    }
}

fn spawn_detached_command(command: &mut Command) -> Result<u32> {
    #[cfg(unix)]
    {
        unsafe {
            command.pre_exec(|| {
                if setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let child = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    Ok(child.id())
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

fn path_contains_newer_file(path: &Path, binary_modified: SystemTime) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if path.is_file() {
        return Ok(is_meaningfully_newer(
            file_modified_at(path)?,
            binary_modified,
        ));
    }

    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read directory {}.", path.display()))?
    {
        let entry = entry?;
        if path_contains_newer_file(&entry.path(), binary_modified)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn file_modified_at(path: &Path) -> Result<SystemTime> {
    fs::metadata(path)
        .with_context(|| format!("Failed to stat {}.", path.display()))?
        .modified()
        .with_context(|| format!("Failed to read modified time for {}.", path.display()))
}

fn is_meaningfully_newer(left: SystemTime, right: SystemTime) -> bool {
    left.duration_since(right)
        .map(|delta| delta > FRESHNESS_TOLERANCE)
        .unwrap_or(false)
}

fn daemon_pids_from_lsof(daemon_socket: &Path) -> Result<Vec<u32>> {
    if !daemon_socket.exists() {
        return Ok(Vec::new());
    }

    let output = Command::new("lsof")
        .arg("-t")
        .arg(daemon_socket)
        .output()
        .context("Failed to invoke lsof for daemon socket lookup.")?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(parse_process_id)
        .collect::<Vec<_>>())
}

fn daemon_pids_from_ps(cli_binary: &Path) -> Result<Vec<u32>> {
    let cli_binary = cli_binary.display().to_string();
    process_ids_from_ps("running daemon processes", move |line| {
        let mut parts = line.split_whitespace();
        let _pid = parts.next();
        let first = parts.next();
        let second = parts.next();
        let third = parts.next();
        let fourth = parts.next();
        command_tokens_match_binary(first, second, &cli_binary)
            && matches!(
                command_args_after_binary(first, second, third, fourth),
                [Some("daemon"), Some("run")]
            )
    })
}

fn tray_pids_from_ps(tray_binary: &Path) -> Result<Vec<u32>> {
    let tray_binary = tray_binary.display().to_string();
    process_ids_from_ps("running tray processes", move |line| {
        let mut parts = line.split_whitespace();
        let _pid = parts.next();
        let first = parts.next();
        let second = parts.next();
        command_tokens_match_binary(first, second, &tray_binary)
    })
}

fn command_tokens_match_binary(first: Option<&str>, second: Option<&str>, binary: &str) -> bool {
    first == Some(binary) || (shell_like_command(first) && second == Some(binary))
}

fn command_args_after_binary<'a>(
    first: Option<&'a str>,
    second: Option<&'a str>,
    third: Option<&'a str>,
    fourth: Option<&'a str>,
) -> [Option<&'a str>; 2] {
    if shell_like_command(first) {
        [third, fourth]
    } else {
        [second, third]
    }
}

fn shell_like_command(command: Option<&str>) -> bool {
    let Some(command) = command else {
        return false;
    };
    let Some(name) = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return false;
    };
    matches!(name, "sh" | "bash" | "zsh" | "dash")
}

fn process_ids_from_ps<F>(label: &str, predicate: F) -> Result<Vec<u32>>
where
    F: Fn(&str) -> bool,
{
    let output = Command::new("ps")
        .args(["ax", "-o", "pid=,command="])
        .output()
        .with_context(|| format!("Failed to query {label}."))?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(anyhow!(if detail.is_empty() {
            format!("Failed to query {label}.")
        } else {
            detail
        }));
    }

    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| predicate(line))
        .filter_map(|line| line.split_whitespace().next().and_then(parse_process_id))
        .collect::<Vec<_>>())
}

fn stop_process(process_id: u32) -> Result<()> {
    let status = Command::new("kill")
        .args(["-TERM", &process_id.to_string()])
        .status()
        .context("Failed to invoke kill.")?;
    if status.success() {
        return Ok(());
    }
    Err(anyhow!("kill exited with status {}.", status))
}

fn parse_process_id(raw: &str) -> Option<u32> {
    raw.trim().parse::<u32>().ok().filter(|value| *value > 0)
}

#[cfg(unix)]
unsafe extern "C" {
    fn setsid() -> c_int;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::thread;

    fn env_mutex() -> &'static Mutex<()> {
        static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn restore_var(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(name, value);
            },
            None => unsafe {
                std::env::remove_var(name);
            },
        }
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    #[test]
    fn detect_local_cli_build_reads_target_layout() {
        let path = PathBuf::from("/tmp/demo/target/debug/codex-rotate");
        let repo_root = PathBuf::from("/tmp/demo");
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

        let detected = detect_local_cli_build(&path).expect("detect build");
        assert_eq!(detected.repo_root, repo_root);
        assert_eq!(detected.profile, BuildProfile::Debug);

        fs::remove_dir_all(&repo_root).ok();
    }

    #[test]
    fn detect_local_tray_build_reads_target_layout() {
        let path = PathBuf::from("/tmp/demo-tray/target/debug/codex-rotate-tray");
        let repo_root = PathBuf::from("/tmp/demo-tray");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri"),
        )
        .expect("create tray crate dir");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri")
                .join("Cargo.toml"),
            "",
        )
        .expect("write tray cargo");

        let detected = detect_local_tray_build(&path).expect("detect tray build");
        assert_eq!(detected.repo_root, repo_root);
        assert_eq!(detected.profile, BuildProfile::Debug);

        fs::remove_dir_all(&repo_root).ok();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn tray_launch_agent_plist_enables_keepalive() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_debug_port = std::env::var_os("CODEX_ROTATE_DEBUG_PORT");
        let fake_home = unique_temp_dir("codex-rotate-tray-agent");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &fake_home);
            std::env::set_var("CODEX_ROTATE_DEBUG_PORT", "9333");
        }

        let plist = tray_launch_agent_plist_contents(Path::new("/tmp/codex-rotate-tray"));
        assert!(plist.contains("<key>KeepAlive</key>"));
        assert!(plist.contains("<true/>"));
        assert!(plist.contains(MACOS_TRAY_LAUNCHD_LABEL));
        assert!(plist.contains("CODEX_ROTATE_DEBUG_PORT"));

        restore_var("CODEX_ROTATE_HOME", previous_home);
        restore_var("CODEX_ROTATE_DEBUG_PORT", previous_debug_port);
        fs::remove_dir_all(&fake_home).ok();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn launchctl_absent_service_output_is_treated_as_benign() {
        let output = Output {
            status: exit_status_from_code(5),
            stdout: Vec::new(),
            stderr: b"Could not find service \"com.astronlab.codex-rotate.tray\" in domain for user gui: 501\n".to_vec(),
        };
        assert!(launchctl_output_is_absent_service(&output));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn ensure_tray_process_registered_skips_when_no_plist_marker_exists() {
        let _guard = env_mutex().lock().expect("env mutex");
        let previous_home = std::env::var_os("CODEX_ROTATE_HOME");
        let fake_home = unique_temp_dir("codex-rotate-tray-supervisor");
        fs::create_dir_all(&fake_home).expect("create fake home");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &fake_home);
        }

        let restored = ensure_tray_process_registered().expect("ensure tray registration");
        assert!(!restored);

        restore_var("CODEX_ROTATE_HOME", previous_home);
        fs::remove_dir_all(&fake_home).ok();
    }

    #[cfg(target_os = "macos")]
    fn exit_status_from_code(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        std::process::ExitStatus::from_raw(code << 8)
    }

    #[test]
    fn local_cli_sources_newer_than_binary_detects_stale_binary() {
        let repo_root = unique_temp_dir("codex-rotate-dev-refresh");
        let cli_binary = repo_root.join("target").join("debug").join("codex-rotate");
        fs::create_dir_all(cli_binary.parent().expect("binary parent")).expect("create target dir");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("src"),
        )
        .expect("create core src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("src"),
        )
        .expect("create runtime src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli")
                .join("src"),
        )
        .expect("create cli src");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(repo_root.join("Cargo.lock"), "").expect("write lock");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("Cargo.toml"),
            "",
        )
        .expect("write core cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("Cargo.toml"),
            "",
        )
        .expect("write runtime cargo");
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
        fs::write(&cli_binary, "").expect("write binary");
        thread::sleep(FRESHNESS_TOLERANCE + Duration::from_millis(50));
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("src")
                .join("lib.rs"),
            "pub fn changed() {}",
        )
        .expect("write newer source");

        let build = detect_local_cli_build(&cli_binary).expect("detect build");
        assert!(local_cli_sources_newer_than_binary(&build).expect("freshness"));

        fs::remove_dir_all(&repo_root).ok();
    }

    #[test]
    fn local_tray_sources_newer_than_binary_detects_stale_binary() {
        let repo_root = unique_temp_dir("codex-rotate-tray-refresh");
        let tray_binary = repo_root
            .join("target")
            .join("debug")
            .join("codex-rotate-tray");
        fs::create_dir_all(tray_binary.parent().expect("binary parent"))
            .expect("create target dir");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("src"),
        )
        .expect("create core src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("src"),
        )
        .expect("create runtime src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-cli")
                .join("src"),
        )
        .expect("create cli src");
        fs::create_dir_all(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri")
                .join("src"),
        )
        .expect("create tray src");
        fs::write(repo_root.join("Cargo.toml"), "").expect("write root cargo");
        fs::write(repo_root.join("Cargo.lock"), "").expect("write lock");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-core")
                .join("Cargo.toml"),
            "",
        )
        .expect("write core cargo");
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate")
                .join("crates")
                .join("codex-rotate-runtime")
                .join("Cargo.toml"),
            "",
        )
        .expect("write runtime cargo");
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
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri")
                .join("Cargo.toml"),
            "",
        )
        .expect("write tray cargo");
        fs::write(&tray_binary, "").expect("write tray binary");
        thread::sleep(FRESHNESS_TOLERANCE + Duration::from_millis(50));
        fs::write(
            repo_root
                .join("packages")
                .join("codex-rotate-app")
                .join("src-tauri")
                .join("src")
                .join("main.rs"),
            "fn changed() {}",
        )
        .expect("write newer tray source");

        let build = detect_local_tray_build(&tray_binary).expect("detect tray build");
        assert!(local_tray_sources_newer_than_binary(&build).expect("tray freshness"));

        fs::remove_dir_all(&repo_root).ok();
    }

    #[test]
    fn daemon_socket_age_detects_newer_binary() {
        let root = unique_temp_dir("codex-rotate-daemon-age");
        let daemon_socket = root.join("daemon.sock");
        let cli_binary = root.join("codex-rotate");
        fs::create_dir_all(&root).expect("create root");
        fs::write(&daemon_socket, "").expect("write socket placeholder");
        thread::sleep(FRESHNESS_TOLERANCE + Duration::from_millis(50));
        fs::write(&cli_binary, "").expect("write binary");

        assert!(
            daemon_socket_is_older_than_binary(&daemon_socket, &cli_binary)
                .expect("socket freshness")
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn resolve_cargo_binary_prefers_override() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let previous = std::env::var_os("CODEX_ROTATE_CARGO_BIN");
        let expected = tempdir.path().join("cargo");
        fs::write(&expected, "").expect("write cargo override");
        unsafe {
            std::env::set_var("CODEX_ROTATE_CARGO_BIN", &expected);
        }

        let resolved = resolve_cargo_binary();

        restore_var("CODEX_ROTATE_CARGO_BIN", previous);
        assert_eq!(resolved, expected);
    }
}
