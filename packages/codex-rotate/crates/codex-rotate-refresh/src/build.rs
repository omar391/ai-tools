use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::paths::resolve_paths;

use crate::process::{process_is_running, spawn_detached_command};
use crate::targets::{
    manifest_path, package_name, tracked_source_paths, BuildProfile, LocalBinaryBuild,
};

const FRESHNESS_TOLERANCE: Duration = Duration::from_secs(1);
const LOCAL_REFRESH_DISABLE_ENV: &str = "CODEX_ROTATE_DISABLE_LOCAL_REFRESH";

pub fn sources_newer_than_binary(build: &LocalBinaryBuild) -> Result<bool> {
    let binary_modified = file_modified_at(&build.binary_path)?;
    source_paths_newer_than_binary(binary_modified, tracked_source_paths(build))
}

pub fn daemon_socket_is_older_than_binary(daemon_socket: &Path, cli_binary: &Path) -> Result<bool> {
    if !daemon_socket.exists() || !cli_binary.exists() {
        return Ok(false);
    }
    let socket_modified = file_modified_at(daemon_socket)?;
    let binary_modified = file_modified_at(cli_binary)?;
    Ok(is_meaningfully_newer(binary_modified, socket_modified))
}

pub fn rebuild_local_binary(build: &LocalBinaryBuild) -> Result<()> {
    let cargo_binary = resolve_cargo_binary();
    let mut command = Command::new(&cargo_binary);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path(build))
        .arg("-p")
        .arg(package_name(build.target));
    if build.profile == BuildProfile::Release {
        command.arg("--release");
    }

    let status = command.status().with_context(|| {
        format!(
            "Failed to invoke {} build for {}.",
            cargo_binary.display(),
            package_name(build.target)
        )
    })?;
    if status.success() {
        return Ok(());
    }

    Err(anyhow!(
        "{} build exited with status {} while rebuilding {} from {}.",
        cargo_binary.display(),
        status,
        package_name(build.target),
        build.repo_root.display()
    ))
}

pub fn maybe_start_background_release_build(build: &LocalBinaryBuild) -> Result<bool> {
    if local_refresh_disabled() || build.profile != BuildProfile::Debug {
        return Ok(false);
    }

    let tracked_paths = tracked_source_paths(build);
    let release_binary = release_binary_path(&build.binary_path);
    if binary_is_current(&release_binary, tracked_paths)? {
        clear_stale_release_build_lock(package_name(build.target))?;
        return Ok(false);
    }

    let Some(lock_path) = try_acquire_release_build_lock(package_name(build.target))? else {
        return Ok(false);
    };

    let cargo_binary = resolve_cargo_binary();
    let mut command = Command::new(&cargo_binary);
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(manifest_path(build))
        .arg("-p")
        .arg(package_name(build.target))
        .arg("--release");
    let pid = spawn_detached_command(&mut command).with_context(|| {
        format!(
            "Failed to invoke {} build --release for {}.",
            cargo_binary.display(),
            package_name(build.target)
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

pub fn preferred_release_binary(build: &LocalBinaryBuild) -> Result<Option<PathBuf>> {
    if local_refresh_disabled() || build.profile != BuildProfile::Debug {
        return Ok(None);
    }

    let release_binary = release_binary_path(&build.binary_path);
    if build.binary_path == release_binary {
        return Ok(None);
    }
    if binary_is_current(&release_binary, tracked_source_paths(build))? {
        return Ok(Some(release_binary));
    }
    Ok(None)
}

pub fn local_refresh_disabled() -> bool {
    std::env::var(LOCAL_REFRESH_DISABLE_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn release_binary_path(current_binary: &Path) -> PathBuf {
    current_binary
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new(""))
        .join("release")
        .join(current_binary.file_name().unwrap_or_default())
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
                    .with_context(|| format!("Failed to create {}.", lock_path.display()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::targets::{detect_local_build, TargetKind};
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::thread;

    fn env_mutex() -> &'static Mutex<()> {
        static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn restore_var(name: &str, value: Option<OsString>) {
        match value {
            Some(value) => unsafe { std::env::set_var(name, value) },
            None => unsafe { std::env::remove_var(name) },
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

        let build = detect_local_build(&cli_binary, TargetKind::Cli).expect("detect build");
        assert!(sources_newer_than_binary(&build).expect("freshness"));

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

        let build = detect_local_build(&tray_binary, TargetKind::Tray).expect("detect tray build");
        assert!(sources_newer_than_binary(&build).expect("tray freshness"));

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
