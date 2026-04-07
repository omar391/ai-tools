use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Context, Result};

const FRESHNESS_TOLERANCE: Duration = Duration::from_secs(1);

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

pub fn detect_local_cli_build(cli_binary: &Path) -> Option<LocalCliBuild> {
    let profile_dir = cli_binary.parent()?;
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
    if !repo_root.join("Cargo.toml").is_file()
        || !repo_root
            .join("packages")
            .join("codex-rotate")
            .join("crates")
            .join("codex-rotate-cli")
            .join("Cargo.toml")
            .is_file()
    {
        return None;
    }

    Some(LocalCliBuild {
        repo_root,
        profile,
        cli_binary: cli_binary.to_path_buf(),
    })
}

pub fn current_process_local_cli_build() -> Option<LocalCliBuild> {
    std::env::current_exe()
        .ok()
        .and_then(|path| detect_local_cli_build(&path))
}

pub fn local_cli_sources_newer_than_binary(build: &LocalCliBuild) -> Result<bool> {
    let binary_modified = file_modified_at(&build.cli_binary)?;
    for candidate in tracked_source_paths(&build.repo_root) {
        if path_contains_newer_file(&candidate, binary_modified)? {
            return Ok(true);
        }
    }
    Ok(false)
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
    let mut command = Command::new("cargo");
    command
        .arg("build")
        .arg("--manifest-path")
        .arg(build.repo_root.join("Cargo.toml"))
        .arg("-p")
        .arg("codex-rotate-cli");
    if build.profile == BuildProfile::Release {
        command.arg("--release");
    }

    let status = command
        .status()
        .context("Failed to invoke cargo build for codex-rotate-cli.")?;
    if status.success() {
        return Ok(());
    }

    Err(anyhow!(
        "cargo build exited with status {} while rebuilding codex-rotate-cli.",
        status
    ))
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

fn tracked_source_paths(repo_root: &Path) -> Vec<PathBuf> {
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
    let output = Command::new("ps")
        .args(["ax", "-o", "pid=,command="])
        .output()
        .context("Failed to query running daemon processes.")?;
    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(anyhow!(if detail.is_empty() {
            "Failed to query running daemon processes.".to_string()
        } else {
            detail
        }));
    }

    let cli_binary = cli_binary.display().to_string();
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| {
            line.contains(&cli_binary) && line.contains("daemon") && line.contains("run")
        })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

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
}
