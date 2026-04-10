use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};

use crate::targets::LocalBinaryBuild;

pub const INSTANCE_HOME_ARG: &str = "--instance-home";

pub fn stop_running_daemons(cli_binary: &Path, daemon_socket: &Path) -> Result<()> {
    let mut pids = daemon_pids_from_lsof(daemon_socket).unwrap_or_default();
    for pid in daemon_pids_from_ps(cli_binary, None)? {
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
    build: &LocalBinaryBuild,
    daemon_socket: &Path,
    keep_pid: u32,
    instance_home: Option<&str>,
) -> Result<()> {
    let Some(binary_name) = build.binary_path.file_name() else {
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
        for pid in daemon_pids_from_ps(binary, instance_home)? {
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

pub(crate) fn process_is_running(process_id: u32) -> bool {
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
            .args([
                "/FI",
                &format!("PID eq {}", process_id),
                "/FO",
                "CSV",
                "/NH",
            ])
            .output()
            .map(|output| {
                output.status.success()
                    && String::from_utf8_lossy(&output.stdout).contains(&process_id.to_string())
            })
            .unwrap_or(false)
    }
}

pub(crate) fn spawn_detached_command(command: &mut Command) -> Result<u32> {
    #[cfg(unix)]
    {
        use std::os::raw::c_int;
        use std::os::unix::process::CommandExt;

        unsafe extern "C" {
            fn setsid() -> c_int;
        }

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

fn daemon_pids_from_ps(cli_binary: &Path, instance_home: Option<&str>) -> Result<Vec<u32>> {
    let cli_binary = cli_binary.display().to_string();
    process_ids_from_ps("running daemon processes", move |line| {
        let mut parts = line.split_whitespace();
        let _pid = parts.next();
        let command_parts = parts.collect::<Vec<_>>();
        let first = command_parts.first().copied();
        let second = command_parts.get(1).copied();
        if shell_like_command(first) {
            return false;
        }
        if !command_tokens_match_binary(first, second, &cli_binary) {
            return false;
        }
        let args = command_args_after_binary(&command_parts);
        args.starts_with(&["daemon"]) && matches_instance_home_arg(&args, instance_home)
    })
}

fn tray_pids_from_ps(tray_binary: &Path) -> Result<Vec<u32>> {
    let tray_binary = tray_binary.display().to_string();
    process_ids_from_ps("running tray processes", move |line| {
        let mut parts = line.split_whitespace();
        let _pid = parts.next();
        let first = parts.next();
        let second = parts.next();
        !shell_like_command(first) && command_tokens_match_binary(first, second, &tray_binary)
    })
}

fn command_tokens_match_binary(first: Option<&str>, second: Option<&str>, binary: &str) -> bool {
    first == Some(binary) || second == Some(binary)
}

fn command_args_after_binary<'a>(command_parts: &'a [&'a str]) -> Vec<&'a str> {
    match command_parts {
        [first, second, remaining @ ..]
            if second.ends_with("codex-rotate") && !shell_like_command(Some(first)) =>
        {
            remaining.to_vec()
        }
        [_, remaining @ ..] => remaining.to_vec(),
        [] => Vec::new(),
    }
}

fn shell_like_command(command: Option<&str>) -> bool {
    matches!(
        command,
        Some("/bin/sh")
            | Some("sh")
            | Some("/bin/zsh")
            | Some("zsh")
            | Some("/bin/bash")
            | Some("bash")
    )
}

fn process_ids_from_ps<F>(label: &str, predicate: F) -> Result<Vec<u32>>
where
    F: Fn(&str) -> bool,
{
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
        .output()
        .with_context(|| format!("Failed to inspect {label}."))?;
    if !output.status.success() {
        return Ok(Vec::new());
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| predicate(line))
        .filter_map(|line| line.split_whitespace().next().and_then(parse_process_id))
        .collect())
}

pub(crate) fn matches_instance_home_arg(args: &[&str], instance_home: Option<&str>) -> bool {
    match instance_home {
        Some(home) => {
            args.windows(2)
                .any(|window| matches!(window, ["--instance-home", value] if *value == home))
                || args
                    .iter()
                    .any(|value| value.strip_prefix("--instance-home=") == Some(home))
        }
        None => true,
    }
}

fn stop_process(process_id: u32) -> Result<()> {
    #[cfg(unix)]
    {
        let status = Command::new("kill")
            .args(["-TERM", &process_id.to_string()])
            .status()
            .context("Failed to invoke kill.")?;
        if !status.success() {
            return Err(anyhow::anyhow!("kill exited with status {}.", status));
        }
    }

    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .status()
            .context("Failed to invoke taskkill.")?;
        if !status.success() {
            return Err(anyhow::anyhow!("taskkill exited with status {}.", status));
        }
    }

    Ok(())
}

fn parse_process_id(raw: &str) -> Option<u32> {
    raw.trim().parse::<u32>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_home_arg_filters_other_homes() {
        let home = "/tmp/codex-home-a";
        let matching = ["daemon", "--instance-home", home];
        let other = ["daemon", "--instance-home=/tmp/codex-home-b"];
        assert!(matches_instance_home_arg(&matching, Some(home)));
        assert!(!matches_instance_home_arg(&other, Some(home)));
        assert!(matches_instance_home_arg(&other, None));
    }
}
