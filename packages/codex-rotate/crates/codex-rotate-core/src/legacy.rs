use std::process::Command;

use anyhow::{anyhow, Context, Result};

use crate::paths::resolve_paths;

pub fn run_legacy_cli_command(args: &[&str]) -> Result<String> {
    let paths = resolve_paths()?;
    let output = Command::new(&paths.bun_bin)
        .arg(&paths.legacy_cli_entrypoint)
        .args(args)
        .current_dir(&paths.repo_root)
        .output()
        .with_context(|| {
            format!(
                "Failed to run {} {}.",
                paths.bun_bin,
                paths.legacy_cli_entrypoint.display()
            )
        })?;
    if output.status.success() {
        return Ok(String::from_utf8_lossy(&output.stdout).to_string());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Err(anyhow!(if !stderr.is_empty() {
        stderr
    } else if !stdout.is_empty() {
        stdout
    } else {
        "Legacy codex-rotate command failed.".to_string()
    }))
}

pub fn run_legacy_create_ignore_current() -> Result<String> {
    run_legacy_cli_command(&["create", "--ignore-current"])
}

pub fn run_legacy_next_auto_create() -> Result<String> {
    run_legacy_cli_command(&["__legacy_next_create"])
}
