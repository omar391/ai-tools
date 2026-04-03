use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

use crate::cdp::is_cdp_ready;
use crate::paths::resolve_paths;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodexLaunchSession {
    pub app_path: String,
    pub port: u16,
    pub profile_dir: String,
    pub launched_at: String,
}

pub fn read_launch_session() -> Result<Option<CodexLaunchSession>> {
    let paths = resolve_paths()?;
    if !paths.session_file.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(&paths.session_file)
        .with_context(|| format!("Failed to read {}.", paths.session_file.display()))?;
    let session = serde_json::from_str(&raw)
        .with_context(|| format!("Invalid launch session at {}.", paths.session_file.display()))?;
    Ok(Some(session))
}

pub fn ensure_debug_codex_instance(
    app_path: Option<&str>,
    port: Option<u16>,
    profile_dir: Option<&Path>,
    wait_ms: Option<u64>,
) -> Result<CodexLaunchSession> {
    let paths = resolve_paths()?;
    let session = CodexLaunchSession {
        app_path: app_path.unwrap_or("/Applications/Codex.app").to_string(),
        port: port.unwrap_or(9333),
        profile_dir: profile_dir
            .unwrap_or(&paths.debug_profile_dir)
            .display()
            .to_string(),
        launched_at: chrono::Utc::now()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    };

    if is_cdp_ready(session.port) {
        write_launch_session(&paths.session_file, &session)?;
        return Ok(session);
    }

    fs::create_dir_all(&paths.rotate_app_home)
        .with_context(|| format!("Failed to create {}.", paths.rotate_app_home.display()))?;
    fs::create_dir_all(&session.profile_dir)
        .with_context(|| format!("Failed to create {}.", session.profile_dir))?;

    let output = Command::new("open")
        .arg("-na")
        .arg(&session.app_path)
        .arg("--args")
        .arg(format!("--user-data-dir={}", session.profile_dir))
        .arg(format!("--remote-debugging-port={}", session.port))
        .output()
        .with_context(|| format!("Failed to launch Codex from {}.", session.app_path))?;
    if !output.status.success() {
        return Err(anyhow!(
            "{}",
            String::from_utf8_lossy(&output.stderr)
                .trim()
                .to_string()
                .if_empty_then(|| format!("Failed to launch Codex from {}.", session.app_path))
        ));
    }

    let deadline = Instant::now() + Duration::from_millis(wait_ms.unwrap_or(15_000));
    while Instant::now() < deadline {
        if is_cdp_ready(session.port) {
            write_launch_session(&paths.session_file, &session)?;
            return Ok(session);
        }
        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Codex did not expose a remote debugging target on port {}.",
        session.port
    ))
}

fn write_launch_session(path: &Path, session: &CodexLaunchSession) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(session)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("Failed to write {}.", path.display()))?;
    file.write_all(raw.as_bytes())?;
    Ok(())
}

trait IfEmptyThen {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String;
}

impl IfEmptyThen for String {
    fn if_empty_then<F>(self, fallback: F) -> String
    where
        F: FnOnce() -> String,
    {
        if self.is_empty() {
            fallback()
        } else {
            self
        }
    }
}
