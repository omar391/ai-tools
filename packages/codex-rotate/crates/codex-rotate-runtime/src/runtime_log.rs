use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use chrono::{Local, SecondsFormat};

const MAX_LOG_BYTES: usize = 256 * 1024;
const RETAIN_LOG_BYTES: usize = 192 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LogTarget {
    Daemon,
    Tray,
}

impl LogTarget {
    fn filename(self) -> &'static str {
        match self {
            Self::Daemon => "daemon.log",
            Self::Tray => "tray.log",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Daemon => "daemon",
            Self::Tray => "tray",
        }
    }
}

fn log_mutex() -> &'static Mutex<()> {
    static LOG_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    LOG_MUTEX.get_or_init(|| Mutex::new(()))
}

fn rotate_home() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("CODEX_ROTATE_HOME") {
        return Ok(PathBuf::from(path));
    }
    let home = dirs::home_dir().context("Failed to resolve home directory for runtime log.")?;
    Ok(home.join(".codex-rotate"))
}

fn log_path(target: LogTarget) -> Result<PathBuf> {
    Ok(rotate_home()?.join(target.filename()))
}

fn append_log_line(target: LogTarget, level: &str, message: &str) -> Result<()> {
    let path = log_path(target)?;
    append_log_line_to_path(&path, target, level, message)
}

fn append_log_line_to_path(
    path: &Path,
    target: LogTarget,
    level: &str,
    message: &str,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let timestamp = Local::now().to_rfc3339_opts(SecondsFormat::Secs, false);
    let line = format!("{timestamp} [{level}] {}: {message}\n", target.label());

    let _guard = log_mutex().lock().expect("runtime log mutex");
    {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("Failed to open {}.", path.display()))?;
        file.write_all(line.as_bytes())
            .with_context(|| format!("Failed to write {}.", path.display()))?;
        file.flush()
            .with_context(|| format!("Failed to flush {}.", path.display()))?;
    }
    truncate_log_file(path)
}

fn truncate_log_file(path: &Path) -> Result<()> {
    let metadata =
        fs::metadata(path).with_context(|| format!("Failed to stat {}.", path.display()))?;
    if metadata.len() <= MAX_LOG_BYTES as u64 {
        return Ok(());
    }

    let contents = fs::read(path)
        .with_context(|| format!("Failed to read {} for truncation.", path.display()))?;
    if contents.len() <= MAX_LOG_BYTES {
        return Ok(());
    }

    let start = contents.len().saturating_sub(RETAIN_LOG_BYTES);
    let retained = match contents[start..].iter().position(|byte| *byte == b'\n') {
        Some(offset) => contents[start + offset + 1..].to_vec(),
        None => contents[start..].to_vec(),
    };

    let mut truncated = format!(
        "{} [WARN] runtime-log: truncated {} after exceeding {} bytes\n",
        Local::now().to_rfc3339_opts(SecondsFormat::Secs, false),
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("runtime.log"),
        MAX_LOG_BYTES
    )
    .into_bytes();
    truncated.extend_from_slice(&retained);
    fs::write(path, truncated)
        .with_context(|| format!("Failed to rewrite {} after truncation.", path.display()))
}

pub fn daemon_log_path() -> Result<PathBuf> {
    log_path(LogTarget::Daemon)
}

pub fn tray_log_path() -> Result<PathBuf> {
    log_path(LogTarget::Tray)
}

pub fn log_daemon_error(message: impl AsRef<str>) {
    if let Err(error) = append_log_line(LogTarget::Daemon, "ERROR", message.as_ref()) {
        eprintln!("codex-rotate: failed to append daemon log: {error:#}");
    }
}

pub fn log_daemon_info(message: impl AsRef<str>) {
    if let Err(error) = append_log_line(LogTarget::Daemon, "INFO", message.as_ref()) {
        eprintln!("codex-rotate: failed to append daemon log: {error:#}");
    }
}

pub fn log_tray_error(message: impl AsRef<str>) {
    if let Err(error) = append_log_line(LogTarget::Tray, "ERROR", message.as_ref()) {
        eprintln!("codex-rotate: failed to append tray log: {error:#}");
    }
}

pub fn log_tray_info(message: impl AsRef<str>) {
    if let Err(error) = append_log_line(LogTarget::Tray, "INFO", message.as_ref()) {
        eprintln!("codex-rotate: failed to append tray log: {error:#}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::tempdir;

    #[test]
    fn daemon_log_writes_expected_line() {
        let tempdir = tempdir().expect("tempdir");
        let path = tempdir.path().join("daemon.log");

        append_log_line_to_path(&path, LogTarget::Daemon, "ERROR", "daemon exploded")
            .expect("append daemon log line");

        let contents = fs::read_to_string(&path).expect("log file");

        assert!(contents.contains("daemon exploded"));
        assert!(contents.contains("[ERROR] daemon:"));
    }

    #[test]
    fn tray_log_truncates_after_limit() {
        let tempdir = tempdir().expect("tempdir");
        let path = tempdir.path().join("tray.log");

        let large_message = "x".repeat(MAX_LOG_BYTES);
        append_log_line_to_path(&path, LogTarget::Tray, "ERROR", &large_message)
            .expect("append large tray log line");
        append_log_line_to_path(&path, LogTarget::Tray, "ERROR", &large_message)
            .expect("append large tray log line");
        append_log_line_to_path(&path, LogTarget::Tray, "ERROR", "tail message")
            .expect("append tail tray log line");

        let contents = fs::read_to_string(&path).expect("tray log");

        assert!(contents.contains("runtime-log: truncated"));
        assert!(contents.contains("tail message"));
        assert!(fs::metadata(&path).expect("metadata").len() <= MAX_LOG_BYTES as u64);
    }
}
