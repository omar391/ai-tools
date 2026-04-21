use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackedPathKind {
    TempPath,
    Socket,
    Mount,
    SymlinkTarget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackedPathRecord {
    pub label: String,
    pub kind: TrackedPathKind,
    pub path: PathBuf,
    pub started_at_unix_ms: u128,
    pub preexisting: bool,
}

pub struct FilesystemTracker {
    tracked: Mutex<BTreeMap<PathBuf, TrackedPathRecord>>,
}

pub struct FilesystemLeakGuard {
    tracker: FilesystemTracker,
    context: String,
    finished: bool,
}

impl FilesystemTracker {
    pub fn new() -> Result<Self> {
        Ok(Self {
            tracked: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn tracked_paths(&self) -> Vec<TrackedPathRecord> {
        self.tracked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect()
    }

    pub fn record_path(
        &self,
        kind: TrackedPathKind,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedPathRecord {
        let record = TrackedPathRecord {
            label: label.as_ref().trim().to_string(),
            kind,
            path: path.as_ref().to_path_buf(),
            started_at_unix_ms: now_unix_millis(),
            preexisting,
        };
        self.tracked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(record.path.clone(), record.clone());
        record
    }

    pub fn record_existing_path(
        &self,
        kind: TrackedPathKind,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
    ) -> TrackedPathRecord {
        self.record_path(kind, path, label, true)
    }

    pub fn record_test_owned_path(
        &self,
        kind: TrackedPathKind,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
    ) -> TrackedPathRecord {
        self.record_path(kind, path, label, false)
    }

    pub fn record_temp_path(
        &self,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedPathRecord {
        self.record_path(TrackedPathKind::TempPath, path, label, preexisting)
    }

    pub fn record_socket_path(
        &self,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedPathRecord {
        self.record_path(TrackedPathKind::Socket, path, label, preexisting)
    }

    pub fn record_mount_path(
        &self,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedPathRecord {
        self.record_path(TrackedPathKind::Mount, path, label, preexisting)
    }

    pub fn record_symlink_target(
        &self,
        path: impl AsRef<Path>,
        label: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedPathRecord {
        self.record_path(TrackedPathKind::SymlinkTarget, path, label, preexisting)
    }

    pub fn active_tracked_paths(&self) -> Result<Vec<TrackedPathRecord>> {
        self.tracked_paths()
            .into_iter()
            .try_fold(Vec::new(), |mut leaked, record| {
                if record.preexisting || !is_path_still_live(record.kind, &record.path)? {
                    return Ok(leaked);
                }
                leaked.push(record);
                Ok(leaked)
            })
    }

    pub fn assert_no_leaks(&self) -> Result<()> {
        self.assert_no_leaks_with_context("filesystem tracker")
    }

    pub fn assert_no_leaks_with_context(&self, context: impl AsRef<str>) -> Result<()> {
        let context = context.as_ref().trim();
        let leaked = self.active_tracked_paths()?;
        if leaked.is_empty() {
            return Ok(());
        }

        Err(anyhow!(
            "{} leaked path(s): {}",
            context,
            leaked
                .iter()
                .map(|record| {
                    format!(
                        "{} {} path={} (preexisting={})",
                        kind_label(record.kind),
                        record.label,
                        record.path.display(),
                        record.preexisting
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    pub fn leak_guard(self, context: impl AsRef<str>) -> FilesystemLeakGuard {
        FilesystemLeakGuard {
            tracker: self,
            context: context.as_ref().trim().to_string(),
            finished: false,
        }
    }
}

impl FilesystemLeakGuard {
    pub fn finish(mut self) -> Result<()> {
        let context = self.context.clone();
        self.finished = true;
        self.tracker.assert_no_leaks_with_context(context)
    }

    pub fn assert_clean(&self) -> Result<()> {
        self.tracker.assert_no_leaks_with_context(&self.context)
    }
}

impl std::ops::Deref for FilesystemLeakGuard {
    type Target = FilesystemTracker;

    fn deref(&self) -> &Self::Target {
        &self.tracker
    }
}

impl Drop for FilesystemLeakGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        if let Err(error) = self.tracker.assert_no_leaks_with_context(&self.context) {
            if thread::panicking() {
                eprintln!("{error:#}");
            } else {
                panic!("{error:#}");
            }
        }
    }
}

fn kind_label(kind: TrackedPathKind) -> &'static str {
    match kind {
        TrackedPathKind::TempPath => "temp-path",
        TrackedPathKind::Socket => "socket",
        TrackedPathKind::Mount => "mount",
        TrackedPathKind::SymlinkTarget => "symlink-target",
    }
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or(0)
}

fn is_path_still_live(kind: TrackedPathKind, path: &Path) -> Result<bool> {
    match kind {
        TrackedPathKind::TempPath | TrackedPathKind::Socket | TrackedPathKind::SymlinkTarget => {
            Ok(path.exists())
        }
        TrackedPathKind::Mount => is_mounted(path),
    }
}

#[cfg(unix)]
fn is_mounted(path: &Path) -> Result<bool> {
    let output = Command::new("mount")
        .output()
        .context("Failed to inspect mounted filesystems.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to inspect mounted filesystems: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().any(|line| {
        let Some((_, mount_and_rest)) = line.split_once(" on ") else {
            return false;
        };
        let Some((mount_point, _rest)) = mount_and_rest.split_once(" (") else {
            return false;
        };
        Path::new(mount_point) == path
    }))
}

#[cfg(not(unix))]
fn is_mounted(_path: &Path) -> Result<bool> {
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::net::UnixListener;
    use tempfile::tempdir;

    #[test]
    fn tracker_ignores_preexisting_paths_when_asserting_leaks() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let tracker = FilesystemTracker::new()?;
        tracker.record_existing_path(TrackedPathKind::TempPath, temp.path(), "operator-tempdir");

        tracker.assert_no_leaks()?;
        Ok(())
    }

    #[test]
    fn tracker_reports_temp_path_leaks_until_removed() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let leak_dir = temp.path().join("temp-profile");
        fs::create_dir_all(&leak_dir).context("create temp path")?;

        let tracker = FilesystemTracker::new()?;
        tracker.record_test_owned_path(TrackedPathKind::TempPath, &leak_dir, "profile root");

        let error = tracker.assert_no_leaks().expect_err("expected leak");
        assert!(error.to_string().contains("temp-path"));
        assert!(error.to_string().contains("profile root"));
        assert!(error.to_string().contains(&leak_dir.display().to_string()));

        drop(temp);
        tracker.assert_no_leaks()?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn tracker_reports_socket_leaks_until_socket_is_removed() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let socket_path = temp.path().join("daemon.sock");
        let listener = UnixListener::bind(&socket_path).context("bind socket")?;

        let tracker = FilesystemTracker::new()?;
        tracker.record_socket_path(&socket_path, "daemon socket", false);
        let error = tracker.assert_no_leaks().expect_err("expected leak");
        assert!(error.to_string().contains("socket"));
        assert!(error.to_string().contains("daemon socket"));

        drop(listener);
        fs::remove_file(&socket_path).ok();
        tracker.assert_no_leaks()?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn tracker_reports_mount_leaks() -> Result<()> {
        let tracker = FilesystemTracker::new()?;
        tracker.record_test_owned_path(TrackedPathKind::Mount, "/", "root mount");

        let error = tracker.assert_no_leaks().expect_err("expected leak");
        assert!(error.to_string().contains("mount"));
        assert!(error.to_string().contains("root mount"));
        assert!(error.to_string().contains("/"));
        Ok(())
    }

    #[test]
    fn leak_guard_reports_symlink_target_leaks_on_drop() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let target = temp.path().join("target");
        fs::create_dir_all(&target).context("create target")?;

        let tracker = FilesystemTracker::new()?;
        tracker.record_test_owned_path(
            TrackedPathKind::SymlinkTarget,
            &target,
            "codex-home target",
        );

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = tracker.leak_guard("filesystem cleanup");
            drop(guard);
        }));
        assert!(result.is_err());

        drop(temp);
        Ok(())
    }
}
