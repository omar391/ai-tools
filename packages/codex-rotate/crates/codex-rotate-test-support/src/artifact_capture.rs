use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde_json::{json, Value};

use codex_rotate_refresh::{FilesystemTracker, ProcessTracker};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureArtifactCapture {
    root: PathBuf,
    scenario: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FailureArtifactBundle {
    pub root: PathBuf,
    pub scenario_dir: PathBuf,
    pub manifest_path: PathBuf,
}

impl FailureArtifactCapture {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        let root = temp_root(prefix.as_ref())?;
        Ok(Self {
            root,
            scenario: "default".to_string(),
        })
    }

    pub fn with_scenario(mut self, scenario: impl AsRef<str>) -> Self {
        self.scenario = sanitize_segment(scenario.as_ref());
        if self.scenario.is_empty() {
            self.scenario = "scenario".to_string();
        }
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn scenario_dir(&self) -> PathBuf {
        self.root.join(&self.scenario)
    }

    pub fn start_bundle(&self) -> Result<FailureArtifactBundle> {
        let scenario_dir = self.scenario_dir();
        fs::create_dir_all(&scenario_dir)
            .with_context(|| format!("create {}", scenario_dir.display()))?;
        let manifest_path = scenario_dir.join("manifest.json");
        fs::write(&manifest_path, "{}\n")
            .with_context(|| format!("create {}", manifest_path.display()))?;
        Ok(FailureArtifactBundle {
            root: self.root.clone(),
            scenario_dir,
            manifest_path,
        })
    }

    pub fn clear(&self) -> Result<()> {
        if self.root.exists() {
            fs::remove_dir_all(&self.root)
                .with_context(|| format!("remove {}", self.root.display()))?;
        }
        Ok(())
    }
}

impl FailureArtifactBundle {
    pub fn write_text(
        &self,
        relative_path: impl AsRef<Path>,
        contents: impl AsRef<str>,
    ) -> Result<PathBuf> {
        let path = self.scenario_dir.join(relative_path.as_ref());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::write(&path, contents.as_ref()).with_context(|| format!("write {}", path.display()))?;
        Ok(path)
    }

    pub fn write_json(&self, relative_path: impl AsRef<Path>, value: &Value) -> Result<PathBuf> {
        self.write_text(relative_path, serde_json::to_string_pretty(value)?)
    }

    pub fn copy_file(
        &self,
        source: impl AsRef<Path>,
        relative_path: impl AsRef<Path>,
    ) -> Result<Option<PathBuf>> {
        let source = source.as_ref();
        if !source.exists() {
            return Ok(None);
        }
        let target = self.scenario_dir.join(relative_path.as_ref());
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        fs::copy(source, &target)
            .with_context(|| format!("copy {} to {}", source.display(), target.display()))?;
        Ok(Some(target))
    }

    pub fn record_process_snapshot(
        &self,
        tracker: &ProcessTracker,
        relative_path: impl AsRef<Path>,
    ) -> Result<PathBuf> {
        let processes = tracker.tracked_processes();
        let records = processes
            .into_iter()
            .map(|record| {
                json!({
                    "label": record.label,
                    "pid": record.record.pid,
                    "parent_pid": record.record.parent_pid,
                    "command": record.record.command,
                    "started_at_unix_ms": record.started_at_unix_ms,
                    "preexisting": record.preexisting,
                })
            })
            .collect::<Vec<_>>();
        self.write_json(relative_path, &Value::Array(records))
    }

    pub fn record_filesystem_snapshot(
        &self,
        tracker: &FilesystemTracker,
        relative_path: impl AsRef<Path>,
    ) -> Result<PathBuf> {
        let records = tracker
            .tracked_paths()
            .into_iter()
            .map(|record| {
                json!({
                    "label": record.label,
                    "kind": format!("{:?}", record.kind),
                    "path": record.path,
                    "started_at_unix_ms": record.started_at_unix_ms,
                    "preexisting": record.preexisting,
                })
            })
            .collect::<Vec<_>>();
        self.write_json(relative_path, &Value::Array(records))
    }
}

fn temp_root(prefix: &str) -> Result<PathBuf> {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or(0);
    let sanitized = sanitize_segment(prefix);
    let root = std::env::temp_dir().join(format!("{sanitized}-{stamp}"));
    fs::create_dir_all(&root).with_context(|| format!("create {}", root.display()))?;
    Ok(root)
}

fn sanitize_segment(value: &str) -> String {
    let mut output = String::new();
    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
            output.push(character);
        } else {
            output.push('-');
        }
    }
    output.trim_matches('-').to_string()
}

fn _os_str_to_string(value: &OsStr) -> String {
    value.to_string_lossy().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_rotate_refresh::TrackedPathKind;
    use tempfile::tempdir;

    #[test]
    fn capture_bundle_writes_transcripts_snapshots_and_copies_files() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let transcript = temp.path().join("transcript.log");
        fs::write(&transcript, "hello transcript\n").context("write transcript")?;

        let capture =
            FailureArtifactCapture::new("codex-rotate-artifacts")?.with_scenario("host-next");
        let bundle = capture.start_bundle()?;
        let text_path = bundle.write_text("logs/daemon.log", "daemon log\n")?;
        let json_path = bundle.write_json("state/processes.json", &json!({"ok": true}))?;
        let copied = bundle
            .copy_file(&transcript, "transcripts/login.log")?
            .expect("copied transcript");

        assert!(text_path.exists());
        assert!(json_path.exists());
        assert!(copied.exists());
        assert!(bundle.manifest_path.exists());
        Ok(())
    }

    #[test]
    fn capture_can_record_process_and_filesystem_snapshots() -> Result<()> {
        let temp = tempdir().context("tempdir")?;
        let capture = FailureArtifactCapture::new("codex-rotate-artifacts-snapshots")?
            .with_scenario("vm-next");
        let bundle = capture.start_bundle()?;
        let process_tracker = ProcessTracker::new()?;
        let filesystem_tracker = FilesystemTracker::new()?;
        let _ = filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            temp.path(),
            "temp-root",
        );

        let process_snapshot =
            bundle.record_process_snapshot(&process_tracker, "processes.json")?;
        let filesystem_snapshot =
            bundle.record_filesystem_snapshot(&filesystem_tracker, "filesystem.json")?;

        assert!(process_snapshot.exists());
        assert!(filesystem_snapshot.exists());
        Ok(())
    }
}
