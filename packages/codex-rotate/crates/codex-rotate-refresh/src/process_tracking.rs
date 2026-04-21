use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::ops::Deref;
use std::process::{Child, Command, ExitStatus};
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};

use crate::process::process_is_running;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessRecord {
    pub pid: u32,
    pub parent_pid: Option<u32>,
    pub command: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessSnapshot {
    processes: BTreeMap<u32, ProcessRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackedProcessRecord {
    pub label: String,
    pub record: ProcessRecord,
    pub started_at_unix_ms: u128,
    pub preexisting: bool,
}

pub struct ProcessTracker {
    baseline: ProcessSnapshot,
    tracked: Mutex<BTreeMap<u32, TrackedProcessRecord>>,
}

pub struct ProcessLeakGuard {
    tracker: ProcessTracker,
    context: String,
    finished: bool,
}

pub struct TrackedProcess {
    child: Child,
    metadata: TrackedProcessRecord,
}

impl ProcessSnapshot {
    pub fn len(&self) -> usize {
        self.processes.len()
    }

    pub fn contains_pid(&self, pid: u32) -> bool {
        self.processes.contains_key(&pid)
    }

    pub fn get(&self, pid: u32) -> Option<&ProcessRecord> {
        self.processes.get(&pid)
    }

    pub fn iter(&self) -> impl Iterator<Item = &ProcessRecord> {
        self.processes.values()
    }

    pub fn new_processes_since(&self, baseline: &ProcessSnapshot) -> Vec<ProcessRecord> {
        self.processes
            .values()
            .filter(|record| !baseline.contains_pid(record.pid))
            .cloned()
            .collect()
    }
}

impl ProcessTracker {
    pub fn new() -> Result<Self> {
        Ok(Self {
            baseline: current_process_snapshot()?,
            tracked: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn baseline(&self) -> &ProcessSnapshot {
        &self.baseline
    }

    pub fn is_preexisting_pid(&self, pid: u32) -> bool {
        self.baseline.contains_pid(pid)
    }

    pub fn current_snapshot(&self) -> Result<ProcessSnapshot> {
        current_process_snapshot()
    }

    pub fn new_processes_since_baseline(&self) -> Result<Vec<ProcessRecord>> {
        Ok(self.current_snapshot()?.new_processes_since(&self.baseline))
    }

    pub fn tracked_processes(&self) -> Vec<TrackedProcessRecord> {
        self.tracked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect()
    }

    pub fn spawn_tracked_command(
        &self,
        label: impl AsRef<str>,
        command: &mut Command,
    ) -> Result<TrackedProcess> {
        let label = label.as_ref().trim().to_string();
        let command_text = describe_command(command);
        let child = command
            .spawn()
            .with_context(|| format!("spawn tracked process {label}"))?;
        let pid = child.id();
        let record = ProcessRecord {
            pid,
            parent_pid: Some(std::process::id()),
            command: command_text,
        };
        let metadata = TrackedProcessRecord {
            label,
            record: record.clone(),
            started_at_unix_ms: now_unix_millis(),
            preexisting: self.baseline.contains_pid(pid),
        };
        self.tracked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(pid, metadata.clone());
        Ok(TrackedProcess { child, metadata })
    }

    pub fn spawn_codex_command(&self, command: &mut Command) -> Result<TrackedProcess> {
        self.spawn_tracked_command("codex", command)
    }

    pub fn spawn_chrome_command(&self, command: &mut Command) -> Result<TrackedProcess> {
        self.spawn_tracked_command("chrome", command)
    }

    pub fn spawn_utmctl_command(&self, command: &mut Command) -> Result<TrackedProcess> {
        self.spawn_tracked_command("utmctl", command)
    }

    pub fn spawn_guest_bridge_command(&self, command: &mut Command) -> Result<TrackedProcess> {
        self.spawn_tracked_command("guest-bridge", command)
    }

    pub fn spawn_helper_script_command(&self, command: &mut Command) -> Result<TrackedProcess> {
        self.spawn_tracked_command("helper-script", command)
    }

    pub fn record_process(
        &self,
        pid: u32,
        label: impl AsRef<str>,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        let metadata = TrackedProcessRecord {
            label: label.as_ref().trim().to_string(),
            record: ProcessRecord {
                pid,
                parent_pid: None,
                command: command.as_ref().trim().to_string(),
            },
            started_at_unix_ms: now_unix_millis(),
            preexisting,
        };
        self.tracked
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(pid, metadata.clone());
        metadata
    }

    pub fn record_existing_process(
        &self,
        pid: u32,
        label: impl AsRef<str>,
        command: impl AsRef<str>,
    ) -> TrackedProcessRecord {
        self.record_process(pid, label, command, true)
    }

    pub fn record_test_owned_process(
        &self,
        pid: u32,
        label: impl AsRef<str>,
        command: impl AsRef<str>,
    ) -> TrackedProcessRecord {
        self.record_process(pid, label, command, false)
    }

    pub fn record_codex_process(
        &self,
        pid: u32,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        self.record_process(pid, "codex", command, preexisting)
    }

    pub fn record_chrome_process(
        &self,
        pid: u32,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        self.record_process(pid, "chrome", command, preexisting)
    }

    pub fn record_utmctl_process(
        &self,
        pid: u32,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        self.record_process(pid, "utmctl", command, preexisting)
    }

    pub fn record_guest_bridge_process(
        &self,
        pid: u32,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        self.record_process(pid, "guest-bridge", command, preexisting)
    }

    pub fn record_helper_script_process(
        &self,
        pid: u32,
        command: impl AsRef<str>,
        preexisting: bool,
    ) -> TrackedProcessRecord {
        self.record_process(pid, "helper-script", command, preexisting)
    }

    pub fn active_tracked_processes(&self) -> Vec<TrackedProcessRecord> {
        self.tracked_processes()
            .into_iter()
            .filter(|record| !record.preexisting && process_is_running(record.record.pid))
            .collect()
    }

    pub fn assert_no_leaks(&self) -> Result<()> {
        self.assert_no_leaks_with_context("process tracker")
    }

    pub fn assert_no_leaks_with_context(&self, context: impl AsRef<str>) -> Result<()> {
        let context = context.as_ref().trim();
        let leaked = self.active_tracked_processes();
        if leaked.is_empty() {
            return Ok(());
        }

        Err(anyhow!(
            "{} leaked process(es): {}",
            context,
            leaked
                .iter()
                .map(|record| {
                    format!(
                        "{} pid {} (preexisting={}, command={})",
                        record.label, record.record.pid, record.preexisting, record.record.command
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    pub fn leak_guard(self, context: impl AsRef<str>) -> ProcessLeakGuard {
        ProcessLeakGuard {
            tracker: self,
            context: context.as_ref().trim().to_string(),
            finished: false,
        }
    }
}

impl ProcessLeakGuard {
    pub fn finish(mut self) -> Result<()> {
        let context = self.context.clone();
        self.finished = true;
        self.tracker.assert_no_leaks_with_context(context)
    }

    pub fn assert_clean(&self) -> Result<()> {
        self.tracker.assert_no_leaks_with_context(&self.context)
    }
}

impl Deref for ProcessLeakGuard {
    type Target = ProcessTracker;

    fn deref(&self) -> &Self::Target {
        &self.tracker
    }
}

impl Drop for ProcessLeakGuard {
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

impl TrackedProcess {
    pub fn pid(&self) -> u32 {
        self.metadata.record.pid
    }

    pub fn metadata(&self) -> &TrackedProcessRecord {
        &self.metadata
    }

    pub fn label(&self) -> &str {
        &self.metadata.label
    }

    pub fn command(&self) -> &str {
        &self.metadata.record.command
    }

    pub fn is_preexisting(&self) -> bool {
        self.metadata.preexisting
    }

    pub fn try_wait(&mut self) -> Result<Option<ExitStatus>> {
        Ok(self.child.try_wait().context("poll tracked process")?)
    }

    pub fn wait(&mut self) -> Result<ExitStatus> {
        self.child.wait().context("wait tracked process")
    }

    pub fn kill(&mut self) -> Result<()> {
        self.child.kill().context("kill tracked process")?;
        Ok(())
    }
}

impl Drop for TrackedProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn current_process_snapshot() -> Result<ProcessSnapshot> {
    let processes = list_processes()?;
    Ok(ProcessSnapshot {
        processes: processes
            .into_iter()
            .map(|record| (record.pid, record))
            .collect(),
    })
}

fn describe_command(command: &Command) -> String {
    let mut parts = vec![os_to_string(command.get_program())];
    parts.extend(command.get_args().map(os_to_string));
    parts.join(" ")
}

fn os_to_string(value: &OsStr) -> String {
    value.to_string_lossy().to_string()
}

fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or(0)
}

#[cfg(unix)]
fn list_processes() -> Result<Vec<ProcessRecord>> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,ppid=,command="])
        .output()
        .context("Failed to list running processes.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to list running processes: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter_map(|line| parse_unix_process_line(line).ok())
        .collect())
}

#[cfg(unix)]
fn parse_unix_process_line(line: &str) -> Result<ProcessRecord> {
    let trimmed = line.trim_start();
    let (pid, rest) = trimmed
        .split_once(' ')
        .ok_or_else(|| anyhow!("missing pid column"))?;
    let (parent_pid, command) = rest
        .trim_start()
        .split_once(' ')
        .ok_or_else(|| anyhow!("missing parent pid or command column"))?;
    let pid = pid.trim().parse::<u32>().context("parse pid")?;
    let parent_pid = parent_pid.trim().parse::<u32>().ok();
    Ok(ProcessRecord {
        pid,
        parent_pid,
        command: command.trim().to_string(),
    })
}

#[cfg(windows)]
fn list_processes() -> Result<Vec<ProcessRecord>> {
    let output = Command::new("tasklist")
        .args(["/FO", "CSV", "/NH"])
        .output()
        .context("Failed to list running processes.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to list running processes: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .filter_map(parse_windows_process_line)
        .collect())
}

#[cfg(windows)]
fn parse_windows_process_line(line: &str) -> Option<ProcessRecord> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let cleaned = trimmed.trim_matches('"');
    let mut columns = cleaned.split("\",\"");
    let image_name = columns.next()?.to_string();
    let pid = columns.next()?.trim().parse::<u32>().ok()?;
    Some(ProcessRecord {
        pid,
        parent_pid: None,
        command: image_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracker_marks_current_process_as_preexisting() -> Result<()> {
        let tracker = ProcessTracker::new()?;
        assert!(tracker.is_preexisting_pid(std::process::id()));
        assert!(tracker.baseline().contains_pid(std::process::id()));
        Ok(())
    }

    #[test]
    fn tracker_records_spawned_processes_and_reports_leaks() -> Result<()> {
        let tracker = ProcessTracker::new()?;
        let mut command = Command::new("sleep");
        command.arg("30");

        let mut child = tracker.spawn_tracked_command("sleep-helper", &mut command)?;
        assert!(!child.is_preexisting());
        assert!(!tracker.is_preexisting_pid(child.pid()));
        assert!(tracker
            .new_processes_since_baseline()?
            .iter()
            .any(|record| record.pid == child.pid()));

        let leak_error = tracker.assert_no_leaks().expect_err("expected leak");
        assert!(leak_error.to_string().contains("sleep-helper"));
        assert!(leak_error.to_string().contains(&child.pid().to_string()));

        child.kill()?;
        let _ = child.wait();
        tracker.assert_no_leaks()?;
        Ok(())
    }

    #[test]
    fn tracker_ignores_preexisting_processes_when_asserting_leaks() -> Result<()> {
        let tracker = ProcessTracker::new()?;
        tracker.record_existing_process(
            std::process::id(),
            "operator-shell",
            std::env::args()
                .next()
                .unwrap_or_else(|| "test-binary".to_string()),
        );

        tracker.assert_no_leaks()?;
        assert!(tracker.active_tracked_processes().is_empty());
        assert!(tracker
            .tracked_processes()
            .iter()
            .any(|record| record.preexisting));
        Ok(())
    }

    #[test]
    fn leak_guard_reports_test_owned_processes_on_drop() -> Result<()> {
        let tracker = ProcessTracker::new()?;
        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .context("spawn leak test process")?;
        tracker.record_test_owned_process(child.id(), "sleep-helper", "sleep 30");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = tracker.leak_guard("test process cleanup");
            drop(guard);
        }));
        assert!(result.is_err());

        child.kill().context("kill leak test process")?;
        let _ = child.wait();
        Ok(())
    }

    #[test]
    fn named_helpers_tag_expected_process_families() -> Result<()> {
        let tracker = ProcessTracker::new()?;
        let codex = tracker.record_codex_process(11, "codex --version", false);
        let chrome = tracker.record_chrome_process(12, "Google Chrome --test", true);
        let utmctl = tracker.record_utmctl_process(13, "utmctl list", false);
        let guest_bridge = tracker.record_guest_bridge_process(14, "guest-bridge serve", false);
        let helper = tracker.record_helper_script_process(15, "bootstrap-vm-base.sh", false);

        assert_eq!(codex.label, "codex");
        assert_eq!(chrome.label, "chrome");
        assert_eq!(utmctl.label, "utmctl");
        assert_eq!(guest_bridge.label, "guest-bridge");
        assert_eq!(helper.label, "helper-script");
        assert!(!codex.preexisting);
        assert!(chrome.preexisting);
        assert!(tracker
            .tracked_processes()
            .iter()
            .any(|record| record.label == "guest-bridge"));
        Ok(())
    }
}
