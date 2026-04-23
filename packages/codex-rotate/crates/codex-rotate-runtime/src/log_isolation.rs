use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use codex_rotate_core::auth::{
    load_codex_auth, summarize_codex_auth, write_codex_auth, AuthSummary, CodexAuth,
};
use codex_rotate_core::fs_security::write_private_string;
use codex_rotate_core::pool::{load_pool, restore_pool_active_index};
use serde::{Deserialize, Serialize};

use crate::cdp::invalidate_local_codex_connection;
use crate::launcher::ensure_debug_codex_instance;
use crate::logs::invalidate_log_connection;
use crate::paths::resolve_paths;
use crate::runtime_log::log_daemon_info;
use crate::thread_recovery::read_active_thread_ids;

const DEFAULT_PORT: u16 = 9333;
const LOGS_MANIFEST_VERSION: u32 = 1;
const PROCESS_STOP_TIMEOUT: Duration = Duration::from_secs(8);
const PROCESS_STOP_POLL_INTERVAL: Duration = Duration::from_millis(200);
const THREAD_IDLE_WAIT_TIMEOUT: Duration = Duration::from_secs(60 * 60 * 2);
const THREAD_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(2);
const THREAD_IDLE_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

pub struct IsolatedAccountOperation<T> {
    pub value: T,
    pub previous_summary: Option<AuthSummary>,
    pub current_summary: Option<AuthSummary>,
    pub account_changed: bool,
    pub managed_running_before: bool,
    pub managed_restarted: bool,
}

#[derive(Clone, Debug)]
struct LogIsolationLayout {
    codex_home: PathBuf,
    manifest_path: PathBuf,
    by_account_root: PathBuf,
    legacy_root: PathBuf,
    transactions_root: PathBuf,
    log_db_basename: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
struct LogsManifest {
    version: u32,
    current_owner_account_id: Option<String>,
    log_db_basename: String,
}

#[derive(Clone, Debug)]
struct LogHandoffState {
    layout: LogIsolationLayout,
    previous_manifest_raw: Option<String>,
    previous_account_id: Option<String>,
    next_account_id: String,
    previous_archive_backup_dir: Option<PathBuf>,
    restored_next_bundle: bool,
    legacy_snapshot_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ProcessInfo {
    pid: u32,
    command: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct OpenHolder {
    pid: u32,
    command: String,
}

pub fn run_account_operation_with_log_isolation<T, F>(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    operation: F,
) -> Result<IsolatedAccountOperation<T>>
where
    F: FnOnce() -> Result<T>,
{
    let port = port.unwrap_or(DEFAULT_PORT);
    let paths = resolve_paths()?;
    let codex_home = paths
        .codex_logs_db_file
        .parent()
        .ok_or_else(|| anyhow!("Failed to resolve the Codex home for logs isolation."))?;
    let layout = log_isolation_layout(codex_home, &paths.rotate_home, &paths.codex_logs_db_file);
    let previous_auth = read_auth_if_exists(&paths.codex_auth_file)?;
    let previous_summary = previous_auth.as_ref().map(summarize_codex_auth);
    let previous_active_index = load_pool().ok().map(|pool| pool.active_index);
    let managed_running_before = managed_codex_is_running(&paths.debug_profile_dir)?;

    let value = match operation() {
        Ok(value) => value,
        Err(error) => {
            rollback_auth_selection(
                &paths.codex_auth_file,
                previous_auth.as_ref(),
                previous_active_index,
            )
            .ok();
            return Err(error);
        }
    };

    let current_auth = read_auth_if_exists(&paths.codex_auth_file)?;
    let current_summary = current_auth.as_ref().map(summarize_codex_auth);
    let account_changed = previous_summary
        .as_ref()
        .map(|summary| summary.account_id.as_str())
        != current_summary
            .as_ref()
            .map(|summary| summary.account_id.as_str());

    if !account_changed {
        return Ok(IsolatedAccountOperation {
            value,
            previous_summary,
            current_summary,
            account_changed: false,
            managed_running_before,
            managed_restarted: false,
        });
    }

    if let Some(progress) = progress.as_ref() {
        progress("Isolating Codex logs for the selected account.".to_string());
    }

    let handoff = match prepare_log_handoff(
        &layout,
        previous_summary
            .as_ref()
            .map(|summary| summary.account_id.as_str()),
        current_summary
            .as_ref()
            .map(|summary| summary.account_id.as_str())
            .ok_or_else(|| {
                anyhow!("Account changed, but the new Codex auth summary is unavailable.")
            })?,
    ) {
        Ok(handoff) => handoff,
        Err(error) => {
            rollback_auth_selection(
                &paths.codex_auth_file,
                previous_auth.as_ref(),
                previous_active_index,
            )
            .ok();
            return Err(error);
        }
    };

    let transition_result = (|| -> Result<bool> {
        invalidate_local_codex_connection(port, true);
        invalidate_log_connection(Some(&paths.codex_logs_db_file));
        if managed_running_before {
            if let Some(progress) = progress.as_ref() {
                progress(
                    "Waiting for active Codex threads to become idle before restart.".to_string(),
                );
            }
            wait_for_all_threads_idle(port, progress.as_ref())?;
            if let Some(progress) = progress.as_ref() {
                progress(
                    "Restarting managed Codex to reopen the isolated logs database.".to_string(),
                );
            }
            stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
        }
        ensure_bundle_is_released(&layout)?;
        if managed_running_before {
            ensure_debug_codex_instance(None, Some(port), None, None)?;
        }
        Ok(managed_running_before)
    })();

    match transition_result {
        Ok(managed_restarted) => {
            finalize_log_handoff(&handoff)?;
            Ok(IsolatedAccountOperation {
                value,
                previous_summary,
                current_summary,
                account_changed: true,
                managed_running_before,
                managed_restarted,
            })
        }
        Err(error) => {
            let rollback_error = rollback_after_failed_isolated_transition(
                &paths.codex_auth_file,
                previous_auth.as_ref(),
                previous_active_index,
                &handoff,
                managed_running_before,
                port,
            );
            if let Err(rollback_error) = rollback_error {
                return Err(anyhow!(
                    "{error} (rollback after failed isolated transition also failed: {rollback_error:#})"
                ));
            }
            Err(error)
        }
    }
}

fn rollback_after_failed_isolated_transition(
    auth_path: &Path,
    previous_auth: Option<&CodexAuth>,
    previous_active_index: Option<usize>,
    handoff: &LogHandoffState,
    managed_running_before: bool,
    port: u16,
) -> Result<()> {
    let mut failures = Vec::new();

    if let Err(error) = rollback_auth_selection(auth_path, previous_auth, previous_active_index) {
        failures.push(format!("auth rollback failed: {error:#}"));
    }
    if let Err(error) = rollback_log_handoff(handoff) {
        failures.push(format!("log rollback failed: {error:#}"));
    }
    if managed_running_before {
        if let Err(error) = ensure_debug_codex_instance(None, Some(port), None, None) {
            failures.push(format!("managed Codex relaunch failed: {error:#}"));
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(failures.join(" | ")))
    }
}

fn rollback_auth_selection(
    auth_path: &Path,
    previous_auth: Option<&CodexAuth>,
    previous_active_index: Option<usize>,
) -> Result<()> {
    match previous_auth {
        Some(auth) => write_codex_auth(auth_path, auth)?,
        None if auth_path.exists() => {
            fs::remove_file(auth_path)
                .with_context(|| format!("Failed to remove {}.", auth_path.display()))?;
        }
        None => {}
    }
    if let Some(index) = previous_active_index {
        let _ = restore_pool_active_index(index)?;
    }
    Ok(())
}

pub(crate) fn wait_for_all_threads_idle(
    port: u16,
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<()> {
    let deadline = Instant::now() + THREAD_IDLE_WAIT_TIMEOUT;
    let mut last_report_at = None::<Instant>;

    loop {
        let active_thread_ids = read_active_thread_ids(Some(port))?;
        if active_thread_ids.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if last_report_at
            .map(|last| now.saturating_duration_since(last) >= THREAD_IDLE_PROGRESS_INTERVAL)
            .unwrap_or(true)
        {
            if let Some(progress) = progress {
                progress(format!(
                    "Waiting for {} active Codex thread(s) to go idle before restart: {}",
                    active_thread_ids.len(),
                    summarize_thread_ids(&active_thread_ids),
                ));
            }
            last_report_at = Some(now);
        }

        if now >= deadline {
            return Err(anyhow!(
                "Timed out waiting for {} active Codex thread(s) to go idle before restart: {}",
                active_thread_ids.len(),
                summarize_thread_ids(&active_thread_ids),
            ));
        }

        std::thread::sleep(THREAD_IDLE_POLL_INTERVAL);
    }
}

pub fn active_managed_codex_thread_ids(port: Option<u16>) -> Result<Vec<String>> {
    let paths = resolve_paths()?;
    if !managed_codex_is_running(&paths.debug_profile_dir)? {
        return Ok(Vec::new());
    }
    read_active_thread_ids(port)
}

fn summarize_thread_ids(thread_ids: &[String]) -> String {
    const MAX_IDS: usize = 3;
    let mut preview = thread_ids.iter().take(MAX_IDS).cloned().collect::<Vec<_>>();
    if thread_ids.len() > MAX_IDS {
        preview.push(format!("+{} more", thread_ids.len() - MAX_IDS));
    }
    preview.join(", ")
}

fn log_isolation_layout(
    codex_home: &Path,
    rotate_home: &Path,
    logs_db_path: &Path,
) -> LogIsolationLayout {
    let rotate_logs_root = rotate_home.join("logs");
    let log_db_basename = logs_db_path
        .file_name()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| "logs.sqlite".to_string());
    LogIsolationLayout {
        codex_home: codex_home.to_path_buf(),
        manifest_path: rotate_logs_root.join("manifest.json"),
        by_account_root: rotate_logs_root.join("by-account"),
        legacy_root: rotate_logs_root.join("legacy"),
        transactions_root: rotate_logs_root.join("transactions"),
        log_db_basename,
    }
}

fn prepare_log_handoff(
    layout: &LogIsolationLayout,
    previous_account_id: Option<&str>,
    next_account_id: &str,
) -> Result<LogHandoffState> {
    fs::create_dir_all(&layout.by_account_root)
        .with_context(|| format!("Failed to create {}.", layout.by_account_root.display()))?;
    fs::create_dir_all(&layout.legacy_root)
        .with_context(|| format!("Failed to create {}.", layout.legacy_root.display()))?;
    fs::create_dir_all(&layout.transactions_root)
        .with_context(|| format!("Failed to create {}.", layout.transactions_root.display()))?;

    let previous_manifest_raw = read_manifest_raw(&layout.manifest_path)?;
    let shared_exists = bundle_exists(&layout.codex_home, &layout.log_db_basename);
    let manifest_exists = previous_manifest_raw.is_some();
    let next_bundle_exists = bundle_exists(
        &account_bundle_dir(layout, next_account_id),
        &layout.log_db_basename,
    );

    let mut state = LogHandoffState {
        layout: layout.clone(),
        previous_manifest_raw,
        previous_account_id: previous_account_id.map(ToOwned::to_owned),
        next_account_id: next_account_id.to_string(),
        previous_archive_backup_dir: None,
        restored_next_bundle: false,
        legacy_snapshot_dir: None,
    };

    let mut perform = || -> Result<()> {
        if !manifest_exists {
            if shared_exists {
                let legacy_dir = layout
                    .legacy_root
                    .join(format!("cutover-{}", Utc::now().timestamp_millis()));
                move_bundle(&layout.codex_home, &legacy_dir, &layout.log_db_basename)?;
                state.legacy_snapshot_dir = Some(legacy_dir);
            }
            if next_bundle_exists {
                move_bundle(
                    &account_bundle_dir(layout, next_account_id),
                    &layout.codex_home,
                    &layout.log_db_basename,
                )?;
                state.restored_next_bundle = true;
            }
        } else {
            if let Some(previous_account_id) = previous_account_id {
                let previous_bundle_dir = account_bundle_dir(layout, previous_account_id);
                if bundle_exists(&previous_bundle_dir, &layout.log_db_basename) {
                    let backup_dir = layout.transactions_root.join(format!(
                        "previous-archive-{}-{}",
                        previous_account_id,
                        Utc::now().timestamp_millis()
                    ));
                    move_bundle(&previous_bundle_dir, &backup_dir, &layout.log_db_basename)?;
                    state.previous_archive_backup_dir = Some(backup_dir);
                }
                if shared_exists {
                    move_bundle(
                        &layout.codex_home,
                        &previous_bundle_dir,
                        &layout.log_db_basename,
                    )?;
                }
            }
            if next_bundle_exists {
                move_bundle(
                    &account_bundle_dir(layout, next_account_id),
                    &layout.codex_home,
                    &layout.log_db_basename,
                )?;
                state.restored_next_bundle = true;
            }
        }

        write_manifest(
            &layout.manifest_path,
            &LogsManifest {
                version: LOGS_MANIFEST_VERSION,
                current_owner_account_id: Some(next_account_id.to_string()),
                log_db_basename: layout.log_db_basename.clone(),
            },
        )?;
        Ok(())
    };

    if let Err(error) = perform() {
        rollback_log_handoff(&state).ok();
        return Err(error);
    }

    Ok(state)
}

fn finalize_log_handoff(handoff: &LogHandoffState) -> Result<()> {
    if let Some(backup_dir) = handoff.previous_archive_backup_dir.as_ref() {
        if backup_dir.exists() {
            fs::remove_dir_all(backup_dir)
                .with_context(|| format!("Failed to remove {}.", backup_dir.display()))?;
        }
    }
    Ok(())
}

fn rollback_log_handoff(handoff: &LogHandoffState) -> Result<()> {
    restore_manifest_raw(
        &handoff.layout.manifest_path,
        handoff.previous_manifest_raw.as_deref(),
    )?;

    if handoff.restored_next_bundle
        && bundle_exists(&handoff.layout.codex_home, &handoff.layout.log_db_basename)
    {
        move_bundle(
            &handoff.layout.codex_home,
            &account_bundle_dir(&handoff.layout, &handoff.next_account_id),
            &handoff.layout.log_db_basename,
        )?;
    } else {
        remove_bundle(&handoff.layout.codex_home, &handoff.layout.log_db_basename)?;
    }

    if let Some(legacy_dir) = handoff.legacy_snapshot_dir.as_ref() {
        if bundle_exists(legacy_dir, &handoff.layout.log_db_basename) {
            move_bundle(
                legacy_dir,
                &handoff.layout.codex_home,
                &handoff.layout.log_db_basename,
            )?;
        }
        if legacy_dir.exists() && fs::read_dir(legacy_dir)?.next().is_none() {
            fs::remove_dir(legacy_dir)
                .with_context(|| format!("Failed to remove {}.", legacy_dir.display()))?;
        }
    } else if let Some(previous_account_id) = handoff.previous_account_id.as_deref() {
        let previous_bundle_dir = account_bundle_dir(&handoff.layout, previous_account_id);
        if bundle_exists(&previous_bundle_dir, &handoff.layout.log_db_basename) {
            move_bundle(
                &previous_bundle_dir,
                &handoff.layout.codex_home,
                &handoff.layout.log_db_basename,
            )?;
        }
    }

    if let (Some(previous_account_id), Some(backup_dir)) = (
        handoff.previous_account_id.as_deref(),
        handoff.previous_archive_backup_dir.as_ref(),
    ) {
        if bundle_exists(backup_dir, &handoff.layout.log_db_basename) {
            move_bundle(
                backup_dir,
                &account_bundle_dir(&handoff.layout, previous_account_id),
                &handoff.layout.log_db_basename,
            )?;
        }
        if backup_dir.exists() && fs::read_dir(backup_dir)?.next().is_none() {
            fs::remove_dir(backup_dir)
                .with_context(|| format!("Failed to remove {}.", backup_dir.display()))?;
        }
    }

    Ok(())
}

fn read_manifest_raw(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }
    fs::read_to_string(path)
        .map(Some)
        .with_context(|| format!("Failed to read {}.", path.display()))
}

fn restore_manifest_raw(path: &Path, raw: Option<&str>) -> Result<()> {
    match raw {
        Some(raw) => write_private_string(path, raw),
        None if path.exists() => {
            fs::remove_file(path).with_context(|| format!("Failed to remove {}.", path.display()))
        }
        None => Ok(()),
    }
}

fn write_manifest(path: &Path, manifest: &LogsManifest) -> Result<()> {
    write_private_string(path, &serde_json::to_string_pretty(manifest)?)
}

fn account_bundle_dir(layout: &LogIsolationLayout, account_id: &str) -> PathBuf {
    layout.by_account_root.join(account_id)
}

fn bundle_exists(parent: &Path, basename: &str) -> bool {
    bundle_file_paths(parent, basename)
        .into_iter()
        .any(|path| path.exists())
}

fn bundle_file_paths(parent: &Path, basename: &str) -> [PathBuf; 3] {
    [
        parent.join(basename),
        parent.join(format!("{basename}-wal")),
        parent.join(format!("{basename}-shm")),
    ]
}

fn move_bundle(from_parent: &Path, to_parent: &Path, basename: &str) -> Result<()> {
    let existing_paths = bundle_file_paths(from_parent, basename)
        .into_iter()
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    if existing_paths.is_empty() {
        return Ok(());
    }

    fs::create_dir_all(to_parent)
        .with_context(|| format!("Failed to create {}.", to_parent.display()))?;
    remove_bundle(to_parent, basename)?;

    for source in existing_paths {
        let file_name = source
            .file_name()
            .map(OsString::from)
            .ok_or_else(|| anyhow!("Missing filename for {}.", source.display()))?;
        let destination = to_parent.join(file_name);
        rename_or_copy(&source, &destination)?;
    }

    Ok(())
}

fn remove_bundle(parent: &Path, basename: &str) -> Result<()> {
    for path in bundle_file_paths(parent, basename) {
        if path.exists() {
            fs::remove_file(&path)
                .with_context(|| format!("Failed to remove {}.", path.display()))?;
        }
    }
    Ok(())
}

fn rename_or_copy(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(rename_error) => {
            fs::copy(from, to).with_context(|| {
                format!(
                    "Failed to copy {} after rename fallback ({rename_error}).",
                    from.display()
                )
            })?;
            fs::remove_file(from).with_context(|| format!("Failed to remove {}.", from.display()))
        }
    }
}

fn read_auth_if_exists(path: &Path) -> Result<Option<CodexAuth>> {
    if !path.exists() {
        return Ok(None);
    }
    load_codex_auth(path).map(Some)
}

pub fn managed_codex_is_running(profile_dir: &Path) -> Result<bool> {
    Ok(!managed_codex_root_pids(profile_dir)?.is_empty())
}

pub fn managed_codex_root_pids(profile_dir: &Path) -> Result<Vec<u32>> {
    let profile_marker = format!("--user-data-dir={}", profile_dir.display());
    Ok(list_processes()?
        .into_iter()
        .filter(|process| {
            process
                .command
                .contains("/Applications/Codex.app/Contents/MacOS/Codex")
                && process.command.contains(&profile_marker)
        })
        .map(|process| process.pid)
        .collect())
}

pub fn stop_managed_codex_instance(port: u16, profile_dir: &Path) -> Result<()> {
    invalidate_local_codex_connection(port, true);
    let root_pids = managed_codex_root_pids(profile_dir)?;
    if root_pids.is_empty() {
        return Ok(());
    }
    log_daemon_info(format!(
        "Stopping managed Codex for profile {} on port {} (pids: {}).",
        profile_dir.display(),
        port,
        root_pids
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    ));

    signal_processes("TERM", &root_pids)?;
    if !wait_for_processes_to_exit(&root_pids, PROCESS_STOP_TIMEOUT)? {
        signal_processes("KILL", &root_pids)?;
        if !wait_for_processes_to_exit(&root_pids, PROCESS_STOP_TIMEOUT)? {
            return Err(anyhow!(
                "Managed Codex did not stop after signalling pids {}.",
                root_pids
                    .iter()
                    .map(u32::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    }
    Ok(())
}

fn signal_processes(signal: &str, pids: &[u32]) -> Result<()> {
    if pids.is_empty() {
        return Ok(());
    }
    let mut command = Command::new("kill");
    command.arg(format!("-{signal}"));
    for pid in pids {
        command.arg(pid.to_string());
    }
    let output = command.output().context("Failed to invoke kill.")?;
    if output.status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "Failed to signal pids {} with {signal}: {}",
        pids.iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        String::from_utf8_lossy(&output.stderr).trim()
    ))
}

fn wait_for_processes_to_exit(pids: &[u32], timeout: Duration) -> Result<bool> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let running = list_processes()?
            .into_iter()
            .any(|process| pids.contains(&process.pid));
        if !running {
            return Ok(true);
        }
        std::thread::sleep(PROCESS_STOP_POLL_INTERVAL);
    }
    Ok(false)
}

fn ensure_bundle_is_released(layout: &LogIsolationLayout) -> Result<()> {
    let existing_paths = bundle_file_paths(&layout.codex_home, &layout.log_db_basename)
        .into_iter()
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    let holders = list_open_bundle_holders(&existing_paths)?;
    if holders.is_empty() {
        return Ok(());
    }
    Err(anyhow!(format_holder_blocker(&holders)))
}

fn list_open_bundle_holders(paths: &[PathBuf]) -> Result<Vec<OpenHolder>> {
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut command = Command::new("lsof");
    command.arg("-Fpc");
    command.arg("--");
    for path in paths {
        command.arg(path);
    }
    let output = command
        .output()
        .context("Failed to query open Codex log files with lsof.")?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() && stdout.trim().is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.code() == Some(1) || stderr.contains("No such file or directory") {
            return Ok(Vec::new());
        }
        return Err(anyhow!(
            "Failed to query open Codex log files with lsof: {}",
            stderr.trim()
        ));
    }

    let mut holders = BTreeMap::<u32, OpenHolder>::new();
    let mut current_pid = None::<u32>;
    for line in stdout.lines() {
        if let Some(pid) = line.strip_prefix('p') {
            current_pid = pid.parse::<u32>().ok();
            if let Some(pid) = current_pid {
                holders.entry(pid).or_insert_with(|| OpenHolder {
                    pid,
                    command: "unknown".to_string(),
                });
            }
            continue;
        }
        if let Some(command) = line.strip_prefix('c') {
            if let Some(pid) = current_pid {
                holders
                    .entry(pid)
                    .and_modify(|holder| holder.command = command.to_string());
            }
        }
    }

    Ok(holders.into_values().collect())
}

fn format_holder_blocker(holders: &[OpenHolder]) -> String {
    format!(
        "Codex log isolation is blocked because the shared logs database is still open: {}",
        holders
            .iter()
            .map(|holder| format!("pid {} ({})", holder.pid, holder.command))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

fn list_processes() -> Result<Vec<ProcessInfo>> {
    let output = Command::new("ps")
        .args(["-axo", "pid=,command="])
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
        .filter_map(|line| {
            let trimmed = line.trim_start();
            let (pid, command) = trimmed.split_once(' ')?;
            let pid = pid.trim().parse::<u32>().ok()?;
            let command = command.trim();
            if command.is_empty() {
                return None;
            }
            Some(ProcessInfo {
                pid,
                command: command.to_string(),
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let suffix = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), suffix))
    }

    fn layout(root: &Path, basename: &str) -> LogIsolationLayout {
        log_isolation_layout(
            &root.join("codex"),
            &root.join("rotate"),
            &root.join("codex").join(basename),
        )
    }

    fn write_bundle(parent: &Path, basename: &str, with_wal: bool, with_shm: bool) {
        fs::create_dir_all(parent).expect("create bundle parent");
        fs::write(parent.join(basename), b"db").expect("write db");
        if with_wal {
            fs::write(parent.join(format!("{basename}-wal")), b"wal").expect("write wal");
        }
        if with_shm {
            fs::write(parent.join(format!("{basename}-shm")), b"shm").expect("write shm");
        }
    }

    fn assert_bundle(parent: &Path, basename: &str, db: bool, wal: bool, shm: bool) {
        assert_eq!(parent.join(basename).exists(), db, "db presence mismatch");
        assert_eq!(
            parent.join(format!("{basename}-wal")).exists(),
            wal,
            "wal presence mismatch"
        );
        assert_eq!(
            parent.join(format!("{basename}-shm")).exists(),
            shm,
            "shm presence mismatch"
        );
    }

    #[test]
    fn first_cutover_moves_shared_bundle_to_legacy() {
        let root = unique_temp_dir("codex-rotate-log-cutover");
        let layout = layout(&root, "logs_2.sqlite");
        write_bundle(&layout.codex_home, &layout.log_db_basename, true, true);

        let handoff = prepare_log_handoff(&layout, Some("acct-a"), "acct-b").expect("handoff");

        assert!(handoff.legacy_snapshot_dir.is_some());
        assert_bundle(
            &layout.codex_home,
            &layout.log_db_basename,
            false,
            false,
            false,
        );
        let legacy_dir = handoff
            .legacy_snapshot_dir
            .as_ref()
            .expect("legacy snapshot dir");
        assert_bundle(legacy_dir, &layout.log_db_basename, true, true, true);

        finalize_log_handoff(&handoff).expect("finalize");
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn handoff_round_trips_between_accounts() {
        let root = unique_temp_dir("codex-rotate-log-roundtrip");
        let layout = layout(&root, "logs_2.sqlite");
        write_bundle(&layout.codex_home, &layout.log_db_basename, true, true);
        write_manifest(
            &layout.manifest_path,
            &LogsManifest {
                version: LOGS_MANIFEST_VERSION,
                current_owner_account_id: Some("acct-a".to_string()),
                log_db_basename: layout.log_db_basename.clone(),
            },
        )
        .expect("write manifest");
        write_bundle(
            &account_bundle_dir(&layout, "acct-b"),
            &layout.log_db_basename,
            true,
            false,
        );

        let handoff = prepare_log_handoff(&layout, Some("acct-a"), "acct-b").expect("handoff");

        assert_bundle(
            &account_bundle_dir(&layout, "acct-a"),
            &layout.log_db_basename,
            true,
            true,
            true,
        );
        assert_bundle(
            &layout.codex_home,
            &layout.log_db_basename,
            true,
            true,
            false,
        );

        rollback_log_handoff(&handoff).expect("rollback");

        assert_bundle(
            &layout.codex_home,
            &layout.log_db_basename,
            true,
            true,
            true,
        );
        assert_bundle(
            &account_bundle_dir(&layout, "acct-b"),
            &layout.log_db_basename,
            true,
            true,
            false,
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn handoff_preserves_missing_sidecars() {
        let root = unique_temp_dir("codex-rotate-log-sidecars");
        let layout = layout(&root, "logs_2.sqlite");
        write_bundle(&layout.codex_home, &layout.log_db_basename, false, false);
        write_manifest(
            &layout.manifest_path,
            &LogsManifest {
                version: LOGS_MANIFEST_VERSION,
                current_owner_account_id: Some("acct-a".to_string()),
                log_db_basename: layout.log_db_basename.clone(),
            },
        )
        .expect("write manifest");

        let handoff = prepare_log_handoff(&layout, Some("acct-a"), "acct-b").expect("handoff");

        assert_bundle(
            &account_bundle_dir(&layout, "acct-a"),
            &layout.log_db_basename,
            true,
            false,
            false,
        );
        assert_bundle(
            &layout.codex_home,
            &layout.log_db_basename,
            false,
            false,
            false,
        );

        rollback_log_handoff(&handoff).expect("rollback");
        assert_bundle(
            &layout.codex_home,
            &layout.log_db_basename,
            true,
            false,
            false,
        );

        fs::remove_dir_all(&root).ok();
    }
}
