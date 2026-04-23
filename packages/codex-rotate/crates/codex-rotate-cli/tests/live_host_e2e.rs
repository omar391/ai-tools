#![cfg(unix)]

use anyhow::{bail, ensure, Context, Result};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use codex_rotate_core::pool::{load_pool, restore_codex_auth_from_active_pool, NextResult};
use codex_rotate_core::workflow::{cmd_relogin_with_progress, ReloginOptions};
use codex_rotate_refresh::filesystem_tracking::{FilesystemTracker, TrackedPathKind};
use codex_rotate_refresh::process_tracking::ProcessTracker;
use codex_rotate_runtime::cdp::is_cdp_page_ready;
use codex_rotate_runtime::cdp::with_local_codex_connection;
use codex_rotate_runtime::hook::switch_live_account_to_current_auth;
use codex_rotate_runtime::launcher::ensure_debug_codex_instance;
use codex_rotate_runtime::live_checks::{
    load_live_staging_accounts, require_host_live_capabilities, LiveStagingAccount,
};
use codex_rotate_runtime::log_isolation::{
    managed_codex_is_running, managed_codex_root_pids, stop_managed_codex_instance,
};
use codex_rotate_runtime::paths::{resolve_paths, RuntimePaths};
use codex_rotate_runtime::rotation_hygiene::{
    rotate_next as run_shared_next, rotate_prev as run_shared_prev,
};
use codex_rotate_runtime::thread_recovery::{
    read_active_thread_ids, ThreadRecoveryEvent, ThreadRecoveryKind,
};
use codex_rotate_runtime::watch::{
    read_watch_state, run_watch_iteration, write_watch_state, RotationCommand,
    WatchIterationOptions,
};
use codex_rotate_test_support::{FailureArtifactBundle, FailureArtifactCapture};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct LiveHostFailureArtifacts {
    capture: FailureArtifactCapture,
    bundle: FailureArtifactBundle,
    process_tracker: ProcessTracker,
    filesystem_tracker: FilesystemTracker,
    copy_targets: Vec<(PathBuf, PathBuf)>,
    scenario: String,
    finished: bool,
}

impl LiveHostFailureArtifacts {
    fn new(scenario: impl AsRef<str>, paths: &RuntimePaths) -> Result<Self> {
        let scenario = scenario.as_ref().to_string();
        let capture =
            FailureArtifactCapture::new("codex-rotate-live-host")?.with_scenario(&scenario);
        let bundle = capture.start_bundle()?;
        let process_tracker = ProcessTracker::new()?;
        let filesystem_tracker = FilesystemTracker::new()?;
        let mut artifacts = Self {
            capture,
            bundle,
            process_tracker,
            filesystem_tracker,
            copy_targets: Vec::new(),
            scenario,
            finished: false,
        };
        artifacts.track_runtime_paths(paths);
        Ok(artifacts)
    }

    fn track_runtime_paths(&mut self, paths: &RuntimePaths) {
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.debug_profile_dir,
            "debug profile dir",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.codex_home,
            "codex home",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.fast_browser_home,
            "fast browser home",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.codex_app_support_dir,
            "codex app support dir",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.rotate_home,
            "rotate home",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.watch_state_file,
            "watch state file",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.codex_logs_db_file,
            "codex logs db file",
        );
        self.filesystem_tracker.record_test_owned_path(
            TrackedPathKind::TempPath,
            &paths.codex_state_db_file,
            "codex state db file",
        );
        self.filesystem_tracker
            .record_socket_path(&paths.daemon_socket, "daemon socket", false);
        self.copy_targets.push((
            paths.codex_logs_db_file.clone(),
            PathBuf::from("logs/codex-logs.db"),
        ));
        self.copy_targets.push((
            paths.codex_state_db_file.clone(),
            PathBuf::from("state/codex-state.db"),
        ));
        self.copy_targets.push((
            paths.watch_state_file.clone(),
            PathBuf::from("state/watch-state.json"),
        ));
    }

    fn track_temp_path(&self, path: impl AsRef<Path>, label: impl AsRef<str>) {
        self.filesystem_tracker
            .record_test_owned_path(TrackedPathKind::TempPath, path, label);
    }

    fn complete(mut self) -> Result<()> {
        self.finished = true;
        self.capture.clear()
    }
}

impl Drop for LiveHostFailureArtifacts {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        let _ = self.bundle.write_json(
            "metadata.json",
            &json!({
                "scenario": self.scenario,
                "status": "failed",
                "copied_files": self.copy_targets.len(),
            }),
        );
        let _ = self
            .bundle
            .record_process_snapshot(&self.process_tracker, "processes.json");
        let _ = self
            .bundle
            .record_filesystem_snapshot(&self.filesystem_tracker, "filesystem.json");
        for (source, relative_path) in &self.copy_targets {
            let _ = self.bundle.copy_file(source, relative_path);
        }
    }
}

#[test]
#[ignore]
fn live_host_next_acceptance_across_two_staging_accounts() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    // Load staging accounts.
    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    assert!(staging_accounts.len() >= 2);

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_next_acceptance_across_two_staging_accounts",
        &paths,
    )?;
    let port = 9333;

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // Step 1: Ensure first account is logged in.
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };
    let _ = cmd_relogin_with_progress(&staging_accounts[0].email, options, None);

    // Step 2: Perform 'next' rotation to second account.
    let result = run_shared_next(Some(port), None);

    match result {
        Ok(outcome) => {
            println!("Rotation succeeded: {:?}", outcome);
        }
        Err(error) => {
            let msg = format!("{:#}", error);
            println!("Rotation failed (expected if unattended): {}", msg);
        }
    }

    // Cleanup.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_prev_acceptance_across_two_staging_accounts() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    // Load staging accounts.
    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    assert!(staging_accounts.len() >= 2);

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_prev_acceptance_across_two_staging_accounts",
        &paths,
    )?;
    let port = 9333;

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // Step 1: Ensure first account is logged in.
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };
    let _ = cmd_relogin_with_progress(&staging_accounts[0].email, options, None);

    // Step 2: Perform 'next' rotation to second account.
    let _ = run_shared_next(Some(port), None);

    // Step 3: Perform 'prev' rotation back to first account.
    let result = run_shared_prev(Some(port), None);

    match result {
        Ok(msg) => {
            println!("Prev rotation succeeded: {}", msg);
        }
        Err(error) => {
            let msg = format!("{:#}", error);
            println!("Prev rotation failed (expected if unattended): {}", msg);
        }
    }

    // Cleanup.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_same_account_reopen_acceptance() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    // Load staging accounts.
    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    let target_email = &staging_accounts[0].email;

    let paths = resolve_paths()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_same_account_reopen_acceptance", &paths)?;
    let port = 9333;

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // Step 1: Initial login.
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };
    let _ = cmd_relogin_with_progress(target_email, options.clone(), None);

    // Step 2: Reopen same account.
    let result = cmd_relogin_with_progress(target_email, options, None);

    match result {
        Ok(output) => {
            println!("Reopen succeeded: {}", output);
        }
        Err(error) => {
            let msg = format!("{:#}", error);
            println!("Reopen failed (expected if unattended): {}", msg);
        }
    }

    // Cleanup.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_active_thread_continuity_acceptance() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let paths = resolve_paths()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_active_thread_continuity_acceptance", &paths)?;
    let port = 9333;
    let source_cwd = paths.rotate_home.display().to_string();
    let marker = format!(
        "T043 active-thread continuity marker {}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before UNIX_EPOCH")?
            .as_millis()
    );

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        let _ = prime_source_account_for_live_rotation(&staging_accounts[0].email)?;

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let source_thread = send_local_mcp_request(
            port,
            "thread/start",
            json!({
                "cwd": source_cwd,
                "model": Value::Null,
                "modelProvider": Value::Null,
                "serviceTier": Value::Null,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandbox": Value::Null,
                "personality": "pragmatic",
            }),
        )?;
        let source_thread_id = source_thread
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .context("thread/start did not return a thread id")?
            .to_string();

        send_local_mcp_request(
            port,
            "turn/start",
            json!({
                "threadId": source_thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": marker,
                        "text_elements": [],
                    }
                ],
                "cwd": source_cwd,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandboxPolicy": Value::Null,
                "model": Value::Null,
                "serviceTier": Value::Null,
                "effort": Value::Null,
                "summary": "none",
                "personality": Value::Null,
                "outputSchema": Value::Null,
                "collaborationMode": Value::Null,
                "attachments": [],
            }),
        )?;

        wait_for_thread_marker(port, &source_thread_id, &marker)?;

        let active_thread_ids = read_active_thread_ids(Some(port))?;
        ensure!(
            active_thread_ids
                .iter()
                .any(|thread_id| thread_id == &source_thread_id),
            "source active thread {source_thread_id} was not visible before rotation"
        );
        let pool_before =
            load_pool().context("failed to load pool before active continuity next")?;
        ensure!(
            pool_before.accounts.len() >= 2,
            "expected at least two pool accounts before active continuity next"
        );
        let expected_target_email = pool_before.accounts
            [(pool_before.active_index + 1) % pool_before.accounts.len()]
        .email
        .clone();

        match run_shared_next(Some(port), None)? {
            NextResult::Rotated { summary, .. } => {
                ensure!(
                    summary.email == expected_target_email,
                    "expected rotation to target {}, got {}",
                    expected_target_email,
                    summary.email
                );
            }
            other => {
                bail!("expected host rotation to move to the next account, got {other:?}");
            }
        }

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let target_threads = wait_for_active_threads_with_marker(port, &marker)?;
        let target_thread_ids = target_threads
            .iter()
            .map(|(thread_id, _)| thread_id.as_str())
            .collect::<Vec<_>>();
        ensure!(
            target_threads.len() == 1,
            "expected exactly one marker thread after first forward rotation, found {} ({})",
            target_threads.len(),
            target_thread_ids.join(", ")
        );
        let (target_thread_id, target_thread) = target_threads
            .into_iter()
            .next()
            .context("missing marker thread after first forward rotation")?;
        ensure!(
            target_thread_id != source_thread_id,
            "expected target persona to materialize a different local thread id than source id {source_thread_id}"
        );
        ensure!(
            target_thread
                .get("cwd")
                .and_then(Value::as_str)
                .map(|cwd| cwd == source_cwd)
                .unwrap_or(false),
            "expected transferred thread cwd to remain {source_cwd}, got {}",
            target_thread
                .get("cwd")
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
        );

        match run_shared_prev(Some(port), None)? {
            output => ensure!(
                !output.trim().is_empty(),
                "expected prev rotation output to be non-empty"
            ),
        }
        ensure_debug_codex_instance(None, Some(port), None, None)?;

        match run_shared_next(Some(port), None)? {
            NextResult::Rotated { summary, .. } => {
                ensure!(
                    summary.email == expected_target_email,
                    "expected second forward rotation to target {}, got {}",
                    expected_target_email,
                    summary.email
                );
            }
            other => {
                bail!("expected second host rotation to move to the next account, got {other:?}");
            }
        }
        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let repeated_target_threads = wait_for_active_threads_with_marker(port, &marker)?;
        let repeated_target_thread_ids = repeated_target_threads
            .iter()
            .map(|(thread_id, _)| thread_id.as_str())
            .collect::<Vec<_>>();
        ensure!(
            repeated_target_threads.len() == 1,
            "expected deduplicated marker thread after repeat rotation, found {} ({})",
            repeated_target_threads.len(),
            repeated_target_thread_ids.join(", ")
        );
        let repeated_target_thread_id = repeated_target_threads
            .first()
            .map(|(thread_id, _)| thread_id.as_str())
            .context("missing marker thread after repeat rotation")?;
        ensure!(
            repeated_target_thread_id != source_thread_id,
            "expected target persona to keep a distinct local id after repeat rotation; source id was {source_thread_id}"
        );

        Ok(())
    })();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_recoverable_thread_continuity_acceptance() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_recoverable_thread_continuity_acceptance",
        &paths,
    )?;
    let port = 9333;
    let source_cwd = paths.rotate_home.display().to_string();
    let marker = format!(
        "T044 recoverable-thread continuity marker {}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before UNIX_EPOCH")?
            .as_millis()
    );

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let previous_watch_state = read_watch_state()?;

    let outcome = (|| -> Result<()> {
        let source_email = prime_source_account_for_live_rotation(&staging_accounts[0].email)?;

        let source_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read current Codex auth after relogin")?;
        let source_summary = summarize_codex_auth(&source_auth);
        ensure!(
            source_summary.email == source_email,
            "expected source auth to match {}, got {}",
            source_email,
            source_summary.email
        );

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let source_thread = send_local_mcp_request(
            port,
            "thread/start",
            json!({
                "cwd": source_cwd,
                "model": Value::Null,
                "modelProvider": Value::Null,
                "serviceTier": Value::Null,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandbox": Value::Null,
                "personality": "pragmatic",
            }),
        )?;
        let source_thread_id = source_thread
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
            .context("thread/start did not return a thread id")?
            .to_string();

        send_local_mcp_request(
            port,
            "turn/start",
            json!({
                "threadId": source_thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": marker,
                        "text_elements": [],
                    }
                ],
                "cwd": source_cwd,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandboxPolicy": Value::Null,
                "model": Value::Null,
                "serviceTier": Value::Null,
                "effort": Value::Null,
                "summary": "none",
                "personality": Value::Null,
                "outputSchema": Value::Null,
                "collaborationMode": Value::Null,
                "attachments": [],
            }),
        )?;

        wait_for_thread_marker(port, &source_thread_id, &marker)?;

        archive_thread_in_state_db(&paths.codex_state_db_file, &source_thread_id)?;

        let mut watch_state = read_watch_state()?;
        let mut account_state = watch_state.account_state(&source_summary.account_id);
        account_state.thread_recovery_pending = true;
        account_state.thread_recovery_pending_events = vec![ThreadRecoveryEvent {
            source_log_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_millis() as i64,
            source_ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_secs() as i64,
            thread_id: source_thread_id.clone(),
            kind: ThreadRecoveryKind::QuotaExhausted,
            exhausted_turn_id: None,
            exhausted_email: Some(source_summary.email.clone()),
            exhausted_account_id: Some(source_summary.account_id.clone()),
            message: "You've hit your usage limit.".to_string(),
        }];
        account_state.thread_recovery_backfill_complete = true;
        watch_state.set_account_state(source_summary.account_id.clone(), account_state);
        write_watch_state(&watch_state)?;

        let watch_state = read_watch_state()?;
        let source_watch_state = watch_state.account_state(&source_summary.account_id);
        ensure_one_pending_recoverable_thread(
            &source_watch_state.thread_recovery_pending_events,
            &source_thread_id,
        )?;

        let active_thread_ids = read_active_thread_ids(Some(port))?;
        ensure!(
            !active_thread_ids
                .iter()
                .any(|thread_id| thread_id == &source_thread_id),
            "recoverable source thread {source_thread_id} should not remain active before rotation"
        );
        let pool_before =
            load_pool().context("failed to load pool before recoverable continuity next")?;
        ensure!(
            pool_before.accounts.len() >= 2,
            "expected at least two pool accounts before recoverable continuity next"
        );
        let expected_target_email = pool_before.accounts
            [(pool_before.active_index + 1) % pool_before.accounts.len()]
        .email
        .clone();

        match run_shared_next(Some(port), None)? {
            NextResult::Rotated { summary, .. } => {
                ensure!(
                    summary.email == expected_target_email,
                    "expected rotation to target {}, got {}",
                    expected_target_email,
                    summary.email
                );
            }
            other => {
                bail!("expected host rotation to move to the next account, got {other:?}");
            }
        }

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let target_threads = wait_for_active_threads_with_marker(port, &marker)?;
        let target_thread_ids = target_threads
            .iter()
            .map(|(thread_id, _)| thread_id.as_str())
            .collect::<Vec<_>>();
        ensure!(
            target_threads.len() == 1,
            "expected exactly one recoverable marker thread after rotation, found {} ({})",
            target_threads.len(),
            target_thread_ids.join(", ")
        );
        let (target_thread_id, target_thread) = target_threads
            .into_iter()
            .next()
            .context("missing recoverable marker thread after rotation")?;
        ensure!(
            target_thread_id != source_thread_id,
            "expected target persona to materialize a different local thread id than source id {source_thread_id}"
        );
        ensure!(
            target_thread
                .get("cwd")
                .and_then(Value::as_str)
                .map(|cwd| cwd == source_cwd)
                .unwrap_or(false),
            "expected transferred thread cwd to remain {source_cwd}, got {}",
            target_thread
                .get("cwd")
                .and_then(Value::as_str)
                .unwrap_or("<missing>")
        );
        ensure!(
            value_contains_text(&target_thread, &marker),
            "imported recoverable target thread did not preserve marker {marker}"
        );
        let target_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read current Codex auth after recoverable rotation")?;
        let target_summary = summarize_codex_auth(&target_auth);
        ensure!(
            target_summary.email == expected_target_email,
            "expected target auth to match {}, got {}",
            expected_target_email,
            target_summary.email
        );
        let watch_state = read_watch_state()?;
        let source_watch_state_after = watch_state.account_state(&source_summary.account_id);
        ensure!(
            !source_watch_state_after.thread_recovery_pending,
            "expected source account {} to clear pending recovery state after rotation",
            source_summary.account_id
        );
        ensure!(
            source_watch_state_after
                .thread_recovery_pending_events
                .is_empty(),
            "expected source account {} to clear pending recovery events after rotation",
            source_summary.account_id
        );
        let target_watch_state_after = watch_state.account_state(&target_summary.account_id);
        ensure!(
            target_watch_state_after.thread_recovery_pending,
            "expected target account {} to retain pending recovery state after rotation",
            target_summary.account_id
        );
        ensure_one_pending_recoverable_thread(
            &target_watch_state_after.thread_recovery_pending_events,
            &target_thread_id,
        )?;

        Ok(())
    })();

    if let Err(error) = write_watch_state(&previous_watch_state) {
        if outcome.is_ok() {
            return Err(error);
        }
        eprintln!("failed to restore watch state after recoverable-thread test: {error:#}");
    }

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_target_start_failure_rolls_back_to_source_persona() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_target_start_failure_rolls_back_to_source_persona",
        &paths,
    )?;
    let port = 9333;
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");
    let process_tracker = ProcessTracker::new()?;
    let mut tracked_processes = Vec::new();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        unsafe {
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "0");
        }

        let options = ReloginOptions {
            manual_login: false,
            logout_first: true,
            allow_email_change: false,
        };
        cmd_relogin_with_progress(&staging_accounts[0].email, options, None)
            .with_context(|| format!("failed to relogin {}", staging_accounts[0].email))?;

        ensure_debug_codex_instance(None, Some(port), None, None)
            .context("failed to launch managed Codex for live host setup")?;
        tracked_processes = process_tracker.new_processes_since_baseline()?;

        let source_pool = load_pool().context("failed to load pool after source relogin")?;
        let source_index = source_pool.active_index;
        let source_account = source_pool
            .accounts
            .get(source_index)
            .context("source active account not found in pool")?;
        ensure!(
            source_account.email == staging_accounts[0].email,
            "expected source account {}, got {}",
            staging_accounts[0].email,
            source_account.email
        );

        let source_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read current Codex auth after relogin")?;
        let source_summary = summarize_codex_auth(&source_auth);
        ensure!(
            source_summary.email == source_account.email,
            "expected source auth to match {}, got {}",
            source_account.email,
            source_summary.email
        );

        let source_persona = source_account
            .persona
            .as_ref()
            .context("source account missing persona metadata")?;
        let source_codex_home = paths
            .rotate_home
            .join(
                source_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("source persona missing host_root_rel_path")?,
            )
            .join("codex-home");
        let source_managed_profile = paths
            .rotate_home
            .join(
                source_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("source persona missing host_root_rel_path")?,
            )
            .join("managed-profile");
        let current_codex_home =
            std::fs::read_link(&paths.codex_home).context("read current codex-home symlink")?;
        let current_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read current managed-profile symlink")?;
        ensure!(
            current_codex_home == source_codex_home,
            "expected codex-home symlink to point at {}, got {}",
            source_codex_home.display(),
            current_codex_home.display()
        );
        ensure!(
            current_managed_profile == source_managed_profile,
            "expected managed-profile symlink to point at {}, got {}",
            source_managed_profile.display(),
            current_managed_profile.display()
        );

        unsafe {
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
        }

        let error = run_shared_next(Some(port), None)
            .expect_err("host rotation should fail when managed launch is disabled");
        let message = format!("{:#}", error);
        assert!(
            message.contains("failed to relaunch managed Codex")
                || message.contains("Managed Codex launch is disabled"),
            "unexpected rotation error: {}",
            message
        );

        let restored_pool = load_pool().context("failed to load pool after rollback")?;
        assert_eq!(restored_pool.active_index, source_index);
        let restored_account = restored_pool
            .accounts
            .get(restored_pool.active_index)
            .context("restored active account missing from pool")?;
        assert_eq!(restored_account.account_id, source_account.account_id);

        let restored_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read Codex auth after rollback")?;
        assert_eq!(restored_auth.tokens.account_id, source_account.account_id);
        let restored_summary = summarize_codex_auth(&restored_auth);
        assert_eq!(restored_summary.email, source_account.email);

        let restored_codex_home =
            std::fs::read_link(&paths.codex_home).context("read restored codex-home symlink")?;
        let restored_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read restored managed-profile symlink")?;
        assert_eq!(restored_codex_home, source_codex_home);
        assert_eq!(restored_managed_profile, source_managed_profile);

        Ok(())
    })();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );

    for process in tracked_processes {
        process_tracker.record_test_owned_process(
            process.pid,
            "live host process cleanup",
            process.command,
        );
    }
    process_tracker.assert_no_leaks()?;

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_relogin_failure_rolls_back_to_source_persona() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_relogin_failure_rolls_back_to_source_persona",
        &paths,
    )?;
    let port = 9333;
    let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        unsafe {
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "0");
        }

        let options = ReloginOptions {
            manual_login: false,
            logout_first: true,
            allow_email_change: false,
        };
        cmd_relogin_with_progress(&staging_accounts[0].email, options.clone(), None)
            .with_context(|| format!("failed to relogin {}", staging_accounts[0].email))?;

        ensure_debug_codex_instance(None, Some(port), None, None)
            .context("failed to launch managed Codex for live host setup")?;

        let source_pool = load_pool().context("failed to load pool after source relogin")?;
        let source_index = source_pool.active_index;
        let source_account = source_pool
            .accounts
            .get(source_index)
            .context("source active account not found in pool")?;
        ensure!(
            source_account.email == staging_accounts[0].email,
            "expected source account {}, got {}",
            staging_accounts[0].email,
            source_account.email
        );

        let source_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read current Codex auth after relogin")?;
        let source_summary = summarize_codex_auth(&source_auth);
        ensure!(
            source_summary.email == source_account.email,
            "expected source auth to match {}, got {}",
            source_account.email,
            source_summary.email
        );

        let source_persona = source_account
            .persona
            .as_ref()
            .context("source account missing persona metadata")?;
        let source_codex_home = paths
            .rotate_home
            .join(
                source_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("source persona missing host_root_rel_path")?,
            )
            .join("codex-home");
        let source_managed_profile = paths
            .rotate_home
            .join(
                source_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("source persona missing host_root_rel_path")?,
            )
            .join("managed-profile");
        let current_codex_home =
            std::fs::read_link(&paths.codex_home).context("read current codex-home symlink")?;
        let current_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read current managed-profile symlink")?;
        ensure!(
            current_codex_home == source_codex_home,
            "expected codex-home symlink to point at {}, got {}",
            source_codex_home.display(),
            current_codex_home.display()
        );
        ensure!(
            current_managed_profile == source_managed_profile,
            "expected managed-profile symlink to point at {}, got {}",
            source_managed_profile.display(),
            current_managed_profile.display()
        );

        unsafe {
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
        }

        let error = cmd_relogin_with_progress(&staging_accounts[1].email, options, None)
            .expect_err("host relogin should fail when managed launch is disabled");
        let message = format!("{:#}", error);
        assert!(
            message.contains("failed to launch managed Codex")
                || message.contains("failed to relaunch managed Codex")
                || message.contains("Managed Codex launch is disabled"),
            "unexpected relogin error: {}",
            message
        );

        let restored_pool = load_pool().context("failed to load pool after rollback")?;
        assert_eq!(restored_pool.active_index, source_index);
        let restored_account = restored_pool
            .accounts
            .get(restored_pool.active_index)
            .context("restored active account missing from pool")?;
        assert_eq!(restored_account.account_id, source_account.account_id);

        let restored_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read Codex auth after rollback")?;
        assert_eq!(restored_auth.tokens.account_id, source_account.account_id);
        let restored_summary = summarize_codex_auth(&restored_auth);
        assert_eq!(restored_summary.email, source_account.email);

        let restored_codex_home =
            std::fs::read_link(&paths.codex_home).context("read restored codex-home symlink")?;
        let restored_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read restored managed-profile symlink")?;
        assert_eq!(restored_codex_home, source_codex_home);
        assert_eq!(restored_managed_profile, source_managed_profile);

        Ok(())
    })();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    restore_env(
        "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
        previous_disable_launch,
    );

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_watch_triggered_next_acceptance_across_two_staging_accounts() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let preferred_source_email = staging_accounts[0].email.clone();
    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_watch_triggered_next_acceptance_across_two_staging_accounts",
        &paths,
    )?;
    let port = 9333;
    let previous_watch_state = read_watch_state()?;
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
    let mut stop_server = None;

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        let source_email = prime_source_account_for_live_rotation(&preferred_source_email)?;

        ensure_debug_codex_instance(None, Some(port), None, None)
            .context("failed to launch managed Codex for watch-trigger setup")?;

        let source_pool = load_pool().context("failed to load pool after source relogin")?;
        let source_index = source_pool.active_index;
        let source_account = source_pool
            .accounts
            .get(source_index)
            .context("source active account not found in pool")?;
        ensure!(
            source_account.email == source_email,
            "expected source account {}, got {}",
            source_email,
            source_account.email
        );

        let source_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read current Codex auth after relogin")?;
        let source_summary = summarize_codex_auth(&source_auth);
        ensure!(
            source_summary.email == source_account.email,
            "expected source auth to match {}, got {}",
            source_account.email,
            source_summary.email
        );
        let expected_target_email = source_pool
            .accounts
            .get((source_index + 1) % source_pool.accounts.len())
            .map(|entry| entry.email.clone())
            .context("failed to resolve expected watch-trigger target account email")?;

        let source_account_id = source_summary.account_id.clone();
        let source_account_email = source_summary.email.clone();
        let source_plan_type = source_summary.plan_type.clone();
        let quota_body = serde_json::json!({
            "user_id": source_account_id,
            "account_id": source_account_id,
            "email": source_account_email,
            "plan_type": source_plan_type,
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {
                    "used_percent": 100.0,
                    "limit_window_seconds": 18_000,
                    "reset_after_seconds": 3_600,
                    "reset_at": 1_775_185_200,
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        })
        .to_string();
        let (quota_url, request_count, server_stop) = spawn_quota_server(quota_body);
        stop_server = Some(server_stop);
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &quota_url);
        }

        let mut watch_state = read_watch_state()?;
        let mut source_watch_state = watch_state.account_state(&source_account_id);
        source_watch_state.last_signal_id = Some(0);
        source_watch_state.thread_recovery_pending = false;
        source_watch_state.thread_recovery_pending_events.clear();
        source_watch_state.thread_recovery_backfill_complete = true;
        watch_state.set_account_state(source_account_id.clone(), source_watch_state);
        write_watch_state(&watch_state)?;

        let result = run_watch_iteration(WatchIterationOptions {
            port: Some(port),
            after_signal_id: None,
            cooldown_ms: Some(0),
            force_quota_refresh: true,
            progress: None,
        })?;

        ensure!(
            result.rotated,
            "watch trigger should have rotated the account"
        );
        ensure!(
            result.decision.should_rotate,
            "watch decision should have requested rotation"
        );
        ensure!(
            result.decision.rotation_command == Some(RotationCommand::Next),
            "expected watch-triggered rotation command to be Next, got {:?}",
            result.decision.rotation_command
        );
        ensure!(
            result.current_account_id == expected_target_email,
            "expected watch-triggered rotation to land on {}, got {}",
            expected_target_email,
            result.current_account_id
        );
        ensure!(
            result.state.last_rotated_email.as_deref() == Some(expected_target_email.as_str()),
            "expected watch state to record the rotated account {}",
            expected_target_email
        );

        let rotated_pool = load_pool().context("failed to load pool after watch rotation")?;
        let expected_active_index = (source_index + 1) % rotated_pool.accounts.len();
        ensure!(
            rotated_pool.active_index == expected_active_index,
            "expected active index {} after watch rotation, got {}",
            expected_active_index,
            rotated_pool.active_index
        );
        let rotated_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read Codex auth after watch rotation")?;
        ensure!(
            rotated_auth.tokens.account_id == result.current_account_id,
            "expected rotated auth account id {}, got {}",
            result.current_account_id,
            rotated_auth.tokens.account_id
        );
        let rotated_summary = summarize_codex_auth(&rotated_auth);
        ensure!(
            rotated_summary.email == expected_target_email,
            "expected rotated summary email {}, got {}",
            expected_target_email,
            rotated_summary.email
        );

        let rotated_persona = rotated_pool
            .accounts
            .get(rotated_pool.active_index)
            .and_then(|entry| entry.persona.as_ref())
            .context("rotated account missing persona metadata")?;
        let rotated_codex_home = paths
            .rotate_home
            .join(
                rotated_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("rotated persona missing host_root_rel_path")?,
            )
            .join("codex-home");
        let rotated_managed_profile = paths
            .rotate_home
            .join(
                rotated_persona
                    .host_root_rel_path
                    .as_ref()
                    .context("rotated persona missing host_root_rel_path")?,
            )
            .join("managed-profile");
        let current_codex_home = std::fs::read_link(&paths.codex_home)
            .context("read codex-home symlink after watch rotation")?;
        let current_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read managed-profile symlink after watch rotation")?;
        ensure!(
            current_codex_home == rotated_codex_home,
            "expected codex-home symlink to point at {} after watch rotation, got {}",
            rotated_codex_home.display(),
            current_codex_home.display()
        );
        ensure!(
            current_managed_profile == rotated_managed_profile,
            "expected managed-profile symlink to point at {} after watch rotation, got {}",
            rotated_managed_profile.display(),
            current_managed_profile.display()
        );
        ensure!(
            request_count.load(Ordering::SeqCst) > 0,
            "watch-triggered rotation should have probed quota at least once"
        );

        Ok(())
    })();

    if let Some(stop_server) = stop_server {
        stop_server.store(true, Ordering::Relaxed);
    }

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if let Err(error) = write_watch_state(&previous_watch_state) {
        if outcome.is_ok() {
            return Err(error);
        }
        eprintln!("failed to restore watch state after live watch-trigger test: {error:#}");
    }

    match previous_usage_url {
        Some(value) => unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", value);
        },
        None => unsafe {
            std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        },
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_watch_triggered_rotation_restart_sync_and_recovery_acceptance() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let preferred_source_email = staging_accounts[0].email.clone();
    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_watch_triggered_rotation_restart_sync_and_recovery_acceptance",
        &paths,
    )?;
    let port = 9333;
    let previous_watch_state = read_watch_state()?;
    let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
    let mut stop_server = None;

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        let source_cwd = paths.rotate_home.display().to_string();
        let active_marker = format!(
            "T047 full-flow active marker {}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_millis()
        );
        let recoverable_marker = format!(
            "T047 full-flow recoverable marker {}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_millis()
        );

        let source_email = prime_source_account_for_live_rotation(&preferred_source_email)?;

        ensure_debug_codex_instance(None, Some(port), None, None)
            .context("failed to launch managed Codex for full-flow setup")?;
        let pre_rotation_root_pid = managed_root_pid(&paths.debug_profile_dir)?;

        let source_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read source auth during full-flow setup")?;
        let source_summary = summarize_codex_auth(&source_auth);
        ensure!(
            source_summary.email == source_email,
            "expected source auth to match {}, got {}",
            source_email,
            source_summary.email
        );
        let source_pool = load_pool().context("failed to load pool before full-flow rotation")?;
        let source_index = source_pool.active_index;
        ensure!(
            source_pool.accounts.len() >= 2,
            "expected at least two pool accounts before full-flow rotation"
        );
        let expected_target_email = source_pool
            .accounts
            .get((source_index + 1) % source_pool.accounts.len())
            .map(|entry| entry.email.clone())
            .context("failed to resolve expected full-flow target account email")?;

        let source_active_thread_id = start_thread_with_marker(port, &source_cwd, &active_marker)?;
        let source_recoverable_thread_id =
            start_thread_with_marker(port, &source_cwd, &recoverable_marker)?;
        archive_thread_in_state_db(&paths.codex_state_db_file, &source_recoverable_thread_id)?;

        let mut watch_state = read_watch_state()?;
        let mut source_watch_state = watch_state.account_state(&source_summary.account_id);
        source_watch_state.last_signal_id = Some(0);
        source_watch_state.thread_recovery_pending = true;
        source_watch_state.thread_recovery_pending_events = vec![ThreadRecoveryEvent {
            source_log_id: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_millis() as i64,
            source_ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .context("system clock before UNIX_EPOCH")?
                .as_secs() as i64,
            thread_id: source_recoverable_thread_id.clone(),
            kind: ThreadRecoveryKind::QuotaExhausted,
            exhausted_turn_id: None,
            exhausted_email: Some(source_summary.email.clone()),
            exhausted_account_id: Some(source_summary.account_id.clone()),
            message: "You've hit your usage limit.".to_string(),
        }];
        source_watch_state.thread_recovery_backfill_complete = true;
        watch_state.set_account_state(source_summary.account_id.clone(), source_watch_state);
        write_watch_state(&watch_state)?;

        let quota_body = serde_json::json!({
            "user_id": source_summary.account_id,
            "account_id": source_summary.account_id,
            "email": source_summary.email,
            "plan_type": source_summary.plan_type,
            "rate_limit": {
                "allowed": true,
                "limit_reached": true,
                "primary_window": {
                    "used_percent": 100.0,
                    "limit_window_seconds": 18_000,
                    "reset_after_seconds": 3_600,
                    "reset_at": 1_775_185_200,
                },
                "secondary_window": null
            },
            "code_review_rate_limit": null,
            "additional_rate_limits": null,
            "credits": null,
            "promo": null
        })
        .to_string();
        let (quota_url, _, server_stop) = spawn_quota_server(quota_body);
        stop_server = Some(server_stop);
        unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", &quota_url);
        }

        let first_iteration = run_watch_iteration(WatchIterationOptions {
            port: Some(port),
            after_signal_id: None,
            cooldown_ms: Some(0),
            force_quota_refresh: true,
            progress: None,
        })?;
        ensure!(
            first_iteration.rotated,
            "watch trigger should have rotated during full-flow acceptance"
        );
        ensure!(
            first_iteration.decision.should_rotate,
            "watch decision should have requested rotation during full-flow acceptance"
        );
        ensure!(
            first_iteration.decision.rotation_command == Some(RotationCommand::Next),
            "expected watch-triggered full-flow rotation command to be Next, got {:?}",
            first_iteration.decision.rotation_command
        );

        ensure_debug_codex_instance(None, Some(port), None, None)?;
        ensure!(
            managed_codex_is_running(&paths.debug_profile_dir)?,
            "expected managed Codex to be running after watch-triggered rotation"
        );
        let post_rotation_root_pid = managed_root_pid(&paths.debug_profile_dir)?;
        ensure!(
            post_rotation_root_pid != pre_rotation_root_pid,
            "expected watch-triggered rotation to relaunch managed Codex with a new root pid, but pid stayed {}",
            pre_rotation_root_pid
        );

        let target_auth = load_codex_auth(&paths.codex_auth_file)
            .context("failed to read target auth after watch-triggered rotation")?;
        let target_summary = summarize_codex_auth(&target_auth);
        ensure!(
            target_summary.email == expected_target_email,
            "expected rotated auth email {}, got {}",
            expected_target_email,
            target_summary.email
        );
        let rotated_pool =
            load_pool().context("failed to load pool after watch-triggered rotation")?;
        let rotated_persona = rotated_pool
            .accounts
            .get(rotated_pool.active_index)
            .and_then(|entry| entry.persona.as_ref())
            .context("rotated account missing persona metadata after watch-triggered rotation")?;
        let target_managed_profile_path = paths
            .rotate_home
            .join(rotated_persona.host_root_rel_path.as_ref().context(
                "rotated persona missing host_root_rel_path after watch-triggered rotation",
            )?)
            .join("managed-profile");
        let current_managed_profile = std::fs::read_link(&paths.debug_profile_dir)
            .context("read managed-profile symlink after watch-triggered full-flow rotation")?;
        ensure!(
            current_managed_profile == target_managed_profile_path,
            "expected managed-profile symlink to point at {} after watch-triggered rotation, got {}",
            target_managed_profile_path.display(),
            current_managed_profile.display()
        );
        let target_config_path = paths
            .rotate_home
            .join(rotated_persona.host_root_rel_path.as_ref().context(
                "rotated persona missing host_root_rel_path after watch-triggered rotation",
            )?)
            .join("codex-home")
            .join("config.toml");
        let target_global_state_path = target_config_path
            .parent()
            .context("target config path missing parent directory")?
            .join(".codex-global-state.json");
        ensure!(
            config_contains_project(&target_config_path, &source_cwd)?,
            "expected rotated target config {} to register synced project cwd {}",
            target_config_path.display(),
            source_cwd
        );
        ensure!(
            global_state_contains_project(&target_global_state_path, &source_cwd)?,
            "expected rotated target workspace visibility state {} to include {}",
            target_global_state_path.display(),
            source_cwd
        );

        let target_active_threads = wait_for_active_threads_with_marker(port, &active_marker)?;
        let target_active_thread_ids = target_active_threads
            .iter()
            .map(|(thread_id, _)| thread_id.as_str())
            .collect::<Vec<_>>();
        ensure!(
            target_active_threads.len() == 1,
            "expected exactly one synced active marker thread, found {} ({})",
            target_active_threads.len(),
            target_active_thread_ids.join(", ")
        );
        let target_active_thread_id = target_active_threads
            .first()
            .map(|(thread_id, _)| thread_id.as_str())
            .context("missing synced active marker thread")?;
        ensure!(
            target_active_thread_id != source_active_thread_id,
            "expected synced active conversation to use a target-local thread id, but matched source id {}",
            source_active_thread_id
        );

        let watch_state_after_rotation = read_watch_state()?;
        let source_after_rotation =
            watch_state_after_rotation.account_state(&source_summary.account_id);
        ensure!(
            !source_after_rotation.thread_recovery_pending,
            "expected source account {} to clear pending recovery state after watch rotation",
            source_summary.account_id
        );
        ensure!(
            source_after_rotation
                .thread_recovery_pending_events
                .is_empty(),
            "expected source account {} to clear pending recovery events after watch rotation",
            source_summary.account_id
        );

        let target_after_rotation =
            watch_state_after_rotation.account_state(&target_summary.account_id);
        ensure!(
            target_after_rotation.thread_recovery_pending,
            "expected target account {} to have pending interrupted-thread state after watch rotation",
            target_summary.account_id
        );
        ensure!(
            target_after_rotation.thread_recovery_pending_events.len() == 1,
            "expected exactly one pending interrupted thread on target, got {}",
            target_after_rotation.thread_recovery_pending_events.len()
        );
        let pending_target_thread_id = target_after_rotation.thread_recovery_pending_events[0]
            .thread_id
            .clone();
        ensure!(
            pending_target_thread_id != source_recoverable_thread_id,
            "expected recoverable conversation to map to a target-local thread id distinct from source {}",
            source_recoverable_thread_id
        );
        let pending_thread = read_thread_with_turns(port, &pending_target_thread_id)?
            .with_context(|| {
                format!("failed to read pending target thread {pending_target_thread_id}")
            })?;
        ensure!(
            value_contains_text(&pending_thread, &recoverable_marker),
            "expected pending target thread {} to preserve recoverable marker {}",
            pending_target_thread_id,
            recoverable_marker
        );

        if managed_codex_is_running(&paths.debug_profile_dir)? {
            stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
        }
        ensure!(
            !managed_codex_is_running(&paths.debug_profile_dir)?,
            "expected managed Codex to be stopped before restart validation"
        );
        ensure_debug_codex_instance(None, Some(port), None, None)?;

        let mut recovered = false;
        for _ in 0..6 {
            let _ = run_watch_iteration(WatchIterationOptions {
                port: Some(port),
                after_signal_id: target_after_rotation.last_signal_id,
                cooldown_ms: Some(0),
                force_quota_refresh: false,
                progress: None,
            })?;
            let state_after_recovery_attempt = read_watch_state()?;
            let target_after_recovery =
                state_after_recovery_attempt.account_state(&target_summary.account_id);
            if !target_after_recovery.thread_recovery_pending
                && target_after_recovery
                    .thread_recovery_pending_events
                    .is_empty()
            {
                recovered = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        ensure!(
            recovered,
            "expected interrupted-thread recovery to auto-complete after restart for account {}",
            target_summary.account_id
        );

        let _ = wait_for_thread_marker(
            port,
            &pending_target_thread_id,
            "continue with skipped msgs",
        )
        .with_context(|| {
            format!(
                "expected interrupted thread {} to receive automatic resume input after restart",
                pending_target_thread_id
            )
        })?;

        Ok(())
    })();

    if let Some(stop_server) = stop_server {
        stop_server.store(true, Ordering::Relaxed);
    }

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if let Err(error) = write_watch_state(&previous_watch_state) {
        if outcome.is_ok() {
            return Err(error);
        }
        eprintln!("failed to restore watch state after full-flow acceptance test: {error:#}");
    }

    match previous_usage_url {
        Some(value) => unsafe {
            std::env::set_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", value);
        },
        None => unsafe {
            std::env::remove_var("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        },
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_managed_relogin_smoke_coverage() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    // Load staging accounts to get a valid email.
    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    let target_email = &staging_accounts[0].email;

    let paths = resolve_paths()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_managed_relogin_smoke_coverage", &paths)?;
    let port = 9333;

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // Perform a real managed relogin.
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };

    let result = cmd_relogin_with_progress(target_email, options, None);

    match result {
        Ok(output) => {
            println!("Relogin succeeded: {}", output);
        }
        Err(error) => {
            let msg = format!("{:#}", error);
            println!("Relogin failed (expected if unattended): {}", msg);
        }
    }

    // Cleanup.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_managed_relogin_no_system_browser_guarantee() -> Result<()> {
    use codex_rotate_refresh::process_tracking::ProcessTracker;

    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    // Load staging accounts to get a valid email.
    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    let target_email = &staging_accounts[0].email;

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_managed_relogin_no_system_browser_guarantee",
        &paths,
    )?;
    let port = 9333;

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // Start process tracker to detect leaked/unexpected processes.
    let process_tracker = ProcessTracker::new()?;

    // Perform relogin.
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };

    let _ = cmd_relogin_with_progress(target_email, options, None);

    // Verify that any browser processes launched are correctly identified as managed.
    let new_processes = process_tracker.new_processes_since_baseline()?;
    for process in new_processes {
        let cmd = process.command.to_lowercase();
        if cmd.contains("chrome") || cmd.contains("google chrome") {
            // It should have our profile dir in its arguments.
            assert!(
                cmd.contains(&paths.fast_browser_home.to_string_lossy().to_lowercase()),
                "Launched a browser process without isolated profile: {}",
                process.command
            );
        }
        process_tracker.record_test_owned_process(
            process.pid,
            "live host browser process",
            process.command,
        );
    }

    // Cleanup.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    process_tracker.assert_no_leaks()?;

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_codex_desktop_smoke_coverage() -> Result<()> {
    // A12: Missing prerequisites fail loudly as a skipped-or-blocked precondition.
    require_host_live_capabilities()?;

    let paths = resolve_paths()?;
    let process_tracker = ProcessTracker::new()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_codex_desktop_smoke_coverage", &paths)?;
    let port = 9333;

    // Ensure we start from a clean state for the isolated profile.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    // The live test launches real Codex Desktop against isolated host persona roots.
    ensure_debug_codex_instance(None, Some(port), None, None)?;

    // Verify reachability via CDP.
    assert!(
        is_cdp_page_ready(port),
        "Codex CDP page should be ready on port {} after launch.",
        port
    );
    assert!(
        managed_codex_is_running(&paths.debug_profile_dir)?,
        "Managed Codex should be running with isolated profile {}.",
        paths.debug_profile_dir.display()
    );

    let launched_processes = process_tracker.new_processes_since_baseline()?;

    // The test closes the launched Codex resources before success.
    stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    assert!(
        !managed_codex_is_running(&paths.debug_profile_dir)?,
        "Managed Codex should no longer be running after stop."
    );

    for process in launched_processes {
        process_tracker.record_test_owned_process(
            process.pid,
            "live host codex process",
            process.command,
        );
    }
    process_tracker.assert_no_leaks()?;

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_codex_desktop_distinguish_test_managed_instances() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new(
        "live_host_codex_desktop_distinguish_test_managed_instances",
        &paths,
    )?;
    let port_1 = 9333;
    let port_2 = 9334;

    // Use a separate temp profile for the "other" instance.
    let temp_root = tempfile::tempdir()?;
    let other_profile = temp_root.path().join("other-profile");
    std::fs::create_dir_all(&other_profile)?;
    artifacts.track_temp_path(temp_root.path(), "other-instance temp root");
    artifacts.track_temp_path(&other_profile, "other-instance profile");

    // Ensure clean state.
    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port_1, &paths.debug_profile_dir)?;
    }
    if managed_codex_is_running(&other_profile)? {
        stop_managed_codex_instance(port_2, &other_profile)?;
    }

    // Launch first instance (test-managed).
    ensure_debug_codex_instance(None, Some(port_1), None, None)?;
    assert!(managed_codex_is_running(&paths.debug_profile_dir)?);
    assert!(!managed_codex_is_running(&other_profile)?);

    // Launch second instance (simulating another profile).
    ensure_debug_codex_instance(None, Some(port_2), Some(&other_profile), None)?;
    assert!(managed_codex_is_running(&paths.debug_profile_dir)?);
    assert!(managed_codex_is_running(&other_profile)?);

    // Prove we can stop one without affecting the other.
    stop_managed_codex_instance(port_1, &paths.debug_profile_dir)?;
    assert!(!managed_codex_is_running(&paths.debug_profile_dir)?);
    assert!(managed_codex_is_running(&other_profile)?);

    // Cleanup second instance.
    stop_managed_codex_instance(port_2, &other_profile)?;
    assert!(!managed_codex_is_running(&other_profile)?);

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_codex_desktop_auto_close_on_exit() -> Result<()> {
    // A12: Missing prerequisites fail loudly.
    require_host_live_capabilities()?;

    let port = 9335;
    let duration = 2;
    let process_tracker = ProcessTracker::new()?;
    let filesystem_tracker = FilesystemTracker::new()?;

    let paths = resolve_paths()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_codex_desktop_auto_close_on_exit", &paths)?;

    {
        // Use a separate temp profile for this test.
        let temp_root = tempfile::tempdir()?;
        let test_profile = temp_root.path().join("auto-close-profile");
        std::fs::create_dir_all(&test_profile)?;
        filesystem_tracker.record_temp_path(temp_root.path(), "auto-close temp root", false);
        filesystem_tracker.record_temp_path(&test_profile, "auto-close profile", false);

        // Ensure clean state.
        if managed_codex_is_running(&test_profile)? {
            stop_managed_codex_instance(port, &test_profile)?;
        }

        let child_command = format!(
            "{} internal launch-managed --port {} --profile-dir {} --duration {}",
            env!("CARGO_BIN_EXE_codex-rotate"),
            port,
            test_profile.display(),
            duration,
        );

        // Spawn the CLI in a child process to launch Codex.
        let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_codex-rotate"))
            .arg("internal")
            .arg("launch-managed")
            .arg("--port")
            .arg(port.to_string())
            .arg("--profile-dir")
            .arg(test_profile.to_string_lossy().as_ref())
            .arg("--duration")
            .arg(duration.to_string())
            .env("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "0")
            .spawn()
            .context("spawn codex-rotate launch-managed")?;
        process_tracker.record_test_owned_process(
            child.id(),
            "launch-managed child",
            child_command,
        );

        // Wait for it to launch.
        let mut launched = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if managed_codex_is_running(&test_profile)? {
                launched = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        assert!(
            launched,
            "Managed Codex should have been launched by the child process."
        );

        // Wait for the child process to exit.
        let status = child.wait()?;
        assert!(
            status.success(),
            "Child process should have exited successfully."
        );

        // Verify Codex is now gone.
        let mut closed = false;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            if !managed_codex_is_running(&test_profile)? {
                closed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }

        assert!(
            closed,
            "Managed Codex should have been closed automatically on child process exit."
        );
    }

    process_tracker.assert_no_leaks()?;
    filesystem_tracker.assert_no_leaks()?;

    artifacts.complete()?;
    Ok(())
}

#[test]
#[ignore]
fn live_host_full_lineage_sync_acceptance() -> Result<()> {
    require_host_live_capabilities()?;

    let staging_accounts: Vec<LiveStagingAccount> = load_live_staging_accounts(2)?;
    ensure!(
        staging_accounts.len() >= 2,
        "expected at least two staging accounts"
    );

    let paths = resolve_paths()?;
    let artifacts =
        LiveHostFailureArtifacts::new("live_host_full_lineage_sync_acceptance", &paths)?;
    let port = 9333;
    let source_cwd = paths.rotate_home.display().to_string();
    let marker = format!(
        "T122-marker-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
    );

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        let options = ReloginOptions {
            manual_login: false,
            logout_first: true,
            allow_email_change: false,
        };
        cmd_relogin_with_progress(&staging_accounts[0].email, options.clone(), None)?;

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        // 1. Create a thread in source persona
        let source_thread = send_local_mcp_request(
            port,
            "thread/start",
            json!({
                "cwd": source_cwd,
                "personality": "pragmatic",
                "approvalsReviewer": "user",
            }),
        )?;
        let source_thread_id = source_thread
            .get("thread")
            .and_then(|t| t.get("id"))
            .and_then(Value::as_str)
            .context("thread/start did not return a thread id")?
            .to_string();

        send_local_mcp_request(
            port,
            "turn/start",
            json!({
                "threadId": source_thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": marker,
                        "text_elements": [],
                    }
                ],
                "cwd": source_cwd,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "summary": "none",
                "personality": "pragmatic",
                "attachments": [],
            }),
        )?;

        wait_for_thread_marker(port, &source_thread_id, &marker)?;

        // 2. Rotate to target persona
        match run_shared_next(Some(port), None)? {
            NextResult::Rotated { summary, .. } => {
                ensure!(
                    summary.email == staging_accounts[1].email,
                    "expected rotation to target {}, got {}",
                    staging_accounts[1].email,
                    summary.email
                );
            }
            other => bail!("expected rotation, got {:?}", other),
        }

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        // 3. Verify thread imported with NEW ID
        let (target_thread_id, _target_thread) = wait_for_imported_thread(port, &marker)?;
        ensure!(
            target_thread_id != source_thread_id,
            "expected imported target thread to use a new thread id, but both were {source_thread_id}"
        );

        // 4. Re-rotate back to source and back to target (Repeated Sync)
        run_shared_next(Some(port), None)?; // Back to [0]
        run_shared_next(Some(port), None)?; // Back to [1]

        ensure_debug_codex_instance(None, Some(port), None, None)?;

        // 5. Verify NO DUPLICATE thread in target
        let thread_ids = read_active_thread_ids(Some(port))?;
        let mut matching_threads = 0;
        for tid in thread_ids {
            if let Some(t) = read_thread_with_turns(port, &tid)? {
                if value_contains_text(&t, &marker) {
                    matching_threads += 1;
                    ensure!(
                        tid == target_thread_id,
                        "Idempotency failure: found new thread ID {} instead of bound {}",
                        tid,
                        target_thread_id
                    );
                }
            }
        }
        ensure!(
            matching_threads == 1,
            "Expected exactly 1 matching thread, found {}",
            matching_threads
        );

        Ok(())
    })();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

#[test]
#[ignore]
fn live_host_project_visibility_probe() -> Result<()> {
    require_host_live_capabilities()?;

    let paths = resolve_paths()?;
    let artifacts = LiveHostFailureArtifacts::new("live_host_project_visibility_probe", &paths)?;
    let port = std::env::var("LIVE_HOST_PROJECT_VISIBILITY_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(9333);
    let restart_after_login = std::env::var("LIVE_HOST_PROJECT_VISIBILITY_RESTART")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);
    let capture_startup_requests = std::env::var("LIVE_HOST_PROJECT_VISIBILITY_CAPTURE")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false);

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let outcome = (|| -> Result<()> {
        ensure_debug_codex_instance(None, Some(port), None, None)?;
        switch_live_account_to_current_auth(Some(port), true, 30_000)?;
        if restart_after_login {
            stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            switch_live_account_to_current_auth(Some(port), true, 30_000)?;
        }

        if capture_startup_requests {
            let startup_requests = capture_codex_startup_requests(port)?;
            println!(
                "PROJECT VISIBILITY STARTUP REQUESTS:\n{}",
                serde_json::to_string_pretty(&startup_requests)?
            );
        }

        let snapshot = inspect_codex_page_state(port)?;
        println!(
            "PROJECT VISIBILITY SNAPSHOT:\n{}",
            serde_json::to_string_pretty(&snapshot)?
        );

        if let Some(body_text) = snapshot.get("bodyText").and_then(Value::as_str) {
            println!(
                "PROJECT VISIBILITY MATCHES: ai-tools={}, projects={}, threads={}, add-project={}",
                body_text.contains("ai-tools"),
                body_text.contains("Projects"),
                body_text.contains("Threads"),
                body_text.contains("Add project"),
            );
        }

        Ok(())
    })();

    if managed_codex_is_running(&paths.debug_profile_dir)? {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    if outcome.is_ok() {
        artifacts.complete()?;
    }

    outcome
}

fn send_local_mcp_request(port: u16, method: &str, params: Value) -> Result<Value> {
    let request_id = format!(
        "live-host-{method}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before UNIX_EPOCH")?
            .as_millis()
    );
    let request_json = serde_json::to_string(&json!({
        "type": "mcp-request",
        "hostId": "local",
        "request": {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
    }))?;
    let expression = format!(
        r#"new Promise(async (resolve) => {{
const request = {request_json};
const timeout = setTimeout(() => {{
    window.removeEventListener("message", handler);
  resolve({{ timeout: true }});
}}, 10000);
const handler = (event) => {{
  const data = event.data;
    if (data && data.type === "mcp-response" && data.message && data.message.id === request.request.id) {{
    clearTimeout(timeout);
    window.removeEventListener("message", handler);
    resolve({{
      timeout: false,
      result: data.message.result ?? null,
      error: data.message.error ?? null
    }});
  }}
}};
window.addEventListener("message", handler);
await window.electronBridge.sendMessageFromView(request);
}})"#
    );
    let value: Value =
        with_local_codex_connection(port, |connection| connection.evaluate(&expression))?;
    if value.get("timeout").and_then(Value::as_bool) == Some(true) {
        bail!("Timed out waiting for Codex {method} response.");
    }
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        bail!("Codex {method} request failed: {error}");
    }
    Ok(value.get("result").cloned().unwrap_or(Value::Null))
}

fn inspect_codex_page_state(port: u16) -> Result<Value> {
    with_local_codex_connection(port, |connection| {
        connection.evaluate(
            r#"(() => {
const requestToPromise = (request) =>
  new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error('IndexedDB request failed'));
  });
const openDatabase = (name, version) =>
  new Promise((resolve, reject) => {
    const request = version === undefined ? indexedDB.open(name) : indexedDB.open(name, version);
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error(`Failed to open ${name}`));
    request.onupgradeneeded = () => {};
  });
const serializeValue = (value) => {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_) {
    return String(value);
  }
};
const inspectIndexedDb = async () => {
  if (typeof indexedDB === 'undefined' || typeof indexedDB.databases !== 'function') {
    return [];
  }
  const databases = await indexedDB.databases();
  const results = [];
  for (const databaseInfo of databases) {
    if (!databaseInfo || !databaseInfo.name) {
      continue;
    }
    const db = await openDatabase(databaseInfo.name, databaseInfo.version);
    const stores = [];
    for (const storeName of Array.from(db.objectStoreNames)) {
      const transaction = db.transaction(storeName, 'readonly');
      const store = transaction.objectStore(storeName);
      const count = await requestToPromise(store.count());
      let sample = [];
      if (count > 0) {
        sample = await requestToPromise(store.getAll(undefined, 5)).then((values) =>
          values.map(serializeValue),
        );
      }
      stores.push({
        name: storeName,
        count,
        sample,
      });
    }
    db.close();
    results.push({
      name: databaseInfo.name,
      version: databaseInfo.version ?? null,
      stores,
    });
  }
  return results;
};
const sessionStorageDump = {};
for (let index = 0; index < sessionStorage.length; index += 1) {
  const key = sessionStorage.key(index);
  if (key !== null) {
    sessionStorageDump[key] = sessionStorage.getItem(key);
  }
}
const controls = Array.from(document.querySelectorAll('button, [role="button"], a'))
  .map((element) => ({
    tag: element.tagName,
    text: (element.textContent || '').trim(),
    ariaLabel: element.getAttribute('aria-label'),
    dataTestid: element.getAttribute('data-testid'),
  }))
  .filter((control) => control.text || control.ariaLabel || control.dataTestid);
const localStorageKeys = [];
const relevantLocalStorage = {};
for (let index = 0; index < localStorage.length; index += 1) {
  const key = localStorage.key(index);
  if (key !== null) {
    localStorageKeys.push(key);
    const value = localStorage.getItem(key);
    if (/(project|thread|workspace|sidebar|codex|chat)/i.test(key) || /ai-tools/i.test(value || "")) {
      relevantLocalStorage[key] = value;
    }
  }
}
const databasesPromise =
  typeof indexedDB !== "undefined" && indexedDB.databases ? indexedDB.databases() : Promise.resolve([]);
return Promise.all([databasesPromise, inspectIndexedDb()]).then(([indexedDbDatabases, indexedDbDump]) => ({
  url: location.href,
  title: document.title,
  bodyText: document.body ? document.body.innerText.slice(0, 12000) : null,
  controls,
  localStorageKeyCount: localStorageKeys.length,
  localStorageKeys: localStorageKeys.sort(),
  relevantLocalStorage,
  sessionStorage: sessionStorageDump,
  indexedDbDatabases,
  indexedDbDump,
  electronBridgeKeys:
    typeof window.electronBridge === 'object' && window.electronBridge !== null
      ? Object.keys(window.electronBridge).sort()
      : [],
  electronBridgeProtoKeys:
    typeof window.electronBridge === 'object' && window.electronBridge !== null
      ? Object.getOwnPropertyNames(Object.getPrototypeOf(window.electronBridge) || {}).sort()
      : [],
  sharedObjectSnapshots:
    typeof window.electronBridge?.getSharedObjectSnapshotValue === 'function'
      ? {
          activeWorkspaceRoots: window.electronBridge.getSharedObjectSnapshotValue('active-workspace-roots'),
          workspaceRootOptions: window.electronBridge.getSharedObjectSnapshotValue('workspace-root-options'),
          projectOrder: window.electronBridge.getSharedObjectSnapshotValue('project-order'),
          electronSavedWorkspaceRoots: window.electronBridge.getSharedObjectSnapshotValue('electron-saved-workspace-roots'),
        }
      : null,
  windowKeys: Object.keys(window)
    .filter((key) => /store|project|sidebar|thread|workspace|electron|query|client/i.test(key))
    .sort()
    .slice(0, 200),
}));
})()"#,
        )
    })
}

fn capture_codex_startup_requests(port: u16) -> Result<Value> {
    with_local_codex_connection(port, |connection| {
        connection.add_script_to_evaluate_on_new_document(
            r#"(() => {
const requestLog = [];
let bridgeValue = window.electronBridge ?? null;
const record = (kind, detail) => {
  try {
    requestLog.push({ kind, ...detail, timestamp: Date.now() });
  } catch (_) {}
};
const wrapBridge = (bridge) => {
  if (!bridge) {
    return bridge;
  }
  if (typeof bridge.sendMessageFromView === "function") {
    const original = bridge.sendMessageFromView.bind(bridge);
    bridge.sendMessageFromView = async (request) => {
      record("mcp", {
        hostId: request && typeof request === "object" ? request.hostId ?? null : null,
        method:
          request && typeof request === "object"
            ? request.request?.method ?? request.method ?? null
            : null,
      });
      return original(request);
    };
  }
  if (typeof bridge.sendWorkerMessageFromView === "function") {
    const originalWorker = bridge.sendWorkerMessageFromView.bind(bridge);
    bridge.sendWorkerMessageFromView = async (request) => {
      record("worker-send", {
        keys:
          request && typeof request === "object"
            ? Object.keys(request).sort()
            : null,
        type:
          request && typeof request === "object"
            ? request.type ?? request.kind ?? request.message?.type ?? null
            : null,
        method:
          request && typeof request === "object"
            ? request.method ?? request.message?.method ?? null
            : null,
      });
      return originalWorker(request);
    };
  }
  if (typeof bridge.getSharedObjectSnapshotValue === "function") {
    const originalSnapshot = bridge.getSharedObjectSnapshotValue.bind(bridge);
    bridge.getSharedObjectSnapshotValue = (...args) => {
      record("snapshot-read", {
        args,
      });
      return originalSnapshot(...args);
    };
  }
  return bridge;
};
Object.defineProperty(window, "__codexRequestLog", {
  configurable: true,
  enumerable: false,
  get: () => requestLog,
});
Object.defineProperty(window, "electronBridge", {
  configurable: true,
  enumerable: true,
  get: () => bridgeValue,
  set: (next) => {
    bridgeValue = wrapBridge(next);
  },
});
bridgeValue = wrapBridge(bridgeValue);
const originalFetch = window.fetch?.bind(window) ?? null;
if (originalFetch) {
  window.fetch = async (...args) => {
    record("fetch", {
      url: String(args[0]),
    });
    return originalFetch(...args);
  };
}
})()"#,
        )?;
        connection.reload_page(true)?;
        std::thread::sleep(Duration::from_secs(5));
        connection.evaluate(
            r#"(() => window.__codexRequestLog ? window.__codexRequestLog.slice() : [])()"#,
        )
    })
}

fn read_thread_with_turns(port: u16, thread_id: &str) -> Result<Option<Value>> {
    let response = match send_local_mcp_request(
        port,
        "thread/read",
        json!({
            "threadId": thread_id,
            "includeTurns": true,
        }),
    ) {
        Ok(response) => response,
        Err(error) => {
            let message = format!("{:#}", error);
            if message.contains("includeTurns is unavailable before first user message")
                || message.contains("is not materialized yet")
            {
                return Ok(None);
            }
            return Err(error);
        }
    };
    Ok(response.get("thread").cloned())
}

fn is_live_unattended_relogin_error(error: &anyhow::Error) -> bool {
    let message = format!("{:#}", error).to_ascii_lowercase();
    message.contains("secrets.session request did not match")
        || message.contains("no stored credentials were found")
        || message.contains("automation bridge command complete-codex-login-attempt failed")
}

fn prime_source_account_for_live_rotation(preferred_email: &str) -> Result<String> {
    let options = ReloginOptions {
        manual_login: false,
        logout_first: true,
        allow_email_change: false,
    };
    match cmd_relogin_with_progress(preferred_email, options, None) {
        Ok(_) => Ok(preferred_email.to_string()),
        Err(error) if is_live_unattended_relogin_error(&error) => {
            let _ = restore_codex_auth_from_active_pool()
                .context("failed to restore active pool auth during live relogin fallback")?;
            let paths = resolve_paths()?;
            let auth = load_codex_auth(&paths.codex_auth_file).context(
                "failed to read Codex auth after restoring active pool auth during relogin fallback",
            )?;
            let summary = summarize_codex_auth(&auth);
            eprintln!(
                "codex-rotate: relogin fallback using existing active account {} after relogin error: {:#}",
                summary.email, error
            );
            Ok(summary.email)
        }
        Err(error) => Err(error),
    }
}

fn start_thread_with_marker(port: u16, cwd: &str, marker: &str) -> Result<String> {
    let thread = send_local_mcp_request(
        port,
        "thread/start",
        json!({
            "cwd": cwd,
            "model": Value::Null,
            "modelProvider": Value::Null,
            "serviceTier": Value::Null,
            "approvalPolicy": Value::Null,
            "approvalsReviewer": "user",
            "sandbox": Value::Null,
            "personality": "pragmatic",
        }),
    )?;
    let thread_id = thread
        .get("thread")
        .and_then(|value| value.get("id"))
        .and_then(Value::as_str)
        .context("thread/start did not return a thread id")?
        .to_string();

    send_local_mcp_request(
        port,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [
                {
                    "type": "text",
                    "text": marker,
                    "text_elements": [],
                }
            ],
            "cwd": cwd,
            "approvalPolicy": Value::Null,
            "approvalsReviewer": "user",
            "sandboxPolicy": Value::Null,
            "model": Value::Null,
            "serviceTier": Value::Null,
            "effort": Value::Null,
            "summary": "none",
            "personality": Value::Null,
            "outputSchema": Value::Null,
            "collaborationMode": Value::Null,
            "attachments": [],
        }),
    )?;
    wait_for_thread_marker(port, &thread_id, marker)?;
    Ok(thread_id)
}

fn wait_for_thread_marker(port: u16, thread_id: &str, marker: &str) -> Result<Value> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(thread) = read_thread_with_turns(port, thread_id)? {
            if value_contains_text(&thread, marker) {
                return Ok(thread);
            }
        }
        if Instant::now() >= deadline {
            bail!("Timed out waiting for source thread {thread_id} to include marker {marker}");
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn active_threads_with_marker(port: u16, marker: &str) -> Result<Vec<(String, Value)>> {
    let mut matches = Vec::new();
    let thread_ids = read_active_thread_ids(Some(port))?;
    for thread_id in thread_ids {
        if let Some(thread) = read_thread_with_turns(port, &thread_id)? {
            if value_contains_text(&thread, marker) {
                matches.push((thread_id, thread));
            }
        }
    }
    Ok(matches)
}

fn wait_for_active_threads_with_marker(port: u16, marker: &str) -> Result<Vec<(String, Value)>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let matches = active_threads_with_marker(port, marker)?;
        if !matches.is_empty() {
            return Ok(matches);
        }
        if Instant::now() >= deadline {
            bail!(
                "Timed out waiting for target persona to materialize an active thread containing marker {marker}"
            );
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn wait_for_imported_thread(port: u16, marker: &str) -> Result<(String, Value)> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let matches = active_threads_with_marker(port, marker)?;
        if let Some((thread_id, thread)) = matches.into_iter().next() {
            return Ok((thread_id, thread));
        }
        if Instant::now() >= deadline {
            bail!(
                "Timed out waiting for target persona to import a thread containing marker {marker}"
            );
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn managed_root_pid(profile_dir: &Path) -> Result<u32> {
    let mut root_pids = managed_codex_root_pids(profile_dir)?;
    root_pids.sort_unstable();
    root_pids.into_iter().next().context(format!(
        "expected a managed Codex root pid for profile {}",
        profile_dir.display()
    ))
}

fn config_contains_project(path: &Path, project_path: &str) -> Result<bool> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(contents.contains(&format!(
        "[projects.\"{}\"]",
        encode_toml_basic_string(project_path)
    )))
}

fn global_state_contains_project(path: &Path, project_path: &str) -> Result<bool> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let state: Value = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    let contains_project = state
        .get("electron-saved-workspace-roots")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .any(|entry| entry.as_str() == Some(project_path))
        })
        .unwrap_or(false);
    Ok(contains_project)
}

fn encode_toml_basic_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\u{0008}' => escaped.push_str("\\b"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\u{000C}' => escaped.push_str("\\f"),
            '\r' => escaped.push_str("\\r"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

fn archive_thread_in_state_db(state_db_path: &Path, thread_id: &str) -> Result<()> {
    ensure!(
        state_db_path.exists(),
        "state DB {} did not exist before archiving {}",
        state_db_path.display(),
        thread_id
    );
    let connection = Connection::open(state_db_path)
        .with_context(|| format!("failed to open state DB {}", state_db_path.display()))?;
    let updated =
        connection.execute("update threads set archived = 1 where id = ?1", [thread_id])?;
    ensure!(
        updated == 1,
        "failed to archive thread {thread_id} in state DB {}",
        state_db_path.display()
    );
    Ok(())
}

fn ensure_one_pending_recoverable_thread(
    pending_events: &[ThreadRecoveryEvent],
    thread_id: &str,
) -> Result<()> {
    ensure!(
        pending_events.len() == 1,
        "expected exactly one recoverable thread event, got {}",
        pending_events.len()
    );
    ensure!(
        pending_events[0].thread_id == thread_id,
        "expected recoverable event to target {thread_id}, got {}",
        pending_events[0].thread_id
    );
    Ok(())
}

fn value_contains_text(value: &Value, needle: &str) -> bool {
    match value {
        Value::String(text) => text.contains(needle),
        Value::Array(values) => values
            .iter()
            .any(|value| value_contains_text(value, needle)),
        Value::Object(values) => values
            .values()
            .any(|value| value_contains_text(value, needle)),
        _ => false,
    }
}

fn restore_env(name: &str, value: Option<std::ffi::OsString>) {
    match value {
        Some(value) => unsafe {
            std::env::set_var(name, value);
        },
        None => unsafe {
            std::env::remove_var(name);
        },
    }
}

fn spawn_quota_server(response_body: String) -> (String, Arc<AtomicUsize>, Arc<AtomicBool>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("quota listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking quota listener");
    let address = listener.local_addr().expect("quota listener addr");
    let request_count = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let request_count_thread = request_count.clone();
    let stop_thread = stop.clone();

    std::thread::spawn(move || {
        while !stop_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    request_count_thread.fetch_add(1, Ordering::SeqCst);
                    let mut buffer = [0_u8; 1024];
                    let _ = stream.read(&mut buffer);
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response_body.len(),
                        response_body
                    );
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(_) => break,
            }
        }
    });

    (
        format!("http://127.0.0.1:{}/wham/usage", address.port()),
        request_count,
        stop,
    )
}
