use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth, AuthSummary, CodexAuth};
use codex_rotate_core::fs_security::write_private_string;
use codex_rotate_core::pool::{
    load_pool, other_usable_account_exists, restore_codex_auth_from_active_pool, Pool,
};
use codex_rotate_core::quota::{
    build_cached_quota_state, inspect_quota, quota_cache_is_stale, CachedQuotaState,
};
use codex_rotate_core::workflow::{
    cmd_create_with_progress, is_auto_create_retry_stopped_for_reusable_account,
    is_create_already_in_progress_error, CreateCommandOptions, CreateCommandSource,
};
use serde::{Deserialize, Serialize};

use crate::hook::{
    live_account_matches_summary, read_live_account_if_running,
    switch_live_account_to_current_auth, AccountReadResult, LiveSwitchResult,
};
use crate::log_isolation::run_account_operation_with_log_isolation;
use crate::logs::{
    codex_logs_availability, read_codex_signals, read_latest_codex_signal_id, CodexLogSignal,
    CodexLogsAvailability, CodexSignalKind,
};
use crate::paths::resolve_paths;
use crate::runtime_log::log_daemon_error;
use crate::thread_recovery::{
    read_latest_recoverable_turn_failure_log_id, run_thread_recovery_iteration,
    RecoveryIterationOptions, ThreadRecoveryEvent,
};

pub const LOW_QUOTA_ROTATION_THRESHOLD_PERCENT: u8 = 20;
pub const DEFAULT_COOLDOWN_MS: u64 = 15_000;
const SIGNAL_CURSOR_RESET_LOOKBACK_LOGS: i64 = 2_000;
const THREAD_RECOVERY_BOOTSTRAP_LOOKBACK_LOGS: i64 = 2_000;

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct AccountWatchState {
    pub last_signal_id: Option<i64>,
    pub last_checked_at: Option<String>,
    pub last_live_email: Option<String>,
    pub last_thread_recovery_log_id: Option<i64>,
    pub thread_recovery_pending: bool,
    pub thread_recovery_pending_events: Vec<ThreadRecoveryEvent>,
    pub thread_recovery_backfill_complete: bool,
    pub quota: Option<CachedQuotaState>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct WatchState {
    pub last_rotation_at: Option<String>,
    pub last_rotation_reason: Option<String>,
    pub last_rotated_email: Option<String>,
    #[serde(default = "default_auto_create_enabled")]
    pub auto_create_enabled: bool,
    #[serde(default = "default_tray_enabled")]
    pub tray_enabled: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub accounts: BTreeMap<String, AccountWatchState>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
struct RawWatchState {
    last_signal_id: Option<i64>,
    last_checked_at: Option<String>,
    last_live_email: Option<String>,
    last_rotation_at: Option<String>,
    last_rotation_reason: Option<String>,
    last_rotated_email: Option<String>,
    last_thread_recovery_log_id: Option<i64>,
    thread_recovery_pending: bool,
    thread_recovery_pending_events: Vec<ThreadRecoveryEvent>,
    thread_recovery_backfill_complete: bool,
    quota: Option<CachedQuotaState>,
    auto_create_enabled: Option<bool>,
    tray_enabled: Option<bool>,
    accounts: BTreeMap<String, AccountWatchState>,
}

impl WatchState {
    pub fn account_state(&self, account_id: &str) -> AccountWatchState {
        self.accounts.get(account_id).cloned().unwrap_or_default()
    }

    pub fn set_account_state(
        &mut self,
        account_id: impl Into<String>,
        account_state: AccountWatchState,
    ) {
        let account_id = account_id.into();
        if account_state == AccountWatchState::default() {
            self.accounts.remove(&account_id);
        } else {
            self.accounts.insert(account_id, account_state);
        }
    }
}

fn default_auto_create_enabled() -> bool {
    true
}

fn default_tray_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionQuotaAssessment {
    pub summary: String,
    pub usable: bool,
    pub blocker: Option<String>,
    pub primary_quota_left_percent: Option<u8>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RotationCommand {
    Next,
    Create,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RotationDecision {
    pub last_signal_id: Option<i64>,
    pub signals: Vec<CodexLogSignal>,
    pub assessment: Option<DecisionQuotaAssessment>,
    pub assessment_error: Option<String>,
    pub should_rotate: bool,
    pub reason: Option<String>,
    pub rotation_command: Option<RotationCommand>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchIterationResult {
    pub state: WatchState,
    pub current_account_id: String,
    pub current_account_state: AccountWatchState,
    pub decision: RotationDecision,
    pub rotated: bool,
    pub rotation: Option<AuthSummary>,
    pub live: Option<LiveSwitchResult>,
    pub logs_availability: CodexLogsAvailability,
}

pub struct WatchIterationOptions {
    pub port: Option<u16>,
    pub after_signal_id: Option<i64>,
    pub cooldown_ms: Option<u64>,
    pub force_quota_refresh: bool,
    pub progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

pub fn read_watch_state() -> Result<WatchState> {
    let paths = resolve_paths()?;
    if !paths.watch_state_file.exists() {
        return Ok(WatchState::default());
    }
    let raw =
        fs::read_to_string(&paths.watch_state_file).context("Failed to read watch-state.json.")?;
    let state = serde_json::from_str::<RawWatchState>(&raw)
        .map(|raw| migrate_watch_state(raw, current_watch_account_id().as_deref()))
        .unwrap_or_default();
    Ok(state)
}

pub fn write_watch_state(state: &WatchState) -> Result<()> {
    let paths = resolve_paths()?;
    let raw = serde_json::to_string_pretty(state)?;
    write_private_string(&paths.watch_state_file, &raw)
}

pub fn auto_create_enabled() -> Result<bool> {
    Ok(read_watch_state()?.auto_create_enabled)
}

pub fn set_auto_create_enabled(enabled: bool) -> Result<()> {
    let mut state = read_watch_state()?;
    state.auto_create_enabled = enabled;
    write_watch_state(&state)
}

pub fn tray_enabled() -> Result<bool> {
    Ok(read_watch_state()?.tray_enabled)
}

pub fn set_tray_enabled(enabled: bool) -> Result<()> {
    let mut state = read_watch_state()?;
    state.tray_enabled = enabled;
    write_watch_state(&state)
}

fn migrate_watch_state(raw: RawWatchState, current_account_id: Option<&str>) -> WatchState {
    let mut state = WatchState {
        last_rotation_at: raw.last_rotation_at,
        last_rotation_reason: raw.last_rotation_reason,
        last_rotated_email: raw.last_rotated_email,
        auto_create_enabled: raw
            .auto_create_enabled
            .unwrap_or_else(default_auto_create_enabled),
        tray_enabled: raw.tray_enabled.unwrap_or_else(default_tray_enabled),
        accounts: raw.accounts,
    };

    if state.accounts.is_empty()
        && current_account_id.is_some()
        && (raw.last_signal_id.is_some()
            || raw.last_checked_at.is_some()
            || raw.last_live_email.is_some()
            || raw.last_thread_recovery_log_id.is_some()
            || raw.thread_recovery_pending
            || !raw.thread_recovery_pending_events.is_empty()
            || raw.thread_recovery_backfill_complete
            || raw.quota.is_some())
    {
        let account_state = AccountWatchState {
            last_signal_id: raw.last_signal_id,
            last_checked_at: raw.last_checked_at,
            last_live_email: raw.last_live_email,
            last_thread_recovery_log_id: raw.last_thread_recovery_log_id,
            thread_recovery_pending: raw.thread_recovery_pending,
            thread_recovery_pending_events: raw.thread_recovery_pending_events,
            thread_recovery_backfill_complete: raw.thread_recovery_backfill_complete,
            quota: raw.quota,
        };
        if let Some(account_id) = current_account_id {
            state.set_account_state(account_id.to_string(), account_state);
        }
    }

    state
}

fn current_watch_account_id() -> Option<String> {
    let paths = resolve_paths().ok()?;
    if !paths.codex_auth_file.exists() {
        return None;
    }
    let auth = load_codex_auth(&paths.codex_auth_file).ok()?;
    Some(summarize_codex_auth(&auth).account_id)
}

pub fn run_watch_iteration(options: WatchIterationOptions) -> Result<WatchIterationResult> {
    let port = options.port.unwrap_or(9333);
    let cooldown_ms = options.cooldown_ms.unwrap_or(DEFAULT_COOLDOWN_MS);
    let paths = resolve_paths()?;
    let logs_availability = codex_logs_availability(&paths.codex_logs_db_file)?;

    let previous_state = read_watch_state()?;
    let current_auth = load_or_restore_codex_auth(&paths.codex_auth_file)?;
    let mut current_summary = summarize_codex_auth(&current_auth);
    let previous_account_id = current_summary.account_id.clone();
    let previous_account_state = previous_state.account_state(&previous_account_id);

    let latest_codex_signal_id = read_latest_codex_signal_id(&paths.codex_logs_db_file)?;
    let mut after_signal_id = options
        .after_signal_id
        .or(previous_account_state.last_signal_id);
    let (normalized_after_signal_id, signal_log_cursor_reset) = normalize_log_cursor(
        after_signal_id,
        latest_codex_signal_id,
        SIGNAL_CURSOR_RESET_LOOKBACK_LOGS,
    );
    after_signal_id = normalized_after_signal_id;
    if after_signal_id.is_none() {
        after_signal_id = latest_codex_signal_id;
    }

    let (mut decision, mut quota_cache) = decide_rotation(
        &current_auth,
        &current_summary,
        after_signal_id,
        previous_account_state.quota.as_ref(),
        options.force_quota_refresh,
        previous_state.auto_create_enabled,
    )?;
    let source_quota_cache = quota_cache.clone();
    if signal_log_cursor_reset && decision.signals.is_empty() {
        decision.last_signal_id = latest_codex_signal_id;
    }
    let live_account = match read_live_account_if_running(Some(port))? {
        Some(live_account) => {
            ensure_live_account_matches_current_auth(port, &current_summary, live_account)?
        }
        None => AccountReadResult {
            account: None,
            requires_openai_auth: None,
        },
    };

    let mut rotated = false;
    let mut rotation = None;
    let mut live = Some(
        live_account
            .account
            .as_ref()
            .map(|account| LiveSwitchResult {
                email: account
                    .email
                    .clone()
                    .unwrap_or_else(|| current_summary.email.clone()),
                plan_type: account
                    .plan_type
                    .clone()
                    .unwrap_or_else(|| current_summary.plan_type.clone()),
                account_id: current_summary.account_id.clone(),
            })
            .unwrap_or_else(|| LiveSwitchResult {
                email: current_summary.email.clone(),
                plan_type: current_summary.plan_type.clone(),
                account_id: current_summary.account_id.clone(),
            }),
    );
    let source_live = live.clone();

    if decision.should_rotate && !cooldown_active(&previous_state, cooldown_ms) {
        rotation = execute_watch_rotation(
            decision.rotation_command,
            Some(port),
            options.progress.clone(),
        )?;
        if rotation.is_some() {
            let refreshed_auth = load_codex_auth(&paths.codex_auth_file)?;
            current_summary = summarize_codex_auth(&refreshed_auth);
            live = Some(LiveSwitchResult {
                email: current_summary.email.clone(),
                plan_type: current_summary.plan_type.clone(),
                account_id: current_summary.account_id.clone(),
            });
            quota_cache = Some(refresh_quota_cache_for_auth(
                &refreshed_auth,
                &current_summary,
                true,
                None,
            )?);
        }
        rotated = rotation.is_some();
    }

    let usage_limit_signal_seen = decision
        .signals
        .iter()
        .any(|signal| signal.kind == CodexSignalKind::UsageLimitReached);
    let account_changed = current_summary.account_id != previous_account_id;
    let current_account_previous_state = if account_changed {
        previous_state.account_state(&current_summary.account_id)
    } else {
        previous_account_state.clone()
    };
    let latest_recoverable_turn_failure_log_id = read_latest_recoverable_turn_failure_log_id()?;
    let (mut thread_recovery_log_id, recoverable_turn_failure_log_reset) = normalize_log_cursor(
        current_account_previous_state.last_thread_recovery_log_id,
        latest_recoverable_turn_failure_log_id,
        THREAD_RECOVERY_BOOTSTRAP_LOOKBACK_LOGS,
    );
    if !current_account_previous_state.thread_recovery_pending
        && !usage_limit_signal_seen
        && !rotated
        && thread_recovery_log_id.is_none()
    {
        thread_recovery_log_id = latest_recoverable_turn_failure_log_id;
    }
    let recoverable_turn_failure_log_advanced = latest_recoverable_turn_failure_log_id
        .zip(thread_recovery_log_id)
        .map(|(latest, current)| latest > current)
        .unwrap_or(false)
        || recoverable_turn_failure_log_reset;
    let bootstrap_thread_recovery =
        !current_account_previous_state.thread_recovery_backfill_complete;
    let now = now_iso();

    let mut next_state = previous_state.clone();
    next_state.last_rotation_at = if rotated {
        Some(now.clone())
    } else {
        previous_state.last_rotation_at.clone()
    };
    next_state.last_rotation_reason = if rotated {
        decision.reason.clone()
    } else {
        previous_state.last_rotation_reason.clone()
    };
    next_state.last_rotated_email = if rotated {
        rotation.as_ref().map(|summary| summary.email.clone())
    } else {
        previous_state.last_rotated_email.clone()
    };
    next_state.auto_create_enabled = previous_state.auto_create_enabled;
    next_state.tray_enabled = previous_state.tray_enabled;

    let mut source_account_state = previous_account_state.clone();
    source_account_state.last_signal_id = decision.last_signal_id;
    source_account_state.last_checked_at = Some(now.clone());
    source_account_state.last_live_email = source_live
        .as_ref()
        .map(|value| value.email.clone())
        .or_else(|| previous_account_state.last_live_email.clone());
    source_account_state.quota = source_quota_cache;
    next_state.set_account_state(previous_account_id.clone(), source_account_state.clone());

    let mut current_account_state = if account_changed {
        current_account_previous_state.clone()
    } else {
        source_account_state
    };
    current_account_state.last_checked_at = Some(now.clone());
    current_account_state.last_live_email = live
        .as_ref()
        .map(|value| value.email.clone())
        .or_else(|| current_account_previous_state.last_live_email.clone());
    current_account_state.last_thread_recovery_log_id = thread_recovery_log_id;
    current_account_state.thread_recovery_pending =
        current_account_previous_state.thread_recovery_pending;
    current_account_state.thread_recovery_pending_events = current_account_previous_state
        .thread_recovery_pending_events
        .clone();
    current_account_state.thread_recovery_backfill_complete =
        current_account_previous_state.thread_recovery_backfill_complete;
    current_account_state.quota = quota_cache.clone();
    if !account_changed {
        current_account_state.last_signal_id = decision.last_signal_id;
    }
    if should_run_thread_recovery(
        &current_account_previous_state,
        usage_limit_signal_seen,
        rotated,
        recoverable_turn_failure_log_advanced,
    ) {
        let recovery_last_log_id = if bootstrap_thread_recovery {
            current_account_state
                .last_thread_recovery_log_id
                .map(|id| id.saturating_sub(THREAD_RECOVERY_BOOTSTRAP_LOOKBACK_LOGS))
        } else {
            current_account_state.last_thread_recovery_log_id
        };
        match run_thread_recovery_iteration(RecoveryIterationOptions {
            port: Some(port),
            current_live_email: live.as_ref().map(|value| value.email.clone()).or_else(|| {
                current_account_state
                    .last_live_email
                    .as_ref()
                    .map(ToOwned::to_owned)
            }),
            current_quota_usable: current_account_state
                .quota
                .as_ref()
                .map(|quota| quota.usable),
            current_primary_quota_left_percent: current_account_state
                .quota
                .as_ref()
                .and_then(|quota| quota.primary_quota_left_percent),
            rotated,
            last_log_id: recovery_last_log_id,
            pending: current_account_state.thread_recovery_pending,
            pending_events: current_account_state.thread_recovery_pending_events.clone(),
        }) {
            Ok(recovery) => {
                current_account_state.last_thread_recovery_log_id = recovery.last_log_id;
                current_account_state.thread_recovery_pending = recovery.pending;
                current_account_state.thread_recovery_pending_events = recovery.pending_events;
                current_account_state.thread_recovery_backfill_complete = true;
            }
            Err(error) => {
                log_daemon_error(format!("thread recovery iteration failed: {error:#}"));
                eprintln!("codex-rotate: thread recovery iteration failed: {error:#}");
                current_account_state.thread_recovery_pending = current_account_state
                    .thread_recovery_pending
                    || !current_account_state
                        .thread_recovery_pending_events
                        .is_empty();
            }
        }
    }
    next_state.set_account_state(
        current_summary.account_id.clone(),
        current_account_state.clone(),
    );
    write_watch_state_if_needed(&previous_state, &next_state)?;

    Ok(WatchIterationResult {
        state: next_state,
        current_account_id: current_summary.account_id.clone(),
        current_account_state,
        decision,
        rotated,
        rotation,
        live,
        logs_availability,
    })
}

fn normalize_log_cursor(
    cursor: Option<i64>,
    latest_available_id: Option<i64>,
    lookback_logs: i64,
) -> (Option<i64>, bool) {
    match (cursor, latest_available_id) {
        (Some(current), Some(latest)) if current > latest => {
            (Some(latest.saturating_sub(lookback_logs.max(0))), true)
        }
        _ => (cursor, false),
    }
}

fn execute_watch_rotation(
    command: Option<RotationCommand>,
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<Option<AuthSummary>> {
    let port = port.unwrap_or(9333);
    match command {
        Some(RotationCommand::Next) => {
            let next_result =
                run_account_operation_with_log_isolation(Some(port), progress.clone(), || {
                    codex_rotate_core::pool::rotate_next_internal_with_progress(progress.clone())
                })?;
            Ok(next_result.current_summary.or(next_result.previous_summary))
        }
        Some(RotationCommand::Create) => {
            if let Some(progress) = progress.as_ref() {
                progress("Auto rotation is creating a replacement account.".to_string());
            }
            let paths = resolve_paths()?;
            let previous_summary = if paths.codex_auth_file.exists() {
                Some(summarize_codex_auth(&load_codex_auth(
                    &paths.codex_auth_file,
                )?))
            } else {
                None
            };
            let create_attempt = || {
                run_account_operation_with_log_isolation(Some(port), progress.clone(), || {
                    cmd_create_with_progress(
                        CreateCommandOptions {
                            force: true,
                            ignore_current: true,
                            require_usable_quota: true,
                            restore_previous_auth_after_create: false,
                            source: CreateCommandSource::Next,
                            ..CreateCommandOptions::default()
                        },
                        progress.clone(),
                    )
                })
            };
            match create_attempt() {
                Ok(result) => return Ok(result.current_summary.or(result.previous_summary)),
                Err(error) if is_auto_create_retry_stopped_for_reusable_account(&error) => {
                    let next_result = run_account_operation_with_log_isolation(
                        Some(port),
                        progress.clone(),
                        || {
                            codex_rotate_core::pool::rotate_next_internal_with_progress(
                                progress.clone(),
                            )
                        },
                    )?;
                    return Ok(next_result.current_summary.or(next_result.previous_summary));
                }
                Err(error) if is_retryable_watch_create_error(&error) => {
                    if let Err(retry_error) = create_attempt() {
                        if is_auto_create_retry_stopped_for_reusable_account(&retry_error) {
                            let next_result = run_account_operation_with_log_isolation(
                                Some(port),
                                progress.clone(),
                                || {
                                    codex_rotate_core::pool::rotate_next_internal_with_progress(
                                        progress.clone(),
                                    )
                                },
                            )?;
                            return Ok(next_result
                                .current_summary
                                .or(next_result.previous_summary));
                        }
                        if let Some(summary) =
                            recover_completed_watch_create(previous_summary.as_ref())?
                        {
                            return Ok(Some(summary));
                        }
                        return Err(retry_error);
                    }
                }
                Err(error) => {
                    if let Some(summary) =
                        recover_completed_watch_create(previous_summary.as_ref())?
                    {
                        return Ok(Some(summary));
                    }
                    return Err(error);
                }
            }

            let refreshed_auth = load_codex_auth(&paths.codex_auth_file)?;
            Ok(Some(summarize_codex_auth(&refreshed_auth)))
        }
        None => Ok(None),
    }
}

fn recover_completed_watch_create(
    previous_summary: Option<&AuthSummary>,
) -> Result<Option<AuthSummary>> {
    let paths = resolve_paths()?;
    if !paths.codex_auth_file.exists() {
        return Ok(None);
    }
    let current_auth = load_codex_auth(&paths.codex_auth_file)?;
    let current_summary = summarize_codex_auth(&current_auth);
    let pool = load_pool()?;
    if created_account_already_materialized(previous_summary, &current_summary, &pool) {
        return Ok(Some(current_summary));
    }
    Ok(None)
}

fn created_account_already_materialized(
    previous_summary: Option<&AuthSummary>,
    current_summary: &AuthSummary,
    pool: &Pool,
) -> bool {
    if previous_summary
        .map(|previous| same_auth_summary(previous, current_summary))
        .unwrap_or(false)
    {
        return false;
    }

    let current_account_id = current_summary.account_id.trim();
    let current_email = normalize_email_for_match(&current_summary.email);
    pool.accounts.iter().any(|entry| {
        entry.account_id.trim() == current_account_id
            || entry.auth.tokens.account_id.trim() == current_account_id
            || normalize_email_for_match(&entry.email) == current_email
    })
}

fn same_auth_summary(left: &AuthSummary, right: &AuthSummary) -> bool {
    left.account_id.trim() == right.account_id.trim()
        || normalize_email_for_match(&left.email) == normalize_email_for_match(&right.email)
}

fn normalize_email_for_match(email: &str) -> String {
    email.trim().to_ascii_lowercase()
}

fn is_retryable_watch_create_error(error: &anyhow::Error) -> bool {
    let message = error.to_string();
    is_create_already_in_progress_error(error)
        || message.contains("Daemon closed the socket before sending a response")
        || (message.contains("fast-browser workflow ") && message.contains(" exited with status 1"))
}

pub fn refresh_quota_cache(
    force_refresh: bool,
    previous: Option<&CachedQuotaState>,
) -> Result<CachedQuotaState> {
    let paths = resolve_paths()?;
    let auth = load_or_restore_codex_auth(&paths.codex_auth_file)?;
    let summary = summarize_codex_auth(&auth);
    refresh_quota_cache_for_auth(&auth, &summary, force_refresh, previous)
}

fn load_or_restore_codex_auth(path: &Path) -> Result<CodexAuth> {
    match load_codex_auth(path) {
        Ok(auth) => Ok(auth),
        Err(error) if !path.exists() => {
            if restore_codex_auth_from_active_pool()? {
                load_codex_auth(path)
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

fn refresh_quota_cache_for_auth(
    auth: &CodexAuth,
    summary: &AuthSummary,
    force_refresh: bool,
    previous: Option<&CachedQuotaState>,
) -> Result<CachedQuotaState> {
    let now = Utc::now();
    if !force_refresh && !quota_cache_is_stale(previous, &summary.account_id, now) {
        if let Some(previous) = previous {
            return Ok(previous.clone());
        }
    }
    match inspect_quota(auth) {
        Ok(assessment) => Ok(build_cached_quota_state(
            &summary.account_id,
            Some(&assessment),
            None,
            now,
        )),
        Err(error) => Ok(build_cached_quota_state(
            &summary.account_id,
            None,
            Some(&error.to_string()),
            now,
        )),
    }
}

fn ensure_live_account_matches_current_auth(
    port: u16,
    summary: &AuthSummary,
    live_account: AccountReadResult,
) -> Result<AccountReadResult> {
    if live_account_matches_summary(&live_account, &summary) {
        return Ok(live_account);
    }
    let switched = switch_live_account_to_current_auth(Some(port), false, 15_000)?;
    Ok(AccountReadResult {
        account: Some(crate::hook::LiveAccount {
            account_type: None,
            email: Some(switched.email),
            plan_type: Some(switched.plan_type),
        }),
        requires_openai_auth: Some(false),
    })
}

fn decide_rotation(
    auth: &CodexAuth,
    summary: &AuthSummary,
    after_signal_id: Option<i64>,
    previous_cache: Option<&CachedQuotaState>,
    force_quota_refresh: bool,
    auto_create_enabled: bool,
) -> Result<(RotationDecision, Option<CachedQuotaState>)> {
    let paths = resolve_paths()?;
    let signals = read_codex_signals(&paths.codex_logs_db_file, after_signal_id, 50)?;
    let last_signal_id = signals.last().map(|signal| signal.id).or(after_signal_id);
    let cache_invalidated = quota_cache_invalidated(previous_cache, &summary.account_id, &signals)?;

    let quota_cache = if force_quota_refresh || cache_invalidated {
        Some(refresh_quota_cache_for_auth(
            auth,
            summary,
            true,
            previous_cache,
        )?)
    } else {
        Some(refresh_quota_cache_for_auth(
            auth,
            summary,
            false,
            previous_cache,
        )?)
    };

    let assessment = quota_cache
        .as_ref()
        .filter(|cache| cache.error.is_none())
        .map(|cache| DecisionQuotaAssessment {
            summary: cache.summary.clone(),
            usable: cache.usable,
            blocker: cache.blocker.clone(),
            primary_quota_left_percent: cache.primary_quota_left_percent,
        });
    let assessment_error = quota_cache.as_ref().and_then(|cache| cache.error.clone());
    let has_usable_other_account = assessment
        .as_ref()
        .and_then(|value| value.primary_quota_left_percent)
        .map(|value| value <= LOW_QUOTA_ROTATION_THRESHOLD_PERCENT)
        .unwrap_or(false)
        && other_usable_account_exists()?;

    let plan = plan_rotation(
        assessment.as_ref(),
        &signals,
        has_usable_other_account,
        auto_create_enabled,
    );
    Ok((
        RotationDecision {
            last_signal_id,
            signals,
            assessment,
            assessment_error,
            should_rotate: plan.0,
            reason: plan.1,
            rotation_command: plan.2,
        },
        quota_cache,
    ))
}

fn quota_cache_invalidated(
    cache: Option<&CachedQuotaState>,
    account_id: &str,
    signals: &[CodexLogSignal],
) -> Result<bool> {
    let Some(cache) = cache else {
        return Ok(true);
    };
    if cache.account_id != account_id {
        return Ok(true);
    }
    let fetched_at = DateTime::parse_from_rfc3339(&cache.fetched_at)
        .map(|value| value.timestamp_millis())
        .unwrap_or(0);
    for signal in signals {
        match signal.kind {
            CodexSignalKind::UsageLimitReached => return Ok(true),
            CodexSignalKind::RateLimitsUpdated if signal.ts > fetched_at => return Ok(true),
            CodexSignalKind::RateLimitsUpdated => {}
        }
    }
    Ok(false)
}

fn plan_rotation(
    assessment: Option<&DecisionQuotaAssessment>,
    signals: &[CodexLogSignal],
    has_usable_other_account: bool,
    auto_create_enabled: bool,
) -> (bool, Option<String>, Option<RotationCommand>, Vec<String>) {
    let Some(assessment) = assessment else {
        return (
            false,
            if signals.is_empty() {
                None
            } else {
                Some("quota assessment unavailable".to_string())
            },
            None,
            Vec::new(),
        );
    };

    if !assessment.usable {
        return (
            true,
            assessment.blocker.clone(),
            Some(RotationCommand::Next),
            Vec::new(),
        );
    }

    if assessment
        .primary_quota_left_percent
        .map(|value| value <= LOW_QUOTA_ROTATION_THRESHOLD_PERCENT)
        .unwrap_or(false)
    {
        let percent = assessment.primary_quota_left_percent.unwrap();
        if !auto_create_enabled {
            return (
                false,
                Some(format!("quota low: {percent}% left, auto create disabled")),
                None,
                Vec::new(),
            );
        }
        if has_usable_other_account {
            return (
                false,
                Some(format!(
                    "quota low: {percent}% left, but another account already has usable quota"
                )),
                None,
                Vec::new(),
            );
        }
        return (
            true,
            Some(format!("quota low: {percent}% left")),
            Some(RotationCommand::Create),
            vec!["--ignore-current".to_string()],
        );
    }

    (false, None, None, Vec::new())
}

fn cooldown_active(state: &WatchState, cooldown_ms: u64) -> bool {
    let Some(last_rotation_at) = &state.last_rotation_at else {
        return false;
    };
    let Ok(last_rotation_at) = DateTime::parse_from_rfc3339(last_rotation_at) else {
        return false;
    };
    let elapsed_ms = Utc::now()
        .signed_duration_since(last_rotation_at.with_timezone(&Utc))
        .num_milliseconds();
    elapsed_ms < cooldown_ms as i64
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn should_run_thread_recovery(
    previous: &AccountWatchState,
    usage_limit_signal_seen: bool,
    rotated: bool,
    recoverable_turn_failure_log_advanced: bool,
) -> bool {
    !previous.thread_recovery_backfill_complete
        || usage_limit_signal_seen
        || rotated
        || previous.thread_recovery_pending
        || recoverable_turn_failure_log_advanced
}

fn write_watch_state_if_needed(previous: &WatchState, next: &WatchState) -> Result<()> {
    if should_persist_watch_state(previous, next) {
        write_watch_state(next)?;
    }
    Ok(())
}

fn should_persist_watch_state(previous: &WatchState, next: &WatchState) -> bool {
    if previous == next {
        return false;
    }
    let mut previous_normalized = previous.clone();
    let mut next_normalized = next.clone();
    for state in previous_normalized.accounts.values_mut() {
        state.last_checked_at = None;
    }
    for state in next_normalized.accounts.values_mut() {
        state.last_checked_at = None;
    }
    previous_normalized != next_normalized
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_mutex;
    use anyhow::anyhow;

    fn state_for_account(account_id: &str, account_state: AccountWatchState) -> WatchState {
        let mut state = WatchState::default();
        state.set_account_state(account_id.to_string(), account_state);
        state
    }

    #[test]
    fn plan_rotation_uses_create_for_low_quota() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 20% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(20),
        };
        let plan = plan_rotation(Some(&assessment), &[], false, true);
        assert!(plan.0);
        assert_eq!(plan.2, Some(RotationCommand::Create));
        assert_eq!(plan.3, vec!["--ignore-current".to_string()]);
    }

    #[test]
    fn plan_rotation_uses_next_for_unusable_quota() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 0% left".to_string(),
            usable: false,
            blocker: Some("5h quota exhausted".to_string()),
            primary_quota_left_percent: Some(0),
        };
        let plan = plan_rotation(Some(&assessment), &[], false, true);
        assert!(plan.0);
        assert_eq!(plan.2, Some(RotationCommand::Next));
    }

    #[test]
    fn plan_rotation_skips_create_for_low_quota_when_other_account_is_usable() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 20% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(20),
        };
        let plan = plan_rotation(Some(&assessment), &[], true, true);
        assert!(!plan.0);
        assert_eq!(plan.2, None);
    }

    #[test]
    fn plan_rotation_skips_create_when_auto_create_is_disabled() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 20% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(20),
        };
        let plan = plan_rotation(Some(&assessment), &[], false, false);
        assert!(!plan.0);
        assert_eq!(plan.2, None);
        assert_eq!(
            plan.1.as_deref(),
            Some("quota low: 20% left, auto create disabled")
        );
    }

    #[test]
    fn retryable_watch_create_error_matches_fast_browser_exit() {
        let error = anyhow!(
            "fast-browser workflow workspace.web.auth-openai-com.codex-rotate-account-flow-main exited with status 1."
        );
        assert!(is_retryable_watch_create_error(&error));
    }

    #[test]
    fn retryable_watch_create_error_matches_create_lock_contention() {
        let error = anyhow!("Another create command is already in progress (pid 42, started 2026-04-11T00:00:00.000Z, source manual, profile dev-1).");
        assert!(is_retryable_watch_create_error(&error));
    }

    #[test]
    fn retryable_watch_create_error_ignores_non_transient_failures() {
        let error = anyhow!("quota inspection unavailable");
        assert!(!is_retryable_watch_create_error(&error));
    }

    #[test]
    fn load_or_restore_codex_auth_restores_missing_file_from_active_pool() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let restored_auth = codex_rotate_core::auth::CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: codex_rotate_core::auth::AuthTokens {
                    id_token: "id".to_string(),
                    access_token: "access".to_string(),
                    refresh_token: None,
                    account_id: "acct-restore".to_string(),
                },
                last_refresh: "2026-04-15T00:00:00.000Z".to_string(),
            };

            let paths = resolve_paths()?;
            fs::write(
                paths.rotate_home.join("accounts.json"),
                serde_json::to_string_pretty(&serde_json::json!({
                    "accounts": [{
                        "label": "dev.restore@astronlab.com_free",
                        "email": "dev.restore@astronlab.com",
                        "account_id": "acct-restore",
                        "plan_type": "free",
                        "auth": restored_auth,
                        "added_at": "2026-04-15T00:00:00.000Z"
                    }],
                    "active_index": 0
                }))?,
            )?;
            assert!(!paths.codex_auth_file.exists());

            let auth = load_or_restore_codex_auth(&paths.codex_auth_file)?;
            assert_eq!(auth.tokens.account_id, "acct-restore");
            assert!(paths.codex_auth_file.exists());
            Ok(())
        })();

        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }
        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }

        result.expect("missing auth should be restored from the active pool");
    }

    #[test]
    fn created_account_materialized_requires_a_new_auth_target() {
        let previous = AuthSummary {
            email: "dev.1@astronlab.com".to_string(),
            account_id: "acct-1".to_string(),
            plan_type: "free".to_string(),
        };
        let pool = Pool {
            active_index: 0,
            accounts: Vec::new(),
        };
        assert!(!created_account_already_materialized(
            Some(&previous),
            &previous,
            &pool
        ));
    }

    #[test]
    fn created_account_materialized_accepts_new_auth_present_in_pool() {
        let previous = AuthSummary {
            email: "dev.1@astronlab.com".to_string(),
            account_id: "acct-1".to_string(),
            plan_type: "free".to_string(),
        };
        let current = AuthSummary {
            email: "dev.2@astronlab.com".to_string(),
            account_id: "acct-2".to_string(),
            plan_type: "free".to_string(),
        };
        let pool = Pool {
            active_index: 0,
            accounts: vec![codex_rotate_core::pool::AccountEntry {
                label: "dev.2@astronlab.com_free".to_string(),
                alias: None,
                email: "dev.2@astronlab.com".to_string(),
                account_id: "acct-2".to_string(),
                plan_type: "free".to_string(),
                auth: codex_rotate_core::auth::CodexAuth {
                    auth_mode: "chatgpt".to_string(),
                    openai_api_key: None,
                    tokens: codex_rotate_core::auth::AuthTokens {
                        id_token: "id".to_string(),
                        access_token: "access".to_string(),
                        refresh_token: None,
                        account_id: "acct-2".to_string(),
                    },
                    last_refresh: "2026-04-06T00:00:00.000Z".to_string(),
                },
                added_at: "2026-04-06T00:00:00.000Z".to_string(),
                last_quota_usable: None,
                last_quota_summary: None,
                last_quota_blocker: None,
                last_quota_checked_at: None,
                last_quota_primary_left_percent: None,
                last_quota_next_refresh_at: None,
            }],
        };
        assert!(created_account_already_materialized(
            Some(&previous),
            &current,
            &pool
        ));
    }

    #[test]
    fn usage_limit_signal_invalidates_cache_immediately() {
        let cache = CachedQuotaState {
            account_id: "acct-123".to_string(),
            fetched_at: "2026-04-03T12:00:00.000Z".to_string(),
            next_refresh_at: "2026-04-03T12:05:00.000Z".to_string(),
            summary: "5h 40% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(40),
            error: None,
        };
        let signals = vec![CodexLogSignal {
            id: 1,
            ts: 1_775_181_601_000,
            kind: CodexSignalKind::UsageLimitReached,
            target: "log".to_string(),
            body: "Received message".to_string(),
        }];
        assert!(quota_cache_invalidated(Some(&cache), "acct-123", &signals).unwrap());
    }

    #[test]
    fn rate_limit_update_only_invalidates_when_newer_than_cache() {
        let fetched_at = DateTime::parse_from_rfc3339("2026-04-03T12:00:00.000Z")
            .unwrap()
            .timestamp_millis();
        let cache = CachedQuotaState {
            account_id: "acct-123".to_string(),
            fetched_at: "2026-04-03T12:00:00.000Z".to_string(),
            next_refresh_at: "2026-04-03T12:05:00.000Z".to_string(),
            summary: "5h 40% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(40),
            error: None,
        };
        let older_signal = vec![CodexLogSignal {
            id: 1,
            ts: fetched_at - 1_000,
            kind: CodexSignalKind::RateLimitsUpdated,
            target: "codex_app_server::outgoing_message".to_string(),
            body: "account/rateLimits/updated".to_string(),
        }];
        assert!(!quota_cache_invalidated(Some(&cache), "acct-123", &older_signal).unwrap());

        let newer_signal = vec![CodexLogSignal {
            ts: fetched_at + 1_000,
            ..older_signal[0].clone()
        }];
        assert!(quota_cache_invalidated(Some(&cache), "acct-123", &newer_signal).unwrap());
    }

    #[test]
    fn watch_state_write_skips_heartbeat_only_changes() {
        let previous = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:00.000Z".to_string()),
                ..AccountWatchState::default()
            },
        );
        let next = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:15.000Z".to_string()),
                ..previous.account_state("acct-123")
            },
        );
        assert!(!should_persist_watch_state(&previous, &next));
    }

    #[test]
    fn watch_state_write_keeps_signal_progress() {
        let previous = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:00.000Z".to_string()),
                last_signal_id: Some(10),
                ..AccountWatchState::default()
            },
        );
        let next = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:15.000Z".to_string()),
                last_signal_id: Some(11),
                ..previous.account_state("acct-123")
            },
        );
        assert!(should_persist_watch_state(&previous, &next));
    }

    #[test]
    fn watch_state_write_keeps_thread_recovery_progress() {
        let previous = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:00.000Z".to_string()),
                last_thread_recovery_log_id: Some(10),
                thread_recovery_pending: true,
                ..AccountWatchState::default()
            },
        );
        let next = state_for_account(
            "acct-123",
            AccountWatchState {
                last_checked_at: Some("2026-04-03T12:00:15.000Z".to_string()),
                last_thread_recovery_log_id: Some(11),
                thread_recovery_pending: false,
                ..previous.account_state("acct-123")
            },
        );
        assert!(should_persist_watch_state(&previous, &next));
    }

    #[test]
    fn thread_recovery_runs_for_pending_state() {
        assert!(should_run_thread_recovery(
            &AccountWatchState {
                thread_recovery_pending: true,
                ..AccountWatchState::default()
            },
            false,
            false,
            false
        ));
    }

    #[test]
    fn thread_recovery_runs_for_bootstrap_backfill() {
        assert!(should_run_thread_recovery(
            &AccountWatchState::default(),
            false,
            false,
            false
        ));
    }

    #[test]
    fn thread_recovery_runs_when_recoverable_turn_failure_log_advances() {
        assert!(should_run_thread_recovery(
            &AccountWatchState {
                thread_recovery_backfill_complete: true,
                ..AccountWatchState::default()
            },
            false,
            false,
            true
        ));
    }

    #[test]
    fn thread_recovery_stays_idle_without_signal_rotation_pending_or_new_log() {
        assert!(!should_run_thread_recovery(
            &AccountWatchState {
                thread_recovery_backfill_complete: true,
                ..AccountWatchState::default()
            },
            false,
            false,
            false
        ));
    }

    #[test]
    fn read_watch_state_migrates_legacy_flat_shape_into_current_account() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = tempfile::tempdir().expect("tempdir");
        let rotate_home = tempdir.path().join("rotate");
        let codex_home = tempdir.path().join("codex");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_HOME", &codex_home);
        }

        let result = (|| -> Result<()> {
            let auth = codex_rotate_core::auth::CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: codex_rotate_core::auth::AuthTokens {
                    id_token: "id".to_string(),
                    access_token: "access".to_string(),
                    refresh_token: None,
                    account_id: "acct-legacy".to_string(),
                },
                last_refresh: "2026-04-15T00:00:00.000Z".to_string(),
            };
            write_private_string(
                &codex_home.join("auth.json"),
                &serde_json::to_string_pretty(&auth)?,
            )?;
            write_private_string(
                &rotate_home.join("watch-state.json"),
                &serde_json::json!({
                    "lastSignalId": 42,
                    "lastCheckedAt": "2026-04-03T12:00:00.000Z",
                    "lastLiveEmail": "dev.legacy@astronlab.com",
                    "threadRecoveryPending": true,
                    "autoCreateEnabled": false,
                    "trayEnabled": true
                })
                .to_string(),
            )?;

            let state = read_watch_state()?;
            let account = state.account_state("acct-legacy");
            assert_eq!(account.last_signal_id, Some(42));
            assert_eq!(
                account.last_live_email.as_deref(),
                Some("dev.legacy@astronlab.com")
            );
            assert!(account.thread_recovery_pending);
            assert!(!state.auto_create_enabled);
            assert!(state.tray_enabled);
            Ok(())
        })();

        match previous_codex_home {
            Some(value) => unsafe { std::env::set_var("CODEX_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_HOME") },
        }
        match previous_rotate_home {
            Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { std::env::remove_var("CODEX_ROTATE_HOME") },
        }

        result.expect("legacy watch state should migrate into the current account");
    }

    #[test]
    fn normalize_log_cursor_resets_when_current_db_ids_roll_over() {
        assert_eq!(
            normalize_log_cursor(Some(9_174_8411), Some(91_649), 2_000),
            (Some(89_649), true)
        );
    }

    #[test]
    fn normalize_log_cursor_keeps_current_when_id_space_is_consistent() {
        assert_eq!(
            normalize_log_cursor(Some(91_000), Some(91_649), 2_000),
            (Some(91_000), false)
        );
    }
}
