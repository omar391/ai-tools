use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

use anyhow::{Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth, AuthSummary, CodexAuth};
use codex_rotate_core::pool::{other_usable_account_exists, rotate_next_internal, NextResult};
use codex_rotate_core::quota::{
    build_cached_quota_state, inspect_quota, quota_cache_is_stale, CachedQuotaState,
};
use codex_rotate_core::workflow::{cmd_create, CreateCommandOptions, CreateCommandSource};
use serde::{Deserialize, Serialize};

use crate::hook::{
    live_account_matches_summary, read_live_account, switch_live_account_to_current_auth,
    AccountReadResult, LiveSwitchResult,
};
use crate::logs::{
    read_codex_signals, read_latest_codex_signal_id, CodexLogSignal, CodexSignalKind,
};
use crate::paths::resolve_paths;

pub const LOW_QUOTA_ROTATION_THRESHOLD_PERCENT: u8 = 10;
pub const DEFAULT_COOLDOWN_MS: u64 = 15_000;

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct WatchState {
    pub last_signal_id: Option<i64>,
    pub last_checked_at: Option<String>,
    pub last_live_email: Option<String>,
    pub last_rotation_at: Option<String>,
    pub last_rotation_reason: Option<String>,
    pub last_rotated_email: Option<String>,
    pub quota: Option<CachedQuotaState>,
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
    pub rotation_args: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchIterationResult {
    pub state: WatchState,
    pub decision: RotationDecision,
    pub rotated: bool,
    pub rotation: Option<AuthSummary>,
    pub live: Option<LiveSwitchResult>,
}

pub struct WatchIterationOptions {
    pub port: Option<u16>,
    pub after_signal_id: Option<i64>,
    pub cooldown_ms: Option<u64>,
    pub force_quota_refresh: bool,
}

pub fn read_watch_state() -> Result<WatchState> {
    let paths = resolve_paths()?;
    if !paths.rotate_app_home.join("watch-state.json").exists() {
        return Ok(WatchState::default());
    }
    let raw = fs::read_to_string(paths.rotate_app_home.join("watch-state.json"))
        .context("Failed to read watch-state.json.")?;
    let state = serde_json::from_str::<WatchState>(&raw).unwrap_or_default();
    Ok(state)
}

pub fn write_watch_state(state: &WatchState) -> Result<()> {
    let paths = resolve_paths()?;
    fs::create_dir_all(&paths.rotate_app_home)
        .with_context(|| format!("Failed to create {}.", paths.rotate_app_home.display()))?;
    let path = paths.rotate_app_home.join("watch-state.json");
    let raw = serde_json::to_string_pretty(state)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .with_context(|| format!("Failed to write {}.", path.display()))?;
    file.write_all(raw.as_bytes())?;
    Ok(())
}

pub fn run_watch_iteration(options: WatchIterationOptions) -> Result<WatchIterationResult> {
    let port = options.port.unwrap_or(9333);
    let cooldown_ms = options.cooldown_ms.unwrap_or(DEFAULT_COOLDOWN_MS);
    let paths = resolve_paths()?;

    let previous_state = read_watch_state()?;
    let mut after_signal_id = options.after_signal_id.or(previous_state.last_signal_id);
    if after_signal_id.is_none() {
        after_signal_id = read_latest_codex_signal_id(&paths.codex_logs_db_file)?;
    }

    let current_auth = load_codex_auth(&paths.codex_auth_file)?;
    let current_summary = summarize_codex_auth(&current_auth);
    let (decision, mut quota_cache) = decide_rotation(
        &current_auth,
        &current_summary,
        after_signal_id,
        previous_state.quota.as_ref(),
        options.force_quota_refresh,
    )?;
    let live_account = ensure_live_account_matches_current_auth(
        port,
        &current_summary,
        read_live_account(Some(port))?,
    )?;

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

    if decision.should_rotate && !cooldown_active(&previous_state, cooldown_ms) {
        let mut refreshed_current = false;
        match decision.rotation_command {
            Some(RotationCommand::Next) => {
                let next_result = rotate_next_internal()?;
                let summary = match next_result {
                    NextResult::Rotated { summary, .. }
                    | NextResult::Stayed { summary, .. }
                    | NextResult::Created { summary, .. } => summary,
                };
                rotation = Some(summary);
            }
            Some(RotationCommand::Create) => {
                cmd_create(CreateCommandOptions {
                    force: true,
                    ignore_current: true,
                    restore_previous_auth_after_create: true,
                    source: CreateCommandSource::Manual,
                    ..CreateCommandOptions::default()
                })?;
                refreshed_current = true;
            }
            None => {}
        }
        if rotation.is_some() || refreshed_current {
            let reload_after_switch = rotation.is_some()
                && matches!(decision.rotation_command, Some(RotationCommand::Next));
            live = Some(switch_live_account_to_current_auth(
                Some(port),
                false,
                15_000,
                reload_after_switch,
            )?);
            let refreshed_auth = load_codex_auth(&paths.codex_auth_file)?;
            let refreshed_summary = summarize_codex_auth(&refreshed_auth);
            quota_cache = Some(refresh_quota_cache_for_auth(
                &refreshed_auth,
                &refreshed_summary,
                true,
                None,
            )?);
        }
        rotated = rotation.is_some();
    }

    let next_state = WatchState {
        last_signal_id: decision.last_signal_id,
        last_checked_at: Some(now_iso()),
        last_live_email: live
            .as_ref()
            .map(|value| value.email.clone())
            .or_else(|| {
                live_account
                    .account
                    .as_ref()
                    .and_then(|account| account.email.clone())
            })
            .or_else(|| previous_state.last_live_email.clone()),
        last_rotation_at: if rotated {
            Some(now_iso())
        } else {
            previous_state.last_rotation_at.clone()
        },
        last_rotation_reason: if rotated {
            decision.reason.clone()
        } else {
            previous_state.last_rotation_reason.clone()
        },
        last_rotated_email: if rotated {
            rotation.as_ref().map(|summary| summary.email.clone())
        } else {
            previous_state.last_rotated_email.clone()
        },
        quota: quota_cache,
    };
    write_watch_state_if_needed(&previous_state, &next_state)?;

    Ok(WatchIterationResult {
        state: next_state,
        decision,
        rotated,
        rotation,
        live,
    })
}

pub fn refresh_quota_cache(
    force_refresh: bool,
    previous: Option<&CachedQuotaState>,
) -> Result<CachedQuotaState> {
    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    let summary = summarize_codex_auth(&auth);
    refresh_quota_cache_for_auth(&auth, &summary, force_refresh, previous)
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
    let switched = switch_live_account_to_current_auth(Some(port), false, 15_000, false)?;
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

    let plan = plan_rotation(assessment.as_ref(), &signals, has_usable_other_account);
    Ok((
        RotationDecision {
            last_signal_id,
            signals,
            assessment,
            assessment_error,
            should_rotate: plan.0,
            reason: plan.1,
            rotation_command: plan.2,
            rotation_args: plan.3,
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
    previous_normalized.last_checked_at = None;
    next_normalized.last_checked_at = None;
    previous_normalized != next_normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_rotation_uses_create_for_low_quota() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 10% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(10),
        };
        let plan = plan_rotation(Some(&assessment), &[], false);
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
        let plan = plan_rotation(Some(&assessment), &[], false);
        assert!(plan.0);
        assert_eq!(plan.2, Some(RotationCommand::Next));
    }

    #[test]
    fn plan_rotation_skips_create_for_low_quota_when_other_account_is_usable() {
        let assessment = DecisionQuotaAssessment {
            summary: "5h 10% left".to_string(),
            usable: true,
            blocker: None,
            primary_quota_left_percent: Some(10),
        };
        let plan = plan_rotation(Some(&assessment), &[], true);
        assert!(!plan.0);
        assert_eq!(plan.2, None);
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
        let previous = WatchState {
            last_checked_at: Some("2026-04-03T12:00:00.000Z".to_string()),
            ..WatchState::default()
        };
        let next = WatchState {
            last_checked_at: Some("2026-04-03T12:00:15.000Z".to_string()),
            ..previous.clone()
        };
        assert!(!should_persist_watch_state(&previous, &next));
    }

    #[test]
    fn watch_state_write_keeps_signal_progress() {
        let previous = WatchState {
            last_checked_at: Some("2026-04-03T12:00:00.000Z".to_string()),
            last_signal_id: Some(10),
            ..WatchState::default()
        };
        let next = WatchState {
            last_checked_at: Some("2026-04-03T12:00:15.000Z".to_string()),
            last_signal_id: Some(11),
            ..previous.clone()
        };
        assert!(should_persist_watch_state(&previous, &next));
    }
}
