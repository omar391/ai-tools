use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::cdp::{invalidate_local_codex_connection, with_local_codex_connection};
use crate::hook::read_live_account;
use crate::launcher::ensure_debug_codex_instance;
use crate::paths::resolve_paths;
use crate::rotation_hygiene::ConversationSyncStore;
use crate::runtime_log::log_daemon_error;

const DEFAULT_PORT: u16 = 9333;
const MAX_RECOVERY_SCAN_EVENTS: usize = 32;
const CONTINUE_INPUT: &str = "continue with skipped msgs";
const MCP_RESPONSE_TIMEOUT_MS: u64 = 8_000;
const HEALTHY_QUOTA_CONTINUE_THRESHOLD_PERCENT: u8 = 10;
const OTEL_METADATA_LOOKUP_WINDOW: i64 = 2_000;
const THREAD_RESUME_SETTLE_MS: u64 = 1_000;
const QUOTA_EXHAUSTION_ERROR_MESSAGE: &str = "You've hit your usage limit.";
const MODEL_CAPACITY_ERROR_MESSAGE: &str =
    "Selected model is at capacity. Please try a different model.";
const TRANSPORT_DISCONNECT_ERROR_PREFIX: &str = "stream disconnected before completion:";
const TRANSIENT_RECOVERY_RETRY_DELAY_SECS: i64 = 15;
const STALLED_TURN_RECOVERY_DELAY_SECS: i64 = 180;
const MAX_STALLED_THREAD_SCAN_THREADS: usize = 16;
const ROLLOUT_TAIL_SCAN_BYTES: usize = 262_144;
const SYNTHETIC_RECOVERY_SOURCE_LOG_ID: i64 = 0;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ThreadRecoveryKind {
    #[default]
    QuotaExhausted,
    ModelCapacity,
    TransportDisconnected,
    StalledTurn,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRecoveryRehydration {
    pub lineage_id: String,
    pub cwd: Option<String>,
    #[serde(default)]
    pub items: Vec<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ThreadRecoveryEvent {
    pub source_log_id: i64,
    pub source_ts: i64,
    pub thread_id: String,
    #[serde(default)]
    pub kind: ThreadRecoveryKind,
    pub exhausted_turn_id: Option<String>,
    pub exhausted_email: Option<String>,
    pub exhausted_account_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rehydration: Option<ThreadRecoveryRehydration>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryIterationResult {
    pub last_log_id: Option<i64>,
    pub pending: bool,
    pub pending_events: Vec<ThreadRecoveryEvent>,
    pub detected: usize,
    pub continued_thread_ids: Vec<String>,
    pub dropped_thread_ids: Vec<String>,
}

#[derive(Clone, Debug, Default)]
pub struct RecoveryIterationOptions {
    pub port: Option<u16>,
    pub current_live_email: Option<String>,
    pub current_quota_usable: Option<bool>,
    pub current_primary_quota_left_percent: Option<u8>,
    pub rotated: bool,
    pub last_log_id: Option<i64>,
    pub pending: bool,
    pub pending_events: Vec<ThreadRecoveryEvent>,
    pub detect_only: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct OtelFailureMetadata {
    exhausted_email: Option<String>,
    exhausted_account_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ThreadReadEnvelope {
    thread: ThreadSummary,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct ThreadSummary {
    id: String,
    cwd: Option<String>,
    status: ThreadStatus,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct ThreadStatus {
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecoveryResolution {
    Continued,
    Dropped,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ContinueStrategy {
    Direct { cwd: Option<String> },
    DirectThenResume { initial_cwd: Option<String> },
    ResumeThenContinue { initial_cwd: Option<String> },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RecoveryProcessingResult {
    continued_thread_ids: Vec<String>,
    dropped_thread_ids: Vec<String>,
    pending_events: Vec<ThreadRecoveryEvent>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct StalledTurnSnapshot {
    thread_id: String,
    turn_id: String,
    last_event_ts: i64,
}

pub fn read_latest_recoverable_turn_failure_log_id() -> Result<Option<i64>> {
    let paths = resolve_paths()?;
    let Some(connection) = open_logs_connection_if_available(&paths.codex_logs_db_file)? else {
        return Ok(None);
    };
    read_latest_recoverable_turn_failure_log_id_from_connection(&connection)
}

pub fn run_thread_recovery_iteration(
    options: RecoveryIterationOptions,
) -> Result<RecoveryIterationResult> {
    let port = options.port.unwrap_or(DEFAULT_PORT);
    let paths = resolve_paths()?;
    let pending = options.pending || !options.pending_events.is_empty();
    let Some(connection) = open_logs_connection_if_available(&paths.codex_logs_db_file)? else {
        return Ok(RecoveryIterationResult {
            last_log_id: options.last_log_id,
            pending,
            pending_events: options.pending_events,
            detected: 0,
            continued_thread_ids: Vec::new(),
            dropped_thread_ids: Vec::new(),
        });
    };

    let seeded_last_log_id = if options.last_log_id.is_none() && !pending {
        read_latest_recoverable_turn_failure_log_id_from_connection(&connection)?
    } else {
        options.last_log_id
    };

    let detected_log_events = if options.last_log_id.is_none() && !pending {
        Vec::new()
    } else {
        scan_recoverable_turn_failure_events(
            &connection,
            options.last_log_id,
            MAX_RECOVERY_SCAN_EVENTS,
        )?
    };
    let stalled_turn_events = scan_stalled_turn_recovery_events(
        &connection,
        &paths.codex_state_db_file,
        Utc::now().timestamp(),
        MAX_STALLED_THREAD_SCAN_THREADS,
    )?;
    if seeded_last_log_id.is_none() && !pending && stalled_turn_events.is_empty() {
        return Ok(RecoveryIterationResult {
            last_log_id: None,
            pending: false,
            pending_events: Vec::new(),
            detected: 0,
            continued_thread_ids: Vec::new(),
            dropped_thread_ids: Vec::new(),
        });
    }
    let detected_events = detected_log_events
        .into_iter()
        .chain(stalled_turn_events)
        .collect::<Vec<_>>();
    let candidate_events = merge_thread_recovery_events(&options.pending_events, detected_events);
    if candidate_events.is_empty() {
        return Ok(RecoveryIterationResult {
            last_log_id: seeded_last_log_id,
            pending: false,
            pending_events: Vec::new(),
            detected: 0,
            continued_thread_ids: Vec::new(),
            dropped_thread_ids: Vec::new(),
        });
    }
    let last_log_id = candidate_events
        .iter()
        .map(|event| event.source_log_id)
        .filter(|log_id| *log_id > 0)
        .max()
        .or(seeded_last_log_id);

    if options.detect_only {
        let mut pending_events = Vec::new();
        let mut dropped_thread_ids = Vec::new();
        for event in candidate_events {
            if thread_has_newer_user_turn(&connection, &event)? {
                dropped_thread_ids.push(event.thread_id);
            } else {
                pending_events.push(event);
            }
        }

        let detected = pending_events.len() + dropped_thread_ids.len();
        return Ok(RecoveryIterationResult {
            last_log_id,
            pending: !pending_events.is_empty(),
            pending_events,
            detected,
            continued_thread_ids: Vec::new(),
            dropped_thread_ids,
        });
    }

    let current_live_email = match options.current_live_email.as_deref() {
        Some(email) => Some(normalize_email(email)),
        None => read_live_account(Some(port))?
            .account
            .and_then(|account| account.email)
            .map(|email| normalize_email(&email)),
    };
    let can_continue_without_email = can_continue_without_email(
        options.rotated,
        options.current_quota_usable,
        options.current_primary_quota_left_percent,
    );
    let processing = process_thread_recovery_events(&candidate_events, |event| {
        resolve_recoverable_turn_failure_event(
            &connection,
            port,
            event,
            &current_live_email,
            can_continue_without_email,
        )
    })?;

    Ok(RecoveryIterationResult {
        last_log_id,
        pending: !processing.pending_events.is_empty(),
        pending_events: processing.pending_events,
        detected: candidate_events.len(),
        continued_thread_ids: processing.continued_thread_ids,
        dropped_thread_ids: processing.dropped_thread_ids,
    })
}

pub fn read_active_thread_ids(port: Option<u16>) -> Result<Vec<String>> {
    let port = port.unwrap_or(DEFAULT_PORT);
    let paths = resolve_paths()?;
    let thread_ids = read_non_archived_thread_ids(&paths.codex_state_db_file)?;
    active_thread_ids_from_candidates(&thread_ids, |thread_id| {
        match read_thread_summary(port, thread_id) {
            Ok(summary) => Ok(summary),
            Err(error) if is_terminal_thread_recovery_error(&error) => Ok(None),
            Err(error) => Err(error),
        }
    })
}

fn resolve_recoverable_turn_failure_event(
    connection: &Connection,
    port: u16,
    event: &mut ThreadRecoveryEvent,
    current_live_email: &Option<String>,
    can_continue_without_email: bool,
) -> Result<RecoveryResolution> {
    if thread_has_newer_user_turn(connection, event)? {
        return Ok(RecoveryResolution::Dropped);
    }

    match event.kind {
        ThreadRecoveryKind::QuotaExhausted => {
            if same_live_account(current_live_email, event.exhausted_email.as_deref()) {
                return Ok(RecoveryResolution::Blocked);
            }

            if event.exhausted_email.is_none() && !can_continue_without_email {
                return Ok(RecoveryResolution::Blocked);
            }
        }
        ThreadRecoveryKind::ModelCapacity => {
            if !transient_recovery_retry_due(event) {
                return Ok(RecoveryResolution::Blocked);
            }
        }
        ThreadRecoveryKind::TransportDisconnected => {
            if !transient_recovery_retry_due(event) {
                return Ok(RecoveryResolution::Blocked);
            }
        }
        ThreadRecoveryKind::StalledTurn => {
            if !transient_recovery_retry_due(event) {
                return Ok(RecoveryResolution::Blocked);
            }
        }
    }

    let thread_summary = match read_thread_summary(port, &event.thread_id) {
        Ok(summary) => summary,
        Err(error) if is_terminal_thread_recovery_error(&error) => {
            let Some(rehydrated_cwd) = rehydrate_recoverable_thread(port, event, None)? else {
                log_daemon_error(format!(
                    "dropping recoverable thread {} after terminal thread/read failure: {error:#}",
                    event.thread_id,
                ));
                eprintln!(
                    "codex-rotate: dropping recoverable thread {} after terminal thread/read failure: {error:#}",
                    event.thread_id,
                );
                return Ok(RecoveryResolution::Dropped);
            };
            return continue_or_rehydrate_thread(port, event, Some(rehydrated_cwd));
        }
        Err(error) => {
            log_daemon_error(format!(
                "failed to read thread {}: {error:#}",
                event.thread_id
            ));
            eprintln!(
                "codex-rotate: failed to read thread {}: {error:#}",
                event.thread_id
            );
            None
        }
    };
    let thread_status_kind = thread_summary
        .as_ref()
        .map(|thread| thread.status.kind.as_str());

    if matches!(thread_status_kind, Some("active")) {
        return Ok(recovery_resolution_for_active_thread(event.kind));
    }

    let cwd = match continue_strategy_for_thread(thread_summary.as_ref()) {
        ContinueStrategy::Direct { cwd } => cwd,
        ContinueStrategy::DirectThenResume { initial_cwd } => {
            match continue_or_rehydrate_thread(port, event, initial_cwd.clone())? {
                RecoveryResolution::Continued => return Ok(RecoveryResolution::Continued),
                RecoveryResolution::Dropped => return Ok(RecoveryResolution::Dropped),
                RecoveryResolution::Blocked => {
                    if event.thread_id
                        != thread_summary
                            .as_ref()
                            .map(|thread| thread.id.as_str())
                            .unwrap_or_default()
                    {
                        return Ok(RecoveryResolution::Blocked);
                    }
                    log_daemon_error(format!(
                        "direct continue failed for thread {}; retrying after resume.",
                        event.thread_id,
                    ));
                    eprintln!(
                        "codex-rotate: direct continue failed for thread {}; retrying after resume.",
                        event.thread_id,
                    );
                }
            }

            match prepare_thread_for_continue(port, &event.thread_id, initial_cwd) {
                Ok(cwd) => cwd,
                Err(error) if is_terminal_thread_recovery_error(&error) => {
                    let Some(rehydrated_cwd) = rehydrate_recoverable_thread(port, event, None)?
                    else {
                        return Ok(log_prepare_continue_error(&event.thread_id, &error));
                    };
                    return continue_or_rehydrate_thread(port, event, Some(rehydrated_cwd));
                }
                Err(error) => return Ok(log_prepare_continue_error(&event.thread_id, &error)),
            }
        }
        ContinueStrategy::ResumeThenContinue { initial_cwd } => {
            match prepare_thread_for_continue(port, &event.thread_id, initial_cwd) {
                Ok(cwd) => cwd,
                Err(error) if is_terminal_thread_recovery_error(&error) => {
                    let Some(rehydrated_cwd) = rehydrate_recoverable_thread(port, event, None)?
                    else {
                        return Ok(log_prepare_continue_error(&event.thread_id, &error));
                    };
                    return continue_or_rehydrate_thread(port, event, Some(rehydrated_cwd));
                }
                Err(error) => return Ok(log_prepare_continue_error(&event.thread_id, &error)),
            }
        }
    };

    continue_or_rehydrate_thread(port, event, cwd)
}

fn thread_has_newer_user_turn(
    connection: &Connection,
    event: &ThreadRecoveryEvent,
) -> Result<bool> {
    let thread_marker = format!("thread.id={}", event.thread_id);
    let mut statement = connection.prepare(
        r#"
select feedback_log_body
from logs
where id > ?1
  and (
    target = 'log'
    or target = 'codex_otel.log_only'
    or target = 'codex_otel.trace_safe'
    or target = 'feedback_tags'
    or target = 'codex_client::default_client'
  )
  and feedback_log_body like '%' || ?2 || '%'
  and feedback_log_body like '%submission_dispatch%'
  and (
    feedback_log_body like '%codex.op="user_input"%'
    or feedback_log_body like '%codex.op="user_input_with_turn_context"%'
  )
order by id asc
limit 20
        "#,
    )?;
    let mut rows = statement.query(params![event.source_log_id, thread_marker])?;
    while let Some(row) = rows.next()? {
        let body: String = row.get(0)?;
        let turn_id = extract_token_field(&body, "turn.id=");
        if turn_id.as_deref() != event.exhausted_turn_id.as_deref() {
            return Ok(true);
        }
    }
    Ok(false)
}

fn scan_recoverable_turn_failure_events(
    connection: &Connection,
    after_log_id: Option<i64>,
    limit: usize,
) -> Result<Vec<ThreadRecoveryEvent>> {
    let mut statement = connection.prepare(
        r#"
select id, ts, feedback_log_body
from logs
where id > ?1
  and (
    (
      target in ('codex_core::codex', 'codex_core::session::turn')
      and (
        feedback_log_body like '%Turn error: You''ve hit your usage limit.%'
        or feedback_log_body like '%Turn error: Selected model is at capacity. Please try a different model.%'
        or feedback_log_body like '%Turn error: stream disconnected before completion:%'
      )
    )
    or (
      target = 'codex_core::compact_remote'
      and (
        feedback_log_body like '%compact_error=You''ve hit your usage limit.%'
        or feedback_log_body like '%compact_error=Selected model is at capacity. Please try a different model.%'
        or feedback_log_body like '%compact_error=stream disconnected before completion:%'
      )
    )
  )
order by id asc
limit ?2
        "#,
    )?;
    let rows = statement.query_map(
        params![after_log_id.unwrap_or(0), limit.clamp(1, 200) as i64],
        |row| {
            let id: i64 = row.get(0)?;
            let ts: i64 = row.get(1)?;
            let body: String = row.get(2)?;
            Ok((id, ts, body))
        },
    )?;

    let mut events = Vec::new();
    for row in rows {
        let (id, ts, body) = row?;
        if let Some(event) =
            parse_codex_core_recoverable_turn_failure_event(connection, id, ts, &body)?
        {
            events.push(event);
        }
    }
    Ok(events)
}

fn read_latest_recoverable_turn_failure_log_id_from_connection(
    connection: &Connection,
) -> Result<Option<i64>> {
    let mut statement = connection.prepare(
        r#"
select id
from logs
where (
    (
      target in ('codex_core::codex', 'codex_core::session::turn')
      and (
        feedback_log_body like '%Turn error: You''ve hit your usage limit.%'
        or feedback_log_body like '%Turn error: Selected model is at capacity. Please try a different model.%'
        or feedback_log_body like '%Turn error: stream disconnected before completion:%'
      )
    )
    or (
      target = 'codex_core::compact_remote'
      and (
        feedback_log_body like '%compact_error=You''ve hit your usage limit.%'
        or feedback_log_body like '%compact_error=Selected model is at capacity. Please try a different model.%'
        or feedback_log_body like '%compact_error=stream disconnected before completion:%'
      )
    )
  )
order by id desc
limit 1
        "#,
    )?;
    let mut rows = statement.query([])?;
    if let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        Ok(Some(id))
    } else {
        Ok(None)
    }
}

fn parse_codex_core_recoverable_turn_failure_event(
    connection: &Connection,
    source_log_id: i64,
    source_ts: i64,
    body: &str,
) -> Result<Option<ThreadRecoveryEvent>> {
    let thread_id = extract_token_field(body, "thread.id=")
        .or_else(|| extract_until(body, "session_loop{thread_id=", "}:"));
    let Some(thread_id) = thread_id else {
        return Ok(None);
    };
    let message = extract_recoverable_error_message(body);
    let Some(message) = message else {
        return Ok(None);
    };
    let Some(kind) = parse_thread_recovery_kind(&message) else {
        return Ok(None);
    };
    let exhausted_turn_id = extract_token_field(body, "turn.id=");
    let metadata = match exhausted_turn_id.as_deref() {
        Some(turn_id) => find_otel_failure_metadata_for_turn(connection, source_log_id, turn_id)?,
        None => None,
    }
    .unwrap_or_default();

    Ok(Some(ThreadRecoveryEvent {
        source_log_id,
        source_ts,
        thread_id,
        kind,
        exhausted_turn_id,
        exhausted_email: metadata.exhausted_email,
        exhausted_account_id: metadata.exhausted_account_id,
        message,
        rehydration: None,
    }))
}

fn extract_recoverable_error_message(body: &str) -> Option<String> {
    extract_after(body, "Turn error: ").or_else(|| extract_after(body, "compact_error="))
}

fn find_otel_failure_metadata_for_turn(
    connection: &Connection,
    source_log_id: i64,
    turn_id: &str,
) -> Result<Option<OtelFailureMetadata>> {
    let min_id = source_log_id.saturating_sub(OTEL_METADATA_LOOKUP_WINDOW);
    let max_id = source_log_id.saturating_add(OTEL_METADATA_LOOKUP_WINDOW);
    let turn_marker = format!("turn.id={turn_id}");
    let mut statement = connection.prepare(
        r#"
select feedback_log_body
from logs
where id between ?1 and ?2
  and target = 'codex_otel.log_only'
  and feedback_log_body like '%event.kind=response.completed%'
  and (
    feedback_log_body like '%error.message=You''ve hit your usage limit.%'
    or feedback_log_body like '%error.message=Selected model is at capacity. Please try a different model.%'
  )
  and feedback_log_body like '%' || ?3 || '%'
order by abs(id - ?4) asc
limit 1
        "#,
    )?;
    let mut rows = statement.query(params![min_id, max_id, turn_marker, source_log_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let body: String = row.get(0)?;
    Ok(parse_otel_failure_metadata(&body))
}

fn parse_otel_failure_metadata(body: &str) -> Option<OtelFailureMetadata> {
    if !body.contains("event.kind=response.completed") || !contains_recoverable_error_message(body)
    {
        return None;
    }
    Some(OtelFailureMetadata {
        exhausted_email: extract_quoted_field(body, "user.email=\""),
        exhausted_account_id: extract_quoted_field(body, "user.account_id=\""),
    })
}

fn parse_thread_recovery_kind(message: &str) -> Option<ThreadRecoveryKind> {
    if message.contains(QUOTA_EXHAUSTION_ERROR_MESSAGE) {
        Some(ThreadRecoveryKind::QuotaExhausted)
    } else if message.contains(MODEL_CAPACITY_ERROR_MESSAGE) {
        Some(ThreadRecoveryKind::ModelCapacity)
    } else if message.starts_with(TRANSPORT_DISCONNECT_ERROR_PREFIX) {
        Some(ThreadRecoveryKind::TransportDisconnected)
    } else {
        None
    }
}

fn contains_recoverable_error_message(body: &str) -> bool {
    body.contains(&format!("error.message={QUOTA_EXHAUSTION_ERROR_MESSAGE}"))
        || body.contains(&format!("error.message={MODEL_CAPACITY_ERROR_MESSAGE}"))
}

fn extract_token_field(body: &str, marker: &str) -> Option<String> {
    let start = body.find(marker)? + marker.len();
    let rest = &body[start..];
    let end = rest
        .find(|c: char| c.is_whitespace() || matches!(c, '}' | ')' | ',' | ';'))
        .unwrap_or(rest.len());
    if end == 0 {
        return None;
    }
    Some(rest[..end].to_string())
}

fn extract_quoted_field(body: &str, marker: &str) -> Option<String> {
    let start = body.find(marker)? + marker.len();
    let rest = &body[start..];
    let end = rest.find('"')?;
    let value = rest[..end].trim();
    if value.is_empty() || value.contains('\n') || value.contains('\r') {
        return None;
    }
    Some(value.to_string())
}

fn extract_until(body: &str, start_marker: &str, end_marker: &str) -> Option<String> {
    let start = body.find(start_marker)? + start_marker.len();
    let rest = &body[start..];
    let end = rest.find(end_marker).unwrap_or(rest.len());
    let value = rest[..end].trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn extract_after(body: &str, start_marker: &str) -> Option<String> {
    let start = body.find(start_marker)? + start_marker.len();
    let value = body[start..].trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn can_continue_without_email(
    rotated: bool,
    current_quota_usable: Option<bool>,
    current_primary_quota_left_percent: Option<u8>,
) -> bool {
    rotated
        || (current_quota_usable == Some(true)
            && current_primary_quota_left_percent
                .map(|percent| percent > HEALTHY_QUOTA_CONTINUE_THRESHOLD_PERCENT)
                .unwrap_or(false))
}

fn transient_recovery_retry_due(event: &ThreadRecoveryEvent) -> bool {
    matches!(
        event.kind,
        ThreadRecoveryKind::ModelCapacity
            | ThreadRecoveryKind::TransportDisconnected
            | ThreadRecoveryKind::StalledTurn
    ) && Utc::now().timestamp()
        >= event
            .source_ts
            .saturating_add(transient_recovery_retry_delay_secs(event.kind))
}

fn transient_recovery_retry_delay_secs(kind: ThreadRecoveryKind) -> i64 {
    match kind {
        ThreadRecoveryKind::StalledTurn => STALLED_TURN_RECOVERY_DELAY_SECS,
        ThreadRecoveryKind::QuotaExhausted
        | ThreadRecoveryKind::ModelCapacity
        | ThreadRecoveryKind::TransportDisconnected => TRANSIENT_RECOVERY_RETRY_DELAY_SECS,
    }
}

fn merge_thread_recovery_events(
    pending_events: &[ThreadRecoveryEvent],
    detected_events: Vec<ThreadRecoveryEvent>,
) -> Vec<ThreadRecoveryEvent> {
    let mut merged = std::collections::BTreeMap::<String, ThreadRecoveryEvent>::new();
    for event in pending_events.iter().chain(detected_events.iter()) {
        match merged.get(&event.thread_id) {
            Some(existing) if existing.source_log_id >= event.source_log_id => {}
            _ => {
                merged.insert(event.thread_id.clone(), event.clone());
            }
        }
    }
    let mut events = merged.into_values().collect::<Vec<_>>();
    events.sort_by_key(|event| event.source_log_id);
    events
}

fn scan_stalled_turn_recovery_events(
    connection: &Connection,
    state_db_path: &Path,
    now_ts: i64,
    limit: usize,
) -> Result<Vec<ThreadRecoveryEvent>> {
    let Some(state_connection) = open_state_connection_if_available(state_db_path)? else {
        return Ok(Vec::new());
    };
    let mut statement = state_connection.prepare(
        r#"
select id, rollout_path, updated_at
from threads
where archived = 0
  and rollout_path != ''
order by updated_at desc
limit ?1
        "#,
    )?;
    let rows = statement.query_map(params![limit.clamp(1, 64) as i64], |row| {
        let thread_id: String = row.get(0)?;
        let rollout_path: String = row.get(1)?;
        let updated_at: i64 = row.get(2)?;
        Ok((thread_id, rollout_path, updated_at))
    })?;

    let mut events = Vec::new();
    for row in rows {
        let (thread_id, rollout_path, updated_at) = row?;
        let Some(snapshot) =
            detect_stalled_turn_snapshot(&thread_id, Path::new(&rollout_path), updated_at, now_ts)?
        else {
            continue;
        };
        let source_log_id = find_turn_source_log_id(connection, &thread_id, &snapshot.turn_id)?
            .unwrap_or(SYNTHETIC_RECOVERY_SOURCE_LOG_ID);
        events.push(ThreadRecoveryEvent {
            source_log_id,
            source_ts: snapshot.last_event_ts,
            thread_id,
            kind: ThreadRecoveryKind::StalledTurn,
            exhausted_turn_id: Some(snapshot.turn_id),
            exhausted_email: None,
            exhausted_account_id: None,
            message: "turn stalled without completion".to_string(),
            rehydration: None,
        });
    }

    Ok(events)
}

fn read_non_archived_thread_ids(state_db_path: &Path) -> Result<Vec<String>> {
    let Some(connection) = open_state_connection_if_available(state_db_path)? else {
        return Ok(Vec::new());
    };
    let mut statement = connection.prepare(
        r#"
select id
from threads
where archived = 0
order by updated_at desc
        "#,
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut thread_ids = Vec::new();
    for row in rows {
        thread_ids.push(row?);
    }
    Ok(thread_ids)
}

fn active_thread_ids_from_candidates<F>(
    thread_ids: &[String],
    mut read_thread: F,
) -> Result<Vec<String>>
where
    F: FnMut(&str) -> Result<Option<ThreadSummary>>,
{
    let mut active = Vec::new();
    for thread_id in thread_ids {
        let Some(summary) = read_thread(thread_id)? else {
            continue;
        };
        if summary.status.kind == "active" {
            active.push(thread_id.clone());
        }
    }
    Ok(active)
}

fn detect_stalled_turn_snapshot(
    thread_id: &str,
    rollout_path: &Path,
    fallback_ts: i64,
    now_ts: i64,
) -> Result<Option<StalledTurnSnapshot>> {
    if !rollout_path.exists() {
        return Ok(None);
    }
    let tail = read_file_tail(rollout_path, ROLLOUT_TAIL_SCAN_BYTES)?;
    let mut terminal_turn_ids = HashSet::<String>::new();
    let mut latest_event_ts = None::<i64>;

    for line in tail.lines().rev() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if latest_event_ts.is_none() {
            latest_event_ts = rollout_event_timestamp(&value).or(Some(fallback_ts));
        }
        if value.get("type").and_then(Value::as_str) != Some("event_msg") {
            continue;
        }
        let Some(payload) = value.get("payload") else {
            continue;
        };
        let Some(payload_type) = payload.get("type").and_then(Value::as_str) else {
            continue;
        };
        match payload_type {
            "task_complete" | "turn_aborted" => {
                if let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) {
                    terminal_turn_ids.insert(turn_id.to_string());
                }
            }
            "task_started" => {
                let Some(turn_id) = payload.get("turn_id").and_then(Value::as_str) else {
                    continue;
                };
                if terminal_turn_ids.contains(turn_id) {
                    return Ok(None);
                }
                if !task_started_is_stalled_turn_eligible(payload) {
                    return Ok(None);
                }
                let last_event_ts = latest_event_ts.unwrap_or(fallback_ts);
                if last_event_ts.saturating_add(STALLED_TURN_RECOVERY_DELAY_SECS) > now_ts {
                    return Ok(None);
                }
                return Ok(Some(StalledTurnSnapshot {
                    thread_id: thread_id.to_string(),
                    turn_id: turn_id.to_string(),
                    last_event_ts,
                }));
            }
            _ => {}
        }
    }

    Ok(None)
}

fn read_file_tail(path: &Path, max_bytes: usize) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("Failed to open rollout {}.", path.display()))?;
    let len = file
        .metadata()
        .with_context(|| format!("Failed to stat rollout {}.", path.display()))?
        .len();
    let max_bytes = max_bytes.max(1) as u64;
    let start = len.saturating_sub(max_bytes);
    file.seek(SeekFrom::Start(start))
        .with_context(|| format!("Failed to seek rollout {}.", path.display()))?;
    let mut buf = Vec::with_capacity((len - start) as usize);
    file.read_to_end(&mut buf)
        .with_context(|| format!("Failed to read rollout {}.", path.display()))?;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

fn rollout_event_timestamp(value: &Value) -> Option<i64> {
    let raw = value.get("timestamp")?.as_str()?;
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .map(|timestamp| timestamp.timestamp())
}

fn find_turn_source_log_id(
    connection: &Connection,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<i64>> {
    let mut statement = connection.prepare(
        r#"
select id
from logs
where thread_id = ?1
  and feedback_log_body like '%' || ?2 || '%'
order by id desc
limit 1
        "#,
    )?;
    let mut rows = statement.query(params![thread_id, turn_id])?;
    if let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        Ok(Some(id))
    } else {
        Ok(None)
    }
}

fn process_thread_recovery_events<F>(
    events: &[ThreadRecoveryEvent],
    mut resolver: F,
) -> Result<RecoveryProcessingResult>
where
    F: FnMut(&mut ThreadRecoveryEvent) -> Result<RecoveryResolution>,
{
    let mut continued_thread_ids = Vec::new();
    let mut dropped_thread_ids = Vec::new();
    let mut pending_events = Vec::new();

    for event in events {
        let mut event = event.clone();
        match resolver(&mut event)? {
            RecoveryResolution::Continued => {
                continued_thread_ids.push(event.thread_id.clone());
            }
            RecoveryResolution::Dropped => {
                dropped_thread_ids.push(event.thread_id.clone());
            }
            RecoveryResolution::Blocked => {
                pending_events.push(event);
            }
        }
    }

    Ok(RecoveryProcessingResult {
        continued_thread_ids,
        dropped_thread_ids,
        pending_events,
    })
}

fn current_account_id() -> Result<String> {
    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    Ok(summarize_codex_auth(&auth).account_id)
}

fn rehydrate_thread_binding(
    account_id: &str,
    lineage_id: &str,
    local_thread_id: &str,
) -> Result<()> {
    let paths = resolve_paths()?;
    let mut store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;
    store.bind_local_thread_id(account_id, lineage_id, local_thread_id)
}

fn start_rehydrated_thread(port: u16, cwd: Option<&str>) -> Result<String> {
    let response: Value = send_codex_app_request(
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
    response
        .get("thread")
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            anyhow!(
                "Codex thread/start did not return a thread id while rehydrating thread recovery."
            )
        })
}

fn inject_rehydrated_thread_items(port: u16, thread_id: &str, items: Vec<Value>) -> Result<()> {
    if items.is_empty() {
        return Ok(());
    }
    let _: Value = send_codex_app_request(
        port,
        "thread/inject_items",
        json!({
            "threadId": thread_id,
            "items": items,
        }),
    )?;
    Ok(())
}

fn rehydrate_recoverable_thread(
    port: u16,
    event: &mut ThreadRecoveryEvent,
    fallback_cwd: Option<String>,
) -> Result<Option<String>> {
    let Some(rehydration) = event.rehydration.as_ref() else {
        return Ok(None);
    };

    let new_thread_id =
        start_rehydrated_thread(port, rehydration.cwd.as_deref().or(fallback_cwd.as_deref()))
            .with_context(|| {
                format!(
                    "Failed to create rehydrated thread for {}.",
                    event.thread_id
                )
            })?;
    inject_rehydrated_thread_items(port, &new_thread_id, rehydration.items.clone())
        .with_context(|| format!("Failed to inject rehydration items into {}.", new_thread_id))?;
    let account_id = current_account_id()?;
    rehydrate_thread_binding(&account_id, &rehydration.lineage_id, &new_thread_id)?;
    event.thread_id = new_thread_id;
    Ok(rehydration.cwd.clone().or(fallback_cwd))
}

fn continue_or_rehydrate_thread(
    port: u16,
    event: &mut ThreadRecoveryEvent,
    cwd: Option<String>,
) -> Result<RecoveryResolution> {
    match send_continue_turn(port, &event.thread_id, cwd.clone()) {
        Ok(()) => Ok(RecoveryResolution::Continued),
        Err(error) if is_terminal_thread_recovery_error(&error) => {
            let Some(rehydrated_cwd) = rehydrate_recoverable_thread(port, event, cwd.clone())?
            else {
                log_daemon_error(format!(
                    "dropping recoverable thread {} after terminal continue failure: {error:#}",
                    event.thread_id,
                ));
                eprintln!(
                    "codex-rotate: dropping recoverable thread {} after terminal continue failure: {error:#}",
                    event.thread_id,
                );
                return Ok(RecoveryResolution::Dropped);
            };
            match send_continue_turn(port, &event.thread_id, Some(rehydrated_cwd)) {
                Ok(()) => Ok(RecoveryResolution::Continued),
                Err(retry_error) if is_terminal_thread_recovery_error(&retry_error) => {
                    log_daemon_error(format!(
                        "rehydrated recoverable thread {} still failed terminal continue: {retry_error:#}",
                        event.thread_id,
                    ));
                    eprintln!(
                        "codex-rotate: rehydrated recoverable thread {} still failed terminal continue: {retry_error:#}",
                        event.thread_id,
                    );
                    Ok(RecoveryResolution::Blocked)
                }
                Err(retry_error) => {
                    log_daemon_error(format!(
                        "rehydrated recoverable thread {} failed continue: {retry_error:#}",
                        event.thread_id,
                    ));
                    eprintln!(
                        "codex-rotate: rehydrated recoverable thread {} failed continue: {retry_error:#}",
                        event.thread_id,
                    );
                    Ok(RecoveryResolution::Blocked)
                }
            }
        }
        Err(error) => {
            log_daemon_error(format!(
                "failed to continue thread {} after {} recovery: {error:#}",
                event.thread_id,
                event.kind.label(),
            ));
            eprintln!(
                "codex-rotate: failed to continue thread {} after {} recovery: {error:#}",
                event.thread_id,
                event.kind.label(),
            );
            Ok(RecoveryResolution::Blocked)
        }
    }
}

fn recovery_resolution_for_active_thread(kind: ThreadRecoveryKind) -> RecoveryResolution {
    match kind {
        ThreadRecoveryKind::StalledTurn => RecoveryResolution::Dropped,
        ThreadRecoveryKind::QuotaExhausted
        | ThreadRecoveryKind::ModelCapacity
        | ThreadRecoveryKind::TransportDisconnected => RecoveryResolution::Blocked,
    }
}

fn task_started_is_stalled_turn_eligible(payload: &Value) -> bool {
    payload
        .get("collaboration_mode_kind")
        .and_then(Value::as_str)
        != Some("plan")
}

fn is_terminal_thread_recovery_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        let message = cause.to_string().to_lowercase();
        message.contains("no rollout found for thread id")
            || message.contains("thread not found")
            || message.contains("no thread found")
            || message.contains("unknown thread")
            || message.contains("does not exist")
    })
}

impl ThreadRecoveryKind {
    fn label(self) -> &'static str {
        match self {
            ThreadRecoveryKind::QuotaExhausted => "quota exhaustion",
            ThreadRecoveryKind::ModelCapacity => "model capacity",
            ThreadRecoveryKind::TransportDisconnected => "transport disconnect",
            ThreadRecoveryKind::StalledTurn => "stalled turn",
        }
    }
}

fn read_thread_summary(port: u16, thread_id: &str) -> Result<Option<ThreadSummary>> {
    let response: Value =
        send_codex_app_request(port, "thread/read", json!({ "threadId": thread_id }))?;
    if response.is_null() {
        return Ok(None);
    }
    if response.get("thread").is_none() {
        return Ok(None);
    }
    let envelope: ThreadReadEnvelope = serde_json::from_value(response)
        .map_err(|error| anyhow!("Failed to decode thread/read response from Codex: {error}"))?;
    Ok(Some(envelope.thread))
}

fn thread_ready_for_continue(status_kind: &str) -> bool {
    matches!(status_kind, "active" | "idle")
}

fn continue_strategy_for_thread(thread_summary: Option<&ThreadSummary>) -> ContinueStrategy {
    match thread_summary {
        Some(thread) if thread_ready_for_continue(&thread.status.kind) => {
            ContinueStrategy::Direct {
                cwd: thread.cwd.clone(),
            }
        }
        Some(thread) if thread.cwd.is_some() => ContinueStrategy::DirectThenResume {
            initial_cwd: thread.cwd.clone(),
        },
        Some(_) => ContinueStrategy::ResumeThenContinue { initial_cwd: None },
        None => ContinueStrategy::ResumeThenContinue { initial_cwd: None },
    }
}

fn recovery_resolution_for_prepare_continue_error(error: &anyhow::Error) -> RecoveryResolution {
    if is_terminal_thread_recovery_error(error) {
        RecoveryResolution::Dropped
    } else {
        RecoveryResolution::Blocked
    }
}

fn log_prepare_continue_error(thread_id: &str, error: &anyhow::Error) -> RecoveryResolution {
    let resolution = recovery_resolution_for_prepare_continue_error(error);
    match resolution {
        RecoveryResolution::Dropped => {
            log_daemon_error(format!(
                "dropping recoverable thread {} after terminal resume failure: {error:#}",
                thread_id,
            ));
            eprintln!(
                "codex-rotate: dropping recoverable thread {} after terminal resume failure: {error:#}",
                thread_id,
            );
        }
        RecoveryResolution::Blocked => {
            log_daemon_error(format!(
                "thread {} could not be resumed for continue: {error:#}",
                thread_id
            ));
            eprintln!(
                "codex-rotate: thread {} could not be resumed for continue: {error:#}",
                thread_id
            );
        }
        RecoveryResolution::Continued => {}
    }
    resolution
}

fn prepare_thread_for_continue(
    port: u16,
    thread_id: &str,
    initial_cwd: Option<String>,
) -> Result<Option<String>> {
    send_thread_resume(port, thread_id)
        .with_context(|| format!("Failed to resume thread {thread_id} before continue."))?;
    // Codex can accept a new turn shortly after resume even while `thread/read`
    // still reports `systemError`, so do not gate on a status transition here.
    sleep(Duration::from_millis(THREAD_RESUME_SETTLE_MS));

    match read_thread_summary(port, thread_id) {
        Ok(Some(thread)) => Ok(thread.cwd.or(initial_cwd)),
        Ok(None) => Ok(initial_cwd),
        Err(error) => {
            log_daemon_error(format!(
                "failed to refresh thread {} after resume: {error:#}",
                thread_id
            ));
            eprintln!(
                "codex-rotate: failed to refresh thread {} after resume: {error:#}",
                thread_id
            );
            Ok(initial_cwd)
        }
    }
}

fn send_thread_resume(port: u16, thread_id: &str) -> Result<()> {
    let _: Value = send_codex_app_request(port, "thread/resume", json!({ "threadId": thread_id }))?;
    Ok(())
}

fn send_continue_turn(port: u16, thread_id: &str, cwd: Option<String>) -> Result<()> {
    let cwd = cwd.unwrap_or_else(|| {
        std::env::current_dir()
            .unwrap_or_default()
            .display()
            .to_string()
    });
    let _: Value = send_codex_app_request(
        port,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [
                {
                    "type": "text",
                    "text": CONTINUE_INPUT,
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
    Ok(())
}

pub fn send_codex_app_request<T>(port: u16, method: &str, params: Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    match send_codex_app_request_once(port, method, &params) {
        Ok(value) => Ok(value),
        Err(first_error) => {
            invalidate_local_codex_connection(port, true);
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            send_codex_app_request_once(port, method, &params).map_err(|retry_error| {
                anyhow!(
                    "{retry_error} (initial {method} request failed before relaunch: {first_error})"
                )
            })
        }
    }
}

pub(crate) fn send_codex_host_fetch_request<T>(port: u16, method: &str, params: Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    match send_codex_host_fetch_request_once(port, method, &params) {
        Ok(value) => Ok(value),
        Err(first_error) => {
            invalidate_local_codex_connection(port, true);
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            send_codex_host_fetch_request_once(port, method, &params).map_err(|retry_error| {
                anyhow!(
                    "{retry_error} (initial {method} host request failed before relaunch: {first_error})"
                )
            })
        }
    }
}

fn send_codex_host_fetch_request_once<T>(port: u16, method: &str, params: &Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let request_id = format!(
        "codex-rotate-host-fetch-{method}-{}",
        Utc::now().timestamp_millis()
    );
    let request = json!({
        "type": "fetch",
        "hostId": "local",
        "requestId": request_id,
        "method": "POST",
        "url": format!("vscode://codex/{method}"),
        "body": serde_json::to_string(params)?,
    });
    let request_json = serde_json::to_string(&request)?;
    let expression = format!(
        r#"new Promise(async (resolve) => {{
const request = {request_json};
const timeout = setTimeout(() => {{
  window.removeEventListener("message", handler);
  resolve({{ timeout: true }});
}}, {MCP_RESPONSE_TIMEOUT_MS});
const handler = (event) => {{
  const data = event.data;
  if (data && data.type === "fetch-response" && data.requestId === request.requestId) {{
    clearTimeout(timeout);
    window.removeEventListener("message", handler);
    resolve({{
      timeout: false,
      responseType: data.responseType ?? null,
      status: data.status ?? null,
      bodyJsonString: data.bodyJsonString ?? null,
      error: data.error ?? null
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
        return Err(anyhow!(
            "Timed out waiting for {method} host response from Codex."
        ));
    }
    if value.get("responseType").and_then(Value::as_str) == Some("error") {
        return Err(anyhow!(
            "Codex {method} host request failed: {}",
            value
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("unknown error")
        ));
    }
    let status = value.get("status").and_then(Value::as_i64).unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(anyhow!(
            "Codex {method} host request returned status {status}: {}",
            value
                .get("bodyJsonString")
                .and_then(Value::as_str)
                .unwrap_or("")
        ));
    }
    let body = value
        .get("bodyJsonString")
        .and_then(Value::as_str)
        .unwrap_or("null");
    serde_json::from_str(body)
        .map_err(|error| anyhow!("Failed to decode {method} host response from Codex: {error}"))
}

fn send_codex_app_request_once<T>(port: u16, method: &str, params: &Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let request = json!({
        "type": "mcp-request",
        "hostId": "local",
        "request": {
            "jsonrpc": "2.0",
            "id": format!("codex-rotate-thread-recovery-{method}-{}", Utc::now().timestamp_millis()),
            "method": method,
            "params": params,
        }
    });
    let request_json = serde_json::to_string(&request)?;
    let expression = format!(
        r#"new Promise(async (resolve) => {{
const request = {request_json};
const timeout = setTimeout(() => {{
  window.removeEventListener("message", handler);
  resolve({{ timeout: true }});
}}, {MCP_RESPONSE_TIMEOUT_MS});
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
        return Err(anyhow!(
            "Timed out waiting for {method} response from Codex."
        ));
    }
    if let Some(error) = value.get("error").filter(|error| !error.is_null()) {
        return Err(anyhow!("Codex {method} request failed: {error}"));
    }
    serde_json::from_value(value.get("result").cloned().unwrap_or(Value::Null))
        .map_err(|error| anyhow!("Failed to decode {method} response from Codex: {error}"))
}

fn same_live_account(current_live_email: &Option<String>, exhausted_email: Option<&str>) -> bool {
    let Some(exhausted_email) = exhausted_email else {
        return false;
    };
    current_live_email
        .as_deref()
        .map(|current| normalize_email(current) == normalize_email(exhausted_email))
        .unwrap_or(false)
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}

fn open_state_connection(state_db_path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        state_db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {}.", state_db_path.display()))
}

fn open_state_connection_if_available(state_db_path: &Path) -> Result<Option<Connection>> {
    if !state_db_path.exists() {
        return Ok(None);
    }
    let connection = open_state_connection(state_db_path)?;
    if !threads_table_exists(&connection)? {
        return Ok(None);
    }
    Ok(Some(connection))
}

fn open_logs_connection(logs_db_path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        logs_db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {}.", logs_db_path.display()))
}

fn open_logs_connection_if_available(logs_db_path: &Path) -> Result<Option<Connection>> {
    if !logs_db_path.exists() {
        return Ok(None);
    }
    let connection = open_logs_connection(logs_db_path)?;
    if !logs_table_exists(&connection)? {
        return Ok(None);
    }
    Ok(Some(connection))
}

fn logs_table_exists(connection: &Connection) -> Result<bool> {
    sqlite_table_exists(connection, "logs")
}

fn threads_table_exists(connection: &Connection) -> Result<bool> {
    sqlite_table_exists(connection, "threads")
}

fn sqlite_table_exists(connection: &Connection, table_name: &str) -> Result<bool> {
    let mut statement = connection.prepare(
        r#"
select 1
from sqlite_master
where type = 'table'
  and name = ?1
limit 1
        "#,
    )?;
    let mut rows = statement.query([table_name])?;
    Ok(rows.next()?.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::NamedTempFile;

    fn thread_summary(status_kind: &str, cwd: Option<&str>) -> ThreadSummary {
        ThreadSummary {
            id: "thread-123".to_string(),
            cwd: cwd.map(str::to_string),
            status: ThreadStatus {
                kind: status_kind.to_string(),
            },
        }
    }

    #[test]
    fn logs_connection_unavailable_when_database_is_missing() {
        let missing = std::env::temp_dir().join(format!(
            "codex-rotate-thread-recovery-missing-{}.sqlite",
            std::process::id()
        ));
        std::fs::remove_file(&missing).ok();

        assert!(open_logs_connection_if_available(&missing)
            .unwrap()
            .is_none());
    }

    #[test]
    fn read_non_archived_thread_ids_returns_recent_non_archived_threads() {
        let tempdir = tempfile::tempdir().unwrap();
        let state_file = tempdir.path().join("state_5.sqlite");
        let connection = Connection::open(&state_file).unwrap();
        connection
            .execute_batch(
                r#"
create table threads (
  id text primary key,
  rollout_path text not null default '',
  updated_at integer not null,
  archived integer not null default 0
);
insert into threads (id, rollout_path, updated_at, archived) values
  ('thread-old', '', 10, 0),
  ('thread-archived', '', 30, 1),
  ('thread-new', '', 20, 0);
                "#,
            )
            .unwrap();

        let thread_ids = read_non_archived_thread_ids(&state_file).unwrap();

        assert_eq!(
            thread_ids,
            vec!["thread-new".to_string(), "thread-old".to_string()]
        );
    }

    #[test]
    fn active_thread_ids_from_candidates_filters_to_active_threads() {
        let thread_ids = vec![
            "thread-active".to_string(),
            "thread-idle".to_string(),
            "thread-missing".to_string(),
        ];

        let active_thread_ids = active_thread_ids_from_candidates(&thread_ids, |thread_id| {
            Ok(match thread_id {
                "thread-active" => Some(thread_summary("active", Some("/tmp/project"))),
                "thread-idle" => Some(thread_summary("idle", Some("/tmp/project"))),
                "thread-missing" => None,
                other => panic!("unexpected thread {other}"),
            })
        })
        .unwrap();

        assert_eq!(active_thread_ids, vec!["thread-active".to_string()]);
    }

    #[test]
    fn active_thread_ids_from_candidates_propagates_non_terminal_errors() {
        let thread_ids = vec!["thread-active".to_string()];

        let error = active_thread_ids_from_candidates(&thread_ids, |_| {
            Err(anyhow!("Codex thread/read request failed"))
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("Codex thread/read request failed"));
    }

    #[test]
    fn logs_connection_unavailable_when_logs_table_is_missing() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table metadata (
  id integer primary key,
  value text
);
                "#,
            )
            .unwrap();

        assert!(open_logs_connection_if_available(file.path())
            .unwrap()
            .is_none());
    }

    #[test]
    fn parses_otel_failure_metadata() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775130266,
    'codex_otel.log_only',
    'event.name="codex.sse_event" event.kind=response.completed error.message=You''ve hit your usage limit. Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), or try again at Apr 9th, 2026 1:05 PM. event.timestamp=2026-04-02T11:44:26.231Z conversation.id=thread-123 user.account_id="acct-123" user.email="user@example.com" turn.id=turn-123'
  );
                "#,
            )
            .unwrap();

        let body: String = connection
            .query_row(
                "select feedback_log_body from logs where id = 10",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let metadata = parse_otel_failure_metadata(&body).unwrap();
        assert_eq!(
            metadata.exhausted_email.as_deref(),
            Some("user@example.com")
        );
        assert_eq!(metadata.exhausted_account_id.as_deref(), Some("acct-123"));
    }

    #[test]
    fn parse_codex_core_event_enriches_from_otel_metadata() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775445612,
    'codex_core::codex',
    'session_loop{thread_id=local-thread}:submission_dispatch{otel.name="op.dispatch.user_input"}:turn{otel.name="session_task.turn" thread.id=local-thread turn.id=turn-123 model=gpt-5.4}:run_turn: Turn error: You''ve hit your usage limit. Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), or try again at Apr 13th, 2026 8:46 AM.'
  ),
  (
    11,
    1775445613,
    'codex_otel.log_only',
    'session_loop{thread_id=local-thread}:turn{thread.id=local-thread turn.id=turn-123}: event.name="codex.sse_event" event.kind=response.completed error.message=You''ve hit your usage limit. Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), or try again at Apr 13th, 2026 8:46 AM. event.timestamp=2026-04-02T11:44:26.231Z conversation.id=remote-conversation user.account_id="acct-123" user.email="user@example.com"'
  );
                "#,
            )
            .unwrap();

        let event = parse_codex_core_recoverable_turn_failure_event(
            &connection,
            10,
            1775445612,
            r#"session_loop{thread_id=local-thread}:submission_dispatch{otel.name="op.dispatch.user_input"}:turn{otel.name="session_task.turn" thread.id=local-thread turn.id=turn-123 model=gpt-5.4}:run_turn: Turn error: You've hit your usage limit. Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), or try again at Apr 13th, 2026 8:46 AM."#,
        )
        .unwrap()
        .unwrap();

        assert_eq!(event.thread_id, "local-thread");
        assert_eq!(event.kind, ThreadRecoveryKind::QuotaExhausted);
        assert_eq!(event.exhausted_turn_id.as_deref(), Some("turn-123"));
        assert_eq!(event.exhausted_email.as_deref(), Some("user@example.com"));
        assert_eq!(event.exhausted_account_id.as_deref(), Some("acct-123"));
    }

    #[test]
    fn parses_quota_exhaustion_event_from_codex_core_log() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775445612,
    'codex_core::session::turn',
    'session_loop{thread_id=thread-456}:submission_dispatch{otel.name="op.dispatch.user_input_with_turn_context" submission.id="turn-456" codex.op="user_input_with_turn_context"}:turn{otel.name="session_task.turn" thread.id=thread-456 turn.id=turn-456 model=gpt-5.4}:run_turn: Turn error: You''ve hit your usage limit. To get more access now, send a request to your admin or try again at 2:20 PM.'
  );
                "#,
            )
            .unwrap();

        let events = scan_recoverable_turn_failure_events(&connection, None, 50).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].thread_id, "thread-456");
        assert_eq!(events[0].kind, ThreadRecoveryKind::QuotaExhausted);
        assert_eq!(events[0].exhausted_turn_id.as_deref(), Some("turn-456"));
        assert!(events[0].exhausted_email.is_none());
        assert!(events[0]
            .message
            .starts_with("You've hit your usage limit."));
    }

    #[test]
    fn parses_model_capacity_event_from_codex_core_log() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775636941,
    'codex_core::codex',
    'session_loop{thread_id=thread-789}:submission_dispatch{otel.name="op.dispatch.user_input"}:turn{otel.name="session_task.turn" thread.id=thread-789 turn.id=turn-789 model=gpt-5.4}:run_turn: Turn error: Selected model is at capacity. Please try a different model.'
  );
                "#,
            )
            .unwrap();

        let events = scan_recoverable_turn_failure_events(&connection, None, 50).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].thread_id, "thread-789");
        assert_eq!(events[0].kind, ThreadRecoveryKind::ModelCapacity);
        assert_eq!(events[0].exhausted_turn_id.as_deref(), Some("turn-789"));
        assert_eq!(
            events[0].message,
            "Selected model is at capacity. Please try a different model."
        );
    }

    #[test]
    fn parses_quota_exhaustion_event_from_remote_compact_log() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775847287,
    'codex_core::compact_remote',
    'session_loop{thread_id=thread-compact}:submission_dispatch{otel.name="op.dispatch.user_input" submission.id="submission-compact" codex.op="user_input"}:turn{otel.name="session_task.turn" thread.id=thread-compact turn.id=turn-compact model=gpt-5.4}:run_turn: remote compaction failed turn_id=turn-compact compact_error=You''ve hit your usage limit. Upgrade to Plus to continue using Codex (https://chatgpt.com/explore/plus), or try again at Apr 18th, 2026 12:19 AM.'
  );
                "#,
            )
            .unwrap();

        let events = scan_recoverable_turn_failure_events(&connection, None, 50).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].thread_id, "thread-compact");
        assert_eq!(events[0].kind, ThreadRecoveryKind::QuotaExhausted);
        assert_eq!(events[0].exhausted_turn_id.as_deref(), Some("turn-compact"));
        assert!(events[0].exhausted_email.is_none());
        assert!(events[0]
            .message
            .starts_with("You've hit your usage limit."));
    }

    #[test]
    fn parses_transport_disconnect_event_from_codex_core_log() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775680284,
    'codex_core::codex',
    'session_loop{thread_id=thread-transport}:submission_dispatch{otel.name="op.dispatch.user_input"}:turn{otel.name="session_task.turn" thread.id=thread-transport turn.id=turn-transport model=gpt-5.4}:run_turn: Turn error: stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)'
  );
                "#,
            )
            .unwrap();

        let events = scan_recoverable_turn_failure_events(&connection, None, 50).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].thread_id, "thread-transport");
        assert_eq!(events[0].kind, ThreadRecoveryKind::TransportDisconnected);
        assert_eq!(
            events[0].exhausted_turn_id.as_deref(),
            Some("turn-transport")
        );
        assert_eq!(
            events[0].message,
            "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)"
        );
    }

    #[test]
    fn newer_user_turn_marks_event_as_resolved() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (
    10,
    1775445612,
    'codex_core::codex',
    'session_loop{thread_id=thread-123}:submission_dispatch{otel.name="op.dispatch.user_input"}:turn{otel.name="session_task.turn" thread.id=thread-123 turn.id=turn-old model=gpt-5.4}:run_turn: Turn error: You''ve hit your usage limit.'
  ),
  (
    11,
    1775445613,
    'log',
    'session_loop{thread_id=thread-123}:submission_dispatch{otel.name="op.dispatch.user_input_with_turn_context" submission.id="submission-new" codex.op="user_input_with_turn_context"}:turn{otel.name="session_task.turn" thread.id=thread-123 turn.id=turn-new model=gpt-5.4}:run_turn'
  );
                "#,
            )
            .unwrap();

        let event = ThreadRecoveryEvent {
            source_log_id: 10,
            source_ts: 1775445612,
            thread_id: "thread-123".to_string(),
            kind: ThreadRecoveryKind::QuotaExhausted,
            exhausted_turn_id: Some("turn-old".to_string()),
            exhausted_email: None,
            exhausted_account_id: None,
            message: "You've hit your usage limit.".to_string(),
            rehydration: None,
        };

        assert!(thread_has_newer_user_turn(&connection, &event).unwrap());
    }

    #[test]
    fn same_live_account_normalizes_email() {
        assert!(same_live_account(
            &Some("User@Example.com".to_string()),
            Some(" user@example.com ")
        ));
        assert!(!same_live_account(
            &Some("other@example.com".to_string()),
            Some("user@example.com")
        ));
    }

    #[test]
    fn reads_latest_recoverable_turn_failure_log_id() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (10, 1000, 'codex_otel.log_only', 'event.kind=response.completed error.message=You''ve hit your usage limit. conversation.id=thread-a'),
  (11, 1001, 'codex_otel.trace_safe', 'event.kind=response.completed error.message=You''ve hit your usage limit. conversation.id=thread-a'),
  (12, 1002, 'codex_otel.log_only', 'event.kind=response.completed error.message=You''ve hit your usage limit. conversation.id=thread-b'),
  (13, 1003, 'codex_core::codex', 'session_loop{thread_id=thread-c}:turn{thread.id=thread-c turn.id=turn-c}:run_turn: Turn error: You''ve hit your usage limit. Upgrade to Plus to continue using Codex.'),
  (14, 1004, 'log', 'error.message=You''ve hit your usage limit. not authoritative'),
  (15, 1005, 'codex_core::codex', 'session_loop{thread_id=thread-d}:turn{thread.id=thread-d turn.id=turn-d}:run_turn: Turn error: Selected model is at capacity. Please try a different model.'),
  (16, 1006, 'codex_core::codex', 'session_loop{thread_id=thread-e}:turn{thread.id=thread-e turn.id=turn-e}:run_turn: Turn error: stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)'),
  (17, 1007, 'codex_core::compact_remote', 'session_loop{thread_id=thread-f}:turn{thread.id=thread-f turn.id=turn-f}:run_turn: remote compaction failed turn_id=turn-f compact_error=You''ve hit your usage limit. Upgrade to Plus to continue using Codex.'),
  (18, 1008, 'codex_core::session::turn', 'session_loop{thread_id=thread-g}:turn{thread.id=thread-g turn.id=turn-g}:run_turn: Turn error: You''ve hit your usage limit. To get more access now, send a request to your admin or try again later.');
                "#,
            )
            .unwrap();

        assert_eq!(
            read_latest_recoverable_turn_failure_log_id_from_connection(&connection).unwrap(),
            Some(18)
        );
    }

    #[test]
    fn extract_quoted_field_rejects_multiline_values() {
        assert_eq!(
            extract_quoted_field("user.account_id=\"acct-123\nnext\"", "user.account_id=\""),
            None
        );
    }

    #[test]
    fn can_continue_without_email_requires_rotation_or_healthy_quota() {
        assert!(can_continue_without_email(true, None, None));
        assert!(can_continue_without_email(false, Some(true), Some(100)));
        assert!(!can_continue_without_email(false, Some(true), Some(10)));
        assert!(!can_continue_without_email(false, Some(false), Some(100)));
        assert!(!can_continue_without_email(false, None, None));
    }

    #[test]
    fn transient_recovery_retry_due_requires_delay_window() {
        let event = ThreadRecoveryEvent {
            source_log_id: 10,
            source_ts: Utc::now()
                .timestamp()
                .saturating_sub(TRANSIENT_RECOVERY_RETRY_DELAY_SECS - 5),
            thread_id: "thread-capacity".to_string(),
            kind: ThreadRecoveryKind::ModelCapacity,
            exhausted_turn_id: Some("turn-capacity".to_string()),
            exhausted_email: None,
            exhausted_account_id: None,
            message: MODEL_CAPACITY_ERROR_MESSAGE.to_string(),
            rehydration: None,
        };
        assert!(!transient_recovery_retry_due(&event));

        let ready = ThreadRecoveryEvent {
            source_ts: Utc::now()
                .timestamp()
                .saturating_sub(TRANSIENT_RECOVERY_RETRY_DELAY_SECS + 1),
            ..event
        };
        assert!(transient_recovery_retry_due(&ready));
    }

    #[test]
    fn transient_transport_retry_due_requires_delay_window() {
        let event = ThreadRecoveryEvent {
            source_log_id: 10,
            source_ts: Utc::now()
                .timestamp()
                .saturating_sub(TRANSIENT_RECOVERY_RETRY_DELAY_SECS - 5),
            thread_id: "thread-transport".to_string(),
            kind: ThreadRecoveryKind::TransportDisconnected,
            exhausted_turn_id: Some("turn-transport".to_string()),
            exhausted_email: None,
            exhausted_account_id: None,
            message: "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)".to_string(),
            rehydration: None,
        };
        assert!(!transient_recovery_retry_due(&event));

        let ready = ThreadRecoveryEvent {
            source_ts: Utc::now()
                .timestamp()
                .saturating_sub(TRANSIENT_RECOVERY_RETRY_DELAY_SECS + 1),
            ..event
        };
        assert!(transient_recovery_retry_due(&ready));
    }

    #[test]
    fn detects_stalled_turn_from_rollout_tail() {
        let tempdir = tempfile::tempdir().unwrap();
        let rollout_path = tempdir.path().join("stalled-rollout.jsonl");
        let state_file = tempdir.path().join("state_5.sqlite");
        let logs_file = tempdir.path().join("logs_2.sqlite");

        let last_event_ts = Utc
            .with_ymd_and_hms(2026, 4, 9, 7, 29, 9)
            .single()
            .unwrap()
            .timestamp();
        std::fs::write(
            &rollout_path,
            concat!(
                "{\"timestamp\":\"2026-04-09T07:28:33Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-stalled\"}}\n",
                "{\"timestamp\":\"2026-04-09T07:29:09Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"working\"}}\n"
            ),
        )
        .unwrap();

        let state_connection = Connection::open(&state_file).unwrap();
        state_connection
            .execute_batch(&format!(
                r#"
create table threads (
  id text primary key,
  rollout_path text not null,
  updated_at integer not null,
  archived integer not null default 0
);
insert into threads (id, rollout_path, updated_at, archived) values
  ('thread-stalled', '{}', {}, 0);
                "#,
                rollout_path.display(),
                last_event_ts
            ))
            .unwrap();

        let logs_connection = Connection::open(&logs_file).unwrap();
        logs_connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text,
  thread_id text
);
insert into logs (id, ts, target, feedback_log_body, thread_id) values
  (
    42,
    1775719713,
    'codex_core::codex',
    'session_loop{thread_id=thread-stalled}: Submission sub=Submission { id: "turn-stalled", op: UserInput { items: [Text { text: "continue with skipped msgs" }] } }',
    'thread-stalled'
  );
                "#,
            )
            .unwrap();

        let events = scan_stalled_turn_recovery_events(
            &logs_connection,
            &state_file,
            last_event_ts + STALLED_TURN_RECOVERY_DELAY_SECS + 1,
            8,
        )
        .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].thread_id, "thread-stalled");
        assert_eq!(events[0].kind, ThreadRecoveryKind::StalledTurn);
        assert_eq!(events[0].source_log_id, 42);
        assert_eq!(events[0].exhausted_turn_id.as_deref(), Some("turn-stalled"));
    }

    #[test]
    fn completed_turn_is_not_marked_as_stalled() {
        let tempdir = tempfile::tempdir().unwrap();
        let rollout_path = tempdir.path().join("completed-rollout.jsonl");
        let state_file = tempdir.path().join("state_5.sqlite");
        let logs_file = tempdir.path().join("logs_2.sqlite");

        let last_event_ts = Utc
            .with_ymd_and_hms(2026, 4, 9, 7, 29, 9)
            .single()
            .unwrap()
            .timestamp();
        std::fs::write(
            &rollout_path,
            concat!(
                "{\"timestamp\":\"2026-04-09T07:28:33Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-complete\"}}\n",
                "{\"timestamp\":\"2026-04-09T07:29:09Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_complete\",\"turn_id\":\"turn-complete\",\"last_agent_message\":null}}\n"
            ),
        )
        .unwrap();

        let state_connection = Connection::open(&state_file).unwrap();
        state_connection
            .execute_batch(&format!(
                r#"
create table threads (
  id text primary key,
  rollout_path text not null,
  updated_at integer not null,
  archived integer not null default 0
);
insert into threads (id, rollout_path, updated_at, archived) values
  ('thread-complete', '{}', {}, 0);
                "#,
                rollout_path.display(),
                last_event_ts
            ))
            .unwrap();

        let logs_connection = Connection::open(&logs_file).unwrap();
        logs_connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text,
  thread_id text
);
                "#,
            )
            .unwrap();

        let events = scan_stalled_turn_recovery_events(
            &logs_connection,
            &state_file,
            last_event_ts + STALLED_TURN_RECOVERY_DELAY_SECS + 1,
            8,
        )
        .unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn plan_mode_turn_is_not_marked_as_stalled() {
        let tempdir = tempfile::tempdir().unwrap();
        let rollout_path = tempdir.path().join("plan-rollout.jsonl");
        let state_file = tempdir.path().join("state_5.sqlite");
        let logs_file = tempdir.path().join("logs_2.sqlite");

        let last_event_ts = Utc
            .with_ymd_and_hms(2026, 4, 9, 7, 29, 9)
            .single()
            .unwrap()
            .timestamp();
        std::fs::write(
            &rollout_path,
            concat!(
                "{\"timestamp\":\"2026-04-09T07:28:33Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"task_started\",\"turn_id\":\"turn-plan\",\"collaboration_mode_kind\":\"plan\"}}\n",
                "{\"timestamp\":\"2026-04-09T07:29:09Z\",\"type\":\"event_msg\",\"payload\":{\"type\":\"agent_message\",\"message\":\"working\"}}\n"
            ),
        )
        .unwrap();

        let state_connection = Connection::open(&state_file).unwrap();
        state_connection
            .execute_batch(&format!(
                r#"
create table threads (
  id text primary key,
  rollout_path text not null,
  updated_at integer not null,
  archived integer not null default 0
);
insert into threads (id, rollout_path, updated_at, archived) values
  ('thread-plan', '{}', {}, 0);
                "#,
                rollout_path.display(),
                last_event_ts
            ))
            .unwrap();

        let logs_connection = Connection::open(&logs_file).unwrap();
        logs_connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text,
  thread_id text
);
                "#,
            )
            .unwrap();

        let events = scan_stalled_turn_recovery_events(
            &logs_connection,
            &state_file,
            last_event_ts + STALLED_TURN_RECOVERY_DELAY_SECS + 1,
            8,
        )
        .unwrap();

        assert!(events.is_empty());
    }

    #[test]
    fn merge_thread_recovery_events_keeps_latest_event_per_thread() {
        let merged = merge_thread_recovery_events(
            &[ThreadRecoveryEvent {
                source_log_id: 10,
                source_ts: 10,
                thread_id: "thread-a".to_string(),
                kind: ThreadRecoveryKind::QuotaExhausted,
                exhausted_turn_id: Some("turn-1".to_string()),
                exhausted_email: Some("a@example.com".to_string()),
                exhausted_account_id: None,
                message: "You've hit your usage limit.".to_string(),
                rehydration: None,
            }],
            vec![
                ThreadRecoveryEvent {
                    source_log_id: 11,
                    source_ts: 11,
                    thread_id: "thread-b".to_string(),
                    kind: ThreadRecoveryKind::QuotaExhausted,
                    exhausted_turn_id: Some("turn-2".to_string()),
                    exhausted_email: Some("b@example.com".to_string()),
                    exhausted_account_id: None,
                    message: "You've hit your usage limit.".to_string(),
                    rehydration: None,
                },
                ThreadRecoveryEvent {
                    source_log_id: 12,
                    source_ts: 12,
                    thread_id: "thread-a".to_string(),
                    kind: ThreadRecoveryKind::QuotaExhausted,
                    exhausted_turn_id: Some("turn-3".to_string()),
                    exhausted_email: Some("a@example.com".to_string()),
                    exhausted_account_id: None,
                    message: "You've hit your usage limit.".to_string(),
                    rehydration: None,
                },
            ],
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].thread_id, "thread-b");
        assert_eq!(merged[0].source_log_id, 11);
        assert_eq!(merged[1].thread_id, "thread-a");
        assert_eq!(merged[1].source_log_id, 12);
    }

    #[test]
    fn process_thread_recovery_events_keeps_blocked_events_without_starving_later_threads() {
        let events = vec![
            ThreadRecoveryEvent {
                source_log_id: 10,
                source_ts: 10,
                thread_id: "thread-a".to_string(),
                kind: ThreadRecoveryKind::QuotaExhausted,
                exhausted_turn_id: Some("turn-a".to_string()),
                exhausted_email: None,
                exhausted_account_id: None,
                message: "You've hit your usage limit.".to_string(),
                rehydration: None,
            },
            ThreadRecoveryEvent {
                source_log_id: 11,
                source_ts: 11,
                thread_id: "thread-b".to_string(),
                kind: ThreadRecoveryKind::QuotaExhausted,
                exhausted_turn_id: Some("turn-b".to_string()),
                exhausted_email: None,
                exhausted_account_id: None,
                message: "You've hit your usage limit.".to_string(),
                rehydration: None,
            },
        ];

        let result = process_thread_recovery_events(&events, |event| {
            Ok(if event.thread_id == "thread-a" {
                RecoveryResolution::Blocked
            } else {
                RecoveryResolution::Continued
            })
        })
        .unwrap();

        assert_eq!(result.continued_thread_ids, vec!["thread-b".to_string()]);
        assert_eq!(result.pending_events.len(), 1);
        assert_eq!(result.pending_events[0].thread_id, "thread-a");
    }

    #[test]
    fn active_thread_drops_stalled_turn_recovery_but_blocks_other_recovery_kinds() {
        assert_eq!(
            recovery_resolution_for_active_thread(ThreadRecoveryKind::StalledTurn),
            RecoveryResolution::Dropped
        );
        assert_eq!(
            recovery_resolution_for_active_thread(ThreadRecoveryKind::QuotaExhausted),
            RecoveryResolution::Blocked
        );
    }

    #[test]
    fn continue_strategy_prefers_direct_continue_for_non_ready_thread_with_cwd() {
        let thread = thread_summary("systemError", Some("/tmp/project"));

        assert_eq!(
            continue_strategy_for_thread(Some(&thread)),
            ContinueStrategy::DirectThenResume {
                initial_cwd: Some("/tmp/project".to_string())
            }
        );
    }

    #[test]
    fn continue_strategy_requires_resume_when_non_ready_thread_has_no_cwd() {
        let thread = thread_summary("systemError", None);

        assert_eq!(
            continue_strategy_for_thread(Some(&thread)),
            ContinueStrategy::ResumeThenContinue { initial_cwd: None }
        );
        assert_eq!(
            continue_strategy_for_thread(None),
            ContinueStrategy::ResumeThenContinue { initial_cwd: None }
        );
    }

    #[test]
    fn terminal_resume_failure_resolves_to_dropped() {
        assert_eq!(
            recovery_resolution_for_prepare_continue_error(&anyhow!(
                "Codex thread/resume request failed: {{\"code\":-32600,\"message\":\"no rollout found for thread id thread-123\"}}"
            )),
            RecoveryResolution::Dropped
        );
        assert_eq!(
            recovery_resolution_for_prepare_continue_error(&anyhow!(
                "Timed out waiting for thread/resume response from Codex."
            )),
            RecoveryResolution::Blocked
        );
    }

    #[test]
    fn terminal_thread_recovery_error_matches_missing_rollout() {
        assert!(is_terminal_thread_recovery_error(&anyhow!(
            "Codex thread/resume request failed: {{\"code\":-32600,\"message\":\"no rollout found for thread id thread-123\"}}"
        )));
        assert!(is_terminal_thread_recovery_error(
            &anyhow!(
                "Codex thread/resume request failed: {{\"code\":-32600,\"message\":\"no rollout found for thread id thread-123\"}}"
            )
            .context("Failed to resume thread thread-123 before continue.")
        ));
        assert!(!is_terminal_thread_recovery_error(&anyhow!(
            "Timed out waiting for thread/resume response from Codex."
        )));
    }
}
