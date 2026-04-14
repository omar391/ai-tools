use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use serde_yaml::Value as YamlValue;

use crate::auth::{
    extract_account_id_from_auth, load_codex_auth, summarize_codex_auth, write_codex_auth,
    CodexAuth,
};
use crate::bridge::{
    run_automation_bridge, run_automation_bridge_with_progress, AutomationProgressCallback,
};
use crate::cancel;
use crate::managed_browser::ensure_managed_browser_wrapper;
use crate::paths::{
    ensure_main_worktree_operation_allowed, legacy_credentials_file, resolve_paths,
};
use crate::pool::{
    cmd_add_expected_email, find_next_usable_account, format_account_summary_for_display,
    inspect_account, load_pool, normalize_pool_entries, resolve_account_selector, save_pool,
    sync_pool_active_account_from_codex, AccountEntry, AccountInspection, Pool,
    ReusableAccountProbeMode,
};
use crate::quota::{describe_quota_blocker, format_compact_quota, has_usable_quota};
use crate::state::{load_rotate_state_json, write_rotate_state_json};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const DEFAULT_CODEX_BIN: &str = "codex";
const DEFAULT_CODEX_APP_BUNDLE_BIN: &str = "/Applications/Codex.app/Contents/Resources/codex";
const DEFAULT_CREATE_BASE_EMAIL: &str = "dev.{n}@astronlab.com";
const ROTATE_STATE_VERSION: u8 = 4;
const EMAIL_FAMILY_PLACEHOLDER: &str = "{n}";
const AUTO_CREATE_RETRY_DELAY: Duration = Duration::from_secs(2);
const DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS: usize = 6;
const FINAL_ADD_PHONE_CODEX_LOGIN_MAX_ATTEMPTS: usize = 10;
const DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES: usize = 5;
const DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS: &[u64] = &[15_000, 30_000, 60_000, 120_000, 240_000];
const DEFAULT_CODEX_LOGIN_VERIFICATION_RETRY_DELAYS_MS: &[u64] =
    &[5_000, 10_000, 20_000, 30_000, 60_000];
const DEFAULT_CODEX_LOGIN_RETRYABLE_TIMEOUT_DELAYS_MS: &[u64] =
    &[8_000, 15_000, 30_000, 60_000, 120_000];
const DEFAULT_CODEX_LOGIN_RATE_LIMIT_RETRY_DELAYS_MS: &[u64] =
    &[30_000, 60_000, 120_000, 240_000, 300_000];
const DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY: u32 = 10;
const AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT: &str =
    "Automatic account creation stopped retrying because a reusable account is now available.";
const CREATE_ALREADY_IN_PROGRESS_PREFIX: &str = "Another create command is already in progress";
const CREATE_LOCK_WAIT_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateCommandSource {
    Manual,
    Next,
}

#[derive(Clone, Debug)]
pub struct CreateCommandOptions {
    pub alias: Option<String>,
    pub profile_name: Option<String>,
    pub base_email: Option<String>,
    pub force: bool,
    pub ignore_current: bool,
    pub restore_previous_auth_after_create: bool,
    pub require_usable_quota: bool,
    pub source: CreateCommandSource,
}

impl Default for CreateCommandOptions {
    fn default() -> Self {
        Self {
            alias: None,
            profile_name: None,
            base_email: None,
            force: false,
            ignore_current: false,
            restore_previous_auth_after_create: false,
            require_usable_quota: false,
            source: CreateCommandSource::Manual,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReloginOptions {
    pub allow_email_change: bool,
    pub logout_first: bool,
    pub manual_login: bool,
}

impl Default for ReloginOptions {
    fn default() -> Self {
        Self {
            allow_email_change: false,
            logout_first: true,
            manual_login: false,
        }
    }
}

#[derive(Clone, Debug)]
struct CreateCommandResult {
    entry: AccountEntry,
    inspection: Option<AccountInspection>,
    profile_name: String,
    base_email: String,
}

#[derive(Debug)]
enum CreateFlowAttemptFailure {
    Fatal(anyhow::Error),
    Retryable(anyhow::Error),
}

#[derive(Debug)]
struct WorkflowSkipAccountError {
    message: String,
}

impl WorkflowSkipAccountError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl Display for WorkflowSkipAccountError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message.as_str())
    }
}

impl std::error::Error for WorkflowSkipAccountError {}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct CreateExecutionLockMetadata {
    pid: u32,
    started_at: String,
    source: String,
    profile_name: Option<String>,
    base_email: Option<String>,
    alias: Option<String>,
    force: bool,
    ignore_current: bool,
    require_usable_quota: bool,
}

struct CreateExecutionLock {
    process_guard: Option<MutexGuard<'static, ()>>,
    file: Option<File>,
    path: PathBuf,
}

impl Drop for CreateExecutionLock {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            drop(file);
        }
        let _ = fs::remove_file(&self.path);
        if let Some(process_guard) = self.process_guard.take() {
            drop(process_guard);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialFamily {
    pub profile_name: String,
    pub base_email: String,
    pub next_suffix: u32,
    #[serde(default = "default_max_skipped_slots_per_family")]
    pub max_skipped_slots: u32,
    pub created_at: String,
    pub updated_at: String,
    pub last_created_email: Option<String>,
}

fn default_max_skipped_slots_per_family() -> u32 {
    DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexRotateSecretRef {
    #[serde(rename = "type")]
    pub ref_type: String,
    pub store: String,
    pub object_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoredCredential {
    pub email: String,
    pub profile_name: String,
    pub base_email: String,
    pub suffix: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_year: Option<u16>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingCredential {
    #[serde(flatten)]
    pub stored: StoredCredential,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CodexRotateSecretLocator {
    LoginLookup {
        store: String,
        username: String,
        uris: Vec<String>,
        field_path: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct CodexRotateAuthFlowSession {
    auth_url: Option<String>,
    callback_url: Option<String>,
    callback_port: Option<u16>,
    device_code: Option<String>,
    session_dir: Option<String>,
    codex_home_path: Option<String>,
    auth_file_path: Option<String>,
    pid: Option<u32>,
    stdout_path: Option<String>,
    stderr_path: Option<String>,
    exit_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct CodexRotateAuthFlowSummary {
    stage: Option<String>,
    current_url: Option<String>,
    headline: Option<String>,
    callback_complete: Option<bool>,
    success: Option<bool>,
    account_ready: Option<bool>,
    needs_email_verification: Option<bool>,
    follow_up_step: Option<bool>,
    retryable_timeout: Option<bool>,
    session_ended: Option<bool>,
    existing_account_prompt: Option<bool>,
    username_not_found: Option<bool>,
    invalid_credentials: Option<bool>,
    rate_limit_exceeded: Option<bool>,
    anti_bot_gate: Option<bool>,
    auth_prompt: Option<bool>,
    consent_blocked: Option<bool>,
    consent_error: Option<String>,
    next_action: Option<String>,
    replay_reason: Option<String>,
    retry_reason: Option<String>,
    error_message: Option<String>,
    verified_account_email: Option<String>,
    codex_session: Option<CodexRotateAuthFlowSession>,
    codex_login_exit_ok: Option<bool>,
    codex_login_exit_code: Option<i32>,
    codex_login_stdout_tail: Option<String>,
    codex_login_stderr_tail: Option<String>,
    saw_oauth_consent: Option<bool>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct FastBrowserStepState {
    #[serde(default)]
    action: Option<Value>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct FastBrowserState {
    #[serde(default)]
    steps: HashMap<String, FastBrowserStepState>,
}

#[derive(Clone, Debug, Deserialize, Default)]
struct FastBrowserRunObservability {
    #[serde(default)]
    run_path: Option<String>,
    #[serde(default)]
    status_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct FastBrowserRunResult {
    #[serde(default)]
    state: Option<FastBrowserState>,
    #[serde(default)]
    output: Option<Value>,
    #[serde(default, alias = "recent_events")]
    recent_events: Option<Vec<Value>>,
    #[serde(default)]
    final_url: Option<String>,
    #[serde(default)]
    page: Option<Value>,
    #[serde(default)]
    current: Option<Value>,
    #[serde(default)]
    observability: Option<FastBrowserRunObservability>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialStore {
    pub version: u8,
    pub default_create_base_email: String,
    pub families: HashMap<String, CredentialFamily>,
    pub pending: HashMap<String, PendingCredential>,
    pub skipped: HashSet<String>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self {
            version: ROTATE_STATE_VERSION,
            default_create_base_email: DEFAULT_CREATE_BASE_EMAIL.to_string(),
            families: HashMap::new(),
            pending: HashMap::new(),
            skipped: HashSet::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AdultBirthDate {
    birth_month: u8,
    birth_day: u8,
    birth_year: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkflowFileMetadata {
    workflow_ref: Option<String>,
    preferred_profile_name: Option<String>,
    preferred_email: Option<String>,
    default_full_name: Option<String>,
    default_birth_month: Option<u8>,
    default_birth_day: Option<u8>,
    default_birth_year: Option<u16>,
}

impl WorkflowFileMetadata {
    fn default_birth_date(&self) -> Option<AdultBirthDate> {
        Some(AdultBirthDate {
            birth_month: self.default_birth_month?,
            birth_day: self.default_birth_day?,
            birth_year: self.default_birth_year?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct LoginWorkflowDefaults {
    workflow_ref: String,
    full_name: String,
    birth_date: AdultBirthDate,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManagedProfileEntry {
    name: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ManagedProfilesPayload {
    default: Option<String>,
    profiles: Vec<ManagedProfileEntry>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManagedProfilesInspection {
    managed_profiles: ManagedProfilesPayload,
}

#[derive(Clone, Debug, Deserialize)]
struct FastBrowserCliErrorPayload {
    message: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct FastBrowserCliEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error: Option<FastBrowserCliErrorPayload>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SystemChromeProfileCandidate {
    directory: String,
    name: String,
    emails: Vec<String>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
struct SystemChromeProfileMatch {
    directory: String,
    name: String,
    emails: Vec<String>,
    matched_email: String,
    score: i32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeEnsureSecretPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
    password: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeDeleteSecretPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeLoginOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    codex_bin: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_run_stamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip_locator_preflight: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_signup_recovery: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_password_login: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    full_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_year: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codex_session: Option<&'a CodexRotateAuthFlowSession>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeCompleteLoginAttemptPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    account_login_locator: Option<&'a CodexRotateSecretLocator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<BridgeLoginOptions<'a>>,
}

#[derive(Clone, Debug, Default)]
struct BridgeLoginAttemptResult {
    result: Option<FastBrowserRunResult>,
    error_message: Option<String>,
}

impl<'de> Deserialize<'de> for BridgeLoginAttemptResult {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        Ok(normalize_bridge_login_attempt_result(raw))
    }
}

fn normalize_bridge_login_attempt_result(raw: Value) -> BridgeLoginAttemptResult {
    let Value::Object(record) = raw else {
        return BridgeLoginAttemptResult::default();
    };
    let wrapped_result = record.get("result").cloned();
    let result = wrapped_result
        .or_else(|| {
            looks_like_fast_browser_run_result_record(&record)
                .then(|| Value::Object(record.clone()))
        })
        .and_then(normalize_bridge_fast_browser_run_result);
    let error_message = read_string_value(&record, "error_message")
        .or_else(|| read_string_value(&record, "errorMessage"));
    BridgeLoginAttemptResult {
        result,
        error_message,
    }
}

fn looks_like_fast_browser_run_result_record(record: &Map<String, Value>) -> bool {
    record.contains_key("state")
        || record.contains_key("output")
        || record.contains_key("observability")
        || record.contains_key("finalUrl")
        || record.contains_key("status")
        || record.contains_key("ok")
        || record.contains_key("page")
        || record.contains_key("current")
        || record.contains_key("recent_events")
}
fn normalize_bridge_fast_browser_run_result(raw: Value) -> Option<FastBrowserRunResult> {
    let raw = match raw {
        Value::String(value) => serde_json::from_str::<Value>(value.trim()).ok()?,
        other => other,
    };
    let record = raw.as_object()?;
    Some(hydrate_fast_browser_run_result_from_observability(
        FastBrowserRunResult {
            state: record
                .get("state")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            output: record.get("output").cloned(),
            recent_events: record
                .get("recentEvents")
                .or_else(|| record.get("recent_events"))
                .or_else(|| record.get("events"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            final_url: read_string_value(record, "finalUrl")
                .or_else(|| read_string_value(record, "final_url")),
            page: record.get("page").cloned(),
            current: record.get("current").cloned(),
            observability: record.get("observability").and_then(|value| {
                let observability = value.as_object()?;
                Some(FastBrowserRunObservability {
                    run_path: read_string_value(observability, "runPath")
                        .or_else(|| read_string_value(observability, "run_path")),
                    status_path: read_string_value(observability, "statusPath")
                        .or_else(|| read_string_value(observability, "status_path")),
                })
            }),
        },
    ))
}

fn hydrate_fast_browser_run_result_from_observability(
    mut result: FastBrowserRunResult,
) -> FastBrowserRunResult {
    if result.final_url.is_some() && result.output.is_some() && result.page.is_some() {
        return result;
    }

    let run_path_candidates = [
        result
            .observability
            .as_ref()
            .and_then(|value| value.run_path.as_deref()),
        result
            .observability
            .as_ref()
            .and_then(|value| value.status_path.as_deref()),
    ];

    for run_path in run_path_candidates.into_iter().flatten() {
        let Ok(contents) = fs::read_to_string(run_path) else {
            continue;
        };
        let Ok(snapshot) = serde_json::from_str::<Value>(&contents) else {
            continue;
        };
        let Some(record) = snapshot.as_object() else {
            continue;
        };
        if result.final_url.is_none() {
            result.final_url = read_string_value(record, "finalUrl")
                .or_else(|| read_string_value(record, "final_url"))
                .or_else(|| {
                    record
                        .get("page")
                        .and_then(Value::as_object)
                        .and_then(|page| read_string_value(page, "url"))
                });
        }
        if result.output.is_none() {
            result.output = record.get("output").cloned();
        }
        if result.recent_events.is_none() {
            result.recent_events = record
                .get("recentEvents")
                .or_else(|| record.get("recent_events"))
                .or_else(|| record.get("events"))
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok());
        }
        if result.page.is_none() {
            result.page = record.get("page").cloned();
        }
        if result.current.is_none() {
            result.current = record.get("current").cloned();
        }
        break;
    }

    result
}

fn normalize_codex_rotate_auth_flow_session(raw: &Value) -> Option<CodexRotateAuthFlowSession> {
    let record = raw.as_object()?;
    let session = CodexRotateAuthFlowSession {
        auth_url: read_string_value(record, "auth_url"),
        callback_url: read_string_value(record, "callback_url"),
        callback_port: read_u16_value(record, "callback_port"),
        device_code: read_string_value(record, "device_code"),
        session_dir: read_string_value(record, "session_dir"),
        codex_home_path: read_string_value(record, "codex_home_path"),
        auth_file_path: read_string_value(record, "auth_file_path"),
        pid: read_u32_value(record, "pid"),
        stdout_path: read_string_value(record, "stdout_path"),
        stderr_path: read_string_value(record, "stderr_path"),
        exit_path: read_string_value(record, "exit_path"),
    };
    if session.auth_url.is_none()
        && session.session_dir.is_none()
        && session.codex_home_path.is_none()
        && session.auth_file_path.is_none()
        && session.stdout_path.is_none()
        && session.stderr_path.is_none()
        && session.exit_path.is_none()
    {
        return None;
    }
    Some(session)
}

#[derive(Clone, Debug)]
struct EmailFamily {
    normalized: String,
    local_part: String,
    domain_part: String,
    mode: EmailFamilyMode,
}

#[derive(Clone, Debug)]
enum EmailFamilyMode {
    GmailPlus,
    Template { prefix: String, suffix: String },
}

fn create_lock_path() -> Result<std::path::PathBuf> {
    Ok(resolve_paths()?
        .rotate_home
        .join("locks")
        .join("create.lock"))
}

fn create_execution_mutex() -> &'static Mutex<()> {
    static CREATE_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    CREATE_MUTEX.get_or_init(|| Mutex::new(()))
}

fn create_lock_source_label(source: CreateCommandSource) -> &'static str {
    match source {
        CreateCommandSource::Manual => "manual",
        CreateCommandSource::Next => "next",
    }
}

fn read_create_execution_lock_metadata(path: &Path) -> Option<CreateExecutionLockMetadata> {
    let raw = fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn format_create_execution_lock_error(metadata: Option<CreateExecutionLockMetadata>) -> String {
    let Some(metadata) = metadata else {
        return format!("{CREATE_ALREADY_IN_PROGRESS_PREFIX}.");
    };
    let mut details = vec![
        format!("pid {}", metadata.pid),
        format!("started {}", metadata.started_at),
        format!("source {}", metadata.source),
    ];
    if let Some(profile_name) = metadata.profile_name.as_deref() {
        details.push(format!("profile {}", profile_name));
    }
    if let Some(base_email) = metadata.base_email.as_deref() {
        details.push(format!("base {}", base_email));
    }
    if let Some(alias) = metadata.alias.as_deref() {
        details.push(format!("alias {}", alias));
    }
    format!(
        "{CREATE_ALREADY_IN_PROGRESS_PREFIX} ({}).",
        details.join(", ")
    )
}

fn acquire_create_execution_lock(
    options: &CreateCommandOptions,
    progress: Option<&AutomationProgressCallback>,
) -> Result<CreateExecutionLock> {
    let process_guard = create_execution_mutex()
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let lock_path = create_lock_path()?;
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .with_context(|| format!("Failed to open {}.", lock_path.display()))?;
    let mut reported_wait = false;
    loop {
        cancel::check_canceled()?;
        match file.try_lock_exclusive() {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if !reported_wait {
                    report_progress(
                        progress,
                        format!(
                            "{} Waiting for it to finish.",
                            format_create_execution_lock_error(
                                read_create_execution_lock_metadata(&lock_path)
                            )
                        ),
                    );
                    reported_wait = true;
                }
                cancel::sleep_with_cancellation(CREATE_LOCK_WAIT_INTERVAL)?;
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("Failed to acquire create lock at {}.", lock_path.display())
                });
            }
        }
    }

    let metadata = CreateExecutionLockMetadata {
        pid: std::process::id(),
        started_at: now_iso(),
        source: create_lock_source_label(options.source).to_string(),
        profile_name: options.profile_name.clone(),
        base_email: options.base_email.clone(),
        alias: options.alias.clone(),
        force: options.force,
        ignore_current: options.ignore_current,
        require_usable_quota: options.require_usable_quota,
    };
    let serialized = serde_json::to_vec_pretty(&metadata)
        .context("Failed to serialize create lock metadata.")?;
    file.set_len(0)
        .with_context(|| format!("Failed to truncate {}.", lock_path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("Failed to seek {}.", lock_path.display()))?;
    file.write_all(&serialized)
        .with_context(|| format!("Failed to write {}.", lock_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("Failed to finalize {}.", lock_path.display()))?;
    file.flush()
        .with_context(|| format!("Failed to flush {}.", lock_path.display()))?;
    Ok(CreateExecutionLock {
        process_guard: Some(process_guard),
        file: Some(file),
        path: lock_path,
    })
}

pub fn is_create_already_in_progress_error(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .starts_with(CREATE_ALREADY_IN_PROGRESS_PREFIX)
}

pub fn cmd_create(options: CreateCommandOptions) -> Result<String> {
    cmd_create_with_progress(options, None)
}

pub fn cmd_create_with_progress(
    options: CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let _lock = acquire_create_execution_lock(&options, progress.as_ref())?;
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;
    let previous_auth = if options.restore_previous_auth_after_create {
        load_codex_auth_if_exists()?
    } else {
        None
    };

    if !options.force && !pool.accounts.is_empty() {
        let previous_index = pool.active_index;
        let previous = pool.accounts[previous_index].clone();
        let mut reasons = Vec::new();
        let skip_indices = HashSet::new();
        let (candidate, candidate_dirty) = find_next_usable_account(
            &mut pool,
            &paths.codex_auth_file,
            if options.ignore_current {
                ReusableAccountProbeMode::OthersOnly
            } else {
                ReusableAccountProbeMode::CurrentFirst
            },
            &mut reasons,
            dirty,
            &skip_indices,
        )?;
        dirty = candidate_dirty;

        if let Some(candidate) = candidate {
            let switched = candidate.index != previous_index;
            if switched {
                pool.active_index = candidate.index;
                write_codex_auth(&paths.codex_auth_file, &candidate.entry.auth)?;
            }
            if dirty || switched {
                save_pool(&pool)?;
            }

            let quota_summary = candidate
                .inspection
                .usage
                .as_ref()
                .map(format_compact_quota)
                .unwrap_or_else(|| "quota unavailable".to_string());

            if switched {
                return Ok(format!(
                    "{GREEN}OK{RESET} Reused {} instead of creating a new account.\nQuota: {}",
                    candidate.entry.label, quota_summary
                ));
            }

            return Ok(format!(
                "{GREEN}OK{RESET} Current account {} still has healthy quota.\nQuota: {}",
                previous.label, quota_summary
            ));
        }
    }

    if dirty {
        save_pool(&pool)?;
    }

    let result = execute_create_flow_with_progress(&options, progress)?;
    let quota_summary = summarize_quota_for_create(&result);
    if options.restore_previous_auth_after_create {
        restore_active_auth(previous_auth.as_ref())?;
        return Ok(format!(
            "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}\nCurrent session unchanged.",
            result.entry.label, result.profile_name, result.base_email, quota_summary
        ));
    }
    Ok(format!(
        "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}",
        result.entry.label, result.profile_name, result.base_email, quota_summary
    ))
}

pub fn migrate_legacy_credential_store_if_needed() -> Result<bool> {
    let legacy_file = legacy_credentials_file()?;
    if !legacy_file.exists() {
        return Ok(false);
    }

    let raw = fs::read_to_string(&legacy_file)
        .with_context(|| format!("Failed to read {}.", legacy_file.display()))?;
    let parsed: Value = serde_json::from_str(&raw)
        .with_context(|| format!("Invalid credential store at {}.", legacy_file.display()))?;
    let store = normalize_credential_store(parsed);
    save_credential_store(&store)?;
    fs::remove_file(&legacy_file)
        .with_context(|| format!("Failed to remove {}.", legacy_file.display()))?;
    Ok(true)
}

pub fn cmd_relogin(selector: &str, options: ReloginOptions) -> Result<String> {
    cmd_relogin_with_progress(selector, options, None)
}

pub fn cmd_relogin_with_progress(
    selector: &str,
    options: ReloginOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let selection = {
        let pool = load_pool()?;
        resolve_account_selector(&pool, selector)?
    };
    let existing = selection.entry.clone();
    let expected_email = existing.email.clone();
    let mut store = load_credential_store()?;
    let stored_credential = resolve_relogin_credential(&store, &existing);

    if should_use_stored_credential_relogin(stored_credential.as_ref(), &options) {
        let stored_credential = stored_credential.ok_or_else(|| {
            anyhow!("Stored credential lookup unexpectedly failed for {expected_email}.")
        })?;
        let mut updated_stored = stored_credential.clone();
        updated_stored.updated_at = now_iso();
        let account_login_locator = build_openai_account_login_locator(&updated_stored.email);

        let previous_auth = load_codex_auth_if_exists()?;
        let login_result = (|| -> Result<CompleteCodexLoginOutcome> {
            if should_logout_before_stored_relogin(&options) {
                let paths = resolve_paths()?;
                if paths.codex_auth_file.exists() {
                    run_codex_command(["logout"])?;
                }
            }

            run_complete_codex_login(CompleteCodexLoginArgs {
                profile_name: &updated_stored.profile_name,
                email: &updated_stored.email,
                account_login_locator: Some(&account_login_locator),
                workflow_ref: None,
                codex_bin: None,
                workflow_run_stamp: None,
                skip_locator_preflight: None,
                prefer_signup_recovery: None,
                prefer_password_login: None,
                birth_date: None,
                progress: progress.clone(),
            })
        })();
        let login_outcome = match login_result {
            Ok(value) => value,
            Err(error) => {
                restore_active_auth(previous_auth.as_ref())?;
                return Err(error);
            }
        };

        let auth = load_auth_for_completed_login(&login_outcome)?;
        let logged_in_email = summarize_codex_auth(&auth).email;
        if !options.allow_email_change
            && normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
            && !workflow_verified_expected_email(
                login_outcome.verified_account_email.as_deref(),
                &expected_email,
            )
        {
            restore_active_auth(previous_auth.as_ref())?;
            return Err(anyhow!(
                "Expected {}, but Codex logged into {}.",
                expected_email,
                logged_in_email
            ));
        }

        let _ = cmd_add_expected_email(&expected_email, existing.alias.as_deref())?;
        if let Some(inspected) =
            inspect_pool_entry_by_account_id(&extract_account_id_from_auth(&auth))?
        {
            let mut dirty = false;
            if store
                .pending
                .remove(&normalize_email_key(&updated_stored.email))
                .is_some()
            {
                dirty = true;
            }
            dirty |= upsert_family_for_account(
                &mut store,
                &StoredCredential {
                    selector: Some(inspected.entry.label.clone()),
                    alias: inspected
                        .entry
                        .alias
                        .clone()
                        .or_else(|| existing.alias.clone()),
                    updated_at: now_iso(),
                    ..updated_stored
                },
            );
            if dirty {
                save_credential_store(&store)?;
            }
        }

        return Ok(format!(
            "{GREEN}OK{RESET} Re-logged {} with stored managed-browser credentials.",
            format_account_summary_for_display(&existing)
        ));
    }

    if stored_credential.is_none() && !options.manual_login {
        eprintln!(
            "{YELLOW}WARN{RESET} No stored credentials were found for {}. Falling back to manual login.",
            expected_email
        );
    }

    if options.logout_first {
        let paths = resolve_paths()?;
        if paths.codex_auth_file.exists() {
            run_codex_command(["logout"])?;
        }
    }

    report_progress(
        progress.as_ref(),
        format!("Opening Codex login flow for {expected_email}."),
    );
    run_codex_command(["login"])?;

    let auth = load_current_auth()?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
        && !options.allow_email_change
    {
        return Err(anyhow!(
            "Logged into {}, but \"{}\" expects {}. The pool was not updated. Re-run with --allow-email-change if you want to replace it.",
            logged_in_email,
            format_account_summary_for_display(&existing),
            expected_email
        ));
    }

    cmd_add_expected_email(&expected_email, existing.alias.as_deref())
}

pub fn should_use_stored_credential_relogin(
    stored_credential: Option<&StoredCredential>,
    options: &ReloginOptions,
) -> bool {
    stored_credential.is_some() && !options.manual_login
}

fn should_logout_before_stored_relogin(options: &ReloginOptions) -> bool {
    options.logout_first
}

pub fn create_next_fallback_options() -> CreateCommandOptions {
    CreateCommandOptions {
        require_usable_quota: true,
        source: CreateCommandSource::Next,
        ..CreateCommandOptions::default()
    }
}

pub fn reconcile_added_account_credential_state(entry: &AccountEntry) -> Result<bool> {
    let raw_state = load_rotate_state_json()?;
    let raw_pending = normalize_pending_credential_map(raw_state.get("pending"));
    let mut store = normalize_credential_store(raw_state);
    let mut dirty = false;
    let updated_at = now_iso();
    let normalized_email = normalize_email_key(&entry.email);

    if let Some(pending) = raw_pending.get(&normalized_email).cloned() {
        dirty = true;
        store.pending.remove(&normalized_email);
        dirty |= upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: pending.stored.profile_name,
                base_email: pending.stored.base_email,
                suffix: pending.stored.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: pending.stored.birth_month,
                birth_day: pending.stored.birth_day,
                birth_year: pending.stored.birth_year,
                created_at: pending.stored.created_at,
                updated_at: updated_at.clone(),
            },
        );
    } else if let Some(family_match) = select_family_for_account_email(&store, &entry.email) {
        dirty |= upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: family_match.family.profile_name,
                base_email: family_match.family.base_email,
                suffix: family_match.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: None,
                birth_day: None,
                birth_year: None,
                created_at: family_match.family.created_at,
                updated_at,
            },
        );
    }

    if dirty {
        save_credential_store(&store)?;
    }

    Ok(dirty)
}

fn execute_create_flow_with_progress(
    options: &CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<CreateCommandResult> {
    let mut attempt = 1usize;
    loop {
        cancel::check_canceled()?;
        match execute_create_flow_attempt(options, progress.clone()) {
            Ok(result) => return Ok(result),
            Err(CreateFlowAttemptFailure::Retryable(error))
                if should_retry_create_after_error(options, &error) =>
            {
                let workflow_skip = is_workflow_skip_account_error(&error);
                if should_stop_create_retry_for_reusable_account(options)
                    && reusable_account_exists_for_auto_create_retry(options)?
                {
                    return Err(anyhow!(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT));
                }
                report_progress(
                    progress.as_ref(),
                    format!(
                        "{} account creation attempt {attempt} failed: {error}. Retrying in {}s.",
                        if workflow_skip {
                            "Workflow-skipped"
                        } else {
                            "Automatic"
                        },
                        AUTO_CREATE_RETRY_DELAY.as_secs()
                    ),
                );
                eprintln!(
                    "{YELLOW}WARN{RESET} {} account creation attempt {attempt} failed: {error}. Retrying with a fresh account in {}s.",
                    if workflow_skip {
                        "Workflow-skipped"
                    } else {
                        "Automatic"
                    },
                    AUTO_CREATE_RETRY_DELAY.as_secs()
                );
                attempt = attempt.saturating_add(1);
                cancel::sleep_with_cancellation(AUTO_CREATE_RETRY_DELAY)?;
            }
            Err(CreateFlowAttemptFailure::Fatal(error))
                if should_retry_create_until_usable(options) =>
            {
                if should_stop_create_retry_for_reusable_account(options)
                    && reusable_account_exists_for_auto_create_retry(options)?
                {
                    return Err(anyhow!(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT));
                }
                report_progress(
                    progress.as_ref(),
                    format!(
                        "Automatic account creation attempt {attempt} failed: {error}. Retrying in {}s.",
                        AUTO_CREATE_RETRY_DELAY.as_secs()
                    ),
                );
                eprintln!(
                    "{YELLOW}WARN{RESET} Automatic account creation attempt {attempt} failed: {error}. Retrying with a fresh account in {}s.",
                    AUTO_CREATE_RETRY_DELAY.as_secs()
                );
                attempt = attempt.saturating_add(1);
                cancel::sleep_with_cancellation(AUTO_CREATE_RETRY_DELAY)?;
            }
            Err(CreateFlowAttemptFailure::Retryable(error))
            | Err(CreateFlowAttemptFailure::Fatal(error)) => return Err(error),
        }
    }
}

fn report_progress(progress: Option<&AutomationProgressCallback>, message: impl Into<String>) {
    if let Some(progress) = progress {
        progress(message.into());
    }
}

fn fatal<T>(result: Result<T>) -> std::result::Result<T, CreateFlowAttemptFailure> {
    result.map_err(CreateFlowAttemptFailure::Fatal)
}

fn should_retry_create_until_usable(options: &CreateCommandOptions) -> bool {
    options.require_usable_quota && matches!(options.source, CreateCommandSource::Next)
}

fn should_retry_create_after_error(options: &CreateCommandOptions, error: &anyhow::Error) -> bool {
    should_retry_create_until_usable(options) || is_workflow_skip_account_error(error)
}

fn should_stop_create_retry_for_reusable_account(options: &CreateCommandOptions) -> bool {
    matches!(options.source, CreateCommandSource::Next)
}

pub fn is_auto_create_retry_stopped_for_reusable_account(error: &anyhow::Error) -> bool {
    error
        .to_string()
        .contains(AUTO_CREATE_RETRY_STOPPED_FOR_REUSABLE_ACCOUNT)
}

fn reusable_account_exists_for_auto_create_retry(options: &CreateCommandOptions) -> Result<bool> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

    if pool.accounts.is_empty() {
        if dirty {
            save_pool(&pool)?;
        }
        return Ok(false);
    }

    let mut reasons = Vec::new();
    let skip_indices = HashSet::new();
    let mode = if options.ignore_current {
        ReusableAccountProbeMode::OthersOnly
    } else {
        ReusableAccountProbeMode::CurrentFirst
    };
    let (candidate, candidate_dirty) = find_next_usable_account(
        &mut pool,
        &paths.codex_auth_file,
        mode,
        &mut reasons,
        dirty,
        &skip_indices,
    )?;
    if candidate_dirty {
        save_pool(&pool)?;
    }
    Ok(candidate.is_some())
}

fn prepare_next_auto_create_attempt(
    store: &mut CredentialStore,
    family_key: &str,
    profile_name: &str,
    base_email: &str,
    suffix: u32,
    created_email: &str,
    started_at: &str,
) -> Result<()> {
    let normalized_email = normalize_email_key(created_email);
    if !store.pending.contains_key(&normalized_email) {
        return Ok(());
    }

    store.pending.remove(&normalized_email);
    let max_skipped_slots = max_skipped_slots_for_family(store.families.get(family_key)).max(1);
    let mut existing_family_skips =
        collect_skipped_account_emails_for_family(store, profile_name, base_email);
    existing_family_skips.retain(|email| normalize_email_key(email) != normalized_email);
    existing_family_skips.sort_by_key(|email| {
        extract_account_family_suffix(email, base_email)
            .ok()
            .flatten()
            .unwrap_or(0)
    });
    let existing_skip_count = existing_family_skips.len() as u32;
    if existing_skip_count >= max_skipped_slots {
        let to_remove =
            existing_skip_count.saturating_sub(max_skipped_slots.saturating_sub(1)) as usize;
        for email in existing_family_skips.into_iter().take(to_remove) {
            store.skipped.remove(&normalize_email_key(&email));
        }
    }
    store.skipped.insert(normalized_email);
    let updated_at = now_iso();
    let next_suffix = suffix.saturating_add(1);
    if let Some(family) = store.families.get_mut(family_key) {
        family.next_suffix = family.next_suffix.max(next_suffix);
        family.updated_at = updated_at;
    } else {
        store.families.insert(
            family_key.to_string(),
            CredentialFamily {
                profile_name: profile_name.to_string(),
                base_email: base_email.to_string(),
                next_suffix,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: started_at.to_string(),
                updated_at,
                last_created_email: None,
            },
        );
    }

    save_credential_store(store)
}

fn prefer_signup_recovery_for_create(_reusing_pending: bool) -> bool {
    true
}

fn execute_create_flow_attempt(
    options: &CreateCommandOptions,
    progress: Option<AutomationProgressCallback>,
) -> std::result::Result<CreateCommandResult, CreateFlowAttemptFailure> {
    let paths = fatal(resolve_paths())?;
    fatal(ensure_main_worktree_operation_allowed(
        &paths.repo_root,
        "Fresh account creation",
    ))?;
    let previous_auth = fatal(load_codex_auth_if_exists())?;
    let mut store = fatal(load_credential_store())?;
    let workflow_file = resolve_account_flow_file_for_create(&paths, options);
    let workflow_file_display = workflow_file.display().to_string();
    let workflow_metadata = fatal(read_workflow_file_metadata(&workflow_file))?;
    let login_workflow_defaults = fatal(resolve_login_workflow_defaults(None))?;
    let profile_name = fatal(resolve_managed_profile_name(
        options.profile_name.as_deref(),
        workflow_metadata.preferred_profile_name.as_deref(),
        Some(workflow_file_display.as_str()),
    ))?;
    let base_email = fatal(resolve_create_base_email_for_profile(
        &store,
        &profile_name,
        options.base_email.as_deref(),
        options.alias.as_deref(),
    ))?;

    let pool = fatal(load_pool())?;
    let family_key = fatal(make_credential_family_key(&profile_name, &base_email))?;
    let family = store.families.get(&family_key).cloned();
    let started_at = now_iso();
    let known_emails = collect_known_account_emails(&pool, &store);
    let skipped_emails =
        collect_skipped_account_emails_for_family(&store, &profile_name, &base_email);
    let existing_pending = select_pending_credential_for_family(
        &store,
        &profile_name,
        &base_email,
        options.alias.as_deref(),
    );
    let reusing_pending = existing_pending.is_some();
    let suffix = match existing_pending.as_ref() {
        Some(entry) => entry.stored.suffix,
        None => fatal(compute_fresh_account_family_suffix(
            family.as_ref(),
            &base_email,
            known_emails,
            skipped_emails,
        ))?,
    };
    let created_email = existing_pending
        .as_ref()
        .map(|entry| entry.stored.email.clone())
        .unwrap_or_else(|| build_account_family_email(&base_email, suffix).unwrap_or_default());
    let existing_pending = existing_pending.unwrap_or_else(|| PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            suffix,
            selector: None,
            alias: normalize_alias(options.alias.as_deref()),
            birth_month: None,
            birth_day: None,
            birth_year: None,
            created_at: started_at.clone(),
            updated_at: started_at.clone(),
        },
        started_at: Some(started_at.clone()),
    });
    let birth_date = resolve_credential_birth_date(
        Some(&existing_pending.stored),
        workflow_metadata.default_birth_date().as_ref(),
    )
    .unwrap_or_else(|| login_workflow_defaults.birth_date.clone());
    report_progress(
        progress.as_ref(),
        if reusing_pending {
            format!(
                "Reusing pending account {} via {}.",
                created_email, profile_name
            )
        } else {
            format!(
                "Creating {} via {} from {}.",
                created_email, profile_name, base_email
            )
        },
    );
    if previous_auth
        .as_ref()
        .map(|auth| auth_matches_target_email(auth, &created_email))
        .unwrap_or(false)
    {
        report_progress(
            progress.as_ref(),
            format!(
                "{} is already the active Codex auth. Finalizing.",
                created_email
            ),
        );
        let auth = previous_auth
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow!("Current Codex auth disappeared before create could finish."))
            .map_err(CreateFlowAttemptFailure::Fatal)?;
        let result = finalize_created_account(FinalizeCreatedAccountArgs {
            store: &mut store,
            family: family.as_ref(),
            family_key: &family_key,
            profile_name: &profile_name,
            base_email: &base_email,
            suffix,
            pending: &PendingCredential {
                stored: StoredCredential {
                    email: created_email.clone(),
                    profile_name: profile_name.clone(),
                    base_email: base_email.clone(),
                    suffix,
                    selector: existing_pending.stored.selector.clone(),
                    alias: existing_pending
                        .stored
                        .alias
                        .clone()
                        .or_else(|| normalize_alias(options.alias.as_deref())),
                    birth_month: Some(birth_date.birth_month),
                    birth_day: Some(birth_date.birth_day),
                    birth_year: Some(birth_date.birth_year),
                    created_at: existing_pending.stored.created_at.clone(),
                    updated_at: started_at.clone(),
                },
                started_at: existing_pending
                    .started_at
                    .clone()
                    .or_else(|| Some(started_at.clone())),
            },
            options,
            auth: &auth,
            started_at: started_at.as_str(),
            previous_auth: previous_auth.as_ref(),
            progress: progress.clone(),
        });
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                if should_retry_create_until_usable(options) {
                    fatal(restore_active_auth(previous_auth.as_ref()))?;
                    fatal(prepare_next_auto_create_attempt(
                        &mut store,
                        &family_key,
                        &profile_name,
                        &base_email,
                        suffix,
                        &created_email,
                        started_at.as_str(),
                    ))?;
                    return Err(CreateFlowAttemptFailure::Retryable(error));
                }
                return Err(CreateFlowAttemptFailure::Fatal(error));
            }
        };
        if options.restore_previous_auth_after_create {
            fatal(restore_active_auth(previous_auth.as_ref()))?;
        }
        return Ok(result);
    }
    let account_login_locator = build_openai_account_login_locator(&created_email);
    let mut skip_locator_preflight = false;
    if !reusing_pending {
        report_progress(
            progress.as_ref(),
            format!("Preparing password for {}.", created_email),
        );
        let generated_password = generate_password(18);
        let _: CodexRotateSecretRef = run_automation_bridge(
            "prepare-account-secret-ref",
            BridgeEnsureSecretPayload {
                profile_name: &profile_name,
                email: &created_email,
                password: generated_password.as_str(),
            },
        )
        .map_err(CreateFlowAttemptFailure::Fatal)?;
        skip_locator_preflight = true;
    }
    let pending = PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            suffix,
            selector: existing_pending.stored.selector.clone(),
            alias: existing_pending
                .stored
                .alias
                .clone()
                .or_else(|| normalize_alias(options.alias.as_deref())),
            birth_month: Some(birth_date.birth_month),
            birth_day: Some(birth_date.birth_day),
            birth_year: Some(birth_date.birth_year),
            created_at: existing_pending.stored.created_at.clone(),
            updated_at: started_at.clone(),
        },
        started_at: existing_pending
            .started_at
            .clone()
            .or_else(|| Some(started_at.clone())),
    };
    store
        .pending
        .insert(normalize_email_key(&created_email), pending.clone());
    fatal(save_credential_store(&store))?;

    report_progress(
        progress.as_ref(),
        format!("Starting managed login for {}.", created_email),
    );
    let login_result = run_complete_codex_login(CompleteCodexLoginArgs {
        profile_name: &profile_name,
        email: &created_email,
        account_login_locator: Some(&account_login_locator),
        workflow_ref: workflow_metadata.workflow_ref.as_deref(),
        codex_bin: Some(codex_bin().as_str()),
        workflow_run_stamp: Some(started_at.as_str()),
        skip_locator_preflight: Some(skip_locator_preflight),
        prefer_signup_recovery: Some(prefer_signup_recovery_for_create(reusing_pending)),
        prefer_password_login: skip_locator_preflight.then_some(true),
        birth_date: Some(&birth_date),
        progress: progress.clone(),
    });
    let login_outcome = match login_result {
        Ok(value) => value,
        Err(error) => {
            fatal(restore_active_auth(previous_auth.as_ref()))?;
            if should_retry_create_after_error(options, &error) {
                fatal(prepare_next_auto_create_attempt(
                    &mut store,
                    &family_key,
                    &profile_name,
                    &base_email,
                    suffix,
                    &created_email,
                    started_at.as_str(),
                ))?;
                return Err(CreateFlowAttemptFailure::Retryable(error));
            }
            fatal(save_credential_store(&store))?;
            return Err(CreateFlowAttemptFailure::Fatal(error));
        }
    };

    let auth = fatal(load_auth_for_completed_login(&login_outcome))?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&created_email)
        && !workflow_verified_expected_email(
            login_outcome.verified_account_email.as_deref(),
            &created_email,
        )
    {
        let error = anyhow!(
            "Expected {}, but Codex logged into {}.",
            created_email,
            logged_in_email
        );
        fatal(restore_active_auth(previous_auth.as_ref()))?;
        if should_retry_create_until_usable(options) {
            fatal(prepare_next_auto_create_attempt(
                &mut store,
                &family_key,
                &profile_name,
                &base_email,
                suffix,
                &created_email,
                started_at.as_str(),
            ))?;
            return Err(CreateFlowAttemptFailure::Retryable(error));
        }
        fatal(save_credential_store(&store))?;
        return Err(CreateFlowAttemptFailure::Fatal(error));
    }

    report_progress(
        progress.as_ref(),
        format!("Managed login finished for {}. Finalizing.", created_email),
    );
    let result = finalize_created_account(FinalizeCreatedAccountArgs {
        store: &mut store,
        family: family.as_ref(),
        family_key: &family_key,
        profile_name: &profile_name,
        base_email: &base_email,
        suffix,
        pending: &pending,
        options,
        auth: &auth,
        started_at: started_at.as_str(),
        previous_auth: previous_auth.as_ref(),
        progress: progress.clone(),
    });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            if should_retry_create_until_usable(options) {
                fatal(restore_active_auth(previous_auth.as_ref()))?;
                fatal(prepare_next_auto_create_attempt(
                    &mut store,
                    &family_key,
                    &profile_name,
                    &base_email,
                    suffix,
                    &created_email,
                    started_at.as_str(),
                ))?;
                return Err(CreateFlowAttemptFailure::Retryable(error));
            }
            return Err(CreateFlowAttemptFailure::Fatal(error));
        }
    };

    if options.restore_previous_auth_after_create {
        fatal(restore_active_auth(previous_auth.as_ref()))?;
    }

    Ok(result)
}

fn auth_matches_target_email(auth: &CodexAuth, target_email: &str) -> bool {
    normalize_email_key(&summarize_codex_auth(auth).email) == normalize_email_key(target_email)
}

struct FinalizeCreatedAccountArgs<'a> {
    store: &'a mut CredentialStore,
    family: Option<&'a CredentialFamily>,
    family_key: &'a str,
    profile_name: &'a str,
    base_email: &'a str,
    suffix: u32,
    pending: &'a PendingCredential,
    options: &'a CreateCommandOptions,
    auth: &'a CodexAuth,
    started_at: &'a str,
    previous_auth: Option<&'a CodexAuth>,
    progress: Option<AutomationProgressCallback>,
}

fn finalize_created_account(args: FinalizeCreatedAccountArgs<'_>) -> Result<CreateCommandResult> {
    let FinalizeCreatedAccountArgs {
        store,
        family,
        family_key,
        profile_name,
        base_email,
        suffix,
        pending,
        options,
        auth,
        started_at,
        previous_auth,
        progress,
    } = args;
    let created_email = pending.stored.email.clone();
    report_progress(
        progress.as_ref(),
        format!("Adding {} to the account pool.", created_email),
    );
    let _ = cmd_add_expected_email(&created_email, options.alias.as_deref())?;
    report_progress(
        progress.as_ref(),
        format!("Inspecting quota for {}.", created_email),
    );
    let inspected =
        inspect_pool_entry_for_created_email(&extract_account_id_from_auth(auth), &created_email)?
            .ok_or_else(|| {
                anyhow!(
                    "Created {}, but could not find the new account in the pool after login.",
                    created_email
                )
            })?;

    let updated_at = now_iso();
    store.pending.remove(&normalize_email_key(&created_email));
    store.skipped.remove(&normalize_email_key(&created_email));
    upsert_family_for_account(
        store,
        &StoredCredential {
            email: created_email.clone(),
            profile_name: profile_name.to_string(),
            base_email: base_email.to_string(),
            suffix,
            selector: Some(inspected.entry.label.clone()),
            alias: inspected
                .entry
                .alias
                .clone()
                .or_else(|| normalize_alias(options.alias.as_deref())),
            birth_month: pending.stored.birth_month,
            birth_day: pending.stored.birth_day,
            birth_year: pending.stored.birth_year,
            created_at: pending.stored.created_at.clone(),
            updated_at: updated_at.clone(),
        },
    );
    store.families.insert(
        family_key.to_string(),
        CredentialFamily {
            profile_name: profile_name.to_string(),
            base_email: base_email.to_string(),
            next_suffix: family
                .map(|entry| entry.next_suffix.max(suffix + 1))
                .unwrap_or(suffix + 1),
            max_skipped_slots: family
                .map(|entry| entry.max_skipped_slots)
                .unwrap_or(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY),
            created_at: family
                .map(|entry| entry.created_at.clone())
                .unwrap_or_else(|| started_at.to_string()),
            updated_at,
            last_created_email: Some(created_email.clone()),
        },
    );
    save_credential_store(store)?;

    if options.require_usable_quota {
        match inspected.inspection.usage.as_ref() {
            Some(usage) if has_usable_quota(usage) => {}
            Some(usage) => {
                restore_active_auth(previous_auth)?;
                return Err(anyhow!(
                    "Created {}, but it does not have usable quota ({}).",
                    inspected.entry.label,
                    describe_quota_blocker(usage)
                ));
            }
            None => {
                restore_active_auth(previous_auth)?;
                return Err(anyhow!(
                    "Created {}, but quota inspection was unavailable ({}).",
                    inspected.entry.label,
                    inspected
                        .inspection
                        .error
                        .clone()
                        .unwrap_or_else(|| "unknown error".to_string())
                ));
            }
        }
    }

    report_progress(
        progress.as_ref(),
        format!("Created {} with usable quota.", inspected.entry.label),
    );

    Ok(CreateCommandResult {
        entry: inspected.entry,
        inspection: Some(inspected.inspection),
        profile_name: profile_name.to_string(),
        base_email: base_email.to_string(),
    })
}

fn summarize_quota_for_create(result: &CreateCommandResult) -> String {
    match result.inspection.as_ref() {
        Some(inspection) => match inspection.usage.as_ref() {
            Some(usage) => format_compact_quota(usage),
            None => format!(
                "quota unavailable ({})",
                inspection
                    .error
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string())
            ),
        },
        None => "quota unavailable".to_string(),
    }
}

fn find_created_pool_entry_index(
    pool: &Pool,
    account_id: &str,
    expected_email: &str,
) -> Option<usize> {
    let normalized_expected_email = normalize_email_key(expected_email);
    if !normalized_expected_email.is_empty() && normalized_expected_email != "unknown" {
        if let Some(index) = pool.accounts.iter().position(|entry| {
            normalize_email_key(entry.email.as_str()) == normalized_expected_email
        }) {
            return Some(index);
        }
    }

    pool.accounts.iter().position(|entry| {
        entry.account_id == account_id || entry.auth.tokens.account_id == account_id
    })
}

fn inspect_pool_entry_for_created_email(
    account_id: &str,
    expected_email: &str,
) -> Result<Option<InspectedPoolEntry>> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let index = find_created_pool_entry_index(&pool, account_id, expected_email);
    let Some(index) = index else {
        return Ok(None);
    };
    let inspection = inspect_account(
        &mut pool.accounts[index],
        &paths.codex_auth_file,
        index == pool.active_index,
    )?;
    if inspection.updated {
        save_pool(&pool)?;
    }
    Ok(Some(InspectedPoolEntry {
        entry: pool.accounts[index].clone(),
        inspection,
    }))
}

fn inspect_pool_entry_by_account_id(account_id: &str) -> Result<Option<InspectedPoolEntry>> {
    inspect_pool_entry_for_created_email(account_id, "")
}

struct InspectedPoolEntry {
    entry: AccountEntry,
    inspection: AccountInspection,
}

fn load_current_auth() -> Result<CodexAuth> {
    let paths = resolve_paths()?;
    load_codex_auth(&paths.codex_auth_file)
}

fn load_auth_for_completed_login(outcome: &CompleteCodexLoginOutcome) -> Result<CodexAuth> {
    if let Some(auth_file_path) = outcome
        .codex_session
        .as_ref()
        .and_then(|session| session.auth_file_path.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let auth_path = Path::new(auth_file_path);
        if auth_path.exists() {
            return load_codex_auth(auth_path);
        }
    }
    load_current_auth()
}

fn load_codex_auth_if_exists() -> Result<Option<CodexAuth>> {
    let paths = resolve_paths()?;
    if !paths.codex_auth_file.exists() {
        return Ok(None);
    }
    Ok(Some(load_codex_auth(&paths.codex_auth_file)?))
}

fn restore_active_auth(previous_auth: Option<&CodexAuth>) -> Result<()> {
    let paths = resolve_paths()?;
    match previous_auth {
        Some(previous_auth) => {
            write_codex_auth(&paths.codex_auth_file, previous_auth)?;
            let mut pool = load_pool()?;
            if let Some(index) = pool.accounts.iter().position(|entry| {
                entry.account_id == extract_account_id_from_auth(previous_auth)
                    || entry.auth.tokens.account_id == previous_auth.tokens.account_id
            }) {
                pool.active_index = index;
                save_pool(&pool)?;
            }
        }
        None => {
            if paths.codex_auth_file.exists() {
                fs::remove_file(&paths.codex_auth_file).with_context(|| {
                    format!("Failed to remove {}.", paths.codex_auth_file.display())
                })?;
            }
        }
    }
    Ok(())
}

struct CompleteCodexLoginArgs<'a> {
    profile_name: &'a str,
    email: &'a str,
    account_login_locator: Option<&'a CodexRotateSecretLocator>,
    workflow_ref: Option<&'a str>,
    codex_bin: Option<&'a str>,
    workflow_run_stamp: Option<&'a str>,
    skip_locator_preflight: Option<bool>,
    prefer_signup_recovery: Option<bool>,
    prefer_password_login: Option<bool>,
    birth_date: Option<&'a AdultBirthDate>,
    progress: Option<AutomationProgressCallback>,
}

#[derive(Clone, Debug, Default)]
struct CompleteCodexLoginOutcome {
    verified_account_email: Option<String>,
    codex_session: Option<CodexRotateAuthFlowSession>,
}

fn run_complete_codex_login(args: CompleteCodexLoginArgs<'_>) -> Result<CompleteCodexLoginOutcome> {
    let CompleteCodexLoginArgs {
        profile_name,
        email,
        account_login_locator,
        workflow_ref,
        codex_bin,
        workflow_run_stamp,
        skip_locator_preflight,
        prefer_signup_recovery,
        prefer_password_login,
        birth_date,
        progress,
    } = args;
    let workflow_defaults = resolve_login_workflow_defaults(workflow_ref)?;
    let fallback_birth_date;
    let birth_date = match birth_date {
        Some(value) => value,
        None => {
            fallback_birth_date = workflow_defaults.birth_date.clone();
            &fallback_birth_date
        }
    };
    let workflow_ref = workflow_defaults.workflow_ref;
    let wrapped_codex_bin =
        ensure_managed_browser_wrapper(profile_name, codex_bin.unwrap_or(DEFAULT_CODEX_BIN))?;
    let wrapped_codex_bin = wrapped_codex_bin.to_string_lossy().into_owned();
    match account_login_locator {
        Some(_) if skip_locator_preflight == Some(true) => report_progress(
            progress.as_ref(),
            format!(
                "Using a freshly generated OpenAI password for {email}; attempting password login first."
            ),
        ),
        Some(_) => report_progress(
            progress.as_ref(),
            format!("Found a stored OpenAI login secret for {email}; attempting password login first."),
        ),
        None => report_progress(
            progress.as_ref(),
            format!("No stored OpenAI login secret was found for {email}; using one-time-code recovery."),
        ),
    }

    let mut allow_signup_recovery = prefer_signup_recovery.unwrap_or(false);
    let mut codex_session: Option<CodexRotateAuthFlowSession> = None;
    let result = (|| -> Result<CompleteCodexLoginOutcome> {
        let mut max_attempts = DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS;
        let mut attempt = 1usize;
        'attempts: while attempt <= max_attempts {
            cancel::check_canceled()?;
            report_progress(
                progress.as_ref(),
                if attempt == 1 {
                    format!("Completing Codex login in managed profile \"{profile_name}\".")
                } else {
                    format!(
                        "Retrying Codex login in managed profile \"{profile_name}\" (attempt {attempt}/{max_attempts})."
                    )
                },
            );

            for replay_pass in 1..=DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES {
                cancel::check_canceled()?;
                let login_workflow_run_stamp = workflow_run_stamp
                    .map(|stamp| format!("{stamp}-codex-login-{attempt}-{replay_pass}"));
                let options = BridgeLoginOptions {
                    codex_bin: Some(wrapped_codex_bin.as_str()),
                    workflow_ref: Some(workflow_ref.as_str()),
                    workflow_run_stamp: login_workflow_run_stamp.as_deref(),
                    skip_locator_preflight,
                    prefer_signup_recovery: Some(allow_signup_recovery),
                    prefer_password_login,
                    full_name: Some(workflow_defaults.full_name.as_str()),
                    birth_month: Some(birth_date.birth_month),
                    birth_day: Some(birth_date.birth_day),
                    birth_year: Some(birth_date.birth_year),
                    codex_session: codex_session.as_ref(),
                };
                let attempt_result_raw: Value = match run_automation_bridge_with_progress(
                    "complete-codex-login-attempt",
                    BridgeCompleteLoginAttemptPayload {
                        profile_name,
                        email,
                        account_login_locator,
                        options: Some(options),
                    },
                    progress.clone(),
                ) {
                    Ok(result) => result,
                    Err(error) => return Err(error),
                };
                maybe_debug_codex_auth_flow_raw(workflow_ref.as_str(), email, &attempt_result_raw);
                let attempt_result = normalize_bridge_login_attempt_result(attempt_result_raw);
                let bridge_error_message = attempt_result.error_message.clone();
                let flow = attempt_result
                    .result
                    .as_ref()
                    .map(read_codex_rotate_auth_flow_summary)
                    .unwrap_or_default();
                maybe_debug_codex_auth_flow_result(
                    workflow_ref.as_str(),
                    email,
                    &attempt_result,
                    &flow,
                );
                if let Some(session) = attempt_result
                    .result
                    .as_ref()
                    .and_then(read_codex_rotate_auth_flow_session)
                    .or_else(|| flow.codex_session.clone())
                {
                    codex_session = Some(session);
                }
                let current_url = flow
                    .current_url
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let next_action = flow
                    .next_action
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let replay_reason = flow
                    .replay_reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let retry_reason = flow
                    .retry_reason
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                let error_message = flow
                    .error_message
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        bridge_error_message
                            .as_deref()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                    });

                if flow.saw_oauth_consent == Some(true)
                    || flow.existing_account_prompt == Some(true)
                    || replay_reason.is_some_and(|value| value != "auth_prompt")
                {
                    allow_signup_recovery = false;
                }

                if next_action == Some("fail_invalid_credentials") {
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!("OpenAI rejected the stored password for {email}.")
                    )));
                }

                if next_action == Some("skip_account") {
                    return Err(anyhow::Error::new(WorkflowSkipAccountError::new(
                        login_error_message(
                            error_message,
                            format!(
                                "The workflow requested skipping {email}{}.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default()
                            ),
                        ),
                    )));
                }

                if next_action == Some("replay_auth_url")
                    && replay_pass < DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES
                {
                    let replay_reason_label =
                        format_retry_reason_label(replay_reason, "the next auth step");
                    report_progress(
                        progress.as_ref(),
                        format!(
                            "OpenAI still needs {replay_reason_label} for {email}{}. Replaying the workflow-owned Codex auth session in managed profile \"{profile_name}\" ({}/{}).",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default(),
                            replay_pass + 1,
                            DEFAULT_CODEX_LOGIN_MAX_REPLAY_PASSES
                        ),
                    );
                    cancel::sleep_with_cancellation(Duration::from_millis(1_000))?;
                    continue;
                }

                if next_action == Some("retry_attempt") {
                    max_attempts = max_attempts.max(codex_login_max_attempts(retry_reason));
                    if attempt < max_attempts {
                        let delay_ms = codex_login_retry_delay_ms(retry_reason, attempt);
                        let reset_session =
                            should_reset_codex_login_session_for_retry(retry_reason, attempt);
                        if reset_session {
                            codex_session = None;
                        }
                        let retry_reason_label =
                            format_retry_reason_label(retry_reason, "needs another retry");
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI {retry_reason_label} for {email}{}. {}Waiting {}s before retrying.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default(),
                                if reset_session {
                                    "Starting a fresh Codex auth session. "
                                } else {
                                    ""
                                },
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    if should_skip_account_after_retry_exhaustion(retry_reason) {
                        return Err(anyhow::Error::new(WorkflowSkipAccountError::new(
                            login_error_message(
                                error_message,
                                format!(
                                    "The workflow requested skipping {email} after exhausting final add-phone retries{}.",
                                    current_url
                                        .map(|value| format!(" ({value})"))
                                        .unwrap_or_default()
                                ),
                            ),
                        )));
                    }
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!("OpenAI could not complete the Codex login for {email}.")
                    )));
                }

                if state_mismatch_in_login_flow(&flow, error_message) {
                    if attempt < max_attempts {
                        let delay_ms = codex_login_retry_delay_ms(Some("state_mismatch"), attempt);
                        codex_session = None;
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI returned a state mismatch during the Codex callback for {email}{}. Starting a fresh Codex auth session and retrying in {}s.",
                                current_url
                                    .map(|value| format!(" ({value})"))
                                    .unwrap_or_default(),
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!(
                            "OpenAI returned a state mismatch during the Codex callback for {email}{}.",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default()
                        )
                    )));
                }

                if let Some(message) = error_message {
                    if is_retryable_codex_login_workflow_error_message(message)
                        && attempt < max_attempts
                    {
                        let delay_ms = codex_login_retry_delay_ms(
                            Some("verification_artifact_pending"),
                            attempt,
                        );
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "OpenAI verification is not ready for {email}. Waiting {}s before retrying the same managed-profile flow.",
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                    if is_device_auth_rate_limited(message) && attempt < max_attempts {
                        let delay_ms =
                            codex_login_retry_delay_ms(Some("device_auth_rate_limit"), attempt);
                        let reset_session = should_reset_device_auth_session_for_rate_limit(
                            message,
                            codex_session.as_ref(),
                        );
                        if reset_session {
                            codex_session = None;
                        }
                        report_progress(
                            progress.as_ref(),
                            format!(
                                "Codex device authorization is rate limited for {email}. {}Waiting {}s before retrying.",
                                if reset_session {
                                    ""
                                } else {
                                    "Reusing the existing device code session when retrying. "
                                },
                                delay_ms / 1_000
                            ),
                        );
                        cancel::sleep_with_cancellation(Duration::from_millis(delay_ms))?;
                        attempt += 1;
                        continue 'attempts;
                    }
                }

                if flow.callback_complete != Some(true) && flow.success != Some(true) {
                    return Err(anyhow!(login_error_message(
                        error_message,
                        format!(
                            "Codex browser login did not reach the callback for {email}{}.",
                            current_url
                                .map(|value| format!(" ({value})"))
                                .unwrap_or_default()
                        )
                    )));
                }
                if flow.codex_login_exit_ok == Some(false) {
                    let detail = flow
                        .codex_login_stderr_tail
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .unwrap_or("");
                    return Err(anyhow!(
                        "\"codex login\" did not exit cleanly for {email}.{}",
                        if detail.is_empty() {
                            String::new()
                        } else {
                            format!("\n{detail}")
                        }
                    ));
                }
                promote_codex_auth_from_session(
                    codex_session.as_ref().or(flow.codex_session.as_ref()),
                )?;
                return Ok(CompleteCodexLoginOutcome {
                    verified_account_email: flow.verified_account_email.clone(),
                    codex_session: codex_session.clone().or(flow.codex_session.clone()),
                });
            }
            attempt += 1;
        }
        Err(anyhow!(
            "Codex browser login exhausted all retry attempts for {email}."
        ))
    })();
    cancel_codex_browser_login_session(codex_session.as_ref());
    result
}

fn resolve_login_workflow_defaults(workflow_ref: Option<&str>) -> Result<LoginWorkflowDefaults> {
    let paths = resolve_paths()?;
    let workflow_metadata = read_workflow_file_metadata(&paths.account_flow_file)?;
    let workflow_ref = workflow_ref
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| workflow_metadata.workflow_ref.clone())
        .ok_or_else(|| {
            anyhow!(
                "Could not resolve a codex-rotate workflow ref from {}.",
                paths.account_flow_file.display()
            )
        })?;
    let full_name = workflow_metadata.default_full_name.clone().ok_or_else(|| {
        anyhow!(
            "Workflow {} is missing input.schema.document.properties.full_name.default.",
            paths.account_flow_file.display()
        )
    })?;
    let birth_date = workflow_metadata.default_birth_date().ok_or_else(|| {
        anyhow!(
            "Workflow {} is missing one or more birth-date defaults.",
            paths.account_flow_file.display()
        )
    })?;

    Ok(LoginWorkflowDefaults {
        workflow_ref,
        full_name,
        birth_date,
    })
}

fn read_string_value(record: &Map<String, Value>, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn read_bool_value(record: &Map<String, Value>, field: &str) -> Option<bool> {
    match record.get(field) {
        Some(Value::Bool(value)) => Some(*value),
        Some(Value::String(value)) => match value.trim() {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn read_i32_value(record: &Map<String, Value>, field: &str) -> Option<i32> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_i64().and_then(|value| i32::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<i32>().ok(),
        _ => None,
    }
}

fn read_u16_value(record: &Map<String, Value>, field: &str) -> Option<u16> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_u64().and_then(|value| u16::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<u16>().ok(),
        _ => None,
    }
}

fn read_u32_value(record: &Map<String, Value>, field: &str) -> Option<u32> {
    match record.get(field) {
        Some(Value::Number(value)) => value.as_u64().and_then(|value| u32::try_from(value).ok()),
        Some(Value::String(value)) => value.trim().parse::<u32>().ok(),
        _ => None,
    }
}

fn merge_codex_rotate_auth_flow_session(
    primary: CodexRotateAuthFlowSession,
    fallback: CodexRotateAuthFlowSession,
) -> CodexRotateAuthFlowSession {
    CodexRotateAuthFlowSession {
        auth_url: primary.auth_url.or(fallback.auth_url),
        callback_url: primary.callback_url.or(fallback.callback_url),
        callback_port: primary.callback_port.or(fallback.callback_port),
        device_code: primary.device_code.or(fallback.device_code),
        session_dir: primary.session_dir.or(fallback.session_dir),
        codex_home_path: primary.codex_home_path.or(fallback.codex_home_path),
        auth_file_path: primary.auth_file_path.or(fallback.auth_file_path),
        pid: primary.pid.or(fallback.pid),
        stdout_path: primary.stdout_path.or(fallback.stdout_path),
        stderr_path: primary.stderr_path.or(fallback.stderr_path),
        exit_path: primary.exit_path.or(fallback.exit_path),
    }
}

fn read_codex_rotate_auth_flow_summary(
    result: &FastBrowserRunResult,
) -> CodexRotateAuthFlowSummary {
    let mut summary = if let Some(record) = read_codex_rotate_auth_flow_summary_record(result) {
        CodexRotateAuthFlowSummary {
            stage: read_string_value(record, "stage"),
            current_url: read_string_value(record, "current_url"),
            headline: read_string_value(record, "headline"),
            callback_complete: read_bool_value(record, "callback_complete"),
            success: read_bool_value(record, "success"),
            account_ready: read_bool_value(record, "account_ready"),
            needs_email_verification: read_bool_value(record, "needs_email_verification"),
            follow_up_step: read_bool_value(record, "follow_up_step"),
            retryable_timeout: read_bool_value(record, "retryable_timeout"),
            session_ended: read_bool_value(record, "session_ended"),
            existing_account_prompt: read_bool_value(record, "existing_account_prompt"),
            username_not_found: read_bool_value(record, "username_not_found"),
            invalid_credentials: read_bool_value(record, "invalid_credentials"),
            rate_limit_exceeded: read_bool_value(record, "rate_limit_exceeded"),
            anti_bot_gate: read_bool_value(record, "anti_bot_gate"),
            auth_prompt: read_bool_value(record, "auth_prompt"),
            consent_blocked: read_bool_value(record, "consent_blocked"),
            consent_error: read_string_value(record, "consent_error"),
            next_action: read_string_value(record, "next_action"),
            replay_reason: read_string_value(record, "replay_reason"),
            retry_reason: read_string_value(record, "retry_reason"),
            error_message: read_string_value(record, "error_message"),
            verified_account_email: read_string_value(record, "verified_account_email"),
            codex_session: record
                .get("codex_session")
                .and_then(normalize_codex_rotate_auth_flow_session),
            codex_login_exit_ok: read_bool_value(record, "codex_login_exit_ok"),
            codex_login_exit_code: read_i32_value(record, "codex_login_exit_code"),
            codex_login_stdout_tail: read_string_value(record, "codex_login_stdout_tail"),
            codex_login_stderr_tail: read_string_value(record, "codex_login_stderr_tail"),
            saw_oauth_consent: read_bool_value(record, "saw_oauth_consent"),
        }
    } else {
        CodexRotateAuthFlowSummary::default()
    };

    if let Some(metadata) = read_codex_rotate_auth_flow_summary_from_result_metadata(result) {
        if metadata.success == Some(true) || metadata.callback_complete == Some(true) {
            summary.stage = metadata.stage.or(summary.stage);
            summary.current_url = metadata.current_url.or(summary.current_url);
            summary.headline = metadata.headline.or(summary.headline);
            summary.callback_complete = metadata.callback_complete.or(summary.callback_complete);
            summary.success = metadata.success.or(summary.success);
            summary.next_action = metadata.next_action.or(summary.next_action);
        } else {
            summary.stage = summary.stage.or(metadata.stage);
            summary.current_url = summary.current_url.or(metadata.current_url);
            summary.headline = summary.headline.or(metadata.headline);
            summary.callback_complete = summary.callback_complete.or(metadata.callback_complete);
            summary.success = summary.success.or(metadata.success);
            summary.next_action = summary.next_action.or(metadata.next_action);
        }
    }

    summary
}

fn read_codex_rotate_auth_flow_summary_from_result_metadata(
    result: &FastBrowserRunResult,
) -> Option<CodexRotateAuthFlowSummary> {
    let step_metadata = read_codex_rotate_auth_flow_step_metadata(result);
    let current_url = result
        .final_url
        .clone()
        .or_else(|| {
            result
                .page
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| read_string_value(record, "url"))
        })
        .or_else(|| {
            result
                .current
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| {
                    record
                        .get("details")
                        .and_then(Value::as_object)
                        .and_then(|details| read_string_value(details, "current_url"))
                })
        })
        .or_else(|| step_metadata.current_url.clone());
    let headline = result
        .current
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|record| {
            record
                .get("details")
                .and_then(Value::as_object)
                .and_then(|details| read_string_value(details, "headline"))
        })
        .or_else(|| {
            result
                .page
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|record| {
                    read_string_value(record, "title").or_else(|| {
                        read_string_value(record, "text").and_then(|text| {
                            text.lines()
                                .map(str::trim)
                                .find(|line| !line.is_empty())
                                .map(str::to_string)
                        })
                    })
                })
        })
        .or_else(|| step_metadata.headline.clone());
    let page_text = result
        .page
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "text"))
        .or_else(|| step_metadata.page_text.clone())
        .unwrap_or_default()
        .to_lowercase();
    let headline_text = headline
        .as_deref()
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    let callback_url = current_url
        .as_deref()
        .map(str::to_lowercase)
        .unwrap_or_default();
    let localhost_callback = callback_url.starts_with("http://localhost")
        || callback_url.starts_with("http://127.0.0.1");
    let device_auth_callback = callback_url.contains("auth.openai.com/deviceauth/callback")
        && (headline_text.contains("signed in to codex")
            || headline_text.contains("you may now close this page")
            || page_text.contains("signed in to codex")
            || page_text.contains("you may now close this page"));
    let success_copy = headline_text.contains("signed in to codex")
        || headline_text.contains("you may now close this page")
        || page_text.contains("signed in to codex")
        || page_text.contains("you may now close this page");
    if current_url.is_none() && headline.is_none() {
        return None;
    }
    Some(CodexRotateAuthFlowSummary {
        stage: if localhost_callback || device_auth_callback || success_copy {
            Some("success".to_string())
        } else {
            None
        },
        current_url,
        headline,
        callback_complete: (localhost_callback || device_auth_callback || success_copy)
            .then_some(true),
        success: (localhost_callback || device_auth_callback || success_copy).then_some(true),
        next_action: (localhost_callback || device_auth_callback || success_copy)
            .then_some("complete".to_string()),
        ..CodexRotateAuthFlowSummary::default()
    })
}

#[derive(Clone, Debug, Default)]
struct FastBrowserStepMetadata {
    current_url: Option<String>,
    headline: Option<String>,
    page_text: Option<String>,
}

fn read_codex_rotate_auth_flow_step_metadata(
    result: &FastBrowserRunResult,
) -> FastBrowserStepMetadata {
    let mut metadata = FastBrowserStepMetadata::default();
    let Some(state) = result.state.as_ref() else {
        return metadata;
    };

    for step in state.steps.values() {
        let Some(action) = step.action.as_ref().and_then(Value::as_object) else {
            continue;
        };
        let url = read_string_value(action, "url")
            .or_else(|| read_string_value(action, "current_url"))
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "current_url"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "current_url"))
            });
        let headline = read_string_value(action, "headline")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "headline"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "headline"))
            });
        let page_text = read_string_value(action, "text")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "text"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_string_value(record, "text"))
            });
        let success = read_bool_value(action, "success")
            .or_else(|| {
                action
                    .get("value")
                    .and_then(Value::as_object)
                    .and_then(|record| read_bool_value(record, "success"))
            })
            .or_else(|| {
                action
                    .get("details")
                    .and_then(Value::as_object)
                    .and_then(|record| read_bool_value(record, "success"))
            });

        let url_text = url.as_deref().map(str::to_lowercase).unwrap_or_default();
        let headline_text = headline
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_default();
        let page_text_lc = page_text
            .as_deref()
            .map(str::to_lowercase)
            .unwrap_or_default();
        let looks_like_callback_success = url_text.contains("auth.openai.com/deviceauth/callback")
            && (success == Some(true)
                || headline_text.contains("signed in to codex")
                || headline_text.contains("you may now close this page")
                || page_text_lc.contains("signed in to codex")
                || page_text_lc.contains("you may now close this page"));

        if looks_like_callback_success {
            return FastBrowserStepMetadata {
                current_url: url,
                headline,
                page_text,
            };
        }

        metadata.current_url = metadata.current_url.or(url);
        metadata.headline = metadata.headline.or(headline);
        metadata.page_text = metadata.page_text.or(page_text);
    }

    metadata
}

fn read_codex_rotate_auth_flow_summary_record(
    result: &FastBrowserRunResult,
) -> Option<&Map<String, Value>> {
    result
        .output
        .as_ref()
        .and_then(Value::as_object)
        .or_else(|| {
            const FINAL_SUMMARY_STEP_IDS: [&str; 3] = [
                "finalize_selected_flow",
                "finalize_flow_summary",
                "finalize_device_auth_tail_summary",
            ];

            let state = result.state.as_ref()?;
            FINAL_SUMMARY_STEP_IDS.iter().find_map(|step_id| {
                state
                    .steps
                    .get(*step_id)
                    .and_then(|step| step.action.as_ref())
                    .and_then(read_codex_rotate_auth_flow_summary_action_record)
            })
        })
        .or_else(|| read_codex_rotate_auth_flow_summary_record_from_recent_events(result))
}

fn read_codex_rotate_auth_flow_summary_action_record(
    action: &Value,
) -> Option<&Map<String, Value>> {
    let record = action.as_object()?;
    record
        .get("value")
        .and_then(Value::as_object)
        .filter(|value| looks_like_codex_rotate_auth_flow_summary_record(value))
        .or_else(|| looks_like_codex_rotate_auth_flow_summary_record(record).then_some(record))
}

fn looks_like_codex_rotate_auth_flow_summary_record(record: &Map<String, Value>) -> bool {
    record.contains_key("callback_complete")
        || record.contains_key("success")
        || record.contains_key("next_action")
        || record.contains_key("retry_reason")
        || record.contains_key("replay_reason")
        || record.contains_key("error_message")
        || record.contains_key("verified_account_email")
        || record.contains_key("codex_session")
}

fn read_codex_rotate_auth_flow_summary_record_from_recent_events(
    result: &FastBrowserRunResult,
) -> Option<&Map<String, Value>> {
    const FINAL_SUMMARY_STEP_IDS: [&str; 3] = [
        "finalize_selected_flow",
        "finalize_flow_summary",
        "finalize_device_auth_tail_summary",
    ];

    result
        .recent_events
        .as_ref()?
        .iter()
        .rev()
        .find_map(|event| {
            let record = event.as_object()?;
            let step_id = read_string_value(record, "step_id")
                .or_else(|| read_string_value(record, "stepId"))?;
            if !FINAL_SUMMARY_STEP_IDS.contains(&step_id.as_str()) {
                return None;
            }
            let phase = read_string_value(record, "phase");
            if phase.as_deref() != Some("action") {
                return None;
            }
            let status = read_string_value(record, "status");
            if status.as_deref() != Some("ok") {
                return None;
            }

            record
                .get("details")
                .and_then(Value::as_object)
                .and_then(|details| {
                    details
                        .get("result")
                        .and_then(Value::as_object)
                        .and_then(|result| result.get("value"))
                        .and_then(Value::as_object)
                        .filter(|value| looks_like_codex_rotate_auth_flow_summary_record(value))
                        .or_else(|| {
                            details
                                .get("value")
                                .and_then(Value::as_object)
                                .filter(|value| {
                                    looks_like_codex_rotate_auth_flow_summary_record(value)
                                })
                        })
                        .or_else(|| {
                            details
                                .get("result")
                                .and_then(Value::as_object)
                                .filter(|value| {
                                    looks_like_codex_rotate_auth_flow_summary_record(value)
                                })
                        })
                })
        })
}

fn read_codex_rotate_auth_flow_session(
    result: &FastBrowserRunResult,
) -> Option<CodexRotateAuthFlowSession> {
    let summary = read_codex_rotate_auth_flow_summary(result);
    let action = result
        .state
        .as_ref()
        .and_then(|state| state.steps.get("start_codex_login_session"))
        .and_then(|step| step.action.as_ref())
        .and_then(Value::as_object);
    let action_session = action.and_then(|action| {
        action
            .get("value")
            .and_then(normalize_codex_rotate_auth_flow_session)
            .or_else(|| normalize_codex_rotate_auth_flow_session(&Value::Object(action.clone())))
    });

    match (summary.codex_session, action_session) {
        (Some(primary), Some(fallback)) => {
            Some(merge_codex_rotate_auth_flow_session(primary, fallback))
        }
        (Some(session), None) | (None, Some(session)) => Some(session),
        (None, None) => None,
    }
}

fn login_error_message(error_message: Option<&str>, fallback: String) -> String {
    error_message.map(str::to_string).unwrap_or(fallback)
}

fn maybe_debug_codex_auth_flow_result(
    workflow_ref: &str,
    email: &str,
    attempt_result: &BridgeLoginAttemptResult,
    flow: &CodexRotateAuthFlowSummary,
) {
    if std::env::var("CODEX_ROTATE_DEBUG_AUTH_FLOW_RESULT").as_deref() != Ok("1") {
        return;
    }

    let result = attempt_result.result.as_ref();
    let final_url = result
        .and_then(|value| value.final_url.as_deref())
        .unwrap_or("");
    let page_url = result
        .and_then(|value| value.page.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "url"))
        .unwrap_or_default();
    let page_title = result
        .and_then(|value| value.page.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "title"))
        .unwrap_or_default();
    let current_url = result
        .and_then(|value| value.current.as_ref())
        .and_then(Value::as_object)
        .and_then(|record| record.get("details"))
        .and_then(Value::as_object)
        .and_then(|record| read_string_value(record, "current_url"))
        .unwrap_or_default();
    let run_path = result
        .and_then(|value| value.observability.as_ref())
        .and_then(|value| value.run_path.as_deref())
        .unwrap_or("");
    let status_path = result
        .and_then(|value| value.observability.as_ref())
        .and_then(|value| value.status_path.as_deref())
        .unwrap_or("");
    eprintln!(
        "[codex-rotate-rust] auth-flow debug workflow={workflow_ref} email={email} final_url={final_url:?} page_url={page_url:?} page_title={page_title:?} current_url={current_url:?} run_path={run_path:?} status_path={status_path:?} callback_complete={:?} success={:?} next_action={:?} error_message={:?} has_output={} has_state={}",
        flow.callback_complete,
        flow.success,
        flow.next_action,
        flow.error_message,
        result.and_then(|value| value.output.as_ref()).is_some(),
        result.and_then(|value| value.state.as_ref()).is_some(),
    );
}

fn maybe_debug_codex_auth_flow_raw(workflow_ref: &str, email: &str, raw: &Value) {
    if std::env::var("CODEX_ROTATE_DEBUG_AUTH_FLOW_RESULT").as_deref() != Ok("1") {
        return;
    }

    let raw_json = serde_json::to_string(raw).unwrap_or_else(|_| "<serialize-failed>".to_string());
    let preview = if raw_json.len() > 4000 {
        format!("{}...", &raw_json[..4000])
    } else {
        raw_json
    };
    eprintln!(
        "[codex-rotate-rust] auth-flow raw workflow={workflow_ref} email={email} payload={preview}"
    );
}

fn workflow_verified_expected_email(
    verified_account_email: Option<&str>,
    expected_email: &str,
) -> bool {
    verified_account_email
        .map(normalize_email_key)
        .is_some_and(|value| value == normalize_email_key(expected_email))
}

fn is_workflow_skip_account_error(error: &anyhow::Error) -> bool {
    error.downcast_ref::<WorkflowSkipAccountError>().is_some()
}

fn is_retryable_codex_login_workflow_error_message(message: &str) -> bool {
    let normalized = message.trim().to_lowercase();
    !normalized.is_empty()
        && (normalized.contains("signup-verification-code-missing")
            || normalized.contains("login-verification-code-missing")
            || normalized.contains("signup-verification-submit-stuck:email_verification")
            || normalized.contains("login-verification-submit-stuck:email_verification"))
}

fn codex_login_retry_delays_ms(reason: Option<&str>) -> &'static [u64] {
    match reason {
        Some("verification_artifact_pending") => DEFAULT_CODEX_LOGIN_VERIFICATION_RETRY_DELAYS_MS,
        Some("retryable_timeout") => DEFAULT_CODEX_LOGIN_RETRYABLE_TIMEOUT_DELAYS_MS,
        Some("device_auth_rate_limit") | Some("rate_limit") => {
            DEFAULT_CODEX_LOGIN_RATE_LIMIT_RETRY_DELAYS_MS
        }
        _ => DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS,
    }
}

fn codex_login_retry_delay_ms(reason: Option<&str>, attempt: usize) -> u64 {
    let delays = codex_login_retry_delays_ms(reason);
    let index = attempt
        .saturating_sub(1)
        .min(delays.len().saturating_sub(1));
    delays
        .get(index)
        .copied()
        .unwrap_or(DEFAULT_CODEX_LOGIN_RETRY_DELAYS_MS[0])
}

fn should_reset_codex_login_session_for_retry(retry_reason: Option<&str>, attempt: usize) -> bool {
    retry_reason == Some("state_mismatch")
        || retry_reason == Some("final_add_phone")
        || (retry_reason == Some("retryable_timeout") && attempt >= 2)
}

fn codex_login_max_attempts(retry_reason: Option<&str>) -> usize {
    if retry_reason == Some("final_add_phone") {
        FINAL_ADD_PHONE_CODEX_LOGIN_MAX_ATTEMPTS
    } else {
        DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
    }
}

fn should_skip_account_after_retry_exhaustion(retry_reason: Option<&str>) -> bool {
    retry_reason == Some("final_add_phone")
}

fn should_reset_device_auth_session_for_rate_limit(
    message: &str,
    session: Option<&CodexRotateAuthFlowSession>,
) -> bool {
    let normalized = message.trim().to_lowercase();
    if normalized.is_empty() {
        return true;
    }
    let has_reusable_device_challenge = session
        .and_then(|value| value.auth_url.as_deref())
        .is_some_and(|value| !value.trim().is_empty())
        && session
            .and_then(|value| value.device_code.as_deref())
            .is_some_and(|value| !value.trim().is_empty());
    if (normalized.contains("device auth failed with status 429")
        || normalized.contains("device auth failed:")
            && normalized.contains("429 too many requests"))
        && has_reusable_device_challenge
    {
        return false;
    }
    true
}

fn is_device_auth_rate_limited(message: &str) -> bool {
    let normalized = message.to_lowercase();
    normalized.contains("device code request failed with status 429")
        || normalized.contains("device auth failed with status 429")
        || normalized.contains("codex-login-exited-before-auth-url:")
            && normalized.contains("429 too many requests")
        || normalized.contains("429 too many requests")
}

fn format_retry_reason_label(reason: Option<&str>, fallback: &str) -> String {
    reason
        .map(|value| value.replace('_', " "))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn state_mismatch_in_login_flow(
    flow: &CodexRotateAuthFlowSummary,
    error_message: Option<&str>,
) -> bool {
    if flow.consent_error.as_deref() == Some("state_mismatch") {
        return true;
    }
    if flow.callback_complete != Some(true) || flow.codex_login_exit_ok != Some(false) {
        return false;
    }
    let combined = [
        flow.headline.as_deref(),
        flow.codex_login_stderr_tail.as_deref(),
        flow.codex_login_stdout_tail.as_deref(),
        error_message,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join("\n")
    .to_lowercase();
    combined.contains("state mismatch")
}

fn promote_codex_auth_from_session(session: Option<&CodexRotateAuthFlowSession>) -> Result<()> {
    let Some(auth_file_path) = session
        .and_then(|value| value.auth_file_path.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let auth_file_path = Path::new(auth_file_path);
    if !auth_file_path.exists() {
        return Err(anyhow!(
            "Codex device authorization completed without producing {}.",
            auth_file_path.display()
        ));
    }
    let paths = resolve_paths()?;
    if let Some(parent) = paths.codex_auth_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::copy(auth_file_path, &paths.codex_auth_file).with_context(|| {
        format!(
            "Failed to copy {} to {}.",
            auth_file_path.display(),
            paths.codex_auth_file.display()
        )
    })?;
    Ok(())
}

fn cancel_codex_browser_login_session(session: Option<&CodexRotateAuthFlowSession>) {
    let Some(pid) = session
        .and_then(|value| value.pid)
        .filter(|value| *value > 1)
    else {
        return;
    };
    #[cfg(unix)]
    {
        Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .ok();
    }
    #[cfg(windows)]
    {
        Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .ok();
    }
}

fn build_openai_account_login_locator(email: &str) -> CodexRotateSecretLocator {
    CodexRotateSecretLocator::LoginLookup {
        store: "bitwarden-cli".to_string(),
        username: email.trim().to_lowercase(),
        uris: vec![
            "https://auth.openai.com".to_string(),
            "https://chatgpt.com".to_string(),
        ],
        field_path: "/password".to_string(),
    }
}

fn resolve_account_flow_file_for_create(
    paths: &crate::paths::CorePaths,
    _options: &CreateCommandOptions,
) -> std::path::PathBuf {
    paths.account_flow_file.clone()
}

fn run_codex_command<const N: usize>(args: [&str; N]) -> Result<()> {
    let args_vec = args
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();
    let result = Command::new(codex_bin())
        .args(args_vec.iter().map(String::as_str))
        .stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .status()
        .with_context(|| format!("Failed to run {} {}.", codex_bin(), args.join(" ")))?;
    if result.success() {
        return Ok(());
    }
    Err(anyhow!(
        "\"{} {}\" exited with status {}.",
        codex_bin(),
        args.join(" "),
        result.code().unwrap_or_default()
    ))
}

fn codex_bin() -> String {
    resolve_codex_bin_with_paths(
        std::env::var("CODEX_ROTATE_CODEX_BIN").ok().as_deref(),
        Path::new(DEFAULT_CODEX_APP_BUNDLE_BIN),
    )
}

fn resolve_codex_bin_with_paths(explicit: Option<&str>, app_bundle_bin: &Path) -> String {
    let explicit = explicit
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    if let Some(explicit) = explicit {
        return explicit;
    }
    if app_bundle_bin.is_file() {
        return app_bundle_bin.to_string_lossy().into_owned();
    }
    DEFAULT_CODEX_BIN.to_string()
}

fn load_credential_store() -> Result<CredentialStore> {
    let _ = migrate_legacy_credential_store_if_needed()?;
    Ok(normalize_credential_store(load_rotate_state_json()?))
}

fn save_credential_store(store: &CredentialStore) -> Result<()> {
    let mut state = load_rotate_state_json()?;
    let dropped_non_dev_pending = normalize_pending_credential_map(state.get("pending"))
        .into_values()
        .filter(|record| should_drop_non_dev_pending_credential(&record.stored.base_email))
        .filter(|record| {
            !store
                .pending
                .contains_key(&normalize_email_key(&record.stored.email))
        })
        .collect::<Vec<_>>();
    if !state.is_object() {
        state = Value::Object(Map::new());
    }
    let object = state
        .as_object_mut()
        .expect("rotate state must be a JSON object");
    let credential_state = serialize_credential_store(store);
    if let Some(version) = credential_state.get("version").cloned() {
        object.insert("version".to_string(), version);
    }
    if let Some(default_create_base_email) =
        credential_state.get("default_create_base_email").cloned()
    {
        object.insert(
            "default_create_base_email".to_string(),
            default_create_base_email,
        );
    }
    if store.families.is_empty() {
        object.remove("families");
    } else if let Some(families) = credential_state.get("families").cloned() {
        object.insert("families".to_string(), families);
    }
    if store.pending.is_empty() {
        object.remove("pending");
    } else if let Some(pending) = credential_state.get("pending").cloned() {
        object.insert("pending".to_string(), pending);
    }
    if store.skipped.is_empty() {
        object.remove("skipped");
    } else if let Some(skipped) = credential_state.get("skipped").cloned() {
        object.insert("skipped".to_string(), skipped);
    }
    write_rotate_state_json(&state)?;
    cleanup_dropped_non_dev_pending_secrets(&dropped_non_dev_pending);
    Ok(())
}

fn cleanup_dropped_non_dev_pending_secrets(records: &[PendingCredential]) {
    for record in records {
        let result = run_automation_bridge::<_, bool>(
            "delete-account-secret-ref",
            BridgeDeleteSecretPayload {
                profile_name: &record.stored.profile_name,
                email: &record.stored.email,
            },
        );
        if let Err(error) = result {
            eprintln!(
                "{YELLOW}WARN{RESET} Failed to remove stale Bitwarden secret for {}: {}",
                record.stored.email, error
            );
        }
    }
}

fn normalize_credential_store(raw: Value) -> CredentialStore {
    let inventory_emails = collect_inventory_emails_from_state(&raw);
    let raw_version = raw
        .get("version")
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .unwrap_or_default();
    let default_create_base_email = raw
        .get("default_create_base_email")
        .and_then(Value::as_str)
        .and_then(|value| normalize_base_email_family(value).ok())
        .unwrap_or_else(|| DEFAULT_CREATE_BASE_EMAIL.to_string());
    let migrate_legacy_non_default_families =
        raw_version < ROTATE_STATE_VERSION || raw.get("default_create_base_email").is_none();
    let mut families = raw
        .get("families")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| {
                    serde_json::from_value::<CredentialFamily>(value.clone())
                        .ok()
                        .map(|record| (key.clone(), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();
    let legacy_accounts = normalize_stored_credential_map(raw.get("accounts"));
    for account in legacy_accounts.values() {
        merge_legacy_account_into_families(&mut families, account);
    }
    if migrate_legacy_non_default_families {
        families.retain(|_, family| {
            !should_drop_legacy_non_default_family(&family.base_email, &default_create_base_email)
        });
    }
    let mut pending = normalize_pending_credential_map(raw.get("pending"))
        .into_iter()
        .filter(|(email, record)| {
            !inventory_emails.contains(email)
                && !should_drop_non_dev_pending_credential(&record.stored.base_email)
        })
        .collect::<HashMap<_, _>>();
    if migrate_legacy_non_default_families {
        pending.retain(|_, record| {
            !should_drop_legacy_non_default_family(
                &record.stored.base_email,
                &default_create_base_email,
            )
        });
    }
    let skipped = normalize_email_set(raw.get("skipped"))
        .into_iter()
        .filter(|email| !inventory_emails.contains(email) && !pending.contains_key(email))
        .collect::<HashSet<_>>();

    CredentialStore {
        version: ROTATE_STATE_VERSION,
        default_create_base_email,
        families,
        pending,
        skipped,
    }
}

fn should_drop_legacy_non_default_family(base_email: &str, default_base_email: &str) -> bool {
    let Ok(normalized_base_email) = normalize_base_email_family(base_email) else {
        return false;
    };
    if normalized_base_email == default_base_email {
        return false;
    }
    let Ok(parsed) = parse_email_family(&normalized_base_email) else {
        return false;
    };
    if parsed.domain_part != "astronlab.com" {
        return false;
    }
    match parsed.mode {
        EmailFamilyMode::Template { prefix, .. } => {
            prefix.starts_with("bench") || prefix.contains("devicefix")
        }
        EmailFamilyMode::GmailPlus => false,
    }
}

fn should_drop_non_dev_pending_credential(base_email: &str) -> bool {
    normalize_base_email_family(base_email)
        .map(|value| value != DEFAULT_CREATE_BASE_EMAIL)
        .unwrap_or(true)
}

fn collect_inventory_emails_from_state(raw: &Value) -> HashSet<String> {
    raw.get("accounts")
        .and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .get("email")
                        .and_then(Value::as_str)
                        .map(normalize_email_key)
                })
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn normalize_stored_credential_map(raw: Option<&Value>) -> HashMap<String, StoredCredential> {
    raw.and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(email, value)| {
                    normalize_stored_credential(value)
                        .map(|record| (normalize_email_key(email), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

fn normalize_pending_credential_map(raw: Option<&Value>) -> HashMap<String, PendingCredential> {
    raw.and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(email, value)| {
                    normalize_pending_credential(value)
                        .map(|record| (normalize_email_key(email), record))
                })
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default()
}

fn normalize_email_set(raw: Option<&Value>) -> HashSet<String> {
    raw.and_then(Value::as_array)
        .map(|entries| {
            entries
                .iter()
                .filter_map(Value::as_str)
                .map(normalize_email_key)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default()
}

fn normalize_stored_credential(raw: &Value) -> Option<StoredCredential> {
    serde_json::from_value::<StoredCredential>(raw.clone()).ok()
}

fn normalize_pending_credential(raw: &Value) -> Option<PendingCredential> {
    let object = raw.as_object()?;
    Some(PendingCredential {
        stored: normalize_stored_credential(raw)?,
        started_at: object
            .get("started_at")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn serialize_credential_store(store: &CredentialStore) -> Value {
    let pending = store
        .pending
        .iter()
        .map(|(email, record)| (email.clone(), serialize_pending_credential(record)))
        .collect::<Map<String, Value>>();
    let mut skipped = store.skipped.iter().cloned().collect::<Vec<_>>();
    skipped.sort();
    json!({
        "version": ROTATE_STATE_VERSION,
        "default_create_base_email": store.default_create_base_email,
        "families": store.families,
        "pending": pending,
        "skipped": skipped,
    })
}

fn serialize_pending_credential(record: &PendingCredential) -> Value {
    let mut value = serialize_stored_credential(&record.stored)
        .as_object()
        .cloned()
        .unwrap_or_default();
    if let Some(started_at) = record.started_at.as_ref() {
        value.insert("started_at".to_string(), Value::String(started_at.clone()));
    }
    Value::Object(value)
}

fn serialize_stored_credential(record: &StoredCredential) -> Value {
    let mut object = Map::new();
    object.insert("email".to_string(), Value::String(record.email.clone()));
    object.insert(
        "profile_name".to_string(),
        Value::String(record.profile_name.clone()),
    );
    object.insert(
        "base_email".to_string(),
        Value::String(record.base_email.clone()),
    );
    object.insert("suffix".to_string(), Value::Number(record.suffix.into()));
    object.insert(
        "selector".to_string(),
        record
            .selector
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
    );
    object.insert(
        "alias".to_string(),
        record
            .alias
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .unwrap_or(Value::Null),
    );
    object.insert(
        "created_at".to_string(),
        Value::String(record.created_at.clone()),
    );
    object.insert(
        "updated_at".to_string(),
        Value::String(record.updated_at.clone()),
    );
    if let Some(value) = record.birth_month {
        object.insert(
            "birth_month".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    if let Some(value) = record.birth_day {
        object.insert(
            "birth_day".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    if let Some(value) = record.birth_year {
        object.insert(
            "birth_year".to_string(),
            Value::Number(u64::from(value).into()),
        );
    }
    Value::Object(object)
}

fn read_workflow_file_metadata(file_path: &std::path::Path) -> Result<WorkflowFileMetadata> {
    if !file_path.exists() {
        return Err(anyhow!(
            "Workflow file was not found at {}.",
            file_path.display()
        ));
    }

    let raw = fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read workflow file {}.", file_path.display()))?;
    Ok(WorkflowFileMetadata {
        workflow_ref: derive_workflow_ref_from_file_path(file_path),
        ..parse_workflow_file_metadata(&raw)
    })
}

fn parse_workflow_file_metadata(raw: &str) -> WorkflowFileMetadata {
    let parsed = serde_yaml::from_str::<YamlValue>(raw).ok();
    let root = parsed.as_ref();
    let document = root
        .as_ref()
        .and_then(|value| yaml_mapping_get(value, "document"));
    let metadata = document.and_then(|value| yaml_mapping_get(value, "metadata"));

    WorkflowFileMetadata {
        workflow_ref: None,
        preferred_profile_name: metadata
            .and_then(|value| yaml_mapping_get(value, "preferredProfile"))
            .and_then(read_yaml_string),
        preferred_email: metadata
            .and_then(|value| yaml_mapping_get(value, "preferredEmail"))
            .and_then(read_yaml_string),
        default_full_name: read_workflow_input_property(root, "full_name")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_string),
        default_birth_month: read_workflow_input_property(root, "birth_month")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u8),
        default_birth_day: read_workflow_input_property(root, "birth_day")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u8),
        default_birth_year: read_workflow_input_property(root, "birth_year")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u16),
    }
}

fn yaml_mapping_get<'a>(value: &'a YamlValue, key: &str) -> Option<&'a YamlValue> {
    value.as_mapping()?.get(YamlValue::String(key.to_string()))
}

fn read_workflow_input_property<'a>(
    root: Option<&'a YamlValue>,
    field: &str,
) -> Option<&'a YamlValue> {
    let properties = root
        .and_then(|value| yaml_mapping_get(value, "input"))
        .and_then(|value| yaml_mapping_get(value, "schema"))
        .and_then(|value| yaml_mapping_get(value, "document"))
        .and_then(|value| yaml_mapping_get(value, "properties"))?;
    yaml_mapping_get(properties, field)
}

fn read_yaml_string(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        YamlValue::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

fn read_yaml_u8(value: &YamlValue) -> Option<u8> {
    match value {
        YamlValue::Number(value) => value.as_u64().and_then(|value| u8::try_from(value).ok()),
        YamlValue::String(value) => value.trim().parse::<u8>().ok(),
        _ => None,
    }
}

fn read_yaml_u16(value: &YamlValue) -> Option<u16> {
    match value {
        YamlValue::Number(value) => value.as_u64().and_then(|value| u16::try_from(value).ok()),
        YamlValue::String(value) => value.trim().parse::<u16>().ok(),
        _ => None,
    }
}

fn derive_workflow_ref_from_file_path(file_path: &Path) -> Option<String> {
    let canonical_path = file_path.canonicalize().ok()?;
    let paths = resolve_paths().ok()?;
    let workspace_root = paths.repo_root.join(".fast-browser").join("workflows");
    derive_workflow_ref_from_root(&canonical_path, &workspace_root, "workspace").or_else(|| {
        paths.repo_root.parent().and_then(|parent| {
            let global_root = parent
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("workflows");
            derive_workflow_ref_from_root(&canonical_path, &global_root, "sys")
        })
    })
}

fn derive_workflow_ref_from_root(
    file_path: &Path,
    root_dir: &Path,
    scope_prefix: &str,
) -> Option<String> {
    let relative_path = file_path.strip_prefix(root_dir).ok()?;
    if relative_path.extension().and_then(|value| value.to_str()) != Some("yaml") {
        return None;
    }

    let segments = relative_path
        .iter()
        .map(|segment| segment.to_str())
        .collect::<Option<Vec<_>>>()?;
    if segments.len() != 3 {
        return None;
    }

    let workflow_name = Path::new(segments[2]).file_stem()?.to_str()?;
    let parts = [
        Some(scope_prefix.to_string()),
        slugify_workflow_path_segment(segments[0]),
        slugify_workflow_path_segment(segments[1]),
        slugify_workflow_path_segment(workflow_name),
    ]
    .into_iter()
    .collect::<Option<Vec<_>>>()?;
    (parts.len() == 4).then(|| parts.join("."))
}

fn slugify_workflow_path_segment(value: &str) -> Option<String> {
    let mut slug = String::new();
    let mut last_was_separator = false;

    for ch in value.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_was_separator = false;
        } else if !last_was_separator {
            slug.push('-');
            last_was_separator = true;
        }
    }

    let normalized = slug.trim_matches('-').to_string();
    (!normalized.is_empty()).then_some(normalized)
}

fn inspect_managed_profiles() -> Result<ManagedProfilesInspection> {
    let paths = resolve_paths()?;
    let fast_browser_runtime = std::env::var("CODEX_ROTATE_FAST_BROWSER_RUNTIME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| paths.node_bin.clone());
    let output = Command::new(&fast_browser_runtime)
        .arg(&paths.fast_browser_script)
        .arg("profiles")
        .arg("inspect")
        .current_dir(&paths.repo_root)
        .output()
        .with_context(|| {
            format!(
                "Failed to run {} {} profiles inspect.",
                fast_browser_runtime,
                paths.fast_browser_script.display()
            )
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !output.status.success() {
        return Err(anyhow!(if !stdout.is_empty() {
            stdout
        } else {
            format!(
                "fast-browser profiles inspect exited with status {}.",
                output.status
            )
        }));
    }
    let envelope: FastBrowserCliEnvelope<ManagedProfilesInspection> =
        serde_json::from_slice(&output.stdout)
            .context("fast-browser profiles inspect returned invalid JSON.")?;
    if !envelope.ok {
        return Err(anyhow!(
            "{}",
            envelope
                .error
                .and_then(|error| error.message)
                .unwrap_or_else(|| "fast-browser profiles inspect failed.".to_string())
        ));
    }
    envelope
        .result
        .context("fast-browser profiles inspect did not return a result.")
}

fn resolve_managed_profile_name(
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
) -> Result<String> {
    let inspection = inspect_managed_profiles()?;
    let available_profile_names = inspection
        .managed_profiles
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect::<Vec<_>>();
    resolve_managed_profile_name_from_candidates(
        &available_profile_names,
        requested_profile_name,
        preferred_profile_name,
        preferred_profile_source,
        inspection.managed_profiles.default.as_deref(),
    )
}

fn resolve_managed_profile_name_from_candidates(
    available_names: &[&str],
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
    default_profile_name: Option<&str>,
) -> Result<String> {
    if let Some(requested_profile_name) = requested_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&requested_profile_name) {
            return Ok(requested_profile_name.to_string());
        }
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\" was not found.",
            requested_profile_name
        ));
    }

    if let Some(preferred_profile_name) = preferred_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&preferred_profile_name) {
            return Ok(preferred_profile_name.to_string());
        }
        let suffix = preferred_profile_source
            .map(|value| format!(" from {value}"))
            .unwrap_or_default();
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\"{} was not found.",
            preferred_profile_name,
            suffix
        ));
    }

    if let Some(default_profile_name) = default_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&default_profile_name) {
            return Ok(default_profile_name.to_string());
        }
    }

    available_names
        .first()
        .map(|value| (*value).to_string())
        .ok_or_else(|| anyhow!("No managed fast-browser profiles are configured."))
}

fn resolve_create_base_email(
    requested_base_email: Option<&str>,
    discovered_base_email: Option<&str>,
) -> Result<String> {
    if let Some(requested_base_email) = requested_base_email {
        return normalize_base_email_family(requested_base_email);
    }
    if let Some(discovered_base_email) = discovered_base_email {
        return normalize_base_email_family(discovered_base_email);
    }
    normalize_base_email_family(DEFAULT_CREATE_BASE_EMAIL)
}

fn resolve_create_base_email_for_profile(
    store: &CredentialStore,
    profile_name: &str,
    requested_base_email: Option<&str>,
    alias: Option<&str>,
) -> Result<String> {
    let discovered_base_email = if requested_base_email.is_none() {
        select_pending_base_email_hint_for_profile(store, profile_name, alias)
    } else {
        None
    };
    resolve_create_base_email(requested_base_email, discovered_base_email.as_deref())
}

fn make_credential_family_key(profile_name: &str, base_email: &str) -> Result<String> {
    Ok(format!(
        "{}::{}",
        profile_name,
        normalize_base_email_family(base_email)?
    ))
}

fn normalize_base_email_family(email: &str) -> Result<String> {
    Ok(parse_email_family(email)?.normalized)
}

fn parse_email_family(value: &str) -> Result<EmailFamily> {
    let normalized = value.trim().to_lowercase();
    let parts = normalized.split('@').collect::<Vec<_>>();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return Err(anyhow!("\"{}\" is not a valid email family.", value));
    }
    let local_part = parts[0].to_string();
    let domain_part = parts[1].to_string();
    let placeholder_count = local_part.matches(EMAIL_FAMILY_PLACEHOLDER).count();

    if placeholder_count == 1 {
        let segments = local_part
            .split(EMAIL_FAMILY_PLACEHOLDER)
            .collect::<Vec<_>>();
        let prefix = segments[0].to_string();
        let suffix = segments[1].to_string();
        if format!("{}{}", prefix, suffix).trim().is_empty() {
            return Err(anyhow!(
                "\"{}\" must keep some stable local-part text around {}.",
                value,
                EMAIL_FAMILY_PLACEHOLDER
            ));
        }
        return Ok(EmailFamily {
            normalized: format!("{prefix}{EMAIL_FAMILY_PLACEHOLDER}{suffix}@{domain_part}"),
            local_part,
            domain_part,
            mode: EmailFamilyMode::Template { prefix, suffix },
        });
    }

    if placeholder_count > 1 {
        return Err(anyhow!(
            "\"{}\" may only contain one {} placeholder.",
            value,
            EMAIL_FAMILY_PLACEHOLDER
        ));
    }

    if domain_part != "gmail.com" {
        return Err(anyhow!(
            "\"{}\" is not a supported base email family. Use gmail.com or a template like dev.{{N}}@example.com.",
            value
        ));
    }

    let base_local = local_part
        .split('+')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("\"{}\" does not contain a valid Gmail local part.", value))?;

    Ok(EmailFamily {
        normalized: format!("{base_local}@gmail.com"),
        local_part: base_local.to_string(),
        domain_part: "gmail.com".to_string(),
        mode: EmailFamilyMode::GmailPlus,
    })
}

fn build_account_family_email(base_email: &str, suffix: u32) -> Result<String> {
    if suffix < 1 {
        return Err(anyhow!("Invalid email family suffix \"{}\".", suffix));
    }
    let parsed = parse_email_family(base_email)?;
    Ok(match parsed.mode {
        EmailFamilyMode::Template {
            prefix,
            suffix: tail,
        } => {
            format!("{prefix}{suffix}{tail}@{}", parsed.domain_part)
        }
        EmailFamilyMode::GmailPlus => {
            format!("{}+{}@{}", parsed.local_part, suffix, parsed.domain_part)
        }
    })
}

fn extract_account_family_suffix(candidate_email: &str, base_email: &str) -> Result<Option<u32>> {
    let parsed = parse_email_family(base_email)?;
    let normalized_candidate = candidate_email.trim().to_lowercase();
    Ok(match parsed.mode {
        EmailFamilyMode::Template { prefix, suffix } => {
            let domain_suffix = format!("@{}", parsed.domain_part);
            if !normalized_candidate.ends_with(&domain_suffix) {
                return Ok(None);
            }
            let without_domain = normalized_candidate
                .strip_suffix(&domain_suffix)
                .unwrap_or_default();
            let middle = without_domain
                .strip_prefix(&prefix)
                .and_then(|value| value.strip_suffix(&suffix));
            middle
                .filter(|value| {
                    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u32>().ok())
        }
        EmailFamilyMode::GmailPlus => {
            let expected_prefix = format!("{}+", parsed.local_part);
            let expected_suffix = format!("@{}", parsed.domain_part);
            let middle = normalized_candidate
                .strip_prefix(&expected_prefix)
                .and_then(|value| value.strip_suffix(&expected_suffix));
            middle
                .filter(|value| {
                    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
                })
                .and_then(|value| value.parse::<u32>().ok())
        }
    })
}

#[cfg(test)]
fn compute_next_account_family_suffix(base_email: &str, known_emails: Vec<String>) -> Result<u32> {
    compute_next_account_family_suffix_with_skips(
        base_email,
        known_emails,
        Vec::new(),
        DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
    )
}

fn compute_next_account_family_suffix_with_skips(
    base_email: &str,
    known_emails: Vec<String>,
    skipped_emails: Vec<String>,
    max_skipped_slots: u32,
) -> Result<u32> {
    let mut used = HashSet::new();
    for email in known_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, base_email)? {
            used.insert(suffix);
        }
    }
    let mut skipped = HashSet::new();
    for email in skipped_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, base_email)? {
            skipped.insert(suffix);
        }
    }
    let mut candidate = 1;
    let should_reserve_skipped = (skipped.len() as u32) <= max_skipped_slots;
    while used.contains(&candidate) || (should_reserve_skipped && skipped.contains(&candidate)) {
        candidate += 1;
    }
    Ok(candidate)
}

fn compute_fresh_account_family_suffix(
    family: Option<&CredentialFamily>,
    base_email: &str,
    known_emails: Vec<String>,
    skipped_emails: Vec<String>,
) -> Result<u32> {
    let computed = compute_next_account_family_suffix_with_skips(
        base_email,
        known_emails,
        skipped_emails.clone(),
        max_skipped_slots_for_family(family),
    )?;
    if skipped_emails.is_empty() {
        Ok(computed)
    } else {
        Ok(family
            .map(|entry| entry.next_suffix.max(computed))
            .unwrap_or(computed))
    }
}

fn collect_known_account_emails(pool: &Pool, store: &CredentialStore) -> Vec<String> {
    let mut emails = pool
        .accounts
        .iter()
        .map(|entry| entry.email.clone())
        .collect::<Vec<_>>();
    emails.extend(store.pending.keys().cloned());
    emails
}

fn collect_skipped_account_emails_for_family(
    store: &CredentialStore,
    profile_name: &str,
    base_email: &str,
) -> Vec<String> {
    let Ok(family_key) = make_credential_family_key(profile_name, base_email) else {
        return Vec::new();
    };
    store
        .skipped
        .iter()
        .filter(|email| {
            select_family_for_account_email(store, email)
                .map(|matched| matched.key == family_key)
                .unwrap_or_else(|| {
                    extract_account_family_suffix(email, base_email)
                        .map(|suffix| suffix.is_some())
                        .unwrap_or(false)
                })
        })
        .cloned()
        .collect()
}

fn max_skipped_slots_for_family(family: Option<&CredentialFamily>) -> u32 {
    family
        .map(|entry| entry.max_skipped_slots)
        .unwrap_or(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY)
}

fn select_pending_credential_for_family(
    store: &CredentialStore,
    profile_name: &str,
    base_email: &str,
    alias: Option<&str>,
) -> Option<PendingCredential> {
    let normalized_base_email = normalize_base_email_family(base_email).ok()?;
    let normalized_alias = normalize_alias(alias);
    let mut matches = store
        .pending
        .values()
        .filter(|entry| {
            entry.stored.profile_name == profile_name
                && normalize_base_email_family(&entry.stored.base_email)
                    .map(|value| value == normalized_base_email)
                    .unwrap_or(false)
                && (normalized_alias.is_none()
                    || normalize_alias(entry.stored.alias.as_deref()) == normalized_alias)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        left.stored
            .suffix
            .cmp(&right.stored.suffix)
            .then_with(|| {
                parse_sortable_timestamp(
                    left.started_at
                        .as_deref()
                        .or(Some(left.stored.created_at.as_str()))
                        .or(Some(left.stored.updated_at.as_str())),
                )
                .cmp(&parse_sortable_timestamp(
                    right
                        .started_at
                        .as_deref()
                        .or(Some(right.stored.created_at.as_str()))
                        .or(Some(right.stored.updated_at.as_str())),
                ))
            })
            .then_with(|| {
                parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
                )
            })
    });
    matches.into_iter().next()
}

fn select_pending_base_email_hint_for_profile(
    store: &CredentialStore,
    profile_name: &str,
    alias: Option<&str>,
) -> Option<String> {
    let normalized_alias = normalize_alias(alias);
    let mut matches = store
        .pending
        .values()
        .filter(|entry| {
            entry.stored.profile_name == profile_name
                && (normalized_alias.is_none()
                    || normalize_alias(entry.stored.alias.as_deref()) == normalized_alias)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by(|left, right| {
        let left_priority =
            get_create_family_hint_priority(&left.stored.base_email, left.stored.suffix + 1);
        let right_priority =
            get_create_family_hint_priority(&right.stored.base_email, right.stored.suffix + 1);
        left_priority
            .family_rank
            .cmp(&right_priority.family_rank)
            .reverse()
            .then_with(|| {
                left_priority
                    .frontier
                    .cmp(&right_priority.frontier)
                    .reverse()
            })
            .then_with(|| {
                parse_sortable_timestamp(
                    left.started_at
                        .as_deref()
                        .or(Some(left.stored.created_at.as_str()))
                        .or(Some(left.stored.updated_at.as_str())),
                )
                .cmp(&parse_sortable_timestamp(
                    right
                        .started_at
                        .as_deref()
                        .or(Some(right.stored.created_at.as_str()))
                        .or(Some(right.stored.updated_at.as_str())),
                ))
            })
            .then_with(|| left.stored.suffix.cmp(&right.stored.suffix))
            .then_with(|| {
                parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
                )
            })
    });

    matches
        .into_iter()
        .find_map(|entry| normalize_base_email_family(&entry.stored.base_email).ok())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CreateFamilyHintPriority {
    family_rank: u8,
    frontier: u32,
}

fn get_create_family_hint_priority(base_email: &str, frontier: u32) -> CreateFamilyHintPriority {
    let normalized_frontier = frontier.max(1);
    let family_rank = parse_email_family(base_email)
        .ok()
        .map(|parsed| match parsed.mode {
            EmailFamilyMode::Template { prefix, suffix } => {
                if parsed.domain_part == "astronlab.com" && prefix == "dev." && suffix.is_empty() {
                    2
                } else {
                    1
                }
            }
            EmailFamilyMode::GmailPlus => 0,
        })
        .unwrap_or(0);
    CreateFamilyHintPriority {
        family_rank,
        frontier: normalized_frontier,
    }
}

#[cfg(test)]
fn should_use_default_create_family_hint(base_email: Option<&str>) -> bool {
    base_email
        .and_then(|value| parse_email_family(value).ok())
        .map(|parsed| matches!(parsed.mode, EmailFamilyMode::Template { .. }))
        .unwrap_or(false)
}

#[cfg(test)]
fn normalize_gmail_base_email(email: &str) -> Result<String> {
    normalize_base_email_family(email)
}

#[cfg(test)]
fn compute_next_gmail_alias_suffix(base_email: &str, known_emails: Vec<String>) -> Result<u32> {
    compute_next_account_family_suffix(base_email, known_emails)
}

#[cfg(test)]
fn normalize_email_candidate(value: &str) -> Option<String> {
    let trimmed = value.trim().to_lowercase();
    let (local, domain) = trimmed.split_once('@')?;
    if local.is_empty() || domain.is_empty() || domain.starts_with('.') || domain.ends_with('.') {
        return None;
    }
    domain.contains('.').then_some(trimmed)
}

#[cfg(test)]
fn extract_supported_gmail_emails(emails: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut supported = Vec::new();
    for email in emails {
        let Ok(normalized) = normalize_gmail_base_email(&email) else {
            continue;
        };
        if seen.insert(normalized.clone()) {
            supported.push(normalized);
        }
    }
    supported
}

#[cfg(test)]
fn tokenize_managed_profile_name(profile_name: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for ch in profile_name.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
fn score_email_for_managed_profile_name(profile_name: &str, email: &str) -> i32 {
    let Some(normalized_email) = normalize_email_candidate(email) else {
        return i32::MIN;
    };

    let local_part = normalized_email
        .split('@')
        .next()
        .unwrap_or_default()
        .split('+')
        .next()
        .unwrap_or_default()
        .to_string();
    let compact_local = local_part
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    let local_segments = local_part
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(ToOwned::to_owned)
        .collect::<HashSet<_>>();
    let significant_tokens = tokenize_managed_profile_name(profile_name)
        .into_iter()
        .filter(|token| {
            token.len() > 1 || token.chars().all(|character| character.is_ascii_digit())
        })
        .collect::<Vec<_>>();

    let mut score = 0;
    for token in significant_tokens {
        if local_segments.contains(&token) {
            score += if token.chars().all(|character| character.is_ascii_digit()) {
                140
            } else {
                120
            };
            continue;
        }
        if compact_local.starts_with(&token) || compact_local.ends_with(&token) {
            score += 40;
            continue;
        }
        if compact_local.contains(&token) {
            score += 25;
        }
    }

    let compact_profile = profile_name
        .to_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    if compact_profile.len() >= 3 {
        if compact_local.contains(&compact_profile) {
            score += 80;
        } else {
            let reversed = compact_profile.chars().rev().collect::<String>();
            if compact_local.contains(&reversed) {
                score += 40;
            }
        }
    }

    score
}

#[cfg(test)]
fn select_best_email_for_managed_profile(
    profile_name: &str,
    emails: impl IntoIterator<Item = String>,
    preferred_base_email: Option<&str>,
) -> Option<String> {
    let normalized_preferred =
        preferred_base_email.and_then(|value| normalize_gmail_base_email(value).ok());
    let mut candidates = extract_supported_gmail_emails(emails)
        .into_iter()
        .enumerate()
        .map(|(index, email)| {
            let exact_preferred = normalized_preferred
                .as_ref()
                .map(|preferred| preferred == &email)
                .unwrap_or(false);
            let score = score_email_for_managed_profile_name(profile_name, &email);
            (index, email, exact_preferred, score)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.2
            .cmp(&right.2)
            .reverse()
            .then_with(|| left.3.cmp(&right.3).reverse())
            .then_with(|| left.0.cmp(&right.0))
    });
    candidates.into_iter().next().map(|(_, email, _, _)| email)
}

#[cfg(test)]
fn select_stored_base_email_hint(store: &CredentialStore, profile_name: &str) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64, u32)>::new();

    let mut remember = |raw_email: Option<&str>, updated_at: Option<&str>, frontier: u32| {
        let Some(raw_email) = raw_email else {
            return;
        };
        let Ok(base_email) = normalize_base_email_family(raw_email) else {
            return;
        };
        let entry = candidates.entry(base_email).or_insert((0, 0, 1));
        entry.0 += 1;
        entry.1 = entry.1.max(parse_sortable_timestamp(updated_at));
        entry.2 = entry.2.max(frontier.max(1));
    };

    for family in store.families.values() {
        if family.profile_name == profile_name {
            remember(
                Some(&family.base_email),
                Some(family.updated_at.as_str()),
                family.next_suffix,
            );
        }
    }
    for pending in store.pending.values() {
        if pending.stored.profile_name == profile_name {
            remember(
                Some(&pending.stored.base_email),
                pending
                    .started_at
                    .as_deref()
                    .or(Some(pending.stored.updated_at.as_str())),
                pending.stored.suffix.saturating_add(1),
            );
        }
    }

    candidates
        .into_iter()
        .max_by(|left, right| {
            let left_priority = get_create_family_hint_priority(&left.0, left.1 .2);
            let right_priority = get_create_family_hint_priority(&right.0, right.1 .2);
            left_priority
                .family_rank
                .cmp(&right_priority.family_rank)
                .then_with(|| left_priority.frontier.cmp(&right_priority.frontier))
                .then_with(|| left.1 .0.cmp(&right.1 .0))
                .then_with(|| left.1 .1.cmp(&right.1 .1))
                .then_with(|| right.0.cmp(&left.0))
        })
        .map(|(base_email, _)| base_email)
}

#[cfg(test)]
fn select_best_system_chrome_profile_match(
    profile_name: &str,
    profiles: &[SystemChromeProfileCandidate],
    preferred_base_email: Option<&str>,
) -> Option<SystemChromeProfileMatch> {
    let normalized_preferred =
        preferred_base_email.and_then(|value| normalize_gmail_base_email(value).ok());
    profiles
        .iter()
        .filter_map(|profile| {
            let matched_email = select_best_email_for_managed_profile(
                profile_name,
                profile.emails.clone(),
                preferred_base_email,
            )?;
            let emails = extract_supported_gmail_emails(profile.emails.clone());
            let score = if normalized_preferred
                .as_ref()
                .map(|preferred| preferred == &matched_email)
                .unwrap_or(false)
            {
                10_000
            } else {
                score_email_for_managed_profile_name(profile_name, &matched_email)
            };
            Some(SystemChromeProfileMatch {
                directory: profile.directory.clone(),
                name: profile.name.clone(),
                emails,
                matched_email,
                score,
            })
        })
        .max_by(|left, right| {
            left.score
                .cmp(&right.score)
                .then_with(|| right.directory.cmp(&left.directory))
        })
}

fn resolve_relogin_credential(
    store: &CredentialStore,
    entry: &AccountEntry,
) -> Option<StoredCredential> {
    if let Some(pending) = store
        .pending
        .get(&normalize_email_key(&entry.email))
        .map(|value| value.stored.clone())
    {
        return Some(pending);
    }
    let family_match = select_family_for_account_email(store, &entry.email)?;
    Some(StoredCredential {
        email: entry.email.clone(),
        profile_name: family_match.family.profile_name.clone(),
        base_email: family_match.family.base_email.clone(),
        suffix: family_match.suffix,
        selector: Some(entry.label.clone()),
        alias: entry.alias.clone(),
        birth_month: None,
        birth_day: None,
        birth_year: None,
        created_at: family_match.family.created_at.clone(),
        updated_at: family_match.family.updated_at.clone(),
    })
}

#[derive(Clone)]
struct FamilyAccountMatch {
    key: String,
    family: CredentialFamily,
    suffix: u32,
}

fn select_family_for_account_email(
    store: &CredentialStore,
    email: &str,
) -> Option<FamilyAccountMatch> {
    let normalized_email = normalize_email_key(email);
    let mut matches = store
        .families
        .iter()
        .filter_map(|(key, family)| {
            extract_account_family_suffix(&normalized_email, &family.base_email)
                .ok()
                .flatten()
                .map(|suffix| FamilyAccountMatch {
                    key: key.clone(),
                    family: family.clone(),
                    suffix,
                })
        })
        .collect::<Vec<_>>();

    if matches.is_empty() {
        return None;
    }

    matches.sort_by(|left, right| {
        let left_exact =
            left.family.last_created_email.as_deref() == Some(normalized_email.as_str());
        let right_exact =
            right.family.last_created_email.as_deref() == Some(normalized_email.as_str());
        left_exact
            .cmp(&right_exact)
            .then_with(|| {
                parse_sortable_timestamp(Some(left.family.updated_at.as_str())).cmp(
                    &parse_sortable_timestamp(Some(right.family.updated_at.as_str())),
                )
            })
            .then_with(|| right.key.cmp(&left.key))
    });

    let top = matches.pop()?;
    let top_exact = top.family.last_created_email.as_deref() == Some(normalized_email.as_str());
    if top_exact {
        let other_exact_exists = matches.iter().any(|entry| {
            entry.family.last_created_email.as_deref() == Some(normalized_email.as_str())
        });
        if other_exact_exists {
            return None;
        }
        return Some(top);
    }

    if matches.is_empty() {
        return Some(top);
    }

    None
}

fn upsert_family_for_account(store: &mut CredentialStore, account: &StoredCredential) -> bool {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.base_email)
    else {
        return false;
    };
    let next_updated_at = account.updated_at.clone();
    let next_created_at = account.created_at.clone();
    let next_last_created_email = Some(account.email.clone());
    let next_suffix = account.suffix.saturating_add(1);
    match store.families.get_mut(&family_key) {
        Some(existing) => {
            let previous = existing.clone();
            existing.next_suffix = existing.next_suffix.max(next_suffix);
            if parse_sortable_timestamp(Some(next_created_at.as_str()))
                < parse_sortable_timestamp(Some(existing.created_at.as_str()))
                || existing.created_at.trim().is_empty()
            {
                existing.created_at = next_created_at.clone();
            }
            if parse_sortable_timestamp(Some(next_updated_at.as_str()))
                >= parse_sortable_timestamp(Some(existing.updated_at.as_str()))
            {
                existing.updated_at = next_updated_at.clone();
                existing.last_created_email = next_last_created_email.clone();
            }
            previous != *existing
        }
        None => {
            store.families.insert(
                family_key,
                CredentialFamily {
                    profile_name: account.profile_name.clone(),
                    base_email: account.base_email.clone(),
                    next_suffix,
                    max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                    created_at: next_created_at,
                    updated_at: next_updated_at,
                    last_created_email: next_last_created_email,
                },
            );
            true
        }
    }
}

fn merge_legacy_account_into_families(
    families: &mut HashMap<String, CredentialFamily>,
    account: &StoredCredential,
) {
    let Ok(family_key) = make_credential_family_key(&account.profile_name, &account.base_email)
    else {
        return;
    };
    let updated_at = parse_sortable_timestamp(Some(account.updated_at.as_str()));
    let created_at = parse_sortable_timestamp(Some(account.created_at.as_str()));
    match families.get_mut(&family_key) {
        Some(existing) => {
            existing.next_suffix = existing.next_suffix.max(account.suffix.saturating_add(1));
            if created_at < parse_sortable_timestamp(Some(existing.created_at.as_str()))
                || existing.created_at.trim().is_empty()
            {
                existing.created_at = account.created_at.clone();
            }
            if updated_at >= parse_sortable_timestamp(Some(existing.updated_at.as_str())) {
                existing.updated_at = account.updated_at.clone();
                existing.last_created_email = Some(account.email.clone());
            }
        }
        None => {
            families.insert(
                family_key,
                CredentialFamily {
                    profile_name: account.profile_name.clone(),
                    base_email: account.base_email.clone(),
                    next_suffix: account.suffix.saturating_add(1),
                    max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                    created_at: account.created_at.clone(),
                    updated_at: account.updated_at.clone(),
                    last_created_email: Some(account.email.clone()),
                },
            );
        }
    }
}

fn parse_sortable_timestamp(value: Option<&str>) -> i64 {
    value
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.timestamp_millis())
        .unwrap_or(0)
}

fn normalize_email_key(email: &str) -> String {
    email.trim().to_lowercase()
}

fn normalize_alias(alias: Option<&str>) -> Option<String> {
    alias
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn generate_password(length: usize) -> String {
    const UPPERCASE: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ";
    const LOWERCASE: &[u8] = b"abcdefghijkmnopqrstuvwxyz";
    const DIGITS: &[u8] = b"23456789";
    const ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";

    assert!(length >= 12);

    let mut chars = vec![
        pick_random_char(UPPERCASE),
        pick_random_char(LOWERCASE),
        pick_random_char(DIGITS),
    ];
    while chars.len() < length {
        chars.push(pick_random_char(ALPHABET));
    }
    fisher_yates_shuffle(&mut chars);
    chars.into_iter().collect()
}

fn pick_random_char(source: &[u8]) -> char {
    let mut bytes = [0u8; 8];
    OsRng.fill_bytes(&mut bytes);
    let index = u64::from_le_bytes(bytes) as usize % source.len();
    source[index] as char
}

fn fisher_yates_shuffle(chars: &mut [char]) {
    for index in (1..chars.len()).rev() {
        let mut bytes = [0u8; 8];
        OsRng.fill_bytes(&mut bytes);
        let swap_index = u64::from_le_bytes(bytes) as usize % (index + 1);
        chars.swap(index, swap_index);
    }
}

fn resolve_credential_birth_date(
    credential: Option<&StoredCredential>,
    fallback_birth_date: Option<&AdultBirthDate>,
) -> Option<AdultBirthDate> {
    if let Some(credential) = credential {
        if let (Some(birth_month), Some(birth_day), Some(birth_year)) = (
            credential.birth_month,
            credential.birth_day,
            credential.birth_year,
        ) {
            return Some(AdultBirthDate {
                birth_month,
                birth_day,
                birth_year,
            });
        }
    }

    fallback_birth_date.cloned()
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ENV_MUTEX;
    use base64::Engine;
    use std::fs;
    use std::path::Path;
    use std::process::Command as ProcessCommand;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    fn with_rotate_home<T>(prefix: &str, test: impl FnOnce(&Path) -> T) -> T {
        let rotate_home = unique_temp_dir(prefix);
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }
        let result = test(&rotate_home);
        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        fs::remove_dir_all(&rotate_home).ok();
        result
    }

    fn make_pending(
        email: &str,
        profile_name: &str,
        base_email: &str,
        suffix: u32,
        created_at: &str,
    ) -> PendingCredential {
        PendingCredential {
            stored: StoredCredential {
                email: email.to_string(),
                profile_name: profile_name.to_string(),
                base_email: base_email.to_string(),
                suffix,
                selector: None,
                alias: None,
                birth_month: None,
                birth_day: None,
                birth_year: None,
                created_at: created_at.to_string(),
                updated_at: created_at.to_string(),
            },
            started_at: Some(created_at.to_string()),
        }
    }

    fn make_jwt(payload: &str) -> String {
        format!(
            "{}.{}.signature",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(r#"{"alg":"none","typ":"JWT"}"#),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
        )
    }

    fn make_auth(email: &str, account_id: &str) -> CodexAuth {
        CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: crate::auth::AuthTokens {
                access_token: make_jwt(&format!(
                    r#"{{"https://api.openai.com/profile":{{"email":"{email}"}},"https://api.openai.com/auth":{{"chatgpt_account_id":"{account_id}","chatgpt_plan_type":"free"}}}}"#
                )),
                id_token: make_jwt(&format!(r#"{{"email":"{email}"}}"#)),
                refresh_token: Some(format!("refresh-{account_id}")),
                account_id: account_id.to_string(),
            },
            last_refresh: "2026-04-13T02:52:15.012Z".to_string(),
        }
    }

    fn repo_root() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .join("..")
            .canonicalize()
            .expect("repo root")
    }

    #[test]
    fn codex_bin_uses_explicit_override_before_app_bundle() {
        let app_root = unique_temp_dir("codex-rotate-codex-bin-override");
        fs::create_dir_all(&app_root).expect("create app root");
        let app_path = app_root.join("Codex");
        fs::write(&app_path, "stub").expect("write app bundle stub");

        let resolved = resolve_codex_bin_with_paths(Some("/tmp/custom-codex"), &app_path);
        assert_eq!(resolved, "/tmp/custom-codex");

        fs::remove_dir_all(&app_root).ok();
    }

    #[test]
    fn codex_bin_prefers_app_bundle_when_present() {
        let app_root = unique_temp_dir("codex-rotate-codex-bin-app");
        fs::create_dir_all(&app_root).expect("create app root");
        let app_path = app_root.join("Codex");
        fs::write(&app_path, "stub").expect("write app bundle stub");

        let resolved = resolve_codex_bin_with_paths(None, &app_path);
        assert_eq!(resolved, app_path.to_string_lossy());

        fs::remove_dir_all(&app_root).ok();
    }

    #[test]
    fn codex_bin_falls_back_to_bare_codex_when_app_bundle_is_absent() {
        let app_root = unique_temp_dir("codex-rotate-codex-bin-fallback");
        let app_path = app_root.join("Codex");

        let resolved = resolve_codex_bin_with_paths(None, &app_path);
        assert_eq!(resolved, DEFAULT_CODEX_BIN);
    }

    #[test]
    fn stored_credentials_used_only_for_non_manual_relogin() {
        let stored = StoredCredential {
            email: "dev.user+1@gmail.com".to_string(),
            profile_name: "dev-1".to_string(),
            base_email: "dev.user@gmail.com".to_string(),
            suffix: 1,
            selector: Some("dev.user+1@gmail.com_free".to_string()),
            alias: None,
            birth_month: None,
            birth_day: None,
            birth_year: None,
            created_at: "2026-03-20T00:00:00.000Z".to_string(),
            updated_at: "2026-03-20T00:00:00.000Z".to_string(),
        };

        assert!(should_use_stored_credential_relogin(
            Some(&stored),
            &ReloginOptions::default()
        ));
        assert!(!should_use_stored_credential_relogin(
            Some(&stored),
            &ReloginOptions {
                manual_login: true,
                ..ReloginOptions::default()
            }
        ));
    }

    #[test]
    fn read_workflow_file_metadata_reads_preferred_profile_from_main_workflow() {
        let workflow_file = repo_root()
            .join(".fast-browser")
            .join("workflows")
            .join("web")
            .join("auth.openai.com")
            .join("codex-rotate-account-flow-main.yaml");

        let metadata = read_workflow_file_metadata(&workflow_file).expect("workflow metadata");

        assert_eq!(
            metadata.workflow_ref.as_deref(),
            Some("workspace.web.auth-openai-com.codex-rotate-account-flow-main")
        );
        assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
        assert_eq!(metadata.preferred_email, None);
        assert_eq!(metadata.default_full_name.as_deref(), Some("Dev Astronlab"));
        assert_eq!(metadata.default_birth_month, Some(1));
        assert_eq!(metadata.default_birth_day, Some(24));
        assert_eq!(metadata.default_birth_year, Some(1990));
    }

    #[test]
    fn derive_workflow_ref_from_file_path_handles_alternate_local_workflow() {
        let workflow_file = repo_root()
            .join(".fast-browser")
            .join("workflows")
            .join("web")
            .join("auth.openai.com")
            .join("codex-rotate-account-flow-minimal.yaml");

        assert_eq!(
            derive_workflow_ref_from_file_path(&workflow_file).as_deref(),
            Some("workspace.web.auth-openai-com.codex-rotate-account-flow-minimal")
        );
    }

    #[test]
    fn parse_workflow_file_metadata_handles_quotes_and_comments() {
        let metadata = parse_workflow_file_metadata(
            r#"
document:
  metadata:
    preferredProfile: "dev-1" # comment
    preferredEmail: 'dev.41@astronlab.com'
    targets:
      - id: primary
input:
  schema:
    document:
      properties:
        full_name:
          default: "Dev Astronlab"
        birth_month:
          default: 1
        birth_day:
          default: 24
        birth_year:
          default: 1990
"#,
        );

        assert_eq!(metadata.preferred_profile_name.as_deref(), Some("dev-1"));
        assert_eq!(
            metadata.preferred_email.as_deref(),
            Some("dev.41@astronlab.com")
        );
        assert_eq!(metadata.default_full_name.as_deref(), Some("Dev Astronlab"));
        assert_eq!(metadata.default_birth_month, Some(1));
        assert_eq!(metadata.default_birth_day, Some(24));
        assert_eq!(metadata.default_birth_year, Some(1990));
    }

    #[test]
    fn resolve_login_workflow_defaults_uses_explicit_value() {
        let defaults =
            resolve_login_workflow_defaults(Some("workspace.web.auth-openai-com.custom-flow"))
                .expect("login workflow defaults");

        assert_eq!(
            defaults.workflow_ref,
            "workspace.web.auth-openai-com.custom-flow"
        );
        assert_eq!(defaults.full_name, "Dev Astronlab");
        assert_eq!(
            defaults.birth_date,
            AdultBirthDate {
                birth_month: 1,
                birth_day: 24,
                birth_year: 1990,
            }
        );
    }

    #[test]
    fn resolve_login_workflow_defaults_falls_back_to_default_for_missing_or_blank_values() {
        let defaults = resolve_login_workflow_defaults(None).expect("login workflow defaults");
        assert_eq!(
            defaults.workflow_ref,
            "workspace.web.auth-openai-com.codex-rotate-account-flow-main"
        );
        assert_eq!(defaults.full_name, "Dev Astronlab");
        assert_eq!(
            resolve_login_workflow_defaults(Some("   "))
                .expect("login workflow defaults")
                .workflow_ref,
            "workspace.web.auth-openai-com.codex-rotate-account-flow-main"
        );
    }

    #[test]
    fn reads_auth_flow_summary_from_raw_fast_browser_output() {
        let result = FastBrowserRunResult {
            output: Some(json!({
                "stage": "email_verification",
                "success": false,
                "account_ready": true,
                "next_action": "retry_verification",
                "codex_session": {
                    "auth_url": "https://auth.openai.com",
                    "callback_port": "8765",
                    "pid": "4321",
                    "session_dir": "/tmp/codex-session"
                }
            })),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);
        assert_eq!(summary.stage.as_deref(), Some("email_verification"));
        assert_eq!(summary.success, Some(false));
        assert_eq!(summary.account_ready, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("retry_verification"));
        assert_eq!(
            summary
                .codex_session
                .as_ref()
                .and_then(|session| session.callback_port),
            Some(8765)
        );
        assert_eq!(
            summary
                .codex_session
                .as_ref()
                .and_then(|session| session.pid),
            Some(4321)
        );
    }

    #[test]
    fn deserializes_bridge_login_attempt_result_with_extra_fast_browser_fields() {
        let payload = json!({
            "result": {
                "ok": true,
                "workflow": {
                    "ref": "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth"
                },
                "finalUrl": "https://chatgpt.com/#settings/Security",
                "output": {
                    "success": true,
                    "next_action": "complete",
                    "codex_session": {
                        "auth_url": "https://auth.openai.com/authorize",
                        "callback_port": "8765"
                    }
                },
                "mode": "managed",
                "steps": {},
                "artifactMode": "full",
                "state": {
                    "steps": {
                        "start_codex_login_session": {
                            "action": {
                                "value": {
                                    "auth_url": "https://auth.openai.com/authorize",
                                    "callback_port": "8765",
                                    "pid": "4321"
                                }
                            }
                        }
                    }
                },
                "runtime_profiles": {
                    "default": "dev-1"
                },
                "observability": {
                    "runId": "demo"
                }
            },
            "error_message": null,
            "managed_runtime_reset_performed": false
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(payload).expect("bridge payload should deserialize");
        let normalized = result.result.expect("normalized fast-browser result");
        let summary = read_codex_rotate_auth_flow_summary(&normalized);
        let session = read_codex_rotate_auth_flow_session(&normalized).expect("session payload");

        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
        assert_eq!(session.callback_port, Some(8765));
        assert_eq!(session.pid, Some(4321));
    }

    #[test]
    fn deserializes_bridge_login_attempt_result_from_bare_fast_browser_result() {
        let payload = json!({
            "ok": true,
            "status": "completed",
            "output": {
                "success": true,
                "next_action": "complete"
            },
            "state": {
                "steps": {
                    "start_codex_login_session": {
                        "action": {
                            "value": {
                                "auth_url": "https://auth.openai.com/authorize",
                                "callback_port": "8765"
                            }
                        }
                    }
                }
            }
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(payload).expect("bare fast-browser result should deserialize");
        let normalized = result.result.expect("normalized fast-browser result");
        let summary = read_codex_rotate_auth_flow_summary(&normalized);
        let session = read_codex_rotate_auth_flow_session(&normalized).expect("session payload");

        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
        assert_eq!(session.callback_port, Some(8765));
    }

    #[test]
    fn deserializes_bridge_login_attempt_result_with_stringified_fast_browser_result() {
        let payload = json!({
            "result": serde_json::to_string(&json!({
                "ok": true,
                "output": {
                    "success": true,
                    "next_action": "complete"
                }
            }))
            .expect("stringify payload"),
            "error_message": null
        });

        let result: BridgeLoginAttemptResult = serde_json::from_value(payload)
            .expect("stringified fast-browser result should deserialize");
        let normalized = result.result.expect("normalized fast-browser result");
        let summary = read_codex_rotate_auth_flow_summary(&normalized);

        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn reads_auth_flow_session_from_start_step_when_summary_omits_it() {
        let result = FastBrowserRunResult {
            state: Some(FastBrowserState {
                steps: HashMap::from([(
                    "start_codex_login_session".to_string(),
                    FastBrowserStepState {
                        action: Some(json!({
                            "value": {
                                "auth_url": "https://auth.openai.com",
                                "callback_port": "7654",
                                "pid": "2468",
                                "stdout_path": "/tmp/codex.stdout"
                            }
                        })),
                    },
                )]),
            }),
            ..FastBrowserRunResult::default()
        };

        let session = read_codex_rotate_auth_flow_session(&result).expect("session");
        assert_eq!(session.auth_url.as_deref(), Some("https://auth.openai.com"));
        assert_eq!(session.callback_port, Some(7654));
        assert_eq!(session.pid, Some(2468));
        assert_eq!(session.stdout_path.as_deref(), Some("/tmp/codex.stdout"));
    }

    #[test]
    fn reads_auth_flow_summary_from_finalize_step_when_output_is_missing() {
        let result = FastBrowserRunResult {
            state: Some(FastBrowserState {
                steps: HashMap::from([(
                    "finalize_selected_flow".to_string(),
                    FastBrowserStepState {
                        action: Some(json!({
                            "ok": true,
                            "value": {
                                "stage": "success",
                                "callback_complete": true,
                                "success": true,
                                "next_action": "complete",
                                "verified_account_email": "dev.49@astronlab.com",
                                "codex_session": {
                                    "auth_url": "https://auth.openai.com/authorize",
                                    "callback_port": "8765",
                                    "pid": "4321"
                                }
                            }
                        })),
                    },
                )]),
            }),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);
        let session = read_codex_rotate_auth_flow_session(&result).expect("session payload");

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
        assert_eq!(
            summary.verified_account_email.as_deref(),
            Some("dev.49@astronlab.com")
        );
        assert_eq!(session.callback_port, Some(8765));
        assert_eq!(session.pid, Some(4321));
    }

    #[test]
    fn reads_auth_flow_summary_from_recent_events_when_output_and_state_summary_are_missing() {
        let result = FastBrowserRunResult {
            recent_events: Some(vec![json!({
                "step_id": "finalize_flow_summary",
                "phase": "action",
                "status": "ok",
                "details": {
                    "result": {
                        "value": {
                            "stage": "add_phone",
                            "next_action": "skip_account",
                            "replay_reason": "add_phone",
                            "error_message": "OpenAI still requires phone setup before the Codex callback can complete.",
                            "callback_complete": false,
                            "success": false
                        }
                    }
                }
            })]),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);

        assert_eq!(summary.stage.as_deref(), Some("add_phone"));
        assert_eq!(summary.callback_complete, Some(false));
        assert_eq!(summary.success, Some(false));
        assert_eq!(summary.next_action.as_deref(), Some("skip_account"));
        assert_eq!(summary.replay_reason.as_deref(), Some("add_phone"));
    }

    #[test]
    fn hydrates_auth_flow_recent_events_from_observability_run_path() {
        let fixture_root = unique_temp_dir("codex-rotate-observability-recent-events");
        fs::create_dir_all(&fixture_root).expect("create fixture root");
        let run_path = fixture_root.join("run.json");
        fs::write(
            &run_path,
            serde_json::to_string(&json!({
                "final_url": "https://auth.openai.com/add-phone",
                "recent_events": [
                    {
                        "step_id": "finalize_flow_summary",
                        "phase": "action",
                        "status": "ok",
                        "details": {
                            "result": {
                                "value": {
                                    "stage": "add_phone",
                                    "next_action": "skip_account",
                                    "replay_reason": "add_phone",
                                    "error_message": "OpenAI still requires phone setup before the Codex callback can complete.",
                                    "callback_complete": false,
                                    "success": false
                                }
                            }
                        }
                    }
                ]
            }))
            .expect("serialize run payload"),
        )
        .expect("write run payload");

        let bridge_payload = json!({
            "result": {
                "ok": true,
                "status": "completed",
                "observability": {
                    "run_path": run_path,
                    "status_path": run_path,
                }
            }
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
        let summary = read_codex_rotate_auth_flow_summary(
            result.result.as_ref().expect("fast-browser result"),
        );

        assert_eq!(summary.stage.as_deref(), Some("add_phone"));
        assert_eq!(summary.next_action.as_deref(), Some("skip_account"));

        let _ = fs::remove_dir_all(&fixture_root);
    }

    #[test]
    fn reads_auth_flow_summary_from_device_auth_callback_metadata_when_output_is_missing() {
        let result = FastBrowserRunResult {
            final_url: Some("https://auth.openai.com/deviceauth/callback".to_string()),
            page: Some(json!({
                "url": "https://auth.openai.com/deviceauth/callback",
                "title": "Signed in to Codex",
                "text": "Signed in to Codex\nYou may now close this page"
            })),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(
            summary.current_url.as_deref(),
            Some("https://auth.openai.com/deviceauth/callback")
        );
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn reads_auth_flow_summary_from_camel_case_localhost_final_url_when_output_is_missing() {
        let bridge_payload = json!({
            "result": {
                "ok": true,
                "finalUrl": "http://localhost:1455/success",
                "page": {
                    "url": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
                    "title": "Signed in to Codex",
                    "text": "Signed in to Codex\nYou may now close this page"
                }
            }
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
        let summary = read_codex_rotate_auth_flow_summary(
            result.result.as_ref().expect("fast-browser result"),
        );

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(
            summary.current_url.as_deref(),
            Some("http://localhost:1455/success")
        );
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn hydrates_auth_flow_summary_from_snake_case_observability_run_path() {
        let fixture_root = unique_temp_dir("codex-rotate-observability-hydrate");
        fs::create_dir_all(&fixture_root).expect("create fixture root");
        let run_path = fixture_root.join("run.json");
        fs::write(
            &run_path,
            serde_json::to_string(&json!({
                "final_url": "http://localhost:1455/success",
                "page": {
                    "url": "http://localhost:1455/success",
                    "title": "Signed in to Codex",
                    "text": "Signed in to Codex\nYou may now close this page"
                }
            }))
            .expect("serialize run payload"),
        )
        .expect("write run payload");

        let bridge_payload = json!({
            "result": {
                "ok": true,
                "status": "completed",
                "observability": {
                    "run_path": run_path,
                    "status_path": run_path,
                }
            }
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(bridge_payload).expect("bridge payload should deserialize");
        let summary = read_codex_rotate_auth_flow_summary(
            result.result.as_ref().expect("fast-browser result"),
        );

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(
            summary.current_url.as_deref(),
            Some("http://localhost:1455/success")
        );
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));

        let _ = fs::remove_dir_all(&fixture_root);
    }

    #[test]
    fn reads_auth_flow_summary_from_success_copy_without_callback_url() {
        let result = FastBrowserRunResult {
            page: Some(json!({
                "url": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
                "title": "Signed in to Codex",
                "text": "Signed in to Codex\nYou may now close this page"
            })),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(
            summary.current_url.as_deref(),
            Some("https://auth.openai.com/sign-in-with-chatgpt/codex/consent")
        );
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn reads_auth_flow_summary_from_action_only_step_metadata_when_output_is_missing() {
        let result = FastBrowserRunResult {
            state: Some(FastBrowserState {
                steps: HashMap::from([(
                    "inspect_device_authorization_after_callback_code".to_string(),
                    FastBrowserStepState {
                        action: Some(json!({
                            "current_url": "https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example",
                            "headline": "Signed in to Codex You may now close this page",
                            "success": true
                        })),
                    },
                )]),
            }),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(
            summary.current_url.as_deref(),
            Some("https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example")
        );
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn callback_metadata_overrides_pessimistic_output_summary() {
        let result = FastBrowserRunResult {
            output: Some(json!({
                "success": false,
                "callback_complete": false,
                "next_action": "retry_attempt",
                "stage": "oauth_consent"
            })),
            state: Some(FastBrowserState {
                steps: HashMap::from([(
                    "inspect_device_authorization_after_callback_code".to_string(),
                    FastBrowserStepState {
                        action: Some(json!({
                            "current_url": "https://auth.openai.com/deviceauth/callback?code=ac_example&state=state_example",
                            "headline": "Signed in to Codex You may now close this page",
                            "success": true
                        })),
                    },
                )]),
            }),
            ..FastBrowserRunResult::default()
        };

        let summary = read_codex_rotate_auth_flow_summary(&result);

        assert_eq!(summary.stage.as_deref(), Some("success"));
        assert_eq!(summary.callback_complete, Some(true));
        assert_eq!(summary.success, Some(true));
        assert_eq!(summary.next_action.as_deref(), Some("complete"));
    }

    #[test]
    fn bridge_login_attempt_result_accepts_failed_runs_without_state_or_output() {
        let payload = json!({
            "result": {
                "ok": false,
                "status": "failed",
                "finalUrl": "https://auth.openai.com/about-you",
                "error": {
                    "message": "about-you-fields-not-found"
                }
            },
            "error_message": "about-you-fields-not-found"
        });

        let result: BridgeLoginAttemptResult =
            serde_json::from_value(payload).expect("bridge payload should deserialize");

        assert!(result.result.is_some());
        assert_eq!(
            result.error_message.as_deref(),
            Some("about-you-fields-not-found")
        );
        let summary = read_codex_rotate_auth_flow_summary(
            result
                .result
                .as_ref()
                .expect("normalized fast-browser result"),
        );
        assert_eq!(summary.next_action, None);
        assert_eq!(summary.error_message, None);
    }

    #[test]
    fn normalize_gmail_base_address_before_suffixing() {
        assert_eq!(
            normalize_gmail_base_email("Dev.User+17@gmail.com").unwrap(),
            "dev.user@gmail.com"
        );
    }

    #[test]
    fn compute_next_gmail_alias_suffix_fills_first_gap() {
        assert_eq!(
            compute_next_gmail_alias_suffix(
                "dev.user@gmail.com",
                vec![
                    "dev.user+1@gmail.com".to_string(),
                    "dev.user+7@gmail.com".to_string(),
                    "other@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            2
        );
        assert_eq!(
            compute_next_gmail_alias_suffix(
                "dev.user@gmail.com",
                vec![
                    "dev.user+1@gmail.com".to_string(),
                    "dev.user+2@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            3
        );
    }

    #[test]
    fn builds_and_normalizes_templated_families() {
        assert_eq!(
            normalize_base_email_family("Dev.{N}@HotspotPrime.com").unwrap(),
            "dev.{n}@hotspotprime.com"
        );
        assert_eq!(
            build_account_family_email("dev.{N}@hotspotprime.com", 7).unwrap(),
            "dev.7@hotspotprime.com"
        );
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@hotspotprime.com",
                vec![
                    "dev.1@hotspotprime.com".to_string(),
                    "dev.4@hotspotprime.com".to_string(),
                    "other@gmail.com".to_string(),
                ],
            )
            .unwrap(),
            2
        );
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@astronlab.com",
                vec!["dev.21@astronlab.com".to_string()],
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn compute_next_account_family_suffix_ignores_failed_skipped_slots() {
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@astronlab.com",
                vec![
                    "dev.1@astronlab.com".to_string(),
                    "dev.3@astronlab.com".to_string(),
                ],
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn compute_next_account_family_suffix_can_reserve_skipped_slots_under_cap() {
        assert_eq!(
            compute_next_account_family_suffix_with_skips(
                "dev.{N}@astronlab.com",
                vec![
                    "dev.1@astronlab.com".to_string(),
                    "dev.2@astronlab.com".to_string(),
                    "dev.3@astronlab.com".to_string(),
                ],
                vec![
                    "dev.4@astronlab.com".to_string(),
                    "dev.5@astronlab.com".to_string(),
                ],
                10,
            )
            .unwrap(),
            6
        );
    }

    #[test]
    fn compute_next_account_family_suffix_reserves_skipped_slots_at_skip_cap() {
        assert_eq!(
            compute_next_account_family_suffix_with_skips(
                "dev.{N}@astronlab.com",
                vec![
                    "dev.1@astronlab.com".to_string(),
                    "dev.2@astronlab.com".to_string(),
                    "dev.3@astronlab.com".to_string(),
                ],
                (4..=13)
                    .map(|suffix| format!("dev.{suffix}@astronlab.com"))
                    .collect(),
                10,
            )
            .unwrap(),
            14
        );
    }

    #[test]
    fn compute_fresh_account_family_suffix_respects_family_frontier_when_skips_exist() {
        let family = CredentialFamily {
            profile_name: "dev-1".to_string(),
            base_email: "devbench.{n}@astronlab.com".to_string(),
            next_suffix: 16,
            max_skipped_slots: 2,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-13T05:00:00.000Z".to_string(),
            last_created_email: None,
        };

        let next_suffix = compute_fresh_account_family_suffix(
            Some(&family),
            "devbench.{n}@astronlab.com",
            (1..=13)
                .map(|suffix| format!("devbench.{suffix}@astronlab.com"))
                .collect(),
            vec![
                "devbench.14@astronlab.com".to_string(),
                "devbench.15@astronlab.com".to_string(),
            ],
        )
        .expect("next suffix");

        assert_eq!(next_suffix, 16);
    }

    #[test]
    fn compute_fresh_account_family_suffix_preserves_gap_fill_without_skips() {
        let family = CredentialFamily {
            profile_name: "dev-1".to_string(),
            base_email: "dev.{n}@astronlab.com".to_string(),
            next_suffix: 99,
            max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
            created_at: "2026-04-13T05:00:00.000Z".to_string(),
            updated_at: "2026-04-13T05:00:00.000Z".to_string(),
            last_created_email: None,
        };

        let next_suffix = compute_fresh_account_family_suffix(
            Some(&family),
            "dev.{n}@astronlab.com",
            vec![
                "dev.1@astronlab.com".to_string(),
                "dev.3@astronlab.com".to_string(),
            ],
            Vec::new(),
        )
        .expect("next suffix");

        assert_eq!(next_suffix, 2);
    }

    #[test]
    fn prepare_next_auto_create_attempt_preserves_current_skip_when_budget_is_full() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "devbench.14@astronlab.com".to_string(),
            make_pending(
                "devbench.14@astronlab.com",
                "dev-1",
                "devbench.{n}@astronlab.com",
                14,
                "2026-04-13T06:00:00.000Z",
            ),
        );
        store
            .skipped
            .extend(["devbench.12@astronlab.com", "devbench.13@astronlab.com"].map(str::to_string));
        store.families.insert(
            "dev-1::devbench.{n}@astronlab.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "devbench.{n}@astronlab.com".to_string(),
                next_suffix: 14,
                max_skipped_slots: 2,
                created_at: "2026-04-13T05:00:00.000Z".to_string(),
                updated_at: "2026-04-13T05:00:00.000Z".to_string(),
                last_created_email: None,
            },
        );

        prepare_next_auto_create_attempt(
            &mut store,
            "dev-1::devbench.{n}@astronlab.com",
            "dev-1",
            "devbench.{n}@astronlab.com",
            14,
            "devbench.14@astronlab.com",
            "2026-04-13T06:00:00.000Z",
        )
        .expect("prepare next attempt");

        assert!(!store.pending.contains_key("devbench.14@astronlab.com"));
        assert!(store.skipped.contains("devbench.14@astronlab.com"));
        assert!(!store.skipped.contains("devbench.12@astronlab.com"));
        assert!(store.skipped.contains("devbench.13@astronlab.com"));

        let next_suffix = compute_next_account_family_suffix_with_skips(
            "devbench.{n}@astronlab.com",
            (1..=13)
                .map(|suffix| format!("devbench.{suffix}@astronlab.com"))
                .collect(),
            collect_skipped_account_emails_for_family(
                &store,
                "dev-1",
                "devbench.{n}@astronlab.com",
            ),
            max_skipped_slots_for_family(store.families.get("dev-1::devbench.{n}@astronlab.com")),
        )
        .expect("next suffix");

        assert_eq!(next_suffix, 15);
    }

    #[test]
    fn compute_next_account_family_suffix_fills_missing_dev_slots_before_frontier() {
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@astronlab.com",
                vec![
                    "dev.45@astronlab.com".to_string(),
                    "dev.47@astronlab.com".to_string(),
                    "dev.48@astronlab.com".to_string(),
                    "dev.58@astronlab.com".to_string(),
                ],
            )
            .unwrap(),
            1
        );
        assert_eq!(
            compute_next_account_family_suffix(
                "dev.{N}@astronlab.com",
                (1..=58)
                    .filter(|suffix| *suffix != 46)
                    .map(|suffix| format!("dev.{suffix}@astronlab.com"))
                    .collect(),
            )
            .unwrap(),
            46
        );
    }

    #[test]
    fn create_always_prefers_signup_recovery() {
        assert!(prefer_signup_recovery_for_create(false));
        assert!(prefer_signup_recovery_for_create(true));
    }

    #[test]
    fn create_execution_lock_blocks_other_process_and_records_metadata() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        with_rotate_home("codex-rotate-create-lock", |_| {
            let options = CreateCommandOptions {
                alias: Some("dev-1".to_string()),
                profile_name: Some("dev-1".to_string()),
                base_email: Some("dev.{n}@astronlab.com".to_string()),
                force: true,
                ignore_current: true,
                require_usable_quota: true,
                source: CreateCommandSource::Manual,
                ..CreateCommandOptions::default()
            };
            let lock = acquire_create_execution_lock(&options, None).expect("acquire create lock");
            let lock_path = create_lock_path().expect("create lock path");
            let metadata = read_create_execution_lock_metadata(&lock_path).expect("lock metadata");
            assert_eq!(metadata.pid, std::process::id());
            assert_eq!(metadata.source, "manual");
            assert_eq!(metadata.profile_name.as_deref(), Some("dev-1"));
            assert_eq!(
                metadata.base_email.as_deref(),
                Some("dev.{n}@astronlab.com")
            );
            assert_eq!(metadata.alias.as_deref(), Some("dev-1"));

            let output = ProcessCommand::new("ruby")
                .arg("-e")
                .arg(
                    r#"
path = ARGV[0]
File.open(path, File::RDWR | File::CREAT, 0o644) do |file|
  locked = file.flock(File::LOCK_EX | File::LOCK_NB)
  exit(locked ? 7 : 0)
end
"#,
                )
                .arg(&lock_path)
                .output()
                .expect("run ruby flock probe");
            assert_eq!(
                output.status.code(),
                Some(0),
                "second process unexpectedly acquired create lock: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );

            drop(lock);

            assert!(
                !lock_path.exists(),
                "create lock file should be removed after the holder drops"
            );

            let output = ProcessCommand::new("ruby")
                .arg("-e")
                .arg(
                    r#"
path = ARGV[0]
File.open(path, File::RDWR | File::CREAT, 0o644) do |file|
  locked = file.flock(File::LOCK_EX | File::LOCK_NB)
  exit(locked ? 0 : 9)
end
"#,
                )
                .arg(&lock_path)
                .output()
                .expect("run ruby flock release probe");
            assert_eq!(
                output.status.code(),
                Some(0),
                "second process could not acquire released create lock: stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        });
    }

    #[test]
    fn create_execution_lock_waits_for_same_process_release() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        with_rotate_home("codex-rotate-create-lock-wait", |_| {
            let options = CreateCommandOptions {
                profile_name: Some("dev-1".to_string()),
                base_email: Some("dev.{n}@astronlab.com".to_string()),
                force: true,
                source: CreateCommandSource::Manual,
                ..CreateCommandOptions::default()
            };
            let first_lock =
                acquire_create_execution_lock(&options, None).expect("acquire first create lock");
            let (tx, rx) = mpsc::channel();
            let options_clone = options.clone();
            let waiter = thread::spawn(move || {
                let acquired = acquire_create_execution_lock(&options_clone, None).is_ok();
                tx.send(acquired).expect("send wait result");
            });

            assert!(rx.recv_timeout(Duration::from_millis(250)).is_err());
            drop(first_lock);
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), true);
            waiter.join().expect("join waiter");
        });
    }

    #[test]
    fn create_family_hint_accepts_templates_and_rejects_gmail() {
        assert!(should_use_default_create_family_hint(Some(
            "dev.{n}@astronlab.com"
        )));
        assert!(should_use_default_create_family_hint(Some(
            "qa.{n}@astronlab.com"
        )));
        assert!(!should_use_default_create_family_hint(Some(
            "dev.user@gmail.com"
        )));
        assert!(!should_use_default_create_family_hint(None));
    }

    #[test]
    fn resolve_managed_profile_name_from_candidates_matches_requested_preferred_and_default() {
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                Some("dev-1"),
                None,
                None,
                Some("other"),
            )
            .unwrap(),
            "dev-1"
        );
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                None,
                Some("other"),
                Some("workflow"),
                Some("dev-1"),
            )
            .unwrap(),
            "other"
        );
        assert_eq!(
            resolve_managed_profile_name_from_candidates(
                &["dev-1", "other"],
                None,
                None,
                None,
                Some("other"),
            )
            .unwrap(),
            "other"
        );
    }

    #[test]
    fn resolve_create_base_email_prefers_requested_then_discovered_then_default() {
        assert_eq!(
            resolve_create_base_email(Some("other@gmail.com"), Some("dev.user@gmail.com")).unwrap(),
            "other@gmail.com"
        );
        assert_eq!(
            resolve_create_base_email(None, Some("Dev.User+4@gmail.com")).unwrap(),
            "dev.user@gmail.com"
        );
        assert_eq!(
            resolve_create_base_email(None, None).unwrap(),
            "dev.{n}@astronlab.com"
        );
    }

    #[test]
    fn score_email_prefers_exact_profile_token_match() {
        assert!(
            score_email_for_managed_profile_name("dev-1", "1.dev.astronlab@gmail.com")
                > score_email_for_managed_profile_name("dev-1", "dev.2.astronlab@gmail.com")
        );
    }

    #[test]
    fn select_best_email_and_system_chrome_profile_match() {
        assert_eq!(
            select_best_email_for_managed_profile(
                "dev-1",
                vec![
                    "other@gmail.com".to_string(),
                    "1.dev.astronlab@gmail.com".to_string(),
                ],
                None,
            )
            .as_deref(),
            Some("1.dev.astronlab@gmail.com")
        );

        let match_result = select_best_system_chrome_profile_match(
            "dev-1",
            &[
                SystemChromeProfileCandidate {
                    directory: "Profile 1".to_string(),
                    name: "Personal".to_string(),
                    emails: vec!["other@gmail.com".to_string()],
                },
                SystemChromeProfileCandidate {
                    directory: "Profile 2".to_string(),
                    name: "Dev".to_string(),
                    emails: vec!["1.dev.astronlab@gmail.com".to_string()],
                },
            ],
            None,
        )
        .expect("profile match");
        assert_eq!(match_result.directory, "Profile 2");
        assert_eq!(match_result.matched_email, "1.dev.astronlab@gmail.com");
    }

    #[test]
    fn stored_relogin_honors_logout_setting() {
        assert!(should_logout_before_stored_relogin(
            &ReloginOptions::default()
        ));
        assert!(!should_logout_before_stored_relogin(&ReloginOptions {
            logout_first: false,
            ..ReloginOptions::default()
        }));
    }

    #[test]
    fn auto_create_retry_gate_only_applies_to_next_with_quota_requirement() {
        assert!(should_retry_create_until_usable(&CreateCommandOptions {
            require_usable_quota: true,
            source: CreateCommandSource::Next,
            ..CreateCommandOptions::default()
        }));
        assert!(!should_retry_create_until_usable(&CreateCommandOptions {
            require_usable_quota: false,
            source: CreateCommandSource::Next,
            ..CreateCommandOptions::default()
        }));
        assert!(!should_retry_create_until_usable(&CreateCommandOptions {
            require_usable_quota: true,
            source: CreateCommandSource::Manual,
            ..CreateCommandOptions::default()
        }));
    }

    #[test]
    fn workflow_skip_account_errors_are_retried_for_create() {
        let error = anyhow::Error::new(WorkflowSkipAccountError::new(
            "skip this account".to_string(),
        ));
        assert!(is_workflow_skip_account_error(&error));
        assert!(should_retry_create_after_error(
            &CreateCommandOptions::default(),
            &error
        ));
    }

    #[test]
    fn unrelated_create_errors_do_not_trigger_policy_skip() {
        let error = anyhow!("Codex browser login did not reach the callback.");
        assert!(!is_workflow_skip_account_error(&error));
        assert!(!should_retry_create_after_error(
            &CreateCommandOptions::default(),
            &error
        ));
    }

    #[test]
    fn reusable_account_retry_stop_only_applies_to_next_source() {
        assert!(should_stop_create_retry_for_reusable_account(
            &CreateCommandOptions {
                source: CreateCommandSource::Next,
                ..CreateCommandOptions::default()
            }
        ));
        assert!(!should_stop_create_retry_for_reusable_account(
            &CreateCommandOptions {
                source: CreateCommandSource::Manual,
                force: true,
                ..CreateCommandOptions::default()
            }
        ));
    }

    #[test]
    fn reads_default_birth_date_from_workflow_metadata() {
        let metadata = read_workflow_file_metadata(
            &repo_root()
                .join(".fast-browser")
                .join("workflows")
                .join("web")
                .join("auth.openai.com")
                .join("codex-rotate-account-flow-main.yaml"),
        )
        .expect("workflow metadata");
        let value = metadata.default_birth_date().expect("default birth date");
        assert_eq!(value.birth_month, 1);
        assert_eq!(value.birth_day, 24);
        assert_eq!(value.birth_year, 1990);
    }

    #[test]
    fn normalize_credential_store_sets_v4_default_create_family() {
        let store = normalize_credential_store(json!({}));
        assert_eq!(store.version, 4);
        assert_eq!(store.default_create_base_email, "dev.{n}@astronlab.com");
    }

    #[test]
    fn normalize_credential_store_reads_skipped_emails() {
        let store = normalize_credential_store(json!({
            "skipped": [
                "dev.91@astronlab.com",
                "dev.92@astronlab.com"
            ]
        }));

        assert!(store.skipped.contains("dev.91@astronlab.com"));
        assert!(store.skipped.contains("dev.92@astronlab.com"));
    }

    #[test]
    fn normalize_credential_store_defaults_family_skip_cap() {
        let store = normalize_credential_store(json!({
            "families": {
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "next_suffix": 23,
                    "created_at": "2026-04-05T00:00:00.000Z",
                    "updated_at": "2026-04-05T00:00:00.000Z",
                    "last_created_email": "dev.22@astronlab.com"
                }
            }
        }));

        assert_eq!(
            store
                .families
                .get("dev-1::dev.{n}@astronlab.com")
                .map(|family| family.max_skipped_slots),
            Some(DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY)
        );
    }

    #[test]
    fn select_pending_base_email_hint_prefers_dev_template_family() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T17:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.30@astronlab.com".to_string(),
            make_pending(
                "dev.30@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                30,
                "2026-04-06T16:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_base_email_hint_for_profile(&store, "dev-1", None).as_deref(),
            Some("dev.{n}@astronlab.com")
        );
    }

    #[test]
    fn select_pending_credential_for_family_drains_lowest_suffix_first() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.user+1@gmail.com".to_string(),
            make_pending(
                "dev.user+1@gmail.com",
                "dev-1",
                "dev.user@gmail.com",
                1,
                "2026-03-20T00:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.user+3@gmail.com".to_string(),
            make_pending(
                "dev.user+3@gmail.com",
                "dev-1",
                "dev.user@gmail.com",
                3,
                "2026-03-20T03:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_credential_for_family(&store, "dev-1", "dev.user@gmail.com", None)
                .map(|entry| entry.stored.email),
            Some("dev.user+1@gmail.com".to_string())
        );
    }

    #[test]
    fn select_pending_credential_for_family_can_filter_by_alias() {
        let mut store = CredentialStore::default();
        let mut left = make_pending(
            "dev.user+2@gmail.com",
            "dev-1",
            "dev.user@gmail.com",
            2,
            "2026-03-20T02:00:00.000Z",
        );
        left.stored.alias = Some("team-a".to_string());
        let mut right = make_pending(
            "dev.user+3@gmail.com",
            "dev-1",
            "dev.user@gmail.com",
            3,
            "2026-03-20T03:00:00.000Z",
        );
        right.stored.alias = Some("team-b".to_string());
        store.pending.insert(left.stored.email.clone(), left);
        store.pending.insert(right.stored.email.clone(), right);

        assert_eq!(
            select_pending_credential_for_family(
                &store,
                "dev-1",
                "dev.user@gmail.com",
                Some("team-a"),
            )
            .map(|entry| entry.stored.email),
            Some("dev.user+2@gmail.com".to_string())
        );
    }

    #[test]
    fn select_pending_base_email_hint_prefers_oldest_family_when_rank_is_equal() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "1.dev.astronlab+1@gmail.com".to_string(),
            make_pending(
                "1.dev.astronlab+1@gmail.com",
                "dev-1",
                "1.dev.astronlab@gmail.com",
                1,
                "2026-03-20T00:00:00.000Z",
            ),
        );
        store.pending.insert(
            "arjuda.anjum+1@gmail.com".to_string(),
            make_pending(
                "arjuda.anjum+1@gmail.com",
                "dev-1",
                "arjuda.anjum@gmail.com",
                1,
                "2026-03-21T00:00:00.000Z",
            ),
        );

        assert_eq!(
            select_pending_base_email_hint_for_profile(&store, "dev-1", None).as_deref(),
            Some("1.dev.astronlab@gmail.com")
        );
    }

    #[test]
    fn select_stored_base_email_hint_prefers_common_and_high_frontier_template() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::qa.{n}@astronlab.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "qa.{n}@astronlab.com".to_string(),
                next_suffix: 300,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-04-06T16:00:00.000Z".to_string(),
                updated_at: "2026-04-06T16:00:00.000Z".to_string(),
                last_created_email: Some("qa.299@astronlab.com".to_string()),
            },
        );
        store.pending.insert(
            "dev.35@astronlab.com".to_string(),
            make_pending(
                "dev.35@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                35,
                "2026-04-06T17:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.36@astronlab.com".to_string(),
            make_pending(
                "dev.36@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                36,
                "2026-04-06T18:00:00.000Z",
            ),
        );

        assert_eq!(
            select_stored_base_email_hint(&store, "dev-1").as_deref(),
            Some("dev.{n}@astronlab.com")
        );

        let mut store = CredentialStore::default();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T18:00:00.000Z",
            ),
        );
        store.pending.insert(
            "dev.35@astronlab.com".to_string(),
            make_pending(
                "dev.35@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                35,
                "2026-04-06T17:00:00.000Z",
            ),
        );

        assert_eq!(
            select_stored_base_email_hint(&store, "dev-1").as_deref(),
            Some("dev.{n}@astronlab.com")
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_uses_pending_hint_before_new_default_family() {
        let mut store = CredentialStore::default();
        store.default_create_base_email = "qa.{n}@astronlab.com".to_string();
        store.pending.insert(
            "qa.300@astronlab.com".to_string(),
            make_pending(
                "qa.300@astronlab.com",
                "dev-1",
                "qa.{n}@astronlab.com",
                300,
                "2026-04-06T17:00:00.000Z",
            ),
        );

        assert_eq!(
            resolve_create_base_email_for_profile(&store, "dev-1", None, None).unwrap(),
            "qa.{n}@astronlab.com"
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_falls_back_to_dev_default_for_new_creates() {
        let mut store = CredentialStore::default();
        store.default_create_base_email = "qa.{n}@astronlab.com".to_string();

        assert_eq!(
            resolve_create_base_email_for_profile(&store, "dev-1", None, None).unwrap(),
            "dev.{n}@astronlab.com"
        );
    }

    #[test]
    fn resolve_create_base_email_for_profile_respects_explicit_override() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.30@astronlab.com".to_string(),
            make_pending(
                "dev.30@astronlab.com",
                "dev-1",
                "dev.{n}@astronlab.com",
                30,
                "2026-04-06T16:00:00.000Z",
            ),
        );

        assert_eq!(
            resolve_create_base_email_for_profile(
                &store,
                "dev-1",
                Some("qa.{n}@astronlab.com"),
                None,
            )
            .unwrap(),
            "qa.{n}@astronlab.com"
        );
    }

    #[test]
    fn normalize_credential_store_drops_legacy_bench_families_on_v4_migration() {
        let store = normalize_credential_store(json!({
            "version": 3,
            "families": {
                "dev-1::bench.devicefix.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "base_email": "bench.devicefix.{n}@astronlab.com",
                    "next_suffix": 8,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z",
                    "last_created_email": "bench.devicefix.7@astronlab.com"
                },
                "dev-1::dev.{n}@astronlab.com": {
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "next_suffix": 35,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z",
                    "last_created_email": "dev.34@astronlab.com"
                }
            },
            "pending": {
                "bench.devicefix.8@astronlab.com": {
                    "email": "bench.devicefix.8@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "bench.devicefix.{n}@astronlab.com",
                    "suffix": 8,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z"
                },
                "dev.35@astronlab.com": {
                    "email": "dev.35@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 35,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T00:00:00.000Z",
                    "updated_at": "2026-04-06T00:00:00.000Z"
                }
            }
        }));
        assert_eq!(store.version, 4);
        assert_eq!(store.default_create_base_email, "dev.{n}@astronlab.com");
        assert!(store.families.contains_key("dev-1::dev.{n}@astronlab.com"));
        assert!(!store
            .families
            .contains_key("dev-1::bench.devicefix.{n}@astronlab.com"));
        assert!(store.pending.contains_key("dev.35@astronlab.com"));
        assert!(!store
            .pending
            .contains_key("bench.devicefix.8@astronlab.com"));
    }

    #[test]
    fn normalize_credential_store_drops_non_dev_pending_even_in_v4_state() {
        let store = normalize_credential_store(json!({
            "version": 4,
            "pending": {
                "qa.300@astronlab.com": {
                    "email": "qa.300@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "qa.{n}@astronlab.com",
                    "suffix": 300,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T17:00:00.000Z",
                    "updated_at": "2026-04-06T17:00:00.000Z",
                    "started_at": "2026-04-06T17:00:00.000Z"
                },
                "dev.user+1@gmail.com": {
                    "email": "dev.user+1@gmail.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.user@gmail.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T18:00:00.000Z",
                    "updated_at": "2026-04-06T18:00:00.000Z",
                    "started_at": "2026-04-06T18:00:00.000Z"
                },
                "dev.35@astronlab.com": {
                    "email": "dev.35@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 35,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-06T19:00:00.000Z",
                    "updated_at": "2026-04-06T19:00:00.000Z",
                    "started_at": "2026-04-06T19:00:00.000Z"
                }
            }
        }));
        assert_eq!(
            store.pending.keys().cloned().collect::<Vec<_>>(),
            vec!["dev.35@astronlab.com".to_string()]
        );
    }

    #[test]
    fn reuses_existing_birth_date() {
        let value = resolve_credential_birth_date(
            Some(&StoredCredential {
                email: "dev.user+1@gmail.com".to_string(),
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                suffix: 1,
                selector: None,
                alias: None,
                birth_month: Some(7),
                birth_day: Some(14),
                birth_year: Some(1994),
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T00:00:00.000Z".to_string(),
            }),
            Some(&AdultBirthDate {
                birth_month: 1,
                birth_day: 24,
                birth_year: 1990,
            }),
        )
        .expect("birth date");
        assert_eq!(value.birth_month, 7);
        assert_eq!(value.birth_day, 14);
        assert_eq!(value.birth_year, 1994);
    }

    #[test]
    fn drops_legacy_secret_fields_from_loaded_records() {
        let store = normalize_credential_store(json!({
            "accounts": {
                "dev.user+1@gmail.com": {
                    "email": "dev.user+1@gmail.com",
                    "password": "pw-1",
                    "account_secret_ref": {
                        "type": "secret_ref",
                        "store": "bitwarden-cli",
                        "object_id": "bw-1"
                    },
                    "profile_name": "dev-1",
                    "base_email": "dev.user@gmail.com",
                    "suffix": 1,
                    "selector": "dev.user+1@gmail.com_free",
                    "alias": null,
                    "created_at": "2026-03-20T00:00:00.000Z",
                    "updated_at": "2026-03-20T00:00:00.000Z"
                }
            }
        }));
        let family = store.families.get("dev-1::dev.user@gmail.com").unwrap();
        assert_eq!(family.profile_name, "dev-1");
        assert_eq!(family.base_email, "dev.user@gmail.com");
        assert_eq!(family.next_suffix, 2);
        assert_eq!(
            family.last_created_email.as_deref(),
            Some("dev.user+1@gmail.com")
        );
    }

    #[test]
    fn drops_pending_entries_that_already_exist_in_inventory() {
        let store = normalize_credential_store(json!({
            "accounts": [
                {
                    "email": "dev.1@astronlab.com"
                }
            ],
            "pending": {
                "dev.1@astronlab.com": {
                    "email": "dev.1@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-05T04:50:10.406Z",
                    "updated_at": "2026-04-05T05:39:48.882Z"
                }
            }
        }));
        assert!(store.pending.is_empty());
    }

    #[test]
    fn keeps_pending_entries_for_missing_lower_suffixes() {
        let store = normalize_credential_store(json!({
            "accounts": [
                {
                    "email": "dev.23@astronlab.com"
                }
            ],
            "pending": {
                "dev.1@astronlab.com": {
                    "email": "dev.1@astronlab.com",
                    "profile_name": "dev-1",
                    "base_email": "dev.{n}@astronlab.com",
                    "suffix": 1,
                    "selector": null,
                    "alias": null,
                    "created_at": "2026-04-05T04:50:10.406Z",
                    "updated_at": "2026-04-05T05:39:48.882Z"
                }
            }
        }));
        assert!(store.pending.contains_key("dev.1@astronlab.com"));
    }

    #[test]
    fn builds_openai_login_locator_from_email() {
        let locator = build_openai_account_login_locator("Dev.User+1@gmail.com");
        match locator {
            CodexRotateSecretLocator::LoginLookup {
                store,
                username,
                uris,
                field_path,
            } => {
                assert_eq!(store, "bitwarden-cli");
                assert_eq!(username, "dev.user+1@gmail.com");
                assert_eq!(field_path, "/password");
                assert_eq!(
                    uris,
                    vec![
                        "https://auth.openai.com".to_string(),
                        "https://chatgpt.com".to_string()
                    ]
                );
            }
        }
    }

    #[test]
    fn codex_login_retry_policy_recognizes_verification_waits() {
        assert!(is_retryable_codex_login_workflow_error_message(
            "signup-verification-code-missing"
        ));
        assert!(is_retryable_codex_login_workflow_error_message(
            "login-verification-submit-stuck:email_verification:https://auth.openai.com/email-verification"
        ));
        assert!(!is_retryable_codex_login_workflow_error_message(
            "OpenAI rejected the stored password"
        ));
        assert!(!is_retryable_codex_login_workflow_error_message(
            "device auth failed with status 429"
        ));
    }

    #[test]
    fn codex_login_retry_policy_uses_expected_delay_tables() {
        assert_eq!(
            codex_login_retry_delay_ms(Some("verification_artifact_pending"), 1),
            5_000
        );
        assert_eq!(
            codex_login_retry_delay_ms(Some("verification_artifact_pending"), 2),
            10_000
        );
        assert_eq!(
            codex_login_retry_delay_ms(Some("device_auth_rate_limit"), 1),
            30_000
        );
        assert_eq!(
            codex_login_retry_delay_ms(Some("device_auth_rate_limit"), 2),
            60_000
        );
    }

    #[test]
    fn codex_login_retry_policy_keeps_reusable_device_auth_session_after_post_issue_429() {
        assert!(!should_reset_device_auth_session_for_rate_limit(
            "Error logging in with device code: device auth failed with status 429 Too Many Requests",
            Some(&CodexRotateAuthFlowSession {
                auth_url: Some("https://auth.openai.com/codex/device".to_string()),
                device_code: Some("ABCD-12345".to_string()),
                ..CodexRotateAuthFlowSession::default()
            })
        ));
    }

    #[test]
    fn codex_login_retry_policy_drops_unissued_device_auth_session_after_429() {
        assert!(should_reset_device_auth_session_for_rate_limit(
            "Error logging in with device code: device code request failed with status 429 Too Many Requests",
            Some(&CodexRotateAuthFlowSession::default())
        ));
    }

    #[test]
    fn codex_login_retry_policy_resets_expected_sessions() {
        assert!(!should_reset_codex_login_session_for_retry(
            Some("retryable_timeout"),
            1
        ));
        assert!(should_reset_codex_login_session_for_retry(
            Some("retryable_timeout"),
            2
        ));
        assert!(should_reset_codex_login_session_for_retry(
            Some("state_mismatch"),
            1
        ));
        assert!(should_reset_codex_login_session_for_retry(
            Some("state_mismatch"),
            2
        ));
        assert!(should_reset_codex_login_session_for_retry(
            Some("final_add_phone"),
            1
        ));
    }

    #[test]
    fn codex_login_retry_policy_extends_final_add_phone_budget() {
        assert_eq!(
            codex_login_max_attempts(None),
            DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
        );
        assert_eq!(
            codex_login_max_attempts(Some("retryable_timeout")),
            DEFAULT_CODEX_LOGIN_MAX_ATTEMPTS
        );
        assert_eq!(
            codex_login_max_attempts(Some("final_add_phone")),
            FINAL_ADD_PHONE_CODEX_LOGIN_MAX_ATTEMPTS
        );
    }

    #[test]
    fn codex_login_retry_policy_skips_account_after_final_add_phone_budget() {
        assert!(!should_skip_account_after_retry_exhaustion(None));
        assert!(!should_skip_account_after_retry_exhaustion(Some(
            "retryable_timeout"
        )));
        assert!(should_skip_account_after_retry_exhaustion(Some(
            "final_add_phone"
        )));
    }

    #[test]
    fn relogin_family_match_prefers_exact_last_created_email() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 4,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T01:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+3@gmail.com".to_string()),
            },
        );
        store.families.insert(
            "dev-2::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-2".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 5,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T02:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+2@gmail.com".to_string()),
            },
        );

        let match_result = select_family_for_account_email(&store, "dev.user+2@gmail.com")
            .expect("expected exact family match");
        assert_eq!(match_result.family.profile_name, "dev-2");
        assert_eq!(match_result.suffix, 2);
    }

    #[test]
    fn relogin_family_match_refuses_ambiguous_non_exact_matches() {
        let mut store = CredentialStore::default();
        store.families.insert(
            "dev-1::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-1".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 4,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T01:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+3@gmail.com".to_string()),
            },
        );
        store.families.insert(
            "dev-2::dev.user@gmail.com".to_string(),
            CredentialFamily {
                profile_name: "dev-2".to_string(),
                base_email: "dev.user@gmail.com".to_string(),
                next_suffix: 5,
                max_skipped_slots: DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY,
                created_at: "2026-03-20T00:00:00.000Z".to_string(),
                updated_at: "2026-03-20T02:00:00.000Z".to_string(),
                last_created_email: Some("dev.user+4@gmail.com".to_string()),
            },
        );

        assert!(select_family_for_account_email(&store, "dev.user+2@gmail.com").is_none());
    }

    #[test]
    fn add_reconciliation_moves_matching_pending_into_family_state() {
        let mut store = CredentialStore::default();
        store.pending.insert(
            "dev.24@astronlab.com".to_string(),
            PendingCredential {
                stored: StoredCredential {
                    email: "dev.24@astronlab.com".to_string(),
                    profile_name: "dev-1".to_string(),
                    base_email: "dev.{n}@astronlab.com".to_string(),
                    suffix: 24,
                    selector: None,
                    alias: None,
                    birth_month: Some(1),
                    birth_day: Some(24),
                    birth_year: Some(1990),
                    created_at: "2026-04-05T05:51:09.049Z".to_string(),
                    updated_at: "2026-04-05T05:51:09.049Z".to_string(),
                },
                started_at: Some("2026-04-05T05:51:09.049Z".to_string()),
            },
        );
        let entry = AccountEntry {
            label: "dev.24@astronlab.com_free".to_string(),
            alias: None,
            email: "dev.24@astronlab.com".to_string(),
            account_id: "acct-24".to_string(),
            plan_type: "free".to_string(),
            auth: CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: crate::auth::AuthTokens {
                    id_token: "id".to_string(),
                    access_token: "access".to_string(),
                    refresh_token: Some("refresh".to_string()),
                    account_id: "acct-24".to_string(),
                },
                last_refresh: "2026-04-05T05:51:09.049Z".to_string(),
            },
            added_at: "2026-04-05T05:51:09.049Z".to_string(),
            last_quota_usable: None,
            last_quota_summary: None,
            last_quota_blocker: None,
            last_quota_checked_at: None,
            last_quota_primary_left_percent: None,
            last_quota_next_refresh_at: None,
        };

        let pending = store.pending.remove("dev.24@astronlab.com").unwrap();
        assert!(upsert_family_for_account(
            &mut store,
            &StoredCredential {
                email: entry.email.clone(),
                profile_name: pending.stored.profile_name,
                base_email: pending.stored.base_email,
                suffix: pending.stored.suffix,
                selector: Some(entry.label.clone()),
                alias: entry.alias.clone(),
                birth_month: pending.stored.birth_month,
                birth_day: pending.stored.birth_day,
                birth_year: pending.stored.birth_year,
                created_at: pending.stored.created_at,
                updated_at: "2026-04-05T05:52:00.000Z".to_string(),
            },
        ));
        let family = store.families.get("dev-1::dev.{n}@astronlab.com").unwrap();
        assert_eq!(family.next_suffix, 25);
        assert_eq!(
            family.last_created_email.as_deref(),
            Some("dev.24@astronlab.com")
        );
        assert!(store.pending.is_empty());
    }

    #[test]
    fn created_pool_lookup_prefers_expected_email_over_stale_account_id_match() {
        let pool = Pool {
            active_index: 0,
            accounts: vec![
                AccountEntry {
                    label: "dev.98@astronlab.com_free".to_string(),
                    alias: None,
                    email: "dev.98@astronlab.com".to_string(),
                    account_id: "acct-shared".to_string(),
                    plan_type: "free".to_string(),
                    auth: CodexAuth {
                        auth_mode: "chatgpt".to_string(),
                        openai_api_key: None,
                        tokens: crate::auth::AuthTokens {
                            id_token: "id-old".to_string(),
                            access_token: "access-old".to_string(),
                            refresh_token: Some("refresh-old".to_string()),
                            account_id: "acct-shared".to_string(),
                        },
                        last_refresh: "2026-04-13T02:52:14.756Z".to_string(),
                    },
                    added_at: "2026-04-13T02:52:14.756Z".to_string(),
                    last_quota_usable: None,
                    last_quota_summary: None,
                    last_quota_blocker: None,
                    last_quota_checked_at: None,
                    last_quota_primary_left_percent: None,
                    last_quota_next_refresh_at: None,
                },
                AccountEntry {
                    label: "devbench.17@astronlab.com_free".to_string(),
                    alias: None,
                    email: "devbench.17@astronlab.com".to_string(),
                    account_id: "acct-devbench-17".to_string(),
                    plan_type: "free".to_string(),
                    auth: CodexAuth {
                        auth_mode: "chatgpt".to_string(),
                        openai_api_key: None,
                        tokens: crate::auth::AuthTokens {
                            id_token: "id-new".to_string(),
                            access_token: "access-new".to_string(),
                            refresh_token: Some("refresh-new".to_string()),
                            account_id: "acct-devbench-17".to_string(),
                        },
                        last_refresh: "2026-04-13T02:52:15.012Z".to_string(),
                    },
                    added_at: "2026-04-13T02:52:15.012Z".to_string(),
                    last_quota_usable: None,
                    last_quota_summary: None,
                    last_quota_blocker: None,
                    last_quota_checked_at: None,
                    last_quota_primary_left_percent: None,
                    last_quota_next_refresh_at: None,
                },
            ],
        };

        assert_eq!(
            find_created_pool_entry_index(&pool, "acct-shared", "devbench.17@astronlab.com",),
            Some(1)
        );
    }

    #[test]
    fn completed_login_prefers_session_auth_file_over_default_auth_home() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        with_rotate_home("codex-rotate-session-auth-preferred", |rotate_home| {
            let codex_home = rotate_home.join("codex-home");
            let detached_home = rotate_home.join("detached-codex-home");
            fs::create_dir_all(&codex_home).expect("create codex home");
            fs::create_dir_all(&detached_home).expect("create detached codex home");
            let previous_codex_home = std::env::var_os("CODEX_HOME");
            unsafe {
                std::env::set_var("CODEX_HOME", &codex_home);
            }

            let result = (|| -> Result<()> {
                let shared_auth_path = codex_home.join("auth.json");
                let detached_auth_path = detached_home.join("auth.json");
                write_codex_auth(
                    &shared_auth_path,
                    &make_auth("dev.98@astronlab.com", "acct-98"),
                )?;
                write_codex_auth(
                    &detached_auth_path,
                    &make_auth("devbench.17@astronlab.com", "acct-devbench-17"),
                )?;

                let auth = load_auth_for_completed_login(&CompleteCodexLoginOutcome {
                    codex_session: Some(CodexRotateAuthFlowSession {
                        auth_file_path: Some(detached_auth_path.display().to_string()),
                        ..CodexRotateAuthFlowSession::default()
                    }),
                    ..CompleteCodexLoginOutcome::default()
                })?;

                assert_eq!(
                    summarize_codex_auth(&auth).email,
                    "devbench.17@astronlab.com"
                );
                assert_eq!(extract_account_id_from_auth(&auth), "acct-devbench-17");
                Ok(())
            })();

            match previous_codex_home {
                Some(value) => unsafe {
                    std::env::set_var("CODEX_HOME", value);
                },
                None => unsafe {
                    std::env::remove_var("CODEX_HOME");
                },
            }

            result.expect("session auth should override default auth");
        });
    }

    #[test]
    fn migrates_legacy_credential_store_into_accounts_json() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let rotate_home = unique_temp_dir("codex-rotate-legacy-store");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let accounts_path = rotate_home.join("accounts.json");
        let legacy_path = rotate_home.join("credentials.json");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = (|| -> Result<()> {
            fs::write(
                &accounts_path,
                serde_json::json!({
                    "active_index": 2,
                    "accounts": [{ "email": "dev.22@astronlab.com" }],
                })
                .to_string(),
            )?;
            fs::write(
                &legacy_path,
                serde_json::json!({
                    "version": 3,
                    "families": {
                        "dev-1::dev.{n}@astronlab.com": {
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "next_suffix": 23,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "last_created_email": "dev.22@astronlab.com"
                        }
                    },
                    "pending": {
                        "dev.23@astronlab.com": {
                            "email": "dev.23@astronlab.com",
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "suffix": 23,
                            "selector": null,
                            "alias": null,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "started_at": "2026-04-05T00:00:00.000Z"
                        }
                    }
                })
                .to_string(),
            )?;

            assert!(migrate_legacy_credential_store_if_needed()?);

            let merged: serde_json::Value =
                serde_json::from_str(&fs::read_to_string(&accounts_path)?)?;
            assert_eq!(merged["active_index"], 2);
            assert_eq!(merged["accounts"][0]["email"], "dev.22@astronlab.com");
            assert_eq!(
                merged["families"]["dev-1::dev.{n}@astronlab.com"]["next_suffix"],
                23
            );
            assert_eq!(
                merged["pending"]["dev.23@astronlab.com"]["email"],
                "dev.23@astronlab.com"
            );
            assert!(!legacy_path.exists());
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        fs::remove_dir_all(&rotate_home).ok();
        result.expect("legacy credential migration");
    }

    #[test]
    fn loading_credential_store_migrates_legacy_file_automatically() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let rotate_home = unique_temp_dir("codex-rotate-load-store");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let legacy_path = rotate_home.join("credentials.json");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = (|| -> Result<()> {
            fs::write(
                &legacy_path,
                serde_json::json!({
                    "version": 3,
                    "families": {
                        "dev-1::dev.{n}@astronlab.com": {
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "next_suffix": 23,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "last_created_email": "dev.22@astronlab.com"
                        }
                    },
                    "pending": {
                        "dev.23@astronlab.com": {
                            "email": "dev.23@astronlab.com",
                            "profile_name": "dev-1",
                            "base_email": "dev.{n}@astronlab.com",
                            "suffix": 23,
                            "selector": null,
                            "alias": null,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "started_at": "2026-04-05T00:00:00.000Z"
                        }
                    }
                })
                .to_string(),
            )?;

            let store = load_credential_store()?;
            assert_eq!(
                store
                    .families
                    .get("dev-1::dev.{n}@astronlab.com")
                    .map(|family| family.next_suffix),
                Some(23)
            );
            assert!(store.pending.contains_key("dev.23@astronlab.com"));
            assert!(!legacy_path.exists());
            assert!(rotate_home.join("accounts.json").exists());
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        fs::remove_dir_all(&rotate_home).ok();
        result.expect("load credential store migration");
    }

    #[test]
    fn loading_credential_store_keeps_read_path_side_effect_free() {
        let _guard = ENV_MUTEX.lock().unwrap_or_else(|error| error.into_inner());
        let rotate_home = unique_temp_dir("codex-rotate-pure-load-store");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let accounts_path = rotate_home.join("accounts.json");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = (|| -> Result<()> {
            fs::write(
                &accounts_path,
                serde_json::json!({
                    "pending": {
                        "bench.5@astronlab.com": {
                            "email": "bench.5@astronlab.com",
                            "profile_name": "dev-1",
                            "base_email": "bench.{n}@astronlab.com",
                            "suffix": 5,
                            "selector": null,
                            "alias": null,
                            "created_at": "2026-04-05T00:00:00.000Z",
                            "updated_at": "2026-04-05T00:00:00.000Z",
                            "started_at": "2026-04-05T00:00:00.000Z"
                        }
                    }
                })
                .to_string(),
            )?;

            let before = fs::read_to_string(&accounts_path)?;
            let store = load_credential_store()?;
            let after = fs::read_to_string(&accounts_path)?;

            assert!(store.pending.is_empty());
            assert_eq!(after, before);
            Ok(())
        })();

        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        fs::remove_dir_all(&rotate_home).ok();
        result.expect("pure credential store load");
    }
}
