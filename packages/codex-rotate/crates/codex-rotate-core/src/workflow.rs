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
use crate::paths::{legacy_credentials_file, resolve_paths};
use crate::persona::OsFamily;
use crate::pool::{
    cmd_add_expected_email, find_next_usable_account, format_account_summary_for_display,
    inspect_account, load_pool, normalize_pool_entries, resolve_account_selector,
    resolve_persona_profile, save_pool, sync_pool_active_account_from_codex, AccountEntry,
    AccountInspection, PersonaEntry, PersonaProfile, Pool, ReusableAccountProbeMode,
};
use crate::quota::{describe_quota_blocker, format_compact_quota, has_usable_quota};
#[cfg(test)]
use crate::state::write_rotate_state_json;
use crate::state::{load_rotate_state_json, update_rotate_state_json, RotateStateOwner};

mod account_family;
mod create_flow;
mod credential_store;
mod login_bridge;
mod managed_profiles;
mod relogin_flow;
mod workflow_metadata;

use self::account_family::*;
use self::create_flow::*;
use self::credential_store::*;
use self::login_bridge::*;
use self::managed_profiles::*;
#[cfg(test)]
use self::relogin_flow::*;
use self::workflow_metadata::*;

pub use self::create_flow::{
    cmd_create, cmd_create_with_progress, create_next_fallback_options,
    is_auto_create_retry_stopped_for_reusable_account, is_create_already_in_progress_error,
    reconcile_added_account_credential_state,
};
pub use self::credential_store::{
    auto_disable_domain_for_account, extract_email_domain, load_disabled_rotation_domains,
    load_relogin_account_emails, record_removed_account,
};
pub(crate) use self::credential_store::{
    migrate_rotate_state_credential_sections, record_terminal_refresh_failures,
};
pub use self::login_bridge::cmd_generate_browser_fingerprint;
pub use self::relogin_flow::{cmd_relogin, cmd_relogin_with_progress};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const DEFAULT_CODEX_BIN: &str = "codex";
const DEFAULT_CODEX_APP_BUNDLE_BIN: &str = "/Applications/Codex.app/Contents/Resources/codex";
const ROTATE_STATE_VERSION: u8 = 9;
const AUTO_DOMAIN_REACTIVATION_DAYS: i64 = 9;
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
const CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE_ENV: &str = "CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE";
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
    pub template: Option<String>,
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
            template: None,
            force: false,
            ignore_current: false,
            restore_previous_auth_after_create: false,
            require_usable_quota: false,
            source: CreateCommandSource::Manual,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Default)]
pub struct ReloginOptions {
    pub allow_email_change: bool,
    pub logout_first: bool,
    pub manual_login: bool,
}

#[derive(Clone, Debug)]
struct CreateCommandResult {
    entry: AccountEntry,
    inspection: Option<AccountInspection>,
    profile_name: String,
    template: String,
}

#[derive(Debug)]
enum CreateFlowAttemptFailure {
    Fatal(anyhow::Error),
    Retryable {
        error: anyhow::Error,
        retry_reserved_email: Option<String>,
    },
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
    #[serde(alias = "base_email")]
    template: Option<String>,
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
    #[serde(alias = "base_email")]
    pub template: String,
    pub next_suffix: u32,
    #[serde(default = "default_max_skipped_slots_per_family")]
    pub max_skipped_slots: u32,
    pub created_at: String,
    pub updated_at: String,
    pub last_created_email: Option<String>,
    #[serde(default, alias = "deleted", skip_serializing)]
    pub relogin: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub suspend_domain_on_terminal_refresh_failure: bool,
}

fn default_max_skipped_slots_per_family() -> u32 {
    DEFAULT_MAX_SKIPPED_SLOTS_PER_FAMILY
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DomainConfig {
    #[serde(default = "default_rotation_enabled")]
    pub rotation_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_suffix_per_family: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reactivate_at: Option<String>,
}

fn default_rotation_enabled() -> bool {
    true
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
    #[serde(alias = "base_email")]
    pub template: String,
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
pub enum CodexRotateSecretLocator {
    LoginLookup {
        store: String,
        username: String,
        uris: Vec<String>,
        field_path: String,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct CodexRotateAuthFlowSession {
    pub auth_url: Option<String>,
    pub callback_url: Option<String>,
    pub callback_port: Option<u16>,
    pub device_code: Option<String>,
    pub session_dir: Option<String>,
    pub codex_home_path: Option<String>,
    pub auth_file_path: Option<String>,
    pub pid: Option<u32>,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub exit_path: Option<String>,
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
    pub default_create_template: String,
    pub domain: HashMap<String, DomainConfig>,
    pub families: HashMap<String, CredentialFamily>,
    pub pending: HashMap<String, PendingCredential>,
    pub skipped: HashSet<String>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self {
            version: ROTATE_STATE_VERSION,
            default_create_template: String::new(),
            domain: HashMap::new(),
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
    #[serde(skip_serializing_if = "Option::is_none")]
    profile_dir: Option<&'a str>,
    email: &'a str,
    password: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeDeleteSecretPayload<'a> {
    profile_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    profile_dir: Option<&'a str>,
    email: &'a str,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeGenerateFingerprintPayload<'a> {
    pub persona_id: &'a str,
    pub options: BridgeGenerateFingerprintOptions<'a>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeGenerateFingerprintOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen_width: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub screen_height: Option<u32>,
    pub os_family: OsFamily,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeLoginOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_dir: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codex_bin: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_ref: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow_run_stamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_locator_preflight: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefer_signup_recovery: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefer_password_login: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub full_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub birth_year: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub codex_session: Option<&'a CodexRotateAuthFlowSession>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub persona_profile: Option<PersonaProfile>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BridgeCompleteLoginAttemptPayload<'a> {
    pub profile_name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_dir: Option<&'a str>,
    pub email: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_login_locator: Option<&'a CodexRotateSecretLocator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<BridgeLoginOptions<'a>>,
}

#[derive(Clone, Debug, Default)]
struct BridgeLoginAttemptResult {
    result: Option<FastBrowserRunResult>,
    browser_fingerprint: Option<Value>,
    error_message: Option<String>,
}

#[derive(Clone, Debug)]
struct EmailFamily {
    normalized: String,
    domain_part: String,
    prefix: String,
    suffix: String,
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

fn restore_active_auth_after_relogin(previous_auth: Option<&CodexAuth>) -> Result<()> {
    if previous_auth.is_some() {
        restore_active_auth(previous_auth)?;
    }
    Ok(())
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

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests;
