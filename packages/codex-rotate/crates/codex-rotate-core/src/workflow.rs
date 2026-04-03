use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Datelike, SecondsFormat, Utc};
use rand::rngs::OsRng;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::auth::{
    extract_account_id_from_auth, load_codex_auth, summarize_codex_auth, write_codex_auth,
    CodexAuth,
};
use crate::bridge::run_automation_bridge;
use crate::paths::resolve_paths;
use crate::pool::{
    cmd_add, find_next_usable_account, format_account_summary_for_display, inspect_account,
    load_pool, normalize_pool_entries, resolve_account_selector, save_pool,
    sync_pool_active_account_from_codex, AccountEntry, AccountInspection, Pool,
    ReusableAccountProbeMode,
};
use crate::quota::{describe_quota_blocker, format_compact_quota, has_usable_quota};

const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const RESET: &str = "\x1b[0m";
const DEFAULT_CODEX_BIN: &str = "codex";
const DEFAULT_CREATE_BASE_EMAIL: &str = "dev.{n}@astronlab.com";
const EMAIL_FAMILY_PLACEHOLDER: &str = "{n}";

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
            require_usable_quota: false,
            source: CreateCommandSource::Manual,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReloginOptions {
    pub allow_email_change: bool,
    pub device_auth: bool,
    pub logout_first: bool,
    pub manual_login: bool,
}

impl Default for ReloginOptions {
    fn default() -> Self {
        Self {
            allow_email_change: false,
            device_auth: false,
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

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialFamily {
    pub profile_name: String,
    pub base_email: String,
    pub next_suffix: u32,
    pub created_at: String,
    pub updated_at: String,
    pub last_created_email: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_secret_ref: Option<CodexRotateSecretRef>,
    #[serde(skip)]
    pub legacy_password: Option<String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CredentialStore {
    pub version: u8,
    pub families: HashMap<String, CredentialFamily>,
    pub accounts: HashMap<String, StoredCredential>,
    pub pending: HashMap<String, PendingCredential>,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self {
            version: 2,
            families: HashMap::new(),
            accounts: HashMap::new(),
            pending: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct AdultBirthDate {
    birth_month: u8,
    birth_day: u8,
    birth_year: u16,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowFileMetadata {
    preferred_profile_name: Option<String>,
    preferred_email: Option<String>,
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeWorkflowMetadataPayload<'a> {
    file_path: &'a str,
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
struct BridgeLoginOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    codex_bin: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_run_stamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefer_signup_recovery: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_month: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_day: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    birth_year: Option<u16>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BridgeCompleteLoginPayload<'a> {
    profile_name: &'a str,
    email: &'a str,
    account_secret_ref: &'a CodexRotateSecretRef,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<BridgeLoginOptions<'a>>,
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

pub fn cmd_create(options: CreateCommandOptions) -> Result<String> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let mut dirty = normalize_pool_entries(&mut pool);
    dirty |= sync_pool_active_account_from_codex(&mut pool, &paths.codex_auth_file)?;

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

    let result = execute_create_flow(&options)?;
    let quota_summary = summarize_quota_for_create(&result);
    Ok(format!(
        "{GREEN}OK{RESET} Created {} via \"{}\" from {}.\nQuota: {}",
        result.entry.label, result.profile_name, result.base_email, quota_summary
    ))
}

pub fn cmd_relogin(selector: &str, options: ReloginOptions) -> Result<String> {
    let selection = {
        let pool = load_pool()?;
        resolve_account_selector(&pool, selector)?
    };
    let existing = selection.entry.clone();
    let expected_email = existing.email.clone();
    let mut store = load_credential_store()?;
    let stored_credential = store
        .accounts
        .get(&normalize_email_key(&expected_email))
        .cloned();

    if should_use_stored_credential_relogin(stored_credential.as_ref(), &options) {
        let stored_credential = stored_credential.ok_or_else(|| {
            anyhow!("Stored credential lookup unexpectedly failed for {expected_email}.")
        })?;
        let account_secret_ref = ensure_credential_account_secret_ref(
            stored_credential.account_secret_ref.as_ref(),
            stored_credential.legacy_password.as_deref(),
            &stored_credential.email,
            &stored_credential.profile_name,
        )?;
        let mut updated_stored = stored_credential.clone();
        updated_stored.account_secret_ref = Some(account_secret_ref.clone());
        updated_stored.legacy_password = None;
        updated_stored.updated_at = now_iso();
        store.accounts.insert(
            normalize_email_key(&updated_stored.email),
            updated_stored.clone(),
        );
        save_credential_store(&store)?;

        let previous_auth = load_codex_auth_if_exists()?;
        let login_result = run_complete_codex_login(
            &updated_stored.profile_name,
            &updated_stored.email,
            &account_secret_ref,
            None,
            None,
            None,
            None,
        );
        if let Err(error) = login_result {
            restore_active_auth(previous_auth.as_ref())?;
            return Err(error);
        }

        let auth = load_current_auth()?;
        let logged_in_email = summarize_codex_auth(&auth).email;
        if !options.allow_email_change
            && normalize_email_key(&logged_in_email) != normalize_email_key(&expected_email)
        {
            restore_active_auth(previous_auth.as_ref())?;
            return Err(anyhow!(
                "Expected {}, but Codex logged into {}.",
                expected_email,
                logged_in_email
            ));
        }

        let _ = cmd_add(existing.alias.as_deref())?;
        if let Some(inspected) =
            inspect_pool_entry_by_account_id(&extract_account_id_from_auth(&auth))?
        {
            let entry = store
                .accounts
                .get(&normalize_email_key(&updated_stored.email))
                .cloned()
                .unwrap_or(updated_stored);
            store.accounts.insert(
                normalize_email_key(&entry.email),
                StoredCredential {
                    selector: Some(inspected.entry.label.clone()),
                    alias: inspected
                        .entry
                        .alias
                        .clone()
                        .or_else(|| existing.alias.clone()),
                    updated_at: now_iso(),
                    ..entry
                },
            );
            save_credential_store(&store)?;
        }

        return Ok(format!(
            "{GREEN}OK{RESET} Re-logged {} with stored managed-browser credentials.",
            format_account_summary_for_display(&existing)
        ));
    }

    if stored_credential.is_none() && !options.manual_login && !options.device_auth {
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

    if options.device_auth {
        run_codex_command(["login", "--device-auth"])?;
    } else {
        run_codex_command(["login"])?;
    }

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

    cmd_add(existing.alias.as_deref())
}

pub fn should_use_stored_credential_relogin(
    stored_credential: Option<&StoredCredential>,
    options: &ReloginOptions,
) -> bool {
    stored_credential.is_some()
        && !options.manual_login
        && !options.device_auth
        && stored_credential
            .and_then(|value| {
                value
                    .account_secret_ref
                    .as_ref()
                    .map(|_| ())
                    .or_else(|| value.legacy_password.as_ref().map(|_| ()))
            })
            .is_some()
}

pub fn create_next_fallback_options() -> CreateCommandOptions {
    CreateCommandOptions {
        require_usable_quota: true,
        source: CreateCommandSource::Next,
        ..CreateCommandOptions::default()
    }
}

fn execute_create_flow(options: &CreateCommandOptions) -> Result<CreateCommandResult> {
    let paths = resolve_paths()?;
    let previous_auth = load_codex_auth_if_exists()?;
    let mut store = load_credential_store()?;
    let workflow_metadata = read_workflow_file_metadata(&paths.account_flow_file)?;
    let profile_name = resolve_managed_profile_name(
        options.profile_name.as_deref(),
        workflow_metadata.preferred_profile_name.as_deref(),
        Some(paths.account_flow_file.display().to_string().as_str()),
    )?;
    let pending_base_hint_raw = if options.base_email.is_some() {
        None
    } else {
        select_pending_base_email_hint_for_profile(&store, &profile_name, options.alias.as_deref())
    };
    let stored_base_hint_raw = select_stored_base_email_hint(&store, &profile_name);
    let pending_base_hint = pending_base_hint_raw
        .as_deref()
        .filter(|value| should_use_default_create_family_hint(Some(value)))
        .map(ToOwned::to_owned);
    let stored_base_hint = stored_base_hint_raw
        .as_deref()
        .filter(|value| should_use_default_create_family_hint(Some(value)))
        .map(ToOwned::to_owned);
    let base_email = resolve_create_base_email(
        options.base_email.as_deref(),
        pending_base_hint
            .as_deref()
            .or(stored_base_hint.as_deref())
            .or(workflow_metadata.preferred_email.as_deref()),
    )?;

    let pool = load_pool()?;
    let family_key = make_credential_family_key(&profile_name, &base_email)?;
    let family = store.families.get(&family_key).cloned();
    let started_at = now_iso();
    let existing_pending = select_pending_credential_for_family(
        &store,
        &profile_name,
        &base_email,
        options.alias.as_deref(),
    );
    let suffix = match existing_pending.as_ref() {
        Some(entry) => entry.stored.suffix,
        None => compute_next_account_family_suffix(
            &base_email,
            family.as_ref().map(|entry| entry.next_suffix).unwrap_or(1),
            collect_known_account_emails(&pool, &store),
        )?,
    };
    let created_email = existing_pending
        .as_ref()
        .map(|entry| entry.stored.email.clone())
        .unwrap_or_else(|| build_account_family_email(&base_email, suffix).unwrap_or_default());
    let existing_pending = existing_pending.unwrap_or_else(|| PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            account_secret_ref: None,
            legacy_password: None,
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

    let account_secret_ref = ensure_credential_account_secret_ref(
        existing_pending.stored.account_secret_ref.as_ref(),
        existing_pending.stored.legacy_password.as_deref(),
        &created_email,
        &profile_name,
    )?;
    let birth_date = resolve_credential_birth_date(Some(&existing_pending.stored), Utc::now());
    let pending = PendingCredential {
        stored: StoredCredential {
            email: created_email.clone(),
            account_secret_ref: Some(account_secret_ref.clone()),
            legacy_password: None,
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            suffix,
            selector: existing_pending.stored.selector.clone(),
            alias: existing_pending
                .stored
                .alias
                .clone()
                .or_else(|| normalize_alias(options.alias.as_deref())),
            birth_month: Some(
                existing_pending
                    .stored
                    .birth_month
                    .unwrap_or(birth_date.birth_month),
            ),
            birth_day: Some(
                existing_pending
                    .stored
                    .birth_day
                    .unwrap_or(birth_date.birth_day),
            ),
            birth_year: Some(
                existing_pending
                    .stored
                    .birth_year
                    .unwrap_or(birth_date.birth_year),
            ),
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
    save_credential_store(&store)?;

    let login_result = run_complete_codex_login(
        &profile_name,
        &created_email,
        &account_secret_ref,
        Some(codex_bin().as_str()),
        Some(started_at.as_str()),
        Some(true),
        Some(&birth_date),
    );
    if let Err(error) = login_result {
        restore_active_auth(previous_auth.as_ref())?;
        save_credential_store(&store)?;
        return Err(error);
    }

    let auth = load_current_auth()?;
    let logged_in_email = summarize_codex_auth(&auth).email;
    if normalize_email_key(&logged_in_email) != normalize_email_key(&created_email) {
        restore_active_auth(previous_auth.as_ref())?;
        save_credential_store(&store)?;
        return Err(anyhow!(
            "Expected {}, but Codex logged into {}.",
            created_email,
            logged_in_email
        ));
    }

    let _ = cmd_add(options.alias.as_deref())?;
    let inspected = inspect_pool_entry_by_account_id(&extract_account_id_from_auth(&auth))?
        .ok_or_else(|| {
            anyhow!(
                "Created {}, but could not find the new account in the pool after login.",
                created_email
            )
        })?;

    let updated_at = now_iso();
    store.pending.remove(&normalize_email_key(&created_email));
    store.accounts.insert(
        normalize_email_key(&created_email),
        StoredCredential {
            email: created_email.clone(),
            account_secret_ref: Some(account_secret_ref.clone()),
            legacy_password: None,
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
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
        family_key,
        CredentialFamily {
            profile_name: profile_name.clone(),
            base_email: base_email.clone(),
            next_suffix: family
                .as_ref()
                .map(|entry| entry.next_suffix.max(suffix + 1))
                .unwrap_or(suffix + 1),
            created_at: family
                .as_ref()
                .map(|entry| entry.created_at.clone())
                .unwrap_or_else(|| started_at.clone()),
            updated_at,
            last_created_email: Some(created_email.clone()),
        },
    );
    save_credential_store(&store)?;

    if options.require_usable_quota {
        match inspected.inspection.usage.as_ref() {
            Some(usage) if has_usable_quota(usage) => {}
            Some(usage) => {
                restore_active_auth(previous_auth.as_ref())?;
                return Err(anyhow!(
                    "Created {}, but it does not have usable quota ({}).",
                    inspected.entry.label,
                    describe_quota_blocker(usage)
                ));
            }
            None => {
                restore_active_auth(previous_auth.as_ref())?;
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

    Ok(CreateCommandResult {
        entry: inspected.entry,
        inspection: Some(inspected.inspection),
        profile_name,
        base_email,
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

fn inspect_pool_entry_by_account_id(account_id: &str) -> Result<Option<InspectedPoolEntry>> {
    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let index = pool.accounts.iter().position(|entry| {
        entry.account_id == account_id || entry.auth.tokens.account_id == account_id
    });
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

struct InspectedPoolEntry {
    entry: AccountEntry,
    inspection: AccountInspection,
}

fn load_current_auth() -> Result<CodexAuth> {
    let paths = resolve_paths()?;
    load_codex_auth(&paths.codex_auth_file)
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

fn run_complete_codex_login(
    profile_name: &str,
    email: &str,
    account_secret_ref: &CodexRotateSecretRef,
    codex_bin: Option<&str>,
    workflow_run_stamp: Option<&str>,
    prefer_signup_recovery: Option<bool>,
    birth_date: Option<&AdultBirthDate>,
) -> Result<()> {
    let options = BridgeLoginOptions {
        codex_bin,
        workflow_run_stamp,
        prefer_signup_recovery,
        birth_month: birth_date.map(|value| value.birth_month),
        birth_day: birth_date.map(|value| value.birth_day),
        birth_year: birth_date.map(|value| value.birth_year),
    };
    let _: Value = run_automation_bridge(
        "complete-codex-login",
        BridgeCompleteLoginPayload {
            profile_name,
            email,
            account_secret_ref,
            options: Some(options),
        },
    )?;
    Ok(())
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
    std::env::var("CODEX_ROTATE_CODEX_BIN").unwrap_or_else(|_| DEFAULT_CODEX_BIN.to_string())
}

fn load_credential_store() -> Result<CredentialStore> {
    let paths = resolve_paths()?;
    if !paths.credentials_file.exists() {
        return Ok(CredentialStore::default());
    }
    let raw = fs::read_to_string(&paths.credentials_file)
        .with_context(|| format!("Failed to read {}.", paths.credentials_file.display()))?;
    let parsed: Value = serde_json::from_str(&raw).with_context(|| {
        format!(
            "Invalid credential store at {}.",
            paths.credentials_file.display()
        )
    })?;
    Ok(normalize_credential_store(parsed))
}

fn save_credential_store(store: &CredentialStore) -> Result<()> {
    let paths = resolve_paths()?;
    if let Some(parent) = paths.credentials_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(&serialize_credential_store(store))?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&paths.credentials_file)
        .with_context(|| format!("Failed to open {}.", paths.credentials_file.display()))?;
    file.write_all(raw.as_bytes())?;
    Ok(())
}

fn normalize_credential_store(raw: Value) -> CredentialStore {
    let families = raw
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

    CredentialStore {
        version: 2,
        families,
        accounts: normalize_stored_credential_map(raw.get("accounts")),
        pending: normalize_pending_credential_map(raw.get("pending")),
    }
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

fn normalize_stored_credential(raw: &Value) -> Option<StoredCredential> {
    let object = raw.as_object()?;
    let mut normalized = serde_json::from_value::<StoredCredential>(raw.clone()).ok()?;
    let secret_ref = normalize_secret_ref(object.get("account_secret_ref"));
    let legacy_password = object
        .get("password")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    normalized.account_secret_ref = secret_ref.clone();
    normalized.legacy_password = if secret_ref.is_some() {
        None
    } else {
        legacy_password
    };
    Some(normalized)
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
    let accounts = store
        .accounts
        .iter()
        .map(|(email, record)| (email.clone(), serialize_stored_credential(record)))
        .collect::<Map<String, Value>>();
    let pending = store
        .pending
        .iter()
        .map(|(email, record)| (email.clone(), serialize_pending_credential(record)))
        .collect::<Map<String, Value>>();
    json!({
        "version": 2,
        "families": store.families,
        "accounts": accounts,
        "pending": pending,
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
    if let Some(secret_ref) = normalize_secret_ref(
        record
            .account_secret_ref
            .as_ref()
            .map(|value| serde_json::to_value(value).unwrap_or(Value::Null))
            .as_ref(),
    ) {
        object.insert(
            "account_secret_ref".to_string(),
            serde_json::to_value(secret_ref).unwrap_or(Value::Null),
        );
    } else if let Some(password) = record.legacy_password.as_ref() {
        object.insert("password".to_string(), Value::String(password.clone()));
    }
    Value::Object(object)
}

fn normalize_secret_ref(raw: Option<&Value>) -> Option<CodexRotateSecretRef> {
    let object = raw?.as_object()?;
    let object_id = object
        .get("object_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let store = object
        .get("store")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("bitwarden-cli");
    if store != "bitwarden-cli" {
        return None;
    }
    let ref_type = object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("secret_ref");
    if ref_type != "secret_ref" {
        return None;
    }
    Some(CodexRotateSecretRef {
        ref_type: "secret_ref".to_string(),
        store: "bitwarden-cli".to_string(),
        object_id,
        field_path: object
            .get("field_path")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        version: object
            .get("version")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn ensure_credential_account_secret_ref(
    account_secret_ref: Option<&CodexRotateSecretRef>,
    legacy_password: Option<&str>,
    email: &str,
    profile_name: &str,
) -> Result<CodexRotateSecretRef> {
    if let Some(account_secret_ref) = account_secret_ref {
        return Ok(account_secret_ref.clone());
    }
    let password = legacy_password
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| generate_password(18));
    run_automation_bridge(
        "ensure-account-secret-ref",
        BridgeEnsureSecretPayload {
            profile_name,
            email,
            password: &password,
        },
    )
}

fn read_workflow_file_metadata(file_path: &std::path::Path) -> Result<WorkflowFileMetadata> {
    let payload = BridgeWorkflowMetadataPayload {
        file_path: file_path
            .to_str()
            .ok_or_else(|| anyhow!("Workflow file path is not valid UTF-8."))?,
    };
    run_automation_bridge("read-workflow-metadata", payload)
}

fn inspect_managed_profiles() -> Result<ManagedProfilesInspection> {
    run_automation_bridge("inspect-managed-profiles", json!({}))
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
        if available_names
            .iter()
            .any(|value| *value == requested_profile_name)
        {
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
        if available_names
            .iter()
            .any(|value| *value == preferred_profile_name)
        {
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
        if available_names
            .iter()
            .any(|value| *value == default_profile_name)
        {
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

fn should_use_default_create_family_hint(base_email: Option<&str>) -> bool {
    base_email
        .and_then(|value| parse_email_family(value).ok())
        .map(|value| matches!(value.mode, EmailFamilyMode::Template { .. }))
        .unwrap_or(false)
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

fn compute_next_account_family_suffix(
    base_email: &str,
    family_next_suffix: u32,
    known_emails: Vec<String>,
) -> Result<u32> {
    let mut used = HashSet::new();
    for email in known_emails {
        if let Some(suffix) = extract_account_family_suffix(&email, base_email)? {
            used.insert(suffix);
        }
    }
    let mut candidate = family_next_suffix.max(1);
    while used.contains(&candidate) {
        candidate += 1;
    }
    Ok(candidate)
}

fn collect_known_account_emails(pool: &Pool, store: &CredentialStore) -> Vec<String> {
    let mut emails = pool
        .accounts
        .iter()
        .map(|entry| entry.email.clone())
        .collect::<Vec<_>>();
    emails.extend(store.accounts.keys().cloned());
    emails.extend(store.pending.keys().cloned());
    emails
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
        .then_with(|| left.stored.suffix.cmp(&right.stored.suffix))
        .then_with(|| {
            parse_sortable_timestamp(Some(left.stored.updated_at.as_str())).cmp(
                &parse_sortable_timestamp(Some(right.stored.updated_at.as_str())),
            )
        })
    });
    let raw_email = matches
        .first()
        .map(|entry| entry.stored.base_email.as_str())
        .or_else(|| matches.first().map(|entry| entry.stored.email.as_str()))?;
    normalize_base_email_family(raw_email).ok()
}

fn select_stored_base_email_hint(store: &CredentialStore, profile_name: &str) -> Option<String> {
    let mut candidates = HashMap::<String, (u32, i64)>::new();
    let mut remember = |raw_email: Option<&str>, updated_at: Option<&str>| {
        let Some(raw_email) = raw_email else {
            return;
        };
        let Ok(base_email) = normalize_base_email_family(raw_email) else {
            return;
        };
        let updated_at_value = parse_sortable_timestamp(updated_at);
        let entry = candidates.entry(base_email).or_insert((0, 0));
        entry.0 += 1;
        entry.1 = entry.1.max(updated_at_value);
    };

    for family in store
        .families
        .values()
        .filter(|entry| entry.profile_name == profile_name)
    {
        remember(Some(&family.base_email), Some(&family.updated_at));
    }
    for account in store
        .accounts
        .values()
        .filter(|entry| entry.profile_name == profile_name)
    {
        remember(Some(&account.base_email), Some(&account.updated_at));
    }
    for pending in store
        .pending
        .values()
        .filter(|entry| entry.stored.profile_name == profile_name)
    {
        remember(
            Some(&pending.stored.base_email),
            pending
                .started_at
                .as_deref()
                .or(Some(pending.stored.updated_at.as_str())),
        );
    }

    candidates
        .into_iter()
        .max_by(|left, right| {
            left.1
                 .0
                .cmp(&right.1 .0)
                .then_with(|| left.1 .1.cmp(&right.1 .1))
                .then_with(|| right.0.cmp(&left.0))
        })
        .map(|entry| entry.0)
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
    now: DateTime<Utc>,
) -> AdultBirthDate {
    if let Some(credential) = credential {
        if let (Some(birth_month), Some(birth_day), Some(birth_year)) = (
            credential.birth_month,
            credential.birth_day,
            credential.birth_year,
        ) {
            if (1..=12).contains(&birth_month)
                && (1..=31).contains(&birth_day)
                && birth_year >= 1900
            {
                return AdultBirthDate {
                    birth_month,
                    birth_day,
                    birth_year,
                };
            }
        }
    }
    generate_random_adult_birth_date(now, 20, 45, None)
}

fn generate_random_adult_birth_date(
    now: DateTime<Utc>,
    min_age_years: i32,
    max_age_years: i32,
    pick_offset_ms: Option<fn(u64) -> u64>,
) -> AdultBirthDate {
    assert!(min_age_years >= 0);
    assert!(max_age_years >= min_age_years);

    let latest_birth =
        chrono::NaiveDate::from_ymd_opt(now.year() - min_age_years, now.month(), now.day())
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
    let earliest_birth =
        chrono::NaiveDate::from_ymd_opt(now.year() - max_age_years, now.month(), now.day())
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc();
    let span_ms = (latest_birth - earliest_birth).num_milliseconds().max(0) as u64;
    let offset_ms = if let Some(picker) = pick_offset_ms {
        if span_ms == 0 {
            0
        } else {
            picker(span_ms + 1)
        }
    } else if span_ms == 0 {
        0
    } else {
        random_below(span_ms + 1)
    };
    let chosen = earliest_birth + chrono::Duration::milliseconds(offset_ms as i64);
    AdultBirthDate {
        birth_month: chosen.month() as u8,
        birth_day: chosen.day() as u8,
        birth_year: chosen.year() as u16,
    }
}

fn random_below(max_exclusive: u64) -> u64 {
    if max_exclusive <= 1 {
        return 0;
    }
    let zone = u64::MAX - (u64::MAX % max_exclusive);
    loop {
        let mut bytes = [0u8; 8];
        OsRng.fill_bytes(&mut bytes);
        let candidate = u64::from_le_bytes(bytes);
        if candidate < zone {
            return candidate % max_exclusive;
        }
    }
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_credentials_used_only_for_non_manual_relogin() {
        let stored = StoredCredential {
            email: "dev.user+1@gmail.com".to_string(),
            account_secret_ref: Some(CodexRotateSecretRef {
                ref_type: "secret_ref".to_string(),
                store: "bitwarden-cli".to_string(),
                object_id: "bw-1".to_string(),
                field_path: None,
                version: None,
            }),
            legacy_password: None,
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
    fn generates_expected_oldest_adult_date() {
        let now = DateTime::parse_from_rfc3339("2026-04-02T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let value = generate_random_adult_birth_date(now, 20, 45, Some(|_| 0));
        assert_eq!(value.birth_month, 4);
        assert_eq!(value.birth_day, 2);
        assert_eq!(value.birth_year, 1981);
    }

    #[test]
    fn reuses_existing_birth_date() {
        let now = DateTime::parse_from_rfc3339("2026-04-02T00:00:00.000Z")
            .unwrap()
            .with_timezone(&Utc);
        let value = resolve_credential_birth_date(
            Some(&StoredCredential {
                email: "dev.user+1@gmail.com".to_string(),
                account_secret_ref: None,
                legacy_password: None,
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
            now,
        );
        assert_eq!(value.birth_month, 7);
        assert_eq!(value.birth_day, 14);
        assert_eq!(value.birth_year, 1994);
    }

    #[test]
    fn normalizes_legacy_password_records() {
        let store = normalize_credential_store(json!({
            "accounts": {
                "dev.user+1@gmail.com": {
                    "email": "dev.user+1@gmail.com",
                    "password": "pw-1",
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
        assert_eq!(
            store
                .accounts
                .get("dev.user+1@gmail.com")
                .and_then(|value| value.legacy_password.as_deref()),
            Some("pw-1")
        );
    }
}
