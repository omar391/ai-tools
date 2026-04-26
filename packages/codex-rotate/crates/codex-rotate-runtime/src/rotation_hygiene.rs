use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::{self, ErrorKind, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{SecondsFormat, TimeZone, Utc};
use codex_rotate_core::auth::{load_codex_auth, summarize_codex_auth};
use codex_rotate_core::bridge::AutomationProgressCallback;
use codex_rotate_core::pool::{
    load_codex_mode_config_from_path, load_pool, load_rotation_checkpoint,
    load_rotation_environment_settings, persist_prepared_rotation_pool,
    prepare_next_rotation_with_progress, prepare_prev_rotation, prepare_set_rotation,
    resolve_persona_profile, resolve_pool_account, restore_pool_active_index,
    rollback_prepared_rotation, save_pool, save_rotation_checkpoint,
    sync_pool_active_account_from_current_auth, write_selected_account_auth, AccountEntry,
    CodexModeConfig, CodexModeProfile, NextResult, PreparedRotation, PreparedRotationAction,
    RotationCheckpoint, RotationCheckpointPhase, RotationEnvironment,
};
use codex_rotate_core::state::RotationLock;
use codex_rotate_core::workflow::{
    cmd_create_with_progress, cmd_generate_browser_fingerprint, cmd_relogin_with_progress,
    CreateCommandOptions, CreateCommandSource, ReloginOptions,
};
use rusqlite::OptionalExtension;
use serde_json::{json, Value};

use crate::launcher::ensure_debug_codex_instance;
use crate::log_isolation::{
    managed_codex_is_running, stop_managed_codex_instance, wait_for_all_threads_idle,
};
use crate::paths::{resolve_paths, RuntimePaths};
use crate::thread_recovery::{
    read_active_thread_ids, read_latest_recoverable_turn_failure_log_id,
    run_thread_recovery_iteration, send_codex_app_request, send_codex_host_fetch_request,
    RecoveryIterationOptions, ThreadRecoveryRehydration,
};
use crate::watch::read_watch_state;
#[cfg(test)]
use crate::watch::write_watch_state;

mod conversation_sync;
mod conversation_sync_store;
mod host_persona;
mod host_snapshot;
mod orchestration;
pub use self::conversation_sync::*;
pub use self::conversation_sync_store::ConversationSyncStore;
use self::conversation_sync_store::{is_pending_lineage_claim, LineageBindingClaim};
use self::host_persona::*;
use self::host_snapshot::*;
pub(crate) use self::orchestration::recover_incomplete_rotation_state;
use self::orchestration::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RotationPhase {
    Prepare,
    Export,
    Activate,
    Import,
    Commit,
    Rollback,
}

impl std::fmt::Display for RotationPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self {
            Self::Prepare => "prepare",
            Self::Export => "export",
            Self::Activate => "activate",
            Self::Import => "import",
            Self::Commit => "commit",
            Self::Rollback => "rollback",
        };
        write!(f, "{}", label)
    }
}

impl From<RotationPhase> for RotationCheckpointPhase {
    fn from(value: RotationPhase) -> Self {
        match value {
            RotationPhase::Prepare => Self::Prepare,
            RotationPhase::Export => Self::Export,
            RotationPhase::Activate => Self::Activate,
            RotationPhase::Import => Self::Import,
            RotationPhase::Commit => Self::Commit,
            RotationPhase::Rollback => Self::Rollback,
        }
    }
}

impl From<RotationCheckpointPhase> for RotationPhase {
    fn from(value: RotationCheckpointPhase) -> Self {
        match value {
            RotationCheckpointPhase::Prepare => Self::Prepare,
            RotationCheckpointPhase::Export => Self::Export,
            RotationCheckpointPhase::Activate => Self::Activate,
            RotationCheckpointPhase::Import => Self::Import,
            RotationCheckpointPhase::Commit => Self::Commit,
            RotationCheckpointPhase::Rollback => Self::Rollback,
        }
    }
}

const DEFAULT_PORT: u16 = 9333;
const MAX_HANDOFF_TEXT_CHARS: usize = 8_000;
const ROTATION_THREAD_RECOVERY_LOOKBACK_LOGS: i64 = 2_000;
const LINEAGE_CLAIM_PREFIX: &str = "__pending_lineage_claim__:";
const LINEAGE_CLAIM_STALE_AFTER_NANOS: u128 = 10 * 60 * 1_000_000_000;
const SHARED_CODEX_HOME_ENTRIES: &[&str] = &[
    CODEX_GLOBAL_STATE_FILE_NAME,
    "AGENTS.md",
    "rules",
    "skills",
    "vendor_imports",
];
const PERSONA_LOCAL_CODEX_HOME_ENTRIES: &[&str] = &["config.toml", "memory"];
const CODEX_GLOBAL_STATE_FILE_NAME: &str = ".codex-global-state.json";
#[cfg(test)]
const ACTIVE_WORKSPACE_ROOTS_KEY: &str = "active-workspace-roots";
#[cfg(test)]
const SAVED_WORKSPACE_ROOTS_KEY: &str = "electron-saved-workspace-roots";
#[cfg(test)]
const PROJECT_ORDER_KEY: &str = "project-order";
#[cfg(test)]
const SESSION_INDEX_FILE_NAME: &str = concat!("session_", "index.jsonl");
const DISABLED_TARGET_ERROR_SNIPPET: &str = "is in a disabled domain and cannot be activated";
const DEBUG_POOL_DRIFT_ENV: &str = "CODEX_ROTATE_DEBUG_POOL_DRIFT";
const PROJECTLESS_THREAD_IDS_KEY: &str = "projectless-thread-ids";
const THREAD_WORKSPACE_ROOT_HINTS_KEY: &str = "thread-workspace-root-hints";
const THREAD_ID_METADATA_KEYS: &[&str] = &[
    "id",
    "thread_id",
    "threadId",
    "threadID",
    "conversation_id",
    "conversationId",
    "local_thread_id",
    "localThreadId",
];

#[cfg(test)]
const LINEAGE_SYNC_CONTRACT: &str = r#"Lineage-sync contract:
- API handoff sync creates different local thread IDs across personas while preserving continuity.
- First materialization uses API handoff/import rather than copying persona-local conversation files.
- Additive sync means one local thread per lineage per persona with no duplicate logical conversations on repeated sync.
- The default runtime executes host sync semantics; dormant VM transport/backend code lives in codex-rotate-vm.
"#;

fn debug_pool_drift_enabled() -> bool {
    std::env::var(DEBUG_POOL_DRIFT_ENV)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn debug_pool_drift_state(label: &str) {
    if !debug_pool_drift_enabled() {
        return;
    }

    let pool_snapshot = load_pool().ok().map(|pool| {
        let active_email = pool
            .accounts
            .get(pool.active_index.min(pool.accounts.len().saturating_sub(1)))
            .map(|entry| entry.email.clone());
        (pool.active_index, active_email)
    });
    let auth_snapshot = resolve_paths()
        .ok()
        .and_then(|paths| load_codex_auth(&paths.codex_auth_file).ok())
        .map(|auth| {
            let summary = summarize_codex_auth(&auth);
            (summary.email, summary.account_id)
        });
    let checkpoint_snapshot = load_rotation_checkpoint().ok().flatten().map(|checkpoint| {
        (
            checkpoint.phase,
            checkpoint.previous_index,
            checkpoint.target_index,
            checkpoint.previous_account_id,
            checkpoint.target_account_id,
        )
    });

    eprintln!(
        "codex-rotate debug [{label}] pool={:?} auth={:?} checkpoint={:?}",
        pool_snapshot, auth_snapshot, checkpoint_snapshot
    );
}

trait RotationBackend {
    fn capture_source_thread_candidates(&self, _port: u16) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    fn activate(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        source_thread_candidates: Vec<String>,
        options: RotationCommandOptions,
    ) -> Result<Vec<ThreadHandoff>>;

    fn rollback_after_failed_activation(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()>;

    fn rotate_next(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        options: RotationCommandOptions,
    ) -> Result<NextResult>;

    fn rotate_prev(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        options: RotationCommandOptions,
    ) -> Result<String>;

    fn relogin(
        &self,
        port: u16,
        selector: &str,
        options: ReloginOptions,
        progress: Option<AutomationProgressCallback>,
    ) -> Result<String>;
}

#[derive(Clone, Copy)]
struct HostBackend;

pub fn current_environment() -> Result<RotationEnvironment> {
    if let Ok(value) = std::env::var("CODEX_ROTATE_ENVIRONMENT") {
        return match value.trim().to_ascii_lowercase().as_str() {
            "host" => Ok(RotationEnvironment::Host),
            "vm" => Ok(RotationEnvironment::Vm),
            other => Err(anyhow!(
                "Unsupported CODEX_ROTATE_ENVIRONMENT value \"{other}\"."
            )),
        };
    }
    Ok(load_rotation_environment_settings()?.environment)
}

fn select_rotation_backend() -> Result<Box<dyn RotationBackend>> {
    match current_environment()? {
        RotationEnvironment::Host => Ok(Box::new(HostBackend)),
        RotationEnvironment::Vm => Err(anyhow!(
            "VM rotation mode has moved to the codex-rotate-vm crate and is not wired into the default runtime."
        )),
    }
}

impl RotationBackend for HostBackend {
    fn capture_source_thread_candidates(&self, port: u16) -> Result<Vec<String>> {
        capture_host_source_thread_candidates(port)
    }

    fn activate(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        source_thread_candidates: Vec<String>,
        options: RotationCommandOptions,
    ) -> Result<Vec<ThreadHandoff>> {
        let paths = resolve_paths()?;
        let activation = activate_host_rotation(
            &paths,
            prepared,
            port,
            progress.as_ref(),
            source_thread_candidates,
            options,
        )?;
        Ok(activation.items)
    }

    fn rollback_after_failed_activation(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let paths = resolve_paths()?;
        rollback_after_failed_host_activation(&paths, prepared, true, port)
    }

    fn rotate_next(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        options: RotationCommandOptions,
    ) -> Result<NextResult> {
        rotate_next_impl(self, port, progress, true, options)
    }

    fn rotate_prev(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
        options: RotationCommandOptions,
    ) -> Result<String> {
        rotate_prev_impl(self, port, progress, options)
    }

    fn relogin(
        &self,
        port: u16,
        selector: &str,
        options: ReloginOptions,
        progress: Option<AutomationProgressCallback>,
    ) -> Result<String> {
        relogin_host(port, selector, options, progress)
    }
}

fn conversation_sync_identity(entry: &AccountEntry) -> String {
    entry
        .persona
        .as_ref()
        .map(|persona| format!("host-persona:{}", persona.persona_id))
        .unwrap_or_else(|| entry.account_id.clone())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RotationCommandOptions {
    pub force_managed_window: bool,
}

pub fn rotate_next(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<NextResult> {
    rotate_next_with_options(port, progress, RotationCommandOptions::default())
}

pub fn rotate_next_without_create(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<NextResult> {
    let _lock = RotationLock::acquire()?;
    rotate_next_impl(
        select_rotation_backend()?.as_ref(),
        port.unwrap_or(DEFAULT_PORT),
        progress,
        false,
        RotationCommandOptions::default(),
    )
}

pub fn rotate_next_with_options(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
) -> Result<NextResult> {
    let _lock = RotationLock::acquire()?;
    select_rotation_backend()?.rotate_next(port.unwrap_or(DEFAULT_PORT), progress, options)
}

pub fn rotate_prev(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    rotate_prev_with_options(port, progress, RotationCommandOptions::default())
}

pub fn rotate_prev_with_options(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
) -> Result<String> {
    let _lock = RotationLock::acquire()?;
    select_rotation_backend()?.rotate_prev(port.unwrap_or(DEFAULT_PORT), progress, options)
}

pub fn rotate_set(
    port: Option<u16>,
    selector: &str,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    rotate_set_with_options(port, selector, progress, RotationCommandOptions::default())
}

pub fn rotate_set_with_options(
    port: Option<u16>,
    selector: &str,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    options: RotationCommandOptions,
) -> Result<String> {
    let _lock = RotationLock::acquire()?;
    let backend = select_rotation_backend()?;
    rotate_set_impl(
        backend.as_ref(),
        port.unwrap_or(DEFAULT_PORT),
        selector,
        progress,
        options,
    )
}

pub fn relogin(
    port: Option<u16>,
    selector: &str,
    options: ReloginOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let _lock = RotationLock::acquire()?;
    select_rotation_backend()?.relogin(port.unwrap_or(DEFAULT_PORT), selector, options, progress)
}

pub fn repair_host_history(
    source_selector: &str,
    target_selectors: &[String],
    all_targets: bool,
    apply: bool,
) -> Result<String> {
    let _lock = RotationLock::acquire()?;
    if current_environment()? != RotationEnvironment::Host {
        return Err(anyhow!(
            "repair-host-history is only supported for host personas."
        ));
    }

    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    let _ = ensure_host_personas_ready(&paths, &mut pool)?;
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let active_entry = pool
        .accounts
        .get(active_index)
        .cloned()
        .ok_or_else(|| anyhow!("No active account is available."))?;

    let source_entry = resolve_pool_account(source_selector)?
        .ok_or_else(|| anyhow!("Unknown source selector \"{source_selector}\"."))?;
    let source_sync_identity = conversation_sync_identity(&source_entry);
    let active_sync_identity = conversation_sync_identity(&active_entry);
    if source_sync_identity != active_sync_identity {
        return Err(anyhow!(
            "repair-host-history currently requires --source to be the active account (active: {}, requested: {}).",
            active_entry.label,
            source_entry.label
        ));
    }
    if source_entry.persona.is_none() {
        return Err(anyhow!("Source account is missing host persona metadata."));
    }

    let port = DEFAULT_PORT;
    let handoffs = export_thread_handoffs_with_identity(
        port,
        &source_entry.account_id,
        &source_sync_identity,
    )?;
    if handoffs.is_empty() {
        let mode = if apply { "apply" } else { "dry-run" };
        return Ok(format!(
            "Repair mode: {mode}\n\nNo conversations found in source account {}.",
            source_entry.label
        ));
    }

    let mode = if apply { "apply" } else { "dry-run" };
    let mut targets = Vec::<AccountEntry>::new();
    let mut seen_target_ids = BTreeSet::<String>::new();
    if all_targets || target_selectors.is_empty() {
        for entry in &pool.accounts {
            let target_sync_identity = conversation_sync_identity(entry);
            if target_sync_identity == source_sync_identity {
                continue;
            }
            if seen_target_ids.insert(target_sync_identity) {
                targets.push(entry.clone());
            }
        }
    } else {
        for selector in target_selectors {
            let target_entry = resolve_pool_account(selector)?
                .ok_or_else(|| anyhow!("Unknown target selector \"{selector}\"."))?;
            let target_sync_identity = conversation_sync_identity(&target_entry);
            if target_sync_identity == source_sync_identity {
                continue;
            }
            if seen_target_ids.insert(target_sync_identity) {
                targets.push(target_entry);
            }
        }
    }
    if targets.is_empty() {
        return Err(anyhow!(
            "No repair targets resolved. Provide --target selectors or choose --all."
        ));
    }

    let mut output = format!(
        "Repair mode: {mode}\nDiscovered {} source conversations from {}.\n\n",
        handoffs.len(),
        source_entry.label
    );
    let mut current_persona = source_entry.clone();

    for target in targets {
        if target.persona.is_none() {
            output.push_str(&format!("- {}: Skipping (no host persona)\n", target.label));
            continue;
        }

        let store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;
        let target_sync_identity = conversation_sync_identity(&target);
        let mut planned_creates = 0;
        let mut planned_updates = 0;
        for handoff in &handoffs {
            if store
                .get_local_thread_id(&target_sync_identity, &handoff.lineage_id)?
                .is_some()
            {
                planned_updates += 1;
            } else {
                planned_creates += 1;
            }
        }
        output.push_str(&format!(
            "- {}: {} creates, {} updates planned\n",
            target.label, planned_creates, planned_updates
        ));

        if apply && (planned_creates > 0 || planned_updates > 0) {
            output.push_str(&format!("  Applying to {}...\n", target.label));
            stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
            switch_host_persona(&paths, &current_persona, &target, false)?;
            write_selected_account_auth(&target)?;
            ensure_debug_codex_instance(None, Some(port), None, None)?;

            let transport = HostConversationTransport::new(port);
            let import_outcome =
                import_thread_handoffs(&transport, &target_sync_identity, &handoffs, None)?;
            output.push_str(&format!("  Import result: {}\n", import_outcome.describe()));
            current_persona = target;
        }
    }

    if apply && conversation_sync_identity(&current_persona) != source_sync_identity {
        output.push_str("\nRestoring source persona...\n");
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
        switch_host_persona(&paths, &current_persona, &source_entry, false)?;
        write_selected_account_auth(&source_entry)?;
        ensure_debug_codex_instance(None, Some(port), None, None)?;
    }

    Ok(output)
}

fn is_empty_directory(path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let metadata =
        fs::metadata(path).with_context(|| format!("Failed to inspect {}.", path.display()))?;
    if !metadata.is_dir() {
        return Ok(false);
    }
    Ok(fs::read_dir(path)
        .with_context(|| format!("Failed to read directory {}.", path.display()))?
        .next()
        .is_none())
}

pub fn report_duplicates() -> Result<String> {
    let paths = resolve_paths()?;
    let pool = load_pool()?;
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let account = &pool.accounts[active_index];
    let sync_identity = conversation_sync_identity(account);

    let port = 9333;
    let transport = HostConversationTransport::new(port);
    let threads = transport.list_threads()?;
    let store = ConversationSyncStore::new(&paths.conversation_sync_db_file)?;

    let mut bound_count = 0;
    let mut historical_duplicates = Vec::new();

    for thread_id in threads {
        if store.get_lineage_id(&sync_identity, &thread_id)?.is_some() {
            bound_count += 1;
        } else {
            historical_duplicates.push(thread_id);
        }
    }

    let mut output = format!(
        "Duplicate observability report for {} ({})\n",
        account.label, sync_identity
    );
    output.push_str(&format!(
        "- Bound threads (active lineages): {}\n",
        bound_count
    ));
    output.push_str(&format!(
        "- Potential historical duplicates (unbound active): {}\n",
        historical_duplicates.len()
    ));
    if !historical_duplicates.is_empty() {
        output.push_str("\nHistorical duplicates found (active):\n");
        for id in historical_duplicates {
            output.push_str(&format!("  - {}\n", id));
        }
    }

    Ok(output)
}

#[cfg(test)]
mod tests;
