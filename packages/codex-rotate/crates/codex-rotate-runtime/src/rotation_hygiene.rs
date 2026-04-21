use std::collections::BTreeSet;
use std::fs;
use std::io::{self, BufRead, BufReader, ErrorKind, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::auth::summarize_codex_auth;
use codex_rotate_core::bridge::{
    AutomationProgressCallback, GuestBridgeRequest, GuestBridgeResponse,
};
use codex_rotate_core::pool::{
    load_pool, load_rotation_checkpoint, load_rotation_environment_settings,
    persist_prepared_rotation_pool, prepare_next_rotation_with_progress, prepare_prev_rotation,
    resolve_persona_profile, resolve_pool_account, restore_pool_active_index,
    rollback_prepared_rotation, save_pool, save_rotation_checkpoint, write_selected_account_auth,
    AccountEntry, NextResult, PersonaEntry, PreparedRotation, PreparedRotationAction,
    RotationCheckpoint, RotationCheckpointPhase, RotationEnvironment,
};
use codex_rotate_core::state::RotationLock;
use codex_rotate_core::workflow::{
    cmd_create_with_progress, cmd_generate_browser_fingerprint, cmd_relogin_with_progress,
    CreateCommandOptions, CreateCommandSource, ReloginOptions,
};
use fs2::available_space;
use serde_json::{json, Value};

use crate::launcher::ensure_debug_codex_instance;
use crate::log_isolation::{
    managed_codex_is_running, stop_managed_codex_instance, wait_for_all_threads_idle,
};
use crate::paths::{resolve_paths, RuntimePaths};
use crate::thread_recovery::{read_active_thread_ids, send_codex_app_request};
use crate::watch::read_watch_state;

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
const MAX_HANDOFF_ITEMS: usize = 48;
const MAX_HANDOFF_TEXT_CHARS: usize = 8_000;
const SEED_CODEX_HOME_ENTRIES: &[&str] = &["config.toml", "AGENTS.md", "rules", "skills"];

fn utmctl_binary() -> String {
    std::env::var("CODEX_ROTATE_UTMCTL_BIN")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "utmctl".to_string())
}

fn guest_bridge_request_url() -> String {
    std::env::var("CODEX_ROTATE_GUEST_BRIDGE_URL")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "http://127.0.0.1:9334/request".to_string())
}

fn guest_bridge_bind_addr() -> String {
    std::env::var("CODEX_ROTATE_GUEST_BRIDGE_BIND")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "127.0.0.1:9334".to_string())
}

trait RotationBackend {
    fn activate(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
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
    ) -> Result<NextResult>;

    fn rotate_prev(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
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

#[derive(Clone)]
struct VmBackend {
    config: Option<codex_rotate_core::pool::VmEnvironmentConfig>,
}

pub fn rotate_next(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<NextResult> {
    let _lock = RotationLock::acquire()?;
    select_rotation_backend()?.rotate_next(port.unwrap_or(DEFAULT_PORT), progress)
}

pub fn rotate_prev(
    port: Option<u16>,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    let _lock = RotationLock::acquire()?;
    select_rotation_backend()?.rotate_prev(port.unwrap_or(DEFAULT_PORT), progress)
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

#[derive(Debug, serde::Deserialize)]
struct IncomingGuestBridgeRequest {
    command: String,
    #[serde(default)]
    payload: Value,
}

#[derive(Debug, serde::Serialize)]
struct OutgoingGuestBridgeError {
    message: String,
}

#[derive(Debug, serde::Serialize)]
struct OutgoingGuestBridgeResponse {
    ok: bool,
    #[serde(default)]
    result: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<OutgoingGuestBridgeError>,
}

pub fn run_guest_bridge_server(bind_addr: Option<&str>) -> Result<()> {
    let bind_addr = bind_addr
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(guest_bridge_bind_addr);
    let listener = std::net::TcpListener::bind(&bind_addr)
        .with_context(|| format!("Failed to bind guest bridge at {bind_addr}."))?;
    eprintln!("Codex Rotate guest bridge listening on http://{bind_addr}/request");
    loop {
        let (mut stream, _) = listener.accept().context("Guest bridge accept failed.")?;
        if let Err(error) = handle_guest_bridge_stream(&mut stream) {
            let _ = write_guest_bridge_response(
                &mut stream,
                200,
                &OutgoingGuestBridgeResponse {
                    ok: false,
                    result: Value::Null,
                    error: Some(OutgoingGuestBridgeError {
                        message: format!("{error:#}"),
                    }),
                },
            );
        }
    }
}

fn handle_guest_bridge_stream(stream: &mut std::net::TcpStream) -> Result<()> {
    let mut reader = BufReader::new(
        stream
            .try_clone()
            .context("Failed to clone guest bridge tcp stream.")?,
    );

    let mut request_line = String::new();
    reader
        .read_line(&mut request_line)
        .context("Failed to read guest bridge request line.")?;
    if request_line.trim().is_empty() {
        return Ok(());
    }
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default();

    let mut content_length = 0usize;
    loop {
        let mut header_line = String::new();
        reader
            .read_line(&mut header_line)
            .context("Failed to read guest bridge header.")?;
        let header = header_line.trim();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse::<usize>().unwrap_or(0);
            }
        }
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader
            .read_exact(&mut body)
            .context("Failed to read guest bridge request body.")?;
    }

    if method != "POST" || path != "/request" {
        return write_guest_bridge_response(
            stream,
            404,
            &OutgoingGuestBridgeResponse {
                ok: false,
                result: Value::Null,
                error: Some(OutgoingGuestBridgeError {
                    message: format!("Unsupported guest bridge route: {method} {path}"),
                }),
            },
        );
    }

    let request: IncomingGuestBridgeRequest =
        serde_json::from_slice(&body).context("Failed to parse guest bridge request JSON.")?;
    let response = match handle_guest_bridge_command(&request.command, request.payload) {
        Ok(result) => OutgoingGuestBridgeResponse {
            ok: true,
            result,
            error: None,
        },
        Err(error) => OutgoingGuestBridgeResponse {
            ok: false,
            result: Value::Null,
            error: Some(OutgoingGuestBridgeError {
                message: format!("{error:#}"),
            }),
        },
    };

    write_guest_bridge_response(stream, 200, &response)
}

fn write_guest_bridge_response(
    stream: &mut std::net::TcpStream,
    status_code: u16,
    response: &OutgoingGuestBridgeResponse,
) -> Result<()> {
    let body = serde_json::to_string(response).context("Failed to encode guest bridge JSON.")?;
    let status = match status_code {
        200 => "200 OK",
        404 => "404 Not Found",
        400 => "400 Bad Request",
        _ => "500 Internal Server Error",
    };
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(headers.as_bytes())
        .context("Failed to write guest bridge response headers.")?;
    stream
        .write_all(body.as_bytes())
        .context("Failed to write guest bridge response body.")?;
    stream
        .flush()
        .context("Failed to flush guest bridge response.")?;
    Ok(())
}

fn handle_guest_bridge_command(command: &str, payload: Value) -> Result<Value> {
    match command {
        "ping" => Ok(json!({ "pong": true })),
        "start-codex" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            Ok(json!({}))
        }
        "relogin" => {
            let selector = payload
                .get("selector")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("Guest relogin requires a non-empty selector."))?;
            let options: ReloginOptions = payload
                .get("options")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .context("Guest relogin options were invalid.")?
                .unwrap_or_default();
            let output = cmd_relogin_with_progress(selector, options, None)?;
            let paths = resolve_paths()?;
            let auth = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)?;
            Ok(json!({
                "output": output,
                "auth": auth,
            }))
        }
        "export-thread-handoffs" => {
            let account_id = payload
                .get("account_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("Guest handoff export requires account_id."))?;
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let handoffs = export_thread_handoffs(port, account_id)?;
            Ok(json!({ "handoffs": handoffs }))
        }
        "import-thread-handoffs" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let handoffs: Vec<ThreadHandoff> = payload
                .get("handoffs")
                .cloned()
                .map(serde_json::from_value)
                .transpose()
                .context("Guest handoff import payload was invalid.")?
                .unwrap_or_default();
            let outcome = if handoffs.is_empty() {
                ThreadHandoffImportOutcome::default()
            } else {
                import_thread_handoffs(port, &handoffs, None)?
            };
            Ok(json!({
                "completed_source_thread_ids": outcome.completed_source_thread_ids,
                "failures": outcome.failures.into_iter().map(|failure| {
                    json!({
                        "source_thread_id": failure.source_thread_id,
                        "created_thread_id": failure.created_thread_id,
                        "stage": thread_handoff_import_stage_label(failure.stage),
                        "error": failure.error,
                    })
                }).collect::<Vec<Value>>()
            }))
        }
        _ => Err(anyhow!("Unsupported guest bridge command \"{command}\".")),
    }
}

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
    let settings = load_rotation_environment_settings()?;
    let environment = match std::env::var("CODEX_ROTATE_ENVIRONMENT") {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "host" => RotationEnvironment::Host,
            "vm" => RotationEnvironment::Vm,
            other => {
                return Err(anyhow!(
                    "Unsupported CODEX_ROTATE_ENVIRONMENT value \"{other}\"."
                ))
            }
        },
        Err(_) => settings.environment,
    };
    Ok(match environment {
        RotationEnvironment::Host => Box::new(HostBackend),
        RotationEnvironment::Vm => Box::new(VmBackend {
            config: settings.vm,
        }),
    })
}

impl RotationBackend for HostBackend {
    fn activate(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<Vec<ThreadHandoff>> {
        let paths = resolve_paths()?;
        let activation = activate_host_rotation(&paths, prepared, port, progress.as_ref())?;
        Ok(activation.items)
    }

    fn rollback_after_failed_activation(
        &self,
        prepared: &PreparedRotation,
        port: u16,
        _progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let paths = resolve_paths()?;
        // For host mode, we don't know for sure if it was running before,
        // but we attempt to restore the source persona state anyway if requested.
        // The helper handles it safely.
        rollback_after_failed_host_activation(&paths, prepared, true, port)
    }

    fn rotate_next(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<NextResult> {
        rotate_next_impl(self, port, progress, true)
    }

    fn rotate_prev(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<String> {
        rotate_prev_impl(self, port, progress)
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

impl RotationBackend for VmBackend {
    fn activate(
        &self,
        prepared: &PreparedRotation,
        _port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<Vec<ThreadHandoff>> {
        self.validate_config()?;
        let handoffs = match self.export_guest_handoffs(&prepared.previous.account_id) {
            Ok(handoffs) => handoffs,
            Err(error) => {
                if let Some(progress) = progress.as_ref() {
                    progress(format!(
                        "Skipping VM source handoff export because guest bridge export was unavailable: {error:#}"
                    ));
                }
                Vec::new()
            }
        };
        self.stop_all_persona_vms(progress.as_ref())?;

        let persona = prepared
            .target
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?;

        self.ensure_persona_package_ready(persona)?;
        self.launch_vm(persona, progress.as_ref())?;
        self.start_guest_codex()?;

        if !handoffs.is_empty() {
            self.import_guest_handoffs(&handoffs)?;
        }

        // VM mode imports handoffs in-guest through the bridge, so the host-side
        // shared import stage should no-op for this backend.
        Ok(Vec::new())
    }

    fn rollback_after_failed_activation(
        &self,
        prepared: &PreparedRotation,
        _port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        self.rollback_after_failed_activation(prepared, progress.as_ref())
    }

    fn rotate_next(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<NextResult> {
        self.validate_config()?;
        rotate_next_impl(self, port, progress, true)
    }

    fn rotate_prev(
        &self,
        port: u16,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<String> {
        self.validate_config()?;
        rotate_prev_impl(self, port, progress)
    }

    fn relogin(
        &self,
        _port: u16,
        selector: &str,
        options: ReloginOptions,
        _progress: Option<AutomationProgressCallback>,
    ) -> Result<String> {
        self.validate_config()?;
        let target_account = resolve_pool_account(selector)?.ok_or_else(|| {
            anyhow!(
                "Cannot relogin to non-pool account {} in VM mode.",
                selector
            )
        })?;

        let persona = target_account
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?;

        let previous_pool = load_pool()?;
        let mut pool = previous_pool.clone();
        let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
        let target_index = pool
            .accounts
            .iter()
            .position(|entry| entry.account_id == target_account.account_id)
            .ok_or_else(|| anyhow!("Failed to resolve relogin target {} in the pool.", selector))?;

        codex_rotate_core::pool::persist_prepared_rotation_pool(&PreparedRotation {
            action: PreparedRotationAction::Stay,
            pool: pool.clone(),
            previous_index: active_index,
            target_index: active_index,
            previous: pool.accounts[active_index].clone(),
            target: pool.accounts[active_index].clone(),
            message: String::new(),
            persist_pool: true,
        })?;

        let is_active_account = target_index == active_index;
        let active_persona = pool.accounts[active_index].persona.clone();

        if !is_active_account {
            self.stop_all_persona_vms(None)?;
        }
        self.ensure_persona_package_ready(persona)?;
        self.launch_vm(persona, None)?;

        let restore_active_vm = || {
            self.stop_all_persona_vms(None).ok();
            if let Some(active_persona) = active_persona.as_ref() {
                self.ensure_persona_package_ready(active_persona).ok();
                self.launch_vm(active_persona, None).ok();
                self.start_guest_codex().ok();
            }
        };

        let result = (|| -> Result<String> {
            // Ask guest to perform relogin
            let relogin_response: Value = self.send_guest_request(
                "relogin",
                json!({
                    "selector": selector,
                    "options": options
                }),
            )?;

            let output = relogin_response
                .get("output")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();

            let guest_auth_val = relogin_response
                .get("auth")
                .ok_or_else(|| anyhow!("Guest relogin response did not include auth state."))?;

            let guest_auth: codex_rotate_core::auth::CodexAuth =
                serde_json::from_value(guest_auth_val.clone())
                    .with_context(|| "Failed to parse guest auth state.")?;

            // Sync auth to host pool
            if let Some(entry) = pool
                .accounts
                .iter_mut()
                .find(|a| a.account_id == target_account.account_id)
            {
                entry.auth = guest_auth.clone();
            }
            save_pool(&pool).with_context(|| {
                format!(
                    "Failed to persist guest auth for {} back to the host pool.",
                    target_account.label
                )
            })?;

            // If this is the active account, also update the host's live auth.json
            if is_active_account {
                if let Some(active_entry) = pool.accounts.get(pool.active_index) {
                    if let Err(error) = write_selected_account_auth(active_entry) {
                        let mut failures = vec![format!("host auth sync failed: {error:#}")];
                        if let Err(rollback_error) =
                            rollback_vm_relogin_auth_sync_failure(&previous_pool)
                        {
                            failures.push(format!("rollback failed: {rollback_error:#}"));
                        }
                        return Err(anyhow!(failures.join(" | ")));
                    }
                }
            } else {
                restore_active_vm();
            }

            Ok(output)
        })();

        if result.is_err() && !is_active_account {
            restore_active_vm();
        }

        result
    }
}

impl VmBackend {
    fn export_guest_handoffs(&self, account_id: &str) -> Result<Vec<ThreadHandoff>> {
        let result: GuestThreadHandoffExportResult = self.send_guest_request(
            "export-thread-handoffs",
            json!({
                "account_id": account_id,
            }),
        )?;
        Ok(result.handoffs)
    }

    fn import_guest_handoffs(&self, handoffs: &[ThreadHandoff]) -> Result<()> {
        let result: GuestThreadHandoffImportResult = self.send_guest_request(
            "import-thread-handoffs",
            json!({
                "handoffs": handoffs,
            }),
        )?;
        if result.failures.is_empty() {
            return Ok(());
        }
        Err(anyhow!(
            "Guest handoff import reported {} failure(s).",
            result.failures.len()
        ))
    }

    fn rollback_after_failed_activation(
        &self,
        _prepared: &PreparedRotation,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        if let Some(progress) = progress {
            progress("Rolling back failed VM activation...".to_string());
        }
        // If VM activation failed, we ensure any target VM is stopped.
        self.stop_all_persona_vms(progress)
    }

    fn start_guest_codex(&self) -> Result<()> {
        self.send_guest_request::<Value, Value>("start-codex", json!({}))?;
        Ok(())
    }

    fn send_guest_request<REQ, RES>(&self, command: &str, payload: REQ) -> Result<RES>
    where
        REQ: serde::Serialize,
        RES: serde::de::DeserializeOwned,
    {
        let url = guest_bridge_request_url();

        let client = reqwest::blocking::Client::new();
        let response = client
            .post(url)
            .json(&GuestBridgeRequest { command, payload })
            .send()
            .with_context(|| format!("Failed to send guest request \"{}\".", command))?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Guest request \"{}\" failed with status {}.",
                command,
                response.status()
            ));
        }

        let body: GuestBridgeResponse = response.json().with_context(|| {
            format!(
                "Failed to parse guest response for \"{}\" as GuestBridgeResponse.",
                command
            )
        })?;

        if !body.ok {
            return Err(anyhow!(
                "Guest error in \"{}\": {:#}",
                command,
                body.error
                    .and_then(|error| error.message)
                    .unwrap_or_else(|| "Unknown guest error".to_string())
            ));
        }

        serde_json::from_value(body.result).map_err(|error| {
            anyhow!(
                "Guest response result for \"{}\" was incompatible: {:#}",
                command,
                error
            )
        })
    }

    fn launch_vm(
        &self,
        persona: &PersonaEntry,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let package_path = self.resolve_persona_package_path(persona)?;
        if let Some(progress) = progress {
            progress(format!(
                "Launching VM package at {}...",
                package_path.display()
            ));
        }

        // We use `utmctl start <path>` to launch the VM.
        let status = Command::new(utmctl_binary())
            .arg("start")
            .arg(&package_path)
            .status()
            .with_context(|| {
                format!(
                    "Failed to execute `utmctl start {}`.",
                    package_path.display()
                )
            })?;

        if !status.success() {
            return Err(anyhow!(
                "utmctl start failed (exit code {}).",
                status.code().unwrap_or(-1)
            ));
        }

        // Wait for the VM to be "started" (ready for guest bridge communication)
        self.wait_for_vm_started(persona, progress)?;

        Ok(())
    }

    fn wait_for_vm_started(
        &self,
        persona: &PersonaEntry,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(60);

        if let Some(progress) = progress {
            progress(format!(
                "Waiting for VM \"{}\" to boot...",
                persona.persona_id
            ));
        }

        while start.elapsed() < timeout {
            let output = Command::new(utmctl_binary())
                .arg("status")
                .arg(&persona.persona_id)
                .output()
                .with_context(|| "Failed to execute `utmctl status`.")?;

            if output.status.success() {
                let status = String::from_utf8_lossy(&output.stdout)
                    .trim()
                    .to_lowercase();
                if status == "started" {
                    return Ok(());
                }
            }

            std::thread::sleep(std::time::Duration::from_secs(2));
        }

        Err(anyhow!(
            "Timed out waiting for VM \"{}\" to boot after {}s.",
            persona.persona_id,
            timeout.as_secs()
        ))
    }

    fn stop_all_persona_vms(
        &self,
        progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<()> {
        let output = Command::new(utmctl_binary())
            .arg("list")
            .output()
            .with_context(|| "Failed to execute `utmctl list`.")?;

        if !output.status.success() {
            return Err(anyhow!(
                "utmctl list failed (exit code {}).",
                output.status.code().unwrap_or(-1)
            ));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 3 {
                continue;
            }
            let status = parts[1];
            let name = parts[2..].join(" ");

            if status == "started" && name.starts_with("persona-") {
                if let Some(progress) = progress {
                    progress(format!("Stopping VM \"{}\"...", name));
                }
                let stop_status = Command::new(utmctl_binary())
                    .arg("stop")
                    .arg(parts[0]) // Use UUID
                    .status()
                    .with_context(|| format!("Failed to execute `utmctl stop {}`.", parts[0]))?;

                if !stop_status.success() {
                    return Err(anyhow!(
                        "utmctl stop {} failed (exit code {}).",
                        parts[0],
                        stop_status.code().unwrap_or(-1)
                    ));
                }

                // Wait for the VM to stop with a timeout
                let start = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(30);
                let mut stopped = false;
                while start.elapsed() < timeout {
                    let check_output = Command::new(utmctl_binary())
                        .arg("status")
                        .arg(parts[0])
                        .output();

                    if let Ok(check) = check_output {
                        let check_status =
                            String::from_utf8_lossy(&check.stdout).trim().to_lowercase();
                        if check_status == "stopped"
                            || check_status == "suspended"
                            || check_status.is_empty()
                        {
                            stopped = true;
                            break;
                        }
                    }
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }

                if !stopped {
                    return Err(anyhow!(
                        "Timed out waiting for VM \"{}\" to stop after {}s.",
                        name,
                        timeout.as_secs()
                    ));
                }
            }
        }

        Ok(())
    }

    fn ensure_persona_package_ready(&self, persona: &PersonaEntry) -> Result<()> {
        self.validate_config()?;
        let target_path = self.resolve_persona_package_path(persona)?;
        if target_path.exists() {
            return Ok(());
        }

        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;

        let base_path = config
            .base_package_path
            .as_ref()
            .ok_or_else(|| anyhow!("VM base_package_path is not configured."))?;

        let base_path = PathBuf::from(base_path);
        if !base_path.exists() {
            return Err(anyhow!(
                "VM base package not found at {}.",
                base_path.display()
            ));
        }

        ensure_clone_capacity(&base_path, &target_path)?;

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Clone the base package to the target path.
        // We use `cp -R` to support APFS-friendly cloning (cow) where available.
        let status = Command::new("cp")
            .arg("-R")
            .arg(&base_path)
            .arg(&target_path)
            .status()
            .with_context(|| {
                format!(
                    "Failed to clone VM base package from {} to {}.",
                    base_path.display(),
                    target_path.display()
                )
            })?;

        if !status.success() {
            return Err(anyhow!(
                "Failed to clone VM base package (exit code {}).",
                status.code().unwrap_or(-1)
            ));
        }

        // Materialize BrowserForge-backed browser persona defaults if missing
        if persona.browser_fingerprint.is_none() {
            if let Some(profile) = resolve_persona_profile(
                persona
                    .persona_profile_id
                    .as_deref()
                    .unwrap_or("balanced-us-compact"),
                None,
            ) {
                // Since this is provisioning, we can't call the guest bridge yet (it's not launched).
                // We call the host bridge to generate a deterministic fingerprint for this persona.
                if let Ok(fingerprint) =
                    cmd_generate_browser_fingerprint(&persona.persona_id, &profile)
                {
                    let mut pool = load_pool()?;
                    if let Some(e) = pool.accounts.iter_mut().find(|a| {
                        a.account_id == persona.persona_id
                            || (a
                                .persona
                                .as_ref()
                                .map(|p| p.persona_id == persona.persona_id)
                                .unwrap_or(false))
                    }) {
                        if let Some(p) = e.persona.as_mut() {
                            p.browser_fingerprint = Some(fingerprint);
                            save_pool(&pool)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn resolve_persona_package_path(&self, persona: &PersonaEntry) -> Result<PathBuf> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;

        let persona_root = config
            .persona_root
            .as_ref()
            .ok_or_else(|| anyhow!("VM persona_root is not configured."))?;

        let package_name = validate_vm_persona_id(&persona.persona_id)?;
        Ok(PathBuf::from(persona_root).join(format!("{package_name}.utm")))
    }

    fn validate_config(&self) -> Result<()> {
        let config = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("VM configuration is missing."))?;
        validate_vm_environment_config(config)
    }
}

fn ensure_no_rotation_drift(prepared: &PreparedRotation) -> Result<()> {
    let pool = load_pool()?;
    if pool.active_index != prepared.previous_index {
        return Err(anyhow!(
            "Rotation aborted: pool drift detected (active_index changed from {} to {}).",
            prepared.previous_index,
            pool.active_index
        ));
    }
    Ok(())
}

fn ensure_target_account_still_valid(prepared: &PreparedRotation) -> Result<()> {
    let pool = load_pool()?;
    if !pool
        .accounts
        .iter()
        .any(|a| a.account_id == prepared.target.account_id)
    {
        return Err(anyhow!(
            "Target account {} was removed mid-flow.",
            prepared.target.label
        ));
    }

    let disabled_domains = codex_rotate_core::workflow::load_disabled_rotation_domains()?;
    let domain = codex_rotate_core::workflow::extract_email_domain(&prepared.target.email)
        .unwrap_or_default();
    if disabled_domains.contains(&domain) {
        return Err(anyhow!(
            "Target account {} is in a disabled domain and cannot be activated.",
            prepared.target.label
        ));
    }
    Ok(())
}

fn host_rotation_checkpointing_enabled() -> Result<bool> {
    Ok(matches!(current_environment()?, RotationEnvironment::Host))
}

fn rotation_checkpoint_for_prepared(
    prepared: &PreparedRotation,
    phase: RotationCheckpointPhase,
) -> RotationCheckpoint {
    RotationCheckpoint {
        phase,
        previous_index: prepared.previous_index,
        target_index: prepared.target_index,
        previous_account_id: prepared.previous.account_id.clone(),
        target_account_id: prepared.target.account_id.clone(),
    }
}

fn save_rotation_checkpoint_for_prepared(
    prepared: &PreparedRotation,
    phase: RotationCheckpointPhase,
) -> Result<()> {
    if host_rotation_checkpointing_enabled()? {
        save_rotation_checkpoint(Some(&rotation_checkpoint_for_prepared(prepared, phase)))?;
    }
    Ok(())
}

fn clear_rotation_checkpoint() -> Result<()> {
    save_rotation_checkpoint(None)
}

fn resolve_checkpoint_account_index(
    pool: &codex_rotate_core::pool::Pool,
    account_id: &str,
    fallback_index: usize,
    role: &str,
) -> Result<usize> {
    if let Some(index) = pool
        .accounts
        .iter()
        .position(|entry| entry.account_id == account_id)
    {
        return Ok(index);
    }

    if fallback_index < pool.accounts.len() {
        return Ok(fallback_index);
    }

    Err(anyhow!(
        "Unable to resolve the {role} account for an interrupted rotation."
    ))
}

fn live_root_matches_persona(paths: &RuntimePaths, entry: &AccountEntry) -> Result<bool> {
    let persona = entry
        .persona
        .as_ref()
        .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?;
    let persona_paths = host_persona_paths(paths, persona)?;
    Ok(is_symlink_to(&paths.codex_home, &persona_paths.codex_home)?
        && is_symlink_to(
            &paths.codex_app_support_dir,
            &persona_paths.codex_app_support_dir,
        )?)
}

fn recover_incomplete_rotation_state_without_lock() -> Result<()> {
    let Some(checkpoint) = load_rotation_checkpoint()? else {
        return Ok(());
    };

    let paths = resolve_paths()?;
    let pool = load_pool()?;
    if pool.accounts.is_empty() {
        clear_rotation_checkpoint()?;
        return Ok(());
    }

    let previous_index = resolve_checkpoint_account_index(
        &pool,
        &checkpoint.previous_account_id,
        checkpoint.previous_index,
        "previous",
    )?;
    let target_index = resolve_checkpoint_account_index(
        &pool,
        &checkpoint.target_account_id,
        checkpoint.target_index,
        "target",
    )?;

    if previous_index == target_index {
        clear_rotation_checkpoint()?;
        return Ok(());
    }

    let previous = pool.accounts[previous_index].clone();
    let target = pool.accounts[target_index].clone();
    let target_is_authoritative = match checkpoint.phase {
        RotationCheckpointPhase::Prepare
        | RotationCheckpointPhase::Export
        | RotationCheckpointPhase::Rollback => false,
        RotationCheckpointPhase::Activate => live_root_matches_persona(&paths, &target)?,
        RotationCheckpointPhase::Import | RotationCheckpointPhase::Commit => true,
    };

    if target_is_authoritative {
        switch_host_persona(&paths, &previous, &target, false)?;
        write_selected_account_auth(&target)?;
        restore_pool_active_index(target_index)?;
    } else {
        switch_host_persona(&paths, &target, &previous, false)?;
        write_selected_account_auth(&previous)?;
        restore_pool_active_index(previous_index)?;
    }

    clear_rotation_checkpoint()?;
    Ok(())
}

pub(crate) fn recover_incomplete_rotation_state() -> Result<()> {
    let _lock = RotationLock::acquire()?;
    recover_incomplete_rotation_state_without_lock()
}

fn finalize_rotation_after_import(
    prepared: &PreparedRotation,
    import_outcome: &ThreadHandoffImportOutcome,
) -> Result<()> {
    ensure_no_rotation_drift(prepared)?;
    if !import_outcome.is_complete() {
        return Err(anyhow!(import_outcome.describe()));
    }
    persist_prepared_rotation_pool(prepared)?;
    Ok(())
}

fn rotate_next_impl(
    backend: &dyn RotationBackend,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    allow_create: bool,
) -> Result<NextResult> {
    recover_incomplete_rotation_state_without_lock()?;
    let mut prepared = prepare_next_rotation_with_progress(progress.clone())?;
    let paths = resolve_paths()?;
    let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;

    match prepared.action {
        PreparedRotationAction::Stay => {
            if prepared.persist_pool {
                ensure_no_rotation_drift(&prepared)?;
                persist_prepared_rotation_pool(&prepared)?;
            }
            let summary = summarize_codex_auth(&prepared.target.auth);
            return Ok(NextResult::Stayed {
                message: prepared.message,
                summary,
            });
        }
        PreparedRotationAction::CreateRequired if allow_create => {
            if prepared.persist_pool {
                ensure_no_rotation_drift(&prepared)?;
                persist_prepared_rotation_pool(&prepared)?;
            }
            let create_output = cmd_create_with_progress(
                CreateCommandOptions {
                    force: true,
                    ignore_current: true,
                    require_usable_quota: true,
                    restore_previous_auth_after_create: true,
                    source: CreateCommandSource::Next,
                    ..CreateCommandOptions::default()
                },
                progress.clone(),
            )?;
            restore_pool_active_index(prepared.previous_index)?;
            let next = rotate_next_impl(backend, port, progress, false)?;
            let summary = match &next {
                NextResult::Rotated { summary, .. }
                | NextResult::Stayed { summary, .. }
                | NextResult::Created { summary, .. } => summary.clone(),
            };
            let combined = match next {
                NextResult::Rotated { message, .. }
                | NextResult::Stayed { message, .. }
                | NextResult::Created {
                    output: message, ..
                } => {
                    format!("{}\n{}", create_output.trim_end(), message)
                }
            };
            return Ok(NextResult::Created {
                output: combined,
                summary,
            });
        }
        PreparedRotationAction::CreateRequired => {
            return Err(anyhow!(
                "Auto rotation requires creating a replacement account, but the retry budget is exhausted."
            ));
        }
        PreparedRotationAction::Switch => {}
    }

    ensure_target_account_still_valid(&prepared)?;
    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Activate)?;

    let handoffs = backend
        .activate(&prepared, port, progress.clone())
        .with_context(|| {
            format!(
                "Failed to activate persona {}.",
                prepared
                    .target
                    .persona
                    .as_ref()
                    .map(|persona| persona.persona_id.as_str())
                    .unwrap_or(prepared.target.label.as_str())
            )
        })?;

    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Import)?;

    let import_outcome = if handoffs.is_empty() {
        ThreadHandoffImportOutcome::default()
    } else {
        import_thread_handoffs(port, &handoffs, progress.as_ref())?
    };

    let result = (|| -> Result<()> {
        if let Some(progress) = progress.as_ref() {
            progress(format!("Activated persona for {}.", prepared.target.label));
        }
        finalize_rotation_after_import(&prepared, &import_outcome)?;
        Ok(())
    })();

    if let Err(error) = result {
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    clear_rotation_checkpoint()?;

    Ok(NextResult::Rotated {
        message: prepared.message,
        summary: summarize_codex_auth(&prepared.target.auth),
    })
}

fn rotate_prev_impl(
    backend: &dyn RotationBackend,
    port: u16,
    progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<String> {
    recover_incomplete_rotation_state_without_lock()?;
    let mut prepared = prepare_prev_rotation()?;
    let paths = resolve_paths()?;
    let _ = ensure_host_personas_ready(&paths, &mut prepared.pool)?;
    if prepared.action != PreparedRotationAction::Switch {
        if prepared.persist_pool {
            ensure_no_rotation_drift(&prepared)?;
            persist_prepared_rotation_pool(&prepared)?;
        }
        return Ok(prepared.message);
    }

    ensure_target_account_still_valid(&prepared)?;
    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Activate)?;

    let handoffs = backend.activate(&prepared, port, progress.clone())?;

    save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Import)?;

    let import_outcome = if handoffs.is_empty() {
        ThreadHandoffImportOutcome::default()
    } else {
        import_thread_handoffs(port, &handoffs, progress.as_ref())?
    };

    let result = (|| -> Result<()> {
        if let Some(progress) = progress.as_ref() {
            progress(format!("Activated persona for {}.", prepared.target.label));
        }
        finalize_rotation_after_import(&prepared, &import_outcome)?;
        Ok(())
    })();

    if let Err(error) = result {
        save_rotation_checkpoint_for_prepared(&prepared, RotationCheckpointPhase::Rollback).ok();
        let rollback_result =
            backend.rollback_after_failed_activation(&prepared, port, progress.clone());
        if rollback_result.is_ok() {
            clear_rotation_checkpoint().ok();
        }
        return Err(error);
    }

    clear_rotation_checkpoint()?;

    Ok(prepared.message)
}

fn relogin_host(
    port: u16,
    selector: &str,
    options: ReloginOptions,
    progress: Option<AutomationProgressCallback>,
) -> Result<String> {
    let Some(target_account) = resolve_pool_account(selector)? else {
        return cmd_relogin_with_progress(selector, options, progress);
    };

    // `relogin` already holds the shared rotation lock at the public entry point.
    // Use the no-lock variant here to avoid self-contention.
    recover_incomplete_rotation_state_without_lock()?;

    let paths = resolve_paths()?;
    let mut pool = load_pool()?;
    ensure_host_personas_ready(&paths, &mut pool)?;
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let target_index = pool
        .accounts
        .iter()
        .position(|entry| entry.account_id == target_account.account_id)
        .ok_or_else(|| anyhow!("Failed to resolve relogin target {} in the pool.", selector))?;
    codex_rotate_core::pool::persist_prepared_rotation_pool(&PreparedRotation {
        action: PreparedRotationAction::Stay,
        pool: pool.clone(),
        previous_index: active_index,
        target_index: active_index,
        previous: pool.accounts[active_index].clone(),
        target: pool.accounts[active_index].clone(),
        message: String::new(),
        persist_pool: true,
    })?;
    if target_index == active_index {
        return cmd_relogin_with_progress(selector, options, progress);
    }

    let managed_running_before = managed_codex_is_running(&paths.debug_profile_dir)?;
    if managed_running_before {
        wait_for_all_threads_idle(port, progress.as_ref())?;
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    switch_host_persona(
        &paths,
        &pool.accounts[active_index],
        &pool.accounts[target_index],
        true,
    )?;
    write_selected_account_auth(&pool.accounts[target_index])?;
    let output = cmd_relogin_with_progress(selector, options, progress.clone());
    switch_host_persona(
        &paths,
        &pool.accounts[target_index],
        &pool.accounts[active_index],
        false,
    )?;
    write_selected_account_auth(&pool.accounts[active_index])?;
    if let Ok(mut current_pool) = load_pool() {
        current_pool.active_index = active_index;
        let _ = save_pool(&current_pool);
    }
    if managed_running_before {
        ensure_debug_codex_instance(None, Some(port), None, None)?;
    }
    output
}

#[derive(Debug)]
struct HostRotationActivation {
    items: Vec<ThreadHandoff>,
}

fn activate_host_rotation(
    paths: &RuntimePaths,
    prepared: &PreparedRotation,
    port: u16,
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<HostRotationActivation> {
    let managed_running_before = managed_codex_is_running(&paths.debug_profile_dir)?;
    let handoffs = if managed_running_before {
        if let Some(progress) = progress {
            progress("Waiting for active Codex work to become handoff-safe.".to_string());
        }
        wait_for_all_threads_idle(port, progress)?;
        export_thread_handoffs(port, &prepared.previous.account_id)?
    } else {
        Vec::new()
    };

    if managed_running_before {
        stop_managed_codex_instance(port, &paths.debug_profile_dir)?;
    }

    let transition = (|| -> Result<()> {
        switch_host_persona(paths, &prepared.previous, &prepared.target, true)?;
        write_selected_account_auth(&prepared.target)?;

        Ok(())
    })();

    match transition {
        Ok(_) => {
            if managed_running_before {
                if let Some(progress) = progress {
                    progress(
                        "Restarting managed Codex after committing the target persona.".to_string(),
                    );
                }
                ensure_debug_codex_instance(None, Some(port), None, None).with_context(|| {
                    format!(
                        "Committed host activation for {} but failed to relaunch managed Codex.",
                        prepared.target.label
                    )
                })?;
            }
            Ok(HostRotationActivation { items: handoffs })
        }
        Err(error) => {
            let rollback_error = rollback_after_failed_host_activation(
                paths,
                prepared,
                managed_running_before,
                port,
            );
            if let Err(rollback_error) = rollback_error {
                return Err(anyhow!(
                    "{error} (rollback after failed host activation also failed: {rollback_error:#})"
                ));
            }
            Err(error)
        }
    }
}

fn rollback_after_failed_host_activation(
    paths: &RuntimePaths,
    prepared: &PreparedRotation,
    managed_running_before: bool,
    port: u16,
) -> Result<()> {
    let mut failures = Vec::new();

    if let Err(error) = rollback_prepared_rotation(prepared) {
        failures.push(format!("core rollback failed: {error:#}"));
    }
    if let Err(error) = switch_host_persona(paths, &prepared.target, &prepared.previous, false) {
        failures.push(format!("symlink rollback failed: {error:#}"));
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

fn rollback_vm_relogin_auth_sync_failure(
    previous_pool: &codex_rotate_core::pool::Pool,
) -> Result<()> {
    save_pool(previous_pool)?;
    if let Some(active_entry) = previous_pool.accounts.get(
        previous_pool
            .active_index
            .min(previous_pool.accounts.len().saturating_sub(1)),
    ) {
        write_selected_account_auth(active_entry)?;
    }
    Ok(())
}

fn ensure_host_personas_ready(
    paths: &RuntimePaths,
    pool: &mut codex_rotate_core::pool::Pool,
) -> Result<bool> {
    if pool.accounts.is_empty() {
        return Ok(false);
    }
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let active_entry = pool.accounts[active_index].clone();
    provision_host_persona(paths, &active_entry, None)?;
    ensure_live_root_bindings(paths, &active_entry)?;
    Ok(false)
}

fn provision_host_persona(
    paths: &RuntimePaths,
    entry: &AccountEntry,
    seed_from: Option<&AccountEntry>,
) -> Result<()> {
    let persona = entry
        .persona
        .as_ref()
        .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?;
    let target = host_persona_paths(paths, persona)?;
    fs::create_dir_all(&target.root)
        .with_context(|| format!("Failed to create {}.", target.root.display()))?;
    if !target.codex_home.exists() {
        fs::create_dir_all(&target.codex_home)?;
        if let Some(source_entry) = seed_from {
            let source = host_persona_paths(
                paths,
                source_entry
                    .persona
                    .as_ref()
                    .ok_or_else(|| anyhow!("Source account is missing persona metadata."))?,
            )?;
            copy_allowlisted_entries(
                &source.codex_home,
                &target.codex_home,
                SEED_CODEX_HOME_ENTRIES,
            )?;
        }
    }
    fs::create_dir_all(&target.fast_browser_home)?;
    fs::create_dir_all(&target.codex_app_support_dir)?;
    fs::create_dir_all(&target.debug_profile_dir)?;

    // Materialize BrowserForge-backed browser persona defaults if missing
    if entry
        .persona
        .as_ref()
        .map(|p| p.browser_fingerprint.is_none())
        .unwrap_or(false)
    {
        let persona_entry = entry.persona.as_ref().unwrap();
        if let Some(profile) = resolve_persona_profile(
            persona_entry
                .persona_profile_id
                .as_deref()
                .unwrap_or("balanced-us-compact"),
            None,
        ) {
            if let Ok(fingerprint) =
                cmd_generate_browser_fingerprint(&persona_entry.persona_id, &profile)
            {
                let mut pool = load_pool()?;
                if let Some(e) = pool
                    .accounts
                    .iter_mut()
                    .find(|a| a.account_id == entry.account_id)
                {
                    if let Some(p) = e.persona.as_mut() {
                        p.browser_fingerprint = Some(fingerprint);
                        save_pool(&pool)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn ensure_live_root_bindings(paths: &RuntimePaths, entry: &AccountEntry) -> Result<()> {
    let persona = host_persona_paths(
        paths,
        entry
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?,
    )?;
    migrate_live_root_if_needed(&paths.codex_home, &persona.codex_home)?;
    migrate_live_root_if_needed(&paths.codex_app_support_dir, &persona.codex_app_support_dir)?;
    ensure_symlink_dir(&paths.codex_home, &persona.codex_home)?;
    ensure_symlink_dir(&paths.codex_app_support_dir, &persona.codex_app_support_dir)?;
    Ok(())
}

fn switch_host_persona(
    paths: &RuntimePaths,
    source_entry: &AccountEntry,
    target_entry: &AccountEntry,
    allow_seed: bool,
) -> Result<()> {
    provision_host_persona(paths, target_entry, allow_seed.then_some(source_entry))?;
    let target = host_persona_paths(
        paths,
        target_entry
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?,
    )?;
    ensure_symlink_dir(&paths.codex_home, &target.codex_home)?;
    ensure_symlink_dir(&paths.codex_app_support_dir, &target.codex_app_support_dir)?;
    Ok(())
}

fn export_thread_handoffs(port: u16, account_id: &str) -> Result<Vec<ThreadHandoff>> {
    let mut thread_ids = read_active_thread_ids(Some(port))?;
    if let Ok(watch_state) = read_watch_state() {
        if let Some(account_state) = watch_state.accounts.get(account_id) {
            thread_ids.extend(
                account_state
                    .thread_recovery_pending_events
                    .iter()
                    .map(|event| event.thread_id.clone()),
            );
        }
    }
    let mut unique = BTreeSet::new();
    let mut handoffs = Vec::new();
    for thread_id in thread_ids {
        if !unique.insert(thread_id.clone()) {
            continue;
        }
        if let Some(handoff) = export_single_thread_handoff(port, &thread_id)? {
            handoffs.push(handoff);
        }
    }
    Ok(handoffs)
}

fn export_single_thread_handoff(port: u16, thread_id: &str) -> Result<Option<ThreadHandoff>> {
    let response: Value = send_codex_app_request(
        port,
        "thread/read",
        json!({ "threadId": thread_id, "includeTurns": true }),
    )?;
    let Some(thread) = response.get("thread") else {
        return Ok(None);
    };
    let cwd = thread
        .get("cwd")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let preview = thread
        .get("preview")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut items = Vec::new();
    for turn in thread
        .get("turns")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        for item in turn
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(mapped) = map_thread_item_to_response_item(item) {
                items.push(mapped);
            }
        }
    }
    if items.len() > MAX_HANDOFF_ITEMS {
        items = items.split_off(items.len().saturating_sub(MAX_HANDOFF_ITEMS));
    }
    Ok(Some(ThreadHandoff {
        source_thread_id: thread_id.to_string(),
        cwd,
        items,
        continue_prompt: Some(format!(
            "Continue this transferred conversation from its latest unfinished state. The prior history came from another isolated persona of the same operator. Do not restate the transfer process; continue the actual work. Latest visible preview: {}",
            truncate_handoff_text(&preview)
        )),
    }))
}

fn import_thread_handoffs(
    port: u16,
    handoffs: &[ThreadHandoff],
    progress: Option<&Arc<dyn Fn(String) + Send + Sync>>,
) -> Result<ThreadHandoffImportOutcome> {
    let mut outcome = ThreadHandoffImportOutcome::default();
    for handoff in handoffs {
        if let Some(progress) = progress {
            progress(format!(
                "Restoring transferred thread {}.",
                handoff.source_thread_id
            ));
        }
        let response: Value = match send_codex_app_request(
            port,
            "thread/start",
            json!({
                "cwd": handoff.cwd,
                "model": Value::Null,
                "modelProvider": Value::Null,
                "serviceTier": Value::Null,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandbox": Value::Null,
                "personality": "pragmatic",
            }),
        ) {
            Ok(response) => response,
            Err(error) => {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: None,
                    stage: ThreadHandoffImportFailureStage::Start,
                    error: error.to_string(),
                });
                continue;
            }
        };
        let new_thread_id = match response
            .get("thread")
            .and_then(|thread| thread.get("id"))
            .and_then(Value::as_str)
        {
            Some(thread_id) => thread_id.to_string(),
            None => {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: None,
                    stage: ThreadHandoffImportFailureStage::Start,
                    error: "Codex thread/start did not return a thread id.".to_string(),
                });
                continue;
            }
        };
        if !handoff.items.is_empty() {
            if let Err(error) = send_codex_app_request::<Value>(
                port,
                "thread/inject_items",
                json!({
                    "threadId": new_thread_id,
                    "items": handoff.items,
                }),
            ) {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: Some(new_thread_id),
                    stage: ThreadHandoffImportFailureStage::InjectItems,
                    error: error.to_string(),
                });
                continue;
            }
        }
        if let Some(prompt) = handoff.continue_prompt.as_deref() {
            if let Err(error) = send_codex_app_request::<Value>(
                port,
                "turn/start",
                json!({
                    "threadId": new_thread_id,
                    "input": [
                        {
                            "type": "text",
                            "text": prompt,
                            "text_elements": [],
                        }
                    ],
                    "cwd": handoff.cwd,
                    "approvalPolicy": Value::Null,
                    "approvalsReviewer": "user",
                    "sandboxPolicy": Value::Null,
                    "model": Value::Null,
                    "serviceTier": Value::Null,
                    "effort": Value::Null,
                    "summary": "none",
                    "personality": "pragmatic",
                    "outputSchema": Value::Null,
                    "collaborationMode": Value::Null,
                    "attachments": [],
                }),
            ) {
                outcome.failures.push(ThreadHandoffImportFailure {
                    source_thread_id: handoff.source_thread_id.clone(),
                    created_thread_id: Some(new_thread_id),
                    stage: ThreadHandoffImportFailureStage::TurnStart,
                    error: error.to_string(),
                });
                continue;
            }
        }
        outcome
            .completed_source_thread_ids
            .push(handoff.source_thread_id.clone());
    }
    Ok(outcome)
}

fn map_thread_item_to_response_item(item: &Value) -> Option<Value> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    match item_type {
        "userMessage" => {
            let text = item
                .get("content")
                .and_then(Value::as_array)
                .map(|content| {
                    content
                        .iter()
                        .filter_map(user_input_to_text)
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            (!text.trim().is_empty()).then(|| {
                json!({
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": truncate_handoff_text(&text),
                        }
                    ]
                })
            })
        }
        "agentMessage" => item
            .get("text")
            .and_then(Value::as_str)
            .map(assistant_message_item),
        "plan" => item
            .get("text")
            .and_then(Value::as_str)
            .map(|text| assistant_message_item(&format!("[Plan]\n{text}"))),
        "reasoning" => {
            let summary = item
                .get("summary")
                .and_then(Value::as_array)
                .map(|values| {
                    values
                        .iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .unwrap_or_default();
            (!summary.is_empty())
                .then(|| assistant_message_item(&format!("[Reasoning Summary]\n{summary}")))
        }
        "commandExecution" => {
            let command = item.get("command").and_then(Value::as_str).unwrap_or("");
            let output = item
                .get("aggregatedOutput")
                .and_then(Value::as_str)
                .unwrap_or("");
            let combined = if output.trim().is_empty() {
                format!("[Command]\n{command}")
            } else {
                format!("[Command]\n{command}\n\n[Output]\n{output}")
            };
            Some(assistant_message_item(&combined))
        }
        _ => None,
    }
}

fn assistant_message_item(text: &str) -> Value {
    json!({
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "output_text",
                "text": truncate_handoff_text(text),
            }
        ]
    })
}

fn user_input_to_text(item: &Value) -> Option<String> {
    match item.get("type").and_then(Value::as_str)? {
        "text" => item
            .get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        "image" => item
            .get("url")
            .and_then(Value::as_str)
            .map(|value| format!("[Image] {value}")),
        "localImage" => item
            .get("path")
            .and_then(Value::as_str)
            .map(|value| format!("[Local image] {value}")),
        "skill" => item
            .get("name")
            .and_then(Value::as_str)
            .map(|value| format!("[Skill] {value}")),
        "mention" => item
            .get("name")
            .and_then(Value::as_str)
            .map(|value| format!("[Mention] {value}")),
        _ => None,
    }
}

fn truncate_handoff_text(value: &str) -> String {
    let mut normalized = value.trim().to_string();
    if normalized.chars().count() > MAX_HANDOFF_TEXT_CHARS {
        normalized = normalized
            .chars()
            .take(MAX_HANDOFF_TEXT_CHARS)
            .collect::<String>();
        normalized.push_str("\n[… truncated]");
    }
    normalized
}

#[derive(Debug)]
struct HostPersonaPaths {
    root: PathBuf,
    codex_home: PathBuf,
    fast_browser_home: PathBuf,
    codex_app_support_dir: PathBuf,
    debug_profile_dir: PathBuf,
}

fn host_persona_paths(
    paths: &RuntimePaths,
    persona: &codex_rotate_core::pool::PersonaEntry,
) -> Result<HostPersonaPaths> {
    let root = if let Some(relative) = persona.host_root_rel_path.as_deref() {
        let relative = require_relative_persona_root(relative, "host_root_rel_path")?;
        paths.rotate_home.join(relative)
    } else {
        paths
            .rotate_home
            .join("personas")
            .join("host")
            .join(&persona.persona_id)
    };
    Ok(HostPersonaPaths {
        codex_home: root.join("codex-home"),
        fast_browser_home: root.join("fast-browser-home"),
        codex_app_support_dir: root.join("codex-app-support"),
        debug_profile_dir: root.join("managed-profile"),
        root,
    })
}

fn require_relative_persona_root(path: &str, field: &str) -> Result<PathBuf> {
    let candidate = PathBuf::from(path.trim());
    if candidate.as_os_str().is_empty() {
        return Err(anyhow!("{field} cannot be empty."));
    }
    if candidate.is_absolute() {
        return Err(anyhow!("{field} must be relative to the rotate home."));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err(anyhow!(
            "{field} cannot contain parent-directory segments or absolute path markers."
        ));
    }
    Ok(candidate)
}

fn copy_allowlisted_entries(
    source_root: &Path,
    target_root: &Path,
    entries: &[&str],
) -> Result<()> {
    for entry in entries {
        let source = source_root.join(entry);
        if !source.exists() {
            continue;
        }
        let target = target_root.join(entry);
        if target.exists() {
            continue;
        }
        copy_path(&source, &target)?;
    }
    Ok(())
}

fn copy_path(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect {}.", source.display()))?;
    if metadata.is_dir() {
        fs::create_dir_all(target)
            .with_context(|| format!("Failed to create {}.", target.display()))?;
        for entry in
            fs::read_dir(source).with_context(|| format!("Failed to read {}.", source.display()))?
        {
            let entry = entry?;
            copy_path(&entry.path(), &target.join(entry.file_name()))?;
        }
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "Failed to copy {} to {}.",
            source.display(),
            target.display()
        )
    })?;
    Ok(())
}

fn migrate_live_root_if_needed(live_path: &Path, target_path: &Path) -> Result<()> {
    if is_symlink_to(live_path, target_path)? {
        fs::create_dir_all(target_path)
            .with_context(|| format!("Failed to create {}.", target_path.display()))?;
        return Ok(());
    }

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }

    if live_path.exists() && !live_path.symlink_metadata()?.file_type().is_symlink() {
        if target_path.exists() {
            if is_empty_directory(target_path)? {
                fs::remove_dir_all(target_path).with_context(|| {
                    format!(
                        "Failed to remove empty migration target {}.",
                        target_path.display()
                    )
                })?;
            } else {
                return Err(anyhow!(
                    "Migration conflict: both live root {} and target persona {} exist as real directories. \
                    This indicates a partially interrupted migration or manual intervention. \
                    Please manually merge any required data from the live root into the persona directory, \
                    then remove the live root so the system can create the required symlink.",
                    live_path.display(),
                    target_path.display()
                ));
            }
        }
        fs::rename(live_path, target_path).with_context(|| {
            format!(
                "Failed to move {} into persona root {}.",
                live_path.display(),
                target_path.display()
            )
        })?;
    }

    if !target_path.exists() {
        fs::create_dir_all(target_path)
            .with_context(|| format!("Failed to create {}.", target_path.display()))?;
    }
    Ok(())
}

fn ensure_symlink_dir(live_path: &Path, target_path: &Path) -> Result<()> {
    ensure_symlink_dir_with(live_path, target_path, symlink_dir)
}

fn ensure_symlink_dir_with<F>(live_path: &Path, target_path: &Path, mut symlink_fn: F) -> Result<()>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    if is_symlink_to(live_path, target_path)? {
        return Ok(());
    }
    if let Some(parent) = live_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let original_target = if live_path.exists() || live_path.is_symlink() {
        let metadata = fs::symlink_metadata(live_path)
            .with_context(|| format!("Failed to inspect {}.", live_path.display()))?;
        if metadata.file_type().is_symlink() {
            let original_target = fs::read_link(live_path)
                .with_context(|| format!("Failed to read symlink {}.", live_path.display()))?;
            fs::remove_file(live_path)
                .with_context(|| format!("Failed to remove symlink {}.", live_path.display()))?;
            Some(original_target)
        } else {
            return Err(anyhow!(
                "Unexpected filesystem shape: Expected {} to be a symlink (or absent), but found a real file or directory. \
                Please remove it so the correct symlink to {} can be established.",
                live_path.display(),
                target_path.display()
            ));
        }
    } else {
        None
    };

    match symlink_fn(target_path, live_path) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(original_target) = original_target.as_ref() {
                let restore_result = symlink_fn(original_target, live_path);
                if let Err(restore_error) = restore_result {
                    return Err(anyhow!(
                        "Failed to replace symlink {} -> {} and restore {} -> {}. Replacement error: {}. Restore error: {}",
                        live_path.display(),
                        target_path.display(),
                        live_path.display(),
                        original_target.display(),
                        error,
                        restore_error
                    ));
                }
            }

            let message = if error.kind() == ErrorKind::PermissionDenied {
                format!(
                    "Permission denied while replacing symlink {} -> {}.",
                    live_path.display(),
                    target_path.display()
                )
            } else {
                format!(
                    "Failed to replace symlink {} -> {}.",
                    live_path.display(),
                    target_path.display()
                )
            };
            Err(anyhow!("{} {}", message, error))
        }
    }
}

fn is_symlink_to(path: &Path, target: &Path) -> Result<bool> {
    if !path.exists() && !path.is_symlink() {
        return Ok(false);
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Failed to inspect {}.", path.display()))?;
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let link_target = fs::read_link(path)
        .with_context(|| format!("Failed to read symlink {}.", path.display()))?;
    Ok(link_target == target)
}

#[cfg(unix)]
fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ThreadHandoff {
    source_thread_id: String,
    cwd: Option<String>,
    items: Vec<Value>,
    continue_prompt: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ThreadHandoffImportOutcome {
    completed_source_thread_ids: Vec<String>,
    failures: Vec<ThreadHandoffImportFailure>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ThreadHandoffImportFailure {
    source_thread_id: String,
    created_thread_id: Option<String>,
    stage: ThreadHandoffImportFailureStage,
    error: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ThreadHandoffImportFailureStage {
    Start,
    InjectItems,
    TurnStart,
}

impl ThreadHandoffImportOutcome {
    fn is_complete(&self) -> bool {
        self.failures.is_empty()
    }

    fn describe(&self) -> String {
        if self.failures.is_empty() {
            return format!(
                "Imported {} transferred thread(s).",
                self.completed_source_thread_ids.len()
            );
        }

        let completed = self.completed_source_thread_ids.len();
        let failed = self.failures.len();
        let failure = &self.failures[0];
        let created_thread = failure
            .created_thread_id
            .as_ref()
            .map(|thread_id| format!(" after creating {}", thread_id))
            .unwrap_or_default();
        let stage = thread_handoff_import_stage_label(failure.stage);
        format!(
            "Partial thread handoff import: {completed} completed, {failed} failed. Source thread {}{created_thread} failed at {stage}: {}",
            failure.source_thread_id, failure.error
        )
    }
}

fn thread_handoff_import_stage_label(stage: ThreadHandoffImportFailureStage) -> &'static str {
    match stage {
        ThreadHandoffImportFailureStage::Start => "thread/start",
        ThreadHandoffImportFailureStage::InjectItems => "thread/inject_items",
        ThreadHandoffImportFailureStage::TurnStart => "turn/start",
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct GuestThreadHandoffExportResult {
    #[serde(default)]
    handoffs: Vec<ThreadHandoff>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
struct GuestThreadHandoffImportResult {
    #[serde(default)]
    failures: Vec<Value>,
}

fn validate_vm_environment_config(
    config: &codex_rotate_core::pool::VmEnvironmentConfig,
) -> Result<()> {
    let base_package_path = require_absolute_existing_directory(
        config
            .base_package_path
            .as_deref()
            .ok_or_else(|| anyhow!("VM base_package_path is not configured."))?,
        "VM base_package_path",
    )?;
    let persona_root = require_absolute_path(
        config
            .persona_root
            .as_deref()
            .ok_or_else(|| anyhow!("VM persona_root is not configured."))?,
        "VM persona_root",
    )?;
    let _utm_app_path = require_absolute_existing_directory(
        config
            .utm_app_path
            .as_deref()
            .ok_or_else(|| anyhow!("VM utm_app_path is not configured."))?,
        "VM utm_app_path",
    )?;

    if let Some(bridge_root) = config.bridge_root.as_deref() {
        require_absolute_directory(bridge_root, "VM bridge_root")?;
    }

    if !persona_root.exists() {
        fs::create_dir_all(&persona_root)
            .with_context(|| format!("Failed to create {}.", persona_root.display()))?;
    }
    ensure_apfs_filesystem(&base_package_path, "VM base package")?;
    ensure_apfs_filesystem(&persona_root, "VM persona root")?;
    Ok(())
}

fn require_absolute_existing_directory(path: &str, field: &str) -> Result<PathBuf> {
    let path = require_absolute_path(path, field)?;
    let metadata = fs::metadata(&path)
        .with_context(|| format!("{} does not exist at {}.", field, path.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "{} must be a directory at {}.",
            field,
            path.display()
        ));
    }
    Ok(path)
}

fn require_absolute_directory(path: &str, field: &str) -> Result<PathBuf> {
    let path = require_absolute_path(path, field)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {} parent {}.", field, parent.display()))?;
    }
    if path.exists() {
        let metadata = fs::metadata(&path)
            .with_context(|| format!("Failed to inspect {} at {}.", field, path.display()))?;
        if !metadata.is_dir() {
            return Err(anyhow!(
                "{} must be a directory at {}.",
                field,
                path.display()
            ));
        }
    } else {
        fs::create_dir_all(&path)
            .with_context(|| format!("Failed to create {} at {}.", field, path.display()))?;
    }
    Ok(path)
}

fn require_absolute_path(path: &str, field: &str) -> Result<PathBuf> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Err(anyhow!("{field} cannot be empty."));
    }
    let candidate = PathBuf::from(trimmed);
    if !candidate.is_absolute() {
        return Err(anyhow!("{field} must be an absolute path: {trimmed}."));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(anyhow!(
            "{field} cannot contain parent-directory segments: {trimmed}."
        ));
    }
    Ok(candidate)
}

fn validate_vm_persona_id(persona_id: &str) -> Result<String> {
    let normalized = persona_id.trim();
    if normalized.is_empty() {
        return Err(anyhow!("Persona id cannot be empty."));
    }
    if normalized
        .chars()
        .any(|character| matches!(character, '/' | '\\' | ':'))
    {
        return Err(anyhow!(
            "Persona id {normalized:?} cannot contain path separators or drive prefixes."
        ));
    }
    if normalized.contains("..") {
        return Err(anyhow!(
            "Persona id {normalized:?} cannot contain parent-directory segments."
        ));
    }
    Ok(normalized.to_string())
}

fn ensure_apfs_filesystem(path: &Path, label: &str) -> Result<()> {
    let output = Command::new("mount")
        .output()
        .context("Failed to inspect mounted filesystems.")?;
    if !output.status.success() {
        return Err(anyhow!(
            "Failed to inspect mounted filesystems: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    let path = path.canonicalize().with_context(|| {
        format!(
            "Failed to canonicalize {} for filesystem inspection.",
            path.display()
        )
    })?;
    let mut best_match: Option<(usize, String)> = None;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let Some((_, mount_and_rest)) = line.split_once(" on ") else {
            continue;
        };
        let Some((mount_point, rest)) = mount_and_rest.split_once(" (") else {
            continue;
        };
        let mount_point = Path::new(mount_point);
        if !path.starts_with(mount_point) {
            continue;
        }
        let mount_len = mount_point.as_os_str().len();
        let replace = best_match
            .as_ref()
            .map(|(current_len, _)| mount_len > *current_len)
            .unwrap_or(true);
        if replace {
            let filesystem_type = rest
                .split(',')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            best_match = Some((mount_len, filesystem_type));
        }
    }

    let filesystem_type = best_match
        .map(|(_, filesystem_type)| filesystem_type)
        .ok_or_else(|| {
            anyhow!(
                "Could not determine the filesystem type for {}.",
                path.display()
            )
        })?;

    if filesystem_type != "apfs" {
        return Err(anyhow!(
            "{label} requires APFS-backed storage, but {} is on {}.",
            path.display(),
            filesystem_type
        ));
    }
    Ok(())
}

fn directory_size(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Failed to inspect {}.", path.display()))?;
    if metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }

    let mut size = 0u64;
    for entry in fs::read_dir(path)
        .with_context(|| format!("Failed to read directory {}.", path.display()))?
    {
        let entry = entry?;
        size = size.saturating_add(directory_size(&entry.path())?);
    }
    Ok(size)
}

fn ensure_clone_capacity(base_package_path: &Path, target_root: &Path) -> Result<()> {
    let required_bytes = directory_size(base_package_path)?;
    let target_parent = target_root.parent().unwrap_or(target_root);
    let available_bytes = available_space(target_parent).with_context(|| {
        format!(
            "Failed to determine free space for {}.",
            target_parent.display()
        )
    })?;
    if available_bytes < required_bytes {
        return Err(anyhow!(
            "Not enough free space to provision VM persona at {}: need at least {} bytes, found {} bytes.",
            target_root.display(),
            required_bytes,
            available_bytes
        ));
    }
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_mutex, RecordingUtmctl};
    use codex_rotate_core::pool::{AccountEntry, PersonaEntry};
    use codex_rotate_refresh::FilesystemTracker;
    use codex_rotate_refresh::ProcessTracker;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::process::{Command, Stdio};
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let suffix = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("{}-{}-{}", prefix, std::process::id(), suffix))
    }

    fn write_executable(path: &Path, script: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("create executable parent");
        }
        fs::write(path, script).expect("write executable script");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = fs::metadata(path).expect("stat executable").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(path, permissions).expect("chmod executable");
        }
    }

    fn summarize_pool_state(pool: &codex_rotate_core::pool::Pool) -> String {
        let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
        let active_account = pool
            .accounts
            .get(active_index)
            .map(|entry| format!("{} ({})", entry.label, entry.account_id))
            .unwrap_or_else(|| "none".to_string());
        let account_ids = pool
            .accounts
            .iter()
            .map(|entry| entry.account_id.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "active_index={active_index}, active_account={active_account}, account_ids=[{account_ids}]"
        )
    }

    fn summarize_auth_state(auth: &codex_rotate_core::auth::CodexAuth) -> String {
        format!(
            "account_id={}, last_refresh={}",
            auth.tokens.account_id, auth.last_refresh
        )
    }

    fn summarize_next_result(result: &NextResult) -> String {
        match result {
            NextResult::Rotated { message, summary } => format!(
                "rotated to {} ({}) | {}",
                summary.account_id, summary.email, message
            ),
            NextResult::Stayed { message, summary } => format!(
                "stayed on {} ({}) | {}",
                summary.account_id, summary.email, message
            ),
            NextResult::Created { output, summary } => format!(
                "created for {} ({}) | {}",
                summary.account_id, summary.email, output
            ),
        }
    }

    fn report_sandbox_rotation_lifecycle(
        workspace_root: &Path,
        sandbox_root: &Path,
        live_snapshot_root: &Path,
        usage_url: &str,
        initial_pool: &codex_rotate_core::pool::Pool,
        initial_auth: &codex_rotate_core::auth::CodexAuth,
        first_result: &NextResult,
        first_target_paths: &HostPersonaPaths,
        first_pool_after: &codex_rotate_core::pool::Pool,
        first_auth_after: &codex_rotate_core::auth::CodexAuth,
        first_checkpoint_cleared: bool,
        second_result: &NextResult,
        second_target_paths: &HostPersonaPaths,
        second_pool_after: &codex_rotate_core::pool::Pool,
        second_auth_after: &codex_rotate_core::auth::CodexAuth,
        second_checkpoint_cleared: bool,
        live_accounts_before: &str,
        live_auth_before: &str,
    ) {
        eprintln!();
        eprintln!("=== Sandbox Rotation Lifecycle Report ===");
        eprintln!("Purpose: validate host rotation hygiene in an isolated temp sandbox.");
        eprintln!("Workspace root: {}", workspace_root.display());
        eprintln!("Sandbox root: {}", sandbox_root.display());
        eprintln!("Comparison root: {}", live_snapshot_root.display());
        eprintln!("WHAM usage stub: {usage_url}");
        eprintln!("1. Initial seed");
        eprintln!(
            "  - sandbox/accounts.json before any rotation: {}",
            summarize_pool_state(initial_pool)
        );
        eprintln!(
            "  - sandbox/.codex/auth.json before any rotation: {}",
            summarize_auth_state(initial_auth)
        );
        eprintln!("  - live-snapshot/accounts.json baseline: {live_accounts_before}");
        eprintln!("  - live-snapshot/auth.json baseline: {live_auth_before}");
        eprintln!("  - conversation data is not synthesized in this test; the lifecycle shown here is the real account/auth/symlink/checkpoint path.");

        eprintln!("2. Rotate forward");
        eprintln!("  - {}", summarize_next_result(first_result));
        eprintln!(
            "  - live .codex symlink -> {}",
            first_target_paths.codex_home.display()
        );
        eprintln!(
            "  - live app-support symlink -> {}",
            first_target_paths.codex_app_support_dir.display()
        );
        eprintln!(
            "  - sandbox/accounts.json after forward rotation: {}",
            summarize_pool_state(first_pool_after)
        );
        eprintln!(
            "  - sandbox/.codex/auth.json after forward rotation: {}",
            summarize_auth_state(first_auth_after)
        );
        eprintln!(
            "  - rotation checkpoint cleared after forward rotation: {first_checkpoint_cleared}"
        );

        eprintln!("3. Sync back on the target side");
        eprintln!("  - the sandbox state is now pinned to the target account only.");
        eprintln!("  - this is where any transferred conversation data would continue from the target persona.");

        eprintln!("4. Rotate back");
        eprintln!("  - {}", summarize_next_result(second_result));
        eprintln!(
            "  - live .codex symlink -> {}",
            second_target_paths.codex_home.display()
        );
        eprintln!(
            "  - live app-support symlink -> {}",
            second_target_paths.codex_app_support_dir.display()
        );
        eprintln!(
            "  - sandbox/accounts.json after return rotation: {}",
            summarize_pool_state(second_pool_after)
        );
        eprintln!(
            "  - sandbox/.codex/auth.json after return rotation: {}",
            summarize_auth_state(second_auth_after)
        );
        eprintln!(
            "  - rotation checkpoint cleared after return rotation: {second_checkpoint_cleared}"
        );

        eprintln!("5. Final hygiene checks");
        eprintln!("  - live snapshot files remained unchanged across the full cycle.");
        eprintln!(
            "  - live-snapshot/accounts.json before -> after: unchanged ({live_accounts_before})"
        );
        eprintln!("  - live-snapshot/auth.json before -> after: unchanged ({live_auth_before})");
        eprintln!("  - no state leaked out of the sandbox while the account flipped source -> target -> source.");
        eprintln!("==============================");
        eprintln!();
    }

    struct TestHttpServer {
        shutdown: std::sync::mpsc::Sender<()>,
        handle: Option<thread::JoinHandle<()>>,
        port: u16,
    }

    impl TestHttpServer {
        fn start(response_body: impl Into<String>) -> Result<Self> {
            let response_body = response_body.into();
            let listener = TcpListener::bind("127.0.0.1:0").context("bind test http server")?;
            listener
                .set_nonblocking(true)
                .context("configure test http server")?;
            let port = listener
                .local_addr()
                .context("test http local addr")?
                .port();
            let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
            let handle = thread::spawn(move || loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 4096];
                        let read = stream.read(&mut buffer).unwrap_or(0);
                        let _request = String::from_utf8_lossy(&buffer[..read]);
                        let body = response_body.clone();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            });
            Ok(Self {
                shutdown: shutdown_tx,
                handle: Some(handle),
                port,
            })
        }
    }

    impl Drop for TestHttpServer {
        fn drop(&mut self) {
            let _ = self.shutdown.send(());
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    struct ManagedCodexProcess {
        pid: u32,
        command: String,
        waiter: Option<thread::JoinHandle<()>>,
    }

    impl ManagedCodexProcess {
        fn start(profile_dir: &Path) -> Result<Self> {
            let executable = unique_temp_dir("managed-codex")
                .join("Applications")
                .join("Codex.app")
                .join("Contents")
                .join("MacOS")
                .join("Codex");
            write_executable(
                &executable,
                r#"#!/usr/bin/env python3
import signal
import sys
import time


def exit_cleanly(_signum, _frame):
    sys.exit(0)


signal.signal(signal.SIGTERM, exit_cleanly)
signal.signal(signal.SIGINT, exit_cleanly)
while True:
    time.sleep(1)
"#,
            );
            let command = format!(
                "{} --user-data-dir={}",
                executable.display(),
                profile_dir.display()
            );
            let child = Command::new(&executable)
                .arg(format!("--user-data-dir={}", profile_dir.display()))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .context("spawn fake managed codex")?;
            let pid = child.id();
            let waiter = thread::spawn(move || {
                let mut child = child;
                let _ = child.wait();
            });
            Ok(Self {
                pid,
                command,
                waiter: Some(waiter),
            })
        }

        fn pid(&self) -> u32 {
            self.pid
        }

        fn command(&self) -> &str {
            &self.command
        }
    }

    impl Drop for ManagedCodexProcess {
        fn drop(&mut self) {
            let _ = Command::new("kill")
                .arg("-TERM")
                .arg(self.pid.to_string())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
            if let Some(waiter) = self.waiter.take() {
                let _ = waiter.join();
            }
        }
    }

    fn start_guest_bridge(response_body: impl Into<String>) -> Result<TestHttpServer> {
        TestHttpServer::start(response_body)
    }

    struct RecordingGuestBridge {
        shutdown: std::sync::mpsc::Sender<()>,
        handle: Option<thread::JoinHandle<()>>,
        port: u16,
        commands: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingGuestBridge {
        fn start(command_responses: BTreeMap<String, Value>) -> Result<Self> {
            let listener =
                TcpListener::bind("127.0.0.1:0").context("bind recording guest bridge")?;
            listener
                .set_nonblocking(true)
                .context("configure recording guest bridge")?;
            let port = listener
                .local_addr()
                .context("recording guest bridge local addr")?
                .port();
            let commands = Arc::new(Mutex::new(Vec::new()));
            let commands_for_thread = Arc::clone(&commands);
            let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
            let handle = thread::spawn(move || loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let mut buffer = [0u8; 8192];
                        let read = stream.read(&mut buffer).unwrap_or(0);
                        let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                        let body = request
                            .split("\r\n\r\n")
                            .nth(1)
                            .unwrap_or_default()
                            .to_string();
                        let command = serde_json::from_str::<Value>(&body)
                            .ok()
                            .and_then(|value| {
                                value
                                    .as_object()
                                    .and_then(|record| record.get("command"))
                                    .and_then(Value::as_str)
                                    .map(ToOwned::to_owned)
                            })
                            .unwrap_or_else(|| "<unknown>".to_string());
                        {
                            let mut seen = commands_for_thread
                                .lock()
                                .expect("recording guest bridge command mutex");
                            seen.push(command.clone());
                        }

                        let response_body = command_responses
                            .get(&command)
                            .cloned()
                            .unwrap_or_else(|| json!({"ok": true, "result": {}}));
                        let response_text = response_body.to_string();
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            response_text.len(),
                            response_text
                        );
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                        let _ = stream.shutdown(Shutdown::Both);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(_) => break,
                }
            });
            Ok(Self {
                shutdown: shutdown_tx,
                handle: Some(handle),
                port,
                commands,
            })
        }

        fn commands(&self) -> Vec<String> {
            self.commands
                .lock()
                .expect("recording guest bridge command mutex")
                .clone()
        }
    }

    impl Drop for RecordingGuestBridge {
        fn drop(&mut self) {
            let _ = self.shutdown.send(());
            if let Some(handle) = self.handle.take() {
                let _ = handle.join();
            }
        }
    }

    fn write_fake_utmctl(path: &Path) {
        write_executable(
            path,
            r#"#!/bin/sh
set -eu
case "${1-}" in
  start)
    exit 0
    ;;
  status)
    printf '%s\n' started
    exit 0
    ;;
  list)
    exit 0
    ;;
  stop)
    exit 0
    ;;
  *)
    printf 'unsupported utmctl command: %s\n' "${1-}" >&2
    exit 1
    ;;
esac
"#,
        );
    }

    fn write_fake_codex_bin(path: &Path, log_file: &Path) {
        write_executable(
            path,
            &format!(
                r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> '{log_file}'
exit 91
"#,
                log_file = log_file.display()
            ),
        );
    }

    #[test]
    fn current_environment_defaults_to_host_from_state() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let rotate_home = tempdir().expect("tempdir");
        let previous = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::remove_var("CODEX_ROTATE_ENVIRONMENT");
            std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
        }
        fs::write(
            rotate_home.path().join("accounts.json"),
            serde_json::to_string(&json!({
                "accounts": [],
                "active_index": 0,
            }))
            .expect("serialize state"),
        )
        .expect("write state");

        let environment = current_environment().expect("current environment");
        restore_env("CODEX_ROTATE_ENVIRONMENT", previous);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        assert_eq!(environment, RotationEnvironment::Host);
    }

    #[test]
    fn current_environment_env_override_wins() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let rotate_home = tempdir().expect("tempdir");
        let previous = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
        }

        fs::write(
            rotate_home.path().join("accounts.json"),
            serde_json::to_string(&json!({
                "accounts": [],
                "active_index": 0,
                "environment": "vm",
                "vm": {
                    "basePackagePath": "/vm/base.utm",
                    "personaRoot": "/vm/personas",
                    "utmAppPath": "/Applications/UTM.app"
                }
            }))
            .expect("serialize state"),
        )
        .expect("write state");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", rotate_home.path());
            std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
        }

        let environment = current_environment().expect("current environment");
        restore_env("CODEX_ROTATE_ENVIRONMENT", previous);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        assert_eq!(environment, RotationEnvironment::Host);
    }

    #[test]
    fn live_root_migration_moves_directory_into_persona_and_links_it() {
        let temp = tempdir().expect("tempdir");
        let live = temp.path().join(".codex");
        let target = temp
            .path()
            .join("personas")
            .join("host")
            .join("acct")
            .join("codex-home");
        fs::create_dir_all(&live).expect("create live root");
        fs::write(live.join("config.toml"), "model = \"gpt-5\"\n").expect("write config");

        migrate_live_root_if_needed(&live, &target).expect("migrate root");
        ensure_symlink_dir(&live, &target).expect("link root");

        assert!(target.join("config.toml").exists());
        assert!(is_symlink_to(&live, &target).expect("check symlink"));
    }

    #[test]
    fn live_root_migration_resumes_when_target_directory_is_empty() {
        let temp = tempdir().expect("tempdir");
        let live = temp.path().join(".codex");
        let target = temp
            .path()
            .join("personas")
            .join("host")
            .join("acct")
            .join("codex-home");
        fs::create_dir_all(&live).expect("create live root");
        fs::write(live.join("config.toml"), "model = \"gpt-5\"\n").expect("write config");
        fs::create_dir_all(&target).expect("create empty target");

        migrate_live_root_if_needed(&live, &target).expect("resume migration");
        ensure_symlink_dir(&live, &target).expect("link root");

        assert!(target.join("config.toml").exists());
        assert!(is_symlink_to(&live, &target).expect("check symlink"));
    }

    #[test]
    fn ensure_symlink_dir_repairs_broken_symlink() {
        let temp = tempdir().expect("tempdir");
        let live = temp.path().join(".codex");
        let target = temp.path().join("personas").join("host").join("acct");
        fs::create_dir_all(&target).expect("create target");
        #[cfg(unix)]
        std::os::unix::fs::symlink(temp.path().join("missing"), &live)
            .expect("create broken symlink");
        #[cfg(windows)]
        std::os::windows::fs::symlink_dir(temp.path().join("missing"), &live)
            .expect("create broken symlink");

        ensure_symlink_dir(&live, &target).expect("repair symlink");
        assert!(is_symlink_to(&live, &target).expect("check repaired symlink"));
    }

    #[test]
    fn finalize_rotation_after_import_commits_pool_after_complete_import() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", temp.path());
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let import_outcome = ThreadHandoffImportOutcome {
            completed_source_thread_ids: vec!["thread-source".to_string()],
            failures: Vec::new(),
        };

        finalize_rotation_after_import(&prepared, &import_outcome).expect("finalize import");

        let committed_pool = load_pool().expect("load committed pool");
        assert_eq!(committed_pool.active_index, 1);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    }

    #[test]
    fn finalize_rotation_after_import_rejects_partial_import_without_committing_pool() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", temp.path());
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let import_outcome = ThreadHandoffImportOutcome {
            completed_source_thread_ids: vec!["thread-source".to_string()],
            failures: vec![ThreadHandoffImportFailure {
                source_thread_id: "thread-source".to_string(),
                created_thread_id: Some("thread-target".to_string()),
                stage: ThreadHandoffImportFailureStage::InjectItems,
                error: "permission denied".to_string(),
            }],
        };

        let error = finalize_rotation_after_import(&prepared, &import_outcome)
            .expect_err("partial import should fail");
        assert!(error.to_string().contains("Partial thread handoff import"));

        let committed_pool = load_pool().expect("load committed pool");
        assert_eq!(committed_pool.active_index, 0);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    }

    #[test]
    fn recover_incomplete_rotation_state_repairs_target_authoritative_checkpoint() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("FAST_BROWSER_HOME", paths.fast_browser_home.clone());
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                paths.codex_app_support_dir.clone(),
            );
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        codex_rotate_core::pool::save_rotation_checkpoint(Some(&RotationCheckpoint {
            phase: RotationCheckpointPhase::Import,
            previous_index: 0,
            target_index: 1,
            previous_account_id: source.account_id.clone(),
            target_account_id: target.account_id.clone(),
        }))
        .expect("save checkpoint");

        switch_host_persona(&paths, &source, &target, false).expect("switch persona");
        codex_rotate_core::pool::write_selected_account_auth(&target).expect("write target auth");

        recover_incomplete_rotation_state().expect("recover rotation");

        let recovered_pool = load_pool().expect("load recovered pool");
        assert_eq!(recovered_pool.active_index, 1);
        let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
        let recovered_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(recovered_auth.tokens.account_id, "acct-target");
        assert!(load_rotation_checkpoint()
            .expect("load checkpoint")
            .is_none());

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
    }

    #[test]
    fn recover_incomplete_rotation_state_clears_source_authoritative_checkpoint() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("FAST_BROWSER_HOME", paths.fast_browser_home.clone());
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                paths.codex_app_support_dir.clone(),
            );
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        codex_rotate_core::pool::save_rotation_checkpoint(Some(&RotationCheckpoint {
            phase: RotationCheckpointPhase::Prepare,
            previous_index: 0,
            target_index: 1,
            previous_account_id: source.account_id.clone(),
            target_account_id: target.account_id.clone(),
        }))
        .expect("save checkpoint");

        recover_incomplete_rotation_state().expect("recover rotation");

        let recovered_pool = load_pool().expect("load recovered pool");
        assert_eq!(recovered_pool.active_index, 0);
        let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
        let recovered_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(recovered_auth.tokens.account_id, "acct-source");
        assert!(load_rotation_checkpoint()
            .expect("load checkpoint")
            .is_none());

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
    }

    #[test]
    fn ensure_symlink_dir_restores_original_link_when_replacement_is_denied() {
        let temp = tempdir().expect("tempdir");
        let live = temp.path().join(".codex");
        let original_target = temp.path().join("original");
        let replacement_target = temp.path().join("replacement");
        fs::create_dir_all(&original_target).expect("create original target");
        fs::create_dir_all(&replacement_target).expect("create replacement target");

        symlink_dir(&original_target, &live).expect("create original symlink");

        let mut attempts = 0;
        let result = ensure_symlink_dir_with(&live, &replacement_target, |target, link| {
            attempts += 1;
            if attempts == 1 {
                Err(io::Error::new(
                    ErrorKind::PermissionDenied,
                    "permission denied",
                ))
            } else {
                symlink_dir(target, link)
            }
        });

        let error = result.expect_err("replacement should fail");
        assert!(error
            .to_string()
            .contains("Permission denied while replacing symlink"));
        assert_eq!(attempts, 2);
        assert!(is_symlink_to(&live, &original_target).expect("original symlink restored"));
    }

    #[test]
    fn vm_environment_reports_guarded_backend_entry_points() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let temp = tempdir().expect("tempdir");
        let vm_base = temp.path().join("base.utm");
        let vm_personas = temp.path().join("personas");
        let vm_utm = temp.path().join("UTM.app");
        fs::create_dir_all(&vm_base).expect("create vm base");
        fs::write(vm_base.join("config.plist"), "base").expect("write base config");
        fs::create_dir_all(&vm_personas).expect("create vm personas");
        fs::create_dir_all(&vm_utm).expect("create vm app");

        unsafe {
            std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "vm");
        }

        let env = current_environment().expect("resolve environment");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", temp.path());
        }
        fs::write(
            temp.path().join("accounts.json"),
            serde_json::to_string(&json!({
                "accounts": [],
                "active_index": 0,
                "environment": "vm",
                "vm": {
                    "basePackagePath": vm_base,
                    "personaRoot": vm_personas,
                    "utmAppPath": vm_utm,
                }
            }))
            .expect("serialize vm state"),
        )
        .expect("write vm state");
        let backend = select_rotation_backend().expect("resolve backend");

        restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);

        assert_eq!(env, RotationEnvironment::Vm);

        // We verify it's the VM backend by checking the error message from an unsupported call
        let error = backend
            .relogin(9333, "any", ReloginOptions::default(), None)
            .expect_err("vm backend should be guarded");
        assert!(error
            .to_string()
            .contains("Cannot relogin to non-pool account any in VM mode."));
    }

    #[test]
    fn switch_host_persona_repoints_live_roots_to_target_persona() {
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");

        let source_paths =
            host_persona_paths(&paths, source.persona.as_ref().expect("source persona"))
                .expect("source persona paths");
        let target_paths =
            host_persona_paths(&paths, target.persona.as_ref().expect("target persona"))
                .expect("target persona paths");
        fs::write(source_paths.codex_home.join("history.jsonl"), "source\n")
            .expect("write source history");

        ensure_live_root_bindings(&paths, &source).expect("bind source roots");
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).expect("source symlink"));

        switch_host_persona(&paths, &source, &target, false).expect("switch persona");

        assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).expect("target symlink"));
        assert!(source_paths.codex_home.join("history.jsonl").exists());
    }

    #[test]
    fn rollback_after_failed_host_activation_restores_state_and_symlinks() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        let invalid_account_flow = temp.path().join("missing-workflow.yaml");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        // Simulate partial activation
        switch_host_persona(&paths, &source, &target, true).expect("switch persona");
        codex_rotate_core::pool::write_selected_account_auth(&target).expect("write target auth");

        let target_persona_paths =
            host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &target_persona_paths.codex_home).unwrap());

        rollback_after_failed_host_activation(&paths, &prepared, false, 9333).expect("rollback");

        // Verify restoration
        let source_persona_paths =
            host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &source_persona_paths.codex_home).unwrap());
        let restored_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(restored_auth.tokens.account_id, "acct-source");
        let restored_pool = load_pool().expect("load pool");
        assert_eq!(restored_pool.active_index, 0);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
    }

    #[test]
    fn host_activation_aborts_and_retains_source_when_export_fails() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        // Seed state DB with an active thread so it attempts to connect to the app server
        let runtime_paths = resolve_paths().expect("resolve runtime paths");
        fs::create_dir_all(runtime_paths.codex_state_db_file.parent().unwrap())
            .expect("create state parent");
        let connection =
            rusqlite::Connection::open(&runtime_paths.codex_state_db_file).expect("open state");
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
  ('thread-active', '', 1, 0);
"#,
            )
            .expect("seed state");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let process_guard = ProcessTracker::new()
            .expect("create process tracker")
            .leak_guard("host activation managed codex cleanup");
        let managed_codex =
            ManagedCodexProcess::start(&paths.debug_profile_dir).expect("start managed codex");
        process_guard.record_test_owned_process(
            managed_codex.pid(),
            "managed-codex",
            managed_codex.command(),
        );

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        // Export should fail here due to no listening app server (connection refused)
        let error = activate_host_rotation(&paths, &prepared, 9333, None)
            .expect_err("host activation should fail during export phase");

        let message = format!("{:#}", error);
        assert!(
            message.contains("initial thread/read request failed before relaunch")
                || message.contains("Managed Codex launch is disabled"),
            "Unexpected error message: {}",
            message
        );

        // Verify restoration: pool index remains 0, auth remains source, symlinks remain source
        let restored_pool = load_pool().expect("load pool");
        assert_eq!(restored_pool.active_index, 0);

        let restored_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(restored_auth.tokens.account_id, "acct-source");

        let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());

        drop(managed_codex);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
    }

    #[test]
    fn host_activation_retains_target_state_when_relaunch_fails() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let process_guard = ProcessTracker::new()
            .expect("create process tracker")
            .leak_guard("host activation managed codex cleanup");
        let managed_codex =
            ManagedCodexProcess::start(&paths.debug_profile_dir).expect("start managed codex");
        process_guard.record_test_owned_process(
            managed_codex.pid(),
            "managed-codex",
            managed_codex.command(),
        );

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let error = activate_host_rotation(&paths, &prepared, 9333, None)
            .expect_err("host activation should fail after commit");
        let message = format!("{:#}", error);
        assert!(!message.trim().is_empty());

        let committed_pool = load_pool().expect("load committed pool");
        eprintln!("host activation error: {message}");
        eprintln!(
            "host activation pool: {}",
            fs::read_to_string(paths.rotate_home.join("accounts.json"))
                .expect("read accounts.json")
        );
        assert_eq!(committed_pool.active_index, 0);
        let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
        let restored_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(restored_auth.tokens.account_id, "acct-target");

        drop(managed_codex);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env(
            "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
            previous_disable_launch,
        );
    }

    #[test]
    fn host_activation_rejects_unready_target_without_committing_pool() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let mut target = test_account("acct-target", "persona-target");
        target.persona = None;

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let error = activate_host_rotation(&paths, &prepared, 9333, None)
            .expect_err("host activation should fail before committing pool");
        let message = format!("{:#}", error);
        assert!(message.contains("missing persona metadata"));

        let restored_pool = load_pool().expect("load restored pool");
        assert_eq!(restored_pool.active_index, 0);
        let restored_auth =
            codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file).expect("load auth");
        assert_eq!(restored_auth.tokens.account_id, source.account_id);
        let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap())
            .expect("source persona paths");
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
        assert!(is_symlink_to(
            &paths.codex_app_support_dir,
            &source_paths.codex_app_support_dir
        )
        .unwrap());

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
    }

    #[test]
    fn host_activation_stages_target_without_committing_pool() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let path_guard = FilesystemTracker::new()
            .expect("create filesystem tracker")
            .leak_guard("host activation filesystem cleanup");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let activation =
            activate_host_rotation(&paths, &prepared, 9333, None).expect("host activation");
        assert!(activation.items.is_empty());

        let committed_pool = load_pool().expect("load committed pool");
        assert_eq!(committed_pool.active_index, 0);
        let target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &target_paths.codex_home).unwrap());
        path_guard.record_symlink_target(&target_paths.codex_home, "target codex-home", false);
        path_guard.record_symlink_target(
            &target_paths.codex_app_support_dir,
            "target app-support",
            false,
        );

        drop(temp);
        path_guard
            .assert_clean()
            .expect("host activation targets should be removed");

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
    }

    #[test]
    fn host_sandbox_dry_run_next_preserves_live_snapshot() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let sandbox_root = temp.path().join("sandbox");
        let live_snapshot_root = temp.path().join("live-snapshot");
        fs::create_dir_all(&sandbox_root).expect("create sandbox root");
        fs::create_dir_all(&live_snapshot_root).expect("create live snapshot root");

        let paths = test_runtime_paths(&sandbox_root);
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .canonicalize()
            .expect("workspace root");

        let previous_home = std::env::var_os("HOME");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");

        unsafe {
            std::env::set_var("HOME", temp.path());
            std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
            std::env::set_var("CODEX_ROTATE_REPO_ROOT", &workspace_root);
            std::env::set_var("CODEX_ROTATE_HOME", &paths.rotate_home);
            std::env::set_var("CODEX_HOME", &paths.codex_home);
            std::env::set_var("FAST_BROWSER_HOME", &paths.fast_browser_home);
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                &paths.codex_app_support_dir,
            );
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save sandbox pool");

        let live_accounts = live_snapshot_root.join("accounts.json");
        let live_auth = live_snapshot_root.join("auth.json");
        fs::write(
            &live_accounts,
            serde_json::to_string_pretty(&pool).expect("serialize live pool"),
        )
        .expect("write live accounts");
        fs::write(
            &live_auth,
            serde_json::to_string_pretty(&source.auth).expect("serialize live auth"),
        )
        .expect("write live auth");
        let live_accounts_before = fs::read_to_string(&live_accounts).expect("read live accounts");
        let live_auth_before = fs::read_to_string(&live_auth).expect("read live auth");

        let usage_server = start_guest_bridge(
            json!({
                "user_id": target.account_id.clone(),
                "account_id": target.account_id.clone(),
                "email": target.email.clone(),
                "plan_type": target.plan_type.clone(),
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 10.0,
                        "limit_window_seconds": 3600,
                        "reset_after_seconds": 3600,
                        "reset_at": 2_000_000_000,
                    },
                    "secondary_window": null
                },
                "code_review_rate_limit": null,
                "additional_rate_limits": null,
                "credits": null,
                "promo": null
            })
            .to_string(),
        )
        .expect("start usage server");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE",
                format!("http://127.0.0.1:{}", usage_server.port),
            );
        }

        let first_result = rotate_next(None, None).expect("rotate next");
        match &first_result {
            NextResult::Rotated { message, summary } => {
                assert!(message.contains("ROTATE"));
                assert_eq!(summary.account_id, target.account_id);
            }
            NextResult::Stayed { .. } => panic!("unexpected next result: stayed"),
            NextResult::Created { .. } => panic!("unexpected next result: created"),
        }

        let first_pool_after = load_pool().expect("load sandbox pool after forward rotation");
        assert_eq!(first_pool_after.active_index, 1);
        let first_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
            .expect("load sandbox auth after forward rotation");
        assert_eq!(first_auth_after.tokens.account_id, target.account_id);

        let first_target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap())
            .expect("target persona paths");
        assert!(is_symlink_to(&paths.codex_home, &first_target_paths.codex_home).unwrap());
        assert!(is_symlink_to(
            &paths.codex_app_support_dir,
            &first_target_paths.codex_app_support_dir
        )
        .unwrap());
        let first_checkpoint_cleared = load_rotation_checkpoint()
            .expect("load checkpoint")
            .is_none();
        assert!(first_checkpoint_cleared);

        let second_result = rotate_next(None, None).expect("rotate back to source");
        match &second_result {
            NextResult::Rotated { message, summary } => {
                assert!(message.contains("ROTATE"));
                assert_eq!(summary.account_id, source.account_id);
            }
            NextResult::Stayed { .. } => panic!("unexpected return result: stayed"),
            NextResult::Created { .. } => panic!("unexpected return result: created"),
        }

        let second_pool_after = load_pool().expect("load sandbox pool after return rotation");
        assert_eq!(second_pool_after.active_index, 0);
        let second_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
            .expect("load sandbox auth after return rotation");
        assert_eq!(second_auth_after.tokens.account_id, source.account_id);

        let second_target_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap())
            .expect("source persona paths");
        assert!(is_symlink_to(&paths.codex_home, &second_target_paths.codex_home).unwrap());
        assert!(is_symlink_to(
            &paths.codex_app_support_dir,
            &second_target_paths.codex_app_support_dir
        )
        .unwrap());
        let second_checkpoint_cleared = load_rotation_checkpoint()
            .expect("load checkpoint after return rotation")
            .is_none();
        assert!(second_checkpoint_cleared);

        assert_eq!(
            fs::read_to_string(&live_accounts).expect("read live accounts after lifecycle"),
            live_accounts_before
        );
        assert_eq!(
            fs::read_to_string(&live_auth).expect("read live auth after lifecycle"),
            live_auth_before
        );

        report_sandbox_rotation_lifecycle(
            &workspace_root,
            &sandbox_root,
            &live_snapshot_root,
            &format!("http://127.0.0.1:{}", usage_server.port),
            &pool,
            &source.auth,
            &first_result,
            &first_target_paths,
            &first_pool_after,
            &first_auth_after,
            first_checkpoint_cleared,
            &second_result,
            &second_target_paths,
            &second_pool_after,
            &second_auth_after,
            second_checkpoint_cleared,
            &live_accounts_before,
            &live_auth_before,
        );

        drop(usage_server);
        restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
        restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
        restore_env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
        restore_env(
            "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
            previous_disable_launch,
        );
        restore_env("HOME", previous_home);
    }

    #[test]
    fn host_sandbox_dry_run_prev_restores_live_snapshot() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let sandbox_root = temp.path().join("sandbox");
        let live_snapshot_root = temp.path().join("live-snapshot");
        fs::create_dir_all(&sandbox_root).expect("create sandbox root");
        fs::create_dir_all(&live_snapshot_root).expect("create live snapshot root");

        let paths = test_runtime_paths(&sandbox_root);
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("..")
            .canonicalize()
            .expect("workspace root");

        let previous_home = std::env::var_os("HOME");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_fast_browser_home = std::env::var_os("FAST_BROWSER_HOME");
        let previous_codex_app_support = std::env::var_os("CODEX_ROTATE_CODEX_APP_SUPPORT");
        let previous_repo_root = std::env::var_os("CODEX_ROTATE_REPO_ROOT");
        let previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let previous_usage_url = std::env::var_os("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE");
        let previous_disable_launch = std::env::var_os("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH");

        unsafe {
            std::env::set_var("HOME", temp.path());
            std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "host");
            std::env::set_var("CODEX_ROTATE_REPO_ROOT", &workspace_root);
            std::env::set_var("CODEX_ROTATE_HOME", &paths.rotate_home);
            std::env::set_var("CODEX_HOME", &paths.codex_home);
            std::env::set_var("FAST_BROWSER_HOME", &paths.fast_browser_home);
            std::env::set_var(
                "CODEX_ROTATE_CODEX_APP_SUPPORT",
                &paths.codex_app_support_dir,
            );
            std::env::set_var("CODEX_ROTATE_DISABLE_MANAGED_LAUNCH", "1");
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        fs::create_dir_all(paths.codex_auth_file.parent().unwrap()).expect("create auth parent");
        codex_rotate_core::auth::write_codex_auth(&paths.codex_auth_file, &source.auth)
            .expect("write source auth");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save sandbox pool");

        let live_accounts = live_snapshot_root.join("accounts.json");
        let live_auth = live_snapshot_root.join("auth.json");
        fs::write(
            &live_accounts,
            serde_json::to_string_pretty(&pool).expect("serialize live pool"),
        )
        .expect("write live accounts");
        fs::write(
            &live_auth,
            serde_json::to_string_pretty(&source.auth).expect("serialize live auth"),
        )
        .expect("write live auth");
        let live_accounts_before = fs::read_to_string(&live_accounts).expect("read live accounts");
        let live_auth_before = fs::read_to_string(&live_auth).expect("read live auth");

        let usage_server = start_guest_bridge(
            json!({
                "user_id": target.account_id.clone(),
                "account_id": target.account_id.clone(),
                "email": target.email.clone(),
                "plan_type": target.plan_type.clone(),
                "rate_limit": {
                    "allowed": true,
                    "limit_reached": false,
                    "primary_window": {
                        "used_percent": 10.0,
                        "limit_window_seconds": 3600,
                        "reset_after_seconds": 3600,
                        "reset_at": 2_000_000_000,
                    },
                    "secondary_window": null
                },
                "code_review_rate_limit": null,
                "additional_rate_limits": null,
                "credits": null,
                "promo": null
            })
            .to_string(),
        )
        .expect("start usage server");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE",
                format!("http://127.0.0.1:{}", usage_server.port),
            );
        }

        let first_result = rotate_next(None, None).expect("rotate next");
        match &first_result {
            NextResult::Rotated { message, summary } => {
                assert!(message.contains("ROTATE"));
                assert_eq!(summary.account_id, target.account_id);
            }
            NextResult::Stayed { .. } => panic!("unexpected next result: stayed"),
            NextResult::Created { .. } => panic!("unexpected next result: created"),
        }

        let first_pool_after = load_pool().expect("load sandbox pool after forward rotation");
        assert_eq!(first_pool_after.active_index, 1);
        let first_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
            .expect("load sandbox auth after forward rotation");
        assert_eq!(first_auth_after.tokens.account_id, target.account_id);

        let first_target_paths = host_persona_paths(&paths, target.persona.as_ref().unwrap())
            .expect("target persona paths");
        assert!(is_symlink_to(&paths.codex_home, &first_target_paths.codex_home).unwrap());
        assert!(is_symlink_to(
            &paths.codex_app_support_dir,
            &first_target_paths.codex_app_support_dir
        )
        .unwrap());
        let first_checkpoint_cleared = load_rotation_checkpoint()
            .expect("load checkpoint")
            .is_none();
        assert!(first_checkpoint_cleared);

        let backward_message = rotate_prev(None, None).expect("rotate prev");
        assert!(backward_message.contains("ROTATE"));
        assert!(!backward_message.trim().is_empty());

        let second_pool_after = load_pool().expect("load sandbox pool after prev rotation");
        assert_eq!(second_pool_after.active_index, 0);
        let second_auth_after = codex_rotate_core::auth::load_codex_auth(&paths.codex_auth_file)
            .expect("load sandbox auth after prev rotation");
        assert_eq!(second_auth_after.tokens.account_id, source.account_id);

        let second_target_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap())
            .expect("source persona paths");
        assert!(is_symlink_to(&paths.codex_home, &second_target_paths.codex_home).unwrap());
        assert!(is_symlink_to(
            &paths.codex_app_support_dir,
            &second_target_paths.codex_app_support_dir
        )
        .unwrap());
        let second_checkpoint_cleared = load_rotation_checkpoint()
            .expect("load checkpoint after prev rotation")
            .is_none();
        assert!(second_checkpoint_cleared);

        assert_eq!(
            fs::read_to_string(&live_accounts).expect("read live accounts after lifecycle"),
            live_accounts_before
        );
        assert_eq!(
            fs::read_to_string(&live_auth).expect("read live auth after lifecycle"),
            live_auth_before
        );

        let second_result = NextResult::Rotated {
            message: backward_message,
            summary: summarize_codex_auth(&second_auth_after),
        };
        report_sandbox_rotation_lifecycle(
            &workspace_root,
            &sandbox_root,
            &live_snapshot_root,
            &format!("http://127.0.0.1:{}", usage_server.port),
            &pool,
            &source.auth,
            &first_result,
            &first_target_paths,
            &first_pool_after,
            &first_auth_after,
            first_checkpoint_cleared,
            &second_result,
            &second_target_paths,
            &second_pool_after,
            &second_auth_after,
            second_checkpoint_cleared,
            &live_accounts_before,
            &live_auth_before,
        );

        drop(usage_server);
        restore_env("CODEX_ROTATE_ENVIRONMENT", previous_environment);
        restore_env("CODEX_ROTATE_REPO_ROOT", previous_repo_root);
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("FAST_BROWSER_HOME", previous_fast_browser_home);
        restore_env("CODEX_ROTATE_CODEX_APP_SUPPORT", previous_codex_app_support);
        restore_env("CODEX_ROTATE_WHAM_USAGE_URL_OVERRIDE", previous_usage_url);
        restore_env(
            "CODEX_ROTATE_DISABLE_MANAGED_LAUNCH",
            previous_disable_launch,
        );
        restore_env("HOME", previous_home);
    }

    #[test]
    fn rotation_lock_prevents_concurrent_rotation() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", temp.path());
        }

        let _lock = RotationLock::acquire().expect("acquire first lock");

        let result = rotate_next(None, None);
        let error = match result {
            Ok(_) => panic!("rotate_next should fail due to lock contention"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("Another rotation is already in progress"));
        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    }

    #[test]
    fn relogin_respects_rotation_lock() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        let invalid_account_flow = temp.path().join("missing-workflow.yaml");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
        }

        let source = test_account("acct-source", "persona-source");
        provision_host_persona(&paths, &source, None).expect("provision source");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let _lock = RotationLock::acquire().expect("acquire lock");
        let result = relogin(
            Some(9333),
            "non-pool-selector",
            ReloginOptions::default(),
            None,
        );
        let error = match result {
            Ok(_) => panic!("relogin should fail due to lock contention"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("Another rotation is already in progress"));

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
    }

    #[test]
    fn relogin_pool_selector_does_not_self_contend_on_rotation_lock() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_codex_bin = std::env::var_os("CODEX_ROTATE_CODEX_BIN");
        let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        let invalid_account_flow = temp.path().join("missing-workflow.yaml");
        let fake_codex_log = temp.path().join("fake-codex.log");
        let fake_codex_bin = temp.path().join("bin").join("codex");
        write_fake_codex_bin(&fake_codex_bin, &fake_codex_log);
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex_bin);
            std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");
        fs::write(
            paths.rotate_home.join("accounts.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "version": 9,
                "pending": {
                    target.email.clone(): {
                        "stored": {
                            "email": target.email.clone(),
                            "profile_name": "persona-target",
                            "template": "acct-target@astronlab.com",
                            "suffix": 1,
                            "selector": target.label.clone(),
                            "alias": null,
                            "birth_month": 1,
                            "birth_day": 24,
                            "birth_year": 1990,
                            "created_at": "2026-04-13T02:52:15.012Z",
                            "updated_at": "2026-04-13T02:52:15.012Z"
                        },
                        "started_at": "2026-04-13T02:52:15.012Z"
                    }
                }
            }))
            .expect("serialize credential store"),
        )
        .expect("write credential store");

        let result = relogin(Some(9333), "acct-target", ReloginOptions::default(), None);
        let error = match result {
            Ok(_) => panic!("relogin should fail because workflow file is missing"),
            Err(error) => error.to_string(),
        };
        assert!(
            !error.contains("Another rotation is already in progress"),
            "pool-backed relogin should not self-contend on rotation lock; got: {error}"
        );
        let codex_calls = fs::read_to_string(&fake_codex_log).unwrap_or_default();
        assert!(
            codex_calls.trim().is_empty(),
            "relogin test should not invoke real codex login in this failure path; calls:\n{codex_calls}"
        );

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_CODEX_BIN", previous_codex_bin);
        restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
    }

    #[test]
    fn wait_for_all_threads_idle_reports_unavailable_app_server() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        let runtime_paths = resolve_paths().expect("resolve runtime paths");
        fs::create_dir_all(runtime_paths.codex_state_db_file.parent().unwrap())
            .expect("create state parent");
        let connection =
            rusqlite::Connection::open(&runtime_paths.codex_state_db_file).expect("open state");
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
  ('thread-active', '', 1, 0);
"#,
            )
            .expect("seed state");

        let unavailable_listener =
            TcpListener::bind("127.0.0.1:0").expect("reserve unavailable port");
        let unavailable_port = unavailable_listener
            .local_addr()
            .expect("unavailable port")
            .port();
        drop(unavailable_listener);

        let result = wait_for_all_threads_idle(unavailable_port, None);
        assert!(result.is_err());
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("CDP endpoint failed")
                || message.contains("No Codex page target")
                || message.contains("Failed to query")
                || message.contains("only supported on macOS")
                || message.contains("Managed Codex launch")
        );

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
    }

    #[test]
    fn managed_codex_detection_clears_after_process_exit() {
        let temp = tempdir().expect("tempdir");
        let profile_dir = temp.path().join("managed-profile");
        fs::create_dir_all(&profile_dir).expect("create profile");
        fs::write(profile_dir.join("stale.log"), "stale").expect("write stale state");

        let process_guard = ProcessTracker::new()
            .expect("create process tracker")
            .leak_guard("managed codex detection cleanup");
        let process = ManagedCodexProcess::start(&profile_dir).expect("start managed codex");
        process_guard.record_test_owned_process(process.pid(), "managed-codex", process.command());
        assert!(managed_codex_is_running(&profile_dir).expect("detect running codex"));

        drop(process);
        let mut stopped = false;
        for _ in 0..20 {
            if !managed_codex_is_running(&profile_dir).expect("detect stopped codex") {
                stopped = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(stopped, "managed codex process should stop cleanly");
        stop_managed_codex_instance(9333, &profile_dir).expect("stop should be a no-op");
    }

    #[test]
    fn managed_codex_stop_helper_terminates_running_instance() {
        let temp = tempdir().expect("tempdir");
        let profile_dir = temp.path().join("managed-profile");
        fs::create_dir_all(&profile_dir).expect("create profile");

        let process_guard = ProcessTracker::new()
            .expect("create process tracker")
            .leak_guard("managed codex stop cleanup");
        let process = ManagedCodexProcess::start(&profile_dir).expect("start managed codex");
        process_guard.record_test_owned_process(process.pid(), "managed-codex", process.command());
        assert!(managed_codex_is_running(&profile_dir).expect("detect running codex"));

        stop_managed_codex_instance(9333, &profile_dir).expect("stop running codex");
        let mut stopped = false;
        for _ in 0..20 {
            if !managed_codex_is_running(&profile_dir).expect("detect stopped codex") {
                stopped = true;
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }
        assert!(
            stopped,
            "managed codex process should stop after stop helper"
        );

        drop(process);
        process_guard
            .assert_clean()
            .expect("managed codex should exit cleanly");
    }

    #[test]
    fn guest_bridge_ping_returns_expected_payload() {
        let payload = handle_guest_bridge_command("ping", json!({})).expect("guest ping");
        assert_eq!(payload["pong"], true);
    }

    #[test]
    fn guest_bridge_import_accepts_empty_handoffs() {
        let payload = handle_guest_bridge_command(
            "import-thread-handoffs",
            json!({
                "handoffs": [],
                "port": 9333
            }),
        )
        .expect("guest import");
        assert_eq!(payload["completed_source_thread_ids"], json!([]));
        assert_eq!(payload["failures"], json!([]));
    }

    #[test]
    fn guest_bridge_relogin_requires_selector() {
        let error = handle_guest_bridge_command("relogin", json!({}))
            .expect_err("missing selector should fail");
        assert!(error
            .to_string()
            .contains("Guest relogin requires a non-empty selector"));
    }

    #[test]
    fn rotation_phase_labels_are_actionable() {
        assert_eq!(RotationPhase::Prepare.to_string(), "prepare");
        assert_eq!(RotationPhase::Activate.to_string(), "activate");
        assert_eq!(RotationPhase::Rollback.to_string(), "rollback");
    }

    #[test]
    fn rotation_diagnostics_include_phase_and_context() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let _previous_environment = std::env::var_os("CODEX_ROTATE_ENVIRONMENT");
        let temp_dir = tempdir().expect("tempdir");
        let rotate_home = temp_dir.path().to_path_buf();
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");

        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            std::env::set_var("CODEX_ROTATE_ENVIRONMENT", "vm");
        }

        let persona_id = "persona-target";
        let target = test_account("acct-target", persona_id);
        let source = test_account("acct-source", "persona-source");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        // We verify the diagnostic by calling activate directly on a failing backend.
        // This bypasses rotate_next_impl's need for a correctly initialized pool
        // which is hard to test due to OnceLock caching of CorePaths.
        let backend = VmBackend { config: None };
        let result = backend.activate(&prepared, 9333, None);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);

        let error = result.expect_err("activate should fail");
        let message = format!("{:#}", error);

        // We verify that it reached the VM backend validation path.
        assert!(
            message.contains("VM configuration is missing")
                || message.contains("VM base_package_path")
                || message.contains("VM persona_root")
                || message.contains("VM utm_app_path")
        );
    }
    #[test]
    fn rollback_after_failed_vm_activation_stops_target_vm() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let backend = test_vm_backend(temp.path());

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source.clone(),
            target: target.clone(),
            message: "rotating".to_string(),
            persist_pool: false,
        };

        // It should attempt to call utmctl and fail.
        let result = backend.rollback_after_failed_activation(&prepared, None);
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("utmctl") || error.contains("No such file or directory"));

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
    }
    #[test]
    fn relogin_host_switches_persona_and_restores() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_codex_bin = std::env::var_os("CODEX_ROTATE_CODEX_BIN");
        let previous_account_flow_file = std::env::var_os("CODEX_ROTATE_ACCOUNT_FLOW_FILE");
        let invalid_account_flow = temp.path().join("missing-workflow.yaml");
        let fake_codex_log = temp.path().join("fake-codex.log");
        let fake_codex_bin = temp.path().join("bin").join("codex");
        write_fake_codex_bin(&fake_codex_bin, &fake_codex_log);
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
            std::env::set_var("CODEX_ROTATE_CODEX_BIN", &fake_codex_bin);
            std::env::set_var("CODEX_ROTATE_ACCOUNT_FLOW_FILE", &invalid_account_flow);
        }

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");
        ensure_live_root_bindings(&paths, &source).expect("bind source roots");

        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");
        fs::write(
            paths.rotate_home.join("accounts.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "version": 9,
                "pending": {
                    target.email.clone(): {
                        "stored": {
                            "email": target.email.clone(),
                            "profile_name": "persona-target",
                            "template": "acct-target@astronlab.com",
                            "suffix": 1,
                            "selector": target.label.clone(),
                            "alias": null,
                            "birth_month": 1,
                            "birth_day": 24,
                            "birth_year": 1990,
                            "created_at": "2026-04-13T02:52:15.012Z",
                            "updated_at": "2026-04-13T02:52:15.012Z"
                        },
                        "started_at": "2026-04-13T02:52:15.012Z"
                    }
                }
            }))
            .expect("serialize credential store"),
        )
        .expect("write credential store");

        // Verify initial state
        let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());

        let result = relogin_host(9333, "acct-target", ReloginOptions::default(), None);
        assert!(
            result.is_err(),
            "relogin should fail before browser automation starts"
        );
        let codex_calls = fs::read_to_string(&fake_codex_log).unwrap_or_default();
        assert!(
            codex_calls.trim().is_empty(),
            "host relogin test should not invoke real codex login in this failure path; calls:\n{codex_calls}"
        );

        // Verify restoration after failure/success
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_CODEX_BIN", previous_codex_bin);
        restore_env("CODEX_ROTATE_ACCOUNT_FLOW_FILE", previous_account_flow_file);
    }

    #[test]
    fn map_thread_item_to_response_item_converts_user_and_agent_messages() {
        let user_item = json!({
            "type": "userMessage",
            "content": [
                {
                    "type": "text",
                    "text": "hello"
                }
            ]
        });
        let mapped_user = map_thread_item_to_response_item(&user_item).unwrap();
        assert_eq!(mapped_user["type"], "message");
        assert_eq!(mapped_user["role"], "user");
        assert_eq!(mapped_user["content"][0]["text"], "hello");

        let agent_item = json!({
            "type": "agentMessage",
            "text": "hi there"
        });
        let mapped_agent = map_thread_item_to_response_item(&agent_item).unwrap();
        assert_eq!(mapped_agent["type"], "message");
        assert_eq!(mapped_agent["role"], "assistant");
        assert_eq!(mapped_agent["content"][0]["text"], "hi there");
    }

    #[test]
    fn vm_persona_provisioning_materializes_fingerprint() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());

        let persona = PersonaEntry {
            persona_id: "persona-fingerprint-test".to_string(),
            browser_fingerprint: Some(json!({"seeded": true})),
            ..PersonaEntry::default()
        };

        // Provisioning should succeed even if browser fingerprint synthesis is unavailable.
        let result = backend.ensure_persona_package_ready(&persona);
        assert!(result.is_ok());
    }

    #[test]
    fn vm_backend_attempts_to_relogin_in_guest() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let backend = test_vm_backend(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
        }

        let persona_id = "persona-relogin-test";
        let entry = test_account("acct-1", persona_id);
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![entry.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        // It should attempt to connect to localhost and fail or fail on utmctl.
        let result = backend.relogin(9333, "acct-1", ReloginOptions::default(), None);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("utmctl") || error_msg.contains("guest"));

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
    }

    #[test]
    fn vm_activate_uses_guest_bridge_handoff_export_and_import() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let backend = test_vm_backend(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_utmctl = std::env::var_os("CODEX_ROTATE_UTMCTL_BIN");
        let previous_bridge_url = std::env::var_os("CODEX_ROTATE_GUEST_BRIDGE_URL");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.home_dir.clone());
        }

        let fake_utmctl = temp.path().join("bin").join("utmctl");
        write_fake_utmctl(&fake_utmctl);
        unsafe {
            std::env::set_var("CODEX_ROTATE_UTMCTL_BIN", &fake_utmctl);
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let bridge = RecordingGuestBridge::start(BTreeMap::from([
            (
                "export-thread-handoffs".to_string(),
                json!({
                    "ok": true,
                    "result": {
                        "handoffs": [
                            {
                                "source_thread_id": "thread-source-1",
                                "cwd": null,
                                "items": [],
                                "continue_prompt": "continue"
                            }
                        ]
                    }
                }),
            ),
            ("start-codex".to_string(), json!({"ok": true, "result": {}})),
            (
                "import-thread-handoffs".to_string(),
                json!({
                    "ok": true,
                    "result": {
                        "completed_source_thread_ids": ["thread-source-1"],
                        "failures": []
                    }
                }),
            ),
        ]))
        .expect("start recording guest bridge");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_GUEST_BRIDGE_URL",
                format!("http://127.0.0.1:{}/request", bridge.port),
            );
        }

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source,
            target,
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let handoffs = backend
            .activate(&prepared, 9333, None)
            .expect("activate vm");
        assert!(
            handoffs.is_empty(),
            "vm backend should import handoffs in-guest and return no host-side handoffs"
        );

        let commands = bridge.commands();
        assert!(
            commands.windows(3).any(|window| window
                == [
                    "export-thread-handoffs",
                    "start-codex",
                    "import-thread-handoffs"
                ]),
            "vm activation should export + start + import via guest bridge; got: {commands:?}"
        );

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_UTMCTL_BIN", previous_utmctl);
        restore_env("CODEX_ROTATE_GUEST_BRIDGE_URL", previous_bridge_url);
    }

    #[test]
    fn vm_relogin_non_active_failure_restores_active_vm() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let backend = test_vm_backend(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_utmctl = std::env::var_os("CODEX_ROTATE_UTMCTL_BIN");
        let previous_bridge_url = std::env::var_os("CODEX_ROTATE_GUEST_BRIDGE_URL");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        let recording_utmctl = RecordingUtmctl::install(temp.path()).expect("install utmctl");
        recording_utmctl
            .seed_active_vms(["persona-source"])
            .expect("seed active vm");
        unsafe {
            std::env::set_var("CODEX_ROTATE_UTMCTL_BIN", recording_utmctl.binary_path());
            std::env::set_var(
                "CODEX_ROTATE_GUEST_BRIDGE_URL",
                "http://127.0.0.1:9/request",
            );
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let result = backend.relogin(9333, "acct-target", ReloginOptions::default(), None);
        assert!(result.is_err());

        let utm_calls = recording_utmctl
            .command_log_contents()
            .expect("read utmctl log");
        let target_start = utm_calls.find("persona-target.utm");
        let source_start = utm_calls.rfind("persona-source.utm");
        assert!(
            target_start.is_some(),
            "relogin should launch target vm; calls:\n{utm_calls}"
        );
        assert!(
            source_start.is_some(),
            "failed relogin should relaunch the previously active vm; calls:\n{utm_calls}"
        );
        assert!(
            source_start.unwrap_or_default() > target_start.unwrap_or_default(),
            "active vm should be restored after target relogin attempt; calls:\n{utm_calls}"
        );
        recording_utmctl
            .assert_one_active_vm()
            .expect("one-active-VM invariant");
        let active_vms = recording_utmctl.active_vms().expect("read active vms");
        let expected_active_vms = [String::from("persona-source")].into_iter().collect();
        assert_eq!(active_vms, expected_active_vms);

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_UTMCTL_BIN", previous_utmctl);
        restore_env("CODEX_ROTATE_GUEST_BRIDGE_URL", previous_bridge_url);
    }

    #[test]
    fn recording_utmctl_detects_simultaneous_active_regression() {
        let temp = tempdir().expect("tempdir");
        let recording_utmctl = RecordingUtmctl::install(temp.path()).expect("install utmctl");
        recording_utmctl
            .seed_active_vms(["persona-source"])
            .expect("seed active vm");

        let target_package = temp.path().join("persona-target.utm");
        let status = Command::new(recording_utmctl.binary_path())
            .arg("start")
            .arg(&target_package)
            .status()
            .expect("run fake utmctl start");
        assert!(status.success());

        let error = recording_utmctl
            .assert_one_active_vm()
            .expect_err("overlap should be rejected");
        assert!(error.to_string().contains("simultaneous-active"));
    }

    #[test]
    fn vm_relogin_rolls_back_host_auth_when_sync_fails() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let backend = test_vm_backend(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_utmctl = std::env::var_os("CODEX_ROTATE_UTMCTL_BIN");
        let previous_bridge_url = std::env::var_os("CODEX_ROTATE_GUEST_BRIDGE_URL");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.codex_home.clone());
        }

        let fake_utmctl = temp.path().join("bin").join("utmctl");
        write_fake_utmctl(&fake_utmctl);
        unsafe {
            std::env::set_var("CODEX_ROTATE_UTMCTL_BIN", &fake_utmctl);
        }

        let target = test_account("acct-1", "persona-relogin-test");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let auth_parent = paths.codex_auth_file.parent().unwrap();
        fs::remove_dir_all(auth_parent).ok();
        fs::write(auth_parent, "blocked").expect("block auth parent");

        let mut guest_auth = target.auth.clone();
        guest_auth.last_refresh = "2026-04-18T00:00:00.000Z".to_string();
        let bridge = start_guest_bridge(
            serde_json::json!({
                "ok": true,
                "result": {
                    "output": "guest relogin complete",
                    "auth": guest_auth,
                }
            })
            .to_string(),
        )
        .expect("start guest bridge");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_GUEST_BRIDGE_URL",
                format!("http://127.0.0.1:{}/request", bridge.port),
            );
        }

        let result = backend.relogin(9333, "acct-1", ReloginOptions::default(), None);
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("host auth sync failed"));

        let restored_pool = load_pool().expect("load restored pool");
        assert_eq!(restored_pool.active_index, 0);
        assert_eq!(
            restored_pool.accounts[0].auth.last_refresh,
            target.auth.last_refresh
        );
        assert!(!paths.codex_auth_file.exists());

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_UTMCTL_BIN", previous_utmctl);
        restore_env("CODEX_ROTATE_GUEST_BRIDGE_URL", previous_bridge_url);
    }

    #[test]
    fn vm_backend_attempts_to_send_guest_request() {
        let backend = VmBackend {
            config: Some(codex_rotate_core::pool::VmEnvironmentConfig {
                base_package_path: Some("/vm/base.utm".to_string()),
                persona_root: Some("/vm/personas".to_string()),
                utm_app_path: Some("/Applications/UTM.app".to_string()),
                bridge_root: None,
                expected_egress_mode: codex_rotate_core::pool::VmExpectedEgressMode::ProvisionOnly,
            }),
        };

        // It should attempt to connect to localhost (mocked or default) and fail.
        let result = backend.send_guest_request::<Value, Value>("ping", json!({}));
        assert!(result.is_err());
        let error = result.unwrap_err().to_string().to_lowercase();
        assert!(
            error.contains("connection refused")
                || error.contains("connect")
                || error.contains("incomplete")
                || error.contains("guest")
        );
    }

    #[test]
    fn vm_backend_reports_guest_bridge_version_mismatch() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());

        let previous_bridge_url = std::env::var_os("CODEX_ROTATE_GUEST_BRIDGE_URL");
        let bridge = start_guest_bridge(
            serde_json::json!({
                "ok": true,
                "result": {
                    "unexpected": true
                }
            })
            .to_string(),
        )
        .expect("start bridge");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_GUEST_BRIDGE_URL",
                format!("http://127.0.0.1:{}/request", bridge.port),
            );
        }

        #[derive(Debug, serde::Deserialize)]
        #[allow(dead_code)]
        struct ExpectedGuestResponse {
            version: u32,
        }

        let result = backend.send_guest_request::<Value, ExpectedGuestResponse>("ping", json!({}));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("incompatible"));

        restore_env("CODEX_ROTATE_GUEST_BRIDGE_URL", previous_bridge_url);
    }

    #[test]
    fn vm_activation_reports_guest_codex_unavailable_after_boot() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let backend = test_vm_backend(temp.path());

        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = std::env::var_os("CODEX_HOME");
        let previous_utmctl = std::env::var_os("CODEX_ROTATE_UTMCTL_BIN");
        let previous_bridge_url = std::env::var_os("CODEX_ROTATE_GUEST_BRIDGE_URL");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", paths.rotate_home.clone());
            std::env::set_var("CODEX_HOME", paths.home_dir.clone());
        }

        let fake_utmctl = temp.path().join("bin").join("utmctl");
        write_fake_utmctl(&fake_utmctl);
        unsafe {
            std::env::set_var("CODEX_ROTATE_UTMCTL_BIN", &fake_utmctl);
        }

        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");
        let pool = codex_rotate_core::pool::Pool {
            active_index: 0,
            accounts: vec![source.clone(), target.clone()],
        };
        codex_rotate_core::pool::save_pool(&pool).expect("save pool");

        let bridge = start_guest_bridge(
            serde_json::json!({
                "ok": false,
                "error": {
                    "message": "guest codex unavailable"
                }
            })
            .to_string(),
        )
        .expect("start bridge");
        unsafe {
            std::env::set_var(
                "CODEX_ROTATE_GUEST_BRIDGE_URL",
                format!("http://127.0.0.1:{}/request", bridge.port),
            );
        }

        let prepared = PreparedRotation {
            action: PreparedRotationAction::Switch,
            pool: pool.clone(),
            previous_index: 0,
            target_index: 1,
            previous: source,
            target,
            message: "rotating".to_string(),
            persist_pool: false,
        };

        let error = backend
            .activate(&prepared, 9333, None)
            .expect_err("activate should fail");
        let message = error.to_string();
        assert!(
            message.contains("guest codex unavailable")
                || message.contains("Guest error in \"start-codex\"")
        );

        restore_env("CODEX_ROTATE_HOME", previous_rotate_home);
        restore_env("CODEX_HOME", previous_codex_home);
        restore_env("CODEX_ROTATE_UTMCTL_BIN", previous_utmctl);
        restore_env("CODEX_ROTATE_GUEST_BRIDGE_URL", previous_bridge_url);
    }

    #[test]
    fn vm_backend_attempts_to_launch_vm() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());

        let persona = PersonaEntry {
            persona_id: "persona-test".to_string(),
            browser_fingerprint: Some(json!({"seeded": true})),
            ..PersonaEntry::default()
        };

        // It should attempt to call utmctl and fail because it's not in path.
        let result = backend.launch_vm(&persona, None);
        assert!(result.is_err());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("utmctl") || error.contains("No such file or directory"));
    }

    #[test]
    fn vm_persona_provisioning_clones_from_base() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());

        let persona = PersonaEntry {
            persona_id: "persona-test".to_string(),
            ..PersonaEntry::default()
        };

        let target_path = backend
            .resolve_persona_package_path(&persona)
            .expect("resolve path");

        // First provisioning should clone
        backend
            .ensure_persona_package_ready(&persona)
            .expect("first provision");
        assert!(target_path.exists());
        assert!(target_path.join("config.plist").exists());

        // Second provisioning should be idempotent
        fs::write(target_path.join("config.plist"), "custom").expect("write custom config");
        backend
            .ensure_persona_package_ready(&persona)
            .expect("second provision");
        let config = fs::read_to_string(target_path.join("config.plist")).expect("read config");
        assert_eq!(config, "custom"); // Should NOT have been overwritten
    }

    #[test]
    fn vm_persona_package_resolution_is_deterministic() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());

        let persona = PersonaEntry {
            persona_id: "persona-abc-12345678".to_string(),
            ..PersonaEntry::default()
        };

        let path = backend
            .resolve_persona_package_path(&persona)
            .expect("resolve path");
        assert!(path
            .to_str()
            .unwrap()
            .ends_with("/personas/persona-abc-12345678.utm"));

        // Verify it is deterministic
        let path2 = backend
            .resolve_persona_package_path(&persona)
            .expect("resolve path 2");
        assert_eq!(path, path2);
    }

    #[test]
    fn vm_backend_validates_config_before_activation() {
        let backend = VmBackend { config: None };
        let result = backend.validate_config();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("VM configuration is missing"));
    }

    #[test]
    fn vm_backend_rejects_relative_path_configuration() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend_invalid_relative_paths(temp.path());
        let result = backend.validate_config();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be an absolute path"));
    }

    #[test]
    fn vm_backend_accepts_valid_configuration() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());
        backend.validate_config().expect("valid vm config");
    }

    #[test]
    fn truncate_handoff_text_enforces_max_limit() {
        let long_text = "a".repeat(MAX_HANDOFF_TEXT_CHARS + 10);
        let truncated = truncate_handoff_text(&long_text);
        assert!(truncated.contains("[… truncated]"));
        assert_eq!(
            truncated.chars().count(),
            MAX_HANDOFF_TEXT_CHARS + "\n[… truncated]".chars().count()
        );

        let short_text = "short";
        assert_eq!(truncate_handoff_text(short_text), "short");
    }

    #[test]
    fn ensure_host_personas_ready_repairs_misbound_live_roots() {
        let _env_guard = env_mutex().lock().unwrap_or_else(|e| e.into_inner());
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let source = test_account("acct-source", "persona-source");
        let target = test_account("acct-target", "persona-target");

        provision_host_persona(&paths, &source, None).expect("provision source");
        provision_host_persona(&paths, &target, None).expect("provision target");

        // Bind to target initially
        ensure_live_root_bindings(&paths, &target).expect("bind target roots");

        let mut pool = codex_rotate_core::pool::Pool {
            active_index: 0, // Should be source
            accounts: vec![source.clone(), target.clone()],
        };

        // This should repair the bindings back to source because active_index is 0
        ensure_host_personas_ready(&paths, &mut pool).expect("repair roots");

        let source_paths = host_persona_paths(&paths, source.persona.as_ref().unwrap()).unwrap();
        assert!(is_symlink_to(&paths.codex_home, &source_paths.codex_home).unwrap());
    }

    #[test]
    fn host_persona_paths_rejects_traversal_root() {
        let temp = tempdir().expect("tempdir");
        let paths = test_runtime_paths(temp.path());
        let mut account = test_account("acct-source", "persona-source");
        account.persona.as_mut().unwrap().host_root_rel_path = Some("../escape".to_string());

        let result = host_persona_paths(&paths, account.persona.as_ref().unwrap());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot contain parent-directory segments"));
    }

    #[test]
    fn vm_persona_package_resolution_rejects_unsafe_persona_ids() {
        let temp = tempdir().expect("tempdir");
        let backend = test_vm_backend(temp.path());
        let persona = PersonaEntry {
            persona_id: "../escape".to_string(),
            ..PersonaEntry::default()
        };

        let result = backend.resolve_persona_package_path(&persona);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("cannot contain path separators"));
    }

    fn test_runtime_paths(root: &Path) -> RuntimePaths {
        let home = root.join("home");
        RuntimePaths {
            repo_root: root.to_path_buf(),
            home_dir: home.clone(),
            codex_auth_file: home.join(".codex").join("auth.json"),
            codex_logs_db_file: root.join("logs.sqlite"),
            codex_state_db_file: root.join("state.sqlite"),
            codex_home: home.join(".codex"),
            fast_browser_home: home.join(".fast-browser"),
            codex_app_support_dir: home
                .join("Library")
                .join("Application Support")
                .join("Codex"),
            rotate_home: home.join(".codex-rotate"),
            watch_state_file: home.join(".codex-rotate").join("watch-state.json"),
            debug_profile_dir: home.join(".codex-rotate").join("profile"),
            daemon_socket: home.join(".codex-rotate").join("daemon.sock"),
        }
    }

    fn test_account(account_id: &str, persona_id: &str) -> AccountEntry {
        AccountEntry {
            label: format!("{account_id}_free"),
            alias: None,
            email: format!("{account_id}@astronlab.com"),
            account_id: account_id.to_string(),
            plan_type: "free".to_string(),
            auth: codex_rotate_core::auth::CodexAuth {
                auth_mode: "chatgpt".to_string(),
                openai_api_key: None,
                tokens: codex_rotate_core::auth::AuthTokens {
                    access_token: "header.payload.signature".to_string(),
                    id_token: "header.payload.signature".to_string(),
                    refresh_token: Some("refresh".to_string()),
                    account_id: account_id.to_string(),
                },
                last_refresh: "2026-04-07T00:00:00.000Z".to_string(),
            },
            added_at: "2026-04-07T00:00:00.000Z".to_string(),
            last_quota_usable: Some(true),
            last_quota_summary: Some("5h 90% left".to_string()),
            last_quota_blocker: None,
            last_quota_checked_at: Some("2026-04-07T00:00:00.000Z".to_string()),
            last_quota_primary_left_percent: Some(90),
            last_quota_next_refresh_at: Some("2026-04-07T01:00:00.000Z".to_string()),
            persona: Some(PersonaEntry {
                persona_id: persona_id.to_string(),
                persona_profile_id: Some("balanced-us-compact".to_string()),
                expected_region_code: None,
                ready_at: None,
                host_root_rel_path: Some(format!("personas/host/{persona_id}")),
                vm_package_rel_path: None,
                browser_fingerprint: Some(json!({"seeded": true})),
            }),
        }
    }

    fn restore_env(key: &str, previous: Option<std::ffi::OsString>) {
        match previous {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }
    }

    fn test_vm_backend(root: &Path) -> VmBackend {
        let base_path = root.join("base.utm");
        let persona_root = root.join("personas");
        let utm_app_path = root.join("UTM.app");
        fs::create_dir_all(&base_path).expect("create base");
        fs::write(base_path.join("config.plist"), "base").expect("write base config");
        fs::create_dir_all(&persona_root).expect("create persona root");
        fs::create_dir_all(&utm_app_path).expect("create utm app");

        VmBackend {
            config: Some(codex_rotate_core::pool::VmEnvironmentConfig {
                base_package_path: Some(base_path.to_str().unwrap().to_string()),
                persona_root: Some(persona_root.to_str().unwrap().to_string()),
                utm_app_path: Some(utm_app_path.to_str().unwrap().to_string()),
                bridge_root: None,
                expected_egress_mode: codex_rotate_core::pool::VmExpectedEgressMode::ProvisionOnly,
            }),
        }
    }

    fn test_vm_backend_invalid_relative_paths(root: &Path) -> VmBackend {
        let base_path = root.join("base.utm");
        let persona_root = root.join("personas");
        let utm_app_path = root.join("UTM.app");
        fs::create_dir_all(&base_path).expect("create base");
        fs::write(base_path.join("config.plist"), "base").expect("write base config");
        fs::create_dir_all(&persona_root).expect("create persona root");
        fs::create_dir_all(&utm_app_path).expect("create utm app");

        VmBackend {
            config: Some(codex_rotate_core::pool::VmEnvironmentConfig {
                base_package_path: Some("relative/base.utm".to_string()),
                persona_root: Some(persona_root.to_str().unwrap().to_string()),
                utm_app_path: Some(utm_app_path.to_str().unwrap().to_string()),
                bridge_root: None,
                expected_egress_mode: codex_rotate_core::pool::VmExpectedEgressMode::ProvisionOnly,
            }),
        }
    }
}
