use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use codex_rotate_core::auth::{
    build_login_start_request, load_codex_auth, summarize_codex_auth, AuthSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cdp::{invalidate_local_codex_connection, is_cdp_ready, with_local_codex_connection};
use crate::launcher::ensure_debug_codex_instance;
use crate::paths::resolve_paths;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveAccount {
    #[serde(rename = "type")]
    pub account_type: Option<String>,
    pub email: Option<String>,
    #[serde(rename = "planType")]
    pub plan_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AccountReadResult {
    pub account: Option<LiveAccount>,
    #[serde(rename = "requiresOpenaiAuth")]
    pub requires_openai_auth: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LiveSwitchResult {
    pub email: String,
    #[serde(rename = "planType")]
    pub plan_type: String,
    #[serde(rename = "accountId")]
    pub account_id: String,
}

pub fn read_live_account(port: Option<u16>) -> Result<AccountReadResult> {
    send_mcp_request(port.unwrap_or(9333), "account/read", json!({}))
}

pub fn read_live_account_if_running(port: Option<u16>) -> Result<Option<AccountReadResult>> {
    let port = port.unwrap_or(9333);
    if !is_cdp_ready(port) {
        return Ok(None);
    }
    match send_mcp_request_once(port, "account/read", &json!({})) {
        Ok(value) => Ok(Some(value)),
        Err(_) => Ok(None),
    }
}

pub fn switch_live_account_to_current_auth(
    port: Option<u16>,
    ensure_launched: bool,
    timeout_ms: u64,
) -> Result<LiveSwitchResult> {
    let paths = resolve_paths()?;
    let auth = load_codex_auth(&paths.codex_auth_file)?;
    let expected = build_login_start_request(&auth, Some("local"), None);
    let summary = summarize_codex_auth(&auth);
    let port = port.unwrap_or(9333);
    if ensure_launched {
        ensure_debug_codex_instance(None, Some(port), None, None)?;
    }

    {
        let request = serde_json::to_string(&expected)?;
        let expression = format!(
            "new Promise(async (resolve) => {{ const request = {request}; await window.electronBridge.sendMessageFromView(request); resolve({{ sent: true }}); }})"
        );
        with_live_codex_connection(port, ensure_launched, |connection| {
            let _: serde_json::Value = connection.evaluate(&expression)?;
            Ok(())
        })?;
    };

    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        let current = read_live_account(Some(port))?;
        if current
            .account
            .as_ref()
            .and_then(|account| account.email.as_deref())
            .map(|email| normalize_email(email) == normalize_email(&summary.email))
            .unwrap_or(false)
        {
            let current_account = current.account.clone();
            with_live_codex_connection(port, ensure_launched, |connection| {
                connection.reload_page(true)?;
                Ok(())
            })?;
            thread::sleep(Duration::from_millis(500));
            let render_deadline = Instant::now() + Duration::from_millis(timeout_ms);
            while Instant::now() < render_deadline {
                let body_text_len = with_local_codex_connection(port, |connection| {
                    connection.evaluate::<usize>(
                        "(() => document.body ? document.body.innerText.trim().length : 0)()",
                    )
                })
                .unwrap_or_default();
                if body_text_len > 0 {
                    break;
                }
                thread::sleep(Duration::from_millis(250));
            }
            return Ok(LiveSwitchResult {
                email: current_account
                    .as_ref()
                    .and_then(|account| account.email.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
                plan_type: current_account
                    .as_ref()
                    .and_then(|account| account.plan_type.clone())
                    .unwrap_or_else(|| "unknown".to_string()),
                account_id: summary.account_id,
            });
        }
        thread::sleep(Duration::from_millis(750));
    }

    Err(anyhow!(
        "Codex did not switch to {} in time.",
        summary.email
    ))
}

pub fn live_account_matches_summary(result: &AccountReadResult, expected: &AuthSummary) -> bool {
    result
        .account
        .as_ref()
        .and_then(|account| account.email.as_deref())
        .map(|email| normalize_email(email) == normalize_email(&expected.email))
        .unwrap_or(false)
}

fn send_mcp_request<T>(port: u16, method: &str, params: serde_json::Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    match send_mcp_request_once(port, method, &params) {
        Ok(value) => Ok(value),
        Err(first_error) => {
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            send_mcp_request_once(port, method, &params).map_err(|retry_error| {
                anyhow!(
                    "{retry_error} (initial {method} request failed before relaunch: {first_error})"
                )
            })
        }
    }
}

fn send_mcp_request_once<T>(port: u16, method: &str, params: &serde_json::Value) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let request = json!({
        "type": "mcp-request",
        "hostId": "local",
        "request": {
            "jsonrpc": "2.0",
            "id": format!("codex-rotate-rs-{method}-{}", chrono::Utc::now().timestamp_millis()),
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
}}, 8000);
const handler = (event) => {{
  const data = event.data;
  const message = data && data.type === "mcp-response" ? (data.message ?? data.response) : null;
  if (message && message.id === request.request.id) {{
    clearTimeout(timeout);
    window.removeEventListener("message", handler);
    resolve({{ timeout: false, result: message.result }});
  }}
}};
window.addEventListener("message", handler);
await window.electronBridge.sendMessageFromView(request);
}})"#
    );
    let value: serde_json::Value =
        with_local_codex_connection(port, |connection| connection.evaluate(&expression))?;
    if value.get("timeout").and_then(serde_json::Value::as_bool) == Some(true) {
        return Err(anyhow!(
            "Timed out waiting for {method} response from Codex."
        ));
    }
    serde_json::from_value(
        value
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(|error| anyhow!("Failed to decode {method} response from Codex: {error}"))
}

fn with_live_codex_connection<T, F>(port: u16, ensure_launched: bool, mut operation: F) -> Result<T>
where
    F: FnMut(&mut crate::cdp::CdpConnection) -> Result<T>,
{
    if ensure_launched {
        ensure_debug_codex_instance(None, Some(port), None, None)?;
    }
    match with_local_codex_connection(port, |connection| operation(connection)) {
        Ok(value) => Ok(value),
        Err(first_error) => {
            invalidate_local_codex_connection(port, true);
            ensure_debug_codex_instance(None, Some(port), None, None)?;
            with_local_codex_connection(port, |connection| operation(connection)).map_err(
                |retry_error| {
                    anyhow!(
                        "{retry_error} (initial live Codex connection failed before relaunch: {first_error})"
                    )
                },
            )
        }
    }
}

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}
