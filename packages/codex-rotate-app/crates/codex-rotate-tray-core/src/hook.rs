use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use codex_rotate_core::auth::{
    build_login_start_request, load_codex_auth, summarize_codex_auth, AuthSummary,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::cdp::connect_to_local_codex_page;
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

pub fn switch_live_account_to_current_auth(
    port: Option<u16>,
    ensure_launched: bool,
    timeout_ms: u64,
    reload_after_switch: bool,
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
        let mut connection = connect_to_local_codex_page(port)?;
        let request = serde_json::to_string(&expected)?;
        let expression = format!(
            "new Promise(async (resolve) => {{ const request = {request}; await window.electronBridge.sendMessageFromView(request); resolve({{ sent: true }}); }})"
        );
        let _: serde_json::Value = connection.evaluate(&expression)?;
        connection.close();
    }

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
            if reload_after_switch {
                reload_live_codex_page(port, timeout_ms)?;
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

fn reload_live_codex_page(port: u16, timeout_ms: u64) -> Result<()> {
    {
        let mut connection = connect_to_local_codex_page(port)?;
        connection.reload_page(true)?;
        connection.close();
    }

    let deadline = Instant::now() + Duration::from_millis(timeout_ms.min(8_000));
    while Instant::now() < deadline {
        if connect_to_local_codex_page(port).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(500));
    }

    Err(anyhow!(
        "Codex did not reconnect after reloading the active page."
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
    let mut connection = connect_to_local_codex_page(port)?;
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
  if (data && data.type === "mcp-response" && data.message && data.message.id === request.request.id) {{
    clearTimeout(timeout);
    window.removeEventListener("message", handler);
    resolve({{ timeout: false, result: data.message.result }});
  }}
}};
window.addEventListener("message", handler);
await window.electronBridge.sendMessageFromView(request);
}})"#
    );
    let value: serde_json::Value = connection.evaluate(&expression)?;
    connection.close();
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

fn normalize_email(value: &str) -> String {
    value.trim().to_lowercase()
}
