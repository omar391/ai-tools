use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::fs_security::write_private_string;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexAuth {
    pub auth_mode: String,
    #[serde(rename = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String>,
    pub tokens: AuthTokens,
    pub last_refresh: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthTokens {
    pub id_token: String,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub account_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthSummary {
    pub email: String,
    pub account_id: String,
    pub plan_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceLoginPayload {
    #[serde(rename = "type")]
    pub payload_type: String,
    #[serde(rename = "accessToken")]
    pub access_token: String,
    #[serde(rename = "chatgptAccountId")]
    pub chatgpt_account_id: String,
    #[serde(rename = "chatgptPlanType")]
    pub chatgpt_plan_type: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonRpcRequest<TParams> {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: TParams,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexDesktopMcpRequest<TParams> {
    #[serde(rename = "type")]
    pub request_type: String,
    #[serde(rename = "hostId")]
    pub host_id: String,
    pub request: JsonRpcRequest<TParams>,
}

pub fn load_codex_auth(path: &Path) -> Result<CodexAuth> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Codex auth file not found at {}.", path.display()))?;
    serde_json::from_str(&raw)
        .with_context(|| format!("Invalid Codex auth file at {}.", path.display()))
}

pub fn write_codex_auth(path: &Path, auth: &CodexAuth) -> Result<()> {
    write_private_json(path, auth)
}

pub fn decode_jwt_payload(jwt: &str) -> Result<Map<String, Value>> {
    let parts: Vec<&str> = jwt.split('.').collect();
    if parts.len() != 3 {
        return Err(anyhow!("Invalid JWT"));
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .context("Invalid JWT payload")?;
    let value = serde_json::from_slice::<Value>(&payload).context("Invalid JWT payload")?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("Invalid JWT payload"))
}

pub fn extract_account_id_from_token(jwt: &str) -> Option<String> {
    let payload = decode_jwt_payload(jwt).ok()?;
    payload
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object)
        .and_then(|auth| auth.get("chatgpt_account_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

pub fn extract_account_id_from_auth(auth: &CodexAuth) -> String {
    extract_account_id_from_token(&auth.tokens.access_token)
        .or_else(|| extract_account_id_from_token(&auth.tokens.id_token))
        .unwrap_or_else(|| auth.tokens.account_id.clone())
}

pub fn summarize_codex_auth(auth: &CodexAuth) -> AuthSummary {
    let mut email = "unknown".to_string();
    let mut plan_type = "unknown".to_string();

    if let Ok(payload) = decode_jwt_payload(&auth.tokens.access_token) {
        if let Some(profile) = payload
            .get("https://api.openai.com/profile")
            .and_then(Value::as_object)
        {
            if let Some(value) = profile.get("email").and_then(Value::as_str) {
                email = value.to_string();
            }
        }
        if let Some(auth_info) = payload
            .get("https://api.openai.com/auth")
            .and_then(Value::as_object)
        {
            if let Some(value) = auth_info.get("chatgpt_plan_type").and_then(Value::as_str) {
                plan_type = value.to_string();
            }
        }
    }

    if email == "unknown" {
        if let Ok(payload) = decode_jwt_payload(&auth.tokens.id_token) {
            if let Some(value) = payload.get("email").and_then(Value::as_str) {
                email = value.to_string();
            }
        }
    }

    AuthSummary {
        email,
        account_id: extract_account_id_from_auth(auth),
        plan_type,
    }
}

pub fn build_device_login_payload(auth: &CodexAuth) -> DeviceLoginPayload {
    let summary = summarize_codex_auth(auth);
    DeviceLoginPayload {
        payload_type: "chatgptAuthTokens".to_string(),
        access_token: auth.tokens.access_token.clone(),
        chatgpt_account_id: summary.account_id,
        chatgpt_plan_type: if summary.plan_type == "unknown" {
            None
        } else {
            Some(summary.plan_type)
        },
    }
}

pub fn build_login_start_request(
    auth: &CodexAuth,
    host_id: Option<&str>,
    request_id: Option<&str>,
) -> CodexDesktopMcpRequest<DeviceLoginPayload> {
    CodexDesktopMcpRequest {
        request_type: "mcp-request".to_string(),
        host_id: host_id.unwrap_or("local").to_string(),
        request: JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: request_id.unwrap_or("codex-rotate-app-login").to_string(),
            method: "account/login/start".to_string(),
            params: build_device_login_payload(auth),
        },
    }
}

pub fn token_expiry(jwt: &str) -> Option<i64> {
    decode_jwt_payload(jwt)
        .ok()?
        .get("exp")
        .and_then(Value::as_i64)
}

pub fn is_token_expired(jwt: &str, skew_seconds: i64) -> bool {
    let exp = match token_expiry(jwt) {
        Some(exp) => exp,
        None => return false,
    };
    let now = chrono::Utc::now().timestamp() + skew_seconds;
    exp <= now
}

fn write_private_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_private_string(path, &serde_json::to_string_pretty(value)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base64_url_encode(value: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value)
    }

    fn make_jwt(payload: &str) -> String {
        format!(
            "{}.{}.signature",
            base64_url_encode(r#"{"alg":"none","typ":"JWT"}"#),
            base64_url_encode(payload)
        )
    }

    fn make_auth() -> CodexAuth {
        CodexAuth {
            auth_mode: "chatgpt".to_string(),
            openai_api_key: None,
            tokens: AuthTokens {
                access_token: make_jwt(
                    r#"{"https://api.openai.com/profile":{"email":"dev.22@astronlab.com"},"https://api.openai.com/auth":{"chatgpt_account_id":"acct-123","chatgpt_plan_type":"free"}}"#,
                ),
                id_token: make_jwt(r#"{"email":"dev.22@astronlab.com"}"#),
                refresh_token: Some("refresh".to_string()),
                account_id: "acct-fallback".to_string(),
            },
            last_refresh: "2026-04-02T00:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn summarize_extracts_email_plan_and_account() {
        let summary = summarize_codex_auth(&make_auth());
        assert_eq!(summary.email, "dev.22@astronlab.com");
        assert_eq!(summary.plan_type, "free");
        assert_eq!(summary.account_id, "acct-123");
    }

    #[test]
    fn build_login_request_uses_expected_contract() {
        let request = build_login_start_request(&make_auth(), None, Some("rotate-1"));
        assert_eq!(request.request.method, "account/login/start");
        assert_eq!(request.request.id, "rotate-1");
        assert_eq!(request.request.params.chatgpt_account_id, "acct-123");
        assert_eq!(request.request.params.payload_type, "chatgptAuthTokens");
    }
}
