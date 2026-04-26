use std::io::{BufRead, BufReader, Read, Write};

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::bridge::{GuestBridgeRequest, GuestBridgeResponse};
use codex_rotate_core::workflow::{cmd_relogin_with_progress, ReloginOptions};
use codex_rotate_runtime::launcher::ensure_debug_codex_instance;
use codex_rotate_runtime::paths::resolve_paths;
use codex_rotate_runtime::rotation_hygiene::{
    export_thread_handoffs, import_thread_handoffs, thread_handoff_import_stage_label,
    HostConversationTransport, ThreadHandoff,
};
use codex_rotate_runtime::thread_recovery::{read_active_thread_ids, send_codex_app_request};
use serde_json::{json, Value};

const DEFAULT_PORT: u16 = 9333;

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

pub fn handle_guest_bridge_command(command: &str, payload: Value) -> Result<Value> {
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
            if handoffs.is_empty() {
                return Ok(json!({
                    "completed_source_thread_ids": [],
                    "failures": [],
                }));
            }
            let target_account_id = payload
                .get("target_account_id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("import-thread-handoffs requires target_account_id"))?;
            let transport = HostConversationTransport::new(port);
            let outcome = import_thread_handoffs(&transport, target_account_id, &handoffs, None)?;
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
        "list-threads" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let thread_ids = read_active_thread_ids(Some(port))?;
            Ok(json!({ "thread_ids": thread_ids }))
        }
        "read-thread" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let thread_id = payload
                .get("thread_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("read-thread requires thread_id"))?;
            let thread: Value = send_codex_app_request(
                port,
                "thread/read",
                json!({ "threadId": thread_id, "includeTurns": true }),
            )?;
            Ok(thread)
        }
        "start-thread" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let cwd = payload.get("cwd").and_then(Value::as_str);
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
            Ok(response)
        }
        "inject-items" => {
            let port = payload
                .get("port")
                .and_then(Value::as_u64)
                .and_then(|value| u16::try_from(value).ok())
                .unwrap_or(DEFAULT_PORT);
            let thread_id = payload
                .get("thread_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("inject-items requires thread_id"))?;
            let items = payload
                .get("items")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            send_codex_app_request::<Value>(
                port,
                "thread/inject_items",
                json!({
                    "threadId": thread_id,
                    "items": items,
                }),
            )?;
            Ok(json!({}))
        }
        _ => Err(anyhow!("Unsupported guest bridge command \"{command}\".")),
    }
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

pub fn send_guest_request<REQ, RES>(command: &str, payload: REQ) -> Result<RES>
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
