use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::{Mutex, OnceLock};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{connect, Message, WebSocket};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CdpTargetInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub target_type: String,
    pub title: String,
    pub url: String,
    #[serde(rename = "webSocketDebuggerUrl")]
    pub websocket_debugger_url: String,
}

pub struct CdpConnection {
    socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
    next_id: u64,
}

#[derive(Default)]
struct SharedCdpSession {
    port: Option<u16>,
    websocket_debugger_url: Option<String>,
    connection: Option<CdpConnection>,
}

pub fn list_cdp_targets(port: u16) -> Result<Vec<CdpTargetInfo>> {
    fetch_json(&format!("http://127.0.0.1:{port}/json/list"))
}

pub fn is_cdp_ready(port: u16) -> bool {
    fetch_json::<Value>(&format!("http://127.0.0.1:{port}/json/version")).is_ok()
}

pub fn is_cdp_page_ready(port: u16) -> bool {
    list_cdp_targets(port)
        .map(|targets| has_codex_page_target(&targets))
        .unwrap_or(false)
}

pub fn connect_to_local_codex_page(port: u16) -> Result<CdpConnection> {
    let websocket_debugger_url = local_codex_page_websocket_url(port)?;
    connect_to_debugger_url(&websocket_debugger_url)
}

pub fn with_local_codex_connection<T, F>(port: u16, mut operation: F) -> Result<T>
where
    F: FnMut(&mut CdpConnection) -> Result<T>,
{
    let mut session = shared_cdp_session()
        .lock()
        .expect("shared cdp session mutex");
    let connection = ensure_shared_connection(&mut session, port)?;
    match operation(connection) {
        Ok(value) => Ok(value),
        Err(error) => {
            session.connection = None;
            Err(error)
        }
    }
}

pub fn invalidate_local_codex_connection(port: u16, clear_target_url: bool) {
    let mut session = shared_cdp_session()
        .lock()
        .expect("shared cdp session mutex");
    if session.port != Some(port) {
        return;
    }
    session.connection = None;
    if clear_target_url {
        session.websocket_debugger_url = None;
    }
}

fn connect_to_debugger_url(websocket_debugger_url: &str) -> Result<CdpConnection> {
    let (socket, _) = connect(websocket_debugger_url).with_context(|| {
        format!("Failed to connect to Codex renderer at {websocket_debugger_url}.")
    })?;

    let mut connection = CdpConnection { socket, next_id: 0 };
    let _: Value = connection.send_command("Runtime.enable", json!({}))?;
    Ok(connection)
}

fn local_codex_page_websocket_url(port: u16) -> Result<String> {
    let page = list_cdp_targets(port)?
        .into_iter()
        .find(|target| target.target_type == "page" && target.url.starts_with("app://-/index.html"))
        .ok_or_else(|| anyhow!("No Codex page target is available on port {port}."))?;
    Ok(page.websocket_debugger_url)
}

fn has_codex_page_target(targets: &[CdpTargetInfo]) -> bool {
    targets
        .iter()
        .any(|target| target.target_type == "page" && target.url.starts_with("app://-/index.html"))
}

fn ensure_shared_connection<'a>(
    session: &'a mut SharedCdpSession,
    port: u16,
) -> Result<&'a mut CdpConnection> {
    if session.port != Some(port) {
        *session = SharedCdpSession {
            port: Some(port),
            ..SharedCdpSession::default()
        };
    }
    if session.connection.is_none() {
        let websocket_debugger_url = match session.websocket_debugger_url.as_deref() {
            Some(url) => match connect_to_debugger_url(url) {
                Ok(connection) => {
                    session.connection = Some(connection);
                    return Ok(session.connection.as_mut().expect("shared cdp connection"));
                }
                Err(_) => local_codex_page_websocket_url(port)?,
            },
            None => local_codex_page_websocket_url(port)?,
        };
        let connection = connect_to_debugger_url(&websocket_debugger_url)?;
        session.websocket_debugger_url = Some(websocket_debugger_url);
        session.connection = Some(connection);
    }
    Ok(session.connection.as_mut().expect("shared cdp connection"))
}

fn shared_cdp_session() -> &'static Mutex<SharedCdpSession> {
    static SESSION: OnceLock<Mutex<SharedCdpSession>> = OnceLock::new();
    SESSION.get_or_init(|| Mutex::new(SharedCdpSession::default()))
}

impl CdpConnection {
    pub fn evaluate<T: DeserializeOwned>(&mut self, expression: &str) -> Result<T> {
        let result: Value = self.send_command(
            "Runtime.evaluate",
            json!({
                "expression": expression,
                "returnByValue": true,
                "awaitPromise": true,
            }),
        )?;
        let value = result
            .get("result")
            .and_then(|result| result.get("value"))
            .cloned()
            .unwrap_or(Value::Null);
        serde_json::from_value(value).context("Failed to decode CDP evaluation result.")
    }

    pub fn reload_page(&mut self, ignore_cache: bool) -> Result<()> {
        let _: Value = self.send_command(
            "Page.reload",
            json!({
                "ignoreCache": ignore_cache,
            }),
        )?;
        Ok(())
    }

    pub fn close(&mut self) {
        let _ = self.socket.close(None);
    }

    fn send_command<T: DeserializeOwned>(&mut self, method: &str, params: Value) -> Result<T> {
        self.next_id += 1;
        let request_id = self.next_id;
        let payload = json!({
            "id": request_id,
            "method": method,
            "params": params,
        });
        self.socket
            .send(Message::Text(payload.to_string()))
            .context("Failed to send CDP request.")?;

        loop {
            let message = self.socket.read().context("Failed to read CDP response.")?;
            let text = match message {
                Message::Text(text) => text,
                Message::Binary(bytes) => String::from_utf8(bytes)
                    .context("Failed to decode binary CDP response as UTF-8.")?,
                Message::Ping(_) | Message::Pong(_) => continue,
                Message::Frame(_) => continue,
                Message::Close(_) => return Err(anyhow!("CDP connection closed unexpectedly.")),
            };
            let value: Value =
                serde_json::from_str(&text).context("Failed to parse CDP message as JSON.")?;
            let Some(id) = value.get("id").and_then(Value::as_u64) else {
                continue;
            };
            if id != request_id {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(anyhow!("CDP request failed: {}", error));
            }
            return serde_json::from_value(value.get("result").cloned().unwrap_or(Value::Null))
                .context("Failed to decode CDP response payload.");
        }
    }
}

fn fetch_json<T: DeserializeOwned>(url: &str) -> Result<T> {
    let client = cdp_http_client();
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("CDP endpoint failed: {url}"))?;
    if !response.status().is_success() {
        return Err(anyhow!(
            "CDP endpoint failed ({}): {}",
            response.status().as_u16(),
            url
        ));
    }
    response
        .json()
        .context("Failed to decode CDP JSON response.")
}

fn cdp_http_client() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .expect("failed to build CDP HTTP client")
    })
}

#[cfg(test)]
mod tests {
    use super::{has_codex_page_target, CdpTargetInfo};

    fn target(target_type: &str, url: &str) -> CdpTargetInfo {
        CdpTargetInfo {
            id: "target-id".to_string(),
            target_type: target_type.to_string(),
            title: "target".to_string(),
            url: url.to_string(),
            websocket_debugger_url: "ws://127.0.0.1/devtools/page/target-id".to_string(),
        }
    }

    #[test]
    fn codex_page_target_helper_requires_app_page() {
        assert!(!has_codex_page_target(&[
            target("service_worker", "app://-/index.html"),
            target("page", "https://example.com"),
        ]));
        assert!(has_codex_page_target(&[
            target("page", "https://example.com"),
            target("page", "app://-/index.html#/threads/abc"),
        ]));
    }
}
