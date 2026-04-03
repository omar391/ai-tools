use anyhow::{anyhow, Context, Result};
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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

pub fn list_cdp_targets(port: u16) -> Result<Vec<CdpTargetInfo>> {
    fetch_json(&format!("http://127.0.0.1:{port}/json/list"))
}

pub fn is_cdp_ready(port: u16) -> bool {
    fetch_json::<Value>(&format!("http://127.0.0.1:{port}/json/version")).is_ok()
}

pub fn connect_to_local_codex_page(port: u16) -> Result<CdpConnection> {
    let page = list_cdp_targets(port)?
        .into_iter()
        .find(|target| target.target_type == "page" && target.url.starts_with("app://-/index.html"))
        .ok_or_else(|| anyhow!("No Codex page target is available on port {port}."))?;

    let (socket, _) = connect(page.websocket_debugger_url.as_str())
        .with_context(|| format!("Failed to connect to Codex renderer on port {port}."))?;

    let mut connection = CdpConnection { socket, next_id: 0 };
    let _: Value = connection.send_command("Runtime.enable", json!({}))?;
    Ok(connection)
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
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .context("Failed to build CDP HTTP client.")?;
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
