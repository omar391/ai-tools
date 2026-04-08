use std::env;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::managed_browser::ensure_managed_browser_wrapper;
use serde_json::{json, Value};

const CLIENT_NAME: &str = "codex-rotate-managed-login";
const CLIENT_VERSION: &str = "1.0.0";

pub fn run_managed_login(args: &[String]) -> Result<()> {
    let codex_bin = env::var("CODEX_ROTATE_REAL_CODEX")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "codex".to_string());

    if args.iter().any(|arg| arg == "--device-auth") {
        return run_device_auth_login(&codex_bin);
    }

    let mut client = JsonRpcStdioClient::spawn(&codex_bin, &["app-server", "--listen", "stdio://"])
        .with_context(|| format!("Failed to start {} app-server.", codex_bin))?;
    client.initialize()?;

    let response = client.request("account/login/start", json!({ "type": "chatgpt" }))?;
    let login_id = response
        .get("loginId")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Unexpected account/login/start response."))?
        .to_string();
    let auth_url = response
        .get("authUrl")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Unexpected account/login/start response."))?
        .to_string();
    let login_type = response
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if login_type != "chatgpt" {
        return Err(anyhow!("Unexpected account/login/start response."));
    }

    let callback_origin =
        parse_callback_origin(&auth_url).unwrap_or_else(|| "http://localhost:1455".to_string());
    eprintln!("Starting local login server on {callback_origin}.");
    eprintln!("If your browser did not open, navigate to this URL to authenticate:");
    eprintln!();
    eprintln!("{auth_url}");

    let completion = client.wait_for_login_completion(&login_id);
    if completion.is_err() {
        client.cancel_login(&login_id).ok();
    }
    let close_result = client.close();
    completion?;
    close_result?;
    eprintln!("Successfully logged in");
    Ok(())
}

pub fn run_managed_browser_wrapper(args: &[String]) -> Result<()> {
    let profile_name = args
        .first()
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("Usage: codex-rotate internal managed-browser-wrapper <profile> <codex-bin>")
        })?;
    let codex_bin = args
        .get(1)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            anyhow!("Usage: codex-rotate internal managed-browser-wrapper <profile> <codex-bin>")
        })?;
    let wrapper_path = ensure_managed_browser_wrapper(profile_name, codex_bin)?;
    println!("{}", wrapper_path.display());
    Ok(())
}

fn run_device_auth_login(codex_bin: &str) -> Result<()> {
    let status = Command::new(codex_bin)
        .args(["login", "--device-auth"])
        .status()
        .with_context(|| format!("Failed to run {} login --device-auth.", codex_bin))?;
    if status.success() {
        return Ok(());
    }
    Err(anyhow!(
        "{} login --device-auth exited with status {}.",
        codex_bin,
        status
    ))
}

fn parse_callback_origin(auth_url: &str) -> Option<String> {
    let parsed = url::Url::parse(auth_url).ok()?;
    let redirect_uri = parsed.query_pairs().find_map(|(key, value)| {
        if key == "redirect_uri" {
            Some(value.into_owned())
        } else {
            None
        }
    })?;
    let redirect = url::Url::parse(&redirect_uri).ok()?;
    Some(format!(
        "{}://{}{}",
        redirect.scheme(),
        redirect.host_str()?,
        redirect
            .port()
            .map(|value| format!(":{value}"))
            .unwrap_or_default()
    ))
}

struct JsonRpcStdioClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl JsonRpcStdioClient {
    fn spawn(command: &str, args: &[&str]) -> Result<Self> {
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdin = child
            .stdin
            .take()
            .context("codex app-server stdin unavailable")?;
        let stdout = child
            .stdout
            .take()
            .context("codex app-server stdout unavailable")?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        })
    }

    fn initialize(&mut self) -> Result<()> {
        let _ = self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": CLIENT_NAME,
                    "version": CLIENT_VERSION,
                },
                "capabilities": {},
            }),
        )?;
        self.notify("initialized", json!({}))?;
        Ok(())
    }

    fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let request_id = self.next_id;
        self.next_id += 1;
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }))?;

        loop {
            let message = self.read_message()?;
            let Some(id) = message.get("id").and_then(Value::as_u64) else {
                continue;
            };
            if id != request_id {
                continue;
            }
            if let Some(error) = message.get("error") {
                let detail = error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("codex app-server request failed");
                return Err(anyhow!(detail.to_string()));
            }
            return Ok(message.get("result").cloned().unwrap_or(Value::Null));
        }
    }

    fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write_message(&json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        }))
    }

    fn cancel_login(&mut self, login_id: &str) -> Result<()> {
        let _ = self.request("account/login/cancel", json!({ "loginId": login_id }))?;
        Ok(())
    }

    fn wait_for_login_completion(&mut self, login_id: &str) -> Result<()> {
        loop {
            let message = self.read_message()?;
            let Some(method) = message.get("method").and_then(Value::as_str) else {
                continue;
            };
            if method != "account/login/completed" {
                continue;
            }
            let params = message.get("params").cloned().unwrap_or(Value::Null);
            if let Some(completed_login_id) = params.get("loginId").and_then(Value::as_str) {
                if completed_login_id != login_id {
                    continue;
                }
            }
            if params
                .get("success")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(());
            }
            let error = params
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("Login was not completed.");
            return Err(anyhow!(error.to_string()));
        }
    }

    fn close(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        self.child.kill().ok();
        self.child.wait().ok();
        Ok(())
    }

    fn write_message(&mut self, message: &Value) -> Result<()> {
        serde_json::to_writer(&mut self.stdin, message)?;
        self.stdin.write_all(b"\n")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn read_message(&mut self) -> Result<Value> {
        let mut line = String::new();
        loop {
            line.clear();
            let read = self.stdout.read_line(&mut line)?;
            if read == 0 {
                let status = self.child.wait()?;
                return Err(anyhow!(
                    "codex app-server exited unexpectedly ({}).",
                    status
                        .code()
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "signal".to_string())
                ));
            }
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<Value>(trimmed) {
                Ok(value) => return Ok(value),
                Err(_) => continue,
            }
        }
    }
}
