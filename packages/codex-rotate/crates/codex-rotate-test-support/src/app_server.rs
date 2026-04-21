use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::IsolatedHomeFixture;

const DEFAULT_LOGIN_ID: &str = "login-123";
const DEFAULT_AUTH_URL: &str =
    "https://auth.openai.com/oauth/authorize?redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback";

#[derive(Clone, Debug, PartialEq)]
pub enum FakeCodexAppServerOutcome {
    Result(Value),
    Error(String),
    Hang,
}

impl FakeCodexAppServerOutcome {
    fn to_json(&self) -> Value {
        match self {
            Self::Result(result) => json!({"kind": "result", "value": result}),
            Self::Error(message) => json!({"kind": "error", "message": message}),
            Self::Hang => json!({"kind": "hang"}),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FakeCodexAppServerRequest {
    pub method: String,
    pub id: Option<Value>,
    pub params: Value,
}

#[derive(Debug)]
pub struct FakeCodexAppServerFixtureBuilder {
    home: IsolatedHomeFixture,
    initialize_result: Value,
    login_start_result: Value,
    login_completion: Option<Value>,
    login_cancel_result: Value,
    thread_records: BTreeMap<String, Value>,
    method_outcomes: BTreeMap<String, Vec<FakeCodexAppServerOutcome>>,
    next_thread_ids: Vec<String>,
    launch_failure: Option<String>,
}

#[derive(Debug)]
pub struct FakeCodexAppServerFixture {
    home: IsolatedHomeFixture,
    executable_path: PathBuf,
    config_path: PathBuf,
    request_log_path: PathBuf,
}

impl FakeCodexAppServerFixtureBuilder {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        Ok(Self {
            home: IsolatedHomeFixture::new(prefix)?,
            initialize_result: json!({
                "userAgent": "fake",
                "codexHome": "/tmp",
                "platformFamily": "unix",
                "platformOs": "macos",
            }),
            login_start_result: json!({
                "type": "chatgpt",
                "loginId": DEFAULT_LOGIN_ID,
                "authUrl": DEFAULT_AUTH_URL,
            }),
            login_completion: Some(json!({
                "success": true,
                "loginId": DEFAULT_LOGIN_ID,
                "error": null,
                "delayMs": 25,
            })),
            login_cancel_result: json!({"status": "canceled"}),
            thread_records: BTreeMap::new(),
            method_outcomes: BTreeMap::new(),
            next_thread_ids: Vec::new(),
            launch_failure: None,
        })
    }

    pub fn initialize_result(mut self, result: Value) -> Self {
        self.initialize_result = result;
        self
    }

    pub fn login_start_result(mut self, result: Value) -> Self {
        self.login_start_result = result;
        self
    }

    pub fn login_completion(
        mut self,
        success: bool,
        login_id: impl AsRef<str>,
        error: Option<impl AsRef<str>>,
        delay_ms: u64,
    ) -> Self {
        self.login_completion = Some(json!({
            "success": success,
            "loginId": login_id.as_ref(),
            "error": error.map(|value| value.as_ref().to_string()),
            "delayMs": delay_ms,
        }));
        self
    }

    pub fn disable_login_completion(mut self) -> Self {
        self.login_completion = None;
        self
    }

    pub fn login_cancel_result(mut self, result: Value) -> Self {
        self.login_cancel_result = result;
        self
    }

    pub fn thread(mut self, thread_id: impl AsRef<str>, thread: Value) -> Self {
        self.thread_records
            .insert(thread_id.as_ref().to_string(), thread);
        self
    }

    pub fn next_thread_ids<I, S>(mut self, thread_ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.next_thread_ids = thread_ids
            .into_iter()
            .map(|value| value.as_ref().to_string())
            .collect();
        self
    }

    pub fn method_result(mut self, method: impl AsRef<str>, result: Value) -> Self {
        self.method_outcomes
            .entry(method.as_ref().to_string())
            .or_default()
            .push(FakeCodexAppServerOutcome::Result(result));
        self
    }

    pub fn method_error(mut self, method: impl AsRef<str>, message: impl AsRef<str>) -> Self {
        self.method_outcomes
            .entry(method.as_ref().to_string())
            .or_default()
            .push(FakeCodexAppServerOutcome::Error(
                message.as_ref().to_string(),
            ));
        self
    }

    pub fn method_hang(mut self, method: impl AsRef<str>) -> Self {
        self.method_outcomes
            .entry(method.as_ref().to_string())
            .or_default()
            .push(FakeCodexAppServerOutcome::Hang);
        self
    }

    pub fn launch_failure(mut self, message: impl AsRef<str>) -> Self {
        self.launch_failure = Some(message.as_ref().to_string());
        self
    }

    pub fn build(self) -> Result<FakeCodexAppServerFixture> {
        let request_log_path = self
            .home
            .sandbox_root()
            .join("codex-app-server-requests.jsonl");
        fs::write(&request_log_path, "")
            .with_context(|| format!("create {}", request_log_path.display()))?;

        let config_path = self
            .home
            .sandbox_root()
            .join("codex-app-server-config.json");
        let config = json!({
            "initializeResult": self.initialize_result,
            "login": {
                "startResult": self.login_start_result,
                "completion": self.login_completion,
                "cancelResult": self.login_cancel_result,
            },
            "threads": self.thread_records,
            "methodOutcomes": self.method_outcomes.iter().map(|(method, outcomes)| {
                (method.clone(), outcomes.iter().map(FakeCodexAppServerOutcome::to_json).collect::<Vec<_>>())
            }).collect::<BTreeMap<_, _>>(),
            "nextThreadIds": self.next_thread_ids,
            "launchFailure": self.launch_failure,
        });
        fs::write(&config_path, serde_json::to_string_pretty(&config)?)
            .with_context(|| format!("write {}", config_path.display()))?;

        let executable_path = self.home.sandbox_root().join("fake-codex.mjs");
        fs::write(
            &executable_path,
            build_fake_codex_script(&config_path, &request_log_path),
        )
        .with_context(|| format!("write {}", executable_path.display()))?;
        make_executable(&executable_path)?;

        Ok(FakeCodexAppServerFixture {
            home: self.home,
            executable_path,
            config_path,
            request_log_path,
        })
    }
}

impl FakeCodexAppServerFixture {
    pub fn new(prefix: impl AsRef<str>) -> Result<Self> {
        FakeCodexAppServerFixtureBuilder::new(prefix)?.build()
    }

    pub fn builder(prefix: impl AsRef<str>) -> Result<FakeCodexAppServerFixtureBuilder> {
        FakeCodexAppServerFixtureBuilder::new(prefix)
    }

    pub fn sandbox_root(&self) -> &Path {
        self.home.sandbox_root()
    }

    pub fn codex_bin_path(&self) -> &Path {
        &self.executable_path
    }

    pub fn config_path(&self) -> &Path {
        &self.config_path
    }

    pub fn request_log_path(&self) -> &Path {
        &self.request_log_path
    }

    pub fn requests(&self) -> Result<Vec<Value>> {
        let raw = fs::read_to_string(&self.request_log_path)
            .with_context(|| format!("read {}", self.request_log_path.display()))?;
        Ok(raw
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str::<Value>(line))
            .collect::<Result<Vec<_>, _>>()?)
    }

    pub fn request_methods(&self) -> Result<Vec<String>> {
        Ok(self
            .requests()?
            .into_iter()
            .filter_map(|value| {
                value
                    .get("method")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
            .collect())
    }

    pub fn spawn_stdio_client(&self) -> Result<FakeCodexStdioClient> {
        FakeCodexStdioClient::spawn(&self.executable_path)
    }
}

pub struct FakeCodexStdioClient {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl FakeCodexStdioClient {
    pub fn spawn(executable: &Path) -> Result<Self> {
        let mut child = Command::new(executable)
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("spawn {}", executable.display()))?;
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

    pub fn request(&mut self, method: &str, params: Value) -> Result<Value> {
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

    pub fn wait_for_notification(&mut self, method: &str) -> Result<Value> {
        loop {
            let message = self.read_message()?;
            if message.get("method").and_then(Value::as_str) != Some(method) {
                continue;
            }
            return Ok(message.get("params").cloned().unwrap_or(Value::Null));
        }
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
                    "codex app-server exited unexpectedly ({})",
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

    pub fn close(&mut self) -> Result<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        self.child.kill().ok();
        self.child.wait().ok();
        Ok(())
    }
}

impl Drop for FakeCodexStdioClient {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

fn build_fake_codex_script(config_path: &Path, request_log_path: &Path) -> String {
    let config_path = serde_json::to_string(&config_path.to_string_lossy()).expect("encode path");
    let request_log_path = serde_json::to_string(&request_log_path.to_string_lossy())
        .expect("encode request log path");

    let script = r#"#!/usr/bin/env node
import fs from 'node:fs';
import process from 'node:process';

const CONFIG_PATH = __CONFIG_PATH__;
const REQUEST_LOG_PATH = __REQUEST_LOG_PATH__;
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const methodOutcomes = new Map(Object.entries(config.methodOutcomes ?? {}));
const threads = new Map(Object.entries(config.threads ?? {}));
const nextThreadIds = [...(config.nextThreadIds ?? [])];
let threadCounter = 1;

function write(message) {
  process.stdout.write(JSON.stringify(message) + '\n');
}

function logRequest(message) {
  fs.appendFileSync(REQUEST_LOG_PATH, JSON.stringify(message) + '\n');
}

function queuedOutcome(method) {
  const queue = methodOutcomes.get(method);
  if (!queue || queue.length === 0) {
    return null;
  }
  return queue.shift();
}

function parseLine(line) {
  try {
    return JSON.parse(line);
  } catch {
    return null;
  }
}

function respond(id, result) {
  write({ jsonrpc: '2.0', id, result });
}

function fail(id, message) {
  write({ jsonrpc: '2.0', id, error: { code: -32000, message } });
}

function clone(value) {
  return value === undefined ? null : JSON.parse(JSON.stringify(value));
}

function ensureThread(threadId) {
  if (!threads.has(threadId)) {
    threads.set(threadId, {
      id: threadId,
      cwd: null,
      preview: '',
      status: { type: 'active' },
      turns: [],
    });
  }
  return threads.get(threadId);
}

function handleThreadRead(id, params) {
  const threadId = params?.threadId;
  const thread = threadId ? threads.get(threadId) : null;
    respond(id, thread ? { thread: clone(thread) } : null);
}

function handleThreadStart(id, params) {
  const threadId = nextThreadIds.length > 0 ? nextThreadIds.shift() : `thread-created-${threadCounter++}`;
  const thread = {
    id: threadId,
    cwd: params?.cwd ?? null,
    preview: '',
    status: { type: 'active' },
    turns: [],
  };
  threads.set(threadId, thread);
  respond(id, { thread: { id: threadId } });
}

function handleThreadInjectItems(id, params) {
  const threadId = params?.threadId;
  const items = Array.isArray(params?.items) ? params.items : [];
  if (threadId && items.length > 0) {
    const thread = ensureThread(threadId);
    if (!Array.isArray(thread.turns) || thread.turns.length === 0) {
      thread.turns = [{ id: 'turn-1', items: [...items] }];
    } else {
      const lastTurn = thread.turns[thread.turns.length - 1];
      lastTurn.items = [...(lastTurn.items ?? []), ...items];
    }
  }
  respond(id, {});
}

function handleTurnStart(id, params) {
  const threadId = params?.threadId;
  if (threadId) {
    const thread = ensureThread(threadId);
    thread.turns = thread.turns ?? [];
    thread.turns.push({
      id: `turn-${thread.turns.length + 1}`,
      items: Array.isArray(params?.input) ? params.input : [],
    });
  }
  respond(id, {});
}

function handleAccountLoginStart(id) {
  const result = clone(config.login?.startResult ?? {
    type: 'chatgpt',
    loginId: 'login-123',
    authUrl: 'https://auth.openai.com/oauth/authorize?redirect_uri=http%3A%2F%2Flocalhost%3A1455%2Fauth%2Fcallback',
  });
  respond(id, result);

  const completion = config.login?.completion;
  if (completion) {
    const delayMs = completion.delayMs ?? 0;
    setTimeout(() => {
      write({
        jsonrpc: '2.0',
        method: 'account/login/completed',
        params: {
          success: completion.success ?? true,
          loginId: completion.loginId ?? result.loginId ?? 'login-123',
          error: completion.error ?? null,
        },
      });
    }, delayMs);
  }
}

function handleAccountLoginCancel(id) {
  respond(id, clone(config.login?.cancelResult ?? { status: 'canceled' }));
}

function handleMessage(message) {
  if (!message || typeof message.method !== 'string') {
    return;
  }

  logRequest(message);

  const outcome = queuedOutcome(message.method);
  if (outcome) {
    if (outcome.kind === 'hang') {
      return;
    }
    if (outcome.kind === 'error') {
      fail(message.id, outcome.message ?? 'app-server failure');
      return;
    }
    respond(message.id, outcome.value ?? {});
    return;
  }

  switch (message.method) {
    case 'initialize':
      respond(message.id, clone(config.initializeResult ?? {
        userAgent: 'fake',
        codexHome: '/tmp',
        platformFamily: 'unix',
        platformOs: 'macos',
      }));
      return;
    case 'account/login/start':
      handleAccountLoginStart(message.id);
      return;
    case 'account/login/cancel':
      handleAccountLoginCancel(message.id);
      return;
    case 'thread/read':
      handleThreadRead(message.id, message.params);
      return;
    case 'thread/start':
      handleThreadStart(message.id, message.params);
      return;
    case 'thread/inject_items':
    case 'thread/injectItems':
      handleThreadInjectItems(message.id, message.params);
      return;
    case 'thread/resume':
      respond(message.id, {});
      return;
    case 'turn/start':
      handleTurnStart(message.id, message.params);
      return;
    default:
            if (Object.prototype.hasOwnProperty.call(message, 'id')) {
                respond(message.id, {});
            }
  }
}

if (config.launchFailure) {
  process.stderr.write(String(config.launchFailure) + '\n');
  process.exit(1);
}

if (process.argv[2] === 'login' && process.argv[3] === '--device-auth') {
  process.exit(0);
}

if (process.argv[2] !== 'app-server') {
  process.stderr.write('unexpected args: ' + process.argv.slice(2).join(' ') + '\n');
  process.exit(1);
}

process.stdin.setEncoding('utf8');
let buffer = '';

process.stdin.on('data', (chunk) => {
  buffer += chunk;
  while (true) {
    const newlineIndex = buffer.indexOf('\n');
    if (newlineIndex === -1) {
      break;
    }
    const line = buffer.slice(0, newlineIndex).trim();
    buffer = buffer.slice(newlineIndex + 1);
    if (!line) {
      continue;
    }
    const message = parseLine(line);
    if (!message) {
      continue;
    }
    handleMessage(message);
  }
});

process.stdin.on('end', () => process.exit(0));
"#;

    script
        .replace("__CONFIG_PATH__", &config_path)
        .replace("__REQUEST_LOG_PATH__", &request_log_path)
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .with_context(|| format!("read {} metadata", path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions)
            .with_context(|| format!("chmod +x {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_mutex() -> &'static std::sync::Mutex<()> {
        crate::test_environment_mutex()
    }

    #[test]
    fn fake_app_server_replays_login_completion_and_records_requests() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture =
            FakeCodexAppServerFixture::builder("codex-rotate-app-server-login")?.build()?;
        let mut client = fixture.spawn_stdio_client()?;

        let initialize = client.request("initialize", json!({"capabilities": {}}))?;
        assert_eq!(initialize["platformOs"], Value::String("macos".to_string()));

        let login = client.request("account/login/start", json!({"type": "chatgpt"}))?;
        assert_eq!(
            login["loginId"],
            Value::String(DEFAULT_LOGIN_ID.to_string())
        );

        let completion = client.wait_for_notification("account/login/completed")?;
        assert_eq!(completion["success"], Value::Bool(true));
        assert_eq!(
            completion["loginId"],
            Value::String(DEFAULT_LOGIN_ID.to_string())
        );

        let methods = fixture.request_methods()?;
        assert_eq!(methods, vec!["initialize", "account/login/start"]);
        Ok(())
    }

    #[test]
    fn fake_app_server_handles_thread_handoff_methods_with_scripted_overrides() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = FakeCodexAppServerFixture::builder("codex-rotate-app-server-thread")?
            .thread(
                "thread-source",
                json!({
                    "id": "thread-source",
                    "cwd": "/workspace/source",
                    "preview": "Latest visible preview",
                    "status": { "type": "active" },
                    "turns": [
                        {
                            "id": "turn-1",
                            "items": [
                                { "type": "userMessage", "content": [{ "type": "text", "text": "hello" }] },
                                { "type": "agentMessage", "text": "world" }
                            ]
                        }
                    ]
                }),
            )
            .next_thread_ids(["thread-target"]) 
            .method_result("thread/read", json!({"thread": {"id": "thread-override"}}))
            .build()?;
        let mut client = fixture.spawn_stdio_client()?;

        let _ = client.request("initialize", json!({}))?;
        let read = client.request(
            "thread/read",
            json!({"threadId": "thread-source", "includeTurns": true}),
        )?;
        assert_eq!(
            read["thread"]["id"],
            Value::String("thread-override".to_string())
        );

        let started = client.request(
            "thread/start",
            json!({
                "cwd": "/workspace/target",
                "model": Value::Null,
                "modelProvider": Value::Null,
                "serviceTier": Value::Null,
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandbox": Value::Null,
                "personality": "pragmatic",
            }),
        )?;
        assert_eq!(
            started["thread"]["id"],
            Value::String("thread-target".to_string())
        );

        let inject = client.request(
            "thread/inject_items",
            json!({
                "threadId": "thread-target",
                "items": [{ "type": "message", "role": "user", "content": [] }],
            }),
        )?;
        assert_eq!(inject, Value::Object(Default::default()));

        let turn = client.request(
            "turn/start",
            json!({
                "threadId": "thread-target",
                "input": [{ "type": "text", "text": "continue", "text_elements": [] }],
                "cwd": "/workspace/target",
                "approvalPolicy": Value::Null,
                "approvalsReviewer": "user",
                "sandboxPolicy": Value::Null,
                "model": Value::Null,
                "serviceTier": Value::Null,
                "effort": Value::Null,
                "summary": "none",
                "personality": Value::Null,
                "outputSchema": Value::Null,
                "collaborationMode": Value::Null,
                "attachments": [],
            }),
        )?;
        assert_eq!(turn, Value::Object(Default::default()));

        let methods = fixture.request_methods()?;
        assert_eq!(
            methods,
            vec![
                "initialize",
                "thread/read",
                "thread/start",
                "thread/inject_items",
                "turn/start",
            ]
        );
        Ok(())
    }

    #[test]
    fn launch_failure_exits_before_reading_requests() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let fixture = FakeCodexAppServerFixture::builder("codex-rotate-app-server-failure")?
            .launch_failure("simulated launch failure")
            .build()?;

        let output = Command::new(fixture.codex_bin_path())
            .args(["app-server", "--listen", "stdio://"])
            .output()
            .context("run fake codex app-server")?;

        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("simulated launch failure"));
        Ok(())
    }
}
