use std::io::Write;
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::paths::resolve_paths;

#[derive(Debug, Serialize)]
struct BridgeRequest<'a, TPayload> {
    command: &'a str,
    payload: TPayload,
}

#[derive(Debug, serde::Deserialize)]
struct BridgeResponse<TPayload> {
    ok: bool,
    result: Option<TPayload>,
    error: Option<BridgeErrorPayload>,
}

#[derive(Debug, serde::Deserialize)]
struct BridgeErrorPayload {
    message: Option<String>,
}

pub fn run_automation_bridge<TPayload, TResult>(command: &str, payload: TPayload) -> Result<TResult>
where
    TPayload: Serialize,
    TResult: DeserializeOwned,
{
    let paths = resolve_paths()?;
    let request = serde_json::to_vec(&BridgeRequest { command, payload })?;
    let mut child = Command::new(&paths.bun_bin)
        .arg(&paths.automation_bridge_entrypoint)
        .current_dir(&paths.repo_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to run {} {}.",
                paths.bun_bin,
                paths.automation_bridge_entrypoint.display()
            )
        })?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| anyhow!("Automation bridge stdin was unavailable."))?;
        stdin.write_all(&request)?;
    }

    let output = child.wait_with_output()?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Err(anyhow!(if !stderr.is_empty() {
            stderr
        } else if !stdout.is_empty() {
            stdout
        } else {
            format!("Automation bridge command {command} failed.")
        }));
    }

    let response: BridgeResponse<TResult> = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("Automation bridge returned invalid JSON for {command}."))?;
    if response.ok {
        response
            .result
            .ok_or_else(|| anyhow!("Automation bridge returned no result for {command}."))
    } else {
        Err(anyhow!(
            "{}",
            response
                .error
                .and_then(|error| error.message)
                .unwrap_or_else(|| format!("Automation bridge command {command} failed."))
        ))
    }
}
