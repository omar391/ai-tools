use std::io::Write;
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tempfile::NamedTempFile;

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
    let mut request_file =
        NamedTempFile::new().context("Failed to create automation bridge request file.")?;
    request_file.write_all(&request)?;
    request_file.flush()?;
    let child = Command::new(&paths.bun_bin)
        .arg(&paths.automation_bridge_entrypoint)
        .arg("--request-file")
        .arg(request_file.path())
        .current_dir(&paths.repo_root)
        .env("CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK", "1")
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| {
            format!(
                "Failed to run {} {}.",
                paths.bun_bin,
                paths.automation_bridge_entrypoint.display()
            )
        })?;

    let output = child.wait_with_output()?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let response = serde_json::from_slice::<BridgeResponse<TResult>>(&output.stdout).ok();
    if let Some(response) = response {
        if response.ok && output.status.success() {
            return response
                .result
                .ok_or_else(|| anyhow!("Automation bridge returned no result for {command}."));
        }

        return Err(anyhow!(
            "{}",
            response
                .error
                .and_then(|error| error.message)
                .unwrap_or_else(|| format!("Automation bridge command {command} failed."))
        ));
    }

    if !output.status.success() {
        return Err(anyhow!(if !stdout.is_empty() {
            stdout
        } else {
            format!("Automation bridge command {command} failed.")
        }));
    }

    Err(anyhow!(
        "Automation bridge returned invalid JSON for {command}."
    ))
}
