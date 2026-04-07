use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::paths::resolve_paths;

#[derive(Debug, Serialize)]
struct BridgeRequest<'a, TPayload> {
    command: &'a str,
    payload: TPayload,
}

#[derive(Debug, serde::Deserialize)]
struct BridgeResponse {
    ok: bool,
    #[serde(default)]
    result: Value,
    error: Option<BridgeErrorPayload>,
}

#[derive(Debug, serde::Deserialize)]
struct BridgeErrorPayload {
    message: Option<String>,
}

pub type AutomationProgressCallback = Arc<dyn Fn(String) + Send + Sync + 'static>;

pub fn run_automation_bridge<TPayload, TResult>(command: &str, payload: TPayload) -> Result<TResult>
where
    TPayload: Serialize,
    TResult: DeserializeOwned,
{
    run_automation_bridge_with_progress(command, payload, None)
}

pub fn run_automation_bridge_with_progress<TPayload, TResult>(
    command: &str,
    payload: TPayload,
    progress: Option<AutomationProgressCallback>,
) -> Result<TResult>
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
    let mut process = Command::new(&paths.node_bin);
    if paths
        .automation_bridge_entrypoint
        .extension()
        .and_then(|value| value.to_str())
        == Some("ts")
    {
        process.arg("--experimental-strip-types");
    }
    let child = process
        .arg(&paths.automation_bridge_entrypoint)
        .arg("--request-file")
        .arg(request_file.path())
        .current_dir(&paths.asset_root)
        .env("CODEX_ROTATE_ASSET_ROOT", paths.asset_root.as_os_str())
        .env("CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK", "1")
        .stdin(Stdio::inherit())
        .stdout(Stdio::piped())
        .stderr(if progress.is_some() {
            Stdio::piped()
        } else {
            Stdio::inherit()
        })
        .spawn()
        .with_context(|| {
            format!(
                "Failed to run {} {}.",
                paths.node_bin,
                paths.automation_bridge_entrypoint.display()
            )
        })?;

    let mut child = child;
    let stderr_reader = match progress {
        Some(progress) => {
            let stderr = child
                .stderr
                .take()
                .context("Automation bridge stderr was unavailable for progress streaming.")?;
            Some(thread::spawn(move || {
                stream_bridge_progress(stderr, progress)
            }))
        }
        None => None,
    };

    let output = child.wait_with_output()?;
    if let Some(reader) = stderr_reader {
        reader
            .join()
            .map_err(|_| anyhow!("Automation bridge progress reader panicked."))??;
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let response = serde_json::from_slice::<BridgeResponse>(&output.stdout).ok();
    if let Some(response) = response {
        if response.ok && output.status.success() {
            return serde_json::from_value(response.result).with_context(|| {
                format!("Automation bridge returned an incompatible result for {command}.")
            });
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

fn stream_bridge_progress(
    stderr: impl std::io::Read,
    progress: AutomationProgressCallback,
) -> Result<()> {
    let reader = BufReader::new(stderr);
    for line in reader.lines() {
        let line = line.context("Failed to read automation bridge stderr.")?;
        if line.trim().is_empty() {
            continue;
        }
        let forwarded = line
            .strip_prefix("[codex-rotate] ")
            .map(str::to_string)
            .unwrap_or(line);
        progress(forwarded);
    }
    Ok(())
}
