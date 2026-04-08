use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, Context, Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};
use tempfile::NamedTempFile;

use crate::paths::{cleanup_legacy_rotate_home_artifacts, resolve_paths};

const FAST_BROWSER_EVENT_PREFIX: &str = "__FAST_BROWSER_EVENT__";

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
    cleanup_legacy_rotate_home_artifacts(&paths.rotate_home)?;
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
        if let Some(forwarded) = bridge_progress_line_for_cli(&line) {
            progress(forwarded);
        }
    }
    Ok(())
}

fn bridge_progress_line_for_cli(line: &str) -> Option<String> {
    if line.trim().is_empty() {
        return None;
    }
    if line.starts_with(FAST_BROWSER_EVENT_PREFIX) {
        return format_fast_browser_progress_event_line(line);
    }
    Some(
        line.strip_prefix("[codex-rotate] ")
            .map(str::to_string)
            .unwrap_or_else(|| line.to_string()),
    )
}

fn format_fast_browser_progress_event_line(line: &str) -> Option<String> {
    let raw = line.strip_prefix(FAST_BROWSER_EVENT_PREFIX)?.trim();
    if raw.is_empty() {
        return None;
    }
    let event = serde_json::from_str::<Value>(raw).ok()?;
    let record = event.as_object()?;
    let workflow = read_string_value(record, "workflow");
    let step_id = read_string_value(record, "stepId");
    let phase = read_string_value(record, "phase");
    let status = read_string_value(record, "status");
    if should_suppress_fast_browser_progress_event(phase.as_deref(), status.as_deref()) {
        return None;
    }

    let message = read_string_value(record, "message");
    let time = read_string_value(record, "time");
    let details = record.get("details").and_then(Value::as_object);
    let scope = [workflow, step_id]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join("/");
    let state = format_fast_browser_event_state(phase.as_deref(), status.as_deref());
    let mut detail_parts = Vec::new();
    if let Some(details) = details {
        if let Some(reason) = read_string_value(details, "reason") {
            detail_parts.push(format!("reason={reason}"));
        }
        if let Some(relay_url) = read_string_value(details, "relay_url") {
            detail_parts.push(format!("relay_url={relay_url}"));
        }
        if let Some(workflow_stack_len) = details
            .get("workflow_stack")
            .and_then(Value::as_array)
            .map(Vec::len)
            .filter(|value| *value > 0)
        {
            detail_parts.push(format!("workflow_stack={workflow_stack_len}"));
        }
        if let Some(headline) = read_string_value(details, "headline") {
            detail_parts.push(format!("headline={headline:?}"));
        }
        if let Some(action_kind) = read_string_value(details, "action_kind") {
            detail_parts.push(format!("action={action_kind}"));
        }
        if let Some(stage) = read_string_value(details, "stage") {
            detail_parts.push(format!("stage={stage}"));
        }
        if let Some(current_url) = read_string_value(details, "current_url") {
            detail_parts.push(format!("url={current_url}"));
        }
        if let Some(run_path) = read_string_value(details, "run_path")
            .or_else(|| read_string_value(details, "run_status_path"))
        {
            detail_parts.push(format!("run={run_path}"));
        }
        if let Some(screenshot_path) = read_string_value(details, "screenshot_path") {
            detail_parts.push(format!("screenshot={screenshot_path}"));
        }
    }
    let primary_text = details
        .and_then(|details| read_string_value(details, "step_goal"))
        .or(message);
    let prefix = [
        (!scope.is_empty()).then_some(scope),
        (!state.is_empty()).then_some(state),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ");
    let suffix = (!detail_parts.is_empty()).then(|| format!(" ({})", detail_parts.join(", ")));
    let core = match (prefix.is_empty(), primary_text, suffix) {
        (false, Some(primary_text), Some(suffix)) => {
            Some(format!("{prefix}: {primary_text}{suffix}"))
        }
        (false, Some(primary_text), None) => Some(format!("{prefix}: {primary_text}")),
        (true, Some(primary_text), Some(suffix)) => Some(format!("{primary_text}{suffix}")),
        (true, Some(primary_text), None) => Some(primary_text),
        (false, None, Some(suffix)) => Some(format!("{prefix}{suffix}")),
        (false, None, None) => Some(prefix),
        (true, None, Some(suffix)) => Some(
            suffix
                .trim_start()
                .trim_end_matches(')')
                .trim_start_matches('(')
                .to_string(),
        ),
        (true, None, None) => None,
    }?;
    Some(match time {
        Some(time) => format!("[fast-browser] {time} {core}"),
        None => format!("[fast-browser] {core}"),
    })
}

fn should_suppress_fast_browser_progress_event(phase: Option<&str>, status: Option<&str>) -> bool {
    matches!(
        (phase.unwrap_or_default(), status.unwrap_or_default()),
        ("pre", "start") | ("pre", "ok") | ("post", "start") | ("post", "ok") | ("action", "start")
    )
}

fn format_fast_browser_event_state(phase: Option<&str>, status: Option<&str>) -> String {
    match (phase.unwrap_or_default(), status.unwrap_or_default()) {
        ("step", "start") => "step".to_string(),
        ("step", "ok") => "step ok".to_string(),
        ("step", "skipped") => "step skip".to_string(),
        ("action", "ok") => "done".to_string(),
        ("action", "resume") => "resume".to_string(),
        ("workflow", "finish") => "workflow finish".to_string(),
        _ => [phase, status]
            .into_iter()
            .flatten()
            .filter(|value| !value.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn read_string_value(record: &Map<String, Value>, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{bridge_progress_line_for_cli, format_fast_browser_progress_event_line};

    #[test]
    fn formats_fast_browser_progress_events_in_rust() {
        let line = r#"__FAST_BROWSER_EVENT__{"time":"2026-04-08T02:13:48.020Z","workflow":"collect-verification-artifact","stepId":"collect_verification_artifact","phase":"step","status":"start","details":{"step_goal":"Search Gmail, open the matching message, and extract a code or matching verification link.","action_kind":"playwright","current_url":"https://mail.google.com/mail/u/0/#inbox"}}"#;

        assert_eq!(
            format_fast_browser_progress_event_line(line).as_deref(),
            Some("[fast-browser] 2026-04-08T02:13:48.020Z collect-verification-artifact/collect_verification_artifact step: Search Gmail, open the matching message, and extract a code or matching verification link. (action=playwright, url=https://mail.google.com/mail/u/0/#inbox)")
        );
    }

    #[test]
    fn suppresses_low_signal_fast_browser_progress_events() {
        let line =
            r#"__FAST_BROWSER_EVENT__{"phase":"pre","status":"ok","message":"setup complete"}"#;
        assert_eq!(format_fast_browser_progress_event_line(line), None);
    }

    #[test]
    fn suppresses_raw_fast_browser_event_markers_when_event_is_invalid() {
        let line = "__FAST_BROWSER_EVENT__not-json";
        assert_eq!(bridge_progress_line_for_cli(line), None);
    }
}
