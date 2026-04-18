use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use chrono::{SecondsFormat, Utc};
use codex_rotate_core::paths::resolve_paths;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_PROFILE: &str = "dev-1";
const DEFAULT_RUNS: u32 = 1;
const PROFILE_IDLE_WAIT_TIMEOUT: Duration = Duration::from_secs(90);
const PROFILE_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Track {
    NonDevice,
    DeviceAuth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    All,
    NonDevice,
    DeviceAuth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchmarkOperation {
    Create,
    Relogin,
}

#[derive(Clone, Debug)]
struct BenchmarkCandidate {
    id: &'static str,
    track: Track,
    file_path: PathBuf,
    workflow_ref: &'static str,
}

#[derive(Clone, Debug)]
struct Options {
    mode: Mode,
    runs: u32,
    profile_name: String,
    operation: BenchmarkOperation,
    relogin_selector_override: Option<String>,
    template_override: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct Snapshot {
    auth_email: Option<String>,
    default_create_template: Option<String>,
    account_emails: HashSet<String>,
    pending_emails: HashSet<String>,
}

#[derive(Clone, Debug)]
struct CommandResult {
    exit_status: Option<i32>,
    stdout: String,
    stderr: String,
}

#[derive(Clone, Debug, Serialize)]
struct BenchmarkRecord {
    competitor: String,
    workflow_id: String,
    workflow_ref: String,
    workflow_file: String,
    track: Track,
    operation: BenchmarkOperation,
    task: String,
    run_label: String,
    cold: bool,
    success: bool,
    latency_ms: u128,
    exit_status: Option<i32>,
    failure_mode: Option<String>,
    created_emails: Vec<String>,
    new_pending_emails: Vec<String>,
    intended_target_email: Option<String>,
    displayed_created_label: Option<String>,
    displayed_created_email: Option<String>,
    environment_blocked: bool,
    used_top_level_fallback: bool,
    top_level_fallback_workflows: Vec<String>,
    selection_eligible: bool,
    selection_invalid_reasons: Vec<String>,
    auth_before: Option<String>,
    auth_after: Option<String>,
    auth_restored: bool,
    template: String,
    notes: Option<String>,
    stdout_tail: Option<String>,
    stderr_tail: Option<String>,
    measured_at: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct GroupSummary {
    workflow_id: String,
    workflow_ref: String,
    workflow_file: String,
    track: Track,
    runs: usize,
    successes: usize,
    success_rate: f64,
    median_latency_ms: Option<u128>,
    best_latency_ms: Option<u128>,
    eligible_runs: usize,
    eligible_successes: usize,
    eligible_success_rate: f64,
    eligible_median_latency_ms: Option<u128>,
    eligible_best_latency_ms: Option<u128>,
    failure_modes: BTreeMap<String, usize>,
    latest_run_label: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
struct SummaryOutput {
    groups: BTreeMap<String, GroupSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct SelectionEntry {
    workflow_id: String,
    workflow_ref: String,
    workflow_file: String,
    median_latency_ms: Option<u128>,
    success_rate: f64,
}

#[derive(Clone, Debug, Serialize)]
struct SelectionOutput {
    selected_non_device: Option<SelectionEntry>,
    selected_device_auth: Option<SelectionEntry>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FastBrowserCliEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error: Option<FastBrowserCliError>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FastBrowserCliError {
    message: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileInspectStatus {
    profile_name: Option<String>,
    request_queue: Option<ProfileInspectRequestQueue>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProfileInspectRequestQueue {
    active: Option<Value>,
    queued_count: Option<u64>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let options = parse_args(&env::args().skip(1).collect::<Vec<_>>())?;
    let repo_root = repo_root()?;
    ensure_benchmark_worktree_operation_allowed(&repo_root, options.operation)?;
    let rotate_home = resolve_home_override("CODEX_ROTATE_HOME", ".codex-rotate")?;
    let codex_home = resolve_home_override("CODEX_HOME", ".codex")?;
    let cli_binary = resolve_cli_binary(&repo_root)?;
    let workflow_root = repo_root
        .join(".fast-browser")
        .join("workflows")
        .join("web")
        .join("auth.openai.com");
    let results_root = repo_root
        .join("packages")
        .join("codex-rotate")
        .join("benchmarks")
        .join("results")
        .join("account-flows");
    let raw_dir = results_root.join("raw");
    let rotate_state_path = rotate_home.join("accounts.json");
    let summary_script = repo_root
        .join("..")
        .join("ai-rules")
        .join("skills")
        .join("competitive-benchmark-loop")
        .join("scripts")
        .join("summarize_benchmarks.py");

    fs::create_dir_all(&raw_dir)?;
    let benchmark_run_id = Utc::now().format("%Y-%m-%dT%H-%M-%S%.3fZ").to_string();

    let raw_jsonl_path = raw_dir.join(format!("{benchmark_run_id}.jsonl"));
    let raw_json_path = raw_dir.join(format!("{benchmark_run_id}.json"));
    let compat_summary_path = results_root.join(format!("{benchmark_run_id}.compat-summary.json"));
    let summary_path = results_root.join(format!("{benchmark_run_id}.summary.json"));
    let selection_path = results_root.join(format!("{benchmark_run_id}.selection.json"));
    let report_path = results_root.join(format!("{benchmark_run_id}.report.md"));

    let latest_raw_jsonl_path = raw_dir.join("latest.jsonl");
    let latest_raw_json_path = raw_dir.join("latest.json");
    let latest_compat_summary_path = results_root.join("latest.compat-summary.json");
    let latest_summary_path = results_root.join("latest.summary.json");
    let latest_selection_path = results_root.join("latest.selection.json");
    let latest_report_path = results_root.join("latest.report.md");

    let candidates = benchmark_candidates(&workflow_root)
        .into_iter()
        .filter(|candidate| match options.mode {
            Mode::All => true,
            Mode::NonDevice => candidate.track == Track::NonDevice,
            Mode::DeviceAuth => candidate.track == Track::DeviceAuth,
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(anyhow!("No benchmark candidates were selected."));
    }

    let mut records = Vec::new();
    let mut reserved_relogin_selectors = HashSet::new();
    for candidate in &candidates {
        for iteration in 1..=options.runs {
            wait_for_managed_profile_idle(&repo_root, &options.profile_name)?;
            let snapshot = read_snapshot(&rotate_state_path, &codex_home);
            let template = resolve_benchmark_template(&options, candidate, &snapshot)?;
            let record = benchmark_candidate(
                candidate,
                iteration,
                &options,
                &template,
                &repo_root,
                &cli_binary,
                &rotate_state_path,
                &codex_home,
                &mut reserved_relogin_selectors,
            )?;
            println!(
                "[benchmark] {} success={} latency_ms={} created={} failure={}",
                record.workflow_id,
                record.success,
                record.latency_ms,
                if record.created_emails.is_empty() {
                    "-".to_string()
                } else {
                    record.created_emails.join(",")
                },
                record
                    .failure_mode
                    .clone()
                    .unwrap_or_else(|| "-".to_string())
            );
            records.push(record);
        }
    }

    write_json(&raw_json_path, &records)?;
    write_jsonl(&raw_jsonl_path, &records)?;

    let compat_summary = run_summary_script(
        &repo_root,
        &summary_script,
        &raw_jsonl_path,
        &compat_summary_path,
    )?;
    let summary = build_summary(&records);
    write_json(&summary_path, &summary)?;
    let selection = build_selection(&summary);
    write_json(&selection_path, &selection)?;
    let report = build_report(&benchmark_run_id, &options, &records, &summary, &selection);
    fs::write(&report_path, report)?;

    fs::copy(&raw_json_path, &latest_raw_json_path).ok();
    fs::copy(&raw_jsonl_path, &latest_raw_jsonl_path).ok();
    if compat_summary {
        fs::copy(&compat_summary_path, &latest_compat_summary_path).ok();
    }
    fs::copy(&summary_path, &latest_summary_path).ok();
    fs::copy(&selection_path, &latest_selection_path).ok();
    fs::copy(&report_path, &latest_report_path).ok();

    println!("Results:");
    println!("- raw jsonl: {}", raw_jsonl_path.display());
    if compat_summary {
        println!("- compat summary: {}", compat_summary_path.display());
    }
    println!("- summary: {}", summary_path.display());
    println!("- selection: {}", selection_path.display());
    println!("- report: {}", report_path.display());
    println!(
        "Selected non-device: {}",
        selection
            .selected_non_device
            .as_ref()
            .map(|value| value.workflow_id.as_str())
            .unwrap_or("none")
    );
    println!(
        "Selected device-auth: {}",
        selection
            .selected_device_auth
            .as_ref()
            .map(|value| value.workflow_id.as_str())
            .unwrap_or("none")
    );

    Ok(())
}

fn ensure_benchmark_worktree_operation_allowed(
    repo_root: &Path,
    operation: BenchmarkOperation,
) -> Result<()> {
    let _ = (repo_root, operation);
    Ok(())
}

fn parse_args(args: &[String]) -> Result<Options> {
    let mut mode = Mode::All;
    let mut runs = DEFAULT_RUNS;
    let mut profile_name = DEFAULT_PROFILE.to_string();
    let mut operation = BenchmarkOperation::Create;
    let mut relogin_selector_override = None;
    let mut template_override = HashMap::new();

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--non-device" => mode = Mode::NonDevice,
            "--device-auth" => mode = Mode::DeviceAuth,
            "--all" => mode = Mode::All,
            "--create" => operation = BenchmarkOperation::Create,
            "--relogin" => operation = BenchmarkOperation::Relogin,
            "--selector" => {
                index += 1;
                relogin_selector_override = Some(
                    args.get(index)
                        .cloned()
                        .ok_or_else(|| anyhow!("--selector requires a value"))?,
                );
            }
            _ if arg.starts_with("--selector=") => {
                relogin_selector_override = Some(arg["--selector=".len()..].to_string());
            }
            _ if arg.starts_with("--relogin=") => {
                operation = BenchmarkOperation::Relogin;
                relogin_selector_override = Some(arg["--relogin=".len()..].to_string());
            }
            "--profile" => {
                index += 1;
                profile_name = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| anyhow!("--profile requires a value"))?;
            }
            _ if arg.starts_with("--profile=") => {
                profile_name = arg["--profile=".len()..].to_string();
            }
            "--runs" => {
                index += 1;
                runs = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--runs requires a value"))?
                    .parse::<u32>()
                    .context("--runs must be a positive integer")?;
            }
            _ if arg.starts_with("--runs=") => {
                runs = arg["--runs=".len()..]
                    .parse::<u32>()
                    .context("--runs must be a positive integer")?;
            }
            "--template" => {
                index += 1;
                let value = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| anyhow!("--template requires workflow=value"))?;
                assign_template_override(&mut template_override, &value)?;
            }
            _ if arg.starts_with("--template=") => {
                assign_template_override(&mut template_override, &arg["--template=".len()..])?;
            }
            _ => return Err(anyhow!("Unknown benchmark option: {arg}")),
        }
        index += 1;
    }

    if runs < 1 {
        return Err(anyhow!("--runs must be a positive integer"));
    }

    Ok(Options {
        mode,
        runs,
        profile_name,
        operation,
        relogin_selector_override,
        template_override,
    })
}

fn assign_template_override(overrides: &mut HashMap<String, String>, raw: &str) -> Result<()> {
    if let Some((key, value)) = raw.split_once('=') {
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return Err(anyhow!("Invalid --template override: {raw}"));
        }
        overrides.insert(key.to_string(), value.to_string());
    } else {
        overrides.insert("non_device".to_string(), raw.to_string());
        overrides.insert("device_auth".to_string(), raw.to_string());
    }
    Ok(())
}

fn resolve_benchmark_template(
    options: &Options,
    candidate: &BenchmarkCandidate,
    snapshot: &Snapshot,
) -> Result<String> {
    if let Some(value) = options
        .template_override
        .get(candidate.id)
        .or_else(|| options.template_override.get(track_key(candidate.track)))
    {
        return Ok(value.clone());
    }
    if let Some(value) = snapshot.default_create_template.as_deref() {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_string());
        }
    }
    Err(anyhow!(
        "No default_create_template is configured in ~/.codex-rotate/accounts.json and no --template override was provided."
    ))
}

fn benchmark_candidate(
    candidate: &BenchmarkCandidate,
    iteration: u32,
    options: &Options,
    template: &str,
    repo_root: &Path,
    cli_binary: &Path,
    rotate_state_path: &Path,
    codex_home: &Path,
    reserved_relogin_selectors: &mut HashSet<String>,
) -> Result<BenchmarkRecord> {
    let before = read_snapshot(rotate_state_path, codex_home);
    let run_label = format!("{}-{}", candidate.id, iso_now());
    let relogin_selector = match options.operation {
        BenchmarkOperation::Create => None,
        BenchmarkOperation::Relogin => {
            let selector = resolve_benchmark_relogin_selector(
                options,
                &before,
                template,
                reserved_relogin_selectors,
            )?;
            reserved_relogin_selectors.insert(selector.clone());
            Some(selector)
        }
    };
    let command =
        build_benchmark_command(cli_binary, options, template, relogin_selector.as_deref());
    let mut command_env = env::vars_os().collect::<HashMap<_, _>>();
    command_env.insert(
        "CODEX_ROTATE_ACCOUNT_FLOW_FILE".into(),
        candidate.file_path.clone().into_os_string(),
    );
    command_env.insert("CODEX_ROTATE_STOP_ON_FINAL_ADD_PHONE".into(), "1".into());

    println!(
        "[benchmark] starting {} iteration={} flow={} template={}",
        candidate.id,
        iteration,
        candidate.file_path.display(),
        template
    );

    let measured_at = iso_now();
    let started = Instant::now();
    let run_result = run_command_with_capture(&command, repo_root, &command_env, candidate.id);
    let after = read_snapshot(rotate_state_path, codex_home);
    let result = run_result?;
    let latency_ms = started.elapsed().as_millis();
    let created_emails = difference(&after.account_emails, &before.account_emails);
    let new_pending_emails = difference(&after.pending_emails, &before.pending_emails);
    let command_succeeded = result.exit_status == Some(0);
    let combined_output = if result.stderr.is_empty() {
        result.stdout.clone()
    } else if result.stdout.is_empty() {
        result.stderr.clone()
    } else {
        format!("{}\n{}", result.stdout, result.stderr)
    };
    let environment_blocked =
        is_add_phone_environment_blocker(&combined_output, result.exit_status);
    let success = command_succeeded || environment_blocked;
    let intended_target_email = extract_intended_target_email(&combined_output).or_else(|| {
        relogin_selector
            .as_deref()
            .and_then(|value| normalize_email(Some(value)))
    });
    let displayed_created_label = extract_displayed_account_label(&combined_output);
    let displayed_created_email = displayed_created_label
        .as_deref()
        .and_then(extract_email_from_label)
        .or_else(|| extract_displayed_account_email(&combined_output));
    let top_level_fallback_workflows =
        detect_top_level_fallback_workflows(&combined_output, candidate.workflow_ref);
    let selection_invalid_reasons = classify_selection_invalid_reasons(
        options.operation,
        success,
        intended_target_email.as_deref(),
        &created_emails,
        displayed_created_label.as_deref(),
        displayed_created_email.as_deref(),
        &top_level_fallback_workflows,
    );
    let selection_eligible = success && selection_invalid_reasons.is_empty();

    Ok(BenchmarkRecord {
        competitor: candidate.id.to_string(),
        workflow_id: candidate.id.to_string(),
        workflow_ref: candidate.workflow_ref.to_string(),
        workflow_file: candidate.file_path.display().to_string(),
        track: candidate.track,
        operation: options.operation,
        task: match (options.operation, candidate.track) {
            (BenchmarkOperation::Create, Track::DeviceAuth) => {
                "openai-account-create-device-auth".to_string()
            }
            (BenchmarkOperation::Create, Track::NonDevice) => {
                "openai-account-create-non-device".to_string()
            }
            (BenchmarkOperation::Relogin, Track::DeviceAuth) => {
                "openai-account-relogin-device-auth".to_string()
            }
            (BenchmarkOperation::Relogin, Track::NonDevice) => {
                "openai-account-relogin-non-device".to_string()
            }
        },
        run_label,
        cold: true,
        success,
        latency_ms,
        exit_status: result.exit_status,
        failure_mode: if success {
            None
        } else {
            Some(classify_failure_mode(&combined_output))
        },
        created_emails: created_emails.clone(),
        new_pending_emails: new_pending_emails.clone(),
        intended_target_email: intended_target_email.clone(),
        displayed_created_label,
        displayed_created_email,
        environment_blocked,
        used_top_level_fallback: !top_level_fallback_workflows.is_empty(),
        top_level_fallback_workflows: top_level_fallback_workflows.clone(),
        selection_eligible,
        selection_invalid_reasons: selection_invalid_reasons.clone(),
        auth_before: before.auth_email.clone(),
        auth_after: after.auth_email.clone(),
        auth_restored: before.auth_email == after.auth_email,
        template: template.to_string(),
        notes: build_notes(
            options.operation,
            success,
            environment_blocked,
            &created_emails,
            &new_pending_emails,
            &before,
            &after,
            intended_target_email.as_deref(),
            &top_level_fallback_workflows,
            &selection_invalid_reasons,
        ),
        stdout_tail: tail_text(&result.stdout, 12),
        stderr_tail: tail_text(&result.stderr, 12),
        measured_at,
    })
}

fn build_benchmark_command(
    cli_binary: &Path,
    options: &Options,
    template: &str,
    relogin_selector: Option<&str>,
) -> Vec<PathBuf> {
    match options.operation {
        BenchmarkOperation::Create => vec![
            cli_binary.to_path_buf(),
            PathBuf::from("internal"),
            PathBuf::from("create"),
            PathBuf::from("--force"),
            PathBuf::from("--restore-auth"),
            PathBuf::from("--profile"),
            PathBuf::from(options.profile_name.as_str()),
            PathBuf::from("--template"),
            PathBuf::from(template),
        ],
        BenchmarkOperation::Relogin => vec![
            cli_binary.to_path_buf(),
            PathBuf::from("internal"),
            PathBuf::from("relogin"),
            PathBuf::from(relogin_selector.unwrap_or_default()),
        ],
    }
}

fn resolve_benchmark_relogin_selector(
    options: &Options,
    snapshot: &Snapshot,
    template: &str,
    reserved: &HashSet<String>,
) -> Result<String> {
    if let Some(value) = options.relogin_selector_override.as_deref() {
        return Ok(value.to_string());
    }

    let mut candidates = snapshot
        .account_emails
        .iter()
        .chain(snapshot.pending_emails.iter())
        .filter(|email| email_matches_template(email, template))
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort();
    candidates.dedup();
    candidates.sort_by(|left, right| {
        extract_template_suffix(left, template)
            .cmp(&extract_template_suffix(right, template))
            .then_with(|| left.cmp(right))
    });
    let selector = candidates
        .iter()
        .find(|email| !reserved.contains(*email))
        .cloned()
        .or_else(|| candidates.into_iter().next())
        .ok_or_else(|| {
            anyhow!(
                "No pooled or pending account matches {} for relogin benchmarking.",
                template
            )
        })?;
    Ok(selector)
}

fn resolve_cli_binary(repo_root: &Path) -> Result<PathBuf> {
    let binary_name = if cfg!(windows) {
        "codex-rotate.exe"
    } else {
        "codex-rotate"
    };
    let mut candidates = Vec::new();
    if let Some(value) = env::var_os("CODEX_ROTATE_CLI_BIN") {
        candidates.push(PathBuf::from(value));
    }
    candidates.push(repo_root.join("target").join("debug").join(binary_name));
    candidates.push(repo_root.join("target").join("release").join(binary_name));

    for candidate in &candidates {
        if candidate.is_file() {
            return Ok(candidate.clone());
        }
    }

    Err(anyhow!(
        "Unable to find the codex-rotate CLI binary. Checked:\n{}",
        candidates
            .iter()
            .map(|candidate| format!("- {}", candidate.display()))
            .collect::<Vec<_>>()
            .join("\n")
    ))
}

fn wait_for_managed_profile_idle(repo_root: &Path, profile_name: &str) -> Result<()> {
    let started = Instant::now();
    loop {
        let idle_state = inspect_profile_idle_state(repo_root, profile_name)?;
        if idle_state.idle {
            return Ok(());
        }
        if started.elapsed() >= PROFILE_IDLE_WAIT_TIMEOUT {
            return Err(anyhow!(
                "Managed profile \"{}\" stayed busy before benchmark start ({}).",
                profile_name,
                idle_state
                    .reason
                    .unwrap_or_else(|| "request queue not idle".to_string())
            ));
        }
        thread::sleep(PROFILE_IDLE_POLL_INTERVAL);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProfileIdleState {
    idle: bool,
    reason: Option<String>,
}

fn inspect_profile_idle_state(repo_root: &Path, profile_name: &str) -> Result<ProfileIdleState> {
    let paths = resolve_paths()?;
    let output = Command::new(&paths.node_bin)
        .arg(&paths.fast_browser_script)
        .arg("profiles")
        .arg("inspect")
        .arg("--profile")
        .arg(profile_name)
        .current_dir(repo_root)
        .output()
        .with_context(|| {
            format!(
                "Failed to run {} {} profiles inspect --profile {}.",
                paths.node_bin,
                paths.fast_browser_script.display(),
                profile_name
            )
        })?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        return Err(anyhow!(if !stdout.is_empty() {
            stdout
        } else {
            format!(
                "fast-browser profiles inspect --profile {} exited with status {}.",
                profile_name, output.status
            )
        }));
    }
    parse_profile_idle_state(&output.stdout)
}

fn parse_profile_idle_state(stdout: &[u8]) -> Result<ProfileIdleState> {
    let envelope: FastBrowserCliEnvelope<ProfileInspectStatus> = serde_json::from_slice(stdout)
        .context("fast-browser profiles inspect returned invalid JSON.")?;
    if !envelope.ok {
        return Err(anyhow!(
            "{}",
            envelope
                .error
                .and_then(|error| error.message)
                .unwrap_or_else(|| "fast-browser profiles inspect failed.".to_string())
        ));
    }
    let result = envelope
        .result
        .context("fast-browser profiles inspect did not return a result.")?;
    let queue = result.request_queue.unwrap_or(ProfileInspectRequestQueue {
        active: None,
        queued_count: Some(0),
    });
    let queued_count = queue.queued_count.unwrap_or(0);
    if queue.active.is_none() && queued_count == 0 {
        return Ok(ProfileIdleState {
            idle: true,
            reason: None,
        });
    }
    let reason = match (queue.active, queued_count) {
        (Some(active), 0) => Some(format!(
            "active request present for profile {}: {}",
            result
                .profile_name
                .unwrap_or_else(|| "<unknown>".to_string()),
            summarize_active_request(&active)
        )),
        (None, count) => Some(format!(
            "queuedCount={} for profile {}",
            count,
            result
                .profile_name
                .unwrap_or_else(|| "<unknown>".to_string())
        )),
        (Some(active), count) => Some(format!(
            "active request present and queuedCount={} for profile {}: {}",
            count,
            result
                .profile_name
                .unwrap_or_else(|| "<unknown>".to_string()),
            summarize_active_request(&active)
        )),
    };
    Ok(ProfileIdleState {
        idle: false,
        reason,
    })
}

fn summarize_active_request(active: &Value) -> String {
    let id = active.get("id").and_then(Value::as_i64);
    let method = active.get("method").and_then(Value::as_str);
    let workflow_ref = active.get("workflowRef").and_then(Value::as_str);
    match (id, method, workflow_ref) {
        (Some(id), Some(method), Some(workflow_ref)) => {
            format!("id={id} method={method} workflowRef={workflow_ref}")
        }
        (Some(id), Some(method), None) => format!("id={id} method={method}"),
        (Some(id), None, Some(workflow_ref)) => format!("id={id} workflowRef={workflow_ref}"),
        (None, Some(method), Some(workflow_ref)) => {
            format!("method={method} workflowRef={workflow_ref}")
        }
        _ => active.to_string(),
    }
}

fn run_command_with_capture(
    command: &[PathBuf],
    cwd: &Path,
    env_map: &HashMap<std::ffi::OsString, std::ffi::OsString>,
    label: &str,
) -> Result<CommandResult> {
    let mut child = Command::new(&command[0])
        .args(command.iter().skip(1))
        .current_dir(cwd)
        .envs(env_map.iter())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {}", command[0].display()))?;

    let stdout = child.stdout.take().context("capture stdout")?;
    let stderr = child.stderr.take().context("capture stderr")?;
    let stdout_label = label.to_string();
    let stderr_label = label.to_string();
    let stdout_handle = thread::spawn(move || stream_output(stdout, &stdout_label, false));
    let stderr_handle = thread::spawn(move || stream_output(stderr, &stderr_label, true));
    let status = child.wait().context("wait for benchmark child")?;
    let stdout = stdout_handle
        .join()
        .map_err(|_| anyhow!("stdout thread panicked"))??;
    let stderr = stderr_handle
        .join()
        .map_err(|_| anyhow!("stderr thread panicked"))??;

    Ok(CommandResult {
        exit_status: status.code(),
        stdout,
        stderr,
    })
}

fn stream_output<R: Read>(reader: R, label: &str, is_stderr: bool) -> Result<String> {
    let mut captured = String::new();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        let read = reader.read_line(&mut line)?;
        if read == 0 {
            break;
        }
        captured.push_str(&line);
        if is_stderr {
            eprint!("[{label}] {line}");
            std::io::stderr().flush().ok();
        } else {
            print!("[{label}] {line}");
            std::io::stdout().flush().ok();
        }
    }
    Ok(captured)
}

fn read_snapshot(rotate_state_path: &Path, codex_home: &Path) -> Snapshot {
    let state = read_rotate_state(rotate_state_path);
    let auth_email = read_auth_email(codex_home);
    let default_create_template = state
        .get("default_create_template")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let account_emails = state
        .get("accounts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| normalize_email(entry.get("email").and_then(Value::as_str)))
        .collect::<HashSet<_>>();
    let pending_emails = state
        .get("pending")
        .and_then(Value::as_object)
        .into_iter()
        .flat_map(|map| map.values())
        .filter_map(|entry| normalize_email(entry.get("email").and_then(Value::as_str)))
        .collect::<HashSet<_>>();
    Snapshot {
        auth_email,
        default_create_template,
        account_emails,
        pending_emails,
    }
}

fn read_rotate_state(path: &Path) -> Value {
    fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}))
}

fn read_auth_email(codex_home: &Path) -> Option<String> {
    let auth_path = codex_home.join("auth.json");
    let raw = fs::read_to_string(auth_path).ok()?;
    let parsed = serde_json::from_str::<Value>(&raw).ok()?;
    let tokens = parsed.get("tokens")?.as_object()?;

    if let Some(email) = tokens
        .get("id_token")
        .and_then(Value::as_str)
        .and_then(parse_jwt_payload)
        .and_then(|payload| {
            payload
                .get("email")
                .and_then(Value::as_str)
                .map(|value| value.to_string())
        })
        .and_then(|value| normalize_email(Some(&value)))
    {
        return Some(email);
    }

    tokens
        .get("access_token")
        .and_then(Value::as_str)
        .and_then(parse_jwt_payload)
        .and_then(|payload| {
            payload
                .get("https://api.openai.com/profile")
                .and_then(Value::as_object)
                .and_then(|profile| profile.get("email"))
                .and_then(Value::as_str)
                .map(|value| value.to_string())
        })
        .and_then(|value| normalize_email(Some(&value)))
}

fn parse_jwt_payload(token: &str) -> Option<Value> {
    let segment = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD
        .decode(segment)
        .or_else(|_| URL_SAFE.decode(segment))
        .ok()?;
    serde_json::from_slice::<Value>(&decoded).ok()
}

fn build_notes(
    operation: BenchmarkOperation,
    success: bool,
    environment_blocked: bool,
    created_emails: &[String],
    new_pending_emails: &[String],
    before: &Snapshot,
    after: &Snapshot,
    intended_target_email: Option<&str>,
    top_level_fallback_workflows: &[String],
    selection_invalid_reasons: &[String],
) -> Option<String> {
    let mut parts = Vec::new();
    if environment_blocked {
        parts.push("environment-blocked at final add_phone".to_string());
    }
    if success && matches!(operation, BenchmarkOperation::Create) && created_emails.is_empty() {
        parts.push("create exited successfully but no new pooled account was detected".to_string());
    }
    if success {
        if let Some(intended_target_email) = intended_target_email {
            parts.push(format!("targeted {intended_target_email}"));
        }
        if !selection_invalid_reasons.is_empty() {
            parts.push(format!(
                "selection-invalid: {}",
                selection_invalid_reasons.join(", ")
            ));
        }
    }
    if !success && !new_pending_emails.is_empty() {
        parts.push(format!(
            "left pending reservation(s): {}",
            new_pending_emails.join(", ")
        ));
    }
    if !top_level_fallback_workflows.is_empty() {
        parts.push(format!(
            "used top-level fallback: {}",
            top_level_fallback_workflows.join(", ")
        ));
    }
    if before.auth_email != after.auth_email {
        parts.push(format!(
            "live auth changed from {} to {}",
            before.auth_email.as_deref().unwrap_or("none"),
            after.auth_email.as_deref().unwrap_or("none")
        ));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("; "))
    }
}

fn classify_failure_mode(output: &str) -> String {
    let normalized = output.to_lowercase();
    if normalized.contains("state mismatch") {
        "state_mismatch".to_string()
    } else if normalized.contains("did not reach the callback") {
        "callback_missing".to_string()
    } else if normalized.contains("verification is not ready") {
        "verification_pending".to_string()
    } else if normalized.contains("device authorization is rate limited") {
        "device_auth_rate_limit".to_string()
    } else if normalized.contains("too many requests") {
        "rate_limit".to_string()
    } else if normalized.contains("anti-bot") || normalized.contains("security gate") {
        "anti_bot_gate".to_string()
    } else if normalized.contains("add your phone number") || normalized.contains("add_phone") {
        "add_phone".to_string()
    } else if normalized.contains("invalid credentials") {
        "invalid_credentials".to_string()
    } else if normalized.contains("not exit cleanly") {
        "codex_login_exit".to_string()
    } else {
        "unknown".to_string()
    }
}

fn is_add_phone_environment_blocker(output: &str, exit_status: Option<i32>) -> bool {
    if exit_status == Some(0) {
        return false;
    }
    let normalized = output.to_lowercase();
    normalized.contains("after exhausting final add-phone retries")
        || normalized.contains("final_add_phone")
        || normalized.contains("the workflow requested skipping")
            && (normalized.contains("add_phone") || normalized.contains("add-phone"))
}

fn run_summary_script(
    repo_root: &Path,
    summary_script: &Path,
    input_path: &Path,
    output_path: &Path,
) -> Result<bool> {
    if !summary_script.exists() {
        eprintln!(
            "[benchmark] summarize_benchmarks.py missing at {}",
            summary_script.display()
        );
        return Ok(false);
    }
    let output = Command::new("python3")
        .arg(summary_script)
        .arg(input_path)
        .arg("--output")
        .arg(output_path)
        .current_dir(repo_root)
        .output()
        .context("run summarize_benchmarks.py")?;
    if !output.status.success() {
        eprintln!(
            "[benchmark] summarize_benchmarks.py failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        return Ok(false);
    }
    Ok(true)
}

fn build_summary(records: &[BenchmarkRecord]) -> SummaryOutput {
    let mut grouped: BTreeMap<String, Vec<&BenchmarkRecord>> = BTreeMap::new();
    for record in records {
        let key = format!("{}::{}::cold", record.workflow_id, record.task);
        grouped.entry(key).or_default().push(record);
    }
    let mut groups = BTreeMap::new();
    for (key, bucket) in grouped {
        let mut latencies = bucket
            .iter()
            .map(|record| record.latency_ms)
            .collect::<Vec<_>>();
        latencies.sort_unstable();
        let mut eligible_latencies = bucket
            .iter()
            .filter(|record| record.selection_eligible)
            .map(|record| record.latency_ms)
            .collect::<Vec<_>>();
        eligible_latencies.sort_unstable();
        let mut failure_modes = BTreeMap::new();
        for record in &bucket {
            if !record.success {
                *failure_modes
                    .entry(
                        record
                            .failure_mode
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string()),
                    )
                    .or_insert(0) += 1;
            }
        }
        let successes = bucket.iter().filter(|record| record.success).count();
        let eligible_runs = bucket
            .iter()
            .filter(|record| record.selection_eligible)
            .count();
        let eligible_successes = bucket
            .iter()
            .filter(|record| record.selection_eligible && record.success)
            .count();
        let first = bucket[0];
        groups.insert(
            key,
            GroupSummary {
                workflow_id: first.workflow_id.clone(),
                workflow_ref: first.workflow_ref.clone(),
                workflow_file: first.workflow_file.clone(),
                track: first.track,
                runs: bucket.len(),
                successes,
                success_rate: if bucket.is_empty() {
                    0.0
                } else {
                    successes as f64 / bucket.len() as f64
                },
                median_latency_ms: if latencies.is_empty() {
                    None
                } else {
                    Some(latencies[(latencies.len() - 1) / 2])
                },
                best_latency_ms: latencies.first().copied(),
                eligible_runs,
                eligible_successes,
                eligible_success_rate: if bucket.is_empty() {
                    0.0
                } else {
                    eligible_successes as f64 / bucket.len() as f64
                },
                eligible_median_latency_ms: if eligible_latencies.is_empty() {
                    None
                } else {
                    Some(eligible_latencies[(eligible_latencies.len() - 1) / 2])
                },
                eligible_best_latency_ms: eligible_latencies.first().copied(),
                failure_modes,
                latest_run_label: bucket.last().map(|record| record.run_label.clone()),
            },
        );
    }
    SummaryOutput { groups }
}

fn build_selection(summary: &SummaryOutput) -> SelectionOutput {
    SelectionOutput {
        selected_non_device: choose_winner(summary, Track::NonDevice),
        selected_device_auth: choose_winner(summary, Track::DeviceAuth),
    }
}

fn choose_winner(summary: &SummaryOutput, track: Track) -> Option<SelectionEntry> {
    let mut candidates = summary
        .groups
        .values()
        .filter(|group| group.track == track)
        .cloned()
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .eligible_success_rate
            .partial_cmp(&left.eligible_success_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                left.eligible_median_latency_ms
                    .cmp(&right.eligible_median_latency_ms)
            })
    });
    let winner = candidates
        .into_iter()
        .find(|group| group.eligible_successes > 0)?;
    Some(SelectionEntry {
        workflow_id: winner.workflow_id,
        workflow_ref: winner.workflow_ref,
        workflow_file: winner.workflow_file,
        median_latency_ms: winner.eligible_median_latency_ms,
        success_rate: winner.eligible_success_rate,
    })
}

fn build_report(
    benchmark_run_id: &str,
    options: &Options,
    records: &[BenchmarkRecord],
    summary: &SummaryOutput,
    selection: &SelectionOutput,
) -> String {
    let rows = if records.is_empty() {
        "| - | - | - | - | - | - | - | - | - | - |".to_string()
    } else {
        records
            .iter()
            .map(|record| {
                format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    record.workflow_id,
                    operation_key(record.operation),
                    track_key(record.track),
                    if record.success { "yes" } else { "no" },
                    if record.environment_blocked {
                        "yes"
                    } else {
                        "no"
                    },
                    if record.selection_eligible {
                        "yes"
                    } else {
                        "no"
                    },
                    record.latency_ms,
                    record.intended_target_email.as_deref().unwrap_or("-"),
                    if record.created_emails.is_empty() {
                        "-".to_string()
                    } else {
                        record.created_emails.join(", ")
                    },
                    record.displayed_created_label.as_deref().unwrap_or("-"),
                    record.failure_mode.as_deref().unwrap_or("-"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let group_rows = if summary.groups.is_empty() {
        "| - | - | - | - | - | - | - |".to_string()
    } else {
        summary
            .groups
            .values()
            .map(|group| {
                let failure_modes = if group.failure_modes.is_empty() {
                    "-".to_string()
                } else {
                    group
                        .failure_modes
                        .iter()
                        .map(|(name, count)| format!("{name}:{count}"))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                format!(
                    "| {} | {} | {}/{} | {}/{} | {} | {} | {} | {} |",
                    group.workflow_id,
                    track_key(group.track),
                    group.successes,
                    group.runs,
                    group.eligible_successes,
                    group.eligible_runs,
                    group.eligible_success_rate,
                    group
                        .eligible_median_latency_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    group
                        .eligible_best_latency_ms
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    failure_modes,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        "# OpenAI Account Flow Benchmark\n\n\
         - run: `{benchmark_run_id}`\n\
         - mode: `{}`\n\
         - operation: `{}`\n\
         - runs per workflow: `{}`\n\
         - managed profile: `{}`\n\n\
         ## Raw Runs\n\n\
         | workflow | operation | track | success | env-blocked | eligible | latency_ms | target | created emails | displayed label | failure |\n\
         | --- | --- | --- | --- | --- | --- | ---: | --- | --- | --- | --- |\n\
         {rows}\n\n\
         ## Summary\n\n\
         | workflow | track | successes | eligible successes | eligible success rate | eligible median latency_ms | eligible best latency_ms | failure modes |\n\
         | --- | --- | --- | --- | ---: | ---: | ---: | --- |\n\
         {group_rows}\n\n\
         ## Selection\n\n\
         - selected non-device: {}\n\
         - selected device-auth: {}\n",
        mode_key(options.mode),
        operation_key(options.operation),
        options.runs,
        options.profile_name,
        selection
            .selected_non_device
            .as_ref()
            .map(|value| format!(
                "`{}` ({} ms, success rate {})",
                value.workflow_id,
                value
                    .median_latency_ms
                    .map(|latency| latency.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                value.success_rate
            ))
            .unwrap_or_else(|| "none".to_string()),
        selection
            .selected_device_auth
            .as_ref()
            .map(|value| format!(
                "`{}` ({} ms, success rate {})",
                value.workflow_id,
                value
                    .median_latency_ms
                    .map(|latency| latency.to_string())
                    .unwrap_or_else(|| "-".to_string()),
                value.success_rate
            ))
            .unwrap_or_else(|| "none".to_string()),
    )
}

fn benchmark_candidates(workflow_root: &Path) -> Vec<BenchmarkCandidate> {
    vec![
        BenchmarkCandidate {
            id: "original",
            track: Track::NonDevice,
            file_path: workflow_root.join("codex-rotate-account-flow.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow",
        },
        BenchmarkCandidate {
            id: "stepwise",
            track: Track::NonDevice,
            file_path: workflow_root.join("codex-rotate-account-flow-stepwise.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
        },
        BenchmarkCandidate {
            id: "minimal",
            track: Track::NonDevice,
            file_path: workflow_root.join("codex-rotate-account-flow-minimal.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
        },
        BenchmarkCandidate {
            id: "device-auth",
            track: Track::DeviceAuth,
            file_path: workflow_root.join("codex-rotate-account-flow-device-auth.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
        },
    ]
}

fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, format!("{}\n", serde_json::to_string_pretty(value)?))?;
    Ok(())
}

fn write_jsonl(path: &Path, records: &[BenchmarkRecord]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = records
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .join("\n");
    fs::write(path, format!("{content}\n"))?;
    Ok(())
}

fn tail_text(value: &str, max_lines: usize) -> Option<String> {
    let lines = value
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        None
    } else {
        Some(lines[lines.len().saturating_sub(max_lines)..].join("\n"))
    }
}

fn normalize_text(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_template(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.replace("{N}", "{n}"))
    }
}

fn extract_template_suffix(email: &str, template: &str) -> Option<u32> {
    let normalized_email = normalize_email(Some(email))?;
    let normalized_template = normalize_template(template)?;
    let marker_index = normalized_template.find("{n}")?;
    let prefix = &normalized_template[..marker_index];
    let suffix = &normalized_template[marker_index + 3..];
    if !normalized_email.starts_with(prefix)
        || !normalized_email.ends_with(suffix)
        || normalized_email.len() <= prefix.len() + suffix.len()
    {
        return None;
    }
    let numeric = &normalized_email[prefix.len()..normalized_email.len() - suffix.len()];
    numeric.parse::<u32>().ok()
}

fn email_matches_template(email: &str, template: &str) -> bool {
    extract_template_suffix(email, template).is_some()
}

fn extract_intended_target_email(output: &str) -> Option<String> {
    let mut last_match = None;
    for line in output.lines() {
        let line = line.trim();
        if let Some(value) = extract_between(line, "Creating ", " via ") {
            last_match = normalize_email(Some(value));
        }
        if let Some(value) = extract_between(line, "Reusing pending account ", " via ") {
            last_match = normalize_email(Some(value));
        }
        if let Some(value) = extract_between(line, "Managed login finished for ", ". Finalizing.") {
            last_match = normalize_email(Some(value));
        }
        if let Some(value) = extract_between(line, "Adding ", " to the account pool.") {
            last_match = normalize_email(Some(value));
        }
    }
    last_match
}

fn extract_displayed_account_label(output: &str) -> Option<String> {
    let mut last_match = None;
    for line in output.lines() {
        let line = line.trim();
        if let Some(value) = extract_between(line, "Created ", " with usable quota.") {
            last_match = normalize_text(value);
        }
        if let Some(value) = extract_between(line, "Created ", " via \"") {
            last_match = normalize_text(value);
        }
        if let Some(value) = extract_between(
            line,
            "Re-logged ",
            " with stored managed-browser credentials.",
        ) {
            last_match = normalize_text(value);
        }
        if let Some(value) = extract_between(line, "Updated account \"", "\" (") {
            last_match = normalize_text(value);
        }
    }
    last_match
}

fn extract_displayed_account_email(output: &str) -> Option<String> {
    let mut last_match = None;
    for line in output.lines() {
        let line = line.trim();
        if let Some(value) = extract_between(line, "Updated account \"", "\" (") {
            last_match = extract_email_from_label(value);
        }
        if let Some(value) = extract_between(line, "\" (", ")") {
            last_match = normalize_email(Some(value));
        }
    }
    last_match
}

fn extract_email_from_label(label: &str) -> Option<String> {
    let normalized_label = normalize_text(label)?;
    if let Some((email, _plan)) = normalized_label.rsplit_once('_') {
        return normalize_email(Some(email));
    }
    normalize_email(Some(normalized_label.as_str()))
}

fn detect_top_level_fallback_workflows(output: &str, candidate_workflow_ref: &str) -> Vec<String> {
    let mut fallbacks = top_level_workflow_refs()
        .into_iter()
        .filter(|workflow_ref| {
            *workflow_ref != candidate_workflow_ref
                && contains_workflow_ref_token(output, workflow_ref)
        })
        .map(str::to_string)
        .collect::<Vec<_>>();
    fallbacks.sort();
    fallbacks.dedup();
    fallbacks
}

fn contains_workflow_ref_token(output: &str, workflow_ref: &str) -> bool {
    let mut search_start = 0;
    while let Some(relative_index) = output[search_start..].find(workflow_ref) {
        let start = search_start + relative_index;
        let end = start + workflow_ref.len();
        let before_ok = output[..start]
            .chars()
            .next_back()
            .is_none_or(|ch| !is_workflow_ref_token_char(ch));
        let after_ok = output[end..]
            .chars()
            .next()
            .is_none_or(|ch| !is_workflow_ref_token_char(ch));
        if before_ok && after_ok {
            return true;
        }
        search_start = end;
    }
    false
}

fn is_workflow_ref_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')
}

fn classify_selection_invalid_reasons(
    operation: BenchmarkOperation,
    success: bool,
    intended_target_email: Option<&str>,
    created_emails: &[String],
    displayed_created_label: Option<&str>,
    displayed_created_email: Option<&str>,
    top_level_fallback_workflows: &[String],
) -> Vec<String> {
    if !success {
        return Vec::new();
    }

    let mut reasons = Vec::new();
    let normalized_target = intended_target_email.and_then(|value| normalize_email(Some(value)));
    if normalized_target.is_none() {
        reasons.push("missing_target_email".to_string());
    }
    if matches!(operation, BenchmarkOperation::Create) {
        if displayed_created_label.is_none() {
            reasons.push("missing_displayed_created_label".to_string());
        }
        if displayed_created_email.is_none() {
            reasons.push("missing_displayed_created_email".to_string());
        }
        if created_emails.len() != 1 {
            reasons.push(format!(
                "expected_single_pooled_email_found_{}",
                created_emails.len()
            ));
        }
        if let (Some(target), Some(created)) =
            (normalized_target.as_deref(), created_emails.first())
        {
            if target != created {
                reasons.push(format!("target_pool_mismatch:{target}!={created}"));
            }
        }
        if let (Some(created), Some(displayed)) = (created_emails.first(), displayed_created_email)
        {
            if created != displayed {
                reasons.push(format!("pool_display_mismatch:{created}!={displayed}"));
            }
        }
    }
    if let (Some(target), Some(displayed)) = (normalized_target.as_deref(), displayed_created_email)
    {
        if target != displayed {
            reasons.push(format!("target_display_mismatch:{target}!={displayed}"));
        }
    }
    if !top_level_fallback_workflows.is_empty() {
        reasons.push(format!(
            "top_level_fallback:{}",
            top_level_fallback_workflows.join(",")
        ));
    }
    reasons
}

fn extract_between<'a>(value: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let start = value.find(prefix)?;
    let remainder = &value[start + prefix.len()..];
    let end = remainder.find(suffix)?;
    Some(remainder[..end].trim())
}

fn top_level_workflow_refs() -> [&'static str; 4] {
    [
        "workspace.web.auth-openai-com.codex-rotate-account-flow",
        "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
        "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
        "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
    ]
}

fn normalize_email(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn difference(current: &HashSet<String>, previous: &HashSet<String>) -> Vec<String> {
    let mut values = current.difference(previous).cloned().collect::<Vec<_>>();
    values.sort();
    values
}

fn track_key(track: Track) -> &'static str {
    match track {
        Track::NonDevice => "non_device",
        Track::DeviceAuth => "device_auth",
    }
}

fn mode_key(mode: Mode) -> &'static str {
    match mode {
        Mode::All => "all",
        Mode::NonDevice => "non_device",
        Mode::DeviceAuth => "device_auth",
    }
}

fn operation_key(operation: BenchmarkOperation) -> &'static str {
    match operation {
        BenchmarkOperation::Create => "create",
        BenchmarkOperation::Relogin => "relogin",
    }
}

fn iso_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn repo_root() -> Result<PathBuf> {
    Ok(resolve_paths()?.repo_root)
}

fn resolve_home_override(env_name: &str, default_suffix: &str) -> Result<PathBuf> {
    if let Some(value) = env::var_os(env_name) {
        return Ok(PathBuf::from(value));
    }
    let home = env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(PathBuf::from(home).join(default_suffix))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn env_mutex() -> &'static Mutex<()> {
        static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
        ENV_MUTEX.get_or_init(|| Mutex::new(()))
    }

    fn fresh_temp_path(name: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "codex-rotate-benchmark-tests-{name}-{}-{unique}",
            std::process::id()
        ))
    }

    fn make_record(
        workflow_id: &str,
        track: Track,
        success: bool,
        latency_ms: u128,
        selection_eligible: bool,
    ) -> BenchmarkRecord {
        BenchmarkRecord {
            competitor: workflow_id.to_string(),
            workflow_id: workflow_id.to_string(),
            workflow_ref: match workflow_id {
                "original" => "workspace.web.auth-openai-com.codex-rotate-account-flow",
                "stepwise" => "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
                "minimal" => "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
                _ => "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
            }
            .to_string(),
            workflow_file: format!("{workflow_id}.yaml"),
            track,
            operation: BenchmarkOperation::Create,
            task: match track {
                Track::NonDevice => "openai-account-create-non-device".to_string(),
                Track::DeviceAuth => "openai-account-create-device-auth".to_string(),
            },
            run_label: format!("{workflow_id}-run"),
            cold: true,
            success,
            latency_ms,
            exit_status: Some(if success { 0 } else { 1 }),
            failure_mode: (!success).then(|| "unknown".to_string()),
            created_emails: Vec::new(),
            new_pending_emails: Vec::new(),
            intended_target_email: None,
            displayed_created_label: None,
            displayed_created_email: None,
            environment_blocked: false,
            used_top_level_fallback: false,
            top_level_fallback_workflows: Vec::new(),
            selection_eligible,
            selection_invalid_reasons: if selection_eligible {
                Vec::new()
            } else {
                vec!["invalid".to_string()]
            },
            auth_before: None,
            auth_after: None,
            auth_restored: true,
            template: "dev3astronlab+{n}@gmail.com".to_string(),
            notes: None,
            stdout_tail: None,
            stderr_tail: None,
            measured_at: iso_now(),
        }
    }

    #[test]
    fn parse_args_supports_mode_runs_profile_and_overrides() {
        let options = parse_args(&[
            "--device-auth".to_string(),
            "--runs=3".to_string(),
            "--profile".to_string(),
            "dev-2".to_string(),
            "--template".to_string(),
            "device_auth=qa.{n}@astronlab.com".to_string(),
        ])
        .expect("parse args");

        assert!(matches!(options.mode, Mode::DeviceAuth));
        assert!(matches!(options.operation, BenchmarkOperation::Create));
        assert_eq!(options.runs, 3);
        assert_eq!(options.profile_name, "dev-2");
        assert_eq!(
            options
                .template_override
                .get("device_auth")
                .map(String::as_str),
            Some("qa.{n}@astronlab.com")
        );
    }

    #[test]
    fn parse_args_supports_relogin_operation_and_selector() {
        let options = parse_args(&[
            "--relogin=dev3astronlab+2@gmail.com".to_string(),
            "--runs".to_string(),
            "2".to_string(),
        ])
        .expect("parse args");

        assert!(matches!(options.operation, BenchmarkOperation::Relogin));
        assert_eq!(
            options.relogin_selector_override.as_deref(),
            Some("dev3astronlab+2@gmail.com")
        );
        assert_eq!(options.runs, 2);
    }

    #[test]
    fn extract_template_suffix_reads_matching_values() {
        assert_eq!(
            extract_template_suffix("dev.42@astronlab.com", "dev.{n}@astronlab.com"),
            Some(42)
        );
        assert_eq!(
            extract_template_suffix("other@astronlab.com", "dev.{n}@astronlab.com"),
            None
        );
    }

    #[test]
    fn resolve_benchmark_template_prefers_store_default_without_override() {
        let options = Options {
            mode: Mode::All,
            runs: 1,
            profile_name: "dev-1".to_string(),
            operation: BenchmarkOperation::Create,
            relogin_selector_override: None,
            template_override: HashMap::new(),
        };
        let workflow_root = Path::new("/tmp/auth-workflows");
        let candidate = benchmark_candidates(workflow_root)
            .into_iter()
            .find(|entry| entry.id == "original")
            .expect("candidate");
        let snapshot = Snapshot {
            auth_email: None,
            default_create_template: Some("dev3astronlab+{n}@gmail.com".to_string()),
            account_emails: HashSet::new(),
            pending_emails: HashSet::new(),
        };

        let template =
            resolve_benchmark_template(&options, &candidate, &snapshot).expect("base email");
        assert_eq!(template, "dev3astronlab+{n}@gmail.com");
    }

    #[test]
    fn benchmark_candidate_leaves_real_created_gmail_account_in_shared_state() {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let tempdir = fresh_temp_path("persist");
        fs::create_dir_all(&tempdir).expect("create tempdir");
        let rotate_home = tempdir.join("rotate-home");
        let codex_home = tempdir.join("codex-home");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        fs::create_dir_all(&codex_home).expect("create codex home");
        fs::write(rotate_home.join("accounts.json"), "{\"accounts\":[]}\n").expect("write state");
        fs::write(codex_home.join("auth.json"), "{\"tokens\":{}}\n").expect("write auth");

        let cli_binary = tempdir.join("fake-cli.sh");
        fs::write(
            &cli_binary,
            concat!(
                "#!/bin/sh\n",
                "mkdir -p \"$CODEX_ROTATE_HOME\"\n",
                "cat > \"$CODEX_ROTATE_HOME/accounts.json\" <<'EOF'\n",
                "{\"accounts\":[{\"email\":\"dev3astronlab+1@gmail.com\"}]}\n",
                "EOF\n",
                "echo 'Creating dev3astronlab+1@gmail.com via dev-1 from dev3astronlab+{n}@gmail.com.' 1>&2\n",
                "echo 'Managed login finished for dev3astronlab+1@gmail.com. Finalizing.' 1>&2\n",
                "echo 'Adding dev3astronlab+1@gmail.com to the account pool.' 1>&2\n",
                "echo 'Created dev3astronlab+1@gmail.com_free with usable quota.' 1>&2\n",
                "echo '\\033[32mOK\\033[0m Created dev3astronlab+1@gmail.com_free via \"dev-1\" from dev3astronlab+{n}@gmail.com.'\n",
            ),
        )
        .expect("write fake cli");
        #[cfg(unix)]
        fs::set_permissions(&cli_binary, fs::Permissions::from_mode(0o755))
            .expect("chmod fake cli");

        let previous_rotate_home = env::var_os("CODEX_ROTATE_HOME");
        let previous_codex_home = env::var_os("CODEX_HOME");
        unsafe {
            env::set_var("CODEX_ROTATE_HOME", &rotate_home);
            env::set_var("CODEX_HOME", &codex_home);
        }

        let candidate = BenchmarkCandidate {
            id: "original",
            track: Track::NonDevice,
            file_path: tempdir.join("flow.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow",
        };
        let options = Options {
            mode: Mode::All,
            runs: 1,
            profile_name: "dev-1".to_string(),
            operation: BenchmarkOperation::Create,
            relogin_selector_override: None,
            template_override: HashMap::new(),
        };
        let mut reserved_relogin_selectors = HashSet::new();

        let result = benchmark_candidate(
            &candidate,
            1,
            &options,
            "dev3astronlab+{n}@gmail.com",
            &tempdir,
            &cli_binary,
            &rotate_home.join("accounts.json"),
            &codex_home,
            &mut reserved_relogin_selectors,
        );

        match previous_codex_home {
            Some(value) => unsafe { env::set_var("CODEX_HOME", value) },
            None => unsafe { env::remove_var("CODEX_HOME") },
        }
        match previous_rotate_home {
            Some(value) => unsafe { env::set_var("CODEX_ROTATE_HOME", value) },
            None => unsafe { env::remove_var("CODEX_ROTATE_HOME") },
        }

        let record = result.expect("benchmark candidate should succeed");
        assert_eq!(record.created_emails, vec!["dev3astronlab+1@gmail.com"]);

        let state = read_rotate_state(&rotate_home.join("accounts.json"));
        let account_emails = state
            .get("accounts")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| normalize_email(entry.get("email").and_then(Value::as_str)))
            .collect::<Vec<_>>();
        assert_eq!(account_emails, vec!["dev3astronlab+1@gmail.com"]);

        fs::remove_dir_all(&tempdir).ok();
    }

    #[test]
    fn parse_profile_idle_state_reports_idle_when_queue_is_empty() {
        let state = parse_profile_idle_state(
            br#"{
                "abiVersion":"1.0.0",
                "command":"profiles.inspect",
                "ok":true,
                "result":{
                    "ok":true,
                    "profileName":"dev-1",
                    "requestQueue":{
                        "active":null,
                        "queuedCount":0
                    }
                }
            }"#,
        )
        .expect("idle state");

        assert_eq!(
            state,
            ProfileIdleState {
                idle: true,
                reason: None,
            }
        );
    }

    #[test]
    fn parse_profile_idle_state_reports_busy_active_request() {
        let state = parse_profile_idle_state(
            br#"{
                "abiVersion":"1.0.0",
                "command":"profiles.inspect",
                "ok":true,
                "result":{
                    "ok":true,
                    "profileName":"dev-1",
                    "requestQueue":{
                        "active":{
                            "id":23,
                            "method":"run",
                            "workflowRef":"workspace.web.auth-openai-com.codex-rotate-account-flow-main"
                        },
                        "queuedCount":0
                    }
                }
            }"#,
        )
        .expect("busy state");

        assert!(!state.idle);
        assert_eq!(
            state.reason.as_deref(),
            Some(
                "active request present for profile dev-1: id=23 method=run workflowRef=workspace.web.auth-openai-com.codex-rotate-account-flow-main"
            )
        );
    }

    #[test]
    fn parse_profile_idle_state_reports_busy_queue_without_active_request() {
        let state = parse_profile_idle_state(
            br#"{
                "abiVersion":"1.0.0",
                "command":"profiles.inspect",
                "ok":true,
                "result":{
                    "ok":true,
                    "profileName":"dev-1",
                    "requestQueue":{
                        "active":null,
                        "queuedCount":2
                    }
                }
            }"#,
        )
        .expect("queued state");

        assert!(!state.idle);
        assert_eq!(
            state.reason.as_deref(),
            Some("queuedCount=2 for profile dev-1")
        );
    }

    #[test]
    fn classify_selection_invalid_reasons_flags_email_mismatch_and_top_level_fallback() {
        let reasons = classify_selection_invalid_reasons(
            BenchmarkOperation::Create,
            true,
            Some("devbench.8@astronlab.com"),
            &[String::from("devbench.8@astronlab.com")],
            Some("1.dev.astronlab@gmail.com_free"),
            Some("1.dev.astronlab@gmail.com"),
            &[String::from(
                "workspace.web.auth-openai-com.codex-rotate-account-flow",
            )],
        );

        assert!(reasons
            .iter()
            .any(|reason| reason.starts_with("target_display_mismatch:")));
        assert!(reasons
            .iter()
            .any(|reason| reason.starts_with("pool_display_mismatch:")));
        assert!(reasons
            .iter()
            .any(|reason| reason.starts_with("top_level_fallback:")));
    }

    #[test]
    fn classify_selection_invalid_reasons_allows_relogin_without_created_emails() {
        let reasons = classify_selection_invalid_reasons(
            BenchmarkOperation::Relogin,
            true,
            Some("dev3astronlab+1@gmail.com"),
            &[],
            Some("dev3astronlab+1@gmail.com_free"),
            Some("dev3astronlab+1@gmail.com"),
            &[],
        );

        assert!(reasons.is_empty());
    }

    #[test]
    fn classify_selection_invalid_reasons_allows_environment_blocked_relogin_without_displayed_create_fields(
    ) {
        let reasons = classify_selection_invalid_reasons(
            BenchmarkOperation::Relogin,
            true,
            Some("dev3astronlab+5@gmail.com"),
            &[],
            None,
            None,
            &[],
        );

        assert!(reasons.is_empty());
    }

    #[test]
    fn resolve_benchmark_relogin_selector_picks_first_matching_family_account() {
        let options = Options {
            mode: Mode::All,
            runs: 1,
            profile_name: "dev-1".to_string(),
            operation: BenchmarkOperation::Relogin,
            relogin_selector_override: None,
            template_override: HashMap::new(),
        };
        let snapshot = Snapshot {
            auth_email: None,
            default_create_template: Some("dev3astronlab+{n}@gmail.com".to_string()),
            account_emails: HashSet::from([
                "other@gmail.com".to_string(),
                "dev3astronlab+2@gmail.com".to_string(),
                "dev3astronlab+1@gmail.com".to_string(),
            ]),
            pending_emails: HashSet::new(),
        };

        let reserved = HashSet::new();
        let selector = resolve_benchmark_relogin_selector(
            &options,
            &snapshot,
            "dev3astronlab+{n}@gmail.com",
            &reserved,
        )
        .expect("selector");
        assert_eq!(selector, "dev3astronlab+1@gmail.com");
    }

    #[test]
    fn resolve_benchmark_relogin_selector_allows_pending_family_accounts() {
        let options = Options {
            mode: Mode::All,
            runs: 1,
            profile_name: "dev-1".to_string(),
            operation: BenchmarkOperation::Relogin,
            relogin_selector_override: None,
            template_override: HashMap::new(),
        };
        let snapshot = Snapshot {
            auth_email: None,
            default_create_template: Some("dev3astronlab+{n}@gmail.com".to_string()),
            account_emails: HashSet::new(),
            pending_emails: HashSet::from(["dev3astronlab+5@gmail.com".to_string()]),
        };

        let reserved = HashSet::new();
        let selector = resolve_benchmark_relogin_selector(
            &options,
            &snapshot,
            "dev3astronlab+{n}@gmail.com",
            &reserved,
        )
        .expect("selector");
        assert_eq!(selector, "dev3astronlab+5@gmail.com");
    }

    #[test]
    fn resolve_benchmark_relogin_selector_prefers_unused_family_account_before_reusing_one() {
        let options = Options {
            mode: Mode::All,
            runs: 1,
            profile_name: "dev-1".to_string(),
            operation: BenchmarkOperation::Relogin,
            relogin_selector_override: None,
            template_override: HashMap::new(),
        };
        let snapshot = Snapshot {
            auth_email: None,
            default_create_template: Some("dev3astronlab+{n}@gmail.com".to_string()),
            account_emails: HashSet::from([
                "dev3astronlab+1@gmail.com".to_string(),
                "dev3astronlab+2@gmail.com".to_string(),
                "dev3astronlab+3@gmail.com".to_string(),
            ]),
            pending_emails: HashSet::new(),
        };
        let reserved = HashSet::from(["dev3astronlab+1@gmail.com".to_string()]);

        let selector = resolve_benchmark_relogin_selector(
            &options,
            &snapshot,
            "dev3astronlab+{n}@gmail.com",
            &reserved,
        )
        .expect("selector");

        assert_eq!(selector, "dev3astronlab+2@gmail.com");
    }

    #[test]
    fn add_phone_environment_blocker_is_treated_as_bounded_non_red_signal() {
        assert!(is_add_phone_environment_blocker(
            "The workflow requested skipping dev3astronlab+1@gmail.com after exhausting final add-phone retries (https://auth.openai.com/add-phone).",
            Some(1)
        ));
        assert!(!is_add_phone_environment_blocker(
            "Created dev3astronlab+1@gmail.com_free with usable quota.",
            Some(0)
        ));
    }

    #[test]
    fn detect_top_level_fallback_workflows_ignores_prefix_matches() {
        let output =
            "workflow_ref=workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise";
        let fallbacks = detect_top_level_fallback_workflows(
            output,
            "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
        );
        assert!(fallbacks.is_empty());
    }

    #[test]
    fn build_selection_ignores_successful_runs_that_are_not_selection_eligible() {
        let summary = build_summary(&[
            make_record("original", Track::NonDevice, true, 40, false),
            make_record("stepwise", Track::NonDevice, true, 55, true),
        ]);

        let winner = build_selection(&summary)
            .selected_non_device
            .expect("winner");
        assert_eq!(winner.workflow_id, "stepwise");
    }

    #[test]
    fn benchmark_create_is_allowed_from_any_worktree() {
        let repo_root = repo_root().expect("repo root");
        ensure_benchmark_worktree_operation_allowed(&repo_root, BenchmarkOperation::Create)
            .expect("benchmark create should be allowed");
    }

    #[test]
    fn benchmark_relogin_is_allowed_from_any_worktree() {
        let repo_root = repo_root().expect("repo root");

        ensure_benchmark_worktree_operation_allowed(&repo_root, BenchmarkOperation::Relogin)
            .expect("relogin benchmark should be allowed");
    }
}
