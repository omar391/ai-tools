use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Map, Value};

const DEFAULT_PROFILE: &str = "dev-1";
const DEFAULT_RUNS: u32 = 1;

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

#[derive(Clone, Debug)]
struct BenchmarkCandidate {
    id: &'static str,
    track: Track,
    file_path: PathBuf,
    workflow_ref: &'static str,
    base_email: &'static str,
}

#[derive(Clone, Debug)]
struct Options {
    mode: Mode,
    runs: u32,
    profile_name: String,
    base_email_override: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct Snapshot {
    auth_email: Option<String>,
    account_emails: HashSet<String>,
    pending_emails: HashSet<String>,
}

#[derive(Clone, Debug)]
enum RotateStateSnapshot {
    Missing,
    Present(String),
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
    task: String,
    run_label: String,
    cold: bool,
    success: bool,
    latency_ms: u128,
    exit_status: Option<i32>,
    failure_mode: Option<String>,
    created_emails: Vec<String>,
    new_pending_emails: Vec<String>,
    auth_before: Option<String>,
    auth_after: Option<String>,
    auth_restored: bool,
    base_email: String,
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

fn main() {
    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let options = parse_args(&env::args().skip(1).collect::<Vec<_>>())?;
    let repo_root = repo_root()?;
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
    for candidate in &candidates {
        for iteration in 1..=options.runs {
            let base_email = options
                .base_email_override
                .get(candidate.id)
                .or_else(|| options.base_email_override.get(track_key(candidate.track)))
                .cloned()
                .unwrap_or_else(|| candidate.base_email.to_string());
            let record = benchmark_candidate(
                candidate,
                iteration,
                &options.profile_name,
                &base_email,
                &repo_root,
                &cli_binary,
                &rotate_state_path,
                &codex_home,
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

fn parse_args(args: &[String]) -> Result<Options> {
    let mut mode = Mode::All;
    let mut runs = DEFAULT_RUNS;
    let mut profile_name = DEFAULT_PROFILE.to_string();
    let mut base_email_override = HashMap::new();

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--non-device" => mode = Mode::NonDevice,
            "--device-auth" => mode = Mode::DeviceAuth,
            "--all" => mode = Mode::All,
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
            "--base-email" => {
                index += 1;
                let value = args
                    .get(index)
                    .cloned()
                    .ok_or_else(|| anyhow!("--base-email requires workflow=value"))?;
                assign_base_email_override(&mut base_email_override, &value)?;
            }
            _ if arg.starts_with("--base-email=") => {
                assign_base_email_override(
                    &mut base_email_override,
                    &arg["--base-email=".len()..],
                )?;
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
        base_email_override,
    })
}

fn assign_base_email_override(overrides: &mut HashMap<String, String>, raw: &str) -> Result<()> {
    if let Some((key, value)) = raw.split_once('=') {
        let key = key.trim();
        let value = value.trim();
        if key.is_empty() || value.is_empty() {
            return Err(anyhow!("Invalid --base-email override: {raw}"));
        }
        overrides.insert(key.to_string(), value.to_string());
    } else {
        overrides.insert("non_device".to_string(), raw.to_string());
        overrides.insert("device_auth".to_string(), raw.to_string());
    }
    Ok(())
}

fn benchmark_candidate(
    candidate: &BenchmarkCandidate,
    iteration: u32,
    profile_name: &str,
    base_email: &str,
    repo_root: &Path,
    cli_binary: &Path,
    rotate_state_path: &Path,
    codex_home: &Path,
) -> Result<BenchmarkRecord> {
    let before = read_snapshot(rotate_state_path, codex_home);
    let rotate_state_snapshot = snapshot_rotate_state_file(rotate_state_path)?;
    let run_label = format!("{}-{}", candidate.id, iso_now());
    let command = vec![
        cli_binary.to_path_buf(),
        PathBuf::from("internal"),
        PathBuf::from("create"),
        PathBuf::from("--force"),
        PathBuf::from("--restore-auth"),
        PathBuf::from("--profile"),
        PathBuf::from(profile_name),
        PathBuf::from("--base-email"),
        PathBuf::from(base_email),
    ];
    let mut command_env = env::vars_os().collect::<HashMap<_, _>>();
    command_env.insert(
        "CODEX_ROTATE_ACCOUNT_FLOW_FILE".into(),
        candidate.file_path.clone().into_os_string(),
    );

    println!(
        "[benchmark] starting {} iteration={} flow={} base_email={}",
        candidate.id,
        iteration,
        candidate.file_path.display(),
        base_email
    );

    let measured_at = iso_now();
    let started = Instant::now();
    let run_result = run_command_with_capture(&command, repo_root, &command_env, candidate.id);
    let after = read_snapshot(rotate_state_path, codex_home);
    restore_rotate_state_file(
        rotate_state_path,
        rotate_state_snapshot,
        profile_name,
        base_email,
        &collect_reserved_emails(&before, &after, base_email),
    )?;
    let result = run_result?;
    let latency_ms = started.elapsed().as_millis();
    let created_emails = difference(&after.account_emails, &before.account_emails);
    let new_pending_emails = difference(&after.pending_emails, &before.pending_emails);
    let success = result.exit_status == Some(0);
    let combined_output = if result.stderr.is_empty() {
        result.stdout.clone()
    } else if result.stdout.is_empty() {
        result.stderr.clone()
    } else {
        format!("{}\n{}", result.stdout, result.stderr)
    };

    Ok(BenchmarkRecord {
        competitor: candidate.id.to_string(),
        workflow_id: candidate.id.to_string(),
        workflow_ref: candidate.workflow_ref.to_string(),
        workflow_file: candidate.file_path.display().to_string(),
        track: candidate.track,
        task: match candidate.track {
            Track::DeviceAuth => "openai-account-create-device-auth".to_string(),
            Track::NonDevice => "openai-account-create-non-device".to_string(),
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
        auth_before: before.auth_email.clone(),
        auth_after: after.auth_email.clone(),
        auth_restored: before.auth_email == after.auth_email,
        base_email: base_email.to_string(),
        notes: build_notes(
            success,
            &created_emails,
            &new_pending_emails,
            &before,
            &after,
        ),
        stdout_tail: tail_text(&result.stdout, 12),
        stderr_tail: tail_text(&result.stderr, 12),
        measured_at,
    })
}

fn resolve_cli_binary(repo_root: &Path) -> Result<PathBuf> {
    let binary_name = if cfg!(windows) {
        "codex-rotate.exe"
    } else {
        "codex-rotate"
    };
    let mut candidates = Vec::new();
    if let Some(value) = env::var_os("CODEX_ROTATE_BENCHMARK_CLI_BIN") {
        candidates.push(PathBuf::from(value));
    }
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

fn snapshot_rotate_state_file(path: &Path) -> Result<RotateStateSnapshot> {
    Ok(if path.exists() {
        RotateStateSnapshot::Present(fs::read_to_string(path)?)
    } else {
        RotateStateSnapshot::Missing
    })
}

fn restore_rotate_state_file(
    path: &Path,
    snapshot: RotateStateSnapshot,
    profile_name: &str,
    base_email: &str,
    reserved_emails: &[String],
) -> Result<()> {
    let mut restored_state = match snapshot {
        RotateStateSnapshot::Present(content) => {
            fs::write(path, &content)?;
            serde_json::from_str::<Value>(&content).unwrap_or_else(|_| json!({}))
        }
        RotateStateSnapshot::Missing => {
            if path.exists() {
                fs::remove_file(path).ok();
            }
            json!({})
        }
    };

    if reserved_emails.is_empty() {
        return Ok(());
    }
    let Some(normalized_profile_name) = normalize_text(profile_name) else {
        return Ok(());
    };
    let Some(normalized_base_email) = normalize_base_email(base_email) else {
        return Ok(());
    };
    if !normalized_base_email.contains("{n}") {
        return Ok(());
    }

    let mut highest_suffix = None::<u32>;
    let mut highest_email = None::<String>;
    for email in reserved_emails {
        if let Some(suffix) = extract_template_suffix(email, &normalized_base_email) {
            if highest_suffix.map(|value| suffix > value).unwrap_or(true) {
                highest_suffix = Some(suffix);
                highest_email = Some(email.clone());
            }
        }
    }
    let Some(highest_suffix) = highest_suffix else {
        return Ok(());
    };
    let Some(highest_email) = highest_email else {
        return Ok(());
    };

    if !restored_state.is_object() {
        restored_state = json!({});
    }
    let now = iso_now();
    let family_key = format!("{normalized_profile_name}::{normalized_base_email}");
    let object = restored_state.as_object_mut().expect("state object");
    let families = object
        .entry("families")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .expect("families object");
    let (created_at, existing_next) = {
        let existing = families.get(&family_key).and_then(Value::as_object);
        let created_at = existing
            .and_then(|value| value.get("created_at"))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .unwrap_or(&now)
            .to_string();
        let existing_next = existing
            .and_then(|value| value.get("next_suffix"))
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32;
        (created_at, existing_next)
    };
    families.insert(
        family_key,
        json!({
            "profile_name": normalized_profile_name,
            "base_email": normalized_base_email,
            "next_suffix": existing_next.max(highest_suffix.saturating_add(1)),
            "created_at": created_at,
            "updated_at": now,
            "last_created_email": highest_email,
        }),
    );
    write_json(path, &restored_state)
}

fn collect_reserved_emails(before: &Snapshot, after: &Snapshot, base_email: &str) -> Vec<String> {
    let mut reserved = HashSet::new();
    for candidate in after
        .account_emails
        .iter()
        .chain(after.pending_emails.iter())
        .chain(after.auth_email.iter())
    {
        let Some(email) = normalize_email(Some(candidate.as_str())) else {
            continue;
        };
        if before.account_emails.contains(&email) || before.pending_emails.contains(&email) {
            continue;
        }
        if extract_template_suffix(&email, base_email).is_some() {
            reserved.insert(email);
        }
    }
    let mut values = reserved.into_iter().collect::<Vec<_>>();
    values.sort();
    values
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
    success: bool,
    created_emails: &[String],
    new_pending_emails: &[String],
    before: &Snapshot,
    after: &Snapshot,
) -> Option<String> {
    let mut parts = Vec::new();
    if success && created_emails.is_empty() {
        parts.push("create exited successfully but no new pooled account was detected".to_string());
    }
    if !success && !new_pending_emails.is_empty() {
        parts.push(format!(
            "left pending reservation(s): {}",
            new_pending_emails.join(", ")
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
            .success_rate
            .partial_cmp(&left.success_rate)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.median_latency_ms.cmp(&right.median_latency_ms))
    });
    let winner = candidates
        .into_iter()
        .find(|group| group.success_rate > 0.0)?;
    Some(SelectionEntry {
        workflow_id: winner.workflow_id,
        workflow_ref: winner.workflow_ref,
        workflow_file: winner.workflow_file,
        median_latency_ms: winner.median_latency_ms,
        success_rate: winner.success_rate,
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
        "| - | - | - | - | - | - |".to_string()
    } else {
        records
            .iter()
            .map(|record| {
                format!(
                    "| {} | {} | {} | {} | {} | {} |",
                    record.workflow_id,
                    track_key(record.track),
                    if record.success { "yes" } else { "no" },
                    record.latency_ms,
                    if record.created_emails.is_empty() {
                        "-".to_string()
                    } else {
                        record.created_emails.join(", ")
                    },
                    record.failure_mode.as_deref().unwrap_or("-"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let group_rows = if summary.groups.is_empty() {
        "| - | - | - | - | - | - |".to_string()
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
                    "| {} | {} | {}/{} | {} | {} | {} |",
                    group.workflow_id,
                    track_key(group.track),
                    group.successes,
                    group.runs,
                    group.success_rate,
                    group
                        .median_latency_ms
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
         - runs per workflow: `{}`\n\
         - managed profile: `{}`\n\n\
         ## Raw Runs\n\n\
         | workflow | track | success | latency_ms | created emails | failure |\n\
         | --- | --- | --- | ---: | --- | --- |\n\
         {rows}\n\n\
         ## Summary\n\n\
         | workflow | track | successes | success rate | median latency_ms | failure modes |\n\
         | --- | --- | --- | ---: | ---: | --- |\n\
         {group_rows}\n\n\
         ## Selection\n\n\
         - selected non-device: {}\n\
         - selected device-auth: {}\n",
        mode_key(options.mode),
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
            base_email: "dev.{n}@astronlab.com",
        },
        BenchmarkCandidate {
            id: "stepwise",
            track: Track::NonDevice,
            file_path: workflow_root.join("codex-rotate-account-flow-stepwise.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-stepwise",
            base_email: "dev.{n}@astronlab.com",
        },
        BenchmarkCandidate {
            id: "minimal",
            track: Track::NonDevice,
            file_path: workflow_root.join("codex-rotate-account-flow-minimal.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-minimal",
            base_email: "dev.{n}@astronlab.com",
        },
        BenchmarkCandidate {
            id: "device-auth",
            track: Track::DeviceAuth,
            file_path: workflow_root.join("codex-rotate-account-flow-device-auth.yaml"),
            workflow_ref: "workspace.web.auth-openai-com.codex-rotate-account-flow-device-auth",
            base_email: "dev.{n}@astronlab.com",
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

fn normalize_base_email(value: impl AsRef<str>) -> Option<String> {
    let trimmed = value.as_ref().trim().to_lowercase();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.replace("{N}", "{n}"))
    }
}

fn extract_template_suffix(email: &str, base_email: &str) -> Option<u32> {
    let normalized_email = normalize_email(Some(email))?;
    let normalized_base_email = normalize_base_email(base_email)?;
    let marker_index = normalized_base_email.find("{n}")?;
    let prefix = &normalized_base_email[..marker_index];
    let suffix = &normalized_base_email[marker_index + 3..];
    if !normalized_email.starts_with(prefix)
        || !normalized_email.ends_with(suffix)
        || normalized_email.len() <= prefix.len() + suffix.len()
    {
        return None;
    }
    let numeric = &normalized_email[prefix.len()..normalized_email.len() - suffix.len()];
    numeric.parse::<u32>().ok()
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

fn iso_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn repo_root() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..")
        .canonicalize()
        .context("resolve repo root")
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

    #[test]
    fn parse_args_supports_mode_runs_profile_and_overrides() {
        let options = parse_args(&[
            "--device-auth".to_string(),
            "--runs=3".to_string(),
            "--profile".to_string(),
            "dev-2".to_string(),
            "--base-email".to_string(),
            "device_auth=qa.{n}@astronlab.com".to_string(),
        ])
        .expect("parse args");

        assert!(matches!(options.mode, Mode::DeviceAuth));
        assert_eq!(options.runs, 3);
        assert_eq!(options.profile_name, "dev-2");
        assert_eq!(
            options
                .base_email_override
                .get("device_auth")
                .map(String::as_str),
            Some("qa.{n}@astronlab.com")
        );
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
}
