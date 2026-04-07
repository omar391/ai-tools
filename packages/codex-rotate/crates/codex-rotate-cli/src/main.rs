mod managed_login;

use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::pool::{
    cmd_add, cmd_list_stream, cmd_next_with_progress, cmd_prev, cmd_remove, cmd_status_stream,
};
use codex_rotate_core::workflow::{
    cmd_create_with_progress, cmd_relogin_with_progress, CreateCommandOptions,
    CreateCommandSource, ReloginOptions,
};
use codex_rotate_runtime::daemon::run_daemon_forever;
use codex_rotate_runtime::dev_refresh::{
    clear_tray_service_registration, detect_local_tray_build, launch_tray_process,
    local_tray_sources_newer_than_binary, rebuild_local_tray, stop_running_trays,
};
use codex_rotate_runtime::ipc::{
    daemon_is_reachable, invoke, subscribe, CreateInvocation, InvokeAction, ReloginInvocation,
};
use managed_login::run_managed_login;

const BOLD: &str = "\x1b[1m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";
fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = args.first().map(String::as_str);
    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    if let Some(output) = try_run_via_daemon(command, &args[1..])? {
        write_output(&mut stdout, &output)?;
        return Ok(());
    }

    match command {
        None | Some("help") | Some("--help") | Some("-h") => {
            write_output(&mut stdout, &help_text())?
        }
        Some("daemon") => run_daemon_command(&mut stdout, &args[1..])?,
        Some("internal") => run_internal_command(&args[1..])?,
        Some("tray") => run_tray_command(&mut stdout, &args[1..])?,
        Some("add") => write_output(
            &mut stdout,
            &cmd_add(parse_add_alias(&args[1..])?.as_deref())?,
        )?,
        Some("create") | Some("new") => {
            write_output(
                &mut stdout,
                &cmd_create_with_progress(
                    parse_create_options(&args[1..])?,
                    cli_progress_callback(),
                )?,
            )?
        }
        Some("next") | Some("n") => {
            write_output(&mut stdout, &cmd_next_with_progress(cli_progress_callback())?)?
        }
        Some("prev") | Some("p") => write_output(&mut stdout, &cmd_prev()?)?,
        Some("list") | Some("ls") => cmd_list_stream(&mut stdout)?,
        Some("status") | Some("s") => cmd_status_stream(&mut stdout)?,
        Some("relogin") | Some("reauth") => {
            let (selector, options) = parse_relogin_options(&args[1..])?;
            write_output(
                &mut stdout,
                &cmd_relogin_with_progress(&selector, options, cli_progress_callback())?,
            )?
        }
        Some("remove") | Some("rm") => write_output(
            &mut stdout,
            &cmd_remove(parse_remove_selector(&args[1..])?)?,
        )?,
        Some(other) => {
            return Err(anyhow!(
                "Unknown command: \"{other}\". Run \"codex-rotate help\" for usage."
            ))
        }
    }
    Ok(())
}

fn try_run_via_daemon(command: Option<&str>, args: &[String]) -> Result<Option<String>> {
    if !daemon_is_reachable() {
        return Ok(None);
    }

    let action = match command {
        Some("add") => Some(InvokeAction::Add {
            alias: parse_add_alias(args)?,
        }),
        Some("create") | Some("new") => Some(InvokeAction::Create {
            options: parse_create_invocation(args)?,
        }),
        Some("next") | Some("n") => Some(InvokeAction::Next),
        Some("prev") | Some("p") => Some(InvokeAction::Prev),
        Some("relogin") | Some("reauth") => Some(InvokeAction::Relogin {
            options: parse_relogin_invocation(args)?,
        }),
        Some("remove") | Some("rm") => Some(InvokeAction::Remove {
            selector: parse_remove_selector(args)?.to_string(),
        }),
        Some("list")
        | Some("ls")
        | Some("status")
        | Some("s")
        | Some("daemon")
        | Some("tray")
        | None
        | Some("help")
        | Some("--help")
        | Some("-h") => None,
        Some(_) => None,
    };

    Ok(match action {
        Some(action) => {
            if command_streams_progress(command) {
                spawn_daemon_progress_printer();
            }
            Some(invoke(action)?)
        }
        None => None,
    })
}

fn cli_progress_callback() -> Option<Arc<dyn Fn(String) + Send + Sync>> {
    Some(Arc::new(|message| eprintln!("{message}")))
}

fn spawn_daemon_progress_printer() {
    thread::spawn(|| {
        let Ok(mut subscription) = subscribe() else {
            return;
        };
        if subscription.recv().is_err() {
            return;
        }
        while let Ok(snapshot) = subscription.recv() {
            let Some(message) = snapshot.last_message else {
                continue;
            };
            eprintln!("{message}");
        }
    });
}

fn command_streams_progress(command: Option<&str>) -> bool {
    matches!(
        command,
        Some("create")
            | Some("new")
            | Some("next")
            | Some("n")
            | Some("relogin")
            | Some("reauth")
    )
}

fn run_daemon_command(writer: &mut dyn Write, args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("run") | None => {
            if daemon_is_reachable() {
                return write_output(writer, "Codex Rotate daemon is already running.");
            }
            run_daemon_forever()
        }
        Some(other) => Err(anyhow!(
            "Unknown daemon command: \"{other}\". Run \"codex-rotate help\" for usage."
        )),
    }
}

fn run_internal_command(args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("managed-login") => run_managed_login(&args[1..]),
        Some(other) => Err(anyhow!("Unknown internal command: \"{other}\".")),
        None => Err(anyhow!("Usage: codex-rotate internal <subcommand>")),
    }
}

fn run_tray_command(writer: &mut dyn Write, args: &[String]) -> Result<()> {
    let command = args.first().map(String::as_str).unwrap_or("open");
    match command {
        "open" => write_output(writer, &tray_open_message()?),
        "status" => {
            let tray_running = tray_is_running()?;
            if tray_running && daemon_is_reachable() {
                write_output(writer, "Codex Rotate tray is running.")
            } else if tray_running {
                Err(anyhow!(
                    "Codex Rotate tray is running but the daemon is unavailable."
                ))
            } else {
                Err(anyhow!("Codex Rotate tray is not running."))
            }
        }
        "quit" => write_output(writer, &tray_quit_message()?),
        "restart" => {
            let _ = tray_quit_message()?;
            write_output(writer, &tray_open_message()?)
        }
        "path" => write_output(writer, &resolve_tray_binary()?.display().to_string()),
        "help" | "--help" | "-h" => write_output(
            writer,
            "Usage: codex-rotate tray [open|status|quit|restart|path]",
        ),
        other => Err(anyhow!(
            "Unknown tray command: \"{other}\". Run \"codex-rotate tray help\" for usage."
        )),
    }
}

fn tray_open_message() -> Result<String> {
    let tray_binary = resolve_tray_binary()?;
    refresh_local_tray_if_needed(&tray_binary)?;
    if tray_is_running_with_path(&tray_binary)? {
        if daemon_is_reachable() {
            return Ok("Codex Rotate tray is already running.".to_string());
        }
        stop_running_trays(&tray_binary)?;
        clear_tray_service_registration();
        if !wait_for_tray_state(&tray_binary, false) {
            return Err(anyhow!(
                "Timed out waiting for the unhealthy Codex Rotate tray to stop."
            ));
        }
    }

    launch_tray_binary(&tray_binary)?;

    if wait_for_tray_state(&tray_binary, true) {
        return Ok("Started Codex Rotate tray.".to_string());
    }

    Err(anyhow!(
        "Timed out waiting for the Codex Rotate tray to start."
    ))
}

fn tray_quit_message() -> Result<String> {
    let tray_binary = resolve_tray_binary()?;
    let process_ids = list_running_tray_process_ids(&tray_binary)?;
    if process_ids.is_empty() {
        clear_tray_service_registration();
        return Ok("Codex Rotate tray is not running.".to_string());
    }

    for process_id in process_ids {
        stop_process(process_id)
            .with_context(|| format!("Failed to stop tray pid {}.", process_id))?;
    }
    clear_tray_service_registration();

    if wait_for_tray_state(&tray_binary, false) {
        return Ok("Stopped Codex Rotate tray.".to_string());
    }

    Err(anyhow!(
        "Timed out waiting for the Codex Rotate tray to stop."
    ))
}

fn tray_is_running() -> Result<bool> {
    let tray_binary = resolve_tray_binary()?;
    tray_is_running_with_path(&tray_binary)
}

fn tray_is_running_with_path(tray_binary: &Path) -> Result<bool> {
    Ok(!list_running_tray_process_ids(tray_binary)?.is_empty())
}

fn refresh_local_tray_if_needed(tray_binary: &Path) -> Result<()> {
    let Some(build) = detect_local_tray_build(tray_binary) else {
        return Ok(());
    };
    let sources_newer_than_binary = local_tray_sources_newer_than_binary(&build)?;
    if !sources_newer_than_binary {
        return Ok(());
    }

    rebuild_local_tray(&build)?;
    if tray_is_running_with_path(tray_binary)? {
        stop_running_trays(tray_binary)?;
        if !wait_for_tray_state(tray_binary, false) {
            return Err(anyhow!(
                "Timed out waiting for the stale Codex Rotate tray to stop."
            ));
        }
    }
    Ok(())
}

fn launch_tray_binary(tray_binary: &Path) -> Result<()> {
    launch_tray_process(tray_binary)
}

fn wait_for_tray_state(tray_binary: &Path, running: bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match tray_is_running_with_path(tray_binary) {
            Ok(value) if value == running => return true,
            _ => thread::sleep(Duration::from_millis(100)),
        }
    }
    tray_is_running_with_path(tray_binary).ok() == Some(running)
}

fn list_running_tray_process_ids(tray_binary: &Path) -> Result<Vec<u32>> {
    #[cfg(windows)]
    {
        let output = Command::new("tasklist")
            .args([
                "/FO",
                "CSV",
                "/NH",
                "/FI",
                &format!("IMAGENAME eq {}", tray_binary_name()),
            ])
            .output()
            .context("Failed to query running tray processes.")?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow!(
                "{}",
                if detail.is_empty() {
                    "Failed to query running tray processes.".to_string()
                } else {
                    detail
                }
            ));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with("INFO:"))
            .filter_map(|line| {
                let columns = line
                    .split("\",\"")
                    .map(|value| value.trim_matches('"'))
                    .collect::<Vec<_>>();
                columns.get(1).and_then(|value| parse_process_id(value))
            })
            .collect::<Vec<_>>());
    }

    #[cfg(not(windows))]
    {
        let output = Command::new("ps")
            .args(["ax", "-o", "pid=,command="])
            .output()
            .context("Failed to query running tray processes.")?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
            return Err(anyhow!(
                "{}",
                if detail.is_empty() {
                    "Failed to query running tray processes.".to_string()
                } else {
                    detail
                }
            ));
        }
        let tray_binary = tray_binary.display().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout);
        return Ok(stdout
            .lines()
            .map(str::trim)
            .filter(|line| {
                let mut parts = line.split_whitespace();
                let _pid = parts.next();
                let first = parts.next();
                let second = parts.next();
                command_tokens_match_binary(first, second, &tray_binary)
            })
            .filter_map(|line| line.split_whitespace().next().and_then(parse_process_id))
            .collect::<Vec<_>>());
    }
}

fn command_tokens_match_binary(first: Option<&str>, second: Option<&str>, binary: &str) -> bool {
    first == Some(binary) || (shell_like_command(first) && second == Some(binary))
}

fn shell_like_command(command: Option<&str>) -> bool {
    let Some(command) = command else {
        return false;
    };
    let Some(name) = Path::new(command)
        .file_name()
        .and_then(|value| value.to_str())
    else {
        return false;
    };
    matches!(name, "sh" | "bash" | "zsh" | "dash")
}

fn stop_process(process_id: u32) -> Result<()> {
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &process_id.to_string(), "/T", "/F"])
            .status()
            .context("Failed to invoke taskkill.")?;
        if status.success() {
            return Ok(());
        }
        return Err(anyhow!("taskkill exited with status {}.", status));
    }

    #[cfg(not(windows))]
    {
        let status = Command::new("kill")
            .args(["-TERM", &process_id.to_string()])
            .status()
            .context("Failed to invoke kill.")?;
        if status.success() {
            return Ok(());
        }
        Err(anyhow!("kill exited with status {}.", status))
    }
}

fn parse_process_id(raw: &str) -> Option<u32> {
    raw.trim().parse::<u32>().ok().filter(|value| *value > 0)
}

fn resolve_tray_binary() -> Result<PathBuf> {
    let mut candidates = Vec::new();
    if let Some(value) = env::var_os("CODEX_ROTATE_TRAY_BIN") {
        candidates.push(PathBuf::from(value));
    }

    if let Ok(current_exe) = env::current_exe() {
        if let Some(parent) = current_exe.parent() {
            candidates.push(parent.join(tray_binary_name()));
        }
    }

    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("..");
    candidates.push(
        repo_root
            .join("target")
            .join("debug")
            .join(tray_binary_name()),
    );
    candidates.push(
        repo_root
            .join("target")
            .join("release")
            .join(tray_binary_name()),
    );

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(anyhow!(
        "Unable to find the codex-rotate tray binary. Set CODEX_ROTATE_TRAY_BIN to override."
    ))
}

fn tray_binary_name() -> &'static str {
    #[cfg(windows)]
    {
        "codex-rotate-tray.exe"
    }

    #[cfg(not(windows))]
    {
        "codex-rotate-tray"
    }
}

fn write_output(writer: &mut dyn Write, output: &str) -> Result<()> {
    writer.write_all(output.as_bytes())?;
    if !output.ends_with('\n') {
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn parse_add_alias(args: &[String]) -> Result<Option<String>> {
    if args.len() > 1 {
        return Err(anyhow!("Usage: codex-rotate add [alias]"));
    }
    if let Some(alias) = args.first() {
        if alias.starts_with('-') {
            return Err(anyhow!("Usage: codex-rotate add [alias]"));
        }
        let trimmed = alias.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            Ok(Some(trimmed.to_string()))
        }
    } else {
        Ok(None)
    }
}

fn parse_remove_selector(args: &[String]) -> Result<&str> {
    if args.len() != 1 || args[0].starts_with('-') {
        return Err(anyhow!("Usage: codex-rotate remove <selector>"));
    }
    Ok(args[0].as_str())
}

fn parse_create_options(args: &[String]) -> Result<CreateCommandOptions> {
    let mut positionals = Vec::new();
    let mut profile_name = None;
    let mut base_email = None;
    let mut force = false;
    let mut ignore_current = false;
    let mut restore_previous_auth_after_create = false;

    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--force" => {
                force = true;
            }
            "--ignore-current" => {
                ignore_current = true;
            }
            "--restore-auth" | "--restore-previous-auth" => {
                restore_previous_auth_after_create = true;
            }
            "--profile" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"))?;
                profile_name = Some(value.clone());
                index += 1;
            }
            "--base-email" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"))?;
                base_email = Some(value.clone());
                index += 1;
            }
            _ if arg.starts_with("--profile=") => {
                profile_name = Some(arg["--profile=".len()..].to_string());
            }
            _ if arg.starts_with("--base-email=") => {
                base_email = Some(arg["--base-email=".len()..].to_string());
            }
            _ if arg.starts_with('-') => return Err(anyhow!("Unknown create option: \"{arg}\"")),
            _ => positionals.push(arg.to_string()),
        }
        index += 1;
    }

    if positionals.len() > 1 {
        return Err(anyhow!("Usage: codex-rotate create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"));
    }

    Ok(CreateCommandOptions {
        alias: positionals
            .first()
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        profile_name,
        base_email,
        force,
        ignore_current,
        restore_previous_auth_after_create,
        require_usable_quota: false,
        source: CreateCommandSource::Manual,
    })
}

fn parse_create_invocation(args: &[String]) -> Result<CreateInvocation> {
    let options = parse_create_options(args)?;
    Ok(CreateInvocation {
        alias: options.alias,
        profile_name: options.profile_name,
        base_email: options.base_email,
        force: options.force,
        ignore_current: options.ignore_current,
        restore_previous_auth_after_create: options.restore_previous_auth_after_create,
        require_usable_quota: options.require_usable_quota,
    })
}

fn parse_relogin_options(args: &[String]) -> Result<(String, ReloginOptions)> {
    let mut positionals = Vec::new();
    let mut options = ReloginOptions::default();

    for arg in args {
        match arg.as_str() {
            "--allow-email-change" => options.allow_email_change = true,
            "--device-auth" => {
                return Err(anyhow!(
                    "--device-auth is no longer supported. Use the managed-browser default flow or pass --manual-login if you need to repair it interactively."
                ));
            }
            "--manual-login" | "--browser-login" | "--no-device-auth" => {
                options.manual_login = true;
            }
            "--logout-first" => options.logout_first = true,
            "--keep-session" | "--no-logout-first" => options.logout_first = false,
            _ if arg.starts_with('-') => return Err(anyhow!("Unknown relogin option: \"{arg}\"")),
            _ => positionals.push(arg.clone()),
        }
    }

    if positionals.len() != 1 {
        return Err(anyhow!(
            "Usage: codex-rotate relogin <selector> [--allow-email-change] [--manual-login] [--keep-session]"
        ));
    }

    Ok((positionals[0].clone(), options))
}

fn parse_relogin_invocation(args: &[String]) -> Result<ReloginInvocation> {
    let (selector, options) = parse_relogin_options(args)?;
    Ok(ReloginInvocation {
        selector,
        allow_email_change: options.allow_email_change,
        logout_first: options.logout_first,
        manual_login: options.manual_login,
    })
}

fn help_text() -> String {
    format!(
        r#"
{BOLD}codex-rotate{RESET} - Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

{BOLD}USAGE{RESET}
  codex-rotate <command> [args]

{BOLD}COMMANDS{RESET}
  {CYAN}add{RESET} [alias]      Snapshot current ~/.codex/auth.json into the pool
  {CYAN}create{RESET} [alias]   Reuse a healthy account, or create a new one when needed
  {CYAN}next{RESET}             Swap to the next account with usable quota
  {CYAN}prev{RESET}             Swap to the previous account
  {CYAN}list{RESET}             Show all accounts with live quota info
  {CYAN}status{RESET}           Show the current active account info and quota
  {CYAN}relogin{RESET} <selector> Repair that account in one step
  {CYAN}remove{RESET} <selector>  Remove that account from the pool
  {CYAN}daemon{RESET} run        Start the background runtime daemon
  {CYAN}tray{RESET} [subcommand] Manage the Codex Rotate tray app
  {CYAN}help{RESET}             Show this help message
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::fs;
    use std::io::BufReader;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[cfg(unix)]
    use std::os::unix::net::UnixListener;

    #[cfg(unix)]
    use codex_rotate_runtime::ipc::{
        daemon_socket_path, read_request, write_message, ClientRequest, ServerMessage,
    };

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{stamp}"))
    }

    fn with_rotate_home<T>(test: impl FnOnce() -> Result<T>) -> Result<T> {
        let _guard = ENV_MUTEX.lock().expect("env mutex");
        let rotate_home = unique_temp_dir("codex-rotate-cli-tests");
        fs::create_dir_all(&rotate_home).expect("create rotate home");
        let previous_rotate_home = std::env::var_os("CODEX_ROTATE_HOME");
        unsafe {
            std::env::set_var("CODEX_ROTATE_HOME", &rotate_home);
        }

        let result = test();

        match previous_rotate_home {
            Some(value) => unsafe {
                std::env::set_var("CODEX_ROTATE_HOME", value);
            },
            None => unsafe {
                std::env::remove_var("CODEX_ROTATE_HOME");
            },
        }
        fs::remove_dir_all(&rotate_home).ok();
        result
    }

    #[cfg(unix)]
    fn spawn_proxy_server(response_output: &str) -> std::thread::JoinHandle<Result<ClientRequest>> {
        let socket_path = daemon_socket_path().expect("daemon socket path");
        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent).expect("create daemon socket dir");
        }
        let listener = UnixListener::bind(&socket_path).expect("bind daemon socket");
        let response_output = response_output.to_string();
        thread::spawn(move || -> Result<ClientRequest> {
            let (_probe_stream, _) = listener.accept().context("accept probe")?;
            let (mut stream, _) = listener.accept().context("accept request")?;
            let mut reader = BufReader::new(stream.try_clone()?);
            let request = read_request(&mut reader)?;
            write_message(
                &mut stream,
                &ServerMessage::Result {
                    ok: true,
                    output: Some(response_output),
                    error: None,
                },
            )?;
            fs::remove_file(&socket_path).ok();
            Ok(request)
        })
    }

    #[cfg(unix)]
    fn spawn_reachable_daemon() -> std::thread::JoinHandle<Result<()>> {
        let socket_path = daemon_socket_path().expect("daemon socket path");
        if let Some(parent) = socket_path.parent() {
            fs::create_dir_all(parent).expect("create daemon socket dir");
        }
        let listener = UnixListener::bind(&socket_path).expect("bind daemon socket");
        thread::spawn(move || -> Result<()> {
            let (_probe_stream, _) = listener.accept().context("accept probe")?;
            fs::remove_file(&socket_path).ok();
            Ok(())
        })
    }

    #[test]
    fn add_alias_parser_accepts_trimmed_optional_alias() {
        assert_eq!(
            parse_add_alias(&["  work  ".to_string()]).expect("add alias"),
            Some("work".to_string())
        );
        assert_eq!(parse_add_alias(&[]).expect("empty alias"), None);
    }

    #[test]
    fn create_parser_preserves_flags_and_alias() {
        let options = parse_create_options(&[
            "bench".to_string(),
            "--force".to_string(),
            "--ignore-current".to_string(),
            "--restore-auth".to_string(),
            "--profile".to_string(),
            "dev-1".to_string(),
            "--base-email".to_string(),
            "dev.{n}@astronlab.com".to_string(),
        ])
        .expect("create options");

        assert_eq!(options.alias.as_deref(), Some("bench"));
        assert_eq!(options.profile_name.as_deref(), Some("dev-1"));
        assert_eq!(options.base_email.as_deref(), Some("dev.{n}@astronlab.com"));
        assert!(options.force);
        assert!(options.ignore_current);
        assert!(options.restore_previous_auth_after_create);
        assert_eq!(options.source, CreateCommandSource::Manual);
        assert!(!options.require_usable_quota);
    }

    #[test]
    fn relogin_parser_supports_current_flags() {
        let (selector, options) = parse_relogin_options(&[
            "acct-123".to_string(),
            "--allow-email-change".to_string(),
            "--manual-login".to_string(),
            "--keep-session".to_string(),
        ])
        .expect("relogin options");

        assert_eq!(selector, "acct-123");
        assert!(options.allow_email_change);
        assert!(options.manual_login);
        assert!(!options.logout_first);
    }

    #[test]
    fn relogin_parser_rejects_legacy_device_auth_flag() {
        let error = parse_relogin_options(&["acct-123".to_string(), "--device-auth".to_string()])
            .expect_err("device auth should fail");
        assert!(
            error
                .to_string()
                .contains("--device-auth is no longer supported"),
            "{error}"
        );
    }

    #[test]
    fn help_text_mentions_daemon_command() {
        let help = help_text();
        assert!(help.contains("daemon"));
        assert!(help.contains("Start the background runtime daemon"));
        assert!(help.contains("tray"));
    }

    #[cfg(unix)]
    #[test]
    fn tray_command_can_launch_report_and_stop_tray_binary() {
        with_rotate_home(|| -> Result<()> {
            let fixture_root = unique_temp_dir("codex-rotate-tray-cli");
            fs::create_dir_all(&fixture_root).expect("fixture root");
            let tray_stub_path = fixture_root.join("codex-rotate-tray");
            let started_path = fixture_root.join("started.txt");
            fs::write(
                &tray_stub_path,
                format!(
                    "#!/bin/sh\ntrap 'exit 0' TERM INT\nprintf 'started\\n' > \"{}\"\nwhile true; do\n  sleep 1\ndone\n",
                    started_path.display()
                ),
            )
            .expect("write tray stub");
            let mut permissions = fs::metadata(&tray_stub_path)
                .expect("tray stub metadata")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&tray_stub_path, permissions).expect("set tray stub permissions");

            let previous_tray_bin = std::env::var_os("CODEX_ROTATE_TRAY_BIN");
            let previous_launchd_label = std::env::var_os("CODEX_ROTATE_TRAY_LAUNCHD_LABEL");
            let launchd_label = format!(
                "com.astronlab.codex-rotate.tray.test.{}",
                std::process::id()
            );
            unsafe {
                std::env::set_var("CODEX_ROTATE_TRAY_BIN", &tray_stub_path);
                std::env::set_var("CODEX_ROTATE_TRAY_LAUNCHD_LABEL", &launchd_label);
            }

            let test_result = (|| -> Result<()> {
                let mut output = Vec::new();
                run_tray_command(&mut output, &["open".to_string()])?;
                assert_eq!(
                    String::from_utf8(output).expect("utf8").trim(),
                    "Started Codex Rotate tray."
                );

                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline && !started_path.exists() {
                    thread::sleep(Duration::from_millis(50));
                }
                assert!(started_path.exists(), "tray stub should have started");

                let error = run_tray_command(&mut Vec::new(), &["status".to_string()])
                    .expect_err("tray without daemon should be unhealthy");
                assert!(
                    error.to_string().contains("daemon is unavailable"),
                    "{error}"
                );

                let mut output = Vec::new();
                run_tray_command(&mut output, &["quit".to_string()])?;
                assert_eq!(
                    String::from_utf8(output).expect("utf8").trim(),
                    "Stopped Codex Rotate tray."
                );

                let error = run_tray_command(&mut Vec::new(), &["status".to_string()])
                    .expect_err("tray should be stopped");
                assert!(error.to_string().contains("not running"));
                Ok(())
            })();

            match previous_tray_bin {
                Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TRAY_BIN", value) },
                None => unsafe { std::env::remove_var("CODEX_ROTATE_TRAY_BIN") },
            }
            match previous_launchd_label {
                Some(value) => unsafe { std::env::set_var("CODEX_ROTATE_TRAY_LAUNCHD_LABEL", value) },
                None => unsafe { std::env::remove_var("CODEX_ROTATE_TRAY_LAUNCHD_LABEL") },
            }

            test_result?;

            fs::remove_dir_all(&fixture_root).ok();
            Ok(())
        })
        .expect("tray command");
    }

    #[test]
    fn daemon_command_rejects_unknown_subcommand() {
        let mut output = Vec::new();
        let error = run_daemon_command(&mut output, &["noop".to_string()])
            .expect_err("unknown daemon subcommand should fail");
        assert!(error.to_string().contains("Unknown daemon command"));
    }

    #[cfg(unix)]
    #[test]
    fn daemon_command_reports_existing_daemon() {
        with_rotate_home(|| {
            let handle = spawn_reachable_daemon();
            let mut output = Vec::new();
            run_daemon_command(&mut output, &["run".to_string()])?;
            handle.join().expect("daemon probe thread")?;
            assert_eq!(
                String::from_utf8(output).expect("utf8").trim(),
                "Codex Rotate daemon is already running."
            );
            Ok(())
        })
        .expect("daemon command");
    }

    #[cfg(unix)]
    #[test]
    fn proxy_dispatch_covers_supported_cli_commands() {
        let cases = vec![
            (
                Some("add"),
                vec!["bench".to_string()],
                InvokeAction::Add {
                    alias: Some("bench".to_string()),
                },
            ),
            (
                Some("create"),
                vec![
                    "bench".to_string(),
                    "--force".to_string(),
                    "--ignore-current".to_string(),
                    "--restore-auth".to_string(),
                    "--profile".to_string(),
                    "dev-1".to_string(),
                    "--base-email".to_string(),
                    "dev.{n}@astronlab.com".to_string(),
                ],
                InvokeAction::Create {
                    options: CreateInvocation {
                        alias: Some("bench".to_string()),
                        profile_name: Some("dev-1".to_string()),
                        base_email: Some("dev.{n}@astronlab.com".to_string()),
                        force: true,
                        ignore_current: true,
                        restore_previous_auth_after_create: true,
                        require_usable_quota: false,
                    },
                },
            ),
            (
                Some("new"),
                Vec::new(),
                InvokeAction::Create {
                    options: CreateInvocation {
                        alias: None,
                        profile_name: None,
                        base_email: None,
                        force: false,
                        ignore_current: false,
                        restore_previous_auth_after_create: false,
                        require_usable_quota: false,
                    },
                },
            ),
            (Some("next"), Vec::new(), InvokeAction::Next),
            (Some("n"), Vec::new(), InvokeAction::Next),
            (Some("prev"), Vec::new(), InvokeAction::Prev),
            (Some("p"), Vec::new(), InvokeAction::Prev),
            (
                Some("relogin"),
                vec![
                    "acct-123".to_string(),
                    "--allow-email-change".to_string(),
                    "--manual-login".to_string(),
                    "--logout-first".to_string(),
                ],
                InvokeAction::Relogin {
                    options: ReloginInvocation {
                        selector: "acct-123".to_string(),
                        allow_email_change: true,
                        logout_first: true,
                        manual_login: true,
                    },
                },
            ),
            (
                Some("reauth"),
                vec!["acct-123".to_string(), "--keep-session".to_string()],
                InvokeAction::Relogin {
                    options: ReloginInvocation {
                        selector: "acct-123".to_string(),
                        allow_email_change: false,
                        logout_first: false,
                        manual_login: false,
                    },
                },
            ),
            (
                Some("remove"),
                vec!["acct-123".to_string()],
                InvokeAction::Remove {
                    selector: "acct-123".to_string(),
                },
            ),
            (
                Some("rm"),
                vec!["acct-123".to_string()],
                InvokeAction::Remove {
                    selector: "acct-123".to_string(),
                },
            ),
        ];

        with_rotate_home(|| {
            for (command, args, expected_action) in cases {
                let handle = spawn_proxy_server("daemon-ok");
                let output =
                    try_run_via_daemon(command, &args).expect("proxy dispatch should succeed");
                let request = handle.join().expect("proxy thread")?;
                assert_eq!(output.as_deref(), Some("daemon-ok"));
                assert_eq!(
                    request,
                    ClientRequest::Invoke {
                        action: expected_action
                    }
                );
            }
            Ok(())
        })
        .expect("proxy dispatch cases");
    }

    #[cfg(unix)]
    #[test]
    fn proxy_dispatch_returns_none_without_daemon() {
        with_rotate_home(|| {
            let output = try_run_via_daemon(Some("status"), &[])?;
            assert!(output.is_none());
            Ok(())
        })
        .expect("no daemon path");
    }

    #[cfg(unix)]
    #[test]
    fn proxy_dispatch_bypasses_read_only_commands_even_with_daemon() {
        with_rotate_home(|| {
            let handle = spawn_reachable_daemon();
            let list_output = try_run_via_daemon(Some("list"), &[])?;
            let status_output = try_run_via_daemon(Some("status"), &[])?;
            assert!(list_output.is_none());
            assert!(status_output.is_none());
            handle.join().expect("reachable daemon thread")?;
            Ok(())
        })
        .expect("read-only commands should stay local");
    }
}
