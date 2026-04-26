mod commands;
mod managed_login;
mod parsing;

use std::env;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
#[cfg(test)]
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use codex_rotate_core::pool::{
    cmd_add, cmd_list_stream_with_options, cmd_remove, cmd_status_stream, ListOptions, NextResult,
};
use codex_rotate_core::workflow::{
    cmd_create_with_progress, CreateCommandOptions, CreateCommandSource, ReloginOptions,
};
use codex_rotate_refresh::{resolve_rebuilt_local_binary, TargetKind};
#[cfg(test)]
use codex_rotate_runtime::daemon::DaemonRunOptions;
use codex_rotate_runtime::ipc::{
    daemon_is_reachable, invoke, subscribe, CreateInvocation, InvokeAction, ReloginInvocation,
    SnapshotMessageKind, StatusSnapshot,
};
use codex_rotate_runtime::rotation_hygiene::{
    relogin as run_shared_relogin, repair_host_history,
    rotate_next_with_options as run_shared_next_with_options,
    rotate_prev_with_options as run_shared_prev_with_options,
    rotate_set_with_options as run_shared_set_with_options, RotationCommandOptions,
};
#[cfg(test)]
use commands::daemon::parse_daemon_run_options;
use commands::daemon::run_daemon_command;
#[cfg(test)]
use commands::guest_bridge::parse_guest_bridge_bind;
use commands::guest_bridge::run_guest_bridge_command;
#[cfg(test)]
use commands::internal::parse_internal_vm_bootstrap_options;
use commands::internal::run_internal_command;
use commands::tray::run_tray_command;
#[cfg(test)]
use commands::tray::{command_matches_binary, run_with_timeout};
#[cfg(all(test, target_os = "macos"))]
use commands::tray::{service_command_matches_binary, stable_tray_binary_candidates};
use parsing::*;

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
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    run_with_args(&args, &mut stdout)
}

fn run_with_args(args: &[String], writer: &mut dyn Write) -> Result<()> {
    refresh_local_cli_if_needed(args)?;
    let command = args.first().map(String::as_str);
    ensure_account_creation_commands_allowed(command)?;

    if let Some(output) = try_run_via_daemon(command, &args[1..])? {
        write_output(writer, &output)?;
        return Ok(());
    }

    match command {
        None | Some("help") | Some("--help") | Some("-h") => write_output(writer, &help_text())?,
        Some("daemon") => run_daemon_command(writer, &args[1..])?,
        Some("guest-bridge") => run_guest_bridge_command(&args[1..])?,
        Some("internal") => run_internal_command(&args[1..])?,
        Some("tray") => run_tray_command(writer, &args[1..])?,
        Some("add") => write_output(writer, &cmd_add(parse_add_alias(&args[1..])?.as_deref())?)?,
        Some("create") => write_output(
            writer,
            &cmd_create_with_progress(
                parse_public_create_options(&args[1..])?,
                cli_progress_callback(),
            )?,
        )?,
        Some("repair-host-history") => {
            let options = parse_repair_host_history_options(&args[1..])?;
            write_output(
                writer,
                &repair_host_history(
                    &options.source_selector,
                    &options.target_selectors,
                    options.all_targets,
                    options.apply,
                )?,
            )?
        }
        Some("next") => match parse_next_options(&args[1..])? {
            NextCommandOptions {
                selector: Some(selector),
                rotation_options,
            } => write_output(
                writer,
                &run_shared_set_with_options(
                    None,
                    &selector,
                    cli_progress_callback(),
                    rotation_options,
                )?,
            )?,
            NextCommandOptions {
                selector: None,
                rotation_options,
            } => {
                let result =
                    run_shared_next_with_options(None, cli_progress_callback(), rotation_options)?;
                let output = match result {
                    NextResult::Rotated { message, .. }
                    | NextResult::Stayed { message, .. }
                    | NextResult::Created {
                        output: message, ..
                    } => message,
                };
                write_output(writer, &output)?
            }
        },
        Some("prev") => {
            let rotation_options = parse_prev_options(&args[1..])?;
            write_output(
                writer,
                &run_shared_prev_with_options(None, cli_progress_callback(), rotation_options)?,
            )?
        }
        Some("set") => {
            let options = parse_set_options(&args[1..])?;
            write_output(
                writer,
                &run_shared_set_with_options(
                    None,
                    &options.selector,
                    cli_progress_callback(),
                    options.rotation_options,
                )?,
            )?
        }
        Some("list") => cmd_list_stream_with_options(writer, parse_list_options(&args[1..])?)?,
        Some("status") => cmd_status_stream(writer)?,
        Some("relogin") => {
            let (selector, options) = parse_public_relogin_options(&args[1..])?;
            write_output(
                writer,
                &run_shared_relogin(None, &selector, options, cli_progress_callback())?,
            )?
        }
        Some("report-duplicates") => write_output(
            writer,
            &codex_rotate_runtime::rotation_hygiene::report_duplicates()?,
        )?,
        Some("remove") => write_output(writer, &cmd_remove(parse_remove_selector(&args[1..])?)?)?,
        Some(other) => {
            return Err(anyhow!(
                "Unknown command: \"{other}\". Run \"codex-rotate help\" for usage."
            ))
        }
    }
    Ok(())
}

fn refresh_local_cli_if_needed(args: &[String]) -> Result<()> {
    let current_binary =
        env::current_exe().context("Failed to resolve the codex-rotate CLI binary.")?;
    let Some(relaunch_binary) = resolve_stale_local_cli_binary(&current_binary)? else {
        return Ok(());
    };
    reexec_cli_binary(&relaunch_binary, args)
}

fn resolve_stale_local_cli_binary(current_binary: &Path) -> Result<Option<PathBuf>> {
    resolve_rebuilt_local_binary(current_binary, TargetKind::Cli)
}

#[cfg(unix)]
fn reexec_cli_binary(binary: &Path, args: &[String]) -> Result<()> {
    use std::os::unix::process::CommandExt;

    let error = Command::new(binary).args(args).exec();
    Err(anyhow!(
        "Failed to re-exec {} after rebuilding the local CLI: {}",
        binary.display(),
        error
    ))
}

#[cfg(not(unix))]
fn reexec_cli_binary(binary: &Path, args: &[String]) -> Result<()> {
    let status = Command::new(binary).args(args).status().with_context(|| {
        format!(
            "Failed to relaunch {} after rebuilding the local CLI.",
            binary.display()
        )
    })?;
    std::process::exit(status.code().unwrap_or(1));
}

fn try_run_via_daemon(command: Option<&str>, args: &[String]) -> Result<Option<String>> {
    if !daemon_is_reachable() {
        return Ok(None);
    }

    let action = match command {
        Some("create") => Some(InvokeAction::Create {
            options: parse_public_create_invocation(args)?,
        }),
        Some("next") => match parse_next_options(args)? {
            NextCommandOptions {
                selector: Some(selector),
                rotation_options:
                    RotationCommandOptions {
                        force_managed_window: true,
                    },
            } => Some(InvokeAction::SetManagedWindow { selector }),
            NextCommandOptions {
                selector: Some(selector),
                ..
            } => Some(InvokeAction::Set { selector }),
            NextCommandOptions {
                selector: None,
                rotation_options:
                    RotationCommandOptions {
                        force_managed_window: true,
                    },
            } => Some(InvokeAction::NextManagedWindow),
            NextCommandOptions { selector: None, .. } => Some(InvokeAction::Next),
        },
        Some("prev") => {
            let options = parse_prev_options(args)?;
            if options.force_managed_window {
                Some(InvokeAction::PrevManagedWindow)
            } else {
                Some(InvokeAction::Prev)
            }
        }
        Some("set") => {
            let options = parse_set_options(args)?;
            if options.rotation_options.force_managed_window {
                Some(InvokeAction::SetManagedWindow {
                    selector: options.selector,
                })
            } else {
                Some(InvokeAction::Set {
                    selector: options.selector,
                })
            }
        }
        Some("relogin") => Some(InvokeAction::Relogin {
            options: parse_public_relogin_invocation(args)?,
        }),
        Some("remove") => Some(InvokeAction::Remove {
            selector: parse_remove_selector(args)?.to_string(),
        }),
        Some("add") | Some("list") | Some("status") | Some("daemon") | Some("tray") | None
        | Some("help") | Some("--help") | Some("-h") => None,
        Some(_) => None,
    };

    Ok(match action {
        Some(action) => {
            let progress_printer =
                command_streams_progress(command).then(DaemonProgressPrinter::spawn);
            let result = invoke(action);
            if let Some(printer) = progress_printer {
                printer.stop();
            }
            match result {
                Ok(output) => Some(output),
                Err(error) if error.to_string().starts_with("Daemon repo root mismatch:") => None,
                Err(error) => return Err(error),
            }
        }
        None => None,
    })
}

fn ensure_account_creation_commands_allowed(command: Option<&str>) -> Result<()> {
    ensure_account_creation_commands_allowed_for_repo_root(command)
}

fn ensure_account_creation_commands_allowed_for_repo_root(command: Option<&str>) -> Result<()> {
    let _ = command;
    Ok(())
}

fn cli_progress_callback() -> Option<Arc<dyn Fn(String) + Send + Sync>> {
    Some(Arc::new(|message| eprintln!("{message}")))
}

struct DaemonProgressPrinter {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl DaemonProgressPrinter {
    fn spawn() -> Self {
        let Ok(mut subscription) = subscribe() else {
            return Self {
                stop: Arc::new(AtomicBool::new(false)),
                handle: None,
            };
        };
        if subscription.recv().is_err() {
            return Self {
                stop: Arc::new(AtomicBool::new(false)),
                handle: None,
            };
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = stop.clone();
        let handle = thread::spawn(move || {
            let mut last_printed = None::<String>;
            while !stop_signal.load(Ordering::Relaxed) {
                let snapshot = match subscription.recv_timeout(Duration::from_millis(200)) {
                    Ok(Some(snapshot)) => snapshot,
                    Ok(None) => continue,
                    Err(_) => break,
                };
                if !snapshot_contains_progress(&snapshot) {
                    continue;
                }
                let Some(message) = snapshot.last_message else {
                    continue;
                };
                if last_printed.as_deref() == Some(message.as_str()) {
                    continue;
                }
                last_printed = Some(message.clone());
                eprintln!("{message}");
            }
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

fn snapshot_contains_progress(snapshot: &StatusSnapshot) -> bool {
    snapshot.last_message_kind == Some(SnapshotMessageKind::Progress)
        && snapshot.last_message.is_some()
}

fn command_streams_progress(command: Option<&str>) -> bool {
    matches!(
        command,
        Some("create") | Some("next") | Some("set") | Some("relogin")
    )
}

fn write_output(writer: &mut dyn Write, output: &str) -> Result<()> {
    writer.write_all(output.as_bytes())?;
    if !output.ends_with('\n') {
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
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
  {CYAN}repair-host-history{RESET} --source <selector> [--target <selector> ...|--all] [--apply]
  {CYAN}next{RESET} [-mw] [selector]  Swap to the next account, or a selected target
  {CYAN}prev{RESET} [-mw]             Swap to the previous account
  {CYAN}set{RESET} [-mw] <selector>   Swap to the selected account (skip quota/health gating)
  {CYAN}list{RESET} [-f|--force-refresh]
                   Show cached quota info; force refreshes healthy accounts serially
  {CYAN}status{RESET}           Show the current active account info and quota
  {CYAN}relogin{RESET} <selector> Repair that account in one step
  {CYAN}report-duplicates{RESET}   Show potential historical duplicates in the current persona
  {CYAN}remove{RESET} <selector>  Remove that account from the pool
  {CYAN}daemon{RESET}           Start the background runtime daemon
  {CYAN}guest-bridge{RESET} [--bind <host:port>] Run the VM guest bridge server
    {CYAN}internal live-check{RESET} <host|vm> Verify live-suite prerequisites
  {CYAN}tray{RESET} [subcommand] Manage the Codex Rotate tray app
  {CYAN}help{RESET}             Show this help message
"#
    )
}

#[cfg(test)]
mod tests;
