use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Result};
use codex_rotate_core::workflow::{cmd_create_with_progress, cmd_relogin_with_progress};
use codex_rotate_runtime::live_checks::host_live_capability_report;
use codex_rotate_vm::{bootstrap_vm_base, vm_live_capability_report};

use crate::cli_progress_callback;
use crate::commands::guest_bridge::run_guest_bridge_command;
use crate::managed_login::{run_managed_browser_wrapper, run_managed_login};
use crate::parsing::{parse_internal_create_options, parse_internal_relogin_options};

pub(crate) fn run_internal_command(args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("managed-login") => run_managed_login(&args[1..]),
        Some("managed-browser-wrapper") => run_managed_browser_wrapper(&args[1..]),
        Some("launch-managed") => run_internal_launch_managed_command(&args[1..]),
        Some("guest-bridge") => run_guest_bridge_command(&args[1..]),
        Some("live-check") => run_internal_live_check_command(&args[1..]),
        Some("vm-bootstrap") => run_internal_vm_bootstrap_command(&args[1..]),
        Some("create") => {
            let output = cmd_create_with_progress(
                parse_internal_create_options(&args[1..])?,
                cli_progress_callback(),
            )?;
            println!("{output}");
            Ok(())
        }
        Some("relogin") => {
            let (selector, options) = parse_internal_relogin_options(&args[1..])?;
            let output = cmd_relogin_with_progress(&selector, options, cli_progress_callback())?;
            println!("{output}");
            Ok(())
        }
        Some(other) => Err(anyhow!("Unknown internal command: \"{other}\".")),
        None => Err(anyhow!("Usage: codex-rotate internal <subcommand>")),
    }
}

fn run_internal_launch_managed_command(args: &[String]) -> Result<()> {
    let mut port = None::<u16>;
    let mut profile_dir = None::<PathBuf>;
    let mut duration_secs = 5u64;

    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].as_str();
        if arg == "--port" {
            let value = args.get(index + 1).ok_or_else(|| anyhow!("missing port"))?;
            port = Some(value.parse()?);
            index += 2;
        } else if arg == "--profile-dir" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| anyhow!("missing profile-dir"))?;
            profile_dir = Some(PathBuf::from(value));
            index += 2;
        } else if arg == "--duration" {
            let value = args
                .get(index + 1)
                .ok_or_else(|| anyhow!("missing duration"))?;
            duration_secs = value.parse()?;
            index += 2;
        } else {
            return Err(anyhow!("Unknown launch-managed arg: {arg}"));
        }
    }

    codex_rotate_runtime::launcher::ensure_debug_codex_instance(
        None,
        port,
        profile_dir.as_deref(),
        None,
    )?;

    println!(
        "Managed Codex launched. Waiting {} seconds...",
        duration_secs
    );
    thread::sleep(Duration::from_secs(duration_secs));
    Ok(())
}

fn run_internal_vm_bootstrap_command(args: &[String]) -> Result<()> {
    let (guest_root, bridge_root) = parse_internal_vm_bootstrap_options(args)?;
    let output = bootstrap_vm_base(&guest_root, bridge_root.as_deref())?;
    println!("{output}");
    Ok(())
}

fn run_internal_live_check_command(args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("host") => {
            let report = host_live_capability_report()?;
            report.ensure_ready()?;
            println!("{}", report.format());
            Ok(())
        }
        Some("vm") => {
            let report = vm_live_capability_report()?;
            report.ensure_ready()?;
            println!("{}", report.format());
            Ok(())
        }
        Some(other) => Err(anyhow!("Unknown live-check target: \"{other}\". Usage: codex-rotate internal live-check <host|vm>")),
        None => Err(anyhow!("Usage: codex-rotate internal live-check <host|vm>")),
    }
}

pub(crate) fn parse_internal_vm_bootstrap_options(
    args: &[String],
) -> Result<(PathBuf, Option<PathBuf>)> {
    const USAGE: &str =
        "Usage: codex-rotate internal vm-bootstrap <mounted-guest-root> [--bridge-root <path>]";
    let mut guest_root = None::<PathBuf>;
    let mut bridge_root = None::<PathBuf>;
    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].as_str();
        if matches!(arg, "help" | "--help" | "-h") {
            return Err(anyhow!(USAGE));
        }
        if arg == "--bridge-root" {
            let Some(value) = args.get(index + 1).map(String::as_str) else {
                return Err(anyhow!(USAGE));
            };
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(USAGE));
            }
            bridge_root = Some(PathBuf::from(value));
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--bridge-root=") {
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(USAGE));
            }
            bridge_root = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if arg.starts_with('-') {
            return Err(anyhow!(USAGE));
        }
        if guest_root.is_some() {
            return Err(anyhow!(USAGE));
        }
        guest_root = Some(PathBuf::from(arg));
        index += 1;
    }

    let Some(guest_root) = guest_root else {
        return Err(anyhow!(USAGE));
    };
    Ok((guest_root, bridge_root))
}
