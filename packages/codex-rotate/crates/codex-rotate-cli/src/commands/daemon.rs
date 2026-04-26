use std::io::Write;

use anyhow::{anyhow, Result};
use codex_rotate_runtime::daemon::{run_daemon_forever, DaemonRunOptions, DAEMON_TAKEOVER_ARG};
use codex_rotate_runtime::ipc::daemon_is_reachable;

use crate::write_output;

pub(crate) fn run_daemon_command(writer: &mut dyn Write, args: &[String]) -> Result<()> {
    if matches!(
        args.first().map(String::as_str),
        Some("help") | Some("--help") | Some("-h")
    ) {
        return match args.len() {
            1 => write_output(writer, "Usage: codex-rotate daemon"),
            _ => Err(anyhow!(
                "Unknown daemon command: \"{}\". Usage: codex-rotate daemon",
                args[1]
            )),
        };
    }

    let options = parse_daemon_run_options(args)?;
    if !options.takeover && daemon_is_reachable() {
        return write_output(writer, "Codex Rotate daemon is already running.");
    }
    run_daemon_forever(options)
}

pub(crate) fn parse_daemon_run_options(args: &[String]) -> Result<DaemonRunOptions> {
    let mut options = DaemonRunOptions::default();
    let mut index = 0usize;
    while let Some(arg) = args.get(index).map(String::as_str) {
        if arg == DAEMON_TAKEOVER_ARG {
            options.takeover = true;
            index += 1;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--instance-home=") {
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!("Usage: codex-rotate daemon"));
            }
            options.instance_home = Some(value.to_string());
            index += 1;
            continue;
        }
        if arg == "--instance-home" {
            let Some(value) = args.get(index + 1).map(String::as_str) else {
                return Err(anyhow!("Usage: codex-rotate daemon"));
            };
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!("Usage: codex-rotate daemon"));
            }
            options.instance_home = Some(value.to_string());
            index += 2;
            continue;
        }
        return Err(anyhow!(
            "Unknown daemon command: \"{arg}\". Usage: codex-rotate daemon"
        ));
    }
    Ok(options)
}
