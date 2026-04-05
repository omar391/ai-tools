use std::io::{self, Write};

use anyhow::{anyhow, Result};
use codex_rotate_core::pool::{
    cmd_add, cmd_list_stream, cmd_next, cmd_prev, cmd_remove, cmd_status_stream,
};
use codex_rotate_core::workflow::{
    cmd_create, cmd_relogin, CreateCommandOptions, CreateCommandSource, ReloginOptions,
};

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

    match command {
        None | Some("help") | Some("--help") | Some("-h") => {
            write_output(&mut stdout, &help_text())?
        }
        Some("add") => write_output(
            &mut stdout,
            &cmd_add(parse_add_alias(&args[1..])?.as_deref())?,
        )?,
        Some("create") | Some("new") => {
            write_output(&mut stdout, &cmd_create(parse_create_options(&args[1..])?)?)?
        }
        Some("next") | Some("n") => write_output(&mut stdout, &cmd_next()?)?,
        Some("prev") | Some("p") => write_output(&mut stdout, &cmd_prev()?)?,
        Some("list") | Some("ls") => cmd_list_stream(&mut stdout)?,
        Some("status") | Some("s") => cmd_status_stream(&mut stdout)?,
        Some("relogin") | Some("reauth") => {
            let (selector, options) = parse_relogin_options(&args[1..])?;
            write_output(&mut stdout, &cmd_relogin(&selector, options)?)?
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
  {CYAN}help{RESET}             Show this help message
"#
    )
}
