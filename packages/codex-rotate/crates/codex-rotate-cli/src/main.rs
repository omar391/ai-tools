use anyhow::{anyhow, Result};
use codex_rotate_core::legacy::run_legacy_cli_command;
use codex_rotate_core::pool::{cmd_add, cmd_list, cmd_next, cmd_prev, cmd_remove, cmd_status};

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

    let output = match command {
        None | Some("help") | Some("--help") | Some("-h") => help_text(),
        Some("add") => cmd_add(parse_add_alias(&args[1..])?.as_deref())?,
        Some("next") | Some("n") => cmd_next()?,
        Some("prev") | Some("p") => cmd_prev()?,
        Some("list") | Some("ls") => cmd_list()?,
        Some("status") | Some("s") => cmd_status()?,
        Some("remove") | Some("rm") => cmd_remove(parse_remove_selector(&args[1..])?)?,
        Some("create")
        | Some("new")
        | Some("relogin")
        | Some("reauth")
        | Some("__legacy_next_create") => {
            let forwarded = args.iter().map(String::as_str).collect::<Vec<_>>();
            run_legacy_cli_command(&forwarded)?
        }
        Some(other) => {
            return Err(anyhow!(
                "Unknown command: \"{other}\". Run \"codex-rotate help\" for usage."
            ))
        }
    };

    print!("{output}");
    if !output.ends_with('\n') {
        println!();
    }
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
