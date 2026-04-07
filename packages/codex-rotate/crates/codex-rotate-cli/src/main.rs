use std::io::{self, Write};

use anyhow::{anyhow, Result};
use codex_rotate_core::pool::{
    cmd_add, cmd_list_stream, cmd_next, cmd_prev, cmd_remove, cmd_status_stream,
};
use codex_rotate_core::workflow::{
    cmd_create, cmd_relogin, CreateCommandOptions, CreateCommandSource, ReloginOptions,
};
use codex_rotate_runtime::daemon::run_daemon_forever;
use codex_rotate_runtime::ipc::{
    daemon_is_reachable, invoke, CreateInvocation, InvokeAction, ReloginInvocation,
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

    if let Some(output) = try_run_via_daemon(command, &args[1..])? {
        write_output(&mut stdout, &output)?;
        return Ok(());
    }

    match command {
        None | Some("help") | Some("--help") | Some("-h") => {
            write_output(&mut stdout, &help_text())?
        }
        Some("daemon") => run_daemon_command(&mut stdout, &args[1..])?,
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
                "Unknown command: \"{other}\". Run \"codex-rotate-v2 help\" for usage."
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
        Some("list") | Some("ls") => Some(InvokeAction::List),
        Some("status") | Some("s") => Some(InvokeAction::Status),
        Some("relogin") | Some("reauth") => Some(InvokeAction::Relogin {
            options: parse_relogin_invocation(args)?,
        }),
        Some("remove") | Some("rm") => Some(InvokeAction::Remove {
            selector: parse_remove_selector(args)?.to_string(),
        }),
        Some("daemon") | None | Some("help") | Some("--help") | Some("-h") => None,
        Some(_) => None,
    };

    Ok(match action {
        Some(action) => Some(invoke(action)?),
        None => None,
    })
}

fn run_daemon_command(writer: &mut dyn Write, args: &[String]) -> Result<()> {
    match args.first().map(String::as_str) {
        Some("run") | None => {
            if daemon_is_reachable() {
                return write_output(writer, "Codex Rotate v2 daemon is already running.");
            }
            run_daemon_forever()
        }
        Some(other) => Err(anyhow!(
            "Unknown daemon command: \"{other}\". Run \"codex-rotate-v2 help\" for usage."
        )),
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
        return Err(anyhow!("Usage: codex-rotate-v2 add [alias]"));
    }
    if let Some(alias) = args.first() {
        if alias.starts_with('-') {
            return Err(anyhow!("Usage: codex-rotate-v2 add [alias]"));
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
        return Err(anyhow!("Usage: codex-rotate-v2 remove <selector>"));
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
                    .ok_or_else(|| anyhow!("Usage: codex-rotate-v2 create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"))?;
                profile_name = Some(value.clone());
                index += 1;
            }
            "--base-email" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("Usage: codex-rotate-v2 create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"))?;
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
        return Err(anyhow!("Usage: codex-rotate-v2 create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--base-email <email-family>]"));
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
            "Usage: codex-rotate-v2 relogin <selector> [--allow-email-change] [--manual-login] [--keep-session]"
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
{BOLD}codex-rotate-v2{RESET} - Rotate Codex CLI OAuth tokens across multiple ChatGPT accounts.

{BOLD}USAGE{RESET}
  codex-rotate-v2 <command> [args]

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
        let error = parse_relogin_options(&[
            "acct-123".to_string(),
            "--device-auth".to_string(),
        ])
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
                "Codex Rotate v2 daemon is already running."
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
            (Some("list"), Vec::new(), InvokeAction::List),
            (Some("ls"), Vec::new(), InvokeAction::List),
            (Some("status"), Vec::new(), InvokeAction::Status),
            (Some("s"), Vec::new(), InvokeAction::Status),
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
}
