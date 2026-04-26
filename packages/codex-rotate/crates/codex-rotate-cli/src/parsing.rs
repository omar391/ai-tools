use super::*;

pub(super) fn parse_add_alias(args: &[String]) -> Result<Option<String>> {
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

pub(super) fn parse_remove_selector(args: &[String]) -> Result<&str> {
    if args.len() != 1 || args[0].starts_with('-') {
        return Err(anyhow!("Usage: codex-rotate remove <selector>"));
    }
    Ok(args[0].as_str())
}

pub(super) fn parse_list_options(args: &[String]) -> Result<ListOptions> {
    let mut options = ListOptions::default();
    for arg in args {
        match arg.trim() {
            "-f" | "--force-refresh" => options.force_refresh = true,
            _ => {
                return Err(anyhow!("Usage: codex-rotate list [-f|--force-refresh]"));
            }
        }
    }
    Ok(options)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct NextCommandOptions {
    pub(super) selector: Option<String>,
    pub(super) rotation_options: RotationCommandOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SetCommandOptions {
    pub(super) selector: String,
    pub(super) rotation_options: RotationCommandOptions,
}

pub(super) fn parse_next_options(args: &[String]) -> Result<NextCommandOptions> {
    let mut selector = None::<String>;
    let mut rotation_options = RotationCommandOptions::default();
    for arg in args {
        let trimmed = arg.trim();
        if trimmed == "-mw" || trimmed == "--managed-window" {
            rotation_options.force_managed_window = true;
        } else if trimmed.starts_with('-') || selector.is_some() || trimmed.is_empty() {
            return Err(anyhow!("Usage: codex-rotate next [-mw] [selector]"));
        } else {
            selector = Some(trimmed.to_string());
        }
    }
    Ok(NextCommandOptions {
        selector,
        rotation_options,
    })
}

pub(super) fn parse_prev_options(args: &[String]) -> Result<RotationCommandOptions> {
    let mut rotation_options = RotationCommandOptions::default();
    for arg in args {
        let trimmed = arg.trim();
        if trimmed == "-mw" || trimmed == "--managed-window" {
            rotation_options.force_managed_window = true;
        } else {
            return Err(anyhow!("Usage: codex-rotate prev [-mw]"));
        }
    }
    Ok(rotation_options)
}

pub(super) fn parse_set_options(args: &[String]) -> Result<SetCommandOptions> {
    let mut selector = None::<String>;
    let mut rotation_options = RotationCommandOptions::default();
    for arg in args {
        let trimmed = arg.trim();
        if trimmed == "-mw" || trimmed == "--managed-window" {
            rotation_options.force_managed_window = true;
        } else if trimmed.starts_with('-') || selector.is_some() || trimmed.is_empty() {
            return Err(anyhow!("Usage: codex-rotate set [-mw] <selector>"));
        } else {
            selector = Some(trimmed.to_string());
        }
    }
    let Some(selector) = selector else {
        return Err(anyhow!("Usage: codex-rotate set [-mw] <selector>"));
    };
    Ok(SetCommandOptions {
        selector,
        rotation_options,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RepairHostHistoryOptions {
    pub(super) source_selector: String,
    pub(super) target_selectors: Vec<String>,
    pub(super) all_targets: bool,
    pub(super) apply: bool,
}

pub(super) fn parse_repair_host_history_options(
    args: &[String],
) -> Result<RepairHostHistoryOptions> {
    let usage = "Usage: codex-rotate repair-host-history --source <selector> [--target <selector> ...|--all] [--apply]";
    let mut source_selector = None::<String>;
    let mut target_selectors = Vec::<String>::new();
    let mut all_targets = false;
    let mut apply = false;
    let mut index = 0usize;
    while let Some(arg) = args.get(index).map(String::as_str) {
        if arg == "--source" {
            let Some(value) = args.get(index + 1).map(String::as_str) else {
                return Err(anyhow!(usage));
            };
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(usage));
            }
            source_selector = Some(value.to_string());
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--source=") {
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(usage));
            }
            source_selector = Some(value.to_string());
            index += 1;
            continue;
        }
        if arg == "--target" {
            let Some(value) = args.get(index + 1).map(String::as_str) else {
                return Err(anyhow!(usage));
            };
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(usage));
            }
            target_selectors.push(value.to_string());
            index += 2;
            continue;
        }
        if let Some(value) = arg.strip_prefix("--target=") {
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(usage));
            }
            target_selectors.push(value.to_string());
            index += 1;
            continue;
        }
        if arg == "--all" {
            all_targets = true;
            index += 1;
            continue;
        }
        if arg == "--apply" {
            apply = true;
            index += 1;
            continue;
        }
        if arg == "--dry-run" {
            apply = false;
            index += 1;
            continue;
        }
        return Err(anyhow!("Unknown repair-host-history option: \"{arg}\""));
    }

    let Some(source_selector) = source_selector else {
        return Err(anyhow!(usage));
    };
    if target_selectors.is_empty() {
        all_targets = true;
    }
    Ok(RepairHostHistoryOptions {
        source_selector,
        target_selectors,
        all_targets,
        apply,
    })
}

pub(super) fn parse_create_options(
    args: &[String],
    allow_internal_flags: bool,
) -> Result<CreateCommandOptions> {
    let mut positionals = Vec::new();
    let mut profile_name = None;
    let mut template = None;
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
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown create option: \"{arg}\""));
                }
                ignore_current = true;
            }
            "--restore-auth" => {
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown create option: \"{arg}\""));
                }
                restore_previous_auth_after_create = true;
            }
            "--profile" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("{}", create_usage(allow_internal_flags)))?;
                profile_name = Some(value.clone());
                index += 1;
            }
            "--template" => {
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| anyhow!("{}", create_usage(allow_internal_flags)))?;
                template = Some(value.clone());
                index += 1;
            }
            _ if arg.starts_with("--profile=") => {
                profile_name = Some(arg["--profile=".len()..].to_string());
            }
            _ if arg.starts_with("--template=") => {
                template = Some(arg["--template=".len()..].to_string());
            }
            _ if arg.starts_with('-') => return Err(anyhow!("Unknown create option: \"{arg}\"")),
            _ => positionals.push(arg.to_string()),
        }
        index += 1;
    }

    if positionals.len() > 1 {
        return Err(anyhow!("{}", create_usage(allow_internal_flags)));
    }

    Ok(CreateCommandOptions {
        alias: positionals
            .first()
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        profile_name,
        template,
        force,
        ignore_current,
        restore_previous_auth_after_create,
        require_usable_quota: false,
        source: CreateCommandSource::Manual,
    })
}

pub(super) fn create_usage(allow_internal_flags: bool) -> &'static str {
    if allow_internal_flags {
        "Usage: codex-rotate internal create [alias] [--force] [--ignore-current] [--restore-auth] [--profile <managed-name>] [--template <email-family>]"
    } else {
        "Usage: codex-rotate create [alias] [--force] [--profile <managed-name>] [--template <email-family>]"
    }
}

pub(super) fn parse_public_create_options(args: &[String]) -> Result<CreateCommandOptions> {
    parse_create_options(args, false)
}

pub(super) fn parse_internal_create_options(args: &[String]) -> Result<CreateCommandOptions> {
    parse_create_options(args, true)
}

pub(super) fn parse_public_create_invocation(args: &[String]) -> Result<CreateInvocation> {
    let options = parse_public_create_options(args)?;
    Ok(CreateInvocation {
        alias: options.alias,
        profile_name: options.profile_name,
        template: options.template,
        force: options.force,
        ignore_current: options.ignore_current,
        restore_previous_auth_after_create: options.restore_previous_auth_after_create,
        require_usable_quota: options.require_usable_quota,
    })
}

pub(super) fn parse_relogin_options(
    args: &[String],
    allow_internal_flags: bool,
) -> Result<(String, ReloginOptions)> {
    let mut positionals = Vec::new();
    let mut options = ReloginOptions::default();

    for arg in args {
        match arg.as_str() {
            "--allow-email-change" => {
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown relogin option: \"{arg}\""));
                }
                options.allow_email_change = true;
            }
            "--manual-login" => {
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown relogin option: \"{arg}\""));
                }
                options.manual_login = true;
            }
            "--logout-first" => {
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown relogin option: \"{arg}\""));
                }
                options.logout_first = true;
            }
            "--keep-session" => {
                if !allow_internal_flags {
                    return Err(anyhow!("Unknown relogin option: \"{arg}\""));
                }
                options.logout_first = false;
            }
            _ if arg.starts_with('-') => return Err(anyhow!("Unknown relogin option: \"{arg}\"")),
            _ => positionals.push(arg.clone()),
        }
    }

    if positionals.len() != 1 {
        if allow_internal_flags {
            return Err(anyhow!(
                "Usage: codex-rotate internal relogin <selector> [--allow-email-change] [--manual-login] [--logout-first|--keep-session]"
            ));
        }
        return Err(anyhow!("Usage: codex-rotate relogin <selector>"));
    }

    Ok((positionals[0].clone(), options))
}

pub(super) fn parse_public_relogin_options(args: &[String]) -> Result<(String, ReloginOptions)> {
    parse_relogin_options(args, false)
}

pub(super) fn parse_internal_relogin_options(args: &[String]) -> Result<(String, ReloginOptions)> {
    parse_relogin_options(args, true)
}

pub(super) fn parse_public_relogin_invocation(args: &[String]) -> Result<ReloginInvocation> {
    let (selector, options) = parse_public_relogin_options(args)?;
    Ok(ReloginInvocation {
        selector,
        allow_email_change: options.allow_email_change,
        logout_first: options.logout_first,
        manual_login: options.manual_login,
    })
}
