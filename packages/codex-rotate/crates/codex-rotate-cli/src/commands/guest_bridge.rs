use anyhow::{anyhow, Result};
use codex_rotate_vm::run_guest_bridge_server;

pub(crate) fn run_guest_bridge_command(args: &[String]) -> Result<()> {
    let bind = parse_guest_bridge_bind(args)?;
    run_guest_bridge_server(bind.as_deref())
}

pub(crate) fn parse_guest_bridge_bind(args: &[String]) -> Result<Option<String>> {
    let mut bind = None::<String>;
    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].as_str();
        if matches!(arg, "help" | "--help" | "-h") {
            return Err(anyhow!(
                "Usage: codex-rotate guest-bridge [--bind <host:port>]"
            ));
        }
        if arg == "--bind" {
            let Some(value) = args.get(index + 1).map(String::as_str) else {
                return Err(anyhow!(
                    "Usage: codex-rotate guest-bridge [--bind <host:port>]"
                ));
            };
            let value = value.trim();
            if value.is_empty() {
                return Err(anyhow!(
                    "Usage: codex-rotate guest-bridge [--bind <host:port>]"
                ));
            }
            bind = Some(value.to_string());
            index += 2;
            continue;
        }
        return Err(anyhow!(
            "Unknown guest-bridge command: \"{arg}\". Usage: codex-rotate guest-bridge [--bind <host:port>]"
        ));
    }
    Ok(bind)
}
