use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{anyhow, Context, Result};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

pub(crate) fn env_mutex() -> &'static Mutex<()> {
    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_MUTEX.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone)]
pub(crate) struct RecordingUtmctl {
    binary_path: PathBuf,
    command_log: PathBuf,
    active_state: PathBuf,
    violation_log: PathBuf,
}

impl RecordingUtmctl {
    pub(crate) fn install(root: &Path) -> Result<Self> {
        let bin_dir = root.join("bin");
        let binary_path = bin_dir.join("utmctl");
        let command_log = root.join("utmctl-commands.log");
        let active_state = root.join("utmctl-active-vms.txt");
        let violation_log = root.join("utmctl-violations.log");

        fs::create_dir_all(&bin_dir)
            .with_context(|| format!("Failed to create {}.", bin_dir.display()))?;
        fs::write(&command_log, "")
            .with_context(|| format!("Failed to initialize {}.", command_log.display()))?;
        fs::write(&active_state, "")
            .with_context(|| format!("Failed to initialize {}.", active_state.display()))?;
        fs::write(&violation_log, "")
            .with_context(|| format!("Failed to initialize {}.", violation_log.display()))?;

        let script = format!(
            r#"#!/bin/sh
set -eu
command_log={command_log}
active_state={active_state}
violation_log={violation_log}

normalize_persona_id() {{
  base=$(basename "${{1-}}")
  printf '%s\n' "${{base%.utm}}"
}}

contains_active() {{
  grep -qxF "$1" "$active_state"
}}

active_snapshot() {{
  if [ -s "$active_state" ]; then
    tr '\n' ' ' < "$active_state" | sed 's/[[:space:]]*$//'
  else
    printf ''
  fi
}}

record_command() {{
  printf '%s %s\n' "$cmd" "$*" >> "$command_log"
}}

write_violation() {{
  printf '%s\n' "$1" >> "$violation_log"
}}

add_active() {{
  if contains_active "$1"; then
    return 0
  fi
  printf '%s\n' "$1" >> "$active_state"
}}

remove_active() {{
  if [ ! -f "$active_state" ]; then
    : > "$active_state"
    return 0
  fi
  tmp="${{active_state}}.tmp.$$"
  : > "$tmp"
  grep -vxF "$1" "$active_state" > "$tmp" || true
  mv "$tmp" "$active_state"
}}

cmd="${{1-}}"
shift || true
record_command "$cmd" "$@"

case "$cmd" in
  start)
    raw_target="${{1-}}"
    if [ -z "$raw_target" ]; then
      printf 'missing VM package path\n' >&2
      exit 1
    fi
    target_id=$(normalize_persona_id "$raw_target")
    if [ -s "$active_state" ] && ! contains_active "$target_id"; then
      write_violation "simultaneous-active: started $target_id while $(active_snapshot) active"
    fi
    add_active "$target_id"
    exit 0
    ;;
  status)
    target_id="${{1-}}"
    if contains_active "$target_id"; then
      printf '%s\n' started
    else
      printf '%s\n' stopped
    fi
    exit 0
    ;;
  list)
    if [ -f "$active_state" ]; then
      while IFS= read -r active_id; do
        [ -n "$active_id" ] || continue
        printf '%s started %s\n' "$active_id" "$active_id"
      done < "$active_state"
    fi
    exit 0
    ;;
  stop)
    target_id="${{1-}}"
    if [ -n "$target_id" ]; then
      remove_active "$target_id"
    fi
    exit 0
    ;;
  *)
    printf 'unsupported utmctl command: %s\n' "$cmd" >&2
    exit 1
    ;;
esac
"#,
            command_log = shell_quote(&command_log),
            active_state = shell_quote(&active_state),
            violation_log = shell_quote(&violation_log),
        );

        fs::write(&binary_path, script)
            .with_context(|| format!("Failed to write {}.", binary_path.display()))?;
        make_executable(&binary_path)?;

        Ok(Self {
            binary_path,
            command_log,
            active_state,
            violation_log,
        })
    }

    pub(crate) fn binary_path(&self) -> &Path {
        &self.binary_path
    }

    pub(crate) fn seed_active_vms<I, S>(&self, active_ids: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut unique = BTreeSet::new();
        for active_id in active_ids {
            let trimmed = active_id.as_ref().trim();
            if !trimmed.is_empty() {
                unique.insert(trimmed.to_string());
            }
        }

        let contents = unique.into_iter().collect::<Vec<_>>().join("\n");
        if contents.is_empty() {
            fs::write(&self.active_state, "")
                .with_context(|| format!("Failed to clear {}.", self.active_state.display()))?;
            return Ok(());
        }

        fs::write(&self.active_state, format!("{contents}\n"))
            .with_context(|| format!("Failed to seed {}.", self.active_state.display()))?;
        Ok(())
    }

    pub(crate) fn command_log_contents(&self) -> Result<String> {
        fs::read_to_string(&self.command_log)
            .with_context(|| format!("Failed to read {}.", self.command_log.display()))
    }

    pub(crate) fn active_vms(&self) -> Result<BTreeSet<String>> {
        if !self.active_state.exists() {
            return Ok(BTreeSet::new());
        }

        let contents = fs::read_to_string(&self.active_state)
            .with_context(|| format!("Failed to read {}.", self.active_state.display()))?;
        Ok(contents
            .lines()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect())
    }

    pub(crate) fn assert_one_active_vm(&self) -> Result<()> {
        let violations = fs::read_to_string(&self.violation_log)
            .with_context(|| format!("Failed to read {}.", self.violation_log.display()))?;
        if !violations.trim().is_empty() {
            return Err(anyhow!(
                "one-active-VM invariant violated:\n{}",
                violations.trim_end()
            ));
        }

        let active_vms = self.active_vms()?;
        if active_vms.len() > 1 {
            return Err(anyhow!(
                "one-active-VM invariant violated: multiple active personas remain: {:?}",
                active_vms
            ));
        }

        Ok(())
    }
}

fn shell_quote(path: &Path) -> String {
    let value = path.to_string_lossy().replace('\'', "'\"'\"'");
    format!("'{value}'")
}

#[cfg(unix)]
fn make_executable(path: &Path) -> Result<()> {
    let mut permissions = fs::metadata(path)
        .with_context(|| format!("Failed to stat {}.", path.display()))?
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions)
        .with_context(|| format!("Failed to chmod {}.", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) -> Result<()> {
    Ok(())
}
