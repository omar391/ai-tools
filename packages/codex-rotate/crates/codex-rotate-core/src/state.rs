use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use fs2::FileExt;
use serde_json::{Map, Value};

use crate::fs_security::write_private_string;
use crate::paths::{resolve_paths, CorePaths};

const ROTATE_STATE_BACKUP_PREFIX: &str = "accounts.json.bak.";
const ROTATE_STATE_BACKUP_RETENTION: usize = 20;
type AfterRotateStateWriteHook = dyn Fn(&Path, Option<&Path>) -> Result<()>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RotateStateOwner {
    Pool,
    CredentialStore,
    #[cfg_attr(not(test), allow(dead_code))]
    FullState,
}

impl RotateStateOwner {
    fn label(self) -> &'static str {
        match self {
            Self::Pool => "save_pool",
            Self::CredentialStore => "save_credential_store",
            Self::FullState => "write_rotate_state_json",
        }
    }

    fn owned_top_level_keys(self) -> Option<&'static [&'static str]> {
        match self {
            Self::Pool => Some(&["accounts", "active_index"]),
            Self::CredentialStore => Some(&[
                "version",
                "default_create_base_email",
                "domain",
                "families",
                "pending",
                "skipped",
            ]),
            Self::FullState => None,
        }
    }
}

#[derive(Default)]
struct RotateStateWriteHooks<'a> {
    after_write: Option<&'a AfterRotateStateWriteHook>,
}

struct RotateStateLock {
    _file: File,
}

impl RotateStateLock {
    fn acquire(paths: &CorePaths) -> Result<Self> {
        fs::create_dir_all(&paths.lock_dir)
            .with_context(|| format!("Failed to create {}.", paths.lock_dir.display()))?;

        #[cfg(unix)]
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(&paths.accounts_lock_file)
            .with_context(|| format!("Failed to open {}.", paths.accounts_lock_file.display()))?;

        #[cfg(not(unix))]
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&paths.accounts_lock_file)
            .with_context(|| format!("Failed to open {}.", paths.accounts_lock_file.display()))?;

        file.lock_exclusive()
            .with_context(|| format!("Failed to lock {}.", paths.accounts_lock_file.display()))?;

        Ok(Self { _file: file })
    }
}

pub(crate) fn load_rotate_state_json() -> Result<Value> {
    let paths = resolve_paths()?;
    load_rotate_state_json_from_path(&paths.pool_file)
}

pub(crate) fn update_rotate_state_json<F>(owner: RotateStateOwner, mutate: F) -> Result<()>
where
    F: FnOnce(&mut Value) -> Result<()>,
{
    let paths = resolve_paths()?;
    update_rotate_state_json_with_hooks(&paths, owner, mutate, RotateStateWriteHooks::default())
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn write_rotate_state_json(state: &Value) -> Result<()> {
    let next_state = state.clone();
    update_rotate_state_json(RotateStateOwner::FullState, move |current| {
        *current = next_state.clone();
        Ok(())
    })
}

fn update_rotate_state_json_with_hooks<F>(
    paths: &CorePaths,
    owner: RotateStateOwner,
    mutate: F,
    hooks: RotateStateWriteHooks<'_>,
) -> Result<()>
where
    F: FnOnce(&mut Value) -> Result<()>,
{
    let _lock = RotateStateLock::acquire(paths)?;
    let existing_raw = read_rotate_state_raw(&paths.pool_file)?;
    let before = existing_raw
        .as_deref()
        .map(|raw| parse_rotate_state_json(&paths.pool_file, raw))
        .transpose()?
        .unwrap_or_else(empty_rotate_state);

    let mut after = before.clone();
    mutate(&mut after)?;
    ensure_rotate_state_object(&paths.pool_file, &after)?;

    if after == before {
        return Ok(());
    }

    let backup_path = backup_rotate_state(paths, existing_raw.as_deref())?;
    validate_rotate_state_update(owner, &before, &after, backup_path.as_deref())?;

    let raw = serde_json::to_string_pretty(&after)?;
    write_private_string(&paths.pool_file, &raw)?;

    if let Some(after_write) = hooks.after_write {
        after_write(&paths.pool_file, backup_path.as_deref())?;
    }

    if let Err(error) = verify_rotate_state_write(&paths.pool_file, &after) {
        restore_rotate_state(&paths.pool_file, existing_raw.as_deref())?;
        let backup_hint = format_backup_hint(backup_path.as_deref());
        return Err(anyhow!(
            "Failed to verify rotate state update after {} wrote {}.{} {}",
            owner.label(),
            paths.pool_file.display(),
            backup_hint,
            error
        ));
    }

    prune_managed_rotate_state_backups(&paths.rotate_home)?;
    Ok(())
}

fn empty_rotate_state() -> Value {
    Value::Object(Map::new())
}

fn load_rotate_state_json_from_path(path: &Path) -> Result<Value> {
    let raw = read_rotate_state_raw(path)?;
    match raw {
        Some(raw) => parse_rotate_state_json(path, &raw),
        None => Ok(empty_rotate_state()),
    }
}

fn read_rotate_state_raw(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let raw =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}.", path.display()))?;
    Ok(Some(raw))
}

fn parse_rotate_state_json(path: &Path, raw: &str) -> Result<Value> {
    let parsed: Value = serde_json::from_str(raw).map_err(|error| {
        let backup_hint =
            format_backup_hint(latest_valid_rotate_state_backup_path(path).as_deref());
        anyhow!(
            "Invalid rotate state at {}.{} {}",
            path.display(),
            backup_hint,
            error
        )
    })?;
    ensure_rotate_state_object(path, &parsed)?;
    Ok(parsed)
}

fn ensure_rotate_state_object(path: &Path, state: &Value) -> Result<()> {
    if state.is_object() {
        Ok(())
    } else {
        let backup_hint =
            format_backup_hint(latest_valid_rotate_state_backup_path(path).as_deref());
        Err(anyhow!(
            "Rotate state file {} must contain a JSON object.{}",
            path.display(),
            backup_hint
        ))
    }
}

fn latest_valid_rotate_state_backup_path(path: &Path) -> Option<PathBuf> {
    let parent = path.parent()?;
    let mut backups = fs::read_dir(parent)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|candidate| is_rotate_state_backup_file(candidate))
        .collect::<Vec<_>>();
    backups.sort_by_key(|candidate| {
        fs::metadata(candidate)
            .and_then(|metadata| metadata.modified())
            .ok()
    });
    backups.reverse();

    for backup_path in backups {
        if let Ok(raw) = fs::read_to_string(&backup_path) {
            if let Ok(parsed) = serde_json::from_str::<Value>(&raw) {
                if parsed.is_object() {
                    return Some(backup_path);
                }
            }
        }
    }

    None
}

fn is_rotate_state_backup_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.starts_with(ROTATE_STATE_BACKUP_PREFIX))
}

fn validate_rotate_state_update(
    owner: RotateStateOwner,
    before: &Value,
    after: &Value,
    backup_path: Option<&Path>,
) -> Result<()> {
    let Some(owned_keys) = owner.owned_top_level_keys() else {
        return Ok(());
    };

    let before_object = before
        .as_object()
        .ok_or_else(|| anyhow!("Rotate state must be a JSON object before update."))?;
    let after_object = after
        .as_object()
        .ok_or_else(|| anyhow!("Rotate state must be a JSON object after update."))?;
    let owned_keys = owned_keys.iter().copied().collect::<BTreeSet<_>>();
    let all_keys = before_object
        .keys()
        .chain(after_object.keys())
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    for key in all_keys {
        if owned_keys.contains(key) {
            continue;
        }
        if before_object.get(key) != after_object.get(key) {
            let backup_hint = format_backup_hint(backup_path);
            return Err(anyhow!(
                "Refused to replace rotate state because {} attempted to modify unmanaged top-level key \"{}\".{}",
                owner.label(),
                key,
                backup_hint
            ));
        }
    }

    Ok(())
}

fn backup_rotate_state(paths: &CorePaths, raw: Option<&str>) -> Result<Option<PathBuf>> {
    let Some(raw) = raw else {
        return Ok(None);
    };

    let backup_path = next_rotate_state_backup_path(&paths.rotate_home)?;
    write_private_string(&backup_path, raw)?;
    Ok(Some(backup_path))
}

fn next_rotate_state_backup_path(rotate_home: &Path) -> Result<PathBuf> {
    fs::create_dir_all(rotate_home)
        .with_context(|| format!("Failed to create {}.", rotate_home.display()))?;
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let pid = std::process::id();

    for attempt in 0..32 {
        let nanos = Utc::now().timestamp_subsec_nanos();
        let suffix = format!("{stamp}-{pid}-{nanos:09}-{attempt}");
        let candidate = rotate_home.join(format!("{ROTATE_STATE_BACKUP_PREFIX}{suffix}"));
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Err(anyhow!(
        "Failed to allocate a backup path in {}.",
        rotate_home.display()
    ))
}

fn prune_managed_rotate_state_backups(rotate_home: &Path) -> Result<()> {
    let mut backups = fs::read_dir(rotate_home)
        .with_context(|| format!("Failed to read {}.", rotate_home.display()))?
        .flatten()
        .map(|entry| entry.path())
        .filter(|candidate| is_managed_rotate_state_backup_file(candidate))
        .collect::<Vec<_>>();
    backups.sort_by_key(|candidate| {
        fs::metadata(candidate)
            .and_then(|metadata| metadata.modified())
            .ok()
    });

    let prune_count = backups.len().saturating_sub(ROTATE_STATE_BACKUP_RETENTION);
    for backup_path in backups.into_iter().take(prune_count) {
        fs::remove_file(&backup_path)
            .with_context(|| format!("Failed to prune {}.", backup_path.display()))?;
    }

    Ok(())
}

fn is_managed_rotate_state_backup_file(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(suffix) = file_name.strip_prefix(ROTATE_STATE_BACKUP_PREFIX) else {
        return false;
    };
    let timestamp = suffix.split('-').next().unwrap_or_default();
    timestamp.len() == 16
        && timestamp.as_bytes()[8] == b'T'
        && timestamp.as_bytes()[15] == b'Z'
        && timestamp
            .chars()
            .enumerate()
            .all(|(index, ch)| matches!(index, 8 | 15) || ch.is_ascii_digit())
}

fn verify_rotate_state_write(path: &Path, expected: &Value) -> Result<()> {
    let verified = load_rotate_state_json_from_path(path)?;
    if &verified != expected {
        return Err(anyhow!(
            "Verification mismatch after writing {}.",
            path.display()
        ));
    }
    Ok(())
}

fn restore_rotate_state(path: &Path, original_raw: Option<&str>) -> Result<()> {
    match original_raw {
        Some(raw) => write_private_string(path, raw),
        None => {
            if path.exists() {
                fs::remove_file(path)
                    .with_context(|| format!("Failed to remove {}.", path.display()))?;
            }
            Ok(())
        }
    }
}

fn format_backup_hint(backup_path: Option<&Path>) -> String {
    backup_path
        .map(|path| format!(" Backup: {}", path.display()))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::RotateHomeGuard;
    use serde_json::json;

    #[test]
    fn rotate_state_backup_is_created_before_replace() {
        let guard = RotateHomeGuard::enter("codex-rotate-state-backup");
        let paths = resolve_paths().expect("resolve paths");

        write_rotate_state_json(&json!({
            "accounts": [{ "email": "dev.1@astronlab.com" }],
            "active_index": 0,
        }))
        .expect("write initial state");

        update_rotate_state_json(RotateStateOwner::Pool, |state| {
            let object = state.as_object_mut().expect("object");
            object.insert("active_index".to_string(), Value::Number(1usize.into()));
            Ok(())
        })
        .expect("update rotate state");

        let backups = fs::read_dir(&paths.rotate_home)
            .expect("read rotate home")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| is_managed_rotate_state_backup_file(path))
            .collect::<Vec<_>>();
        assert_eq!(backups.len(), 1);
        assert_eq!(
            load_rotate_state_json().expect("load current")["active_index"],
            Value::Number(1usize.into())
        );
        drop(guard);
    }

    #[test]
    fn rotate_state_backup_retention_prunes_to_twenty_managed_files() {
        let _guard = RotateHomeGuard::enter("codex-rotate-state-backup-prune");
        let paths = resolve_paths().expect("resolve paths");

        write_rotate_state_json(&json!({
            "accounts": [],
            "active_index": 0,
        }))
        .expect("write initial state");

        for index in 1..=25usize {
            update_rotate_state_json(RotateStateOwner::Pool, |state| {
                let object = state.as_object_mut().expect("object");
                object.insert("active_index".to_string(), Value::Number(index.into()));
                Ok(())
            })
            .expect("update rotate state");
        }

        let backups = fs::read_dir(&paths.rotate_home)
            .expect("read rotate home")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| is_managed_rotate_state_backup_file(path))
            .collect::<Vec<_>>();
        assert_eq!(backups.len(), ROTATE_STATE_BACKUP_RETENTION);
    }

    #[test]
    fn rotate_state_guard_rejects_unmanaged_top_level_changes() {
        let _guard = RotateHomeGuard::enter("codex-rotate-state-guard");
        write_rotate_state_json(&json!({
            "accounts": [],
            "active_index": 0,
            "families": { "keep": { "profile_name": "dev-1" } }
        }))
        .expect("write initial state");
        let before = load_rotate_state_json().expect("load before");

        let error = update_rotate_state_json(RotateStateOwner::Pool, |state| {
            state.as_object_mut().expect("object").remove("families");
            Ok(())
        })
        .expect_err("reject unmanaged mutation");

        assert!(error
            .to_string()
            .contains("attempted to modify unmanaged top-level key \"families\""));
        assert_eq!(load_rotate_state_json().expect("load after"), before);
    }

    #[test]
    fn rotate_state_verification_failure_restores_original_file() {
        let _guard = RotateHomeGuard::enter("codex-rotate-state-verify");
        let paths = resolve_paths().expect("resolve paths");
        write_rotate_state_json(&json!({
            "accounts": [],
            "active_index": 0,
        }))
        .expect("write initial state");
        let original_raw = fs::read_to_string(&paths.pool_file).expect("read original");

        let error = update_rotate_state_json_with_hooks(
            &paths,
            RotateStateOwner::Pool,
            |state| {
                state
                    .as_object_mut()
                    .expect("object")
                    .insert("active_index".to_string(), Value::Number(1usize.into()));
                Ok(())
            },
            RotateStateWriteHooks {
                after_write: Some(&|path, _backup_path| {
                    fs::write(path, "{\"broken\": true} trailing")
                        .with_context(|| format!("Failed to corrupt {}.", path.display()))?;
                    Ok(())
                }),
            },
        )
        .expect_err("verification should fail");

        assert!(error
            .to_string()
            .contains("Failed to verify rotate state update"));
        assert_eq!(
            fs::read_to_string(&paths.pool_file).expect("read restored"),
            original_raw
        );
    }

    #[test]
    fn load_rotate_state_json_reports_latest_valid_backup() {
        let _guard = RotateHomeGuard::enter("codex-rotate-state-backup-error");
        let paths = resolve_paths().expect("resolve paths");
        let backup_path = paths
            .rotate_home
            .join(format!("{ROTATE_STATE_BACKUP_PREFIX}20260415T030000Z"));
        write_private_string(&backup_path, "{\"accounts\":[],\"active_index\":0}")
            .expect("write backup");
        fs::write(&paths.pool_file, "{\"broken\": true} trailing").expect("write invalid state");

        let error = load_rotate_state_json().expect_err("state should be invalid");
        let rendered = error.to_string();
        assert!(rendered.contains("Latest valid backup") || rendered.contains("Backup:"));
        assert!(rendered.contains(backup_path.to_string_lossy().as_ref()));
    }
}
