use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::paths::resolve_paths;

pub(crate) fn load_rotate_state_json() -> Result<Value> {
    let paths = resolve_paths()?;
    if !paths.pool_file.exists() {
        return Ok(Value::Object(Map::new()));
    }
    let raw = fs::read_to_string(&paths.pool_file)
        .with_context(|| format!("Failed to read {}.", paths.pool_file.display()))?;
    let parsed: Value = serde_json::from_str(&raw)
        .with_context(|| format!("Invalid rotate state at {}.", paths.pool_file.display()))?;
    if parsed.is_object() {
        Ok(parsed)
    } else {
        Err(anyhow::anyhow!(
            "Rotate state file {} must contain a JSON object.",
            paths.pool_file.display()
        ))
    }
}

pub(crate) fn write_rotate_state_json(state: &Value, remove_legacy_credentials: bool) -> Result<()> {
    let paths = resolve_paths()?;
    if let Some(parent) = paths.pool_file.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let raw = serde_json::to_string_pretty(state)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .mode(0o600)
        .open(&paths.pool_file)
        .with_context(|| format!("Failed to open {}.", paths.pool_file.display()))?;
    file.write_all(raw.as_bytes())?;
    if remove_legacy_credentials && paths.credentials_file.exists() {
        fs::remove_file(&paths.credentials_file).with_context(|| {
            format!(
                "Failed to remove legacy credential store {}.",
                paths.credentials_file.display()
            )
        })?;
    }
    Ok(())
}
