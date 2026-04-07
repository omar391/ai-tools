use std::fs;

use anyhow::{Context, Result};
use serde_json::{Map, Value};

use crate::fs_security::write_private_string;
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

pub(crate) fn write_rotate_state_json(state: &Value) -> Result<()> {
    let paths = resolve_paths()?;
    let raw = serde_json::to_string_pretty(state)?;
    write_private_string(&paths.pool_file, &raw)
}
