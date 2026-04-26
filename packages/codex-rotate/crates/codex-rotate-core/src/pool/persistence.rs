use super::*;

pub fn load_codex_mode_config() -> Result<CodexModeConfig> {
    let state = load_rotate_state_json()?;
    load_codex_mode_config_from_state(&state)
}

pub fn load_codex_mode_config_from_path(path: &Path) -> Result<CodexModeConfig> {
    let state = crate::state::load_rotate_state_json_from_path(path)?;
    load_codex_mode_config_from_state(&state)
}

fn load_codex_mode_config_from_state(state: &Value) -> Result<CodexModeConfig> {
    let config = state
        .get("codex-mode")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .context("Invalid codex-mode config in rotate state.")?;
    Ok(CodexModeConfig::with_defaults(config))
}

pub fn load_pool() -> Result<Pool> {
    let state = load_rotate_state_json()?;
    let object = state.as_object().cloned().unwrap_or_default();
    let mut pool: Pool = serde_json::from_value(json!({
        "active_index": object.get("active_index").cloned().unwrap_or_else(|| Value::Number(0usize.into())),
        "accounts": object.get("accounts").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
    }))
    .context("Invalid pool data in rotate state.")?;
    normalize_pool_entries(&mut pool);
    Ok(pool)
}

pub fn load_rotation_environment_settings() -> Result<RotationEnvironmentSettings> {
    let state = load_rotate_state_json()?;
    let parsed: RotationEnvironmentState =
        serde_json::from_value(state).context("Invalid environment config in rotate state.")?;
    Ok(RotationEnvironmentSettings {
        environment: parsed.environment,
        vm: parsed.vm,
    })
}

pub fn save_pool(pool: &Pool) -> Result<()> {
    let active_index = pool.active_index;
    let accounts = serde_json::to_value(&pool.accounts)?;
    update_rotate_state_json(RotateStateOwner::Pool, move |state| {
        let codex_mode = load_codex_mode_config_from_state(state)?;
        if !state.is_object() {
            *state = Value::Object(Map::new());
        }
        let object = state
            .as_object_mut()
            .expect("rotate state must be a JSON object");
        object.insert(
            "active_index".to_string(),
            Value::Number(active_index.into()),
        );
        object.insert("accounts".to_string(), accounts.clone());
        object.insert("codex-mode".to_string(), serde_json::to_value(&codex_mode)?);
        Ok(())
    })
}

pub fn load_rotation_checkpoint() -> Result<Option<RotationCheckpoint>> {
    let state = load_rotate_state_json()?;
    let Some(rotation) = state.get("rotation") else {
        return Ok(None);
    };

    if rotation.is_null() {
        return Ok(None);
    }

    serde_json::from_value(rotation.clone())
        .map(Some)
        .context("Invalid rotation checkpoint in rotate state.")
}

pub fn save_rotation_checkpoint(checkpoint: Option<&RotationCheckpoint>) -> Result<()> {
    update_rotate_state_json(RotateStateOwner::FullState, move |state| {
        if !state.is_object() {
            *state = Value::Object(Map::new());
        }
        let object = state
            .as_object_mut()
            .expect("rotate state must be a JSON object");
        match checkpoint {
            Some(checkpoint) => {
                object.insert("rotation".to_string(), serde_json::to_value(checkpoint)?);
            }
            None => {
                object.remove("rotation");
            }
        }
        Ok(())
    })
}

pub fn write_selected_account_auth(entry: &AccountEntry) -> Result<()> {
    let paths = resolve_paths()?;
    let Some(parent) = paths.codex_auth_file.parent() else {
        return Err(anyhow!(
            "Failed to resolve the parent directory for {}.",
            paths.codex_auth_file.display()
        ));
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("Failed to create {}.", parent.display()))?;
    write_codex_auth(&paths.codex_auth_file, &entry.auth)
}
