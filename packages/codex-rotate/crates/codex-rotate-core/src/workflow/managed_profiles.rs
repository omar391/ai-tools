use super::*;

pub(super) fn inspect_managed_profiles() -> Result<ManagedProfilesInspection> {
    let paths = resolve_paths()?;
    let fast_browser_runtime = std::env::var("CODEX_ROTATE_FAST_BROWSER_RUNTIME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| paths.node_bin.clone());
    let output = Command::new(&fast_browser_runtime)
        .arg(&paths.fast_browser_script)
        .arg("profiles")
        .arg("inspect")
        .current_dir(&paths.repo_root)
        .output()
        .with_context(|| {
            format!(
                "Failed to run {} {} profiles inspect.",
                fast_browser_runtime,
                paths.fast_browser_script.display()
            )
        })?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !output.status.success() {
        return Err(anyhow!(if !stdout.is_empty() {
            stdout
        } else {
            format!(
                "fast-browser profiles inspect exited with status {}.",
                output.status
            )
        }));
    }
    let envelope: FastBrowserCliEnvelope<ManagedProfilesInspection> =
        serde_json::from_slice(&output.stdout)
            .context("fast-browser profiles inspect returned invalid JSON.")?;
    if !envelope.ok {
        return Err(anyhow!(
            "{}",
            envelope
                .error
                .and_then(|error| error.message)
                .unwrap_or_else(|| "fast-browser profiles inspect failed.".to_string())
        ));
    }
    envelope
        .result
        .context("fast-browser profiles inspect did not return a result.")
}

pub(super) fn resolve_managed_profile_name(
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
) -> Result<String> {
    let inspection = inspect_managed_profiles()?;
    let available_profile_names = inspection
        .managed_profiles
        .profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect::<Vec<_>>();
    resolve_managed_profile_name_from_candidates(
        &available_profile_names,
        requested_profile_name,
        preferred_profile_name,
        preferred_profile_source,
        inspection.managed_profiles.default.as_deref(),
    )
}

pub(super) fn resolve_managed_profile_name_from_candidates(
    available_names: &[&str],
    requested_profile_name: Option<&str>,
    preferred_profile_name: Option<&str>,
    preferred_profile_source: Option<&str>,
    default_profile_name: Option<&str>,
) -> Result<String> {
    if let Some(requested_profile_name) = requested_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&requested_profile_name) {
            return Ok(requested_profile_name.to_string());
        }
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\" was not found.",
            requested_profile_name
        ));
    }

    if let Some(preferred_profile_name) = preferred_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&preferred_profile_name) {
            return Ok(preferred_profile_name.to_string());
        }
        let suffix = preferred_profile_source
            .map(|value| format!(" from {value}"))
            .unwrap_or_default();
        return Err(anyhow!(
            "Managed fast-browser profile \"{}\"{} was not found.",
            preferred_profile_name,
            suffix
        ));
    }

    if let Some(default_profile_name) = default_profile_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if available_names.contains(&default_profile_name) {
            return Ok(default_profile_name.to_string());
        }
    }

    available_names
        .first()
        .map(|value| (*value).to_string())
        .ok_or_else(|| anyhow!("No managed fast-browser profiles are configured."))
}
