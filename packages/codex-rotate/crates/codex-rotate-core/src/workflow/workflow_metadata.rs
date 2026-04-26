use super::*;

pub(super) fn read_workflow_file_metadata(
    file_path: &std::path::Path,
) -> Result<WorkflowFileMetadata> {
    if !file_path.exists() {
        return Err(anyhow!(
            "Workflow file was not found at {}.",
            file_path.display()
        ));
    }

    let raw = fs::read_to_string(file_path)
        .with_context(|| format!("Failed to read workflow file {}.", file_path.display()))?;
    Ok(WorkflowFileMetadata {
        workflow_ref: derive_workflow_ref_from_file_path(file_path),
        ..parse_workflow_file_metadata(&raw)
    })
}

pub(super) fn parse_workflow_file_metadata(raw: &str) -> WorkflowFileMetadata {
    let parsed = serde_yaml::from_str::<YamlValue>(raw).ok();
    let root = parsed.as_ref();
    let document = root
        .as_ref()
        .and_then(|value| yaml_mapping_get(value, "document"));
    let metadata = document.and_then(|value| yaml_mapping_get(value, "metadata"));

    WorkflowFileMetadata {
        workflow_ref: None,
        preferred_profile_name: metadata
            .and_then(|value| yaml_mapping_get(value, "preferredProfile"))
            .and_then(read_yaml_string),
        preferred_email: metadata
            .and_then(|value| yaml_mapping_get(value, "preferredEmail"))
            .and_then(read_yaml_string),
        default_full_name: read_workflow_input_property(root, "full_name")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_string),
        default_birth_month: read_workflow_input_property(root, "birth_month")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u8),
        default_birth_day: read_workflow_input_property(root, "birth_day")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u8),
        default_birth_year: read_workflow_input_property(root, "birth_year")
            .and_then(|value| yaml_mapping_get(value, "default"))
            .and_then(read_yaml_u16),
    }
}

pub(super) fn yaml_mapping_get<'a>(value: &'a YamlValue, key: &str) -> Option<&'a YamlValue> {
    value.as_mapping()?.get(YamlValue::String(key.to_string()))
}

pub(super) fn read_workflow_input_property<'a>(
    root: Option<&'a YamlValue>,
    field: &str,
) -> Option<&'a YamlValue> {
    let properties = root
        .and_then(|value| yaml_mapping_get(value, "input"))
        .and_then(|value| yaml_mapping_get(value, "schema"))
        .and_then(|value| yaml_mapping_get(value, "document"))
        .and_then(|value| yaml_mapping_get(value, "properties"))?;
    yaml_mapping_get(properties, field)
}

pub(super) fn read_yaml_string(value: &YamlValue) -> Option<String> {
    match value {
        YamlValue::String(value) => {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        YamlValue::Number(value) => Some(value.to_string()),
        _ => None,
    }
}

pub(super) fn read_yaml_u8(value: &YamlValue) -> Option<u8> {
    match value {
        YamlValue::Number(value) => value.as_u64().and_then(|value| u8::try_from(value).ok()),
        YamlValue::String(value) => value.trim().parse::<u8>().ok(),
        _ => None,
    }
}

pub(super) fn read_yaml_u16(value: &YamlValue) -> Option<u16> {
    match value {
        YamlValue::Number(value) => value.as_u64().and_then(|value| u16::try_from(value).ok()),
        YamlValue::String(value) => value.trim().parse::<u16>().ok(),
        _ => None,
    }
}

pub(super) fn derive_workflow_ref_from_file_path(file_path: &Path) -> Option<String> {
    let canonical_path = file_path.canonicalize().ok()?;
    let paths = resolve_paths().ok()?;
    let mut repo_roots = vec![paths.repo_root.clone()];
    if let Some(bridge_repo_root) = std::env::var_os("CODEX_ROTATE_BRIDGE_REPO_ROOT")
        .map(PathBuf::from)
        .filter(|value| value.exists())
    {
        let canonical_bridge_root = bridge_repo_root.canonicalize().ok()?;
        if !repo_roots.contains(&canonical_bridge_root) {
            repo_roots.push(canonical_bridge_root);
        }
    }

    for repo_root in &repo_roots {
        let workspace_root = repo_root.join(".fast-browser").join("workflows");
        if let Some(workflow_ref) =
            derive_workflow_ref_from_root(&canonical_path, &workspace_root, "workspace")
        {
            return Some(workflow_ref);
        }
    }

    for repo_root in &repo_roots {
        if let Some(workflow_ref) = repo_root.parent().and_then(|parent| {
            let global_root = parent
                .join("ai-rules")
                .join("skills")
                .join("fast-browser")
                .join("workflows");
            derive_workflow_ref_from_root(&canonical_path, &global_root, "sys")
        }) {
            return Some(workflow_ref);
        }
    }

    None
}

pub(super) fn derive_workflow_ref_from_root(
    file_path: &Path,
    root_dir: &Path,
    scope_prefix: &str,
) -> Option<String> {
    let relative_path = file_path.strip_prefix(root_dir).ok()?;
    if relative_path.extension().and_then(|value| value.to_str()) != Some("yaml") {
        return None;
    }

    let segments = relative_path
        .iter()
        .map(|segment| segment.to_str())
        .collect::<Option<Vec<_>>>()?;
    if segments.len() != 3 {
        return None;
    }

    let workflow_name = Path::new(segments[2]).file_stem()?.to_str()?;
    let parts = [
        Some(scope_prefix.to_string()),
        slugify_workflow_path_segment(segments[0]),
        slugify_workflow_path_segment(segments[1]),
        slugify_workflow_path_segment(workflow_name),
    ]
    .into_iter()
    .collect::<Option<Vec<_>>>()?;
    (parts.len() == 4).then(|| parts.join("."))
}

pub(super) fn slugify_workflow_path_segment(value: &str) -> Option<String> {
    let mut slug = String::new();
    let mut last_was_separator = false;

    for ch in value.chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            last_was_separator = false;
        } else if !last_was_separator {
            slug.push('-');
            last_was_separator = true;
        }
    }

    let normalized = slug.trim_matches('-').to_string();
    (!normalized.is_empty()).then_some(normalized)
}
