use super::*;
use std::path::Component;

pub(super) fn ensure_host_personas_ready(
    paths: &RuntimePaths,
    pool: &mut codex_rotate_core::pool::Pool,
) -> Result<bool> {
    if pool.accounts.is_empty() {
        return Ok(false);
    }
    let active_index = pool.active_index.min(pool.accounts.len().saturating_sub(1));
    let active_entry = pool.accounts[active_index].clone();
    ensure_live_root_bindings(paths, &active_entry)?;
    provision_host_persona(paths, &active_entry, None)?;
    Ok(false)
}

pub(super) fn provision_host_persona(
    paths: &RuntimePaths,
    entry: &AccountEntry,
    _seed_from: Option<&AccountEntry>,
) -> Result<()> {
    let persona = entry
        .persona
        .as_ref()
        .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?;
    let target = host_persona_paths(paths, persona)?;
    fs::create_dir_all(&target.root)
        .with_context(|| format!("Failed to create {}.", target.root.display()))?;
    if !target.codex_home.exists() {
        fs::create_dir_all(&target.codex_home)?;
    }
    ensure_host_persona_shared_codex_home_links(paths, &target)?;
    ensure_host_persona_local_codex_home_entries(paths, &target)?;

    // Materialize BrowserForge-backed browser persona defaults if missing
    if entry
        .persona
        .as_ref()
        .map(|p| p.browser_fingerprint.is_none())
        .unwrap_or(false)
    {
        let persona_entry = entry.persona.as_ref().unwrap();
        if let Some(profile) = resolve_persona_profile(
            persona_entry
                .persona_profile_id
                .as_deref()
                .unwrap_or("balanced-us-compact"),
            None,
        ) {
            if let Ok(fingerprint) =
                cmd_generate_browser_fingerprint(&persona_entry.persona_id, &profile)
            {
                let mut pool = load_pool()?;
                if let Some(e) = pool
                    .accounts
                    .iter_mut()
                    .find(|a| a.account_id == entry.account_id)
                {
                    if let Some(p) = e.persona.as_mut() {
                        p.browser_fingerprint = Some(fingerprint);
                        save_pool(&pool)?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn ensure_live_root_bindings(paths: &RuntimePaths, entry: &AccountEntry) -> Result<()> {
    let persona = host_persona_paths(
        paths,
        entry
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Account {} is missing persona metadata.", entry.label))?,
    )?;
    migrate_live_root_if_needed(&paths.codex_home, &persona.codex_home)?;
    migrate_live_root_if_needed(&paths.codex_app_support_dir, &persona.codex_app_support_dir)?;
    migrate_live_root_if_needed(&paths.debug_profile_dir, &persona.debug_profile_dir)?;
    ensure_host_persona_shared_codex_home_links(paths, &persona)?;
    ensure_host_persona_local_codex_home_entries(paths, &persona)?;
    ensure_symlink_dir(&paths.codex_home, &persona.codex_home)?;
    ensure_symlink_dir(&paths.codex_app_support_dir, &persona.codex_app_support_dir)?;
    ensure_symlink_dir(&paths.debug_profile_dir, &persona.debug_profile_dir)?;
    Ok(())
}

pub(super) fn switch_host_persona(
    paths: &RuntimePaths,
    source_entry: &AccountEntry,
    target_entry: &AccountEntry,
    _allow_seed: bool,
) -> Result<()> {
    provision_host_persona(paths, target_entry, None)?;
    let source = host_persona_paths(
        paths,
        source_entry
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Source account is missing persona metadata."))?,
    )?;
    let target = host_persona_paths(
        paths,
        target_entry
            .persona
            .as_ref()
            .ok_or_else(|| anyhow!("Target account is missing persona metadata."))?,
    )?;
    // Project visibility and archive state are source-persona UI decisions.
    // Capture and propagate them before thread history becomes bidirectional.
    sync_host_persona_thread_archive_state(
        &source.codex_home,
        &conversation_sync_identity(source_entry),
        &target.codex_home,
        &conversation_sync_identity(target_entry),
        &paths.conversation_sync_db_file,
    )?;
    ensure_host_persona_shared_codex_home_links(paths, &source)?;
    ensure_host_persona_shared_codex_home_links(paths, &target)?;
    ensure_host_persona_local_codex_home_entries(paths, &source)?;
    ensure_host_persona_local_codex_home_entries(paths, &target)?;
    sync_host_persona_local_codex_home_entries(&source.codex_home, &target.codex_home)?;
    fs::create_dir_all(&target.codex_app_support_dir).with_context(|| {
        format!(
            "Failed to create {}.",
            target.codex_app_support_dir.display()
        )
    })?;
    fs::create_dir_all(&target.debug_profile_dir)
        .with_context(|| format!("Failed to create {}.", target.debug_profile_dir.display()))?;
    ensure_symlink_dir(&paths.codex_home, &target.codex_home)?;
    ensure_symlink_dir(&paths.codex_app_support_dir, &target.codex_app_support_dir)?;
    ensure_symlink_dir(&paths.debug_profile_dir, &target.debug_profile_dir)?;
    Ok(())
}

pub(super) fn ensure_host_persona_shared_codex_home_links(
    paths: &RuntimePaths,
    persona: &HostPersonaPaths,
) -> Result<()> {
    fs::create_dir_all(&persona.codex_home)
        .with_context(|| format!("Failed to create {}.", persona.codex_home.display()))?;
    let shared_codex_home = host_shared_codex_home_root(paths);
    fs::create_dir_all(&shared_codex_home)
        .with_context(|| format!("Failed to create {}.", shared_codex_home.display()))?;
    for entry in SHARED_CODEX_HOME_ENTRIES {
        ensure_shared_codex_home_entry_link(
            entry,
            &persona.codex_home.join(entry),
            &shared_codex_home.join(entry),
        )?;
    }
    Ok(())
}

pub(super) fn ensure_host_persona_local_codex_home_entries(
    paths: &RuntimePaths,
    persona: &HostPersonaPaths,
) -> Result<()> {
    fs::create_dir_all(&persona.codex_home)
        .with_context(|| format!("Failed to create {}.", persona.codex_home.display()))?;
    for entry in PERSONA_LOCAL_CODEX_HOME_ENTRIES {
        ensure_persona_local_codex_home_entry(paths, entry, &persona.codex_home.join(entry))?;
    }
    Ok(())
}

fn ensure_persona_local_codex_home_entry(
    paths: &RuntimePaths,
    entry: &str,
    persona_path: &Path,
) -> Result<()> {
    if let Some(link_target) = existing_symlink_target(persona_path)? {
        let seed_path = if path_exists_or_symlink(&link_target) {
            Some(link_target)
        } else {
            resolve_persona_local_codex_home_seed_path(paths, entry)
        };
        remove_path_if_exists(persona_path)?;
        if let Some(seed_path) = seed_path {
            copy_path_best_effort_cow(&seed_path, persona_path)?;
        } else {
            materialize_persona_local_codex_home_default(entry, persona_path)?;
        }
    } else if !path_exists_or_symlink(persona_path) {
        if let Some(seed_path) = resolve_persona_local_codex_home_seed_path(paths, entry) {
            copy_path_best_effort_cow(&seed_path, persona_path)?;
        } else {
            materialize_persona_local_codex_home_default(entry, persona_path)?;
        }
    }

    ensure_persona_local_codex_home_entry_shape(entry, persona_path)
}

fn resolve_persona_local_codex_home_seed_path(
    paths: &RuntimePaths,
    entry: &str,
) -> Option<PathBuf> {
    [
        host_shared_codex_home_root(paths).join(entry),
        legacy_host_shared_codex_home_root(paths).join(entry),
    ]
    .into_iter()
    .find(|candidate| path_exists_or_symlink(candidate))
}

pub(super) fn sync_host_persona_local_codex_home_entries(
    source_codex_home: &Path,
    target_codex_home: &Path,
) -> Result<()> {
    if source_codex_home == target_codex_home {
        return Ok(());
    }
    for entry in PERSONA_LOCAL_CODEX_HOME_ENTRIES {
        sync_host_persona_local_codex_home_entry(
            entry,
            &source_codex_home.join(entry),
            &target_codex_home.join(entry),
        )?;
    }
    Ok(())
}

fn sync_host_persona_local_codex_home_entry(
    entry: &str,
    source_path: &Path,
    target_path: &Path,
) -> Result<()> {
    if path_exists_or_symlink(source_path) {
        copy_path_best_effort_cow(source_path, target_path)?;
    } else {
        remove_path_if_exists(target_path)?;
        materialize_persona_local_codex_home_default(entry, target_path)?;
    }
    ensure_persona_local_codex_home_entry_shape(entry, target_path)
}

pub(super) fn read_thread_handoff_candidate_ids_from_state_db(
    state_db_path: &Path,
) -> Result<Vec<String>> {
    if !state_db_path.exists() {
        return Ok(Vec::new());
    }
    let connection = rusqlite::Connection::open(state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(Vec::new());
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(Vec::new());
    }
    let mut order_terms = Vec::new();
    if columns.iter().any(|column| column == "archived") {
        order_terms.push("coalesce(archived, 0) asc".to_string());
    }
    if columns.iter().any(|column| column == "updated_at_ms") {
        order_terms.push("coalesce(updated_at_ms, 0) desc".to_string());
    }
    if columns.iter().any(|column| column == "updated_at") {
        order_terms.push("coalesce(updated_at, 0) desc".to_string());
    }
    order_terms.push("id asc".to_string());
    let sql = format!(
        "select id from threads where id is not null and trim(id) != '' order by {}",
        order_terms.join(", ")
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut thread_ids = Vec::new();
    for row in rows {
        thread_ids.push(row?);
    }
    Ok(thread_ids)
}

pub(super) fn host_shared_codex_home_root(paths: &RuntimePaths) -> PathBuf {
    paths
        .rotate_home
        .join("personas")
        .join("shared-data")
        .join("codex-home")
}

pub(super) fn legacy_host_shared_codex_home_root(paths: &RuntimePaths) -> PathBuf {
    paths
        .rotate_home
        .join("personas")
        .join("host")
        .join("shared-data")
        .join("codex-home")
}

pub(super) fn ensure_shared_codex_home_entry_link(
    entry: &str,
    persona_path: &Path,
    shared_path: &Path,
) -> Result<()> {
    if !path_exists_or_symlink(shared_path) {
        if is_symlink_to(persona_path, shared_path)? {
            materialize_shared_codex_home_default(entry, shared_path)?;
        } else if let Some(link_target) = existing_symlink_target(persona_path)? {
            if link_target.exists() {
                copy_path(&link_target, shared_path)?;
            } else {
                materialize_shared_codex_home_default(entry, shared_path)?;
            }
        } else if path_exists_or_symlink(persona_path) {
            copy_path(persona_path, shared_path)?;
        } else {
            materialize_shared_codex_home_default(entry, shared_path)?;
        }
    }
    ensure_shared_codex_home_entry_shape(entry, shared_path)?;

    if is_symlink_to(persona_path, shared_path)? {
        return Ok(());
    }
    remove_path_if_exists(persona_path)?;
    ensure_symlink_path(persona_path, shared_path)
}

pub(super) fn materialize_shared_codex_home_default(entry: &str, shared_path: &Path) -> Result<()> {
    if let Some(parent) = shared_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    match shared_codex_home_entry_kind(entry) {
        CodexHomeEntryKind::Directory => fs::create_dir_all(shared_path)
            .with_context(|| format!("Failed to create {}.", shared_path.display())),
        CodexHomeEntryKind::File(default_contents) => fs::write(shared_path, default_contents)
            .with_context(|| format!("Failed to write {}.", shared_path.display())),
    }
}

pub(super) fn ensure_shared_codex_home_entry_shape(entry: &str, shared_path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(shared_path)
        .with_context(|| format!("Failed to inspect {}.", shared_path.display()))?;
    match shared_codex_home_entry_kind(entry) {
        CodexHomeEntryKind::Directory if metadata.is_dir() => Ok(()),
        CodexHomeEntryKind::File(_) if metadata.is_file() => Ok(()),
        CodexHomeEntryKind::Directory => Err(anyhow!(
            "Expected shared Codex-home path {} for {entry} to be a directory.",
            shared_path.display()
        )),
        CodexHomeEntryKind::File(_) => Err(anyhow!(
            "Expected shared Codex-home path {} for {entry} to be a file.",
            shared_path.display()
        )),
    }
}

#[derive(Clone, Copy)]
pub(super) enum CodexHomeEntryKind {
    File(&'static str),
    Directory,
}

pub(super) fn shared_codex_home_entry_kind(entry: &str) -> CodexHomeEntryKind {
    match entry {
        "rules" | "skills" | "vendor_imports" => CodexHomeEntryKind::Directory,
        CODEX_GLOBAL_STATE_FILE_NAME => CodexHomeEntryKind::File("{}\n"),
        _ => CodexHomeEntryKind::File(""),
    }
}

fn materialize_persona_local_codex_home_default(entry: &str, persona_path: &Path) -> Result<()> {
    if let Some(parent) = persona_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    match persona_local_codex_home_entry_kind(entry) {
        CodexHomeEntryKind::Directory => fs::create_dir_all(persona_path)
            .with_context(|| format!("Failed to create {}.", persona_path.display())),
        CodexHomeEntryKind::File(default_contents) => fs::write(persona_path, default_contents)
            .with_context(|| format!("Failed to write {}.", persona_path.display())),
    }
}

fn ensure_persona_local_codex_home_entry_shape(entry: &str, persona_path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(persona_path)
        .with_context(|| format!("Failed to inspect {}.", persona_path.display()))?;
    match persona_local_codex_home_entry_kind(entry) {
        CodexHomeEntryKind::Directory if metadata.is_dir() => Ok(()),
        CodexHomeEntryKind::File(_) if metadata.is_file() => Ok(()),
        CodexHomeEntryKind::Directory => Err(anyhow!(
            "Expected persona-local Codex-home path {} for {entry} to be a directory.",
            persona_path.display()
        )),
        CodexHomeEntryKind::File(_) => Err(anyhow!(
            "Expected persona-local Codex-home path {} for {entry} to be a file.",
            persona_path.display()
        )),
    }
}

fn persona_local_codex_home_entry_kind(entry: &str) -> CodexHomeEntryKind {
    match entry {
        "memory" => CodexHomeEntryKind::Directory,
        _ => CodexHomeEntryKind::File(""),
    }
}

pub(super) fn ensure_symlink_path(link_path: &Path, target_path: &Path) -> Result<()> {
    ensure_symlink_dir_with(link_path, target_path, symlink_path)
}

pub(super) fn path_exists_or_symlink(path: &Path) -> bool {
    path.exists() || path.is_symlink()
}

pub(super) fn existing_symlink_target(path: &Path) -> Result<Option<PathBuf>> {
    let Some(metadata) = symlink_metadata_optional(path)? else {
        return Ok(None);
    };
    if !metadata.file_type().is_symlink() {
        return Ok(None);
    }
    let link_target = fs::read_link(path)
        .with_context(|| format!("Failed to read symlink {}.", path.display()))?;
    if link_target.is_absolute() {
        return Ok(Some(link_target));
    }
    Ok(path.parent().map(|parent| parent.join(link_target)))
}

pub(super) fn remove_path_if_exists(path: &Path) -> Result<()> {
    let Some(metadata) = symlink_metadata_optional(path)? else {
        return Ok(());
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)
            .with_context(|| format!("Failed to remove {}.", path.display()))?;
    } else {
        fs::remove_file(path).with_context(|| format!("Failed to remove {}.", path.display()))?;
    }
    Ok(())
}

pub(super) fn symlink_metadata_optional(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("Failed to inspect {}.", path.display())),
    }
}

pub(super) fn sync_host_persona_thread_archive_state(
    source_codex_home: &Path,
    source_account_id: &str,
    target_codex_home: &Path,
    target_account_id: &str,
    conversation_sync_db_file: &Path,
) -> Result<()> {
    if source_codex_home == target_codex_home {
        return Ok(());
    }

    let Some(source_state_db) = resolve_state_db_file_in_codex_home(source_codex_home) else {
        return Ok(());
    };
    let Some(target_state_db) = resolve_state_db_file_in_codex_home(target_codex_home) else {
        return Ok(());
    };
    if !source_state_db.exists() || !target_state_db.exists() {
        return Ok(());
    }

    let source_connection = rusqlite::Connection::open(&source_state_db)
        .with_context(|| format!("Failed to open {}.", source_state_db.display()))?;
    if !sqlite_table_exists(&source_connection, "threads")? {
        return Ok(());
    }
    let source_columns = sqlite_table_columns(&source_connection, "main", "threads")?;
    if !source_columns.iter().any(|column| column == "id")
        || !source_columns.iter().any(|column| column == "archived")
    {
        return Ok(());
    }

    let mut source_statement = source_connection
        .prepare("select id, archived from threads")
        .with_context(|| format!("Failed to query {}.", source_state_db.display()))?;
    let source_rows = source_statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .with_context(|| {
            format!(
                "Failed to read archive state from {}.",
                source_state_db.display()
            )
        })?;
    let mut archive_states = Vec::new();
    for row in source_rows {
        archive_states.push(row.with_context(|| {
            format!(
                "Failed to decode archived thread state from {}.",
                source_state_db.display()
            )
        })?);
    }

    let mut target_connection = rusqlite::Connection::open(&target_state_db)
        .with_context(|| format!("Failed to open {}.", target_state_db.display()))?;
    if !sqlite_table_exists(&target_connection, "threads")? {
        return Ok(());
    }
    let target_columns = sqlite_table_columns(&target_connection, "main", "threads")?;
    if !target_columns.iter().any(|column| column == "id")
        || !target_columns.iter().any(|column| column == "archived")
    {
        return Ok(());
    }

    let sync_store = ConversationSyncStore::new(conversation_sync_db_file)?;

    let transaction = target_connection.transaction().with_context(|| {
        format!(
            "Failed to open transaction for {}.",
            target_state_db.display()
        )
    })?;
    {
        let mut update_statement = transaction
            .prepare("update threads set archived = ?1 where id = ?2 and archived != ?1")
            .with_context(|| format!("Failed to prepare {}.", target_state_db.display()))?;
        for (source_thread_id, archived) in archive_states {
            let mut candidate_thread_ids = Vec::new();
            if let Some(lineage_id) =
                sync_store.get_lineage_id(source_account_id, &source_thread_id)?
            {
                if let Some(mapped_thread_id) =
                    sync_store.get_local_thread_id(target_account_id, &lineage_id)?
                {
                    if !is_pending_lineage_claim(&mapped_thread_id) {
                        candidate_thread_ids.push(mapped_thread_id);
                    }
                }
            }
            candidate_thread_ids.push(source_thread_id);

            let mut seen_target_thread_ids = HashSet::new();
            for target_thread_id in candidate_thread_ids {
                if !seen_target_thread_ids.insert(target_thread_id.clone()) {
                    continue;
                }
                update_statement
                    .execute(rusqlite::params![archived, target_thread_id])
                    .with_context(|| {
                        format!(
                            "Failed to sync archived state into {}.",
                            target_state_db.display()
                        )
                    })?;
            }
        }
    }
    transaction
        .commit()
        .with_context(|| format!("Failed to commit {}.", target_state_db.display()))?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct HostPersonaPaths {
    pub(super) root: PathBuf,
    pub(super) codex_home: PathBuf,
    pub(super) codex_app_support_dir: PathBuf,
    pub(super) debug_profile_dir: PathBuf,
}

pub(super) fn host_persona_paths(
    paths: &RuntimePaths,
    persona: &codex_rotate_core::pool::PersonaEntry,
) -> Result<HostPersonaPaths> {
    let root = if let Some(relative) = persona.host_root_rel_path.as_deref() {
        let relative = require_relative_persona_root(relative, "host_root_rel_path")?;
        paths.rotate_home.join(relative)
    } else {
        paths
            .rotate_home
            .join("personas")
            .join("host")
            .join(&persona.persona_id)
    };
    Ok(HostPersonaPaths {
        codex_home: root.join("codex-home"),
        codex_app_support_dir: root.join("codex-app-support"),
        debug_profile_dir: root.join("managed-profile"),
        root,
    })
}

pub(super) fn require_relative_persona_root(path: &str, field: &str) -> Result<PathBuf> {
    let candidate = PathBuf::from(path.trim());
    if candidate.as_os_str().is_empty() {
        return Err(anyhow!("{field} cannot be empty."));
    }
    if candidate.is_absolute() {
        return Err(anyhow!("{field} must be relative to the rotate home."));
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::RootDir))
    {
        return Err(anyhow!(
            "{field} cannot contain parent-directory segments or absolute path markers."
        ));
    }
    Ok(candidate)
}

pub(super) fn copy_path(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect {}.", source.display()))?;
    if metadata.is_dir() {
        fs::create_dir_all(target)
            .with_context(|| format!("Failed to create {}.", target.display()))?;
        for entry in
            fs::read_dir(source).with_context(|| format!("Failed to read {}.", source.display()))?
        {
            let entry = entry?;
            copy_path(&entry.path(), &target.join(entry.file_name()))?;
        }
        return Ok(());
    }
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    fs::copy(source, target).with_context(|| {
        format!(
            "Failed to copy {} to {}.",
            source.display(),
            target.display()
        )
    })?;
    Ok(())
}

pub(super) fn migrate_live_root_if_needed(live_path: &Path, target_path: &Path) -> Result<()> {
    if is_symlink_to(live_path, target_path)? {
        fs::create_dir_all(target_path)
            .with_context(|| format!("Failed to create {}.", target_path.display()))?;
        return Ok(());
    }

    if let Some(parent) = target_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }

    if live_path.exists() && !live_path.symlink_metadata()?.file_type().is_symlink() {
        if target_path.exists() {
            if is_empty_directory(target_path)? {
                fs::remove_dir_all(target_path).with_context(|| {
                    format!(
                        "Failed to remove empty migration target {}.",
                        target_path.display()
                    )
                })?;
            } else {
                return Err(anyhow!(
                    "Migration conflict: both live root {} and target persona {} exist as real directories. \
                    This indicates a partially interrupted migration or manual intervention. \
                    Please manually merge any required data from the live root into the persona directory, \
                    then remove the live root so the system can create the required symlink.",
                    live_path.display(),
                    target_path.display()
                ));
            }
        }
        fs::rename(live_path, target_path).with_context(|| {
            format!(
                "Failed to move {} into persona root {}.",
                live_path.display(),
                target_path.display()
            )
        })?;
    }

    if !target_path.exists() {
        fs::create_dir_all(target_path)
            .with_context(|| format!("Failed to create {}.", target_path.display()))?;
    }
    Ok(())
}

pub(super) fn ensure_symlink_dir(live_path: &Path, target_path: &Path) -> Result<()> {
    ensure_symlink_dir_with(live_path, target_path, symlink_dir)
}

pub(super) fn ensure_symlink_dir_with<F>(
    live_path: &Path,
    target_path: &Path,
    mut symlink_fn: F,
) -> Result<()>
where
    F: FnMut(&Path, &Path) -> io::Result<()>,
{
    if is_symlink_to(live_path, target_path)? {
        return Ok(());
    }
    if let Some(parent) = live_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let original_target = if live_path.exists() || live_path.is_symlink() {
        let metadata = fs::symlink_metadata(live_path)
            .with_context(|| format!("Failed to inspect {}.", live_path.display()))?;
        if metadata.file_type().is_symlink() {
            let original_target = fs::read_link(live_path)
                .with_context(|| format!("Failed to read symlink {}.", live_path.display()))?;
            fs::remove_file(live_path)
                .with_context(|| format!("Failed to remove symlink {}.", live_path.display()))?;
            Some(original_target)
        } else {
            return Err(anyhow!(
                "Unexpected filesystem shape: Expected {} to be a symlink (or absent), but found a real file or directory. \
                Please remove it so the correct symlink to {} can be established.",
                live_path.display(),
                target_path.display()
            ));
        }
    } else {
        None
    };

    match symlink_fn(target_path, live_path) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(original_target) = original_target.as_ref() {
                let restore_result = symlink_fn(original_target, live_path);
                if let Err(restore_error) = restore_result {
                    return Err(anyhow!(
                        "Failed to replace symlink {} -> {} and restore {} -> {}. Replacement error: {}. Restore error: {}",
                        live_path.display(),
                        target_path.display(),
                        live_path.display(),
                        original_target.display(),
                        error,
                        restore_error
                    ));
                }
            }

            let message = if error.kind() == ErrorKind::PermissionDenied {
                format!(
                    "Permission denied while replacing symlink {} -> {}.",
                    live_path.display(),
                    target_path.display()
                )
            } else {
                format!(
                    "Failed to replace symlink {} -> {}.",
                    live_path.display(),
                    target_path.display()
                )
            };
            Err(anyhow!("{} {}", message, error))
        }
    }
}

pub(super) fn is_symlink_to(path: &Path, target: &Path) -> Result<bool> {
    if !path.exists() && !path.is_symlink() {
        return Ok(false);
    }
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("Failed to inspect {}.", path.display()))?;
    if !metadata.file_type().is_symlink() {
        return Ok(false);
    }
    let link_target = fs::read_link(path)
        .with_context(|| format!("Failed to read symlink {}.", path.display()))?;
    Ok(link_target == target)
}

#[cfg(unix)]
pub(super) fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(unix)]
pub(super) fn symlink_path(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
pub(super) fn symlink_dir(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[cfg(windows)]
pub(super) fn symlink_path(target: &Path, link: &Path) -> io::Result<()> {
    if target.is_dir() {
        std::os::windows::fs::symlink_dir(target, link)
    } else {
        std::os::windows::fs::symlink_file(target, link)
    }
}
