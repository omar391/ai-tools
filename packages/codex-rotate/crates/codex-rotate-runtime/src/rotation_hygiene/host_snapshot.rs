use super::*;
#[cfg(test)]
use std::process::Command;

pub(super) struct HostThreadSnapshot {
    source_thread_id: String,
    target_thread_id: String,
    archived: bool,
    source_rollout_path: Option<PathBuf>,
    target_rollout_path: Option<PathBuf>,
    metadata: ThreadHandoffMetadata,
    state_row: Option<ThreadStateDbRow>,
    session_index_entry: Option<Value>,
}

#[derive(Clone)]
pub(super) struct ThreadStateDbSnapshot {
    db_path: PathBuf,
    create_sql: Option<String>,
    rows: BTreeMap<String, ThreadStateDbRow>,
}

#[derive(Clone)]
pub(super) struct ThreadStateDbRow {
    values: BTreeMap<String, rusqlite::types::Value>,
}

pub(super) fn sync_host_persona_conversation_snapshot(
    source_codex_home: &Path,
    source_account_id: &str,
    target_codex_home: &Path,
    target_account_id: &str,
    conversation_sync_db_file: &Path,
) -> Result<()> {
    if source_codex_home == target_codex_home {
        return Ok(());
    }
    if !source_codex_home.exists() {
        return Ok(());
    }

    fs::create_dir_all(target_codex_home)
        .with_context(|| format!("Failed to create {}.", target_codex_home.display()))?;

    let state_snapshot = read_thread_state_db_snapshot(source_codex_home)?;
    let target_state_snapshot = read_thread_state_db_snapshot(target_codex_home)?;
    let session_index_entries = read_session_index_entries(source_codex_home)?;
    let mut source_thread_ids = BTreeSet::new();
    if let Some(state_snapshot) = state_snapshot.as_ref() {
        source_thread_ids.extend(state_snapshot.rows.keys().cloned());
    }
    source_thread_ids.extend(session_index_entries.keys().cloned());

    if source_thread_ids.is_empty() {
        return Ok(());
    }

    let mut store = ConversationSyncStore::new(conversation_sync_db_file)?;
    let mut used_target_thread_ids = BTreeSet::new();
    if let Some(target_state_db) = resolve_state_db_file_in_codex_home(target_codex_home) {
        used_target_thread_ids.extend(read_thread_ids_from_state_db(&target_state_db)?);
    }
    let mut snapshots = Vec::new();

    for source_thread_id in source_thread_ids {
        let lineage_id = match store.get_lineage_id(source_account_id, &source_thread_id)? {
            Some(lineage_id) => lineage_id,
            None => {
                store.bind_local_thread_id(
                    source_account_id,
                    &source_thread_id,
                    &source_thread_id,
                )?;
                source_thread_id.clone()
            }
        };
        let target_thread_id = match store.get_local_thread_id(target_account_id, &lineage_id)? {
            Some(local_thread_id) if !is_pending_lineage_claim(&local_thread_id) => {
                used_target_thread_ids.insert(local_thread_id.clone());
                local_thread_id
            }
            _ => {
                let local_thread_id = allocate_snapshot_thread_id(
                    &source_thread_id,
                    target_account_id,
                    &lineage_id,
                    &used_target_thread_ids,
                );
                store.bind_local_thread_id(target_account_id, &lineage_id, &local_thread_id)?;
                used_target_thread_ids.insert(local_thread_id.clone());
                local_thread_id
            }
        };

        let state_row = state_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.rows.get(&source_thread_id).cloned());
        let mut metadata = state_row
            .as_ref()
            .map(thread_metadata_from_state_row)
            .unwrap_or_default();
        let session_index_entry = session_index_entries.get(&source_thread_id).cloned();
        if let Some(entry) = session_index_entry.as_ref() {
            merge_thread_sidebar_metadata(
                &mut metadata,
                thread_metadata_from_session_index_entry(entry),
            );
        }
        let archived = metadata.archived.unwrap_or(false);
        let source_rollout_path = resolve_source_thread_rollout_path(
            source_codex_home,
            &source_thread_id,
            state_row.as_ref(),
            archived,
        );
        // Preserve a target-local rollout filename when one already exists so a running
        // Codex instance does not keep an in-memory thread bound to a different path.
        let existing_target_state_row = target_state_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.rows.get(&target_thread_id));
        let existing_target_rollout_path = resolve_existing_target_thread_rollout_path(
            target_codex_home,
            &target_thread_id,
            existing_target_state_row,
        );
        let target_rollout_path = source_rollout_path.as_ref().map(|source_rollout_path| {
            target_thread_rollout_path(
                source_codex_home,
                target_codex_home,
                source_rollout_path,
                &source_thread_id,
                &target_thread_id,
                existing_target_rollout_path.as_deref(),
                archived,
            )
        });
        snapshots.push(HostThreadSnapshot {
            source_thread_id,
            target_thread_id,
            archived,
            source_rollout_path,
            target_rollout_path,
            metadata,
            state_row,
            session_index_entry,
        });
    }

    sync_host_snapshot_rollout_files(target_codex_home, &snapshots)?;
    sync_host_snapshot_state_db(target_codex_home, state_snapshot.as_ref(), &snapshots)?;
    sync_host_snapshot_session_index(target_codex_home, &snapshots)?;
    Ok(())
}

pub(super) fn read_thread_state_db_snapshot(
    codex_home: &Path,
) -> Result<Option<ThreadStateDbSnapshot>> {
    let Some(db_path) = resolve_state_db_file_in_codex_home(codex_home) else {
        return Ok(None);
    };
    if !db_path.exists() {
        return Ok(None);
    }
    let connection = rusqlite::Connection::open(&db_path)
        .with_context(|| format!("Failed to open {}.", db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(None);
    }
    let columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !columns.iter().any(|column| column == "id") {
        return Ok(None);
    }
    let create_sql = connection
        .query_row(
            "select sql from sqlite_master where type = 'table' and name = 'threads'",
            [],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()?
        .flatten();
    let sql = format!(
        "select {} from threads",
        columns
            .iter()
            .map(|column| quote_sql_identifier(column))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut statement = connection
        .prepare(&sql)
        .with_context(|| format!("Failed to query {}.", db_path.display()))?;
    let mut rows = statement.query([])?;
    let mut snapshots = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let mut values = BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            values.insert(column.clone(), row.get::<_, rusqlite::types::Value>(index)?);
        }
        let Some(source_thread_id) = sql_value_string(values.get("id"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        snapshots.insert(source_thread_id, ThreadStateDbRow { values });
    }
    Ok(Some(ThreadStateDbSnapshot {
        db_path,
        create_sql,
        rows: snapshots,
    }))
}

pub(super) fn read_session_index_entries(codex_home: &Path) -> Result<BTreeMap<String, Value>> {
    let path = codex_home.join("session_index.jsonl");
    let contents = match fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("Failed to read {}.", path.display()))
        }
    };
    let mut entries = BTreeMap::new();
    for line in contents.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let Some(thread_id) = value
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
        else {
            continue;
        };
        entries.insert(thread_id, value);
    }
    Ok(entries)
}

pub(super) fn read_thread_ids_from_state_db(state_db_path: &Path) -> Result<Vec<String>> {
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
    let mut statement = connection.prepare("select id from threads")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row?);
    }
    Ok(ids)
}

pub(super) fn thread_metadata_from_state_row(row: &ThreadStateDbRow) -> ThreadHandoffMetadata {
    ThreadHandoffMetadata {
        title: row_string(row, "title"),
        first_user_message: row_string(row, "first_user_message"),
        updated_at: row_i64(row, "updated_at"),
        updated_at_ms: row_i64(row, "updated_at_ms"),
        session_index_updated_at: None,
        source: row_string(row, "source"),
        model_provider: row_string(row, "model_provider"),
        cwd: row_string(row, "cwd"),
        sandbox_policy: row_string(row, "sandbox_policy"),
        approval_mode: row_string(row, "approval_mode"),
        projectless: None,
        workspace_root_hint: None,
        archived: row_i64(row, "archived").map(|value| value != 0),
    }
}

pub(super) fn thread_metadata_from_session_index_entry(entry: &Value) -> ThreadHandoffMetadata {
    ThreadHandoffMetadata {
        title: entry
            .get("thread_name")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        session_index_updated_at: entry
            .get("updated_at")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        ..ThreadHandoffMetadata::default()
    }
}

pub(super) fn row_string(row: &ThreadStateDbRow, column: &str) -> Option<String> {
    sql_value_string(row.values.get(column))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn row_i64(row: &ThreadStateDbRow, column: &str) -> Option<i64> {
    match row.values.get(column) {
        Some(rusqlite::types::Value::Integer(value)) => Some(*value),
        Some(rusqlite::types::Value::Real(value)) => Some(*value as i64),
        Some(rusqlite::types::Value::Text(value)) => value.trim().parse::<i64>().ok(),
        _ => None,
    }
}

pub(super) fn sql_value_string(value: Option<&rusqlite::types::Value>) -> Option<&str> {
    match value {
        Some(rusqlite::types::Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

pub(super) fn allocate_snapshot_thread_id(
    source_thread_id: &str,
    target_account_id: &str,
    lineage_id: &str,
    used_target_thread_ids: &BTreeSet<String>,
) -> String {
    let mut salt = 0_u64;
    loop {
        let seed = format!("{target_account_id}\n{lineage_id}\n{source_thread_id}\n{salt}");
        let mut candidate = stable_local_thread_id_like(source_thread_id, &seed);
        if candidate == source_thread_id {
            flip_first_thread_id_char(&mut candidate);
        }
        if !used_target_thread_ids.contains(&candidate) {
            return candidate;
        }
        salt = salt.saturating_add(1);
    }
}

pub(super) fn stable_local_thread_id_like(source_thread_id: &str, seed: &str) -> String {
    if source_thread_id.is_empty() {
        return format!("thread-{}", fnv1a64_hex(seed.as_bytes()));
    }
    let digest = repeated_hex_digest(seed, source_thread_id.len());
    let mut digest_chars = digest.chars();
    source_thread_id
        .chars()
        .map(|ch| {
            if matches!(ch, '-' | '_') {
                ch
            } else if ch.is_ascii_alphanumeric() {
                digest_chars.next().unwrap_or('0')
            } else {
                'x'
            }
        })
        .collect()
}

pub(super) fn repeated_hex_digest(seed: &str, len: usize) -> String {
    let mut output = String::with_capacity(len);
    let mut counter = 0_u64;
    while output.len() < len {
        let chunk = format!("{seed}\n{counter}");
        output.push_str(&fnv1a64_hex(chunk.as_bytes()));
        counter = counter.saturating_add(1);
    }
    output.truncate(len);
    output
}

pub(super) fn fnv1a64_hex(bytes: &[u8]) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

pub(super) fn flip_first_thread_id_char(value: &mut String) {
    let mut chars = value.chars().collect::<Vec<_>>();
    if let Some(ch) = chars.iter_mut().find(|ch| ch.is_ascii_alphanumeric()) {
        *ch = if *ch == '0' { '1' } else { '0' };
    }
    *value = chars.into_iter().collect();
}

pub(super) fn resolve_source_thread_rollout_path(
    source_codex_home: &Path,
    source_thread_id: &str,
    state_row: Option<&ThreadStateDbRow>,
    archived: bool,
) -> Option<PathBuf> {
    if let Some(path) = state_row
        .and_then(|row| row_string(row, "rollout_path"))
        .map(PathBuf::from)
    {
        if path.exists() {
            return Some(path);
        }
        if let Some((_, relative_path)) = thread_artifact_relative_path(source_codex_home, &path) {
            let candidate = source_codex_home.join(relative_path);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }

    let preferred_root = if archived {
        source_codex_home.join("archived_sessions")
    } else {
        source_codex_home.join("sessions")
    };
    find_thread_rollout_path(&preferred_root, source_thread_id)
        .or_else(|| find_thread_rollout_path(source_codex_home, source_thread_id))
}

pub(super) fn resolve_existing_target_thread_rollout_path(
    target_codex_home: &Path,
    target_thread_id: &str,
    state_row: Option<&ThreadStateDbRow>,
) -> Option<PathBuf> {
    let mut fallback = None::<PathBuf>;
    if let Some(path) = state_row
        .and_then(|row| row_string(row, "rollout_path"))
        .map(PathBuf::from)
    {
        if path.exists() {
            return Some(path);
        }
        if let Some((root, relative_path)) = thread_artifact_relative_path(target_codex_home, &path)
        {
            let candidate = target_codex_home.join(root).join(&relative_path);
            if candidate.exists() {
                return Some(candidate);
            }
            fallback = Some(candidate);
        } else {
            fallback = Some(path);
        }
    }

    find_thread_rollout_path(&target_codex_home.join("sessions"), target_thread_id)
        .or_else(|| {
            find_thread_rollout_path(
                &target_codex_home.join("archived_sessions"),
                target_thread_id,
            )
        })
        .or(fallback)
}

pub(super) fn target_thread_rollout_path(
    source_codex_home: &Path,
    target_codex_home: &Path,
    source_rollout_path: &Path,
    source_thread_id: &str,
    target_thread_id: &str,
    existing_target_rollout_path: Option<&Path>,
    archived: bool,
) -> PathBuf {
    if let Some(existing_target_rollout_path) = existing_target_rollout_path {
        if let Some((_, relative_path)) =
            thread_artifact_relative_path(target_codex_home, existing_target_rollout_path)
        {
            return target_codex_home
                .join(preferred_thread_artifact_root(archived))
                .join(relative_path);
        }
    }
    let relative_path = thread_artifact_relative_path(source_codex_home, source_rollout_path)
        .map(|(_, relative_path)| relative_path)
        .unwrap_or_else(|| {
            source_rollout_path
                .file_name()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from(format!("{target_thread_id}.jsonl")))
        });
    let localized = localize_path_thread_id(&relative_path, source_thread_id, target_thread_id);
    target_codex_home
        .join(preferred_thread_artifact_root(archived))
        .join(localized)
}

fn preferred_thread_artifact_root(archived: bool) -> &'static str {
    if archived {
        "archived_sessions"
    } else {
        "sessions"
    }
}

pub(super) fn thread_artifact_relative_path(
    codex_home: &Path,
    path: &Path,
) -> Option<(&'static str, PathBuf)> {
    if let Ok(relative) = path.strip_prefix(codex_home) {
        return split_thread_artifact_relative_path(relative);
    }
    split_thread_artifact_relative_path(path)
}

pub(super) fn split_thread_artifact_relative_path(path: &Path) -> Option<(&'static str, PathBuf)> {
    let components = path
        .components()
        .map(|component| component.as_os_str().to_os_string())
        .collect::<Vec<_>>();
    for (index, component) in components.iter().enumerate() {
        let name = component.to_string_lossy();
        if name == "sessions" || name == "archived_sessions" {
            let mut relative = PathBuf::new();
            for component in components.iter().skip(index + 1) {
                relative.push(component);
            }
            if !relative.as_os_str().is_empty() {
                return Some((
                    if name == "sessions" {
                        "sessions"
                    } else {
                        "archived_sessions"
                    },
                    relative,
                ));
            }
        }
    }
    None
}

pub(super) fn localize_path_thread_id(
    path: &Path,
    source_thread_id: &str,
    target_thread_id: &str,
) -> PathBuf {
    let mut localized = PathBuf::new();
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy();
        localized.push(value.replace(source_thread_id, target_thread_id));
    }
    localized
}

pub(super) fn sync_host_snapshot_rollout_files(
    target_codex_home: &Path,
    snapshots: &[HostThreadSnapshot],
) -> Result<()> {
    for snapshot in snapshots {
        remove_thread_artifact_files_with_id(
            &target_codex_home.join("sessions"),
            &snapshot.target_thread_id,
        )?;
        remove_thread_artifact_files_with_id(
            &target_codex_home.join("archived_sessions"),
            &snapshot.target_thread_id,
        )?;
        if snapshot.target_thread_id != snapshot.source_thread_id {
            remove_thread_artifact_files_with_id(
                &target_codex_home.join("sessions"),
                &snapshot.source_thread_id,
            )?;
            remove_thread_artifact_files_with_id(
                &target_codex_home.join("archived_sessions"),
                &snapshot.source_thread_id,
            )?;
        }
        let (Some(source), Some(target)) = (
            snapshot.source_rollout_path.as_ref(),
            snapshot.target_rollout_path.as_ref(),
        ) else {
            continue;
        };
        copy_thread_jsonl_with_cow_and_localization(
            source,
            target,
            &snapshot.source_thread_id,
            &snapshot.target_thread_id,
        )?;
    }
    Ok(())
}

pub(super) fn sync_host_snapshot_state_db(
    target_codex_home: &Path,
    state_snapshot: Option<&ThreadStateDbSnapshot>,
    snapshots: &[HostThreadSnapshot],
) -> Result<()> {
    let Some(state_snapshot) = state_snapshot else {
        return Ok(());
    };
    if state_snapshot.rows.is_empty() {
        return Ok(());
    }
    let target_db_path = resolve_state_db_file_in_codex_home(target_codex_home)
        .or_else(|| {
            state_snapshot
                .db_path
                .file_name()
                .map(|name| target_codex_home.join(name))
        })
        .unwrap_or_else(|| target_codex_home.join("state_5.sqlite"));
    if let Some(parent) = target_db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let connection = rusqlite::Connection::open(&target_db_path)
        .with_context(|| format!("Failed to open {}.", target_db_path.display()))?;
    connection
        .busy_timeout(Duration::from_secs(5))
        .with_context(|| format!("Failed to configure {}.", target_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        if let Some(create_sql) = state_snapshot.create_sql.as_deref() {
            connection.execute_batch(create_sql).with_context(|| {
                format!(
                    "Failed to create threads table in {}.",
                    target_db_path.display()
                )
            })?;
        }
    }
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(());
    }
    let target_columns = sqlite_table_columns(&connection, "main", "threads")?;
    if !target_columns.iter().any(|column| column == "id") {
        return Ok(());
    }

    for snapshot in snapshots {
        if snapshot.target_thread_id != snapshot.source_thread_id {
            connection
                .execute(
                    "delete from threads where id = ?1",
                    [&snapshot.source_thread_id],
                )
                .with_context(|| {
                    format!(
                        "Failed to remove stale source thread row {} from {}.",
                        snapshot.source_thread_id,
                        target_db_path.display()
                    )
                })?;
        }
        let Some(state_row) = snapshot.state_row.as_ref() else {
            publish_thread_sidebar_metadata_to_state_db(
                target_codex_home,
                &target_db_path,
                &snapshot.target_thread_id,
                &snapshot.metadata,
            )?;
            continue;
        };
        let mut insert_columns = Vec::new();
        let mut values = Vec::new();
        for column in &target_columns {
            if let Some(value) = state_row.values.get(column) {
                insert_columns.push(column.clone());
                values.push(localized_state_db_value(column, value, snapshot));
            }
        }
        if !insert_columns.iter().any(|column| column == "id") {
            continue;
        }
        let placeholders = (1..=insert_columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>();
        let sql = format!(
            "insert or replace into threads ({}) values ({})",
            insert_columns
                .iter()
                .map(|column| quote_sql_identifier(column))
                .collect::<Vec<_>>()
                .join(", "),
            placeholders.join(", ")
        );
        connection
            .execute(&sql, rusqlite::params_from_iter(values))
            .with_context(|| {
                format!(
                    "Failed to upsert synced thread {} into {}.",
                    snapshot.target_thread_id,
                    target_db_path.display()
                )
            })?;
        publish_thread_sidebar_metadata_to_state_db(
            target_codex_home,
            &target_db_path,
            &snapshot.target_thread_id,
            &snapshot.metadata,
        )?;
    }
    Ok(())
}

pub(super) fn localized_state_db_value(
    column: &str,
    value: &rusqlite::types::Value,
    snapshot: &HostThreadSnapshot,
) -> rusqlite::types::Value {
    if column == "id" {
        return rusqlite::types::Value::Text(snapshot.target_thread_id.clone());
    }
    if column == "rollout_path" {
        return rusqlite::types::Value::Text(
            snapshot
                .target_rollout_path
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_default(),
        );
    }
    if column == "archived" {
        return rusqlite::types::Value::Integer(snapshot.archived as i64);
    }
    match value {
        rusqlite::types::Value::Text(text)
            if thread_id_metadata_key(column) && text == &snapshot.source_thread_id =>
        {
            rusqlite::types::Value::Text(snapshot.target_thread_id.clone())
        }
        _ => value.clone(),
    }
}

pub(super) fn sync_host_snapshot_session_index(
    target_codex_home: &Path,
    snapshots: &[HostThreadSnapshot],
) -> Result<()> {
    let path = target_codex_home.join("session_index.jsonl");
    let contents = fs::read_to_string(&path).unwrap_or_default();
    let synced_source_ids = snapshots
        .iter()
        .map(|snapshot| snapshot.source_thread_id.as_str())
        .collect::<BTreeSet<_>>();
    let synced_target_ids = snapshots
        .iter()
        .map(|snapshot| snapshot.target_thread_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut lines = Vec::new();
    for line in contents.lines() {
        let should_replace = serde_json::from_str::<Value>(line)
            .ok()
            .and_then(|value| value.get("id").and_then(Value::as_str).map(str::to_owned))
            .map(|id| {
                synced_source_ids.contains(id.as_str()) || synced_target_ids.contains(id.as_str())
            })
            .unwrap_or(false);
        if !should_replace {
            lines.push(line.to_string());
        }
    }
    for snapshot in snapshots {
        if let Some(entry) = localized_session_index_entry(snapshot) {
            lines.push(serde_json::to_string(&entry)?);
        }
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let output = if lines.is_empty() {
        String::new()
    } else {
        let mut output = lines.join("\n");
        output.push('\n');
        output
    };
    fs::write(&path, output).with_context(|| format!("Failed to write {}.", path.display()))
}

pub(super) fn localized_session_index_entry(snapshot: &HostThreadSnapshot) -> Option<Value> {
    let mut entry = snapshot.session_index_entry.clone().unwrap_or_else(|| {
        json!({
            "id": snapshot.source_thread_id,
            "thread_name": snapshot
                .metadata
                .title
                .as_deref()
                .or(snapshot.metadata.first_user_message.as_deref())
                .map(truncate_handoff_text)
                .unwrap_or_else(|| snapshot.source_thread_id.clone()),
            "updated_at": snapshot
                .metadata
                .session_index_updated_at
                .clone()
                .or_else(|| snapshot.metadata.updated_at_ms.and_then(timestamp_millis_to_rfc3339))
                .or_else(|| snapshot.metadata.updated_at.and_then(timestamp_secs_to_rfc3339))
                .unwrap_or_else(current_rfc3339_timestamp),
        })
    });
    localize_thread_ids_in_json_value(
        &mut entry,
        &snapshot.source_thread_id,
        &snapshot.target_thread_id,
    );
    entry
        .get("thread_name")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|_| ())?;
    Some(entry)
}

pub(super) fn copy_thread_jsonl_with_cow_and_localization(
    source: &Path,
    target: &Path,
    source_thread_id: &str,
    target_thread_id: &str,
) -> Result<()> {
    copy_file_best_effort_cow(source, target)?;
    localize_thread_jsonl_file(target, source_thread_id, target_thread_id)
}

pub(super) fn copy_path_best_effort_cow(source: &Path, target: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)
        .with_context(|| format!("Failed to inspect {}.", source.display()))?;
    if metadata.file_type().is_symlink() {
        let resolved = existing_symlink_target(source)?
            .ok_or_else(|| anyhow!("Failed to resolve symlink {}.", source.display()))?;
        return copy_path_best_effort_cow(&resolved, target);
    }
    if metadata.is_dir() {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}.", parent.display()))?;
        }
        let temp_path = temporary_copy_path(target);
        remove_path_if_exists(&temp_path)?;
        fs::create_dir_all(&temp_path)
            .with_context(|| format!("Failed to create {}.", temp_path.display()))?;
        if let Err(error) = copy_dir_contents_best_effort_cow(source, &temp_path) {
            remove_path_if_exists(&temp_path).ok();
            return Err(error);
        }
        remove_path_if_exists(target)?;
        return fs::rename(&temp_path, target).with_context(|| {
            format!(
                "Failed to move {} to {}.",
                temp_path.display(),
                target.display()
            )
        });
    }
    copy_file_best_effort_cow(source, target)
}

fn copy_dir_contents_best_effort_cow(source: &Path, target: &Path) -> Result<()> {
    for entry in
        fs::read_dir(source).with_context(|| format!("Failed to read {}.", source.display()))?
    {
        let entry = entry?;
        copy_path_best_effort_cow(&entry.path(), &target.join(entry.file_name()))?;
    }
    Ok(())
}

pub(super) fn copy_file_best_effort_cow(source: &Path, target: &Path) -> Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}.", parent.display()))?;
    }
    let temp_path = temporary_copy_path(target);
    remove_path_if_exists(&temp_path)?;
    let copy_result = copy_file_cow(source, &temp_path).or_else(|_| {
        fs::copy(source, &temp_path).map(|_| ()).with_context(|| {
            format!(
                "Failed to copy {} to {}.",
                source.display(),
                temp_path.display()
            )
        })
    });
    if let Err(error) = copy_result {
        remove_path_if_exists(&temp_path).ok();
        return Err(error);
    }
    remove_path_if_exists(target)?;
    fs::rename(&temp_path, target).with_context(|| {
        format!(
            "Failed to move {} to {}.",
            temp_path.display(),
            target.display()
        )
    })
}

pub(super) fn temporary_copy_path(target: &Path) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let file_name = target
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("thread.jsonl");
    target.with_file_name(format!(
        ".{file_name}.codex-rotate-copy-{}-{nonce}",
        std::process::id()
    ))
}

#[cfg(target_os = "macos")]
pub(super) fn copy_file_cow(source: &Path, target: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    extern "C" {
        fn clonefile(
            src: *const std::os::raw::c_char,
            dst: *const std::os::raw::c_char,
            flags: u32,
        ) -> i32;
    }

    let source = CString::new(source.as_os_str().as_bytes())
        .context("Source path contained an interior NUL byte.")?;
    let target = CString::new(target.as_os_str().as_bytes())
        .context("Target path contained an interior NUL byte.")?;
    let result = unsafe { clonefile(source.as_ptr(), target.as_ptr(), 0) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("clonefile failed")
    }
}

#[cfg(all(target_os = "linux", not(target_os = "macos")))]
pub(super) fn copy_file_cow(source: &Path, target: &Path) -> Result<()> {
    use std::os::fd::AsRawFd;

    const FICLONE: u64 = 0x4004_9409;
    unsafe extern "C" {
        fn ioctl(fd: i32, request: u64, ...) -> i32;
    }

    let source_file =
        fs::File::open(source).with_context(|| format!("Failed to open {}.", source.display()))?;
    let target_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .with_context(|| format!("Failed to create {}.", target.display()))?;
    let result = unsafe { ioctl(target_file.as_raw_fd(), FICLONE, source_file.as_raw_fd()) };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("FICLONE failed")
    }
}

#[cfg(windows)]
pub(super) fn copy_file_cow(source: &Path, target: &Path) -> Result<()> {
    use std::ffi::c_void;
    use std::os::windows::io::AsRawHandle;

    const FSCTL_DUPLICATE_EXTENTS_TO_FILE: u32 = 0x0009_8344;

    #[repr(C)]
    struct DuplicateExtentsData {
        file_handle: *mut c_void,
        source_file_offset: i64,
        target_file_offset: i64,
        byte_count: i64,
    }

    #[link(name = "Kernel32")]
    extern "system" {
        fn DeviceIoControl(
            hDevice: *mut c_void,
            dwIoControlCode: u32,
            lpInBuffer: *mut c_void,
            nInBufferSize: u32,
            lpOutBuffer: *mut c_void,
            nOutBufferSize: u32,
            lpBytesReturned: *mut u32,
            lpOverlapped: *mut c_void,
        ) -> i32;
    }

    let source_file =
        fs::File::open(source).with_context(|| format!("Failed to open {}.", source.display()))?;
    let target_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(target)
        .with_context(|| format!("Failed to create {}.", target.display()))?;
    let size = source_file.metadata()?.len();
    target_file.set_len(size)?;
    if size == 0 {
        return Ok(());
    }
    let mut data = DuplicateExtentsData {
        file_handle: source_file.as_raw_handle() as *mut c_void,
        source_file_offset: 0,
        target_file_offset: 0,
        byte_count: size as i64,
    };
    let mut bytes_returned = 0_u32;
    let ok = unsafe {
        DeviceIoControl(
            target_file.as_raw_handle() as *mut c_void,
            FSCTL_DUPLICATE_EXTENTS_TO_FILE,
            (&mut data as *mut DuplicateExtentsData).cast(),
            std::mem::size_of::<DuplicateExtentsData>() as u32,
            std::ptr::null_mut(),
            0,
            &mut bytes_returned,
            std::ptr::null_mut(),
        )
    };
    if ok != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error()).context("FSCTL_DUPLICATE_EXTENTS_TO_FILE failed")
    }
}

#[cfg(not(any(target_os = "macos", target_os = "linux", windows)))]
pub(super) fn copy_file_cow(_source: &Path, _target: &Path) -> Result<()> {
    Err(anyhow!(
        "copy-on-write clone is unsupported on this platform"
    ))
}

pub(super) fn localize_thread_jsonl_file(
    path: &Path,
    source_thread_id: &str,
    target_thread_id: &str,
) -> Result<()> {
    if source_thread_id == target_thread_id {
        return Ok(());
    }
    if source_thread_id.len() == target_thread_id.len() {
        let bytes =
            fs::read(path).with_context(|| format!("Failed to read {}.", path.display()))?;
        let replacements =
            json_thread_id_string_replacements(&bytes, source_thread_id, target_thread_id);
        if replacements.is_empty() {
            return Ok(());
        }
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| {
                format!(
                    "Failed to open {} for thread ID localization.",
                    path.display()
                )
            })?;
        for (offset, replacement) in replacements {
            file.seek(SeekFrom::Start(offset as u64))?;
            file.write_all(replacement.as_bytes())?;
        }
        return Ok(());
    }

    let contents =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}.", path.display()))?;
    let mut changed = false;
    let mut lines = Vec::new();
    for line in contents.lines() {
        match serde_json::from_str::<Value>(line) {
            Ok(mut value) => {
                if localize_thread_ids_in_json_value(&mut value, source_thread_id, target_thread_id)
                {
                    changed = true;
                    lines.push(serde_json::to_string(&value)?);
                } else {
                    lines.push(line.to_string());
                }
            }
            Err(_) => lines.push(line.to_string()),
        }
    }
    if changed {
        let mut output = lines.join("\n");
        if contents.ends_with('\n') {
            output.push('\n');
        }
        fs::write(path, output).with_context(|| format!("Failed to write {}.", path.display()))?;
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) enum JsonObjectScanMode {
    KeyOrEnd,
    Colon { key: String },
    Value { key: String },
    CommaOrEnd,
}

#[derive(Clone, Debug)]
pub(super) enum JsonScanFrame {
    Object(JsonObjectScanMode),
    Array,
}

pub(super) fn json_thread_id_string_replacements(
    bytes: &[u8],
    source_thread_id: &str,
    target_thread_id: &str,
) -> Vec<(usize, String)> {
    let source_bytes = source_thread_id.as_bytes();
    let mut replacements = Vec::new();
    let mut stack = Vec::<JsonScanFrame>::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'{' => {
                mark_json_container_value_started(&mut stack);
                stack.push(JsonScanFrame::Object(JsonObjectScanMode::KeyOrEnd));
                index += 1;
            }
            b'[' => {
                mark_json_container_value_started(&mut stack);
                stack.push(JsonScanFrame::Array);
                index += 1;
            }
            b'}' | b']' => {
                stack.pop();
                mark_json_scalar_value_consumed(&mut stack);
                index += 1;
            }
            b':' => {
                if let Some(JsonScanFrame::Object(mode)) = stack.last_mut() {
                    if let JsonObjectScanMode::Colon { key } = mode {
                        let key = std::mem::take(key);
                        *mode = JsonObjectScanMode::Value { key };
                    }
                }
                index += 1;
            }
            b',' => {
                if let Some(JsonScanFrame::Object(mode)) = stack.last_mut() {
                    *mode = JsonObjectScanMode::KeyOrEnd;
                }
                index += 1;
            }
            b'"' => {
                let Some((content_start, content_end, next_index)) =
                    json_string_token_bounds(bytes, index)
                else {
                    break;
                };
                let string_value = serde_json::from_slice::<String>(&bytes[index..next_index]).ok();
                if let Some(JsonScanFrame::Object(mode)) = stack.last_mut() {
                    match mode.clone() {
                        JsonObjectScanMode::KeyOrEnd => {
                            *mode = JsonObjectScanMode::Colon {
                                key: string_value.unwrap_or_default(),
                            };
                        }
                        JsonObjectScanMode::Value { key } => {
                            if thread_id_metadata_key(&key)
                                && &bytes[content_start..content_end] == source_bytes
                            {
                                replacements.push((content_start, target_thread_id.to_string()));
                            }
                            *mode = JsonObjectScanMode::CommaOrEnd;
                        }
                        _ => {}
                    }
                }
                index = next_index;
            }
            b'-' | b'0'..=b'9' | b't' | b'f' | b'n' => {
                mark_json_scalar_value_consumed(&mut stack);
                index += 1;
            }
            _ => index += 1,
        }
    }
    replacements
}

pub(super) fn json_string_token_bounds(
    bytes: &[u8],
    start: usize,
) -> Option<(usize, usize, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return None;
    }
    let mut index = start + 1;
    while index < bytes.len() {
        match bytes[index] {
            b'\\' => index = index.saturating_add(2),
            b'"' => return Some((start + 1, index, index + 1)),
            _ => index += 1,
        }
    }
    None
}

pub(super) fn mark_json_container_value_started(stack: &mut [JsonScanFrame]) {
    if let Some(JsonScanFrame::Object(mode @ JsonObjectScanMode::Value { .. })) = stack.last_mut() {
        *mode = JsonObjectScanMode::CommaOrEnd;
    }
}

pub(super) fn mark_json_scalar_value_consumed(stack: &mut [JsonScanFrame]) {
    if let Some(JsonScanFrame::Object(mode @ JsonObjectScanMode::Value { .. })) = stack.last_mut() {
        *mode = JsonObjectScanMode::CommaOrEnd;
    }
}

pub(super) fn localize_thread_ids_in_json_value(
    value: &mut Value,
    source_thread_id: &str,
    target_thread_id: &str,
) -> bool {
    match value {
        Value::Object(object) => {
            let mut changed = false;
            for (key, value) in object.iter_mut() {
                if thread_id_metadata_key(key) && value.as_str() == Some(source_thread_id) {
                    *value = Value::String(target_thread_id.to_string());
                    changed = true;
                    continue;
                }
                changed |=
                    localize_thread_ids_in_json_value(value, source_thread_id, target_thread_id);
            }
            changed
        }
        Value::Array(items) => items.iter_mut().fold(false, |changed, value| {
            localize_thread_ids_in_json_value(value, source_thread_id, target_thread_id) || changed
        }),
        _ => false,
    }
}

pub(super) fn thread_id_metadata_key(key: &str) -> bool {
    THREAD_ID_METADATA_KEYS.contains(&key)
}

#[cfg(test)]
pub(super) fn read_thread_cwds_from_state_db(state_db_path: &Path) -> Result<BTreeSet<String>> {
    let mut cwd_values = BTreeSet::new();
    if !state_db_path.exists() {
        return Ok(cwd_values);
    }

    let connection = rusqlite::Connection::open(state_db_path)
        .with_context(|| format!("Failed to open {}.", state_db_path.display()))?;
    if !sqlite_table_exists(&connection, "threads")? {
        return Ok(cwd_values);
    }
    if !sqlite_table_columns(&connection, "main", "threads")?
        .iter()
        .any(|column| column == "cwd")
    {
        return Ok(cwd_values);
    }

    let mut statement = connection
        .prepare(
            "select distinct cwd from threads \
             where cwd is not null and trim(cwd) != '' \
             order by cwd",
        )
        .with_context(|| {
            format!(
                "Failed to query cwd values from {}.",
                state_db_path.display()
            )
        })?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .with_context(|| {
            format!(
                "Failed to read cwd values from {}.",
                state_db_path.display()
            )
        })?;
    for row in rows {
        cwd_values.insert(
            row.with_context(|| format!("Failed to decode cwd from {}.", state_db_path.display()))?,
        );
    }
    Ok(cwd_values)
}

#[cfg(test)]
pub(super) fn should_sync_project_path(path: &str, known_projects: &BTreeSet<String>) -> bool {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return false;
    }
    if is_excluded_project_registry_path(Path::new(trimmed)) {
        return false;
    }
    if known_projects.contains(trimmed) {
        return true;
    }
    let project_path = Path::new(trimmed);
    if project_path.exists() {
        return true;
    }
    fs::canonicalize(project_path)
        .map(|canonical| !is_excluded_project_registry_path(&canonical))
        .unwrap_or(false)
}

#[cfg(test)]
pub(super) fn normalize_workspace_visibility_path(path: &str) -> Result<Option<String>> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if is_excluded_workspace_visibility_path(Path::new(trimmed)) {
        return Ok(None);
    }

    let mut normalized = match fs::canonicalize(trimmed) {
        Ok(path) => path,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("Failed to canonicalize workspace visibility path {trimmed}.")
            })
        }
    };

    if let Some(git_root) = git_repo_root_for_path(&normalized) {
        normalized = git_root;
    }
    if let Some(main_root) = main_repo_root_for_worktree(&normalized) {
        normalized = main_root;
    }
    if is_excluded_workspace_visibility_path(&normalized) {
        return Ok(None);
    }

    Ok(Some(normalized.to_string_lossy().into_owned()))
}

#[cfg(test)]
pub(super) fn is_excluded_workspace_visibility_path(path: &Path) -> bool {
    if path
        .components()
        .any(|component| component.as_os_str() == ".live-host-env")
    {
        return true;
    }

    let mut excluded_prefixes = vec![
        PathBuf::from("/tmp"),
        PathBuf::from("/private/tmp"),
        PathBuf::from("/var/folders"),
        PathBuf::from("/private/var/folders"),
    ];
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        excluded_prefixes.push(home.join(".codex"));
        excluded_prefixes.push(home.join(".codex-rotate"));
        excluded_prefixes.push(home.join("Documents"));
        excluded_prefixes.push(home.join("Downloads"));
    }

    excluded_prefixes
        .iter()
        .any(|prefix| path == prefix || path.starts_with(prefix))
}

#[cfg(test)]
pub(super) fn is_excluded_project_registry_path(path: &Path) -> bool {
    if path
        .components()
        .any(|component| component.as_os_str() == ".live-host-env")
    {
        return true;
    }

    let mut excluded_prefixes = Vec::new();
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        excluded_prefixes.push(home.join(".codex"));
        excluded_prefixes.push(home.join(".codex-rotate"));
        excluded_prefixes.push(home.join("Documents"));
        excluded_prefixes.push(home.join("Downloads"));
    }

    excluded_prefixes
        .iter()
        .any(|prefix| path == prefix || path.starts_with(prefix))
}

#[cfg(test)]
pub(super) fn main_repo_root_for_worktree(path: &Path) -> Option<PathBuf> {
    let top_level = git_repo_root_for_path(path)?;
    let common_dir = git_common_dir_for_path(&top_level)?;
    let main_root = common_dir.parent()?.canonicalize().ok()?;
    (main_root != top_level).then_some(main_root)
}

#[cfg(test)]
pub(super) fn git_repo_root_for_path(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .current_dir(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let root = String::from_utf8(output.stdout).ok()?;
    let trimmed = root.trim();
    if trimmed.is_empty() {
        return None;
    }
    PathBuf::from(trimmed).canonicalize().ok()
}

#[cfg(test)]
pub(super) fn git_common_dir_for_path(path: &Path) -> Option<PathBuf> {
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--path-format=absolute")
        .arg("--git-common-dir")
        .current_dir(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let common_dir = String::from_utf8(output.stdout).ok()?;
    let trimmed = common_dir.trim();
    if trimmed.is_empty() {
        return None;
    }
    PathBuf::from(trimmed).canonicalize().ok()
}

#[cfg(test)]
pub(super) fn encode_toml_basic_string(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\u{0008}' => escaped.push_str("\\b"),
            '\t' => escaped.push_str("\\t"),
            '\n' => escaped.push_str("\\n"),
            '\u{000C}' => escaped.push_str("\\f"),
            '\r' => escaped.push_str("\\r"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

pub(super) fn resolve_state_db_file_in_codex_home(codex_home: &Path) -> Option<PathBuf> {
    let entries = fs::read_dir(codex_home).ok()?;
    let mut best_versioned = None::<(u32, PathBuf)>;
    let mut fallback_unversioned = None::<PathBuf>;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(version) = parse_versioned_state_db_name(&name) {
            let should_replace = match best_versioned.as_ref() {
                Some((current_version, _)) => version > *current_version,
                None => true,
            };
            if should_replace {
                best_versioned = Some((version, path));
            }
            continue;
        }
        if name == "state.sqlite" {
            fallback_unversioned = Some(path);
        }
    }
    best_versioned
        .map(|(_, path)| path)
        .or(fallback_unversioned)
}

pub(super) fn parse_versioned_state_db_name(name: &str) -> Option<u32> {
    if !name.starts_with("state_") || !name.ends_with(".sqlite") {
        return None;
    }
    let version = &name["state_".len()..name.len() - ".sqlite".len()];
    version.parse::<u32>().ok()
}

pub(super) fn sqlite_table_exists(
    connection: &rusqlite::Connection,
    table_name: &str,
) -> Result<bool> {
    sqlite_table_exists_in_schema(connection, "main", table_name)
}

pub(super) fn sqlite_table_exists_in_schema(
    connection: &rusqlite::Connection,
    schema: &str,
    table_name: &str,
) -> Result<bool> {
    let sql = format!(
        "select 1 from {}.sqlite_master where type = 'table' and name = ?1 limit 1",
        quote_sql_identifier(schema)
    );
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query([table_name])?;
    Ok(rows.next()?.is_some())
}

pub(super) fn sqlite_table_columns(
    connection: &rusqlite::Connection,
    schema: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let sql = format!(
        "PRAGMA {}.table_info({})",
        quote_sql_identifier(schema),
        quote_sql_identifier(table_name)
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

pub(super) fn quote_sql_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
