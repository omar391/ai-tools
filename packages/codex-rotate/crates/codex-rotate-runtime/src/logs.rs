use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexLogsAvailability {
    MissingDatabase,
    MissingTable,
    Ready,
}

impl CodexLogsAvailability {
    pub fn status_message(self) -> Option<&'static str> {
        match self {
            Self::MissingDatabase => Some("watch waiting for Codex logs database"),
            Self::MissingTable => Some("watch waiting for Codex logs schema"),
            Self::Ready => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CodexSignalKind {
    RateLimitsUpdated,
    UsageLimitReached,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CodexLogSignal {
    pub id: i64,
    pub ts: i64,
    pub kind: CodexSignalKind,
    pub target: String,
    pub body: String,
}

#[derive(Default)]
struct SharedLogReader {
    path: Option<PathBuf>,
    connection: Option<Connection>,
}

pub fn read_codex_signals(
    logs_db_path: &Path,
    after_id: Option<i64>,
    limit: usize,
) -> Result<Vec<CodexLogSignal>> {
    if !logs_db_path.exists() {
        return Ok(Vec::new());
    }
    with_log_connection(logs_db_path, |connection| {
        match query_logs_availability(connection)? {
            CodexLogsAvailability::Ready => query_codex_signals(connection, after_id, limit),
            CodexLogsAvailability::MissingDatabase | CodexLogsAvailability::MissingTable => {
                Ok(Vec::new())
            }
        }
    })
}

pub fn read_latest_codex_signal_id(logs_db_path: &Path) -> Result<Option<i64>> {
    if !logs_db_path.exists() {
        return Ok(None);
    }
    with_log_connection(logs_db_path, |connection| {
        match query_logs_availability(connection)? {
            CodexLogsAvailability::Ready => query_latest_codex_signal_id(connection),
            CodexLogsAvailability::MissingDatabase | CodexLogsAvailability::MissingTable => {
                Ok(None)
            }
        }
    })
}

pub fn codex_logs_availability(logs_db_path: &Path) -> Result<CodexLogsAvailability> {
    if !logs_db_path.exists() {
        return Ok(CodexLogsAvailability::MissingDatabase);
    }
    with_log_connection(logs_db_path, query_logs_availability)
}

fn query_codex_signals(
    connection: &Connection,
    after_id: Option<i64>,
    limit: usize,
) -> Result<Vec<CodexLogSignal>> {
    let mut statement = connection.prepare(
        r#"
select id, ts, target, feedback_log_body
from logs
where id > ?1
  and (
    (target = 'codex_app_server::outgoing_message' and feedback_log_body like 'app-server event: account/rateLimits/updated%')
    or
    (
      target = 'log'
      and feedback_log_body like 'Received message {"type":"error"%'
      and feedback_log_body like '%"type":"usage_limit_reached"%'
      and feedback_log_body like '%"status_code":429%'
    )
  )
order by id asc
limit ?2
        "#,
    )?;
    let rows = statement.query_map(
        params![after_id.unwrap_or(0), (limit.max(1).min(500)) as i64],
        |row| {
            let id: i64 = row.get(0)?;
            let ts: i64 = row.get(1)?;
            let target: String = row.get(2)?;
            let body: String = row.get(3)?;
            Ok((id, ts, target, body))
        },
    )?;

    let mut signals = Vec::new();
    for row in rows {
        let (id, ts, target, body) = row?;
        if let Some(kind) = classify_signal(&target, &body) {
            signals.push(CodexLogSignal {
                id,
                ts,
                kind,
                target,
                body,
            });
        }
    }
    Ok(signals)
}

fn query_latest_codex_signal_id(connection: &Connection) -> Result<Option<i64>> {
    let mut statement = connection.prepare(
        r#"
select id
from logs
where
  (
    (target = 'codex_app_server::outgoing_message' and feedback_log_body like 'app-server event: account/rateLimits/updated%')
    or
    (
      target = 'log'
      and feedback_log_body like 'Received message {"type":"error"%'
      and feedback_log_body like '%"type":"usage_limit_reached"%'
      and feedback_log_body like '%"status_code":429%'
    )
  )
order by id desc
limit 1
        "#,
    )?;
    let mut rows = statement.query([])?;
    if let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        Ok(Some(id))
    } else {
        Ok(None)
    }
}

fn query_logs_availability(connection: &Connection) -> Result<CodexLogsAvailability> {
    if logs_table_exists(connection)? {
        Ok(CodexLogsAvailability::Ready)
    } else {
        Ok(CodexLogsAvailability::MissingTable)
    }
}

fn logs_table_exists(connection: &Connection) -> Result<bool> {
    let mut statement = connection.prepare(
        r#"
select 1
from sqlite_master
where type = 'table'
  and name = 'logs'
limit 1
        "#,
    )?;
    let mut rows = statement.query([])?;
    Ok(rows.next()?.is_some())
}

pub fn classify_signal(target: &str, body: &str) -> Option<CodexSignalKind> {
    if target == "codex_app_server::outgoing_message"
        && body.starts_with("app-server event: account/rateLimits/updated")
    {
        return Some(CodexSignalKind::RateLimitsUpdated);
    }

    if target == "log"
        && body.starts_with("Received message {\"type\":\"error\"")
        && body.contains("\"type\":\"usage_limit_reached\"")
        && body.contains("\"status_code\":429")
    {
        return Some(CodexSignalKind::UsageLimitReached);
    }

    None
}

fn open_logs_connection(logs_db_path: &Path) -> Result<Connection> {
    Connection::open_with_flags(
        logs_db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("Failed to open {}.", logs_db_path.display()))
}

fn with_log_connection<T, F>(logs_db_path: &Path, mut operation: F) -> Result<T>
where
    F: FnMut(&Connection) -> Result<T>,
{
    let mut reader = shared_log_reader().lock().expect("shared log reader mutex");
    let connection = ensure_log_connection(&mut reader, logs_db_path)?;
    match operation(connection) {
        Ok(value) => Ok(value),
        Err(first_error) => {
            reader.connection = None;
            let retry_connection = ensure_log_connection(&mut reader, logs_db_path)?;
            operation(retry_connection).map_err(|retry_error| {
                anyhow::anyhow!(
                    "{retry_error} (initial log query failed before reconnect: {first_error})"
                )
            })
        }
    }
}

fn ensure_log_connection<'a>(
    reader: &'a mut SharedLogReader,
    logs_db_path: &Path,
) -> Result<&'a Connection> {
    if reader.path.as_deref() != Some(logs_db_path) {
        reader.path = Some(logs_db_path.to_path_buf());
        reader.connection = None;
    }
    if reader.connection.is_none() {
        reader.connection = Some(open_logs_connection(logs_db_path)?);
    }
    Ok(reader.connection.as_ref().expect("shared log connection"))
}

fn shared_log_reader() -> &'static Mutex<SharedLogReader> {
    static READER: OnceLock<Mutex<SharedLogReader>> = OnceLock::new();
    READER.get_or_init(|| Mutex::new(SharedLogReader::default()))
}

pub fn invalidate_log_connection(logs_db_path: Option<&Path>) {
    let mut reader = shared_log_reader().lock().expect("shared log reader mutex");
    match logs_db_path {
        Some(path) if reader.path.as_deref() == Some(path) => {
            reader.connection = None;
            reader.path = None;
        }
        Some(_) => {}
        None => {
            reader.connection = None;
            reader.path = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn reports_missing_database_when_logs_db_does_not_exist() {
        let missing = std::env::temp_dir().join(format!(
            "codex-rotate-missing-logs-{}.sqlite",
            std::process::id()
        ));
        std::fs::remove_file(&missing).ok();

        assert_eq!(
            codex_logs_availability(&missing).unwrap(),
            CodexLogsAvailability::MissingDatabase
        );
        assert_eq!(read_codex_signals(&missing, None, 50).unwrap(), Vec::new());
        assert_eq!(read_latest_codex_signal_id(&missing).unwrap(), None);
    }

    #[test]
    fn reports_missing_table_when_logs_db_has_no_logs_schema() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table metadata (
  id integer primary key,
  value text
);
                "#,
            )
            .unwrap();

        assert_eq!(
            codex_logs_availability(file.path()).unwrap(),
            CodexLogsAvailability::MissingTable
        );
        assert_eq!(
            read_codex_signals(file.path(), None, 50).unwrap(),
            Vec::new()
        );
        assert_eq!(read_latest_codex_signal_id(file.path()).unwrap(), None);
    }

    #[test]
    fn filters_only_real_quota_signals() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (1, 1000, 'log', 'Received message {"type":"error","error":{"type":"usage_limit_reached","message":"The usage limit has been reached"},"status_code":429}'),
  (2, 1001, 'codex_app_server::outgoing_message', 'app-server event: account/rateLimits/updated targeted_connections=1'),
  (3, 1002, 'codex_api::endpoint::responses_websocket', 'local tool output mentioning usage_limit_reached but not a real limit event');
                "#,
            )
            .unwrap();

        let signals = read_codex_signals(file.path(), None, 50).unwrap();
        assert_eq!(signals.len(), 2);
        assert_eq!(signals[0].kind, CodexSignalKind::UsageLimitReached);
        assert_eq!(signals[1].kind, CodexSignalKind::RateLimitsUpdated);
    }

    #[test]
    fn shared_reader_still_observes_new_rows() {
        let file = NamedTempFile::new().unwrap();
        let connection = Connection::open(file.path()).unwrap();
        connection
            .execute_batch(
                r#"
create table logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
insert into logs (id, ts, target, feedback_log_body) values
  (1, 1000, 'codex_app_server::outgoing_message', 'app-server event: account/rateLimits/updated targeted_connections=1');
                "#,
            )
            .unwrap();

        let first = read_codex_signals(file.path(), None, 50).unwrap();
        assert_eq!(first.len(), 1);

        connection
            .execute(
                "insert into logs (id, ts, target, feedback_log_body) values (?1, ?2, ?3, ?4)",
                params![
                    2i64,
                    1001i64,
                    "log",
                    "Received message {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\"},\"status_code\":429}"
                ],
            )
            .unwrap();

        let next = read_codex_signals(file.path(), Some(1), 50).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].kind, CodexSignalKind::UsageLimitReached);
    }
}
