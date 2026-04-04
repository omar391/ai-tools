use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OpenFlags};
use serde::{Deserialize, Serialize};

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

pub fn read_codex_signals(
    logs_db_path: &Path,
    after_id: Option<i64>,
    limit: usize,
) -> Result<Vec<CodexLogSignal>> {
    let connection = open_logs_connection(logs_db_path)?;
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

pub fn read_latest_codex_signal_id(logs_db_path: &Path) -> Result<Option<i64>> {
    let connection = open_logs_connection(logs_db_path)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

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
}
