use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use codex_rotate_runtime::logs::{
    invalidate_log_connection, read_codex_signals, read_latest_codex_signal_id,
};
use codex_rotate_runtime::paths::resolve_paths;
use codex_rotate_runtime::watch::{
    read_watch_state, run_watch_iteration, write_watch_state, WatchIterationOptions,
    WatchIterationResult, WatchState,
};
use rusqlite::{params, Connection};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WatchSignalRow {
    pub id: i64,
    pub ts: i64,
    pub target: String,
    pub feedback_log_body: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct WatchTriggerHarness;

impl WatchSignalRow {
    pub fn rate_limits_updated(id: i64, ts: i64) -> Self {
        Self {
            id,
            ts,
            target: "codex_app_server::outgoing_message".to_string(),
            feedback_log_body:
                "app-server event: account/rateLimits/updated targeted_connections=1".to_string(),
        }
    }

    pub fn usage_limit_reached(id: i64, ts: i64) -> Self {
        Self {
            id,
            ts,
            target: "log".to_string(),
            feedback_log_body: "Received message {\"type\":\"error\",\"error\":{\"type\":\"usage_limit_reached\",\"message\":\"The usage limit has been reached\"},\"status_code\":429}"
                .to_string(),
        }
    }
}

impl WatchTriggerHarness {
    pub fn new() -> Self {
        Self
    }

    pub fn watch_state_path(&self) -> Result<PathBuf> {
        Ok(resolve_paths()?.watch_state_file)
    }

    pub fn logs_db_path(&self) -> Result<PathBuf> {
        Ok(resolve_paths()?.codex_logs_db_file)
    }

    pub fn read_watch_state(&self) -> Result<WatchState> {
        read_watch_state()
    }

    pub fn write_watch_state(&self, state: &WatchState) -> Result<()> {
        let path = self.watch_state_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        write_watch_state(state)
    }

    pub fn ensure_logs_database(&self) -> Result<PathBuf> {
        let logs_db_path = self.logs_db_path()?;
        if let Some(parent) = logs_db_path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
        }
        let connection = Connection::open(&logs_db_path)
            .with_context(|| format!("open {}", logs_db_path.display()))?;
        connection
            .execute_batch(
                r#"
create table if not exists logs (
  id integer primary key,
  ts integer,
  target text,
  feedback_log_body text
);
                "#,
            )
            .context("ensure logs schema")?;
        Ok(logs_db_path)
    }

    pub fn clear_signals(&self) -> Result<()> {
        let logs_db_path = self.ensure_logs_database()?;
        let connection = Connection::open(&logs_db_path)
            .with_context(|| format!("open {}", logs_db_path.display()))?;
        connection
            .execute("delete from logs", [])
            .context("clear logs rows")?;
        invalidate_log_connection(Some(&logs_db_path));
        Ok(())
    }

    pub fn insert_signal_row(&self, row: &WatchSignalRow) -> Result<()> {
        let logs_db_path = self.ensure_logs_database()?;
        let connection = Connection::open(&logs_db_path)
            .with_context(|| format!("open {}", logs_db_path.display()))?;
        connection
            .execute(
                "insert or replace into logs (id, ts, target, feedback_log_body) values (?1, ?2, ?3, ?4)",
                params![row.id, row.ts, row.target, row.feedback_log_body],
            )
            .context("insert log row")?;
        invalidate_log_connection(Some(&logs_db_path));
        Ok(())
    }

    pub fn insert_rate_limit_signal(&self, id: i64, ts: i64) -> Result<()> {
        self.insert_signal_row(&WatchSignalRow::rate_limits_updated(id, ts))
    }

    pub fn insert_usage_limit_signal(&self, id: i64, ts: i64) -> Result<()> {
        self.insert_signal_row(&WatchSignalRow::usage_limit_reached(id, ts))
    }

    pub fn latest_signal_id(&self) -> Result<Option<i64>> {
        read_latest_codex_signal_id(&self.ensure_logs_database()?)
    }

    pub fn signal_rows(
        &self,
        after_signal_id: Option<i64>,
    ) -> Result<Vec<codex_rotate_runtime::logs::CodexLogSignal>> {
        read_codex_signals(&self.ensure_logs_database()?, after_signal_id, 50)
    }

    pub fn run_iteration(&self, options: WatchIterationOptions) -> Result<WatchIterationResult> {
        run_watch_iteration(options)
    }

    pub fn trigger_now(&self) -> Result<WatchIterationResult> {
        self.run_iteration(WatchIterationOptions {
            port: Some(0),
            after_signal_id: None,
            cooldown_ms: Some(0),
            force_quota_refresh: false,
            progress: None,
        })
    }

    pub fn trigger_with_progress(
        &self,
        port: Option<u16>,
        progress: Option<Arc<dyn Fn(String) + Send + Sync>>,
    ) -> Result<WatchIterationResult> {
        self.run_iteration(WatchIterationOptions {
            port,
            after_signal_id: None,
            cooldown_ms: Some(0),
            force_quota_refresh: false,
            progress,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IsolatedAccountStateFixture, IsolatedHomeFixture};

    fn env_mutex() -> &'static std::sync::Mutex<()> {
        crate::test_environment_mutex()
    }

    #[test]
    fn harness_tracks_signal_rows_and_latest_ids() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let home = IsolatedHomeFixture::new("codex-rotate-watch-trigger")?;
        let _home_guard = home.install();
        let harness = WatchTriggerHarness::new();

        harness.clear_signals()?;
        harness.insert_rate_limit_signal(1, 1_000)?;
        harness.insert_usage_limit_signal(2, 1_001)?;

        assert_eq!(harness.latest_signal_id()?, Some(2));
        assert_eq!(harness.signal_rows(None)?.len(), 2);
        assert_eq!(harness.signal_rows(Some(1))?.len(), 1);
        Ok(())
    }

    #[test]
    fn trigger_now_uses_current_env_without_real_file_churn() -> Result<()> {
        let _guard = env_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let account_state = IsolatedAccountStateFixture::new("codex-rotate-watch-trigger")?;
        let harness = WatchTriggerHarness::new();

        let account_id = account_state.active_account().account_id.clone();
        let mut watch_state = WatchState::default();
        watch_state.set_account_state(
            account_id.clone(),
            codex_rotate_runtime::watch::AccountWatchState {
                quota: Some(codex_rotate_core::quota::CachedQuotaState {
                    account_id: account_id.clone(),
                    fetched_at: "2026-04-17T00:00:00.000Z".to_string(),
                    next_refresh_at: "2099-04-17T00:00:00.000Z".to_string(),
                    summary: "5h 60% left".to_string(),
                    usable: true,
                    blocker: None,
                    primary_quota_left_percent: Some(60),
                    error: None,
                }),
                thread_recovery_backfill_complete: true,
                ..Default::default()
            },
        );
        harness.write_watch_state(&watch_state)?;
        harness.clear_signals()?;

        let result = harness.trigger_now()?;
        assert_eq!(result.current_account_id, account_id);
        assert!(!result.rotated);
        assert_eq!(result.logs_availability.status_message(), None);
        Ok(())
    }
}
