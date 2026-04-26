use super::*;
use rusqlite::OptionalExtension;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ConversationSyncStore {
    db: rusqlite::Connection,
}

pub(super) enum LineageBindingClaim {
    Existing(String),
    Claimed { claim_token: String },
    Busy,
}

fn make_lineage_claim_token() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{LINEAGE_CLAIM_PREFIX}{}-{nanos}", std::process::id())
}

pub(super) fn is_pending_lineage_claim(local_thread_id: &str) -> bool {
    local_thread_id.starts_with(LINEAGE_CLAIM_PREFIX)
}

fn pending_lineage_claim_is_stale(local_thread_id: &str) -> bool {
    if !is_pending_lineage_claim(local_thread_id) {
        return false;
    }
    let Some((_, timestamp_nanos)) = local_thread_id.rsplit_once('-') else {
        return true;
    };
    let Ok(claim_nanos) = timestamp_nanos.parse::<u128>() else {
        return true;
    };
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    now_nanos.saturating_sub(claim_nanos) >= LINEAGE_CLAIM_STALE_AFTER_NANOS
}

fn encode_watermark(watermark: Option<&str>) -> &str {
    watermark.unwrap_or("")
}

fn decode_watermark(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

impl ConversationSyncStore {
    pub fn new(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut db = rusqlite::Connection::open(path)?;
        Self::migrate(&mut db)?;
        Ok(Self { db })
    }

    pub fn has_account_bindings(&self, account_id: &str) -> Result<bool> {
        let mut stmt = self
            .db
            .prepare("SELECT 1 FROM conversation_bindings WHERE account_id = ?1 LIMIT 1")?;
        let mut rows = stmt.query([account_id])?;
        Ok(rows.next()?.is_some())
    }

    fn migrate(db: &mut rusqlite::Connection) -> Result<()> {
        let tx = db.transaction()?;
        tx.execute(
            "CREATE TABLE IF NOT EXISTS conversation_bindings (
                account_id TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                local_thread_id TEXT NOT NULL,
                PRIMARY KEY (account_id, lineage_id)
            )",
            [],
        )?;
        tx.execute(
            "CREATE TABLE IF NOT EXISTS watermarks (
                account_id TEXT NOT NULL,
                lineage_id TEXT NOT NULL,
                last_synced_turn_id TEXT NOT NULL,
                PRIMARY KEY (account_id, lineage_id)
            )",
            [],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_local_thread_id(
        &self,
        account_id: &str,
        lineage_id: &str,
    ) -> Result<Option<String>> {
        let mut stmt = self.db.prepare(
            "SELECT local_thread_id FROM conversation_bindings WHERE account_id = ?1 AND lineage_id = ?2"
        )?;
        let mut rows = stmt.query([account_id, lineage_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_lineage_id(
        &self,
        account_id: &str,
        local_thread_id: &str,
    ) -> Result<Option<String>> {
        let mut stmt = self.db.prepare(
            "SELECT lineage_id FROM conversation_bindings WHERE account_id = ?1 AND local_thread_id = ?2"
        )?;
        let mut rows = stmt.query([account_id, local_thread_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn bind_local_thread_id(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        local_thread_id: &str,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO conversation_bindings (account_id, lineage_id, local_thread_id) VALUES (?1, ?2, ?3)",
            [account_id, lineage_id, local_thread_id]
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn get_watermark(&self, account_id: &str, lineage_id: &str) -> Result<Option<String>> {
        let mut stmt = self.db.prepare(
            "SELECT last_synced_turn_id FROM watermarks WHERE account_id = ?1 AND lineage_id = ?2",
        )?;
        let mut rows = stmt.query([account_id, lineage_id])?;
        if let Some(row) = rows.next()? {
            let value: String = row.get(0)?;
            Ok(decode_watermark(value))
        } else {
            Ok(None)
        }
    }

    pub fn set_watermark(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        turn_id: Option<&str>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO watermarks (account_id, lineage_id, last_synced_turn_id) VALUES (?1, ?2, ?3)",
            [account_id, lineage_id, encode_watermark(turn_id)]
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn bind_and_update_watermark(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        local_thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "INSERT OR REPLACE INTO conversation_bindings (account_id, lineage_id, local_thread_id) VALUES (?1, ?2, ?3)",
            [account_id, lineage_id, local_thread_id]
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO watermarks (account_id, lineage_id, last_synced_turn_id) VALUES (?1, ?2, ?3)",
            [account_id, lineage_id, encode_watermark(turn_id)]
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(super) fn claim_lineage_binding(
        &mut self,
        account_id: &str,
        lineage_id: &str,
    ) -> Result<LineageBindingClaim> {
        let tx = self.db.transaction()?;
        let existing_local_thread_id =
            Self::query_lineage_local_thread_id(&tx, account_id, lineage_id)?;
        let claim = if let Some(local_thread_id) = existing_local_thread_id {
            if pending_lineage_claim_is_stale(&local_thread_id) {
                let claim_token = make_lineage_claim_token();
                let changed = tx.execute(
                    "UPDATE conversation_bindings
                     SET local_thread_id = ?3
                     WHERE account_id = ?1 AND lineage_id = ?2 AND local_thread_id = ?4",
                    [
                        account_id,
                        lineage_id,
                        claim_token.as_str(),
                        local_thread_id.as_str(),
                    ],
                )?;
                if changed == 1 {
                    LineageBindingClaim::Claimed { claim_token }
                } else {
                    match Self::query_lineage_local_thread_id(&tx, account_id, lineage_id)? {
                        Some(current) if is_pending_lineage_claim(&current) => {
                            LineageBindingClaim::Busy
                        }
                        Some(current) => LineageBindingClaim::Existing(current),
                        None => LineageBindingClaim::Busy,
                    }
                }
            } else if is_pending_lineage_claim(&local_thread_id) {
                LineageBindingClaim::Busy
            } else {
                LineageBindingClaim::Existing(local_thread_id)
            }
        } else {
            let claim_token = make_lineage_claim_token();
            tx.execute(
                "INSERT INTO conversation_bindings (account_id, lineage_id, local_thread_id) VALUES (?1, ?2, ?3)",
                [account_id, lineage_id, claim_token.as_str()],
            )?;
            LineageBindingClaim::Claimed { claim_token }
        };
        tx.commit()?;
        Ok(claim)
    }

    pub(super) fn reclaim_lineage_binding(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        expected_local_thread_id: &str,
    ) -> Result<LineageBindingClaim> {
        let tx = self.db.transaction()?;
        let claim_token = make_lineage_claim_token();
        let changed = tx.execute(
            "UPDATE conversation_bindings
             SET local_thread_id = ?3
             WHERE account_id = ?1 AND lineage_id = ?2 AND local_thread_id = ?4",
            [
                account_id,
                lineage_id,
                claim_token.as_str(),
                expected_local_thread_id,
            ],
        )?;
        let claim = if changed == 1 {
            LineageBindingClaim::Claimed { claim_token }
        } else {
            let existing_local_thread_id =
                Self::query_lineage_local_thread_id(&tx, account_id, lineage_id)?;
            let claim = if let Some(local_thread_id) = existing_local_thread_id {
                if pending_lineage_claim_is_stale(&local_thread_id) {
                    let changed = tx.execute(
                        "UPDATE conversation_bindings
                         SET local_thread_id = ?3
                         WHERE account_id = ?1 AND lineage_id = ?2 AND local_thread_id = ?4",
                        [
                            account_id,
                            lineage_id,
                            claim_token.as_str(),
                            local_thread_id.as_str(),
                        ],
                    )?;
                    if changed == 1 {
                        LineageBindingClaim::Claimed { claim_token }
                    } else {
                        LineageBindingClaim::Busy
                    }
                } else if is_pending_lineage_claim(&local_thread_id) {
                    LineageBindingClaim::Busy
                } else {
                    LineageBindingClaim::Existing(local_thread_id)
                }
            } else {
                tx.execute(
                    "INSERT INTO conversation_bindings (account_id, lineage_id, local_thread_id) VALUES (?1, ?2, ?3)",
                    [account_id, lineage_id, claim_token.as_str()],
                )?;
                LineageBindingClaim::Claimed { claim_token }
            };
            claim
        };
        tx.commit()?;
        Ok(claim)
    }

    fn query_lineage_local_thread_id(
        tx: &rusqlite::Transaction<'_>,
        account_id: &str,
        lineage_id: &str,
    ) -> Result<Option<String>> {
        tx.query_row(
            "SELECT local_thread_id FROM conversation_bindings WHERE account_id = ?1 AND lineage_id = ?2",
            [account_id, lineage_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(Into::into)
    }

    pub(super) fn release_lineage_claim(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        claim_token: &str,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        tx.execute(
            "DELETE FROM conversation_bindings WHERE account_id = ?1 AND lineage_id = ?2 AND local_thread_id = ?3",
            [account_id, lineage_id, claim_token],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub(super) fn finalize_lineage_claim(
        &mut self,
        account_id: &str,
        lineage_id: &str,
        claim_token: &str,
        local_thread_id: &str,
        watermark: Option<&str>,
    ) -> Result<()> {
        let tx = self.db.transaction()?;
        let mut stmt = tx.prepare(
            "SELECT local_thread_id FROM conversation_bindings WHERE account_id = ?1 AND lineage_id = ?2",
        )?;
        let mut rows = stmt.query([account_id, lineage_id])?;
        let Some(row) = rows.next()? else {
            return Err(anyhow!(
                "Missing lineage claim while finalizing {}:{}.",
                account_id,
                lineage_id
            ));
        };
        let current_local_thread_id: String = row.get(0)?;
        if current_local_thread_id != claim_token {
            return Err(anyhow!(
                "Lineage claim for {}:{} was lost to {}.",
                account_id,
                lineage_id,
                current_local_thread_id
            ));
        }
        drop(rows);
        drop(stmt);
        tx.execute(
            "UPDATE conversation_bindings SET local_thread_id = ?3 WHERE account_id = ?1 AND lineage_id = ?2",
            [account_id, lineage_id, local_thread_id],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO watermarks (account_id, lineage_id, last_synced_turn_id) VALUES (?1, ?2, ?3)",
            [account_id, lineage_id, encode_watermark(watermark)],
        )?;
        tx.commit()?;
        Ok(())
    }
}
