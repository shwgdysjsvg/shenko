//! Memory persistence and search — SPEC §12.2 (intent → committed) and
//! §12.3 (中文检索 / Chinese retrieval).
//!
//! Two representations live in SQLite:
//! * `memory_intents` — the durable intent queue. An intent is accepted
//!   before any memory entry exists and later transitions to `committed`
//!   (with an `entry_id`) or `failed` (with a reason).
//! * `memory_entries` — the searchable memory records.
//!
//! Search uses FTS5 when the build provides it and a LIKE scan otherwise.
//! Chinese text is routed to LIKE even when FTS5 exists: `unicode61` treats a
//! run of Han characters as one token, so a substring query such as "深刻"
//! does not match the token for "我的深刻记忆". This limitation is deliberate
//! and covered by tests.
use chrono::Utc;
use rusqlite::params;
use serde::{Deserialize, Serialize};

use shenko_types::commit::{CommitReceipt, CommitRef, DurabilityProfile, UpdatedEntity};
use shenko_types::error::ShenkoError;
use shenko_types::ids::{EntityId, RecordRevision};

use crate::store::Store;

const ENTRY_COLUMNS: &str = "m.entry_id, m.agent_id, m.kind, m.content, m.source_ref, \
     m.scope, m.record_revision, m.created_at_ms, m.updated_commit_seq";

/// A persisted memory record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub entry_id: EntityId,
    pub agent_id: EntityId,
    pub kind: String,
    pub content: String,
    pub source_ref: Option<String>,
    pub scope: String,
    pub record_revision: u64,
    pub created_at_ms: i64,
    pub updated_commit_seq: Option<u64>,
}

impl MemoryEntry {
    pub fn new(agent_id: EntityId, content: impl Into<String>) -> Self {
        Self {
            entry_id: EntityId::new(),
            agent_id,
            kind: "note".to_string(),
            content: content.into(),
            source_ref: None,
            scope: "personal".to_string(),
            record_revision: 0,
            created_at_ms: Utc::now().timestamp_millis(),
            updated_commit_seq: None,
        }
    }
}

/// Durable intent row (`state` = accepted | committed | failed).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryIntentRecord {
    pub intent_id: EntityId,
    pub agent_id: EntityId,
    pub kind: String,
    pub scope: String,
    pub content: String,
    pub content_ref: Option<String>,
    pub entry_id: Option<EntityId>,
    pub state: String,
    pub reason: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl MemoryIntentRecord {
    pub fn accepted(
        intent_id: EntityId,
        agent_id: EntityId,
        kind: impl Into<String>,
        scope: impl Into<String>,
        content: impl Into<String>,
        content_ref: Option<String>,
        entry_id: Option<EntityId>,
    ) -> Self {
        let now = Utc::now().timestamp_millis();
        Self {
            intent_id,
            agent_id,
            kind: kind.into(),
            scope: scope.into(),
            content: content.into(),
            content_ref,
            entry_id,
            state: "accepted".to_string(),
            reason: None,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }
}

impl Store {
    /// Insert a memory entry and record the commit in the ledger commit log.
    pub fn insert_memory_entry(&self, entry: &MemoryEntry) -> Result<CommitReceipt, ShenkoError> {
        self.commit_entry(entry, None)
    }

    /// Atomically insert the entry produced by an intent and mark that intent
    /// committed. One ledger commit covers both facts.
    pub fn commit_memory_intent(
        &self,
        entry: &MemoryEntry,
        intent_id: EntityId,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.commit_entry(entry, Some(intent_id))
    }

    fn commit_entry(
        &self,
        entry: &MemoryEntry,
        intent_id: Option<EntityId>,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.ledger
            .with_mutation(|| self.try_commit_entry(entry, intent_id), |_| true)
    }

    fn try_commit_entry(
        &self,
        entry: &MemoryEntry,
        intent_id: Option<EntityId>,
    ) -> Result<CommitReceipt, ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        let seq = self.ledger.allocate_seq(&tx)?;

        Self::write_memory_entry(&tx, entry, intent_id, seq)?;

        tx.execute(
            "INSERT INTO commit_log (ledger_id, seq, incarnation, entity_count)
             VALUES (?1,?2,?3,1)",
            params![
                self.ledger.ledger_id().to_string(),
                seq.0 as i64,
                self.capabilities.incarnation.to_string(),
            ],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;

        tx.commit()
            .map_err(|e| ShenkoError::storage(e.to_string()))?;

        Ok(CommitReceipt {
            commit_ref: CommitRef::new(self.ledger.ledger_id(), seq),
            durability_profile: DurabilityProfile::PowerLossLocal,
            store_incarnation: self.capabilities.incarnation,
            updated: vec![UpdatedEntity {
                entity_id: entry.entry_id,
                record_revision: RecordRevision(entry.record_revision),
            }],
        })
    }

    pub(crate) fn write_memory_entry(
        tx: &rusqlite::Transaction<'_>,
        entry: &MemoryEntry,
        intent_id: Option<EntityId>,
        seq: shenko_types::ids::LedgerSeq,
    ) -> Result<(), ShenkoError> {
        // A true upsert, not INSERT OR REPLACE: REPLACE deletes the conflicting
        // row without firing DELETE triggers (unless recursive_triggers is on),
        // which would leave the old text in the FTS index as a stale orphan.
        // DO UPDATE fires the update trigger and keeps the rowid stable.
        tx.execute(
            "INSERT INTO memory_entries
             (entry_id, agent_id, kind, content, source_ref, scope, record_revision,
              created_at_ms, updated_commit_seq, updated_at, deleted)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,datetime('now'),0)
             ON CONFLICT(entry_id) DO UPDATE SET
                agent_id = excluded.agent_id,
                kind = excluded.kind,
                content = excluded.content,
                source_ref = excluded.source_ref,
                scope = excluded.scope,
                record_revision = excluded.record_revision,
                created_at_ms = excluded.created_at_ms,
                updated_commit_seq = excluded.updated_commit_seq,
                updated_at = datetime('now'),
                deleted = 0",
            params![
                entry.entry_id.to_string(),
                entry.agent_id.to_string(),
                entry.kind,
                entry.content,
                entry.source_ref,
                entry.scope,
                entry.record_revision as i64,
                entry.created_at_ms,
                seq.0 as i64,
            ],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;

        if let Some(intent_id) = intent_id {
            tx.execute(
                "UPDATE memory_intents
                    SET state='committed', reason=NULL, entry_id=?2, updated_at_ms=?3
                  WHERE intent_id=?1",
                params![
                    intent_id.to_string(),
                    entry.entry_id.to_string(),
                    Utc::now().timestamp_millis(),
                ],
            )
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        }

        Ok(())
    }

    /// Persist an accepted intent (state = "accepted") before it is processed.
    pub fn insert_memory_intent(&self, rec: &MemoryIntentRecord) -> Result<(), ShenkoError> {
        self.ledger
            .with_mutation(|| self.try_insert_memory_intent(rec), |_| true)
    }

    fn try_insert_memory_intent(&self, rec: &MemoryIntentRecord) -> Result<(), ShenkoError> {
        let conn = self.ledger.conn.lock();
        conn.execute(
            "INSERT OR REPLACE INTO memory_intents
             (intent_id, agent_id, kind, scope, content, content_ref, entry_id,
              state, reason, created_at_ms, updated_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                rec.intent_id.to_string(),
                rec.agent_id.to_string(),
                rec.kind,
                rec.scope,
                rec.content,
                rec.content_ref,
                rec.entry_id.map(|id| id.to_string()),
                rec.state,
                rec.reason,
                rec.created_at_ms,
                rec.updated_at_ms,
            ],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;
        Ok(())
    }

    /// Current durable state of an intent, if present.
    pub fn memory_intent_state(&self, intent_id: EntityId) -> Result<Option<String>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        match conn.query_row(
            "SELECT state FROM memory_intents WHERE intent_id=?1",
            [intent_id.to_string()],
            |r| r.get::<_, String>(0),
        ) {
            Ok(state) => Ok(Some(state)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(ShenkoError::storage(e.to_string())),
        }
    }

    /// Mark an intent failed with a human-readable reason.
    pub fn fail_memory_intent(&self, intent_id: EntityId, reason: &str) -> Result<(), ShenkoError> {
        self.ledger
            .with_mutation(|| self.try_fail_memory_intent(intent_id, reason), |_| true)
    }

    fn try_fail_memory_intent(&self, intent_id: EntityId, reason: &str) -> Result<(), ShenkoError> {
        let conn = self.ledger.conn.lock();
        conn.execute(
            "UPDATE memory_intents
                SET state='failed', reason=?2, updated_at_ms=?3
              WHERE intent_id=?1",
            params![intent_id.to_string(), reason, Utc::now().timestamp_millis(),],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;
        Ok(())
    }

    /// Search memory content.
    ///
    /// * CJK queries use the LIKE substring path (see module docs).
    /// * Other queries use FTS5 `MATCH` when available; any FTS error falls
    ///   back to LIKE so a build without FTS5 still searches correctly.
    pub fn search_memory(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, ShenkoError> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        if contains_cjk(query) {
            return self.search_memory_like(query, limit);
        }
        match self.search_memory_fts(query, limit) {
            Ok(entries) => Ok(entries),
            Err(e) => {
                tracing::debug!(error = %e, "FTS5 memory search failed; using LIKE fallback");
                self.search_memory_like(query, limit)
            }
        }
    }

    fn search_memory_fts(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, ShenkoError> {
        // Quoting makes user input a phrase and avoids FTS5 query syntax errors.
        let quoted = format!("\"{}\"", query.replace('"', "\"\""));
        let sql = format!(
            "SELECT {ENTRY_COLUMNS}
               FROM memory_entries_fts f
               JOIN memory_entries m ON m.rowid = f.rowid
              WHERE memory_entries_fts MATCH ?1 AND m.deleted = 0
              ORDER BY rank
              LIMIT ?2"
        );
        self.query_entries(&sql, params![quoted, limit as i64])
    }

    fn search_memory_like(
        &self,
        query: &str,
        limit: usize,
    ) -> Result<Vec<MemoryEntry>, ShenkoError> {
        let sql = format!(
            "SELECT {ENTRY_COLUMNS}
               FROM memory_entries m
              WHERE m.deleted = 0 AND m.content LIKE ?1 ESCAPE '\\'
              ORDER BY m.created_at_ms DESC, m.rowid DESC
              LIMIT ?2"
        );
        self.query_entries(&sql, params![like_pattern(query), limit as i64])
    }

    /// Most recent memory entries, newest first.
    pub fn list_memory(&self, limit: usize) -> Result<Vec<MemoryEntry>, ShenkoError> {
        let sql = format!(
            "SELECT {ENTRY_COLUMNS}
               FROM memory_entries m
              WHERE m.deleted = 0
              ORDER BY m.created_at_ms DESC, m.rowid DESC
              LIMIT ?1"
        );
        self.query_entries(&sql, params![limit as i64])
    }

    fn query_entries(
        &self,
        sql: &str,
        params: impl rusqlite::Params,
    ) -> Result<Vec<MemoryEntry>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        let rows = stmt
            .query_map(params, |row| Ok(row_to_components(row)))
            .map_err(|e| ShenkoError::storage(e.to_string()))?;

        let mut entries = Vec::new();
        for row in rows {
            let parts = row.map_err(|e| ShenkoError::storage(e.to_string()))?;
            entries.push(parts.into_entry()?);
        }
        Ok(entries)
    }
}

/// Raw column values, converted to domain types outside the rusqlite closure
/// so invalid ids surface as ShenkoError rather than opaque SQL errors.
struct RawEntry {
    entry_id: String,
    agent_id: String,
    kind: String,
    content: String,
    source_ref: Option<String>,
    scope: String,
    record_revision: i64,
    created_at_ms: i64,
    updated_commit_seq: Option<i64>,
}

fn row_to_components(row: &rusqlite::Row<'_>) -> RawEntry {
    RawEntry {
        entry_id: row.get(0).unwrap_or_default(),
        agent_id: row.get(1).unwrap_or_default(),
        kind: row.get(2).unwrap_or_default(),
        content: row.get(3).unwrap_or_default(),
        source_ref: row.get(4).unwrap_or_default(),
        scope: row.get(5).unwrap_or_default(),
        record_revision: row.get(6).unwrap_or_default(),
        created_at_ms: row.get(7).unwrap_or_default(),
        updated_commit_seq: row.get(8).unwrap_or_default(),
    }
}

impl RawEntry {
    fn into_entry(self) -> Result<MemoryEntry, ShenkoError> {
        Ok(MemoryEntry {
            entry_id: parse_entity(&self.entry_id)?,
            agent_id: parse_entity(&self.agent_id)?,
            kind: self.kind,
            content: self.content,
            source_ref: self.source_ref,
            scope: self.scope,
            record_revision: self.record_revision.max(0) as u64,
            created_at_ms: self.created_at_ms,
            updated_commit_seq: self.updated_commit_seq.map(|v| v.max(0) as u64),
        })
    }
}

fn parse_entity(value: &str) -> Result<EntityId, ShenkoError> {
    value
        .parse()
        .map_err(|_| ShenkoError::storage(format!("invalid stored entity id: {value}")))
}

/// `unicode61` cannot segment Han text, so queries containing CJK code points
/// take the substring-capable LIKE path.
fn contains_cjk(s: &str) -> bool {
    s.chars().any(|c| {
        matches!(
            c as u32,
            0x3400..=0x4DBF   // CJK Extension A
            | 0x4E00..=0x9FFF // CJK Unified Ideographs
            | 0xF900..=0xFAFF // CJK Compatibility Ideographs
            | 0x20000..=0x2FA1F // CJK Extension B+
        )
    })
}

/// Escape LIKE metacharacters so user text matches literally.
fn like_pattern(query: &str) -> String {
    let mut pattern = String::with_capacity(query.len() + 2);
    pattern.push('%');
    for c in query.chars() {
        if matches!(c, '%' | '_' | '\\') {
            pattern.push('\\');
        }
        pattern.push(c);
    }
    pattern.push('%');
    pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_pattern_escapes_wildcards() {
        assert_eq!(like_pattern("100%_x"), "%100\\%\\_x%");
    }

    #[test]
    fn cjk_detection_covers_han_ranges() {
        assert!(contains_cjk("深刻"));
        assert!(!contains_cjk("shenko memory"));
    }

    /// The FTS MATCH path must actually serve ASCII queries, and reopening a
    /// store must repair an index left stale by a crash (SPEC §12.2 index
    /// repair), rather than silently hiding committed memories forever.
    #[test]
    fn fts_index_serves_ascii_and_repairs_on_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "shenko-fts-repair-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("memory.db");
        let cas = dir.join("cas");
        let agent = EntityId::new();
        let lost = EntityId::new();

        {
            let store = Store::open(db.clone(), cas.clone()).unwrap();
            if !store.capabilities.fts5_available {
                let _ = std::fs::remove_dir_all(&dir);
                return; // index repair only applies to FTS5 builds
            }
            let original = MemoryEntry::new(agent, "quick brown fox");
            store.insert_memory_entry(&original).unwrap();
            assert_eq!(store.search_memory_fts("quick", 5).unwrap().len(), 1);

            // Re-saving the same entry_id must reindex: the old text becomes
            // unsearchable and the FTS index stays consistent (upsert fires
            // the update trigger; INSERT OR REPLACE would leave an orphan).
            let mut revised = MemoryEntry::new(agent, "lazy dog sleeps");
            revised.entry_id = original.entry_id;
            revised.record_revision = 1;
            store.insert_memory_entry(&revised).unwrap();
            assert!(store.search_memory_fts("quick", 5).unwrap().is_empty());
            assert_eq!(store.search_memory_fts("lazy", 5).unwrap().len(), 1);
            {
                let conn = store.ledger.conn.lock();
                conn.execute_batch(
                    "INSERT INTO memory_entries_fts(memory_entries_fts, rank)
                     VALUES('integrity-check', 1)",
                )
                .unwrap();
            }

            // Simulate a crash that left the index stale: drop the insert
            // trigger and append a row behind the index's back.
            {
                let conn = store.ledger.conn.lock();
                conn.execute_batch("DROP TRIGGER memory_entries_ai;")
                    .unwrap();
                conn.execute(
                    "INSERT INTO memory_entries
                        (entry_id, agent_id, kind, content, created_at_ms)
                     VALUES (?1, ?2, 'note', 'lost until reopen', 1)",
                    params![lost.to_string(), agent.to_string()],
                )
                .unwrap();
            }
        }

        let store = Store::open(db, cas).unwrap();
        let hits = store.search_memory("lost", 10).unwrap();
        assert_eq!(
            hits.len(),
            1,
            "reopening should rebuild the stale FTS index"
        );
        assert_eq!(hits[0].entry_id, lost);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
