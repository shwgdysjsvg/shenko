//! Ledger — single mutation line per local store. SPEC §41.2.1
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::sync::Arc;
use tracing::instrument;

use shenko_types::commit::{CommitReceipt, CommitRef, DurabilityProfile, UpdatedEntity};
use shenko_types::error::ShenkoError;
use shenko_types::ids::{EntityId, LedgerSeq};
use shenko_types::store::WriteBatch;

pub struct Ledger {
    pub(crate) conn: Arc<Mutex<Connection>>,
    incarnation: EntityId,
    ledger_id: EntityId,
    mutation_open: Mutex<bool>,
}

impl Ledger {
    pub fn new(conn: Arc<Mutex<Connection>>, incarnation: EntityId) -> Result<Self> {
        let ledger_id = {
            let mut c = conn.lock();
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // 旧库沿用第一条真实提交的身份；新库在首笔业务写入前固定身份。
            let previous: Option<String> = tx
                .query_row(
                    "SELECT ledger_id FROM commit_log ORDER BY id LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .optional()?;
            let candidate = previous.unwrap_or_else(|| EntityId::new().to_string());
            tx.execute(
                "INSERT OR IGNORE INTO ledger_identity (singleton, ledger_id) VALUES (1, ?1)",
                [&candidate],
            )?;
            let id: String = tx.query_row(
                "SELECT ledger_id FROM ledger_identity WHERE singleton=1",
                [],
                |r| r.get(0),
            )?;
            let id = id.parse::<EntityId>()?;
            tx.commit()?;
            id
        };
        Ok(Self {
            conn,
            incarnation,
            ledger_id,
            mutation_open: Mutex::new(true),
        })
    }

    // 必须在同一 IMMEDIATE 写事务内调用，不能先分配再等待写锁。
    pub(crate) fn allocate_seq(
        &self,
        tx: &rusqlite::Transaction<'_>,
    ) -> Result<LedgerSeq, ShenkoError> {
        let seq: i64 = tx
            .query_row(
                "SELECT COALESCE(MAX(seq), 0) + 1 FROM commit_log",
                [],
                |r| r.get(0),
            )
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        Ok(LedgerSeq(seq as u64))
    }

    #[instrument(skip(self, batch))]
    pub fn commit(&self, batch: WriteBatch) -> Result<CommitReceipt, ShenkoError> {
        self.with_mutation(|| self.try_commit(batch), |_| true)
    }

    fn try_commit(&self, batch: WriteBatch) -> Result<CommitReceipt, ShenkoError> {
        let mut conn = self.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        let seq = self.allocate_seq(&tx)?;
        let commit_ref = CommitRef::new(self.ledger_id, seq);
        let mut updated = vec![];

        // 本次只统一序号/写锁；下列通用 upsert 仍缺业务 revision CAS 和 writer 失效协议。
        for task in &batch.task_upserts {
            let json =
                serde_json::to_string(task).map_err(|e| ShenkoError::storage(e.to_string()))?;
            tx.execute(
                "INSERT OR REPLACE INTO tasks (task_id, record_revision, data, updated_commit_seq)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    task.task_id.to_string(),
                    task.record_revision.0 as i64,
                    json,
                    seq.0 as i64,
                ],
            )
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
            updated.push(UpdatedEntity {
                entity_id: task.task_id,
                record_revision: task.record_revision,
            });
        }

        for op in &batch.operation_upserts {
            let json =
                serde_json::to_string(op).map_err(|e| ShenkoError::storage(e.to_string()))?;
            tx.execute(
                "INSERT OR REPLACE INTO operations (operation_id, record_revision, data, updated_commit_seq)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    op.operation_id.to_string(),
                    op.record_revision.0 as i64,
                    json,
                    seq.0 as i64,
                ],
            ).map_err(|e| ShenkoError::storage(e.to_string()))?;
        }

        for att in &batch.attempt_upserts {
            let json =
                serde_json::to_string(att).map_err(|e| ShenkoError::storage(e.to_string()))?;
            tx.execute(
                "INSERT OR REPLACE INTO attempts (attempt_id, operation_id, data, updated_commit_seq)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    att.attempt_id.to_string(),
                    att.operation_id.to_string(),
                    json,
                    seq.0 as i64,
                ],
            ).map_err(|e| ShenkoError::storage(e.to_string()))?;
        }

        for ev in &batch.evidence_upserts {
            let json =
                serde_json::to_string(ev).map_err(|e| ShenkoError::storage(e.to_string()))?;
            tx.execute(
                "INSERT OR REPLACE INTO evidence_receipts (receipt_id, operation_id, data, updated_commit_seq)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    ev.receipt_id.to_string(),
                    ev.operation_id.map(|id| id.to_string()),
                    json,
                    seq.0 as i64,
                ],
            ).map_err(|e| ShenkoError::storage(e.to_string()))?;
        }

        for ie in &batch.inbox_event_upserts {
            tx.execute(
                "INSERT OR IGNORE INTO inbox_events (event_id, agent_id, kind, source, consumed, data)
                 VALUES (?1, ?2, ?3, ?4, 0, ?5)",
                params![
                    ie.event_id.to_string(),
                    ie.agent_id.to_string(),
                    ie.kind,
                    ie.source,
                    ie.payload_json,
                ],
            ).map_err(|e| ShenkoError::storage(e.to_string()))?;
            updated.push(UpdatedEntity {
                entity_id: ie.event_id,
                record_revision: shenko_types::ids::RecordRevision(0),
            });
        }

        for se in &batch.session_event_upserts {
            tx.execute(
                "INSERT OR IGNORE INTO session_events (event_id, session_id, seq, event_type, role, data)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    se.event_id.to_string(),
                    se.session_id.to_string(),
                    se.seq,
                    se.event_type,
                    se.role,
                    se.data,
                ],
            ).map_err(|e| ShenkoError::storage(e.to_string()))?;
            updated.push(UpdatedEntity {
                entity_id: se.event_id,
                record_revision: shenko_types::ids::RecordRevision(0),
            });
        }

        // record commit in ledger log
        tx.execute(
            "INSERT INTO commit_log (ledger_id, seq, incarnation, entity_count)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                self.ledger_id.to_string(),
                seq.0 as i64,
                self.incarnation.to_string(),
                updated.len() as i64,
            ],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;

        tx.commit()
            .map_err(|e| ShenkoError::storage(e.to_string()))?;

        Ok(CommitReceipt {
            commit_ref,
            durability_profile: DurabilityProfile::PowerLossLocal,
            store_incarnation: self.incarnation,
            updated,
        })
    }

    /// One admission line for ordinary commits, subject/memory writes and T1/E1.
    /// Failure is published before queued writers can enter. Pessimistic closure
    /// also leaves the handle failed if a writer unwinds. Never hold over I/O.
    pub(crate) fn with_mutation<T, E: From<ShenkoError>>(
        &self,
        write: impl FnOnce() -> Result<T, E>,
        storage_failure: impl FnOnce(&E) -> bool,
    ) -> Result<T, E> {
        let mut open = self.mutation_open.lock();
        if !*open {
            return Err(
                ShenkoError::storage("writer closed; reopen and query original operation").into(),
            );
        }
        *open = false;
        let result = write();
        if result.as_ref().err().is_none_or(|e| !storage_failure(e)) {
            *open = true;
        }
        result
    }

    pub(crate) fn close_writer(&self) {
        *self.mutation_open.lock() = false;
    }

    pub fn ledger_id(&self) -> EntityId {
        self.ledger_id
    }
}
