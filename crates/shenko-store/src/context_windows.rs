//! Committed context windows — SPEC §12.3.1–12.3.2.
//!
//! The store keeps each scope's window chain opaquely (policy crates own the
//! body format) and enforces the commit transaction: a window is appended
//! only while its expected predecessor is still the head. Derivatives live in
//! the same transaction as their window, so a window never commits without
//! the frames it references. Appends ride the ledger's single mutation line.
use rusqlite::{OptionalExtension, params};

use shenko_types::error::ShenkoError;
use shenko_types::ids::EntityId;

use crate::store::Store;

pub(crate) const CONTEXT_WINDOWS_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS context_windows (
    window_id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    body TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    UNIQUE (scope_id, generation)
);
CREATE TABLE IF NOT EXISTS context_window_derivatives (
    window_id TEXT NOT NULL REFERENCES context_windows(window_id),
    idx INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    blake3 TEXT NOT NULL,
    bytes BLOB NOT NULL,
    PRIMARY KEY (window_id, idx)
);
CREATE TABLE IF NOT EXISTS context_window_scopes (
    scope_id TEXT PRIMARY KEY,
    model_generation INTEGER NOT NULL
);
";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDerivative {
    pub media_type: String,
    pub blake3: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredContextWindow {
    pub window_id: EntityId,
    pub generation: i64,
    pub body: String,
    pub derivatives: Vec<StoredDerivative>,
}

fn storage(e: impl std::fmt::Display) -> ShenkoError {
    ShenkoError::storage(e.to_string())
}

impl Store {
    /// The scope's window chain, oldest first, with derivatives verified
    /// against their recorded blake3.
    pub fn context_windows(&self, scope_id: EntityId) -> Result<Vec<StoredContextWindow>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let mut windows = conn
            .prepare_cached(
                "SELECT window_id, generation, body FROM context_windows
                 WHERE scope_id = ?1 ORDER BY generation",
            )
            .map_err(storage)?
            .query_map(params![scope_id.to_string()], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, String>(2)?))
            })
            .map_err(storage)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(storage)?
            .into_iter()
            .map(|(id, generation, body)| {
                let window_id = id.parse::<EntityId>().map_err(storage)?;
                Ok(StoredContextWindow { window_id, generation, body, derivatives: Vec::new() })
            })
            .collect::<Result<Vec<_>, ShenkoError>>()?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT media_type, blake3, bytes FROM context_window_derivatives
                 WHERE window_id = ?1 ORDER BY idx",
            )
            .map_err(storage)?;
        for window in &mut windows {
            window.derivatives = stmt
                .query_map(params![window.window_id.to_string()], |r| {
                    Ok(StoredDerivative { media_type: r.get(0)?, blake3: r.get(1)?, bytes: r.get(2)? })
                })
                .map_err(storage)?
                .collect::<rusqlite::Result<Vec<_>>>()
                .map_err(storage)?;
            for d in &window.derivatives {
                if blake3::hash(&d.bytes).to_hex().as_str() != d.blake3 {
                    return Err(storage(format!("derivative of window {} fails blake3", window.window_id)));
                }
            }
        }
        Ok(windows)
    }

    /// Append `window` as generation `head + 1` only if the current head is
    /// `expected_head` (None: the chain is empty). Ok(false) means the head
    /// moved and nothing was written. A storage error closes the writer:
    /// reopen and read the chain to learn whether the append landed.
    pub fn append_context_window(
        &self,
        scope_id: EntityId,
        expected_head: Option<EntityId>,
        window: &StoredContextWindow,
    ) -> Result<bool, ShenkoError> {
        self.ledger.with_mutation(
            || {
                let mut conn = self.ledger.conn.lock();
                let tx = conn
                    .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                    .map_err(storage)?;
                let head: Option<(String, i64)> = tx
                    .query_row(
                        "SELECT window_id, generation FROM context_windows
                         WHERE scope_id = ?1 ORDER BY generation DESC LIMIT 1",
                        params![scope_id.to_string()],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .optional()
                    .map_err(storage)?;
                if head.as_ref().map(|(id, _)| id.clone()) != expected_head.map(|id| id.to_string()) {
                    return Ok(false);
                }
                let generation = head.map_or(0, |(_, generation)| generation + 1);
                if generation != window.generation {
                    return Err(storage(format!(
                        "window {} claims generation {} but the chain is at {generation}",
                        window.window_id, window.generation
                    )));
                }
                let seq = self.ledger.allocate_seq(&tx)?;
                tx.execute(
                    "INSERT INTO context_windows (window_id, scope_id, generation, body, commit_seq)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        window.window_id.to_string(),
                        scope_id.to_string(),
                        generation,
                        window.body,
                        seq.0 as i64
                    ],
                )
                .map_err(storage)?;
                for (idx, d) in window.derivatives.iter().enumerate() {
                    tx.execute(
                        "INSERT INTO context_window_derivatives (window_id, idx, media_type, blake3, bytes)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![window.window_id.to_string(), idx as i64, d.media_type, d.blake3, d.bytes],
                    )
                    .map_err(storage)?;
                }
                tx.execute(
                    "INSERT INTO commit_log (ledger_id, seq, incarnation, entity_count)
                     VALUES (?1, ?2, ?3, 1)",
                    params![
                        self.ledger.ledger_id().to_string(),
                        seq.0 as i64,
                        self.capabilities.incarnation.to_string()
                    ],
                )
                .map_err(storage)?;
                tx.commit().map_err(storage)?;
                Ok(true)
            },
            |_| true,
        )
    }

    /// Model/template/tokenizer/encoder binding generation of the scope (0 if unset).
    pub fn context_model_generation(&self, scope_id: EntityId) -> Result<i64, ShenkoError> {
        let conn = self.ledger.conn.lock();
        conn.query_row(
            "SELECT model_generation FROM context_window_scopes WHERE scope_id = ?1",
            params![scope_id.to_string()],
            |r| r.get(0),
        )
        .optional()
        .map(|g| g.unwrap_or(0))
        .map_err(storage)
    }

    pub fn set_context_model_generation(&self, scope_id: EntityId, generation: i64) -> Result<(), ShenkoError> {
        self.ledger.with_mutation(
            || {
                let conn = self.ledger.conn.lock();
                conn.execute(
                    "INSERT INTO context_window_scopes (scope_id, model_generation) VALUES (?1, ?2)
                     ON CONFLICT(scope_id) DO UPDATE SET model_generation = excluded.model_generation",
                    params![scope_id.to_string(), generation],
                )
                .map_err(storage)?;
                Ok(())
            },
            |_| true,
        )
    }
}
