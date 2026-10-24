//! Read-only access to committed raw session events — SPEC §12.1, §12.3.
//! Writes stay on the ledger commit path; this adds no schema or writer.
use rusqlite::{OptionalExtension, Row, params};

use shenko_types::error::ShenkoError;
use shenko_types::ids::EntityId;

use crate::store::Store;

/// One committed `session_events` row, exactly as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredSessionEvent {
    pub event_id: EntityId,
    pub session_id: EntityId,
    pub seq: i64,
    pub event_type: String,
    pub role: String,
    pub data: String,
}

const COLUMNS: &str = "event_id, session_id, seq, event_type, role, data";

fn row(r: &Row<'_>) -> rusqlite::Result<(String, String, i64, String, String, String)> {
    Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?))
}

fn parse(
    (event_id, session_id, seq, event_type, role, data): (String, String, i64, String, String, String),
) -> Result<StoredSessionEvent, ShenkoError> {
    let id = |s: &str| s.parse::<EntityId>().map_err(|e| ShenkoError::storage(format!("bad id {s}: {e}")));
    Ok(StoredSessionEvent { event_id: id(&event_id)?, session_id: id(&session_id)?, seq, event_type, role, data })
}

impl Store {
    /// Events of `session_id` with `from <= seq <= to`, in seq order.
    pub fn session_events_range(
        &self,
        session_id: EntityId,
        from: i64,
        to: i64,
    ) -> Result<Vec<StoredSessionEvent>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let mut stmt = conn
            .prepare_cached(&format!(
                "SELECT {COLUMNS} FROM session_events
                 WHERE session_id = ?1 AND seq BETWEEN ?2 AND ?3 ORDER BY seq"
            ))
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        let rows = stmt
            .query_map(params![session_id.to_string(), from, to], row)
            .map_err(|e| ShenkoError::storage(e.to_string()))?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        drop(stmt);
        drop(conn);
        rows.into_iter().map(parse).collect()
    }

    pub fn session_event(&self, event_id: EntityId) -> Result<Option<StoredSessionEvent>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let found = conn
            .query_row(
                &format!("SELECT {COLUMNS} FROM session_events WHERE event_id = ?1"),
                params![event_id.to_string()],
                row,
            )
            .optional()
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        drop(conn);
        found.map(parse).transpose()
    }

    /// Highest committed seq of `session_id`, if any.
    pub fn session_leaf_seq(&self, session_id: EntityId) -> Result<Option<i64>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        conn.query_row(
            "SELECT MAX(seq) FROM session_events WHERE session_id = ?1",
            params![session_id.to_string()],
            |r| r.get(0),
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))
    }
}
