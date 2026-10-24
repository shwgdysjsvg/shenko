//! Versioned instance identity storage — SPEC §6, R117.
//!
//! Every revision is kept; the latest one is current. A revision can only be
//! appended as exactly `current + 1`, so two concurrent editors cannot both
//! claim the same successor and no change silently overwrites another.
use rusqlite::{Connection, OptionalExtension, params};

use shenko_types::error::ShenkoError;
use shenko_types::identity::{IdentityProfile, IdentityReceipt};
use shenko_types::ids::EntityId;

use crate::store::Store;
use rusqlite::TransactionBehavior;

pub(crate) const IDENTITY_SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS identity_profiles (
    agent_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    profile TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, revision)
);
CREATE TABLE IF NOT EXISTS subject_identity_receipts (
    step_id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    commit_seq INTEGER NOT NULL
);
";

impl Store {
    /// Current identity revision of `agent_id`, if any was recorded.
    pub fn latest_identity(
        &self,
        agent_id: EntityId,
    ) -> Result<Option<IdentityProfile>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        read_latest(&conn, agent_id)
    }

    /// All revisions, oldest first — the traceable history of name and soul.
    pub fn identity_history(
        &self,
        agent_id: EntityId,
    ) -> Result<Vec<IdentityProfile>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let mut stmt = conn
            .prepare("SELECT profile FROM identity_profiles WHERE agent_id = ?1 ORDER BY revision")
            .map_err(db)?;
        let rows = stmt
            .query_map(params![agent_id.to_string()], |r| r.get::<_, String>(0))
            .map_err(db)?;
        rows.map(|row| decode(&row.map_err(db)?)).collect()
    }

    /// Initialize from an explicit prepared genesis; never invent policy in storage.
    pub fn initialize_identity(
        &self,
        genesis: &IdentityProfile,
    ) -> Result<IdentityProfile, ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;
        if let Some(current) = read_latest(&tx, genesis.agent_id)? {
            return Ok(current);
        }
        if genesis.revision != 1 {
            return Err(ShenkoError::conflict("identity genesis must be revision 1"));
        }
        insert(&tx, genesis)?;
        tx.commit().map_err(db)?;
        Ok(genesis.clone())
    }

    /// The editor must supply the revision it actually read. No read-latest rebasing.
    pub fn append_identity(
        &self,
        next: &IdentityProfile,
        expected_revision: u64,
    ) -> Result<(), ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db)?;
        let actual = read_latest(&tx, next.agent_id)?.map(|p| p.revision);
        if actual != Some(expected_revision) || next.revision != expected_revision + 1 {
            return Err(shenko_types::error::ProtocolError::new(
                "IDENTITY_REVISION_CONFLICT",
                shenko_types::error::ErrorScope::Call,
                shenko_types::error::RetryStrategy::NewRevision,
            )
            .with_details(
                serde_json::json!({"expected_revision":expected_revision,"actual_revision":actual}),
            )
            .into());
        }
        insert(&tx, next)?;
        tx.commit().map_err(db)
    }

    pub fn identity_receipt(
        &self,
        step_id: EntityId,
    ) -> Result<Option<IdentityReceipt>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let data: Option<String> = conn
            .query_row(
                "SELECT data FROM subject_identity_receipts WHERE step_id=?1",
                [step_id.to_string()],
                |r| r.get(0),
            )
            .optional()
            .map_err(db)?;
        data.map(|data| serde_json::from_str(&data).map_err(ShenkoError::from))
            .transpose()
    }
}

pub(crate) fn read_latest(
    conn: &Connection,
    agent_id: EntityId,
) -> Result<Option<IdentityProfile>, ShenkoError> {
    conn.query_row(
        "SELECT profile FROM identity_profiles WHERE agent_id = ?1 ORDER BY revision DESC LIMIT 1",
        params![agent_id.to_string()],
        |r| r.get::<_, String>(0),
    )
    .optional()
    .map_err(db)?
    .map(|json| decode(&json))
    .transpose()
}

/// The primary key rejects a second writer of the same revision.
pub(crate) fn insert(conn: &Connection, profile: &IdentityProfile) -> Result<(), ShenkoError> {
    let json = serde_json::to_string(profile).map_err(|e| ShenkoError::storage(e.to_string()))?;
    conn.execute(
        "INSERT INTO identity_profiles (agent_id, revision, profile, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![
            profile.agent_id.to_string(),
            profile.revision as i64,
            json,
            profile.created_at.to_rfc3339(),
        ],
    )
    .map_err(|e| match e {
        rusqlite::Error::SqliteFailure(f, _) if f.code == rusqlite::ErrorCode::ConstraintViolation => {
            ShenkoError::conflict(format!("identity revision {} already exists", profile.revision))
        }
        other => db(other),
    })?;
    Ok(())
}

fn decode(json: &str) -> Result<IdentityProfile, ShenkoError> {
    serde_json::from_str(json)
        .map_err(|e| ShenkoError::storage(format!("invalid identity profile: {e}")))
}

fn db(error: rusqlite::Error) -> ShenkoError {
    ShenkoError::storage(error.to_string())
}
