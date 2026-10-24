//! SQLite migrations for Shenko store.
use anyhow::Result;
use rusqlite::Connection;

pub fn run_migrations(conn: &Connection) -> Result<()> {
    // Step 1 (v1): base schema, incl. the legacy `memory_entries` table.
    conn.execute_batch(SCHEMA_V1)?;
    // 加法迁移不改写已有历史；旧重复步会显式阻止迁移，不能静默选赢家。
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS ledger_identity (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), ledger_id TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_commit_sequence ON commit_log(seq);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_subject_committed_step
            ON subject_steps(branch_id, step_no) WHERE state='committed';
        CREATE TABLE IF NOT EXISTS subject_event_dispositions (
            branch_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            step_id TEXT NOT NULL REFERENCES subject_steps(step_id),
            disposition TEXT NOT NULL,
            commit_seq INTEGER NOT NULL,
            PRIMARY KEY (branch_id, event_id)
        );
    ",
    )?;

    // Step 2 (memory v2): intent persistence plus the columns that the
    // memory store append/search path needs. `memory_entries` predates this
    // step with a narrower shape, so we extend it additively instead of
    // recreating it (that would discard already persisted memories).
    conn.execute_batch(MEMORY_INTENTS_SCHEMA)?;
    ensure_memory_columns(conn)?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_memory_scope ON memory_entries(scope, created_at_ms);",
    )?;

    // Versioned instance identity (SPEC §6): additive, never rewrites rows.
    conn.execute_batch(crate::identity::IDENTITY_SCHEMA)?;

    // Step 3 (best effort): FTS5 index + sync triggers. FTS5 is optional in
    // the SQLite build; when it is missing, migrations must still succeed and
    // searches fall back to a LIKE scan.
    if let Err(e) = apply_memory_fts(conn) {
        tracing::warn!(error = %e, "FTS5 unavailable; memory search will use LIKE fallback");
    }
    conn.execute_batch(crate::intents::SCHEMA)?;
    Ok(())
}

/// Add memory-v2 columns to an existing `memory_entries` table.
/// SQLite has no `ADD COLUMN IF NOT EXISTS`, so each column is checked first.
fn ensure_memory_columns(conn: &Connection) -> Result<()> {
    ensure_column(conn, "memory_entries", "source_ref", "TEXT")?;
    ensure_column(
        conn,
        "memory_entries",
        "scope",
        "TEXT NOT NULL DEFAULT 'personal'",
    )?;
    ensure_column(
        conn,
        "memory_entries",
        "record_revision",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        conn,
        "memory_entries",
        "created_at_ms",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(conn, "memory_entries", "updated_commit_seq", "INTEGER")?;

    // Backfill millisecond timestamps for rows written by the legacy schema.
    conn.execute_batch(
        "UPDATE memory_entries
            SET created_at_ms = CAST(strftime('%s', created_at) AS INTEGER) * 1000
          WHERE created_at_ms = 0 AND created_at IS NOT NULL;",
    )?;
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, decl: &str) -> Result<()> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if !columns.iter().any(|c| c == column) {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {decl};"))?;
    }
    Ok(())
}

/// Create the FTS5 index and its sync triggers, rebuilding when the index is
/// new or fails its external-content integrity check against `memory_entries`.
///
/// CJK limitation (honest note): the `unicode61` tokenizer splits on
/// non-alphanumeric characters, and a run of Han characters with no spaces is
/// a single token. A Chinese substring such as "深刻" therefore does not match
/// the FTS token for "我的深刻记忆"; `search_memory` routes CJK queries to the
/// LIKE path below, which matches substrings and is exercised by the tests.
fn apply_memory_fts(conn: &Connection) -> Result<()> {
    let existed: i64 = conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='memory_entries_fts'",
        [],
        |r| r.get(0),
    )?;
    conn.execute_batch(FTS_SCHEMA)?;

    // Rebuild when the index is new, or when its external-content integrity
    // check fails. A crash between table creation and the first rebuild (or a
    // table created by an older build without sync triggers) would otherwise
    // leave committed memories missing from every FTS search forever;
    // reopening the store repairs the index. A plain `count(*)` cannot detect
    // this: for an external-content table it scans `memory_entries` itself.
    let stale = existed != 0 && fts_index_is_stale(conn);
    if existed == 0 || stale {
        conn.execute_batch(
            "INSERT INTO memory_entries_fts(memory_entries_fts) VALUES('rebuild');",
        )?;
    }
    Ok(())
}

/// Ask FTS5 to compare its index with the external `memory_entries` content.
/// The `rank` argument `1` selects the external-content consistency check; it
/// fails when the index is missing rows the content table has (or vice versa).
fn fts_index_is_stale(conn: &Connection) -> bool {
    conn.execute(
        "INSERT INTO memory_entries_fts(memory_entries_fts, rank) VALUES('integrity-check', 1)",
        [],
    )
    .is_err()
}

const SCHEMA_V1: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

INSERT OR IGNORE INTO schema_version (version) VALUES (1);

CREATE TABLE IF NOT EXISTS commit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ledger_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    incarnation TEXT NOT NULL,
    entity_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (ledger_id, seq)
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    record_revision INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS operations (
    operation_id TEXT PRIMARY KEY,
    record_revision INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS evidence_receipts (
    receipt_id TEXT PRIMARY KEY,
    operation_id TEXT,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS agent_profiles (
    agent_id TEXT PRIMARY KEY,
    data TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS raw_sessions (
    session_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_events (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES raw_sessions(session_id),
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    role TEXT NOT NULL,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (session_id, seq)
);

CREATE TABLE IF NOT EXISTS memory_entries (
    entry_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    content TEXT NOT NULL,
    source TEXT,
    applicability TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    deleted INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_memory_agent ON memory_entries(agent_id, deleted);
CREATE INDEX IF NOT EXISTS idx_memory_kind ON memory_entries(kind);
CREATE INDEX IF NOT EXISTS idx_session_events_session ON session_events(session_id, seq);
CREATE INDEX IF NOT EXISTS idx_tasks_state ON tasks(task_id);
CREATE INDEX IF NOT EXISTS idx_ops_task ON operations(operation_id);

CREATE TABLE IF NOT EXISTS subject_branch_heads (
    agent_id TEXT PRIMARY KEY,
    branch_id TEXT NOT NULL,
    committed_step_no INTEGER NOT NULL DEFAULT 0,
    committed_step_id TEXT,
    next_step_id TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'active',
    record_revision INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subject_steps (
    step_id TEXT PRIMARY KEY,
    branch_id TEXT NOT NULL,
    step_no INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'planned',
    record_revision INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS subject_genesis (
    genesis_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL UNIQUE,
    bootstrap_id TEXT NOT NULL,
    data TEXT NOT NULL,
    created_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS bootstrap_records (
    bootstrap_id TEXT PRIMARY KEY,
    installation_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'preparing',
    data TEXT NOT NULL,
    updated_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS inbox_events (
    event_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    source TEXT NOT NULL,
    consumed INTEGER NOT NULL DEFAULT 0,
    data TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_inbox_agent ON inbox_events(agent_id, consumed, created_at);
"#;

/// Memory-v2 tables: durable intent queue (§12.2) and supporting indexes.
/// Never contains FTS statements, so this step always succeeds.
const MEMORY_INTENTS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS memory_intents (
    intent_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'personal',
    content TEXT NOT NULL DEFAULT '',
    content_ref TEXT,
    entry_id TEXT,
    state TEXT NOT NULL DEFAULT 'accepted',
    reason TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memory_intents_state ON memory_intents(state, created_at_ms);

INSERT OR IGNORE INTO schema_version (version) VALUES (2);
"#;

/// FTS5 index over memory content. Creation is best-effort: some SQLite builds
/// (and the bundled feature in some configurations) omit FTS5.
const FTS_SCHEMA: &str = r#"
CREATE VIRTUAL TABLE IF NOT EXISTS memory_entries_fts USING fts5(
    content,
    content='memory_entries',
    content_rowid='rowid',
    tokenize='unicode61'
);
CREATE TRIGGER IF NOT EXISTS memory_entries_ai AFTER INSERT ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(rowid, content) VALUES (new.rowid, new.content);
END;
CREATE TRIGGER IF NOT EXISTS memory_entries_ad AFTER DELETE ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(memory_entries_fts, rowid, content)
        VALUES ('delete', old.rowid, old.content);
END;
CREATE TRIGGER IF NOT EXISTS memory_entries_au AFTER UPDATE ON memory_entries BEGIN
    INSERT INTO memory_entries_fts(memory_entries_fts, rowid, content)
        VALUES ('delete', old.rowid, old.content);
    INSERT INTO memory_entries_fts(rowid, content) VALUES (new.rowid, new.content);
END;
"#;
