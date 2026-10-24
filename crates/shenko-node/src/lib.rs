//! Connected, credential-controlled native node journey, not an ElectionProvider.
//! Only a pure blake3 child is implemented: no arbitrary external effects, no
//! model/secret synchronization, no competing subject loop or background daemon.
use anyhow::{Context, Result, ensure};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shenko_types::{commit::CommitRef, ids::*, object::*, task::*};
use std::path::Path;

const SCHEMA: u32 = 1; // Record/wire schema; storage migrations are separate.
const STORAGE_SCHEMA: u32 = 2;
const APPLICATION_ID: u32 = 0x53484e4b; // SHNK, not a filename-based adoption rule.
const NODE_TABLES: &str = "
CREATE TABLE meta(k TEXT PRIMARY KEY,v TEXT NOT NULL);
CREATE TABLE commands(id TEXT PRIMARY KEY,seal TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE members(node TEXT PRIMARY KEY,data TEXT NOT NULL);
CREATE TABLE tasks(id TEXT PRIMARY KEY,data TEXT NOT NULL);
CREATE TABLE work(task TEXT NOT NULL,epoch TEXT NOT NULL,seal TEXT NOT NULL,result TEXT,PRIMARY KEY(task,epoch));
CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE NOT NULL,source TEXT NOT NULL,digest TEXT NOT NULL,data TEXT NOT NULL);
CREATE TABLE conflicts(seq INTEGER PRIMARY KEY,id TEXT NOT NULL,data TEXT NOT NULL);
CREATE TABLE results(task TEXT PRIMARY KEY,data TEXT NOT NULL);
CREATE TABLE objects(digest TEXT PRIMARY KEY,bytes BLOB NOT NULL);
CREATE TABLE assignments(task TEXT NOT NULL,epoch TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(task,epoch));";
const RECEIPT_TABLE: &str = "CREATE TABLE receipts(seq INTEGER PRIMARY KEY AUTOINCREMENT,source TEXT NOT NULL,event_id TEXT NOT NULL,digest TEXT NOT NULL,task TEXT NOT NULL,data TEXT NOT NULL,status TEXT NOT NULL,reason TEXT,UNIQUE(source,event_id,digest));";

const KIND: &str = "node.blake3.v1";
fn contract() -> Digest {
    Digest::blake3(b"node.blake3.v1:pure:bytes->blake3:no-external-effects")
}
fn digest<T: Serialize>(v: &T) -> Result<Digest> {
    Ok(Digest::blake3(&serde_json::to_vec(v)?))
}
fn number(s: &str) -> Result<u64> {
    let n = s.parse()?;
    ensure!(format!("{n}") == s, "noncanonical integer");
    Ok(n)
}
fn text<T: Serialize>(v: &T) -> Result<String> {
    Ok(serde_json::to_string(v)?)
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub agent_id: EntityId,
    pub branch_id: EntityId,
    /// Reference to an already verified earlier identity, not a remote claim of age.
    pub origin_proof: Digest,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub source_revision: String,
    pub artifact_digest: Digest,
    pub schema: u32,
    pub os: String,
    pub arch: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ticket {
    pub schema: u32,
    pub task_id: EntityId,
    pub identity: Identity,
    pub node_id: EntityId,
    pub task_revision: String,
    pub record_revision: String,
    pub epoch: String,
    pub invocation_id: EntityId,
    pub input_digest: Digest,
    pub contract_digest: Digest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawEvent {
    pub schema: u32,
    pub event_id: EntityId,
    pub agent_id: EntityId,
    pub branch_id: EntityId,
    pub node_id: EntityId,
    pub epoch: String,
    pub local_seq: String,
    pub causal_parents: Vec<EntityId>,
    pub payload: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Event {
    pub raw: RawEvent,
    pub digest: Digest,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultReceipt {
    pub ticket: Ticket,
    pub output_digest: Digest,
    pub event: Event,
}

/// Restricted to an OS/SSH account with installation control. A future registry
/// adapter must derive this from authenticated IPC, never model tool arguments.
#[derive(Debug, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Initialize {
        node_id: EntityId,
        identity: Option<Identity>,
    },
    Join {
        operation_id: EntityId,
        expected_node: EntityId,
        identity: Identity,
        manifest: Manifest,
    },
    Register {
        node_id: EntityId,
        target_id: String,
        binding_digest: Digest,
    },
    Create {
        task_id: EntityId,
        node_id: EntityId,
        input: Vec<u8>,
    },
    Transfer {
        task_id: EntityId,
        expected_revision: String,
        expected_epoch: String,
        to_node: EntityId,
    },
    Execute {
        ticket: Ticket,
        input: Vec<u8>,
    },
    Query {
        task_id: EntityId,
        epoch: String,
    },
    Accept {
        from_node: EntityId,
        receipt: ResultReceipt,
    },
    Status {
        operation_id: Option<EntityId>,
    },
    Export {
        after: String,
        limit: u32,
    },
    ExportReceipts {
        after: String,
        limit: u32,
    },
}

pub struct Node {
    conn: Connection,
    failed: bool,
}
impl Node {
    pub fn open(dir: &Path) -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
        if !dir.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)?;
        }
        let metadata = std::fs::symlink_metadata(dir)?;
        ensure!(
            metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.permissions().mode() & 0o077 == 0,
            "private nonsymlink data directory required"
        );
        let db = dir.join("node.sqlite3");
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            ensure!(
                [
                    "node.sqlite3",
                    "node.sqlite3-journal",
                    "node.sqlite3-wal",
                    "node.sqlite3-shm"
                ]
                .iter()
                .any(|name| entry.file_name() == *name)
                    && entry.file_type()?.is_file(),
                "non-installation data present; choose a new directory"
            );
        }
        let existing = db.try_exists()?;
        if existing {
            // immutable prevents even a read-only SQLite open from creating WAL
            // sidecars or recovering another application's journal during preflight.
            let readonly = Connection::open_with_flags(
                readonly_uri(&db)?,
                OpenFlags::SQLITE_OPEN_READ_ONLY
                    | OpenFlags::SQLITE_OPEN_URI
                    | OpenFlags::SQLITE_OPEN_NOFOLLOW,
            )?;
            validate_installation(&readonly)?;
        } else {
            ensure!(
                std::fs::read_dir(dir)?.next().is_none(),
                "non-installation data present"
            );
            use std::os::unix::fs::OpenOptionsExt;
            // Never follow a replacement symlink or adopt an existing empty DB.
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&db)?;
        }
        let mut conn = Connection::open_with_flags(
            &db,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        if existing {
            validate_installation(&conn)?;
        }
        // Only our new file or a validated installation may reach persistent PRAGMAs.
        conn.execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let journal: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?;
        let foreign_keys: i64 = conn.query_row("PRAGMA foreign_keys", [], |r| r.get(0))?;
        ensure!(
            journal == "delete" && synchronous == 2 && foreign_keys == 1,
            "node storage profile not adopted"
        );
        let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version != STORAGE_SCHEMA {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if !existing {
                tx.execute_batch(NODE_TABLES)?;
            } else {
                // The exact v1 fingerprint and durable identities were validated
                // read-only. Preserve all existing rows, identities and ledger seq.
                validate_installation(&tx)?;
            }
            tx.execute_batch(RECEIPT_TABLE)?;
            set(&tx, "installation", &EntityId::new())?;
            tx.pragma_update(None, "application_id", APPLICATION_ID)?;
            tx.pragma_update(None, "user_version", STORAGE_SCHEMA)?;
            tx.commit()?;
        }
        Ok(Self {
            conn,
            failed: false,
        })
    }
    pub fn handle(&mut self, request: Request) -> Result<Value> {
        ensure!(
            !self.failed,
            "writer failed; reopen and query stable identity"
        );
        let result = self.handle_inner(request);
        if let Err(e) = &result {
            if e.is::<rusqlite::Error>() || e.is::<serde_json::Error>() || e.is::<std::io::Error>()
            {
                self.failed = true;
            }
        }
        result
    }
    fn handle_inner(&mut self, request: Request) -> Result<Value> {
        match request {
            Request::Initialize { node_id, identity } => {
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                if let Some(old) = meta(&tx, "node")? {
                    ensure!(old == text(&node_id)?, "node identity conflict");
                    // Opening an existing installation is not rebirth/reset.
                } else {
                    set(&tx, "node", &node_id)?;
                    set(&tx, "identity", &identity)?;
                    set(&tx, "retired_identities", &Vec::<Identity>::new())?;
                    set(&tx, "ledger", &EntityId::new())?;
                }
                tx.commit()?;
                self.status(None)
            }
            Request::Join {
                operation_id,
                expected_node,
                identity,
                manifest,
            } => {
                let actual = running_manifest()?;
                let seal = digest(&(expected_node, &identity, &manifest))?;
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                if let Some((old, result)) = tx
                    .query_row(
                        "SELECT seal,result FROM commands WHERE id=?1",
                        [operation_id.to_string()],
                        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                    )
                    .optional()?
                {
                    ensure!(old == seal.0, "join operation payload conflict");
                    return Ok(serde_json::from_str(&result)?);
                }
                ensure!(
                    get::<EntityId>(&tx, "node")? == expected_node,
                    "wrong managed node"
                );
                ensure!(
                    manifest.schema == SCHEMA
                        && manifest.os == std::env::consts::OS
                        && manifest.arch == std::env::consts::ARCH,
                    "incompatible artifact/schema"
                );
                ensure!(
                    manifest.source_revision.len() == 40
                        && manifest
                            .source_revision
                            .bytes()
                            .all(|b| b.is_ascii_hexdigit()),
                    "source revision required"
                );
                ensure!(manifest == actual, "running artifact/source mismatch");
                let old: Option<Identity> = get(&tx, "identity")?;
                if old.as_ref().is_some_and(|v| v != &identity) {
                    let used: i64 = tx.query_row(
                        "SELECT (SELECT count(*) FROM tasks)+(SELECT count(*) FROM work)",
                        [],
                        |r| r.get(0),
                    )?;
                    ensure!(
                        used == 0 && meta(&tx, "joined")?.is_none(),
                        "existing evolved/joined branch requires merge, not new-node adoption"
                    );
                    let mut retired: Vec<Identity> = get(&tx, "retired_identities")?;
                    retired.extend(old);
                    set(&tx, "retired_identities", &retired)?;
                }
                if let Some(joined) = meta(&tx, "joined")? {
                    ensure!(joined == text(&identity)?, "joined identity conflict");
                }
                set(&tx, "identity", &Some(&identity))?;
                set(&tx, "joined", &identity)?;
                set(&tx, "manifest", &manifest)?;
                let result = json!({"operation_id":operation_id,"node_id":expected_node,"identity":identity,"manifest":manifest,"service":"one_shot_worker_no_subject","durability":"process_crash"});
                tx.execute(
                    "INSERT INTO commands VALUES(?1,?2,?3)",
                    params![operation_id.to_string(), seal.0, text(&result)?],
                )?;
                tx.commit()?;
                Ok(result)
            }
            Request::Register {
                node_id,
                target_id,
                binding_digest,
            } => {
                ensure!(
                    !target_id.is_empty() && target_id.len() <= 128,
                    "invalid target ID"
                );
                let value = json!({"node_id":node_id,"target_id":target_id,"binding_digest":binding_digest});
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                let _: Identity = identity(&tx)?;
                let old: Option<String> = tx
                    .query_row(
                        "SELECT data FROM members WHERE node=?1",
                        [node_id.to_string()],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(old) = old {
                    ensure!(old == text(&value)?, "registered target conflict");
                } else {
                    tx.execute(
                        "INSERT INTO members VALUES(?1,?2)",
                        params![node_id.to_string(), text(&value)?],
                    )?;
                }
                tx.commit()?;
                Ok(value)
            }
            Request::Create {
                task_id,
                node_id,
                input,
            } => {
                ensure!(input.len() <= 65536, "task input exceeds 64KiB");
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                let object = seal(&tx, &input, "application/octet-stream", "bytes")?;
                member(&tx, node_id)?;
                let own = identity(&tx)?;
                if let Some(old) = task(&tx, task_id)? {
                    ensure!(
                        old.input_ref.digest == object.digest
                            && old.agent_id == own.agent_id
                            && old.memos.get("creation_node") == Some(&json!(node_id)),
                        "task immutable input conflict"
                    );
                    return Ok(json!({"ticket":ticket(&old,&own)?,"terminal":old.is_terminal()}));
                }
                let commit = commit(&tx)?;
                let mut task = TaskRecord {
                    record_schema_version: 1,
                    task_id,
                    task_revision: TaskRevision(1),
                    record_revision: RecordRevision(1),
                    agent_id: own.agent_id,
                    conversation_id: own.branch_id,
                    owner: TaskOwner::Conversation { id: own.branch_id },
                    background: false,
                    kind: KIND.into(),
                    definition_version: 1,
                    implementation_ref: ImplementationRef {
                        plugin_id: EntityId::nil(),
                        artifact_digest: contract(),
                        registry_revision: 1,
                    },
                    input_ref: object,
                    state: TaskState::Running {
                        checkpoint_ref: None,
                        invocation_id: EntityId::new(),
                        mode: RunMode::Run,
                        epoch: ControllerEpoch(1),
                    },
                    abort_request: None,
                    memos: Default::default(),
                    residual_case_ids: vec![],
                    created_by_step_id: None,
                    dispatch_eligibility: DispatchEligibility::Ready,
                    no_progress_crashes: 0,
                    first_run_at: Some(chrono::Utc::now()),
                    terminal_at: None,
                    created_commit: commit,
                    updated_commit: commit,
                };
                task.memos.insert("execution_node".into(), json!(node_id));
                task.memos.insert("creation_node".into(), json!(node_id));
                let lease = ticket(&task, &own)?;
                assignment(&tx, &lease)?;
                put_task(&tx, &task)?;
                tx.commit()?;
                Ok(json!({"ticket":lease,"terminal":false}))
            }
            Request::Transfer {
                task_id,
                expected_revision,
                expected_epoch,
                to_node,
            } => {
                let tx = self
                    .conn
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                member(&tx, to_node)?;
                let own = identity(&tx)?;
                let mut t = task(&tx, task_id)?.context("task missing")?;
                let old = ticket(&t, &own)?;
                ensure!(
                    !t.is_terminal() && t.kind == KIND && t.abort_request.is_none(),
                    "task not transferable"
                );
                ensure!(
                    old.record_revision == expected_revision && old.epoch == expected_epoch,
                    "stale owner revision/epoch"
                );
                // This contract has no external effects. Unknown old executions
                // may remain, but cannot mutate resources or settle the new task.
                // Never generalize this to shell, deploy, messages or model work.
                let epoch = number(&expected_epoch)?
                    .checked_add(1)
                    .context("epoch exhausted")?;
                t.task_revision = TaskRevision(
                    t.task_revision
                        .0
                        .checked_add(1)
                        .context("task revision exhausted")?,
                );
                t.record_revision = RecordRevision(
                    t.record_revision
                        .0
                        .checked_add(1)
                        .context("record revision exhausted")?,
                );
                t.state = TaskState::Running {
                    checkpoint_ref: None,
                    invocation_id: EntityId::new(),
                    mode: RunMode::Run,
                    epoch: ControllerEpoch(epoch),
                };
                t.memos.insert("execution_node".into(), json!(to_node));
                t.memos.insert("previous_assignment".into(),json!({"ticket":old,"reconciliation":"pure_contract_no_external_effect; old evidence retained"}));
                t.updated_commit = commit(&tx)?;
                let lease = ticket(&t, &own)?;
                assignment(&tx, &lease)?;
                put_task(&tx, &t)?;
                tx.commit()?;
                Ok(json!({"ticket":lease}))
            }
            Request::Execute {
                ticket: lease,
                input,
            } => self.execute(lease, input),
            Request::Query { task_id, epoch } => {
                number(&epoch)?;
                let row: Option<Option<String>> = self
                    .conn
                    .query_row(
                        "SELECT result FROM work WHERE task=?1 AND epoch=?2",
                        params![task_id.to_string(), epoch],
                        |r| r.get(0),
                    )
                    .optional()?;
                Ok(match row {
                    Some(Some(r)) => {
                        json!({"state":"completed","receipt":serde_json::from_str::<Value>(&r)?})
                    }
                    Some(None) => {
                        json!({"state":"unknown","retry":"query_or_explicit_pure_handoff"})
                    }
                    None => json!({"state":"not_found","retry":"no_implicit_dispatch"}),
                })
            }
            Request::Accept { from_node, receipt } => self.accept(from_node, receipt),
            Request::Status { operation_id } => self.status(operation_id),
            Request::Export { after, limit } => {
                ensure!((1..=128).contains(&limit), "export limit 1..128");
                let after = number(&after)?;
                let mut stmt = self
                    .conn
                    .prepare("SELECT seq,data FROM events WHERE seq>?1 ORDER BY seq LIMIT ?2")?;
                let entries = stmt
                    .query_map(params![i64::try_from(after)?, limit], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let cursor = entries
                    .last()
                    .map(|v| v.0.to_string())
                    .unwrap_or(after.to_string());
                let events = entries
                    .iter()
                    .map(|v| serde_json::from_str::<Value>(&v.1))
                    .collect::<serde_json::Result<Vec<_>>>()?;
                Ok(
                    json!({"events":events,"through":cursor,"profile":"process_crash","scope":"native_node_journey_only"}),
                )
            }
            Request::ExportReceipts { after, limit } => {
                ensure!((1..=128).contains(&limit), "export limit 1..128");
                let after = number(&after)?;
                let mut stmt = self.conn.prepare(
                    "SELECT seq,data,status,reason FROM receipts WHERE seq>?1 ORDER BY seq LIMIT ?2",
                )?;
                let rows = stmt
                    .query_map(params![i64::try_from(after)?, limit], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, Option<String>>(3)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let through = rows
                    .last()
                    .map(|r| r.0.to_string())
                    .unwrap_or(after.to_string());
                let receipts = rows.into_iter().map(|(seq, data, status, reason)| {
                    Ok(json!({"seq":seq.to_string(), "receipt":serde_json::from_str::<Value>(&data)?,
                        "status":status, "reason":reason}))
                }).collect::<Result<Vec<_>>>()?;
                Ok(json!({"receipts":receipts,"through":through,"profile":"process_crash"}))
            }
        }
    }
    fn execute(&mut self, lease: Ticket, input: Vec<u8>) -> Result<Value> {
        ensure!(
            lease.schema == SCHEMA
                && lease.contract_digest == contract()
                && input.len() <= 65536
                && Digest::blake3(&input) == lease.input_digest,
            "incompatible or changed task input"
        );
        number(&lease.epoch)?;
        number(&lease.record_revision)?;
        number(&lease.task_revision)?;
        let seal = digest(&lease)?;
        {
            let tx = self
                .conn
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            ensure!(
                get::<EntityId>(&tx, "node")? == lease.node_id && identity(&tx)? == lease.identity,
                "ticket for wrong joined node/identity"
            );
            ensure!(
                meta(&tx, "joined")?.is_some(),
                "worker must join before execution"
            );
            let old: Option<(String, Option<String>)> = tx
                .query_row(
                    "SELECT seal,result FROM work WHERE task=?1 AND epoch=?2",
                    params![lease.task_id.to_string(), lease.epoch],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((old, result)) = old {
                ensure!(old == seal.0, "work identity conflict");
                return Ok(match result {
                    Some(r) => {
                        json!({"state":"completed","receipt":serde_json::from_str::<Value>(&r)?})
                    }
                    None => json!({"state":"unknown","retry":"query_or_explicit_pure_handoff"}),
                });
            }
            let epochs = tx
                .prepare("SELECT epoch FROM work WHERE task=?1")?
                .query_map([lease.task_id.to_string()], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for epoch in epochs {
                ensure!(
                    number(&epoch)? < number(&lease.epoch)?,
                    "stale worker epoch"
                );
            }
            tx.execute(
                "INSERT INTO work VALUES(?1,?2,?3,NULL)",
                params![lease.task_id.to_string(), lease.epoch, seal.0],
            )?;
            tx.commit()?;
        }
        // The only handler: finite deterministic computation, no external effect.
        let output_digest = Digest::blake3(&input);
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let seq: i64 = tx.query_row("SELECT coalesce(max(seq),0)+1 FROM events", [], |r| {
            r.get(0)
        })?;
        let raw = RawEvent {
            schema: SCHEMA,
            event_id: EntityId::new(),
            agent_id: lease.identity.agent_id,
            branch_id: lease.identity.branch_id,
            node_id: lease.node_id,
            epoch: lease.epoch.clone(),
            local_seq: seq.to_string(),
            causal_parents: vec![lease.task_id],
            payload: json!({"kind":KIND,"task_id":lease.task_id,"input_digest":lease.input_digest,"output_digest":output_digest,"invocation_id":lease.invocation_id,"contract_digest":lease.contract_digest}),
        };
        let event = Event {
            digest: digest(&raw)?,
            raw,
        };
        append_event(&tx, &event)?;
        let receipt = ResultReceipt {
            ticket: lease,
            output_digest,
            event,
        };
        tx.execute(
            "UPDATE work SET result=?3 WHERE task=?1 AND epoch=?2",
            params![
                receipt.ticket.task_id.to_string(),
                receipt.ticket.epoch,
                text(&receipt)?
            ],
        )?;
        tx.commit()?;
        Ok(json!({"state":"completed","receipt":receipt}))
    }
    fn accept(&mut self, from: EntityId, receipt: ResultReceipt) -> Result<Value> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        member(&tx, from)?;
        let own = identity(&tx)?;
        let ev = &receipt.event;
        let lease = &receipt.ticket;
        ensure!(
            ev.digest == digest(&ev.raw)? && ev.raw.schema == SCHEMA && lease.schema == SCHEMA,
            "event schema/digest mismatch"
        );
        ensure!(
            from == lease.node_id
                && ev.raw.node_id == from
                && own == lease.identity
                && ev.raw.agent_id == own.agent_id
                && ev.raw.branch_id == own.branch_id
                && ev.raw.epoch == lease.epoch,
            "untrusted result source"
        );
        number(&ev.raw.local_seq)?;
        number(&ev.raw.epoch)?;
        // E1 is independently committed before any task lookup or settlement
        // predicate. A later T2 rejection/storage failure cannot erase the source.
        let fresh = append_event(&tx, ev)?;
        let receipt_digest = digest(&receipt)?;
        tx.execute(
            "INSERT OR IGNORE INTO receipts(source,event_id,digest,task,data,status) VALUES(?1,?2,?3,?4,?5,'pending')",
            params![from.to_string(),ev.raw.event_id.to_string(),receipt_digest.0,
                lease.task_id.to_string(),text(&receipt)?],
        )?;
        let receipt_seq: i64 = tx.query_row(
            "SELECT seq FROM receipts WHERE source=?1 AND event_id=?2 AND digest=?3",
            params![
                from.to_string(),
                ev.raw.event_id.to_string(),
                receipt_digest.0
            ],
            |r| r.get(0),
        )?;
        if fresh == "conflict" {
            return evidence_only(tx, receipt_seq, "invalid", "event_conflict");
        }
        tx.commit()?;

        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(mut t) = task(&tx, lease.task_id)? else {
            return evidence_only(tx, receipt_seq, "unassociated", "task_missing");
        };
        let expected_payload = json!({"kind":KIND,"task_id":lease.task_id,"input_digest":lease.input_digest,"output_digest":receipt.output_digest,"invocation_id":lease.invocation_id,"contract_digest":lease.contract_digest});
        if ev.raw.payload != expected_payload || ev.raw.causal_parents != vec![lease.task_id] {
            return evidence_only(tx, receipt_seq, "invalid", "result_transcript_mismatch");
        }
        // Source facts survive staleness; only the current owner may settle.
        let current = ticket(&t, &own)?;
        let valid = lease.node_id == current.node_id
            && lease.epoch == current.epoch
            && lease.invocation_id == current.invocation_id
            && lease.task_revision == current.task_revision;
        let old: Option<String> = tx
            .query_row(
                "SELECT data FROM results WHERE task=?1",
                [t.task_id.to_string()],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(old) = old {
            let same = digest(&serde_json::from_str::<ResultReceipt>(&old)?)? == receipt_digest;
            return evidence_only(
                tx,
                receipt_seq,
                if same { "settled" } else { "terminal" },
                if same {
                    "already_settled"
                } else {
                    "evidence_only_terminal"
                },
            );
        }
        if !valid || t.abort_request.is_some() {
            return evidence_only(tx, receipt_seq, "stale_owner", "evidence_only_stale_owner");
        }
        if lease.record_revision != current.record_revision {
            return evidence_only(tx, receipt_seq, "stale_owner", "stale_record_revision");
        }
        if t.kind != KIND
            || lease.contract_digest != contract()
            || lease.input_digest != t.input_ref.digest
        {
            return evidence_only(tx, receipt_seq, "invalid", "task_contract_input_conflict");
        }
        let bytes: Vec<u8> = tx.query_row(
            "SELECT bytes FROM objects WHERE digest=?1",
            [&t.input_ref.digest.0],
            |r| r.get(0),
        )?;
        if Digest::blake3(&bytes) != t.input_ref.digest {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stored object corruption",
            )
            .into());
        }
        if receipt.output_digest != Digest::blake3(&bytes) {
            return evidence_only(
                tx,
                receipt_seq,
                "invalid",
                "task_acceptance_predicate_failed",
            );
        }
        let result = seal(
            &tx,
            &serde_json::to_vec(&receipt)?,
            "application/json",
            "node-result",
        )?;
        t.memos
            .insert("settlement_ticket".into(), serde_json::to_value(&current)?);
        t.state = TaskState::Terminal {
            outcome_ref: result,
        };
        t.terminal_at = Some(chrono::Utc::now());
        t.record_revision = RecordRevision(
            t.record_revision
                .0
                .checked_add(1)
                .context("record revision exhausted")?,
        );
        t.updated_commit = commit(&tx)?;
        put_task(&tx, &t)?;
        tx.execute(
            "INSERT INTO results VALUES(?1,?2)",
            params![t.task_id.to_string(), text(&receipt)?],
        )?;
        tx.execute(
            "UPDATE receipts SET status='settled',reason=NULL WHERE seq=?1",
            [receipt_seq],
        )?;
        tx.commit()?;
        Ok(
            json!({"status":"settled","accepted":true,"task_id":t.task_id,"record_revision":t.record_revision.0.to_string()}),
        )
    }
    fn status(&self, op: Option<EntityId>) -> Result<Value> {
        let node: EntityId = serde_json::from_str(&self.conn.query_row(
            "SELECT v FROM meta WHERE k='node'",
            [],
            |r| r.get::<_, String>(0),
        )?)?;
        let command: Option<Value> = if let Some(id) = op {
            self.conn
                .query_row(
                    "SELECT result FROM commands WHERE id=?1",
                    [id.to_string()],
                    |r| r.get::<_, String>(0),
                )
                .optional()?
                .map(|s| serde_json::from_str(&s))
                .transpose()?
        } else {
            None
        };
        let values = self
            .conn
            .prepare("SELECT k,v FROM meta")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let metadata = values
            .into_iter()
            .map(|(k, v)| Ok((k, serde_json::from_str::<Value>(&v)?)))
            .collect::<Result<serde_json::Map<_, _>>>()?;
        let sqlite: String = self
            .conn
            .query_row("SELECT sqlite_version()", [], |r| r.get(0))?;
        Ok(
            json!({"node_id":node,"metadata":metadata,"running_manifest":running_manifest()?,"observed_at":chrono::Utc::now(),"operation":command,"capabilities":{"os":std::env::consts::OS,"arch":std::env::consts::ARCH,"tools":[KIND],"llm":"not_configured_or_probed","smart_node_evidence":false,"service":"one_shot_worker_no_subject"},"storage":{"schema":STORAGE_SCHEMA,"application_id":APPLICATION_ID,"sqlite":sqlite,"journal":"delete","durability":"process_crash","global_quorum":false,"objects":"bounded_inline_same_transaction"}}),
        )
    }
}
// Accept only the exact original schema plus real initialized identities for
// v1 migration. user_version=0 and a filename never confer installation ownership.
fn validate_installation(conn: &Connection) -> Result<u32> {
    let version: u32 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let app: u32 = conn.query_row("PRAGMA application_id", [], |r| r.get(0))?;
    ensure!(
        (version == 1 && app == 0) || (version == STORAGE_SCHEMA && app == APPLICATION_ID),
        "unrecognized node installation/application/schema"
    );
    let expected = Connection::open_in_memory()?;
    expected.execute_batch(NODE_TABLES)?;
    if version == STORAGE_SCHEMA {
        expected.execute_batch(RECEIPT_TABLE)?;
    }
    fn schema(conn: &Connection) -> Result<Vec<(String, String, String)>> {
        Ok(conn.prepare("SELECT type,name,sql FROM sqlite_master WHERE name NOT GLOB 'sqlite_*' ORDER BY type,name")?
            .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }
    ensure!(
        schema(conn)? == schema(&expected)?,
        "unrecognized installation schema fingerprint"
    );
    fn value<T: for<'a> Deserialize<'a>>(conn: &Connection, key: &str) -> Result<T> {
        let s: String = conn.query_row("SELECT v FROM meta WHERE k=?1", [key], |r| r.get(0))?;
        Ok(serde_json::from_str(&s)?)
    }
    if version == STORAGE_SCHEMA {
        let _: EntityId = value(conn, "installation")?;
    } else {
        let _: EntityId = value(conn, "node")?;
        let _: EntityId = value(conn, "ledger")?;
        let _: Option<Identity> = value(conn, "identity")?;
        let _: Vec<Identity> = value(conn, "retired_identities")?;
    }
    Ok(version)
}
fn readonly_uri(db: &Path) -> Result<String> {
    use std::os::unix::ffi::OsStrExt;
    let db = if db.is_absolute() {
        db.to_path_buf()
    } else {
        std::env::current_dir()?.join(db)
    };
    let encoded: String = db
        .as_os_str()
        .as_bytes()
        .iter()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"/-_.".contains(b) {
                (*b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect();
    Ok(format!("file:{encoded}?immutable=1"))
}
fn evidence_only(tx: Transaction<'_>, seq: i64, status: &str, reason: &str) -> Result<Value> {
    tx.execute(
        "UPDATE receipts SET status=?2,reason=?3 WHERE seq=?1",
        params![seq, status, reason],
    )?;
    tx.commit()?;
    Ok(
        json!({"status":reason,"accepted":false,"evidence_seq":seq.to_string(),"classification":status}),
    )
}

fn meta(tx: &Transaction<'_>, key: &str) -> Result<Option<String>> {
    Ok(tx
        .query_row("SELECT v FROM meta WHERE k=?1", [key], |r| r.get(0))
        .optional()?)
}
fn get<T: for<'a> Deserialize<'a>>(tx: &Transaction<'_>, key: &str) -> Result<T> {
    Ok(serde_json::from_str(
        &meta(tx, key)?.context("node not initialized")?,
    )?)
}
fn set<T: Serialize>(tx: &Transaction<'_>, key: &str, v: &T) -> Result<()> {
    tx.execute(
        "INSERT INTO meta VALUES(?1,?2) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        params![key, text(v)?],
    )?;
    Ok(())
}
fn identity(tx: &Transaction<'_>) -> Result<Identity> {
    get::<Option<Identity>>(tx, "identity")?.context("no existing identity")
}
fn member(tx: &Transaction<'_>, id: EntityId) -> Result<()> {
    let n: i64 = tx.query_row(
        "SELECT count(*) FROM members WHERE node=?1",
        [id.to_string()],
        |r| r.get(0),
    )?;
    ensure!(n == 1, "target is not registered");
    Ok(())
}
fn task(tx: &Transaction<'_>, id: EntityId) -> Result<Option<TaskRecord>> {
    tx.query_row(
        "SELECT data FROM tasks WHERE id=?1",
        [id.to_string()],
        |r| r.get::<_, String>(0),
    )
    .optional()?
    .map(|s| Ok(serde_json::from_str(&s)?))
    .transpose()
}
fn put_task(tx: &Transaction<'_>, t: &TaskRecord) -> Result<()> {
    tx.execute(
        "INSERT INTO tasks VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET data=excluded.data",
        params![t.task_id.to_string(), text(t)?],
    )?;
    Ok(())
}
fn commit(tx: &Transaction<'_>) -> Result<CommitRef> {
    let seq = meta(tx, "seq")?
        .map(|s| serde_json::from_str::<String>(&s))
        .transpose()?
        .map(|s| number(&s))
        .transpose()?
        .unwrap_or(0)
        .checked_add(1)
        .context("sequence exhausted")?;
    set(tx, "seq", &seq.to_string())?;
    Ok(CommitRef::new(get(tx, "ledger")?, LedgerSeq(seq)))
}
fn ticket(t: &TaskRecord, identity: &Identity) -> Result<Ticket> {
    let (invocation_id, epoch) = match t.state {
        TaskState::Running {
            invocation_id,
            epoch,
            ..
        } => (invocation_id, epoch),
        TaskState::Terminal { .. } => {
            let v = t
                .memos
                .get("settlement_ticket")
                .context("terminal ticket missing")?;
            return Ok(serde_json::from_value(v.clone())?);
        }
        _ => anyhow::bail!("task not running"),
    };
    Ok(Ticket {
        schema: SCHEMA,
        task_id: t.task_id,
        identity: identity.clone(),
        node_id: serde_json::from_value(
            t.memos
                .get("execution_node")
                .context("assignment missing")?
                .clone(),
        )?,
        task_revision: t.task_revision.0.to_string(),
        record_revision: t.record_revision.0.to_string(),
        epoch: epoch.0.to_string(),
        invocation_id,
        input_digest: t.input_ref.digest.clone(),
        contract_digest: contract(),
    })
}
fn append_event(tx: &Transaction<'_>, e: &Event) -> Result<&'static str> {
    let old: Option<(String, String)> = tx
        .query_row(
            "SELECT source,digest FROM events WHERE id=?1",
            [e.raw.event_id.to_string()],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((source, d)) = old {
        if source == e.raw.node_id.to_string() && d == e.digest.0 {
            return Ok("duplicate");
        }
        tx.execute(
            "INSERT INTO conflicts(id,data) VALUES(?1,?2)",
            params![e.raw.event_id.to_string(), text(e)?],
        )?;
        return Ok("conflict");
    }
    tx.execute(
        "INSERT INTO events(id,source,digest,data) VALUES(?1,?2,?3,?4)",
        params![
            e.raw.event_id.to_string(),
            e.raw.node_id.to_string(),
            e.digest.0,
            text(e)?
        ],
    )?;
    Ok("new")
}

/// Native artifact identity is derived from the running executable, never from
/// a repository HEAD that may differ from deployed code. Build with the actual
/// clean product commit in SHENKO_SOURCE_REVISION.
pub fn running_manifest() -> Result<Manifest> {
    Ok(Manifest {
        source_revision: option_env!("SHENKO_SOURCE_REVISION")
            .unwrap_or("unbound")
            .into(),
        artifact_digest: Digest::blake3(&std::fs::read(std::env::current_exe()?)?),
        schema: SCHEMA,
        os: std::env::consts::OS.into(),
        arch: std::env::consts::ARCH.into(),
    })
}
// Bounded input/result objects live inside this one SQLite commit domain. No
// separate filesystem CAS publication window or shared temporary filename.
fn seal(tx: &Transaction<'_>, bytes: &[u8], media: &str, codec: &str) -> Result<ObjectRef> {
    let digest = Digest::blake3(bytes);
    let old: Option<Vec<u8>> = tx
        .query_row(
            "SELECT bytes FROM objects WHERE digest=?1",
            [&digest.0],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(old) = old {
        if old != bytes {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "stored object corruption",
            )
            .into());
        }
    } else {
        tx.execute(
            "INSERT INTO objects VALUES(?1,?2)",
            params![digest.0, bytes],
        )?;
    }
    Ok(ObjectRef {
        digest: digest.clone(),
        bytes: bytes.len() as u64,
        media_type: media.into(),
        store_namespace: "node-inline-v1".into(),
        locator: format!("sqlite:objects/{}", digest.0),
        codec: SchemaRef {
            name: codec.into(),
            major: SCHEMA,
            digest: Digest::blake3(codec.as_bytes()),
        },
    })
}

fn assignment(tx: &Transaction<'_>, ticket: &Ticket) -> Result<()> {
    tx.execute(
        "INSERT INTO assignments VALUES(?1,?2,?3)",
        params![ticket.task_id.to_string(), ticket.epoch, text(ticket)?],
    )?;
    Ok(())
}
