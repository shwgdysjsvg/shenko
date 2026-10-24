//! Recoverable consumption of S1, on the existing ledger mutation line.
//! Dispatch is a projection of committed records, never of a broadcast.
use crate::{Store, external::TaskInvocation, memory::MemoryEntry};
use anyhow::{Result, ensure};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use shenko_types::{commit::CommitRef, ids::*, object::Digest, subject::*, task::*};

pub(crate) const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS subject_intent_steps(step_id TEXT PRIMARY KEY, migration TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS subject_intents(
 intent_id TEXT PRIMARY KEY, step_id TEXT NOT NULL, agent_id TEXT NOT NULL,
 kind TEXT NOT NULL, contract TEXT NOT NULL, payload TEXT NOT NULL, decision_ref TEXT NOT NULL,
 state TEXT NOT NULL, owner TEXT, revision INTEGER NOT NULL DEFAULT 0,
 created_seq INTEGER NOT NULL, updated_seq INTEGER NOT NULL, result_ref TEXT);
CREATE INDEX IF NOT EXISTS subject_intents_ready ON subject_intents(state,kind,created_seq,intent_id);
CREATE TABLE IF NOT EXISTS subject_dispatch_owner(singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner TEXT NOT NULL);
"#;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedIntent {
    pub intent_id: EntityId,
    pub step_id: EntityId,
    pub kind: String,
    pub contract: String,
    pub payload: serde_json::Value,
    pub state: String,
    pub owner: Option<String>,
    pub revision: u64,
    pub result_ref: Option<String>,
}

impl Store {
    /// Same SQL transaction as decision, dispositions, head and successor.
    pub(crate) fn enqueue_subject_intents(
        &self,
        tx: &Transaction<'_>,
        head: &SubjectBranchHead,
        step: &SubjectStep,
        decision: &StepDecision,
        commit: CommitRef,
        historical: bool,
    ) -> Result<(), shenko_types::error::ShenkoError> {
        use shenko_types::error::ShenkoError;
        let decision_ref = step
            .decision_ref
            .as_ref()
            .ok_or_else(|| ShenkoError::conflict("missing S1 decision"))?;
        // Historical postcommit actions had no reliable completion marker. Keep
        // them discoverable, but do NOT silently turn them into fresh effects.
        let state = if historical { "unknown" } else { "ready" };
        let insert = |id: EntityId,
                      kind: &str,
                      contract: &str,
                      payload: serde_json::Value|
         -> Result<(), ShenkoError> {
            tx.execute("INSERT INTO subject_intents(intent_id,step_id,agent_id,kind,contract,payload,decision_ref,state,created_seq,updated_seq,result_ref) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?9,?10)",
                params![id.to_string(),step.step_id.to_string(),head.agent_id.to_string(),kind,contract,
                payload.to_string(),serde_json::to_string(decision_ref)?,state,commit.seq.0 as i64,
                historical.then_some("pre-dispatch-ledger: reconcile original postcommit effects")]).map_err(|e|ShenkoError::storage(e.to_string()))?;
            Ok(())
        };
        for intent in &decision.memory_intents {
            insert(
                intent.intent_id,
                "memory",
                "shenko.memory/sqlite-save/1",
                serde_json::to_value(intent)?,
            )?;
            if !historical && matches!(intent.kind, MemoryIntentKind::Save) {
                let object = intent
                    .content_ref
                    .as_ref()
                    .ok_or_else(|| ShenkoError::conflict("memory intent missing content"))?;
                let bytes = self.cas.read_object(&object.digest)?;
                if bytes.len() as u64 != object.bytes || Digest::blake3(&bytes) != object.digest {
                    return Err(ShenkoError::ObjectClosureIncomplete { missing_count: 1 });
                }
                let content =
                    String::from_utf8(bytes).map_err(|e| ShenkoError::conflict(e.to_string()))?;
                tx.execute("INSERT INTO memory_intents(intent_id,agent_id,kind,scope,content,content_ref,entry_id,state,created_at_ms,updated_at_ms) VALUES(?1,?2,'note',?3,?4,?5,?1,'accepted',?6,?6)",
                    params![intent.intent_id.to_string(),head.agent_id.to_string(),intent.scope,content,object.locator,chrono::Utc::now().timestamp_millis()]).map_err(|e|ShenkoError::storage(e.to_string()))?;
            }
        }
        for intent in &decision.delegated_task_intents {
            insert(
                intent.intent_id,
                "delegation",
                "shenko.task/delegation-queue/1",
                serde_json::to_value(intent)?,
            )?;
            if !historical {
                // A durable handoff is NOT a claim that a child/seat has run.
                let task = self.intent_task(head, step, intent.intent_id, "delegation", commit);
                insert_task(tx, &task, commit)?;
                tx.execute(
                    "UPDATE subject_intents SET state='settled',result_ref=?2 WHERE intent_id=?1",
                    params![
                        intent.intent_id.to_string(),
                        format!("task:{}:pending", intent.intent_id)
                    ],
                )
                .map_err(|e| ShenkoError::storage(e.to_string()))?;
            }
        }
        for intent in &decision.publication_intents {
            insert(
                intent.intent_id,
                "publication",
                "shenko.napcat/onebot11-send/1",
                serde_json::to_value(intent)?,
            )?;
            if !historical {
                insert_task(
                    tx,
                    &self.intent_task(head, step, intent.intent_id, "subject_publication", commit),
                    commit,
                )?;
            }
        }
        // These are not produced by the current parser. Preserve future/older
        // payloads explicitly, without inventing an execution implementation.
        for intent in &decision.child_dialogue_intents {
            insert(
                intent.intent_id,
                "dialogue",
                "unbound/dialogue/1",
                serde_json::to_value(intent)?,
            )?;
        }
        for intent in &decision.policy_intents {
            insert(
                intent.intent_id,
                "policy",
                "unbound/policy/1",
                serde_json::to_value(intent)?,
            )?;
        }
        if !historical {
            tx.execute("UPDATE subject_intents SET state='blocked',result_ref='no installed dispatcher for frozen contract; explicit migration required' WHERE step_id=?1 AND (kind IN ('dialogue','policy') OR (kind='memory' AND json_extract(payload,'$.kind')!='save'))",[step.step_id.to_string()]).map_err(|e|ShenkoError::storage(e.to_string()))?;
        }
        tx.execute(
            "INSERT INTO subject_intent_steps(step_id,migration) VALUES(?1,?2)",
            params![
                step.step_id.to_string(),
                if historical {
                    "historical-reconcile"
                } else {
                    "S1-native-v1"
                }
            ],
        )
        .map_err(|e| ShenkoError::storage(e.to_string()))?;
        Ok(())
    }

    fn intent_task(
        &self,
        head: &SubjectBranchHead,
        step: &SubjectStep,
        id: EntityId,
        kind: &str,
        commit: CommitRef,
    ) -> TaskRecord {
        TaskRecord {
            record_schema_version: 1,
            task_id: id,
            task_revision: TaskRevision(0),
            record_revision: RecordRevision(0),
            agent_id: head.agent_id,
            conversation_id: head.agent_id,
            owner: TaskOwner::Task {
                id: head.subject_root_task_id,
            },
            background: false,
            kind: kind.into(),
            definition_version: 1,
            implementation_ref: ImplementationRef {
                plugin_id: EntityId::nil(),
                artifact_digest: Digest::blake3(include_bytes!("intents.rs")),
                registry_revision: 1,
            },
            input_ref: step.decision_ref.clone().expect("validated decision"),
            state: TaskState::Pending {
                checkpoint_ref: None,
            },
            abort_request: None,
            memos: serde_json::Map::from_iter([("intent_id".into(), serde_json::json!(id))]),
            residual_case_ids: vec![],
            created_by_step_id: Some(step.step_id),
            dispatch_eligibility: DispatchEligibility::Ready,
            no_progress_crashes: 0,
            first_run_at: None,
            terminal_at: None,
            created_commit: commit,
            updated_commit: commit,
        }
    }

    /// Called once before releasing the recovered subject. Invalidates only
    /// dispatch authority; old callbacks can still enter EvidenceIngress.
    pub fn start_subject_dispatcher(&self) -> Result<()> {
        self.ledger.with_mutation(|| {
            let mut c=self.ledger.conn.lock(); let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let old_steps = {
                let mut q=tx.prepare("SELECT s.data,h.data FROM subject_steps s JOIN subject_branch_heads h ON s.branch_id=h.branch_id LEFT JOIN subject_intent_steps i ON i.step_id=s.step_id WHERE s.state='committed' AND i.step_id IS NULL ORDER BY s.updated_commit_seq")?;
                q.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (step,head) in old_steps {
                let step:SubjectStep=serde_json::from_str(&step)?; let head:SubjectBranchHead=serde_json::from_str(&head)?;
                let decoded=step.decision_ref.as_ref().and_then(|o| self.cas.read_object(&o.digest).ok())
                    .and_then(|bytes|serde_json::from_slice::<StepDecision>(&bytes).ok()).filter(|d|d.schema_version==3);
                if let Some(decision)=decoded {
                    self.enqueue_subject_intents(&tx,&head,&step,&decision,step.commit_ref.unwrap(),true)?;
                } else {
                    // Raw decision/receipt references remain rooted in the step.
                    tx.execute("INSERT INTO subject_intent_steps VALUES(?1,'blocked: unreadable historical decision; preserve source and install migrator')",[step.step_id.to_string()])?;
                }
            }
            let seq=self.ledger.allocate_seq(&tx)?;
            tx.execute("INSERT INTO subject_dispatch_owner VALUES(1,?1) ON CONFLICT(singleton) DO UPDATE SET owner=excluded.owner",[self.capabilities.incarnation.to_string()])?;
            // No T1 means safe to re-admit; any armed operation is UNKNOWN until
            // its existing E1 proves otherwise. Never infer 'not sent' from crash.
            tx.execute("UPDATE subject_intents SET state=CASE WHEN EXISTS(SELECT 1 FROM operations o WHERE o.operation_id=subject_intents.intent_id) THEN 'unknown' ELSE 'ready' END, owner=NULL,revision=revision+1,updated_seq=?1 WHERE state='claimed'",[seq.0 as i64])?;
            self.intent_log(&tx,CommitRef::new(self.ledger.ledger_id(),seq))?;
            tx.commit()?; Ok(())
        },storage_failure)
    }

    pub fn committed_intents(
        &self,
        kind: Option<&str>,
        state: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CommittedIntent>> {
        let c = self.ledger.conn.lock();
        let mut q=c.prepare("SELECT intent_id,step_id,kind,contract,payload,state,owner,revision,result_ref FROM subject_intents WHERE (?1 IS NULL OR kind=?1) AND (?2 IS NULL OR state=?2) ORDER BY created_seq,intent_id LIMIT ?3")?;
        let rows = q
            .query_map(params![kind, state, limit as i64], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, u64>(7)?,
                    r.get::<_, Option<String>>(8)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(
                |(id, step, kind, contract, payload, state, owner, revision, result_ref)| {
                    Ok(CommittedIntent {
                        intent_id: id.parse()?,
                        step_id: step.parse()?,
                        kind,
                        contract,
                        payload: serde_json::from_str(&payload)?,
                        state,
                        owner,
                        revision,
                        result_ref,
                    })
                },
            )
            .collect()
    }

    /// Deterministic memory worker: same DB transaction as its progress/result.
    /// No model, network or filesystem worker runs under this mutation line.
    pub fn settle_subject_memory(&self) -> Result<usize> {
        self.ledger.with_mutation(|| {
            let mut c=self.ledger.conn.lock();let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            self.check_dispatch_owner(&tx)?;
            let rows={let mut q=tx.prepare("SELECT i.intent_id,i.payload,i.revision,m.agent_id,m.content,m.scope,m.created_at_ms FROM subject_intents i JOIN memory_intents m ON i.intent_id=m.intent_id WHERE i.kind='memory' AND i.state='ready' ORDER BY i.created_seq LIMIT 32")?;
                q.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,u64>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?,r.get::<_,i64>(6)?)))?.collect::<rusqlite::Result<Vec<_>>>()?};
            if rows.is_empty(){return Ok(0)}
            let seq=self.ledger.allocate_seq(&tx)?;let count=rows.len();
            for (id,payload,rev,agent,content,scope,time) in rows {
                let intent:MemoryIntent=serde_json::from_str(&payload)?;ensure!(matches!(intent.kind,MemoryIntentKind::Save),"unbound_memory_contract");
                let entry=MemoryEntry{entry_id:id.parse()?,agent_id:agent.parse()?,kind:"note".into(),content,scope,source_ref:Some(format!("subject-intent:{id}")),record_revision:0,created_at_ms:time,updated_commit_seq:None};
                Store::write_memory_entry(&tx,&entry,Some(intent.intent_id),seq)?;
                ensure!(tx.execute("UPDATE subject_intents SET state='settled',owner=?2,revision=revision+1,updated_seq=?3,result_ref=?4 WHERE intent_id=?1 AND state='ready' AND revision=?5",
                    params![id,self.capabilities.incarnation.to_string(),seq.0 as i64,format!("memory:{}",entry.entry_id),rev])?==1,"stale_intent");
            }
            self.intent_log(&tx,CommitRef::new(self.ledger.ledger_id(),seq))?;tx.commit()?;Ok(count)
        },storage_failure)
    }

    pub fn subject_publication(&self, id: EntityId) -> Result<PublicationIntent> {
        let c = self.ledger.conn.lock();
        let payload: String = c.query_row(
            "SELECT payload FROM subject_intents WHERE intent_id=?1 AND kind='publication'",
            [id.to_string()],
            |r| r.get(0),
        )?;
        Ok(serde_json::from_str(&payload)?)
    }

    /// Claim creates the normal TaskInvocation used by the shared T1 service.
    /// A repeated admission returns None; that NEVER grants a second send.
    pub fn claim_subject_publication(&self, id: EntityId) -> Result<Option<TaskInvocation>> {
        self.ledger.with_mutation(|| {
            let mut c=self.ledger.conn.lock();let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;self.check_dispatch_owner(&tx)?;
            let state:Option<String>=tx.query_row("SELECT state FROM subject_intents WHERE intent_id=?1 AND kind='publication' AND contract='shenko.napcat/onebot11-send/1'",[id.to_string()],|r|r.get(0)).optional()?;
            if state.as_deref()!=Some("ready"){return Ok(None)}
            let data:String=tx.query_row("SELECT data FROM tasks WHERE task_id=?1",[id.to_string()],|r|r.get(0))?;let mut task:TaskRecord=serde_json::from_str(&data)?;
            ensure!(task.abort_request.is_none()&&!task.is_terminal(),"publication_cancelled");
            let step:SubjectStep={let data:String=tx.query_row("SELECT s.data FROM subject_steps s WHERE s.step_id=?1",[task.created_by_step_id.unwrap().to_string()],|r|r.get(0))?;serde_json::from_str(&data)?};
            let head:SubjectBranchHead={let data:String=tx.query_row("SELECT data FROM subject_branch_heads WHERE branch_id=?1",[step.branch_id.0.to_string()],|r|r.get(0))?;serde_json::from_str(&data)?};
            ensure!(head.controller_epoch==step.controller_epoch && head.mode==SubjectMode::Active,"publication_authority_changed: reconcile");
            let seq=self.ledger.allocate_seq(&tx)?;let commit=CommitRef::new(self.ledger.ledger_id(),seq);let invocation_id=EntityId::new();
            task.state=TaskState::Running{checkpoint_ref:None,invocation_id,mode:RunMode::Run,epoch:head.controller_epoch};task.record_revision=task.record_revision.next();task.updated_commit=commit;
            tx.execute("UPDATE tasks SET record_revision=?2,data=?3,updated_commit_seq=?4 WHERE task_id=?1",params![id.to_string(),task.record_revision.0 as i64,serde_json::to_string(&task)?,seq.0 as i64])?;
            tx.execute("UPDATE subject_intents SET state='claimed',owner=?2,revision=revision+1,updated_seq=?3 WHERE intent_id=?1 AND state='ready'",params![id.to_string(),self.capabilities.incarnation.to_string(),seq.0 as i64])?;
            self.intent_log(&tx,commit)?;tx.commit()?;
            Ok(Some(TaskInvocation{task_id:id,task_revision:task.task_revision,record_revision:task.record_revision,invocation_id,epoch:head.controller_epoch,worker_id:self.capabilities.incarnation,node_id:self.ledger.ledger_id()}))
        },storage_failure)
    }

    pub(crate) fn check_dispatch_owner(&self, tx: &Transaction<'_>) -> Result<()> {
        let owner: String = tx.query_row(
            "SELECT owner FROM subject_dispatch_owner WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        ensure!(
            owner == self.capabilities.incarnation.to_string(),
            "stale_subject_dispatcher"
        );
        Ok(())
    }

    /// E1 remains append-only and independent. A CURRENT dispatcher derives T2
    /// progress from it; a late callback itself does not advance the task.
    pub fn reconcile_subject_publications(&self) -> Result<()> {
        // Seal terminal outcome objects before entering the write transaction.
        // A process restart is local loss-of-control evidence, NOT proof that
        // the remote send stopped or failed.
        let mut outcomes = std::collections::HashMap::new();
        let changed = {
            let c = self.ledger.conn.lock();
            let mut q=c.prepare("SELECT i.intent_id FROM subject_intents i JOIN operations o ON o.operation_id=i.intent_id WHERE i.kind='publication' AND i.state IN ('claimed','unknown') AND (i.result_ref IS NULL OR i.result_ref!=o.data) AND (json_extract(o.data,'$.state') IN ('resolved','reconciling') OR (i.state='unknown' AND i.owner IS NULL)) ORDER BY i.updated_seq LIMIT 32")?;
            q.query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        for id in changed {
            let work = {
                let c = self.ledger.conn.lock();
                let (state, owner): (String, Option<String>) = c.query_row(
                    "SELECT state,owner FROM subject_intents WHERE intent_id=?1",
                    [&id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )?;
                CommittedIntent {
                    intent_id: id.parse()?,
                    step_id: EntityId::nil(),
                    kind: "publication".into(),
                    contract: String::new(),
                    payload: serde_json::Value::Null,
                    state,
                    owner,
                    revision: 0,
                    result_ref: None,
                }
            };
            if !matches!(work.state.as_str(), "claimed" | "unknown") {
                continue;
            }
            if let Some(mut op) = self.get_operation(work.intent_id)? {
                if work.state == "unknown"
                    && work.owner.is_none()
                    && op.state == shenko_types::operation::OperationState::InFlight
                {
                    let attempt = self.external_operation_attempt(work.intent_id)?;
                    let raw=self.cas.seal_object(br#"{"observation":"dispatcher restarted; armed send outcome unknown","provider_stop_proven":false}"#,"application/json","recovery_evidence","json",1)?.object;
                    let ev = shenko_types::evidence::EvidenceReceipt {
                        record_schema_version: 1,
                        receipt_id: EntityId::new(),
                        operation_id: Some(work.intent_id),
                        attempt_id: Some(attempt.attempt_id),
                        source_id: "subject-dispatch-recovery".into(),
                        source_instance_id: self.capabilities.incarnation.to_string(),
                        source_event_key: None,
                        producer_epoch: None,
                        producer_schema_version: Some("1".into()),
                        raw_ref: raw,
                        decoder_ref: None,
                        decode_status: shenko_types::evidence::DecodeStatus::Decoded,
                        observation_level: None,
                        normalized_ref: None,
                        observed_at: Some(chrono::Utc::now()),
                        received_commit: attempt.armed_commit,
                    };
                    op = self.record_external_evidence(
                        work.intent_id,
                        attempt.attempt_id,
                        ev,
                        None,
                        shenko_types::operation::EffectStatus::Unknown,
                    )?;
                }
                if op.state == shenko_types::operation::OperationState::Resolved {
                    let bytes = serde_json::to_vec(&op)?;
                    let object = self
                        .cas
                        .seal_object(
                            &bytes,
                            "application/json",
                            "publication_outcomes",
                            "OperationRecord",
                            1,
                        )?
                        .object;
                    outcomes.insert(work.intent_id, (op.record_revision, object));
                }
            }
        }
        self.ledger.with_mutation(|| {
            let mut c=self.ledger.conn.lock();let tx=c.transaction_with_behavior(TransactionBehavior::Immediate)?;self.check_dispatch_owner(&tx)?;
            let rows={let mut q=tx.prepare("SELECT i.intent_id,o.data FROM subject_intents i JOIN operations o ON o.operation_id=i.intent_id WHERE i.kind='publication' AND i.state IN ('claimed','unknown') AND json_extract(o.data,'$.state') IN ('resolved','reconciling') AND (i.result_ref IS NULL OR i.result_ref!=o.data) LIMIT 32")?;
                q.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?.collect::<rusqlite::Result<Vec<_>>>()?};
            if rows.is_empty(){return Ok(())}let seq=self.ledger.allocate_seq(&tx)?;let commit=CommitRef::new(self.ledger.ledger_id(),seq);
            for (id,data) in rows {
                let op:shenko_types::operation::OperationRecord=serde_json::from_str(&data)?;
                let resolved=op.state==shenko_types::operation::OperationState::Resolved;
                if resolved && !outcomes.get(&op.operation_id).is_some_and(|(revision,_)|*revision==op.record_revision) {continue;}
                let result=serde_json::to_string(&op)?;
                tx.execute("UPDATE subject_intents SET state=?2,owner=?3,revision=revision+1,updated_seq=?4,result_ref=?5 WHERE intent_id=?1",params![id,if resolved{"settled"}else{"unknown"},self.capabilities.incarnation.to_string(),seq.0 as i64,result])?;
                if resolved {
                    let data:Option<String>=tx.query_row("SELECT data FROM tasks WHERE task_id=?1",[&id],|r|r.get(0)).optional()?;
                    if let Some(data)=data {let mut task:TaskRecord=serde_json::from_str(&data)?;
                        if !task.is_terminal() && task.abort_request.is_none(){task.state=TaskState::Terminal{outcome_ref:outcomes.get(&op.operation_id).unwrap().1.clone()};task.record_revision=task.record_revision.next();task.updated_commit=commit;task.terminal_at=Some(chrono::Utc::now());
                            tx.execute("UPDATE tasks SET record_revision=?2,data=?3,updated_commit_seq=?4 WHERE task_id=?1",params![id,task.record_revision.0 as i64,serde_json::to_string(&task)?,seq.0 as i64])?;}
                    }
                }
            }
            self.intent_log(&tx,commit)?;tx.commit()?;Ok(())
        },storage_failure)
    }

    pub fn committed_decisions_after(
        &self,
        seq: u64,
        limit: usize,
    ) -> Result<Vec<(SubjectStep, StepDecision)>> {
        let rows = {
            let c = self.ledger.conn.lock();
            let mut q=c.prepare("SELECT data FROM subject_steps WHERE state='committed' AND updated_commit_seq>?1 ORDER BY updated_commit_seq LIMIT ?2")?;
            q.query_map(params![seq as i64, limit as i64], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        rows.into_iter()
            .map(|data| {
                let step: SubjectStep = serde_json::from_str(&data)?;
                let object = step
                    .decision_ref
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("committed step missing decision"))?;
                let bytes = self.cas.read_object(&object.digest)?;
                ensure!(
                    bytes.len() as u64 == object.bytes && Digest::blake3(&bytes) == object.digest,
                    "decision_object_corruption"
                );
                let decision = serde_json::from_slice(&bytes)?;
                Ok((step, decision))
            })
            .collect()
    }

    fn intent_log(&self, tx: &Transaction<'_>, commit: CommitRef) -> Result<()> {
        tx.execute(
            "INSERT INTO commit_log(ledger_id,seq,incarnation,entity_count) VALUES(?1,?2,?3,1)",
            params![
                commit.ledger_id.to_string(),
                commit.seq.0 as i64,
                self.capabilities.incarnation.to_string()
            ],
        )?;
        Ok(())
    }
}
fn insert_task(
    tx: &Transaction<'_>,
    task: &TaskRecord,
    commit: CommitRef,
) -> Result<(), shenko_types::error::ShenkoError> {
    tx.execute(
        "INSERT INTO tasks(task_id,record_revision,data,updated_commit_seq) VALUES(?1,?2,?3,?4)",
        params![
            task.task_id.to_string(),
            task.record_revision.0 as i64,
            serde_json::to_string(task)?,
            commit.seq.0 as i64
        ],
    )
    .map_err(|e| shenko_types::error::ShenkoError::storage(e.to_string()))?;
    Ok(())
}
fn storage_failure(e: &anyhow::Error) -> bool {
    e.is::<rusqlite::Error>()
        || e.is::<serde_json::Error>()
        || e.downcast_ref::<shenko_types::error::ShenkoError>()
            .is_some_and(|e| matches!(e, shenko_types::error::ShenkoError::Storage(_)))
}
