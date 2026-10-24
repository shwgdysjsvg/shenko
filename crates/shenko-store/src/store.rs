//! Main store — SPEC §41.2.1
use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

use shenko_types::commit::{CommitReceipt, CommitRef, DurabilityProfile, UpdatedEntity};
use shenko_types::error::ShenkoError;
use shenko_types::ids::EntityId;
use shenko_types::ids::{BranchId, RecordRevision};
use shenko_types::store::{StoreCapabilities, WriteBatch};
use shenko_types::subject::{
    SubjectBranchHead, SubjectGenesis, SubjectMode, SubjectStep, SubjectStepState,
};

use crate::cas::ContentStore;
use crate::ledger::Ledger;

pub struct Store {
    pub ledger: Arc<Ledger>,
    pub cas: Arc<ContentStore>,
    pub capabilities: StoreCapabilities,
}

impl Store {
    pub fn open(db_path: PathBuf, cas_dir: PathBuf) -> Result<Self> {
        let conn = Connection::open(&db_path)?;
        configure_sqlite(&conn)?;

        // Run migrations
        crate::migrations::run_migrations(&conn)?;

        let sqlite_version: String = conn.query_row("SELECT sqlite_version()", [], |r| r.get(0))?;
        let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
        let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |r| r.get(0))?;

        // FTS5 detection
        let fts5: bool = conn
            .query_row(
                "SELECT count(*) FROM pragma_compile_options WHERE compile_options='ENABLE_FTS5'",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_or(0)
            > 0;

        let incarnation = EntityId::new();
        let capabilities = StoreCapabilities {
            sqlite_version,
            wal_mode: journal_mode.to_lowercase() == "wal",
            fts5_available: fts5,
            synchronous: synchronous.to_string(),
            journal_mode,
            incarnation,
            durability_profile: DurabilityProfile::PowerLossLocal,
        };

        info!(
            fts5 = capabilities.fts5_available,
            wal = capabilities.wal_mode,
            sqlite = %capabilities.sqlite_version,
            "store opened"
        );

        let conn = Arc::new(Mutex::new(conn));
        let ledger = Arc::new(Ledger::new(Arc::clone(&conn), incarnation)?);
        let cas = Arc::new(ContentStore::new(cas_dir));

        Ok(Self {
            ledger,
            cas,
            capabilities,
        })
    }

    pub fn commit(&self, batch: WriteBatch) -> Result<CommitReceipt, ShenkoError> {
        self.ledger.commit(batch)
    }

    pub fn capabilities(&self) -> &StoreCapabilities {
        &self.capabilities
    }

    /// 同笔创建出生记录、head 与第一步；提交引用由真实事务赋值。§44.6
    pub fn initialize_genesis(
        &self,
        genesis: &SubjectGenesis,
        head: &SubjectBranchHead,
        first_step: &SubjectStep,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.ledger.with_mutation(
            || self.try_initialize_genesis(genesis, head, first_step),
            |e| matches!(e, ShenkoError::Storage(_)),
        )
    }

    fn try_initialize_genesis(
        &self,
        genesis: &SubjectGenesis,
        head: &SubjectBranchHead,
        first_step: &SubjectStep,
    ) -> Result<CommitReceipt, ShenkoError> {
        if genesis.subject_id != head.agent_id
            || head.genesis_ref.genesis_id != genesis.genesis_id
            || head.genesis_ref.ledger_id != self.ledger.ledger_id()
            || head.committed_step_no != 0
            || head.committed_step_id.is_some()
            || head.record_revision != RecordRevision(0)
            || head.mode != SubjectMode::Active
            || first_step.state != SubjectStepState::Planned
            || first_step.record_revision != RecordRevision(0)
        {
            return Err(ShenkoError::conflict("invalid genesis/head/first step"));
        }
        validate_step_binding(head, first_step)?;
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let seq = self.ledger.allocate_seq(&tx)?;
        let commit_ref = CommitRef::new(self.ledger.ledger_id(), seq);
        let mut genesis = genesis.clone();
        let mut head = head.clone();
        let mut first_step = first_step.clone();
        genesis.created_commit = commit_ref;
        head.committed_at = commit_ref;
        first_step.expected_parent_commit = commit_ref;
        tx.execute(
            "INSERT INTO subject_genesis (genesis_id, subject_id, bootstrap_id, data, created_commit_seq) VALUES (?1,?2,?3,?4,?5)",
            params![genesis.genesis_id.to_string(), genesis.subject_id.to_string(), genesis.bootstrap_id.to_string(), serde_json::to_string(&genesis)?, seq.0 as i64],
        ).map_err(storage_error)?;
        tx.execute(
            "INSERT INTO subject_branch_heads (agent_id, branch_id, committed_step_no, committed_step_id, next_step_id, mode, record_revision, data, updated_commit_seq) VALUES (?1,?2,0,NULL,?3,'active',0,?4,?5)",
            params![head.agent_id.to_string(), head.branch_id.0.to_string(), head.next_step_id.to_string(), serde_json::to_string(&head)?, seq.0 as i64],
        ).map_err(storage_error)?;
        insert_step(&tx, &first_step, seq.0)?;
        self.record_subject_commit(&tx, commit_ref, 3)?;
        tx.commit().map_err(storage_error)?;
        Ok(self.subject_receipt(
            commit_ref,
            vec![
                UpdatedEntity {
                    entity_id: genesis.genesis_id,
                    record_revision: RecordRevision(0),
                },
                UpdatedEntity {
                    entity_id: head.agent_id,
                    record_revision: head.record_revision,
                },
                UpdatedEntity {
                    entity_id: first_step.step_id,
                    record_revision: first_step.record_revision,
                },
            ],
        ))
    }

    /// S0 与结果落账只更新已登记步骤，不允许插入随机新步骤。§41.5
    pub fn upsert_step(
        &self,
        step: &SubjectStep,
        expected_head: &SubjectBranchHead,
        expected_revision: RecordRevision,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.ledger.with_mutation(
            || self.try_upsert_step(step, expected_head, expected_revision),
            |e| matches!(e, ShenkoError::Storage(_)),
        )
    }

    fn try_upsert_step(
        &self,
        step: &SubjectStep,
        expected_head: &SubjectBranchHead,
        expected_revision: RecordRevision,
    ) -> Result<CommitReceipt, ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let head = checked_head(&tx, expected_head)?;
        validate_step_binding(&head, step)?;
        let previous = read_step(&tx, &head.branch_id, step.step_id)?
            .ok_or_else(|| ShenkoError::not_found("registered subject step"))?;
        validate_step_binding(&head, &previous)?;
        use SubjectStepState::*;
        let legal = matches!(
            (previous.state, step.state),
            (Planned, Preparing)
                | (Preparing, AwaitingModel)
                | (AwaitingModel, ResultRecorded)
                | (AwaitingModel, RetryPending)
                | (RetryPending, AwaitingModel)
        );
        if !legal
            || previous.record_revision != expected_revision
            || step.record_revision.0 != expected_revision.0 + 1
            || step.created_by_step_id != previous.created_by_step_id
            || (previous.state != Planned
                && (step.input_plan_ref != previous.input_plan_ref
                    || step.context_revision != previous.context_revision))
            || (previous.active_request_id.is_some()
                && step.active_request_id != previous.active_request_id)
            || (previous.active_attempt_id.is_some()
                && step.active_attempt_id != previous.active_attempt_id)
            || step.input_plan_ref.is_none()
            || (step.state == AwaitingModel && step.active_request_id.is_none())
            || (step.state == ResultRecorded
                && (step.model_result_ref.is_none() || step.model_outcome_ref.is_none()))
        {
            return Err(ShenkoError::conflict("stale or invalid subject transition"));
        }
        if step.state == RetryPending || previous.state == RetryPending {
            let evidence_ref = if step.state == RetryPending {
                &step.retry_reason_ref
            } else {
                &previous.retry_reason_ref
            };
            let object = evidence_ref.as_ref().ok_or_else(|| {
                ShenkoError::conflict("retry requires positive dispatch evidence")
            })?;
            let bytes = self.cas.read_object(&object.digest)?;
            let evidence: shenko_types::subject::SubjectRetryEvidence =
                serde_json::from_slice(&bytes)?;
            if bytes.len() as u64 != object.bytes
                || shenko_types::object::Digest::blake3(&bytes) != object.digest
                || !evidence.permits_retry(previous.active_request_id)
                || previous.active_attempt_id.is_some()
                || step.model_result_ref.is_some()
            {
                return Err(ShenkoError::conflict("request is not proven safe to retry"));
            }
        }
        self.verify_step_objects(step)?;
        let seq = self.ledger.allocate_seq(&tx)?;
        update_step(&tx, step, expected_revision, seq.0)?;
        let commit_ref = CommitRef::new(self.ledger.ledger_id(), seq);
        self.record_subject_commit(&tx, commit_ref, 1)?;
        tx.commit().map_err(storage_error)?;
        Ok(self.subject_receipt(
            commit_ref,
            vec![UpdatedEntity {
                entity_id: step.step_id,
                record_revision: step.record_revision,
            }],
        ))
    }

    /// R0 for a conclusive but invalid control result: retain the retired step
    /// and rejection, register a sealed correction, and CAS only next_step_id.
    /// No S1, disposition, identity change or external intent is admitted here.
    pub fn reject_output_and_replace_step(
        &self,
        retired: &SubjectStep,
        expected_head: &SubjectBranchHead,
        replacement: &SubjectStep,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.ledger.with_mutation(
            || self.try_reject_output_and_replace_step(retired, expected_head, replacement),
            |e| matches!(e, ShenkoError::Storage(_)),
        )
    }

    fn try_reject_output_and_replace_step(
        &self,
        retired: &SubjectStep,
        expected_head: &SubjectBranchHead,
        replacement: &SubjectStep,
    ) -> Result<CommitReceipt, ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let mut head = checked_head(&tx, expected_head)?;
        validate_step_binding(&head, retired)?;
        let previous = read_step(&tx, &head.branch_id, retired.step_id)?
            .ok_or_else(|| ShenkoError::not_found("rejected subject step"))?;
        validate_step_binding(&head, &previous)?;
        if previous.state != SubjectStepState::ResultRecorded
            || retired.state != SubjectStepState::Retired
            || retired.record_revision.0 != previous.record_revision.0 + 1
            || retired.active_request_id != previous.active_request_id
            || retired.active_attempt_id != previous.active_attempt_id
            || retired.model_result_ref != previous.model_result_ref
            || retired.model_outcome_ref != previous.model_outcome_ref
            || retired.input_plan_ref != previous.input_plan_ref
            || retired.context_revision != previous.context_revision
            || retired.created_by_step_id != previous.created_by_step_id
            || retired.decision_ref.is_some()
            || retired.commit_ref.is_some()
            || replacement.step_id == retired.step_id
            || replacement.state != SubjectStepState::Preparing
            || replacement.record_revision != RecordRevision(0)
            || replacement.active_request_id.is_some()
            || replacement.active_attempt_id.is_some()
            || replacement.model_result_ref.is_some()
            || replacement.model_outcome_ref.is_some()
            || replacement.decision_ref.is_some()
            || replacement.commit_ref.is_some()
            || replacement.created_by_step_id != previous.created_by_step_id
            || replacement.input_plan_ref.is_none()
            || replacement.input_plan_ref == previous.input_plan_ref
            || replacement.context_revision.is_none()
            || replacement.context_revision
                != previous
                    .context_revision
                    .and_then(|revision| revision.checked_add(1))
            || replacement.retry_reason_ref != retired.retry_reason_ref
        {
            return Err(ShenkoError::conflict("invalid rejected-output replacement"));
        }
        self.verify_step_objects(retired)?;
        self.verify_step_objects(replacement)?;
        let reason = retired
            .retry_reason_ref
            .as_ref()
            .ok_or_else(|| ShenkoError::conflict("missing control rejection"))?;
        let rejection: shenko_types::subject::ControlOutputRejection =
            serde_json::from_slice(&self.cas.read_object(&reason.digest)?)?;
        if rejection.schema_version != 2
            || rejection.supersedes_step_id != previous.step_id
            || rejection.replacement_step_id != replacement.step_id
            || Some(rejection.request_id) != previous.active_request_id
            || Some(&rejection.input_plan_ref) != previous.input_plan_ref.as_ref()
            || Some(&rejection.model_result_ref) != previous.model_result_ref.as_ref()
            || Some(&rejection.model_outcome_ref) != previous.model_outcome_ref.as_ref()
            || !matches!(
                rejection.error.code.as_str(),
                "CONTROL_PROTOCOL_INVALID" | "MODEL_RESULT_INCOMPLETE"
            )
            || rejection.error.scope != shenko_types::error::ErrorScope::Request
            || rejection.error.retry != shenko_types::error::RetryStrategy::NewRevision
        {
            return Err(ShenkoError::conflict(
                "rejection is not bound to recorded result",
            ));
        }
        self.recorded_model_outcome(&previous)?;
        head.next_step_id = replacement.step_id;
        head.record_revision = RecordRevision(head.record_revision.0 + 1);
        validate_step_binding(&head, replacement)?;
        let seq = self.ledger.allocate_seq(&tx)?;
        let commit_ref = CommitRef::new(self.ledger.ledger_id(), seq);
        update_step(&tx, retired, previous.record_revision, seq.0)?;
        // A duplicate replacement ID fails the entire transaction, including
        // retirement, so the head can never point at a missing/retired step.
        insert_step(&tx, replacement, seq.0)?;
        let changed = tx.execute(
            "UPDATE subject_branch_heads SET next_step_id=?1, record_revision=?2, data=?3, updated_commit_seq=?4
             WHERE agent_id=?5 AND branch_id=?6 AND record_revision=?7 AND next_step_id=?8",
            params![replacement.step_id.to_string(), head.record_revision.0 as i64,
                serde_json::to_string(&head)?, seq.0 as i64, head.agent_id.to_string(),
                head.branch_id.0.to_string(), expected_head.record_revision.0 as i64,
                previous.step_id.to_string()],
        ).map_err(storage_error)?;
        if changed != 1 {
            return Err(ShenkoError::conflict("stale branch head"));
        }
        self.record_subject_commit(&tx, commit_ref, 3)?;
        tx.commit().map_err(storage_error)?;
        Ok(self.subject_receipt(
            commit_ref,
            vec![
                UpdatedEntity {
                    entity_id: retired.step_id,
                    record_revision: retired.record_revision,
                },
                UpdatedEntity {
                    entity_id: head.agent_id,
                    record_revision: head.record_revision,
                },
                UpdatedEntity {
                    entity_id: replacement.step_id,
                    record_revision: replacement.record_revision,
                },
            ],
        ))
    }

    /// S1：条件更新 step/head，并原子插入真实 planned 后继。§41.5
    pub fn commit_step_and_advance_head(
        &self,
        step: &SubjectStep,
        expected_head: &SubjectBranchHead,
        next_step_id: EntityId,
    ) -> Result<CommitReceipt, ShenkoError> {
        self.ledger.with_mutation(
            || self.try_commit_step_and_advance_head(step, expected_head, next_step_id),
            |e| matches!(e, ShenkoError::Storage(_)),
        )
    }

    fn try_commit_step_and_advance_head(
        &self,
        step: &SubjectStep,
        expected_head: &SubjectBranchHead,
        next_step_id: EntityId,
    ) -> Result<CommitReceipt, ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage_error)?;
        let mut head = checked_head(&tx, expected_head)?;
        validate_step_binding(&head, step)?;
        let previous = read_step(&tx, &head.branch_id, step.step_id)?
            .ok_or_else(|| ShenkoError::not_found("registered subject step"))?;
        validate_step_binding(&head, &previous)?;
        if previous.state != SubjectStepState::ResultRecorded
            || step.state != SubjectStepState::Committed
            || step.record_revision.0 != previous.record_revision.0 + 1
            || step.active_request_id != previous.active_request_id
            || step.active_attempt_id != previous.active_attempt_id
            || step.model_result_ref != previous.model_result_ref
            || step.model_outcome_ref != previous.model_outcome_ref
            || step.input_plan_ref != previous.input_plan_ref
            || step.context_revision != previous.context_revision
            || step.created_by_step_id != previous.created_by_step_id
        {
            return Err(ShenkoError::conflict("S1 requires current recorded result"));
        }
        self.verify_step_objects(step)?;
        if !self.recorded_model_outcome(step)?.is_complete() {
            return Err(ShenkoError::conflict("S1 requires a complete model result"));
        }
        let decision_ref = step
            .decision_ref
            .as_ref()
            .ok_or_else(|| ShenkoError::conflict("S1 missing decision"))?;
        let decision: shenko_types::subject::StepDecision =
            serde_json::from_slice(&self.cas.read_object(&decision_ref.digest)?)?;
        if decision.schema_version != 3
            || decision.output.schema_version != 1
            || decision.output.step_id != step.step_id
            || Some(&decision.output.raw_ref) != step.model_result_ref.as_ref()
            || decision.step_id != step.step_id
            || decision.successor_step_id != next_step_id
            || Some(&decision.input_plan_ref) != step.input_plan_ref.as_ref()
            || decision.model_result_ref != step.model_result_ref
        {
            return Err(ShenkoError::conflict("decision/step/successor mismatch"));
        }
        let seq = self.ledger.allocate_seq(&tx)?;
        let commit_ref = CommitRef::new(self.ledger.ledger_id(), seq);
        // Identity is settled under the SAME IMMEDIATE transaction as S1. The
        // intent carries the S0 revision; never rebase model edits onto latest.
        use shenko_types::identity::{IdentityOutcome, IdentityReceipt, IdentitySource};
        let current_identity = crate::identity::read_latest(&tx, head.agent_id)?;
        let actual_revision = current_identity.as_ref().map(|p| p.revision);
        let outcome = if let Some(intent) = &decision.identity_intent {
            let frozen: serde_json::Value =
                serde_json::from_slice(&self.cas.read_object(&decision.input_plan_ref.digest)?)?;
            if frozen
                .get("identity_provider_revision")
                .and_then(|v| v.as_str())
                != Some(intent.provider_revision.as_str())
                || frozen.get("schema_version").and_then(|v| v.as_u64()) != Some(6)
                || frozen
                    .pointer("/identity/revision")
                    .and_then(|v| v.as_u64())
                    != Some(intent.expected_revision)
                || intent.proposed.agent_id != head.agent_id
                || intent.proposed.revision != intent.expected_revision + 1
                || intent.proposed.source
                    != (IdentitySource::Subject {
                        step_id: step.step_id,
                        step_no: step.step_no,
                    })
            {
                return Err(ShenkoError::conflict(
                    "identity intent is not bound to its S0 snapshot/source",
                ));
            }
            if actual_revision != Some(intent.expected_revision) {
                IdentityOutcome::Conflict {
                    expected_revision: intent.expected_revision,
                    actual_revision,
                }
            } else {
                crate::identity::insert(&tx, &intent.proposed)?;
                IdentityOutcome::Applied {
                    expected_revision: intent.expected_revision,
                    revision: intent.proposed.revision,
                }
            }
        } else {
            IdentityOutcome::Unchanged {
                revision: actual_revision,
            }
        };
        let identity_receipt = IdentityReceipt {
            step_id: step.step_id,
            intent_id: decision.identity_intent.as_ref().map(|i| i.intent_id),
            outcome,
            committed_at: commit_ref,
        };
        tx.execute(
            "INSERT INTO subject_identity_receipts (step_id,data,commit_seq) VALUES (?1,?2,?3)",
            params![
                step.step_id.to_string(),
                serde_json::to_string(&identity_receipt)?,
                seq.0 as i64
            ],
        )
        .map_err(storage_error)?;
        let mut step = step.clone();
        step.commit_ref = Some(commit_ref);
        let successor = SubjectStep {
            step_id: next_step_id,
            branch_id: head.branch_id.clone(),
            step_no: step.step_no + 1,
            expected_parent_commit: commit_ref,
            controller_epoch: head.controller_epoch,
            software_generation: head.software_generation.clone(),
            state: SubjectStepState::Planned,
            input_plan_ref: None,
            context_revision: None,
            active_request_id: None,
            active_attempt_id: None,
            model_result_ref: None,
            model_outcome_ref: None,
            decision_ref: None,
            commit_ref: None,
            retry_reason_ref: None,
            record_revision: RecordRevision(0),
            created_by_step_id: Some(step.step_id),
        };
        update_step(&tx, &step, previous.record_revision, seq.0)?;
        // 与 S1 同事务保存处置索引；I 只据此确认，不把送入内存 channel 当消费。
        // deferred 可由后续步继续处理，已接受的处置不能产生第二份决定/外部意图。
        for disposition in &decision.event_dispositions {
            let kind = serde_json::to_string(&disposition.disposition)?;
            let changed = tx.execute(
                "INSERT INTO subject_event_dispositions (branch_id,event_id,step_id,disposition,commit_seq)
                 VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT(branch_id,event_id) DO UPDATE SET
                     step_id=excluded.step_id, disposition=excluded.disposition, commit_seq=excluded.commit_seq
                 WHERE subject_event_dispositions.disposition='\"deferred\"'",
                params![head.branch_id.0.to_string(), disposition.event_id.to_string(),
                    step.step_id.to_string(), kind, seq.0 as i64],
            ).map_err(storage_error)?;
            if changed != 1 {
                return Err(ShenkoError::conflict(
                    "event already disposed by a committed step",
                ));
            }
        }
        self.enqueue_subject_intents(&tx, &head, &step, &decision, commit_ref, false)?;
        // INSERT 的全局 step_id 主键使后继身份冲突导致整笔回滚。
        insert_step(&tx, &successor, seq.0)?;
        head.committed_step_no = step.step_no;
        head.committed_step_id = Some(step.step_id);
        head.committed_at = commit_ref;
        head.next_step_id = next_step_id;
        head.record_revision = RecordRevision(head.record_revision.0 + 1);
        let changed = tx.execute(
            "UPDATE subject_branch_heads SET committed_step_no=?1, committed_step_id=?2, next_step_id=?3, record_revision=?4, data=?5, updated_commit_seq=?6
             WHERE agent_id=?7 AND branch_id=?8 AND record_revision=?9 AND next_step_id=?10",
            params![head.committed_step_no as i64, step.step_id.to_string(), next_step_id.to_string(), head.record_revision.0 as i64,
                serde_json::to_string(&head)?, seq.0 as i64, head.agent_id.to_string(), head.branch_id.0.to_string(),
                expected_head.record_revision.0 as i64, expected_head.next_step_id.to_string()],
        ).map_err(storage_error)?;
        if changed != 1 {
            return Err(ShenkoError::conflict("stale branch head"));
        }
        self.record_subject_commit(&tx, commit_ref, 3)?;
        tx.commit().map_err(storage_error)?;
        Ok(self.subject_receipt(
            commit_ref,
            vec![
                UpdatedEntity {
                    entity_id: step.step_id,
                    record_revision: step.record_revision,
                },
                UpdatedEntity {
                    entity_id: head.agent_id,
                    record_revision: head.record_revision,
                },
                UpdatedEntity {
                    entity_id: successor.step_id,
                    record_revision: successor.record_revision,
                },
            ],
        ))
    }

    /// Read terminal evidence bound to this exact result/request, independently
    /// of whether it is complete enough to admit actions.
    pub fn recorded_model_outcome(
        &self,
        step: &SubjectStep,
    ) -> Result<shenko_types::subject::SubjectModelOutcome, ShenkoError> {
        let object = step.model_outcome_ref.as_ref().ok_or_else(|| {
            ShenkoError::conflict("missing model terminal evidence; migrate/reconcile explicitly")
        })?;
        let bytes = self.cas.read_object(&object.digest)?;
        if object.locator.is_empty()
            || bytes.len() as u64 != object.bytes
            || shenko_types::object::Digest::blake3(&bytes) != object.digest
        {
            return Err(ShenkoError::ObjectClosureIncomplete { missing_count: 1 });
        }
        let outcome: shenko_types::subject::SubjectModelOutcome = serde_json::from_slice(&bytes)?;
        if outcome.schema_version != 2
            || Some(outcome.request_id) != step.active_request_id
            || Some(&outcome.result_ref) != step.model_result_ref.as_ref()
        {
            return Err(ShenkoError::conflict(
                "model terminal evidence/result mismatch",
            ));
        }
        Ok(outcome)
    }

    fn verify_step_objects(&self, step: &SubjectStep) -> Result<(), ShenkoError> {
        for object in [
            &step.input_plan_ref,
            &step.model_result_ref,
            &step.model_outcome_ref,
            &step.decision_ref,
            &step.retry_reason_ref,
        ]
        .into_iter()
        .flatten()
        {
            let bytes = self.cas.read_object(&object.digest)?;
            if object.locator.is_empty()
                || bytes.len() as u64 != object.bytes
                || shenko_types::object::Digest::blake3(&bytes) != object.digest
            {
                return Err(ShenkoError::ObjectClosureIncomplete { missing_count: 1 });
            }
        }
        Ok(())
    }

    fn record_subject_commit(
        &self,
        tx: &rusqlite::Transaction<'_>,
        commit: CommitRef,
        count: i64,
    ) -> Result<(), ShenkoError> {
        tx.execute("INSERT INTO commit_log (ledger_id, seq, incarnation, entity_count) VALUES (?1,?2,?3,?4)",
            params![commit.ledger_id.to_string(), commit.seq.0 as i64, self.capabilities.incarnation.to_string(), count])
            .map_err(storage_error)?;
        Ok(())
    }

    fn subject_receipt(&self, commit_ref: CommitRef, updated: Vec<UpdatedEntity>) -> CommitReceipt {
        CommitReceipt {
            commit_ref,
            durability_profile: self.capabilities.durability_profile,
            store_incarnation: self.capabilities.incarnation,
            updated,
        }
    }

    /// 当前 schema 每个 agent 只保存一个本地 branch；读取实际记录，不猜 branch。
    pub fn load_subject(
        &self,
        agent_id: EntityId,
    ) -> Result<(SubjectBranchHead, SubjectStep), ShenkoError> {
        let mut conn = self.ledger.conn.lock();
        let tx = conn.transaction().map_err(storage_error)?;
        let json: String = tx
            .query_row(
                "SELECT data FROM subject_branch_heads WHERE agent_id=?1",
                [agent_id.to_string()],
                |r| r.get(0),
            )
            .map_err(storage_error)?;
        let head: SubjectBranchHead = serde_json::from_str(&json)?;
        if head.agent_id != agent_id {
            return Err(ShenkoError::conflict("stored head identity mismatch"));
        }
        let head = checked_head(&tx, &head)?;
        let step = read_step(&tx, &head.branch_id, head.next_step_id)?.ok_or_else(|| {
            ShenkoError::not_found("head.next_step_id has no stored successor; recovery required")
        })?;
        validate_step_binding(&head, &step)?;
        tx.commit().map_err(storage_error)?;
        Ok((head, step))
    }

    /// 返回当前分支已提交的非 deferred 处置；接收库 ACK 只是此真相的镜像。
    pub fn event_disposition_commit(
        &self,
        agent_id: EntityId,
        event_id: EntityId,
    ) -> Result<Option<CommitRef>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let seq: Option<u64> = conn
            .query_row(
                "SELECT d.commit_seq FROM subject_event_dispositions d
             JOIN subject_branch_heads h ON h.branch_id=d.branch_id
             WHERE h.agent_id=?1 AND d.event_id=?2 AND d.disposition!='\"deferred\"'",
                params![agent_id.to_string(), event_id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage_error)?;
        Ok(seq
            .map(|seq| CommitRef::new(self.ledger.ledger_id(), shenko_types::ids::LedgerSeq(seq))))
    }

    pub fn get_step(
        &self,
        branch_id: &BranchId,
        step_id: EntityId,
    ) -> Result<Option<SubjectStep>, ShenkoError> {
        read_step(&self.ledger.conn.lock(), branch_id, step_id)
    }

    /// 数据库错误不能冒充首次运行，否则会重复出生。
    pub fn get_existing_agent_id(&self) -> Result<Option<EntityId>, ShenkoError> {
        let conn = self.ledger.conn.lock();
        let result: Option<String> = conn
            .query_row("SELECT subject_id FROM subject_genesis LIMIT 1", [], |r| {
                r.get(0)
            })
            .optional()
            .map_err(storage_error)?;
        result
            .map(|id| {
                id.parse()
                    .map_err(|_| ShenkoError::storage("invalid stored agent_id"))
            })
            .transpose()
    }

    /// Read the latest committed OperationRecord by id. SPEC §41.4 X03.
    pub fn get_operation(
        &self,
        operation_id: shenko_types::ids::EntityId,
    ) -> Result<Option<shenko_types::operation::OperationRecord>, shenko_types::error::ShenkoError>
    {
        let conn = self.ledger.conn.lock();
        let data: Option<String> = conn
            .query_row(
                "SELECT data FROM operations WHERE operation_id = ?1",
                [operation_id.to_string()],
                |r| r.get(0),
            )
            .ok();
        drop(conn);
        match data {
            Some(json) => serde_json::from_str(&json).map(Some).map_err(|e| {
                shenko_types::error::ShenkoError::storage(format!("bad operation record: {e}"))
            }),
            None => Ok(None),
        }
    }

    /// Newest tasks for the WebUI 任务 page. Read-only; no schema change.
    ///
    /// `kind` and `state` are lifted from the stored `TaskRecord` JSON; `state`
    /// is the serde tag of `TaskState` (e.g. `running`). A row whose JSON is
    /// unreadable keeps its id/created_at and reports `unknown` rather than
    /// failing the whole list.
    pub fn list_recent_tasks(&self, limit: usize) -> Result<Vec<RecentTask>, ShenkoError> {
        let limit = limit.clamp(1, 500) as i64;
        let conn = self.ledger.conn.lock();
        let mut stmt = conn
            .prepare(
                "SELECT task_id, data, created_at FROM tasks \
                 ORDER BY created_at DESC, task_id DESC LIMIT ?1",
            )
            .map_err(|e| ShenkoError::storage(e.to_string()))?;
        let rows = stmt
            .query_map([limit], |r| {
                let task_id: String = r.get(0)?;
                let data: String = r.get(1)?;
                let created_at: String = r.get(2)?;
                Ok((task_id, data, created_at))
            })
            .map_err(|e| ShenkoError::storage(e.to_string()))?;

        let mut out = Vec::new();
        for row in rows {
            let (task_id, data, created_at) =
                row.map_err(|e| ShenkoError::storage(e.to_string()))?;
            let v: serde_json::Value = serde_json::from_str(&data).unwrap_or_default();
            let kind = v
                .get("kind")
                .and_then(|k| k.as_str())
                .unwrap_or("unknown")
                .to_string();
            let state = v
                .get("state")
                .and_then(|s| s.get("status"))
                .and_then(|s| s.as_str())
                .unwrap_or("unknown")
                .to_string();
            out.push(RecentTask {
                task_id,
                kind,
                state,
                created_at,
            });
        }
        Ok(out)
    }
}

/// One row of the WebUI task list. SPEC §21, §41.4 X02.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RecentTask {
    pub task_id: String,
    pub kind: String,
    pub state: String,
    pub created_at: String,
}

/// Apply required SQLite settings. SPEC §41.2.1
pub fn configure_sqlite(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=FULL;
        PRAGMA foreign_keys=ON;
        PRAGMA busy_timeout=5000;
    ",
    )?;
    Ok(())
}

fn storage_error(error: rusqlite::Error) -> ShenkoError {
    ShenkoError::storage(error.to_string())
}

fn read_step(
    conn: &Connection,
    branch: &BranchId,
    id: EntityId,
) -> Result<Option<SubjectStep>, ShenkoError> {
    let json: Option<String> = conn
        .query_row(
            "SELECT data FROM subject_steps WHERE step_id=?1 AND branch_id=?2",
            params![id.to_string(), branch.0.to_string()],
            |r| r.get(0),
        )
        .optional()
        .map_err(storage_error)?;
    json.map(|json| serde_json::from_str(&json).map_err(ShenkoError::from))
        .transpose()
}

fn checked_head(
    conn: &Connection,
    expected: &SubjectBranchHead,
) -> Result<SubjectBranchHead, ShenkoError> {
    let row: Option<(String, i64, String)> = conn.query_row(
        "SELECT data, record_revision, next_step_id FROM subject_branch_heads WHERE agent_id=?1 AND branch_id=?2",
        params![expected.agent_id.to_string(), expected.branch_id.0.to_string()], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).optional().map_err(storage_error)?;
    let (json, revision, next) =
        row.ok_or_else(|| ShenkoError::conflict("branch head not found"))?;
    let head: SubjectBranchHead = serde_json::from_str(&json)?;
    if head.agent_id != expected.agent_id
        || head.branch_id != expected.branch_id
        || head.record_revision != expected.record_revision
        || revision as u64 != head.record_revision.0
        || next != head.next_step_id.to_string()
        || head.next_step_id != expected.next_step_id
        || head.committed_at != expected.committed_at
        || head.committed_step_no != expected.committed_step_no
        || head.committed_step_id != expected.committed_step_id
        || head.controller_epoch != expected.controller_epoch
        || head.software_generation != expected.software_generation
        || head.mode != SubjectMode::Active
    {
        return Err(ShenkoError::conflict(
            "stale/inactive branch head or controller",
        ));
    }
    Ok(head)
}

fn validate_step_binding(head: &SubjectBranchHead, step: &SubjectStep) -> Result<(), ShenkoError> {
    if step.step_id != head.next_step_id
        || step.branch_id != head.branch_id
        || step.step_no != head.committed_step_no + 1
        || step.expected_parent_commit != head.committed_at
        || step.controller_epoch != head.controller_epoch
        || step.software_generation != head.software_generation
    {
        return Err(ShenkoError::conflict(
            "step does not match registered head/parent/epoch",
        ));
    }
    Ok(())
}

fn insert_step(
    tx: &rusqlite::Transaction<'_>,
    step: &SubjectStep,
    seq: u64,
) -> Result<(), ShenkoError> {
    let state = serde_json::to_value(step.state)?
        .as_str()
        .unwrap()
        .to_owned();
    tx.execute("INSERT INTO subject_steps (step_id, branch_id, step_no, state, record_revision, data, updated_commit_seq) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![step.step_id.to_string(), step.branch_id.0.to_string(), step.step_no as i64, state,
            step.record_revision.0 as i64, serde_json::to_string(step)?, seq as i64]).map_err(storage_error)?;
    Ok(())
}

fn update_step(
    tx: &rusqlite::Transaction<'_>,
    step: &SubjectStep,
    expected: RecordRevision,
    seq: u64,
) -> Result<(), ShenkoError> {
    let state = serde_json::to_value(step.state)?
        .as_str()
        .unwrap()
        .to_owned();
    let changed = tx.execute("UPDATE subject_steps SET state=?1, record_revision=?2, data=?3, updated_commit_seq=?4 WHERE step_id=?5 AND branch_id=?6 AND record_revision=?7",
        params![state, step.record_revision.0 as i64, serde_json::to_string(step)?, seq as i64,
            step.step_id.to_string(), step.branch_id.0.to_string(), expected.0 as i64]).map_err(storage_error)?;
    if changed != 1 {
        return Err(ShenkoError::conflict("stale subject step"));
    }
    Ok(())
}
