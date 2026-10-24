//! Durable QQ outbound operation ledger — SPEC §35.6, §41.4 X03
//!
//! The invariant is: **persist the outbound intent before the bytes leave the
//! process**. A send whose response never arrives is not a failure — it is an
//! *unknown* external effect, recorded for reconciliation and never blindly
//! resent.
//!
//! Mapping onto `OperationRecord` (shenko-types §41.4 X03):
//!   intent recorded            → state `prepared`,  effect `none`
//!   echo received (retcode ok) → state `resolved`,  outcome `succeeded`, effect `complete`
//!   echo received (retcode !0) → state `resolved`,  outcome `failed`,    effect `none`
//!   no echo / disconnect       → state `reconciling`, effect `unknown`
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use shenko_store::store::Store;
use shenko_types::commit::CommitRef;
use shenko_types::ids::{EntityId, LedgerSeq, RecordRevision};
use shenko_types::object::{ArtifactRef, Digest, ObjectRef, SchemaRef, VersionedRef};
use shenko_types::operation::{
    AuthorityRef, EffectStatus, OperationContractRef, OperationOutcome, OperationRecord,
    OperationState, ResourceBindings,
};

/// Which OneBot action this operation performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboundKind {
    SendGroup,
    SendPrivate,
}

impl OutboundKind {
    pub fn as_str(self) -> &'static str {
        match self {
            OutboundKind::SendGroup => "send_group",
            OutboundKind::SendPrivate => "send_private",
        }
    }

    pub fn action(self) -> &'static str {
        match self {
            OutboundKind::SendGroup => "send_group_msg",
            OutboundKind::SendPrivate => "send_private_msg",
        }
    }
}

/// Lifecycle of one outbound operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum OutboundState {
    /// Intent recorded; the action was handed to the transport.
    Sent,
    /// NapCat answered with a retcode. `retcode == 0` means accepted.
    Acked { retcode: i64 },
    /// No answer within the response window (or the socket dropped first).
    /// The effect status is genuinely unknown.
    Unknown { since: DateTime<Utc> },
}

/// One outbound QQ message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboundOperation {
    pub operation_id: EntityId,
    pub kind: OutboundKind,
    pub target: u64,
    pub message: String,
    /// OneBot `echo` token; correlates the wire response with this operation.
    pub echo_token: String,
    pub state: OutboundState,
    pub created_at: DateTime<Utc>,
}

impl OutboundOperation {
    pub fn new(kind: OutboundKind, target: u64, message: impl Into<String>) -> Self {
        Self {
            operation_id: EntityId::new(),
            kind,
            target,
            message: message.into(),
            echo_token: uuid::Uuid::new_v4().to_string(),
            state: OutboundState::Sent,
            created_at: Utc::now(),
        }
    }

    /// `(task_id, logical_key)` is the uniqueness key of an operation. There is
    /// no user task for a channel reply, so the task component is nil and the
    /// echo token makes the key unique.
    pub fn logical_key(&self) -> String {
        format!(
            "qq.{}:{}:{}",
            self.kind.as_str(),
            self.target,
            self.echo_token
        )
    }
}

/// Resolved-or-not view used after a restart to answer "what happened?".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum AckState {
    Acked { retcode: i64 },
    Failed { retcode: i64 },
    Unknown { since: DateTime<Utc> },
}

/// In-memory operation ledger plus an optional durable store.
pub struct OutboundLedger {
    ops: Mutex<HashMap<EntityId, OutboundOperation>>,
    store: Option<Arc<Store>>,
    /// Bot QQ number, recorded as the account resource identity.
    account: u64,
}

impl Default for OutboundLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl OutboundLedger {
    /// In-memory only — used by tests and by transports without a store.
    pub fn new() -> Self {
        Self {
            ops: Mutex::new(HashMap::new()),
            store: None,
            account: 0,
        }
    }

    /// Durable ledger writing `OperationRecord`s into the shared store.
    pub fn with_store(store: Arc<Store>) -> Self {
        Self {
            ops: Mutex::new(HashMap::new()),
            store: Some(store),
            account: 0,
        }
    }

    pub fn with_account(mut self, account: u64) -> Self {
        self.account = account;
        self
    }

    pub fn get(&self, operation_id: EntityId) -> Option<OutboundOperation> {
        self.ops.lock().get(&operation_id).cloned()
    }

    /// T1 through the shared Task/Operation service. Only `true` authorizes
    /// the caller to send. Repeated admission, including after reopen, is a read.
    pub fn record_intent(&self, op: &OutboundOperation) -> anyhow::Result<bool> {
        if let Some(store) = &self.store {
            let intent = store.subject_publication(op.operation_id)?;
            let target = match intent.target {
                shenko_types::output::PublicationTarget::QqGroup(id) => {
                    (OutboundKind::SendGroup, id)
                }
                shenko_types::output::PublicationTarget::QqPrivate(id) => {
                    (OutboundKind::SendPrivate, id)
                }
            };
            anyhow::ensure!(
                target == (op.kind, op.target)
                    && intent.content == op.message
                    && op.echo_token == op.operation_id.to_string(),
                "publication_payload_conflict"
            );
            let Some(ctx) = store.claim_subject_publication(op.operation_id)? else {
                return Ok(false);
            };
            let mut record = operation_record(store, self.account, op)?;
            record.task_id = ctx.task_id;
            record.authority_ref.delegation_id = ctx.task_id;
            record.authority_ref.decision_ref = store
                .external_task(ctx.task_id)?
                .created_by_step_id
                .unwrap();
            record.state = OperationState::InFlight;
            let mut attempt = shenko_types::operation::AttemptRecord {
                record_schema_version: 1,
                attempt_id: EntityId::new(),
                operation_id: op.operation_id,
                attempt_no: 1,
                invocation_id: ctx.invocation_id,
                worker_id: ctx.worker_id,
                node_id: ctx.node_id,
                epoch: ctx.epoch,
                implementation_ref: record.contract_ref.receipt_reader_ref.clone(),
                dispatch_stage: shenko_types::operation::AttemptDispatchStage::Armed,
                armed_commit: record.prepared_commit,
                provider_handle: None,
                provider_idempotency: None,
                transport_ref: Some(record.resource_bindings_ref.clone()),
                started_at: None,
                last_observed_at: None,
                final_receipt_id: None,
            };
            if store
                .arm_external_operation(&ctx, &mut record, &mut attempt)?
                .is_some()
            {
                return Ok(false);
            }
        } else if let Some(old) = self.ops.lock().get(&op.operation_id) {
            anyhow::ensure!(
                old.kind == op.kind && old.target == op.target && old.message == op.message,
                "publication_payload_conflict"
            );
            return Ok(false);
        }
        self.ops.lock().insert(op.operation_id, op.clone());
        Ok(true)
    }

    /// E1 retains the complete provider response, including platform message ID.
    /// It is valid even when the callback's dispatcher has lost ownership.
    pub fn record_response(
        &self,
        id: EntityId,
        receipt: &crate::client::SendReceipt,
    ) -> anyhow::Result<()> {
        let success = receipt.is_success();
        self.evidence(
            id,
            serde_json::to_value(receipt)?,
            Some(if success {
                OperationOutcome::Succeeded
            } else {
                OperationOutcome::Failed
            }),
            if success {
                EffectStatus::Complete
            } else {
                EffectStatus::None
            },
        )?;
        if let Some(op) = self.ops.lock().get_mut(&id) {
            op.state = OutboundState::Acked {
                retcode: receipt.retcode,
            };
        }
        Ok(())
    }

    pub fn record_unknown(&self, id: EntityId, reason: &str) -> anyhow::Result<()> {
        self.evidence(
            id,
            serde_json::json!({"transport_error":reason,"effect":"unknown"}),
            None,
            EffectStatus::Unknown,
        )?;
        if let Some(op) = self.ops.lock().get_mut(&id) {
            op.state = OutboundState::Unknown { since: Utc::now() };
        }
        Ok(())
    }

    fn evidence(
        &self,
        id: EntityId,
        raw: serde_json::Value,
        outcome: Option<OperationOutcome>,
        effect: EffectStatus,
    ) -> anyhow::Result<()> {
        if let Some(store) = &self.store {
            let attempt = store.external_operation_attempt(id)?;
            let object = seal_json(store, "qq_outbound_receipts", &raw)?;
            let ev = shenko_types::evidence::EvidenceReceipt {
                record_schema_version: 1,
                receipt_id: EntityId::new(),
                operation_id: Some(id),
                attempt_id: Some(attempt.attempt_id),
                source_id: "napcat".into(),
                source_instance_id: format!("qq:{}", self.account),
                source_event_key: None,
                producer_epoch: Some(attempt.epoch.0),
                producer_schema_version: Some("onebot-v11".into()),
                raw_ref: object,
                decoder_ref: Some(attempt.implementation_ref.clone()),
                decode_status: shenko_types::evidence::DecodeStatus::Decoded,
                observation_level: outcome
                    .map(|_| shenko_types::evidence::ObservationLevel::ProviderAccepted),
                normalized_ref: None,
                observed_at: Some(Utc::now()),
                received_commit: attempt.armed_commit,
            };
            store.record_external_evidence(id, attempt.attempt_id, ev, outcome, effect)?;
        }
        Ok(())
    }

    /// Resolve the durable state of an operation, e.g. after a restart.
    /// Queries the store first, then the in-memory ledger.
    pub fn reconcile(&self, operation_id: EntityId) -> anyhow::Result<Option<AckState>> {
        if let Some(store) = &self.store {
            let record = store
                .get_operation(operation_id)
                .map_err(|e| anyhow::anyhow!("read operation {operation_id}: {e}"))?;
            if let Some(record) = record {
                return Ok(Some(ack_from_record(&record)));
            }
        }
        Ok(self
            .ops
            .lock()
            .get(&operation_id)
            .map(|op| ack_from_state(&op.state, op.created_at)))
    }
}

fn ack_from_state(state: &OutboundState, created_at: DateTime<Utc>) -> AckState {
    match state {
        OutboundState::Acked { retcode: 0 } => AckState::Acked { retcode: 0 },
        OutboundState::Acked { retcode } => AckState::Failed { retcode: *retcode },
        OutboundState::Unknown { since } => AckState::Unknown { since: *since },
        OutboundState::Sent => AckState::Unknown { since: created_at },
    }
}

fn ack_from_record(record: &OperationRecord) -> AckState {
    if record.effect_status == EffectStatus::Complete
        || record.outcome == Some(OperationOutcome::Succeeded)
    {
        return AckState::Acked { retcode: 0 };
    }
    if record.outcome == Some(OperationOutcome::Failed)
        || record.outcome == Some(OperationOutcome::Cancelled)
    {
        // The exact provider retcode is not part of OperationRecord; the
        // committed outcome is what matters after a restart.
        return AckState::Failed { retcode: -1 };
    }
    AckState::Unknown { since: Utc::now() }
}

fn seal_json(
    store: &Store,
    namespace: &str,
    value: &serde_json::Value,
) -> anyhow::Result<ObjectRef> {
    let bytes = serde_json::to_vec(value)?;
    let sealed = store
        .cas
        .seal_object(&bytes, "application/json", namespace, "json", 1)
        .map_err(|e| anyhow::anyhow!("CAS seal {namespace}: {e}"))?;
    Ok(sealed.object)
}

fn operation_record(
    store: &Store,
    account: u64,
    op: &OutboundOperation,
) -> anyhow::Result<OperationRecord> {
    let payload = serde_json::json!({
        "kind": op.kind.as_str(),
        "target": op.target.to_string(),
        "message": op.message,
        "echo_token": op.echo_token,
    });
    let input = seal_json(store, "qq_outbound_input", &payload)?;

    let bindings = serde_json::to_vec(&ResourceBindings {
        provider: "napcat".to_string(),
        account_identity: format!("qq:{account}"),
        runtime_node: "local".to_string(),
        mount_generation: 0,
        capability_level: "send_message".to_string(),
        extra: serde_json::json!({ "kind": op.kind.as_str(), "target": op.target.to_string() }),
    })?;
    let resource_bindings_ref = store
        .cas
        .seal_object(
            &bindings,
            "application/json",
            "qq_outbound_bindings",
            "json",
            1,
        )
        .map_err(|e| anyhow::anyhow!("CAS seal resource bindings: {e}"))?
        .object;

    let acceptance_predicate_ref = seal_json(
        store,
        "qq_outbound_predicates",
        &serde_json::json!({ "predicate": "onebot_retcode_zero", "provider": "napcat" }),
    )?;
    let retry_contract_ref = seal_json(
        store,
        "qq_outbound_predicates",
        &serde_json::json!({
            "max_attempts": 1,
            "on_timeout": "reconcile",
            "blind_resend": false,
        }),
    )?;

    let record = OperationRecord {
        record_schema_version: 1,
        operation_id: op.operation_id,
        record_revision: RecordRevision(0),
        // Channel replies are not tied to a user task.
        task_id: EntityId::nil(),
        logical_key: op.logical_key(),
        operation_name: "qq.send".to_string(),
        input_schema_version: 1,
        output_schema_version: 1,
        final_input_ref: input.clone(),
        input_digest: input.digest.clone(),
        contract_ref: OperationContractRef {
            plugin_id: EntityId::nil(),
            artifact_digest: Digest::blake3(include_bytes!("outbound.rs")),
            contract_digest: Digest::blake3(
                b"shenko.napcat/onebot11-send/1;no-blind-replay;provider-accepted-not-read",
            ),
            receipt_reader_ref: VersionedRef {
                artifact: ArtifactRef {
                    digest: Digest::blake3(include_bytes!("outbound.rs")),
                    locator: "crates/shenko-channel-napcat".to_string(),
                },
                symbol: "SendReceipt".to_string(),
                schema: SchemaRef {
                    name: "onebot_v11_response".to_string(),
                    major: 1,
                    digest: Digest::blake3(b"onebot_v11_response"),
                },
            },
        },
        resource_bindings_ref,
        authority_ref: AuthorityRef {
            // A channel reply is an ordinary subject action; no delegated
            // authority record exists yet, so the nil delegation is explicit.
            delegation_id: EntityId::nil(),
            delegation_revision: RecordRevision(0),
            decision_ref: EntityId::nil(),
        },
        acceptance_predicate_ref,
        retry_contract_ref,
        checkpoint_scope_ref: None,
        state: OperationState::InFlight,
        outcome: None,
        outcome_conflict: false,
        effect_status: EffectStatus::Unknown,
        retry_of: None,
        compensates: None,
        possible_duplicate_of: vec![],
        // The commit that durably carries this record is the one produced by
        // committing this batch; `seq 0` denotes "prepared before the write".
        prepared_commit: CommitRef::new(store.ledger.ledger_id(), LedgerSeq::ZERO),
        evidence_through: None,
    };

    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intent_then_ack_resolves_operation() {
        let ledger = OutboundLedger::new();
        let op = OutboundOperation::new(OutboundKind::SendGroup, 123456, "hello");
        let id = op.operation_id;

        ledger.record_intent(&op).expect("record intent");
        assert_eq!(
            ledger.get(id).expect("op present").state,
            OutboundState::Sent
        );
        assert_eq!(
            ledger.reconcile(id).expect("reconcile"),
            Some(AckState::Unknown {
                since: op.created_at
            }),
            "before the ack the external effect is unknown"
        );

        ledger
            .record_response(
                id,
                &crate::client::SendReceipt {
                    echo: id.to_string(),
                    status: "ok".into(),
                    retcode: 0,
                    data: serde_json::json!({"fixture":true}),
                    message: None,
                },
            )
            .expect("record ack");
        assert_eq!(
            ledger.get(id).expect("op present").state,
            OutboundState::Acked { retcode: 0 }
        );
        assert_eq!(
            ledger.reconcile(id).expect("reconcile"),
            Some(AckState::Acked { retcode: 0 })
        );
    }

    #[test]
    fn unknown_outcome_reconciles_without_blind_resend() {
        let ledger = OutboundLedger::new();
        let op = OutboundOperation::new(OutboundKind::SendPrivate, 998877, "hi");
        let id = op.operation_id;

        ledger.record_intent(&op).expect("record intent");
        ledger
            .record_unknown(id, "fixture disconnect")
            .expect("record unknown");

        let state = ledger.get(id).expect("op present").state;
        assert!(matches!(state, OutboundState::Unknown { .. }));

        // Reconciliation reports the unknown effect and leaves the operation
        // untouched: the caller must decide, not resend automatically.
        assert!(matches!(
            ledger.reconcile(id).expect("reconcile"),
            Some(AckState::Unknown { .. })
        ));
        assert!(matches!(
            ledger.get(id).expect("op present").state,
            OutboundState::Unknown { .. }
        ));
    }
}
