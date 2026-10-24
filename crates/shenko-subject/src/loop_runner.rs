//! Continuous subject loop — SPEC §5.1, §41.5
//! Real S0/S1 step state machine. Never terminates on empty inbox.
//! Semantic wait hard-max 600s. Yields inference slot after each step.
use crate::inbox::{Inbox, InboxEvent};
use crate::outbox::OutboxMessage;
use shenko_identity::IdentityService;
use shenko_kernel::hooks::{HookPayload, HookRegistry};
use shenko_types::commit::CommitRef;
use shenko_types::error::ShenkoError;
use shenko_types::identity::{
    IdentityIntent, IdentityOutcome, IdentityProfile, IdentityReceipt, IdentitySource,
};
use shenko_types::ids::{EntityId, RecordRevision};
use shenko_types::object::ObjectRef;
use shenko_types::output::OutputAction;
use shenko_types::subject::{
    DelegationIntent, Disposition, EventDisposition, InferenceRetryFact, MemoryIntent,
    MemoryIntentKind, SemanticWait, StepDecision, SubjectBranchHead, SubjectRetryEvidence,
    SubjectStep, SubjectStepState,
};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Recoverable S0 input, including the immutable request projection and original
/// event payloads. A list of UUIDs alone cannot reconstruct a result's dispositions.
#[derive(serde::Serialize, serde::Deserialize)]
struct FrozenStepInput {
    schema_version: u32,
    step_id: EntityId,
    step_no: u64,
    events: Vec<InboxEvent>,
    provider_binding: Option<shenko_broker::request::ProviderBinding>,
    payload: shenko_broker::request::InferencePayload,
    in_semantic_wait: bool,
    identity: Option<IdentityProfile>,
    identity_provider_revision: String,
}

fn read_sealed(
    store: &shenko_store::store::Store,
    object: &ObjectRef,
) -> Result<Vec<u8>, ShenkoError> {
    let bytes = store.cas.read_object(&object.digest)?;
    if object.locator.is_empty()
        || bytes.len() as u64 != object.bytes
        || shenko_types::object::Digest::blake3(&bytes) != object.digest
    {
        return Err(ShenkoError::ObjectClosureIncomplete { missing_count: 1 });
    }
    Ok(bytes)
}

fn frozen_input(
    store: &shenko_store::store::Store,
    step: &SubjectStep,
) -> Result<FrozenStepInput, ShenkoError> {
    let object = step
        .input_plan_ref
        .as_ref()
        .ok_or_else(|| ShenkoError::conflict("recovery_blocked: missing frozen input"))?;
    let input: FrozenStepInput = serde_json::from_slice(&read_sealed(store, object)?)
        .map_err(|_| ShenkoError::conflict("recovery_blocked: obsolete/incomplete S0 protocol: explicit reconciliation/migration required"))?;
    if input.schema_version != 6 || input.step_id != step.step_id || input.step_no != step.step_no {
        return Err(ShenkoError::conflict(
            "recovery_blocked: frozen input identity mismatch",
        ));
    }
    Ok(input)
}

fn validate_resume(
    store: &shenko_store::store::Store,
    step: &SubjectStep,
) -> Result<Option<FrozenStepInput>, ShenkoError> {
    use SubjectStepState::*;
    let no_request = step.active_request_id.is_none() && step.active_attempt_id.is_none();
    match step.state {
        Planned if no_request => return Ok(None),
        Preparing if no_request => {}
        RetryPending => {
            let object = step
                .retry_reason_ref
                .as_ref()
                .ok_or_else(|| ShenkoError::conflict("recovery_blocked: missing retry evidence"))?;
            let evidence: SubjectRetryEvidence =
                serde_json::from_slice(&read_sealed(store, object)?)?;
            if step.active_attempt_id.is_some() || !evidence.permits_retry(step.active_request_id) {
                return Err(ShenkoError::conflict(
                    "recovery_blocked: retry outcome not conclusive",
                ));
            }
        }
        ResultRecorded if step.active_request_id.is_some() => {
            if step.model_result_ref.is_none() || step.model_outcome_ref.is_none() {
                return Err(ShenkoError::conflict(
                    "recovery_blocked: missing terminal model evidence; explicit migration required",
                ));
            }
        }
        _ => {
            return Err(ShenkoError::conflict(format!(
                "recovery_blocked: step {} is {:?}; reconcile original request/attempt before inference",
                step.step_id, step.state,
            )));
        }
    }
    frozen_input(store, step).map(Some)
}

pub struct SubjectLoopState {
    pub agent_id: EntityId,
    pub head: SubjectBranchHead,
    pub step: SubjectStep,
    pub inbox: Arc<Inbox>,
    pub active_wait: Option<SemanticWait>,
    pub store: Arc<shenko_store::store::Store>,
    pub broker: Option<shenko_broker::broker::BrokerHandle>,
    pub hooks: Option<Arc<HookRegistry>>,
    pub max_tokens: u64,
    /// Last model output — feeds the next step's attention context.
    pub last_thought: Option<String>,
    /// Rolling goals the subject is pursuing.
    pub goals: Vec<String>,
    /// Outbound path for completed model output — SPEC §5.1, §21.12.
    pub outbox_tx: Option<tokio::sync::broadcast::Sender<OutboxMessage>>,
    /// Identity revision fixed for the current step — SPEC §6. Reloaded only
    /// at the S0 boundary, so a change applies from the next step on.
    pub identity: Option<IdentityProfile>,
    pub identity_receipt: Option<IdentityReceipt>,
    identity_context: cordis_core::Context,
    identity_service: Arc<IdentityService>,
}

impl SubjectLoopState {
    /// 从真实 head/唯一后继恢复；未知模型状态不允许重新发起推理。
    pub fn new(
        agent_id: EntityId,
        inbox: Arc<Inbox>,
        store: Arc<shenko_store::store::Store>,
        identity_context: cordis_core::Context,
    ) -> Result<Self, ShenkoError> {
        let (head, step) = store.load_subject(agent_id)?;
        let resumed_input = validate_resume(&store, &step)?;
        let identity_service = identity_context
            .try_service::<IdentityService>()
            .map_err(|e| ShenkoError::other(format!("IDENTITY_PROVIDER_UNAVAILABLE: {e}")))?;
        if resumed_input
            .as_ref()
            .is_some_and(|input| input.identity_provider_revision != identity_service.revision())
        {
            return Err(ShenkoError::conflict(
                "IDENTITY_PROVIDER_REVISION_MISMATCH: migrate/reconcile the sealed request before resuming",
            ));
        }
        let last_thought = if let Some(id) = head.committed_step_id {
            let previous = store
                .get_step(&head.branch_id, id)?
                .ok_or_else(|| ShenkoError::not_found("committed subject step"))?;
            if previous.state != SubjectStepState::Committed
                || previous.commit_ref != Some(head.committed_at)
            {
                return Err(ShenkoError::conflict("committed head/step mismatch"));
            }
            previous
                .model_result_ref
                .map(|object| {
                    let bytes = read_sealed(&store, &object)?;
                    String::from_utf8(bytes).map_err(|e| ShenkoError::storage(e.to_string()))
                })
                .transpose()?
        } else {
            None
        };
        let identity = if step.state == SubjectStepState::Planned {
            store.latest_identity(agent_id)?
        } else {
            resumed_input.and_then(|input| input.identity)
        };
        let identity_receipt = head
            .committed_step_id
            .map(|id| store.identity_receipt(id))
            .transpose()?
            .flatten();
        Ok(Self {
            agent_id,
            head,
            step,
            inbox,
            active_wait: None,
            store,
            broker: None,
            hooks: None,
            max_tokens: 4096,
            last_thought,
            goals: vec![],
            outbox_tx: None,
            identity,
            identity_receipt,
            identity_context,
            identity_service,
        })
    }

    pub fn with_broker(mut self, broker: shenko_broker::broker::BrokerHandle) -> Self {
        self.broker = Some(broker);
        self
    }

    pub fn with_hooks(mut self, registry: Arc<HookRegistry>) -> Self {
        self.hooks = Some(registry);
        self
    }

    /// Attach the outbound timeline broadcaster — SPEC §5.1, §21.12.
    pub fn with_outbox(mut self, tx: tokio::sync::broadcast::Sender<OutboxMessage>) -> Self {
        self.outbox_tx = Some(tx);
        self
    }
}

/// Run the continuous subject loop. SPEC §5.1, §41.5.
/// - Never stops for empty inbox
/// - S0: seal SubjectStep as Planned → Preparing
/// - AwaitingModel: real inference via broker
/// - S1: commit as Committed, record next_step_id
/// - Semantic wait: max 600s, same model, same thought stream
pub async fn run_subject_loop(
    mut state: SubjectLoopState,
    cancel: tokio_util::sync::CancellationToken,
) -> Result<(), ShenkoError> {
    info!(agent_id=%state.agent_id, "subject loop started");

    let result = loop {
        if cancel.is_cancelled() {
            info!("subject loop: shutdown");
            break Ok(());
        }

        let seq = state.step.step_no;

        // Collect available events — short timeout to stay responsive
        let mut events = if state.step.state == SubjectStepState::Planned {
            state.inbox.recv_batch(32, 80).await
        } else {
            // Never consume new input into a recovered, already frozen step.
            vec![]
        };
        // An ingress replay may already be queued when ResultRecorded resumes S1.
        // Recheck L at consumption time so that stable IDs are never executed twice.
        let filtered = events
            .into_iter()
            .map(|event| {
                state
                    .store
                    .event_disposition_commit(state.agent_id, event.event_id)
                    .map(|commit| if commit.is_none() { Some(event) } else { None })
            })
            .collect::<Result<Vec<_>, _>>();
        events = match filtered {
            Ok(events) => events.into_iter().flatten().collect(),
            Err(error) => break Err(error),
        };
        let has_events = !events.is_empty();

        // Check semantic wait — SPEC §5.1/U55, §41.5
        let in_semantic_wait = match state.active_wait.as_ref() {
            Some(wait) if wait.is_expired() => {
                info!(seq, "semantic wait expired — resuming normal thought");
                state.active_wait = None;
                false
            }
            Some(_) if has_events => {
                state.active_wait = None;
                false
            }
            Some(_) => true,
            None => false,
        };

        // Execute one subject step (S0 → AwaitingModel → Committed)
        if let Err(error) = execute_step(seq, &events, &mut state, in_semantic_wait, &cancel).await
        {
            // 失败或结果未知后关闭本运行器；不能在旧内存状态中造一个新步骤。
            warn!(seq, err=%error, "主体恢复阻塞，停止新决定，等待重开和对账");
            break Err(error);
        }

        // Yield inference slot to children — cooperative multitasking. SPEC §5.1
        tokio::task::yield_now().await;
    };

    // Keeping Arc<Inbox> in main must not leave a dead consumer accepting input.
    state.inbox.close().await;
    info!("subject loop terminated");
    result
}

/// Role and output contract of the subject. It names the framework, never the
/// instance, and does not vary with identity revisions (R054). SPEC §5.1, §6.
const SUBJECT_CONTRACT: &str = "你是一个持续运行的个人 agent，运行在 Shenko 框架上。\n\
     你有持续的内心生活：关注、兴趣、承诺、疑问。\n\
     你在没有外部消息时也继续思考——空闲不是停止的理由。\n\
     思考是连续的，上一步的想法会成为这一步的起点。\n\
     回复 QQ 时输出 \"[reply:group:群号] 内容\" 或 \"[reply:private:QQ号] 内容\"。";

/// System prompt for the continuous subject: contract → persona → per-step
/// suffix. SPEC §5.1, §6 ordering; §13.5 keeps the stable part first.
fn build_system_prompt(state: &SubjectLoopState, in_semantic_wait: bool) -> String {
    let mut prompt = String::from(SUBJECT_CONTRACT);
    prompt.push_str("\n\n");
    prompt.push_str(crate::output::OUTPUT_CONTRACT);

    if let Some(identity) = &state.identity {
        prompt.push_str("\n\n");
        prompt.push_str(&state.identity_service.persona_section(identity));
    }

    if !state.goals.is_empty() {
        prompt.push_str("\n\n当前关注的方向：\n");
        for g in &state.goals {
            prompt.push_str(&format!("- {g}\n"));
        }
    }

    if let Some(receipt) = &state.identity_receipt {
        if matches!(receipt.outcome, IdentityOutcome::Conflict { .. }) {
            prompt.push_str(
                "\n上一步身份意图未采用：S0 后身份被编辑，请基于本轮快照重新判断。回执：",
            );
            prompt.push_str(&serde_json::to_string(receipt).expect("identity receipt serializes"));
        }
    }
    if let Some(prev) = &state.last_thought {
        prompt.push_str("\n\n你上一步的想法（继续这条思路，不要重新开始）：\n");
        prompt.push_str(prev);
        prompt.push('\n');
    }

    if in_semantic_wait {
        prompt.push_str("\n\n当前处于语义等待：条件尚未满足。继续思考相关可能性，不要空转。\n");
    }

    prompt
}

/// Build the message list from recent events + carried thought.
///
/// Event text may arrive under different keys depending on the source:
/// OneBot/NapCat uses `raw_message`, other channels use `message`, and the
/// WebUI sends `content` (see `shenko-ui/src/ws.rs`).
/// 批次内每条输入均呈现；无文本的事件保留 JSON，不能未呈现便在 S1 确认。
fn build_messages(
    _state: &SubjectLoopState,
    events: &[InboxEvent],
) -> Vec<shenko_broker::request::InferenceMessage> {
    let mut messages = vec![];

    for ev in events {
        let text = ["raw_message", "message", "content", "text"]
            .iter()
            .find_map(|key| ev.payload.get(*key).and_then(|v| v.as_str()))
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| ev.payload.to_string());
        let who = ev
            .payload
            .get("nickname")
            .and_then(|v| v.as_str())
            .unwrap_or(&ev.source);
        messages.push(shenko_broker::request::InferenceMessage {
            role: "user".to_string(),
            content: format!("[{who}] {text}"),
        });
    }

    if messages.is_empty() {
        // Continuous thought — no external input. SPEC §5.1
        messages.push(shenko_broker::request::InferenceMessage {
            role: "user".to_string(),
            content: "继续。".to_string(),
        });
    }

    messages
}

/// CAS 失败直接返回，绝不制造空 locator 后继续提交。§41.2.2
fn seal_step_object(
    store: &shenko_store::store::Store,
    _seq: u64,
    data: &[u8],
    media_type: &str,
    namespace: &str,
    codec_name: &str,
    codec_major: u32,
) -> Result<ObjectRef, ShenkoError> {
    Ok(store
        .cas
        .seal_object(data, media_type, namespace, codec_name, codec_major)?
        .object)
}

/// Execute one subject step: S0 (Planned→Preparing) → AwaitingModel → S1 (Committed).
/// Returns the commit ref if S1 succeeded.
async fn execute_step(
    seq: u64,
    events: &[InboxEvent],
    state: &mut SubjectLoopState,
    in_semantic_wait: bool,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Option<CommitRef>, ShenkoError> {
    let store = Arc::clone(&state.store);
    let hooks_ref = state.hooks.clone(); // Option<Arc<HookRegistry>> — cloned to avoid reborrow

    let step_id = state.step.step_id;
    let next_step_id = EntityId::new();
    let mut step = state.step.clone();
    if step.step_no != seq {
        return Err(ShenkoError::conflict(
            "step is not the registered successor",
        ));
    }
    let resumed_input = validate_resume(&store, &step)?;
    let input = if step.state == SubjectStepState::Planned {
        state.identity_service = state
            .identity_context
            .try_service::<IdentityService>()
            .map_err(|e| ShenkoError::other(format!("IDENTITY_PROVIDER_UNAVAILABLE: {e}")))?;
        state.identity = store.latest_identity(state.agent_id)?;
        let input = FrozenStepInput {
            schema_version: 6,
            step_id,
            step_no: seq,
            events: events.to_vec(),
            provider_binding: state.broker.as_ref().map(|b| b.binding().clone()),
            payload: shenko_broker::request::InferencePayload {
                system_prompt: build_system_prompt(state, in_semantic_wait),
                messages: build_messages(state, events),
                max_tokens: Some(state.max_tokens),
                tools: vec![],
            },
            in_semantic_wait,
            identity: state.identity.clone(),
            identity_provider_revision: state.identity_service.revision().into(),
        };
        step.input_plan_ref = Some(seal_step_object(
            &store,
            seq,
            &serde_json::to_vec(&input)?,
            "application/json",
            "input_plans",
            "subject_step_input",
            6,
        )?);
        step.context_revision = Some(seq);
        step.state = SubjectStepState::Preparing;
        let expected = step.record_revision;
        step.record_revision = RecordRevision(expected.0 + 1);
        store.upsert_step(&step, &state.head, expected)?;
        state.step = step.clone();
        input
    } else {
        resumed_input.ok_or_else(|| ShenkoError::conflict("missing validated S0"))?
    };
    let events = input.events.as_slice();
    let in_semantic_wait = input.in_semantic_wait;
    let input_identity = input.identity.clone();
    let (model_output, stop_reason) = if step.state == SubjectStepState::ResultRecorded {
        // The actual sealed result is authoritative; no broker admission on this path.
        let object = step
            .model_result_ref
            .as_ref()
            .ok_or_else(|| ShenkoError::conflict("missing recorded result"))?;
        let outcome = store.recorded_model_outcome(&step)?;
        (
            Some(
                String::from_utf8(read_sealed(&store, object)?)
                    .map_err(|e| ShenkoError::storage(e.to_string()))?,
            ),
            outcome.stop_reason,
        )
    } else {
        if step.state == SubjectStepState::Preparing {
            if let Some(reg) = &hooks_ref {
                reg.run_phase(&HookPayload::BeforeInference {
                    request_id: step_id,
                    agent_id: state.agent_id,
                    model: input
                        .provider_binding
                        .as_ref()
                        .map(|binding| binding.model())
                        .unwrap_or("unbound")
                        .into(),
                })
                .await;
            }
        }
        // Close is not abort. Before dispatch, the frozen Preparing/RetryPending
        // fact is sufficient to resume. After dispatch, keep waiting for real evidence.
        if cancel.is_cancelled() {
            return Ok(None);
        }
        let broker = state
            .broker
            .as_ref()
            .ok_or_else(|| ShenkoError::other("no broker; frozen preparation remains resumable"))?;
        if input.provider_binding.as_ref() != Some(broker.binding()) {
            return Err(ShenkoError::Protocol(shenko_types::error::ProtocolError::new(
                "MODEL_PROVIDER_BINDING_MISMATCH", shenko_types::error::ErrorScope::Request,
                shenko_types::error::RetryStrategy::NewRevision,
            ).with_details(serde_json::json!({"reason":"explicit S0 migration/reconciliation required; request not dispatched"}))));
        }
        let request_id = step.active_request_id.unwrap_or_else(EntityId::new);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let req = shenko_broker::request::InferenceRequest {
            request_id,
            agent_id: state.agent_id,
            conversation_id: state.agent_id,
            provider_binding: broker.binding().clone(),
            priority: shenko_broker::request::InferencePriority::Subject,
            epoch: state.head.controller_epoch,
            payload: input.payload.clone(),
            result_tx: tx,
        };
        step.state = SubjectStepState::AwaitingModel;
        step.active_request_id = Some(request_id);
        let expected = step.record_revision;
        step.record_revision = RecordRevision(expected.0 + 1);
        store.upsert_step(&step, &state.head, expected)?;
        state.step = step.clone();
        // A failed channel send proves this admission was never enqueued. A crash
        // between the write and send cannot prove that, and stays AwaitingModel.
        if broker.submit(req).is_err() {
            record_retry(state, InferenceRetryFact::NeverDispatched)?;
            return Err(ShenkoError::other(
                "broker admission rejected; never_dispatched persisted",
            ));
        }
        let result = rx
            .await
            .map_err(|_| ShenkoError::other("model outcome unknown; reconcile original request"))?;
        if result.request_id != request_id {
            return Err(ShenkoError::conflict(
                "model result request identity mismatch",
            ));
        }
        use shenko_broker::request::InferenceStatus;
        match result.status {
            InferenceStatus::QueuedCapacityWait => {
                record_retry(state, InferenceRetryFact::NeverDispatched)?;
                return Err(ShenkoError::other(
                    "broker capacity rejection; never_dispatched persisted",
                ));
            }
            InferenceStatus::Rejected { http_status: 429 } => {
                record_retry(
                    state,
                    InferenceRetryFact::ProviderRejected { http_status: 429 },
                )?;
                return Err(ShenkoError::other(
                    "provider rejection persisted; same frozen request may resume",
                ));
            }
            InferenceStatus::Success => {}
            _ => {
                return Err(ShenkoError::other(
                    "model outcome not conclusive; preserve original awaiting_model for reconciliation",
                ));
            }
        }
        let response = result
            .response
            .ok_or_else(|| ShenkoError::other("successful model result missing response"))?;
        let output = response.content;
        step.model_result_ref = Some(seal_step_object(
            &store,
            seq,
            output.as_bytes(),
            "text/plain",
            "model_results",
            "text",
            1,
        )?);
        let outcome = shenko_types::subject::SubjectModelOutcome {
            schema_version: 2,
            request_id,
            result_ref: step.model_result_ref.clone().unwrap(),
            stop_reason: response.stop_reason.clone(),
            raw_response: response.raw_response,
            usage: response
                .usage
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
        };
        step.model_outcome_ref = Some(seal_step_object(
            &store,
            seq,
            &serde_json::to_vec(&outcome)?,
            "application/json",
            "model_outcomes",
            "SubjectModelOutcome",
            2,
        )?);
        step.state = SubjectStepState::ResultRecorded;
        let expected = step.record_revision;
        step.record_revision = RecordRevision(expected.0 + 1);
        store.upsert_step(&step, &state.head, expected)?;
        state.step = step.clone();
        (Some(output), response.stop_reason)
    };

    // Hook: PostModel — SPEC §37. Fires after inference completes, before S1 commit.
    if let Some(reg) = &hooks_ref {
        let results = reg
            .run_phase(&HookPayload::AfterInference {
                request_id: step_id,
                stop_reason: stop_reason.clone(),
                output_tokens: model_output.as_ref().map(|o| o.len() as u64).unwrap_or(0),
            })
            .await;
        debug!(seq, hooks=%results.len(), "PostModel hooks ran");
    }

    // S1 decision — SPEC §41.5: commit decision + event disposition +
    // delegation intent + unique successor before the ledger write.
    let complete = matches!(stop_reason.as_str(), "stop" | "end_turn" | "stop_sequence");
    let output = if complete {
        crate::output::parse_output(
            step_id,
            step.model_result_ref
                .clone()
                .ok_or_else(|| ShenkoError::conflict("missing model result"))?,
            model_output.as_deref().unwrap_or(""),
            &state.identity_service,
        )
    } else {
        Err(ShenkoError::Protocol(shenko_types::error::ProtocolError::new(
            "MODEL_RESULT_INCOMPLETE", shenko_types::error::ErrorScope::Request,
            shenko_types::error::RetryStrategy::NewRevision,
        ).with_details(serde_json::json!({"stop_reason":stop_reason,"reason":"partial/refused/unknown result is not an action decision"}))))
    };
    let output = match output {
        Ok(output) => output,
        Err(ShenkoError::Protocol(error))
            if matches!(
                error.code.as_str(),
                "CONTROL_PROTOCOL_INVALID" | "MODEL_RESULT_INCOMPLETE"
            ) =>
        {
            replace_rejected_output(state, input, model_output.as_deref().unwrap_or(""), error)?;
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    // ResultRecorded (including real CAS bytes) is already durable before hooks/S1.
    step.state = SubjectStepState::Committed;
    step.record_revision = RecordRevision(step.record_revision.0 + 1);

    // Hook: PreEffect — SPEC §37. Fires before S1 commit is written to the ledger.
    if let Some(reg) = &hooks_ref {
        let results = reg
            .run_phase(&HookPayload::BeforeOperation {
                operation_id: step_id,
                operation_name: format!("commit_step/{seq}"),
            })
            .await;
        debug!(seq, hooks=%results.len(), "PreEffect hooks ran");
    }

    let mut decision_delegation_intents = vec![];
    let mut memory_candidates = vec![];
    let mut publication_intents = vec![];
    let mut identity_changes = vec![];
    let mut identity_action_ids = vec![];
    for control in &output.controls {
        match &control.action {
            OutputAction::Delegate { text } => decision_delegation_intents.push(DelegationIntent {
                intent_id: control.action_id,
                goal: text.clone(),
                system_prompt: text.clone(),
                model: None,
                capabilities: vec![],
                budget_hint: None,
                reply_grant: None,
            }),
            OutputAction::Remember { text } => {
                let content_ref = seal_step_object(
                    &store,
                    seq,
                    text.as_bytes(),
                    "text/plain",
                    "memory_intents",
                    "text",
                    1,
                )?;
                memory_candidates.push((
                    MemoryIntent {
                        intent_id: control.action_id,
                        kind: MemoryIntentKind::Save,
                        scope: "personal".into(),
                        content_ref: Some(content_ref),
                    },
                    text.clone(),
                ));
            }
            OutputAction::Reply { target, text } if !text.trim().is_empty() => {
                publication_intents.push(shenko_types::subject::PublicationIntent {
                    intent_id: control.action_id,
                    target: target.clone(),
                    content: text.clone(),
                    reply_grant_id: None,
                });
            }
            OutputAction::Identity { change } => {
                identity_changes.push(change.clone());
                identity_action_ids.push(control.action_id);
            }
            _ => {}
        }
    }
    let identity_intent = if identity_changes.is_empty() {
        None
    } else {
        let current = input_identity.as_ref().ok_or_else(|| {
            ShenkoError::conflict("identity edit requires initialized S0 profile")
        })?;
        state
            .identity_service
            .revise(
                current,
                &identity_changes,
                IdentitySource::Subject {
                    step_id,
                    step_no: seq,
                },
            )
            .map(|proposed| IdentityIntent {
                intent_id: identity_action_ids[0],
                action_ids: identity_action_ids,
                expected_revision: current.revision,
                provider_revision: state.identity_service.revision().into(),
                proposed,
            })
    };

    let event_dispositions: Vec<EventDisposition> = events
        .iter()
        .map(|ev| EventDisposition {
            event_id: ev.event_id.clone(),
            disposition: Disposition::Acknowledged,
        })
        .collect();

    // 决定对象没有自引用；后继身份在 CAS 决定与数据库事务中保持一致。
    let decision = StepDecision {
        schema_version: 3,
        step_id,
        output: output.clone(),
        identity_intent,
        input_plan_ref: step
            .input_plan_ref
            .clone()
            .ok_or_else(|| ShenkoError::conflict("missing input plan"))?,
        model_result_ref: step.model_result_ref.clone(),
        applied_steering_refs: vec![],
        event_dispositions,
        delegated_task_intents: decision_delegation_intents.clone(),
        child_dialogue_intents: vec![],
        memory_intents: memory_candidates.iter().map(|(i, _)| i.clone()).collect(),
        policy_intents: vec![],
        publication_intents: publication_intents.clone(),
        pending_obligations: vec![],
        evidence_refs: vec![],
        successor_step_id: next_step_id,
    };
    let decision_bytes = serde_json::to_vec(&decision)?;
    step.decision_ref = Some(seal_step_object(
        &store,
        seq,
        &decision_bytes,
        "application/json",
        "decisions",
        "StepDecision",
        3,
    )?);

    match store.commit_step_and_advance_head(&step, &state.head, next_step_id) {
        Ok(receipt) => {
            // 只在提交明确成功后采用内存状态并发布。
            let (head, successor) = store.load_subject(state.agent_id)?;
            state.head = head;
            state.step = successor;
            state.last_thought = model_output.clone();

            // Read our committed receipt, not latest profile. Its revision and
            // typed conflict were settled atomically with this exact S1.
            let identity_receipt = store
                .identity_receipt(step_id)?
                .ok_or_else(|| ShenkoError::conflict("S1 identity receipt missing"))?;
            let identity_revision = identity_receipt.outcome.revision();
            state.identity_receipt = Some(identity_receipt);

            // Outbound path — publish ONLY after the S1 commit succeeded.
            // SPEC §41.2.3 committed-only publication: never show a thought
            // that is not durably recorded, or a retried step could show a
            // different one.
            if model_output.is_some() {
                if let Some(tx) = &state.outbox_tx {
                    let _ = tx.send(OutboxMessage {
                        kind: "thought".into(),
                        step: seq,
                        step_id,
                        commit_ref: receipt.commit_ref.clone(),
                        text: decision.output.text.clone(),
                        publications: publication_intents.clone(),
                        identity_revision,
                    });
                }
            }

            if seq % 50 == 0 || !events.is_empty() {
                debug!(
                    seq,
                    events=%events.len(),
                    in_wait=in_semantic_wait,
                    has_model=model_output.is_some(),
                    commit=%receipt.commit_ref,
                    "S1 committed"
                );
            }

            // Hook: PostEffect — SPEC §37. Fires after S1 is durably committed.
            if let Some(reg) = &hooks_ref {
                let results = reg
                    .run_phase(&HookPayload::AfterOperation {
                        operation_id: step_id,
                        outcome: "committed".to_string(),
                        effect_status: format!("commit/{seq}"),
                    })
                    .await;
                debug!(seq, hooks=%results.len(), "PostEffect hooks ran");
            }

            Ok(Some(receipt.commit_ref))
        }
        Err(e) => {
            warn!(seq, err=%e, "S1 commit failed");
            Err(e)
        }
    }
}

/// A protocol rejection is conclusive, unlike an unknown provider outcome.
/// R0 records it and freezes a new correction request before adopting memory.
fn replace_rejected_output(
    state: &mut SubjectLoopState,
    mut input: FrozenStepInput,
    raw_output: &str,
    error: shenko_types::error::ProtocolError,
) -> Result<(), ShenkoError> {
    use shenko_types::subject::ControlOutputRejection;
    let store = &state.store;
    let mut retired = state.step.clone();
    let raw_ref = retired
        .model_result_ref
        .clone()
        .ok_or_else(|| ShenkoError::conflict("rejection missing recorded result"))?;
    let replacement_id = EntityId::new();
    let rejection = ControlOutputRejection {
        schema_version: 2,
        supersedes_step_id: retired.step_id,
        replacement_step_id: replacement_id,
        request_id: retired
            .active_request_id
            .ok_or_else(|| ShenkoError::conflict("rejection missing request"))?,
        input_plan_ref: retired
            .input_plan_ref
            .clone()
            .ok_or_else(|| ShenkoError::conflict("rejection missing input plan"))?,
        model_result_ref: raw_ref.clone(),
        model_outcome_ref: retired
            .model_outcome_ref
            .clone()
            .ok_or_else(|| ShenkoError::conflict("rejection missing terminal outcome"))?,
        error,
    };
    let reason_ref = seal_step_object(
        store,
        retired.step_no,
        &serde_json::to_vec(&rejection)?,
        "application/json",
        "control_rejections",
        "ControlOutputRejection",
        2,
    )?;
    // Preserve the selected events and pinned persona/provider. This is a new
    // step/request seal with explicit feedback, not replay of the invalid wire.
    input.step_id = replacement_id;
    input
        .payload
        .messages
        .push(shenko_broker::request::InferenceMessage {
            role: "assistant".into(),
            content: raw_output.into(),
        });
    input.payload.messages.push(shenko_broker::request::InferenceMessage {
        role: "user".into(),
        content: format!(
            "运行时控制协议拒绝：{} {}。此结果的全部动作均未接纳，输入事件仍待处理。请依据原输入纠正完整结果，不要重用旧语法；示例放入引用或代码块。",
            rejection.error.code, rejection.error.details,
        ),
    });
    let mut replacement = retired.clone();
    replacement.step_id = replacement_id;
    replacement.state = SubjectStepState::Preparing;
    replacement.input_plan_ref = Some(seal_step_object(
        store,
        retired.step_no,
        &serde_json::to_vec(&input)?,
        "application/json",
        "input_plans",
        "subject_step_input",
        6,
    )?);
    replacement.context_revision = Some(
        retired
            .context_revision
            .and_then(|revision| revision.checked_add(1))
            .ok_or_else(|| {
                ShenkoError::conflict("correction context revision exhausted/missing")
            })?,
    );
    replacement.active_request_id = None;
    replacement.active_attempt_id = None;
    replacement.model_result_ref = None;
    replacement.model_outcome_ref = None;
    replacement.decision_ref = None;
    replacement.commit_ref = None;
    replacement.retry_reason_ref = Some(reason_ref.clone());
    replacement.record_revision = RecordRevision(0);
    retired.state = SubjectStepState::Retired;
    retired.retry_reason_ref = Some(reason_ref);
    retired.record_revision = RecordRevision(retired.record_revision.0 + 1);
    let receipt = store.reject_output_and_replace_step(&retired, &state.head, &replacement)?;
    let (head, step) = store.load_subject(state.agent_id)?;
    state.head = head;
    state.step = step;
    warn!(step_id=%retired.step_id, replacement_step_id=%replacement_id,
        commit=%receipt.commit_ref, error=%rejection.error, "control result rejected; correction registered");
    Ok(())
}

fn record_retry(state: &mut SubjectLoopState, fact: InferenceRetryFact) -> Result<(), ShenkoError> {
    let request_id = state
        .step
        .active_request_id
        .ok_or_else(|| ShenkoError::conflict("missing retry request"))?;
    let evidence = SubjectRetryEvidence { request_id, fact };
    let mut step = state.step.clone();
    step.retry_reason_ref = Some(seal_step_object(
        &state.store,
        step.step_no,
        &serde_json::to_vec(&evidence)?,
        "application/json",
        "retry_evidence",
        "subject_retry_evidence",
        1,
    )?);
    step.state = SubjectStepState::RetryPending;
    let expected = step.record_revision;
    step.record_revision = RecordRevision(expected.0 + 1);
    state.store.upsert_step(&step, &state.head, expected)?;
    state.step = step;
    Ok(())
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod recovery_tests;
