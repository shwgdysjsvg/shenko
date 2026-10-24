//! 本地 SQLite/CAS 恢复验证，不代表真实 provider 或部署验收。
use super::{SubjectLoopState, execute_step};
use crate::inbox::Inbox;
use shenko_store::store::Store;
use shenko_types::{commit::CommitRef, ids::*, subject::*};
use std::{path::PathBuf, sync::Arc};

fn fixture() -> (PathBuf, Arc<Store>, EntityId) {
    let dir = std::env::temp_dir().join(format!("shenko-recovery-{}", EntityId::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let agent = EntityId::new();
    let genesis_id = EntityId::new();
    let first_id = EntityId::new();
    let ledger_id = store.ledger.ledger_id();
    let precommit = CommitRef::new(ledger_id, LedgerSeq(0));
    let object = store
        .cas
        .seal_object(b"seed", "text/plain", "test", "raw", 1)
        .unwrap()
        .object;
    let genesis = SubjectGenesis {
        genesis_id,
        subject_id: agent,
        bootstrap_id: EntityId::new(),
        launch_id: EntityId::new(),
        seed_manifest_ref: object.clone(),
        initial_profile_ref: object.clone(),
        home_manifest_ref: object.clone(),
        bootstrap_event_ref: object,
        created_commit: precommit,
    };
    let head = SubjectBranchHead {
        agent_id: agent,
        branch_id: BranchId::new(),
        subject_root_task_id: EntityId::new(),
        controller_epoch: ControllerEpoch(7),
        software_generation: RuntimeGeneration("fixture-generation".into()),
        genesis_ref: GenesisRef {
            ledger_id,
            genesis_id,
        },
        committed_step_no: 0,
        committed_step_id: None,
        committed_at: precommit,
        next_step_id: first_id,
        mode: SubjectMode::Active,
        mode_reason_ref: None,
        record_revision: RecordRevision(0),
    };
    let first = SubjectStep {
        step_id: first_id,
        branch_id: head.branch_id.clone(),
        step_no: 1,
        expected_parent_commit: precommit,
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
        created_by_step_id: None,
    };
    store.initialize_genesis(&genesis, &head, &first).unwrap();
    (dir, store, agent)
}

fn state(
    store: Arc<Store>,
    agent: EntityId,
) -> Result<SubjectLoopState, shenko_types::error::ShenkoError> {
    let ctx = cordis_core::Context::new();
    let _publication = ctx
        .provide(Arc::new(shenko_identity::IdentityService::new(Arc::new(
            shenko_identity::DefaultIdentityPolicy,
        ))))
        .unwrap();
    SubjectLoopState::new(agent, Arc::new(Inbox::new(8)), store, ctx)
}

#[test]
fn reopening_awaiting_model_keeps_original_request_blocked() {
    let (dir, store, agent) = fixture();
    let (head, mut step) = store.load_subject(agent).unwrap();
    step.state = SubjectStepState::Preparing;
    step.input_plan_ref = Some(
        store
            .cas
            .seal_object(b"plan", "text/plain", "test", "raw", 1)
            .unwrap()
            .object,
    );
    step.record_revision = RecordRevision(1);
    store.upsert_step(&step, &head, RecordRevision(0)).unwrap();
    let request = EntityId::new();
    let attempt = EntityId::new();
    step.state = SubjectStepState::AwaitingModel;
    step.active_request_id = Some(request);
    step.active_attempt_id = Some(attempt);
    step.record_revision = RecordRevision(2);
    store.upsert_step(&step, &head, RecordRevision(1)).unwrap();
    drop(store);

    let reopened = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let error = state(reopened.clone(), agent)
        .err()
        .expect("未知请求不得构造可运行主体");
    assert!(error.to_string().contains("recovery_blocked"));
    let (restored, unresolved) = reopened.load_subject(agent).unwrap();
    assert_eq!(restored.committed_at, head.committed_at);
    assert_eq!(restored.next_step_id, step.step_id);
    assert_eq!(unresolved.state, SubjectStepState::AwaitingModel);
    assert_eq!(unresolved.active_request_id, Some(request));
    assert_eq!(unresolved.active_attempt_id, Some(attempt));
    assert_eq!(unresolved.record_revision, RecordRevision(2));
    assert!(unresolved.commit_ref.is_none());
    drop(reopened);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn cas_seal_failure_returns_error_without_head_advance_or_publication() {
    let (dir, store, agent) = fixture();
    let (head, planned) = store.load_subject(agent).unwrap();
    let mut subject = state(store.clone(), agent).unwrap();
    assert_eq!(subject.step.step_id, planned.step_id);
    assert_eq!(subject.head.branch_id, head.branch_id);
    let (tx, mut rx) = tokio::sync::broadcast::channel(8);
    subject = subject.with_outbox(tx);
    // 用普通文件占据 CAS 根，确定性触发 mkdir 错误，不依赖进程权限。
    std::fs::remove_dir_all(dir.join("cas")).unwrap();
    std::fs::write(dir.join("cas"), b"not a directory").unwrap();
    let result = execute_step(
        planned.step_no,
        &[],
        &mut subject,
        false,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(
        rx.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
    let (after, still_planned) = store.load_subject(agent).unwrap();
    assert_eq!(after.committed_at, head.committed_at);
    assert_eq!(after.next_step_id, planned.step_id);
    assert_eq!(after.record_revision, head.record_revision);
    assert_eq!(still_planned.state, SubjectStepState::Planned);
    assert!(still_planned.input_plan_ref.is_none());
    assert_eq!(still_planned.record_revision, RecordRevision(0));
    drop(subject);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// SPEC §6 R053/R054/R117：人格只在 S0 边界生效，合同前缀不随身份变化，改名不换 agent。
#[tokio::test]
async fn identity_revision_applies_at_step_boundary_behind_fixed_contract() {
    use shenko_types::identity::{IdentityChange, IdentitySource, NamingOrigin};
    let (dir, store, agent) = fixture();
    store
        .initialize_identity(&shenko_identity::DefaultIdentityPolicy::genesis(
            agent, None,
        ))
        .unwrap();
    let mut subject = state(store.clone(), agent).unwrap();
    let before = super::build_system_prompt(&subject, false);
    assert!(before.starts_with(super::SUBJECT_CONTRACT));
    assert!(!before.contains("你是 Shenko"));
    assert!(before.contains("你还没有名字"));

    let source = IdentitySource::Subject {
        step_id: subject.step.step_id,
        step_no: 1,
    };
    let current = subject.identity.as_ref().unwrap();
    let renamed = shenko_identity::DefaultIdentityPolicy::successor(
        current,
        &IdentityChange::Name("岚".into()),
        source,
    )
    .unwrap();
    store.append_identity(&renamed, current.revision).unwrap();
    assert_eq!(
        (renamed.agent_id, renamed.revision, renamed.naming),
        (agent, 2, NamingOrigin::SelfChosen)
    );
    // 本步尚未到边界：仍是 revision 1。
    assert_eq!(subject.identity.as_ref().unwrap().revision, 1);

    // 无 broker 时 execute_step 在 S0 之后报错，但 S0 已装配新身份并封存输入计划。
    let planned = subject.step.step_no;
    assert!(
        execute_step(
            planned,
            &[],
            &mut subject,
            false,
            &tokio_util::sync::CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert_eq!(subject.identity.as_ref().unwrap().revision, 2);
    assert!(subject.step.input_plan_ref.is_some());
    let after = super::build_system_prompt(&subject, false);
    assert!(after.starts_with(super::SUBJECT_CONTRACT));
    assert!(after.contains("「岚」") && after.contains("自己取的"));
    assert_eq!(store.identity_history(agent).unwrap().len(), 2);
    drop(subject);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

/// One interrupted input's recovery journey: close before dispatch, rejected
/// admission, then persisted result recovery and stale ingress replay.
#[tokio::test]
async fn frozen_input_survives_shutdown_rejection_and_recorded_result_reopen() {
    let (dir, store, agent) = fixture();
    let event = crate::inbox::InboxEvent {
        event_id: EntityId::new(),
        kind: crate::inbox::InboxEventKind::Message,
        payload: serde_json::json!({"content":"original frozen input"}),
        source: "fixture".into(),
        created_at: chrono::Utc::now(),
    };
    let (offline, handle) = shenko_broker::broker::Broker::offline();
    drop(offline);
    let mut subject = state(store.clone(), agent).unwrap().with_broker(handle);
    let close = tokio_util::sync::CancellationToken::new();
    close.cancel();
    assert!(
        execute_step(1, &[event.clone()], &mut subject, false, &close)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(subject.step.state, SubjectStepState::Preparing);
    drop(subject);
    drop(store);

    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let (closed_broker, handle) = shenko_broker::broker::Broker::offline();
    drop(closed_broker); // positively rejected admission; no provider ever dispatched
    let subject = state(store.clone(), agent).unwrap().with_broker(handle);
    let retained_inbox = subject.inbox.clone();
    assert!(
        super::run_subject_loop(subject, tokio_util::sync::CancellationToken::new())
            .await
            .is_err()
    );
    assert!(retained_inbox.sender().is_closed());
    let (head, mut step) = store.load_subject(agent).unwrap();
    assert_eq!(step.state, SubjectStepState::RetryPending);
    let request_id = step.active_request_id;
    drop(retained_inbox);
    drop(store);

    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    // Reopen validates the typed certificate; simulate the durable result boundary
    // of that same request, not a fabricated S1 or a second model invocation.
    let subject = state(store.clone(), agent).unwrap();
    assert_eq!(
        super::frozen_input(&store, &subject.step).unwrap().events[0].payload,
        event.payload
    );
    drop(subject);
    let expected = step.record_revision;
    step.state = SubjectStepState::AwaitingModel;
    step.record_revision = RecordRevision(expected.0 + 1);
    store.upsert_step(&step, &head, expected).unwrap();
    let expected = step.record_revision;
    step.state = SubjectStepState::ResultRecorded;
    step.model_result_ref = Some(
        store
            .cas
            .seal_object(b"[delegate]", "text/plain", "model_results", "text", 1)
            .unwrap()
            .object,
    );
    step.record_revision = RecordRevision(expected.0 + 1);
    step.model_outcome_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&shenko_types::subject::SubjectModelOutcome {
                    schema_version: 2,
                    request_id: step.active_request_id.unwrap(),
                    result_ref: step.model_result_ref.clone().unwrap(),
                    stop_reason: "end_turn".into(),
                    raw_response: r#"{"fixture":true,"stop_reason":"end_turn"}"#.into(),
                    usage: None,
                })
                .unwrap(),
                "application/json",
                "model_outcomes",
                "SubjectModelOutcome",
                2,
            )
            .unwrap()
            .object,
    );
    store.upsert_step(&step, &head, expected).unwrap();
    drop(store);

    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let mut subject = state(store.clone(), agent).unwrap(); // no broker: resending would fail
    let (tx, mut rx) = tokio::sync::broadcast::channel(8);
    subject = subject.with_outbox(tx);
    assert_eq!(subject.step.active_request_id, request_id);
    assert!(
        execute_step(
            1,
            &[],
            &mut subject,
            false,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await
        .unwrap()
        .is_none()
    );
    // A complete malformed control is durably rejected, not a permanent
    // ResultRecorded crashloop or a fabricated successful S1/ACK.
    assert_eq!(subject.head.committed_step_no, 0);
    assert_eq!(subject.head.committed_at, head.committed_at);
    assert_ne!(subject.step.step_id, step.step_id);
    assert!(subject.step.context_revision > step.context_revision);
    assert_eq!(subject.step.state, SubjectStepState::Preparing);
    assert!(rx.try_recv().is_err());
    assert!(
        store
            .event_disposition_commit(agent, event.event_id)
            .unwrap()
            .is_none()
    );
    let retired = store
        .get_step(&head.branch_id, step.step_id)
        .unwrap()
        .unwrap();
    assert_eq!(retired.state, SubjectStepState::Retired);
    assert_eq!(retired.model_result_ref, step.model_result_ref);
    assert_eq!(retired.input_plan_ref, step.input_plan_ref);
    let rejection: ControlOutputRejection = serde_json::from_slice(
        &super::read_sealed(&store, retired.retry_reason_ref.as_ref().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(rejection.request_id, request_id.unwrap());
    assert_eq!(rejection.error.code, "CONTROL_PROTOCOL_INVALID");
    assert_eq!(rejection.replacement_step_id, subject.step.step_id);
    let correction = super::frozen_input(&store, &subject.step).unwrap();
    assert_eq!(correction.events[0].event_id, event.event_id);
    assert_eq!(correction.payload.messages[1].content, "[delegate]");
    assert!(
        correction.payload.messages[2]
            .content
            .contains("CONTROL_PROTOCOL_INVALID")
    );
    drop(subject);
    drop(store);
    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let subject = state(store.clone(), agent).unwrap();
    assert_eq!(subject.step.step_id, rejection.replacement_step_id);
    // Close/reopen before dispatch retains the correction. A terminal result
    // of the NEW request can now settle the original input exactly once.
    let mut corrected = subject.step.clone();
    corrected.state = SubjectStepState::AwaitingModel;
    corrected.active_request_id = Some(EntityId::new());
    assert_ne!(corrected.active_request_id, request_id);
    corrected.record_revision = RecordRevision(1);
    store
        .upsert_step(&corrected, &subject.head, RecordRevision(0))
        .unwrap();
    corrected.state = SubjectStepState::ResultRecorded;
    corrected.model_result_ref = Some(
        store
            .cas
            .seal_object(
                b"persisted complete result",
                "text/plain",
                "model_results",
                "text",
                1,
            )
            .unwrap()
            .object,
    );
    corrected.record_revision = RecordRevision(2);
    corrected.model_outcome_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&shenko_types::subject::SubjectModelOutcome {
                    schema_version: 2,
                    request_id: corrected.active_request_id.unwrap(),
                    result_ref: corrected.model_result_ref.clone().unwrap(),
                    stop_reason: "end_turn".into(),
                    raw_response: r#"{"fixture":true,"stop_reason":"end_turn"}"#.into(),
                    usage: None,
                })
                .unwrap(),
                "application/json",
                "model_outcomes",
                "SubjectModelOutcome",
                2,
            )
            .unwrap()
            .object,
    );

    store
        .upsert_step(&corrected, &subject.head, RecordRevision(1))
        .unwrap();
    drop(subject);
    let mut subject = state(store.clone(), agent).unwrap();
    let (tx, mut rx) = tokio::sync::broadcast::channel(8);
    subject = subject.with_outbox(tx);
    execute_step(
        1,
        &[],
        &mut subject,
        false,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(subject.head.committed_step_no, 1);
    assert_eq!(rx.try_recv().unwrap().text, "persisted complete result");
    assert!(
        store
            .event_disposition_commit(agent, event.event_id)
            .unwrap()
            .is_some()
    );
    // A previously queued replay must be filtered after the resumed S1, even
    // though main admitted it before that S1's disposition became visible.
    subject.inbox.sender().send(event).await.unwrap();
    assert!(
        super::run_subject_loop(subject, tokio_util::sync::CancellationToken::new())
            .await
            .is_err()
    );
    let (head, next) = store.load_subject(agent).unwrap();
    assert_eq!(head.committed_step_no, 1);
    assert!(
        super::frozen_input(&store, &next)
            .unwrap()
            .events
            .is_empty()
    );
    // Keep the fixture on container disk for independent inspection.
}

/// One real process-crash/CAS recovery journey: all-or-none S1, death before
/// postcommit work, then an operator editing during the successor's inference.
#[tokio::test]
async fn identity_s1_crash_and_concurrent_editor_keep_durable_decision() {
    use shenko_identity::DefaultIdentityPolicy as Policy;
    use shenko_types::{identity::*, output::*};
    if let Some(dir) = std::env::var_os("SHENKO_C22_CRASH_CHILD") {
        let dir = PathBuf::from(dir);
        let store = Store::open(dir.join("store.db"), dir.join("cas")).unwrap();
        let (step, head, next): (SubjectStep, SubjectBranchHead, EntityId) =
            serde_json::from_slice(&std::fs::read(dir.join("candidate.json")).unwrap()).unwrap();
        store
            .commit_step_and_advance_head(&step, &head, next)
            .unwrap();
        // Actual process death immediately after the SQL commit, with no loop
        // identity write, in-memory adoption, broadcaster or worker ever run.
        #[cfg(unix)]
        {
            std::process::Command::new("kill")
                .args(["-KILL", &std::process::id().to_string()])
                .status()
                .unwrap();
            unreachable!("SIGKILL must end the process");
        }
        #[cfg(not(unix))]
        std::process::exit(73); // abrupt exit without destructors on other platforms
    }
    let (dir, store, agent) = fixture();
    let profile = store
        .initialize_identity(&Policy::genesis(agent, None))
        .unwrap();
    let event = crate::inbox::InboxEvent {
        event_id: EntityId::new(),
        kind: crate::inbox::InboxEventKind::Message,
        payload: serde_json::json!({"content":"choose your name"}),
        source: "fixture".into(),
        created_at: chrono::Utc::now(),
    };
    let mut subject = state(store.clone(), agent).unwrap();
    let close = tokio_util::sync::CancellationToken::new();
    close.cancel();
    execute_step(1, &[event.clone()], &mut subject, false, &close)
        .await
        .unwrap();
    let head = subject.head.clone();
    let mut step = subject.step.clone();
    step.state = SubjectStepState::AwaitingModel;
    step.active_request_id = Some(EntityId::new());
    step.record_revision = RecordRevision(2);
    store.upsert_step(&step, &head, RecordRevision(1)).unwrap();
    let raw = "[identity:name] 岚\n[identity:soul] 记住关系，不靠 delegate 或委派替代理解";
    let raw_ref = store
        .cas
        .seal_object(raw.as_bytes(), "text/plain", "model_results", "text", 1)
        .unwrap()
        .object;
    step.state = SubjectStepState::ResultRecorded;
    step.model_result_ref = Some(raw_ref.clone());
    step.record_revision = RecordRevision(3);
    step.model_outcome_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&shenko_types::subject::SubjectModelOutcome {
                    schema_version: 2,
                    request_id: step.active_request_id.unwrap(),
                    result_ref: step.model_result_ref.clone().unwrap(),
                    stop_reason: "end_turn".into(),
                    raw_response: r#"{"fixture":true,"stop_reason":"end_turn"}"#.into(),
                    usage: None,
                })
                .unwrap(),
                "application/json",
                "model_outcomes",
                "SubjectModelOutcome",
                2,
            )
            .unwrap()
            .object,
    );
    store.upsert_step(&step, &head, RecordRevision(2)).unwrap();
    let output = crate::output::parse_output(
        step.step_id,
        raw_ref.clone(),
        raw,
        &subject.identity_service,
    )
    .unwrap();
    assert_eq!(output.controls.len(), 2);
    let changes: Vec<_> = output
        .controls
        .iter()
        .map(|control| {
            let OutputAction::Identity { change } = &control.action else {
                panic!("no synthetic memory/task")
            };
            change.clone()
        })
        .collect();
    let next = EntityId::new();
    let mut decision = StepDecision {
        schema_version: 3,
        step_id: step.step_id,
        output,
        identity_intent: Some(IdentityIntent {
            intent_id: EntityId::new(),
            action_ids: vec![],
            expected_revision: 1,
            provider_revision: subject.identity_service.revision().into(),
            proposed: Policy::revise(
                &profile,
                &changes,
                IdentitySource::Subject {
                    step_id: step.step_id,
                    step_no: 1,
                },
            )
            .unwrap(),
        }),
        input_plan_ref: step.input_plan_ref.clone().unwrap(),
        model_result_ref: Some(raw_ref.clone()),
        applied_steering_refs: vec![],
        event_dispositions: vec![EventDisposition {
            event_id: event.event_id,
            disposition: Disposition::Acknowledged,
        }],
        delegated_task_intents: vec![],
        child_dialogue_intents: vec![],
        memory_intents: vec![],
        policy_intents: vec![],
        publication_intents: vec![],
        pending_obligations: vec![],
        evidence_refs: vec![],
        successor_step_id: step.step_id,
    };
    step.state = SubjectStepState::Committed;
    step.record_revision = RecordRevision(4);
    step.decision_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&decision).unwrap(),
                "application/json",
                "decisions",
                "StepDecision",
                3,
            )
            .unwrap()
            .object,
    );
    // A successor collision happens after identity insertion inside S1. It must
    // roll back the identity, receipt and event disposition as well as the head.
    assert!(
        store
            .commit_step_and_advance_head(&step, &head, step.step_id)
            .is_err()
    );
    assert_eq!(store.latest_identity(agent).unwrap().unwrap().revision, 1);
    assert!(store.identity_receipt(step.step_id).unwrap().is_none());
    assert!(
        store
            .event_disposition_commit(agent, event.event_id)
            .unwrap()
            .is_none()
    );
    decision.successor_step_id = next;
    step.decision_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&decision).unwrap(),
                "application/json",
                "decisions",
                "StepDecision",
                3,
            )
            .unwrap()
            .object,
    );
    std::fs::write(
        dir.join("candidate.json"),
        serde_json::to_vec(&(&step, &head, next)).unwrap(),
    )
    .unwrap();
    drop(subject);
    drop(store);
    let status=std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact","loop_runner::recovery_tests::identity_s1_crash_and_concurrent_editor_keep_durable_decision","--nocapture"])
        .env("SHENKO_C22_CRASH_CHILD",&dir).status().unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    assert_eq!(
        std::os::unix::process::ExitStatusExt::signal(&status),
        Some(9)
    );
    let store = Arc::new(Store::open(dir.join("store.db"), dir.join("cas")).unwrap());
    let receipt = store.identity_receipt(step.step_id).unwrap().unwrap();
    assert!(matches!(
        receipt.outcome,
        IdentityOutcome::Applied {
            expected_revision: 1,
            revision: 2
        }
    ));
    assert_eq!(
        store.load_subject(agent).unwrap().0.committed_at,
        receipt.committed_at
    );
    assert_eq!(
        store
            .latest_identity(agent)
            .unwrap()
            .unwrap()
            .display_name
            .as_deref(),
        Some("岚")
    );
    assert_eq!(
        store.cas.read_object(&raw_ref.digest).unwrap(),
        raw.as_bytes()
    );
    let mut subject = state(store.clone(), agent).unwrap();
    execute_step(2, &[], &mut subject, false, &close)
        .await
        .unwrap();
    let sealed = super::frozen_input(&store, &subject.step).unwrap();
    assert_eq!(sealed.identity.unwrap().revision, 2);
    let current = store.latest_identity(agent).unwrap().unwrap();
    let operator = Policy::revise(
        &current,
        &[IdentityChange::Name("operator choice".into())],
        IdentitySource::Operator,
    )
    .unwrap();
    store.append_identity(&operator, 2).unwrap();
    let head = subject.head.clone();
    let mut step = subject.step.clone();
    step.state = SubjectStepState::AwaitingModel;
    step.active_request_id = Some(EntityId::new());
    step.record_revision = RecordRevision(2);
    store.upsert_step(&step, &head, RecordRevision(1)).unwrap();
    step.state = SubjectStepState::ResultRecorded;
    step.record_revision = RecordRevision(3);
    step.model_result_ref = Some(
        store
            .cas
            .seal_object(
                "[identity:name] stale model choice".as_bytes(),
                "text/plain",
                "model_results",
                "text",
                1,
            )
            .unwrap()
            .object,
    );
    step.model_outcome_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&shenko_types::subject::SubjectModelOutcome {
                    schema_version: 2,
                    request_id: step.active_request_id.unwrap(),
                    result_ref: step.model_result_ref.clone().unwrap(),
                    stop_reason: "end_turn".into(),
                    raw_response: r#"{"fixture":true,"stop_reason":"end_turn"}"#.into(),
                    usage: None,
                })
                .unwrap(),
                "application/json",
                "model_outcomes",
                "SubjectModelOutcome",
                2,
            )
            .unwrap()
            .object,
    );
    store.upsert_step(&step, &head, RecordRevision(2)).unwrap();
    drop(subject);
    let mut subject = state(store.clone(), agent).unwrap();
    let (tx, mut rx) = tokio::sync::broadcast::channel(4);
    subject = subject.with_outbox(tx);
    execute_step(
        2,
        &[],
        &mut subject,
        false,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(rx.try_recv().unwrap().identity_revision, Some(3));
    let conflict = store.identity_receipt(step.step_id).unwrap().unwrap();
    assert!(matches!(
        conflict.outcome,
        IdentityOutcome::Conflict {
            expected_revision: 2,
            actual_revision: Some(3)
        }
    ));
    assert_eq!(
        store
            .latest_identity(agent)
            .unwrap()
            .unwrap()
            .display_name
            .as_deref(),
        Some("operator choice")
    );
    assert_eq!(subject.head.committed_step_no, 2);
    assert!(super::build_system_prompt(&subject, false).contains("上一步身份意图未采用"));
    // Retain this container-disk fixture as crash evidence, not production data.
}

/// Native Cordis replacement reaches the real S0 consumer. The old generation
/// finishes only its sealed request; recovery never reinterprets it with B.
#[tokio::test]
async fn native_identity_replacement_is_bound_to_sealed_request() {
    use cordis_core::{Context, Plugin, PreparedPlugin};
    use shenko_identity::{
        DefaultIdentityPolicy as DefaultPolicy, IdentityPlugin, IdentityPolicy, IdentityService,
    };
    use shenko_types::identity::*;
    struct OtherPolicy;
    impl IdentityPolicy for OtherPolicy {
        fn revision(&self) -> &str {
            "test.identity/other@1"
        }
        fn genesis(&self, agent: EntityId, name: Option<&str>) -> IdentityProfile {
            DefaultPolicy::genesis(agent, name)
        }
        fn persona_section(&self, profile: &IdentityProfile) -> String {
            format!("replacement provider at revision {}", profile.revision)
        }
        fn parse_control(&self, header: &str, value: &str) -> Option<IdentityChange> {
            DefaultPolicy.parse_control(header, value)
        }
        fn revise(
            &self,
            profile: &IdentityProfile,
            changes: &[IdentityChange],
            source: IdentitySource,
        ) -> Option<IdentityProfile> {
            DefaultPolicy::revise(profile, changes, source)
        }
    }
    let (_dir, store, agent) = fixture();
    let ctx = Context::new();
    let first = ctx.spawn(shenko_identity::default_plugin()).await.unwrap();
    let service = ctx.try_service::<IdentityService>().unwrap();
    store
        .initialize_identity(&service.genesis(agent, None))
        .unwrap();
    let mut subject =
        SubjectLoopState::new(agent, Arc::new(Inbox::new(8)), store.clone(), ctx.clone()).unwrap();
    let close = tokio_util::sync::CancellationToken::new();
    close.cancel();
    execute_step(1, &[], &mut subject, false, &close)
        .await
        .unwrap();
    let frozen = super::frozen_input(&store, &subject.step).unwrap();
    assert_eq!(
        frozen.identity_provider_revision,
        "shenko.identity/default@1"
    );
    assert!(frozen.payload.system_prompt.contains("你还没有名字"));
    first.dispose().await.unwrap();
    assert!(ctx.try_service::<IdentityService>().is_err());
    let plugin = IdentityPlugin;
    let prepared = plugin.prepare(Arc::new(OtherPolicy)).unwrap();
    let second = ctx
        .spawn(PreparedPlugin::from_input(plugin, prepared))
        .await
        .unwrap();
    assert_eq!(
        ctx.try_service::<IdentityService>().unwrap().revision(),
        "test.identity/other@1"
    );
    let reopened =
        SubjectLoopState::new(agent, Arc::new(Inbox::new(8)), store.clone(), ctx.clone());
    assert!(
        reopened
            .err()
            .unwrap()
            .to_string()
            .contains("IDENTITY_PROVIDER_REVISION_MISMATCH")
    );
    // The original live request retained its original service until S1, even
    // though Cordis withdrew that service from new consumers.
    let head = subject.head.clone();
    let mut step = subject.step.clone();
    step.state = SubjectStepState::AwaitingModel;
    step.active_request_id = Some(EntityId::new());
    step.record_revision = RecordRevision(2);
    store.upsert_step(&step, &head, RecordRevision(1)).unwrap();
    step.state = SubjectStepState::ResultRecorded;
    step.record_revision = RecordRevision(3);
    step.model_result_ref = Some(
        store
            .cas
            .seal_object(
                "[identity:name] 岚".as_bytes(),
                "text/plain",
                "model_results",
                "text",
                1,
            )
            .unwrap()
            .object,
    );
    step.model_outcome_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&shenko_types::subject::SubjectModelOutcome {
                    schema_version: 2,
                    request_id: step.active_request_id.unwrap(),
                    result_ref: step.model_result_ref.clone().unwrap(),
                    stop_reason: "end_turn".into(),
                    raw_response: r#"{"fixture":true,"stop_reason":"end_turn"}"#.into(),
                    usage: None,
                })
                .unwrap(),
                "application/json",
                "model_outcomes",
                "SubjectModelOutcome",
                2,
            )
            .unwrap()
            .object,
    );
    store.upsert_step(&step, &head, RecordRevision(2)).unwrap();
    subject.step = step;
    execute_step(1, &[], &mut subject, false, &close)
        .await
        .unwrap();
    assert_eq!(store.latest_identity(agent).unwrap().unwrap().revision, 2);
    execute_step(2, &[], &mut subject, false, &close)
        .await
        .unwrap();
    let next = super::frozen_input(&store, &subject.step).unwrap();
    assert_eq!(next.identity_provider_revision, "test.identity/other@1");
    assert!(
        next.payload
            .system_prompt
            .contains("replacement provider at revision 2")
    );
    assert!(!next.payload.system_prompt.contains("你还没有名字"));
    second.dispose().await.unwrap();
    assert!(ctx.try_service::<IdentityService>().is_err());
}
