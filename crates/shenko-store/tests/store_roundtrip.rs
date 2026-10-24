//! S1 验证：SQLite WAL store round-trip + CAS seal/read
//! 验证 §41.2: 单 mutation line, CAS object seal/read, FTS5 detection

use shenko_store::store::Store;
use shenko_types::store::WriteBatch;
use std::path::PathBuf;

fn tmp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "shenko-test-{}",
        shenko_types::ids::EntityId::new()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn store_opens_with_wal_and_fts5_detection() {
    let dir = tmp_dir();
    let db = dir.join("test.db");
    let cas = dir.join("cas");
    let store = Store::open(db, cas).unwrap();

    assert!(store.capabilities.wal_mode, "WAL mode should be enabled");
    assert_eq!(store.capabilities.synchronous, "2", "synchronous=FULL");
    // FTS5 may or may not be available depending on build; just check it's detected
    let _ = store.capabilities.fts5_available;
}

#[test]
fn cas_seal_and_read_roundtrip() {
    let dir = tmp_dir();
    let db = dir.join("cas_test.db");
    let cas = dir.join("cas");
    let store = Store::open(db, cas).unwrap();

    let data = b"hello shenko cas";
    let sealed = store
        .cas
        .seal_object(data, "text/plain", "test", "raw", 1)
        .unwrap();

    assert_eq!(sealed.object.bytes, 16);
    assert_eq!(sealed.object.media_type, "text/plain");

    // Read back
    let read = store.cas.read_object(&sealed.object.digest).unwrap();
    assert_eq!(read, data);

    // Exists
    assert!(store.cas.object_exists(&sealed.object.digest));
}

#[test]
fn empty_batch_commit_returns_receipt() {
    let dir = tmp_dir();
    let db = dir.join("empty.db");
    let cas = dir.join("cas");
    let store = Store::open(db, cas).unwrap();

    let batch = WriteBatch::new();
    let receipt = store.commit(batch).unwrap();

    assert_eq!(
        receipt.durability_profile,
        shenko_types::commit::DurabilityProfile::PowerLossLocal
    );
    assert!(!receipt.commit_ref.ledger_id.as_uuid().is_nil());
}

fn initialized_subject() -> (PathBuf, Store, shenko_types::ids::EntityId) {
    use shenko_types::commit::CommitRef;
    use shenko_types::ids::*;
    use shenko_types::subject::{
        GenesisRef, SubjectBranchHead, SubjectGenesis, SubjectMode, SubjectStep, SubjectStepState,
    };

    let dir = tmp_dir();
    let db = dir.join("head.db");
    let cas = dir.join("cas");
    let store = Store::open(db, cas).unwrap();

    let agent_id = EntityId::new();
    let branch_id = BranchId::new();
    let genesis_id = EntityId::new();
    let bootstrap_id = EntityId::new();
    let launch_id = EntityId::new();
    let first_step_id = EntityId::new();
    let ledger_id = store.ledger.ledger_id();

    let dummy_ref = store
        .cas
        .seal_object(b"seed", "application/json", "test", "raw", 1)
        .unwrap()
        .object;
    let genesis_commit = CommitRef::new(ledger_id, LedgerSeq(0));

    let genesis = SubjectGenesis {
        genesis_id,
        subject_id: agent_id,
        bootstrap_id,
        launch_id,
        seed_manifest_ref: dummy_ref.clone(),
        initial_profile_ref: dummy_ref.clone(),
        home_manifest_ref: dummy_ref.clone(),
        bootstrap_event_ref: dummy_ref.clone(),
        created_commit: genesis_commit,
    };

    let head = SubjectBranchHead {
        agent_id,
        branch_id: branch_id.clone(),
        subject_root_task_id: EntityId::new(),
        controller_epoch: ControllerEpoch(1),
        software_generation: RuntimeGeneration::default(),
        genesis_ref: GenesisRef {
            ledger_id,
            genesis_id,
        },
        committed_step_no: 0,
        committed_step_id: None,
        committed_at: genesis_commit,
        next_step_id: first_step_id,
        mode: SubjectMode::Active,
        mode_reason_ref: None,
        record_revision: RecordRevision(0),
    };

    let first_step = SubjectStep {
        step_id: first_step_id,
        branch_id: branch_id.clone(),
        step_no: 1,
        expected_parent_commit: genesis_commit,
        controller_epoch: ControllerEpoch(1),
        software_generation: RuntimeGeneration::default(),
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

    // Initialize genesis atomically
    let receipt = store
        .initialize_genesis(&genesis, &head, &first_step)
        .unwrap();
    assert_eq!(receipt.updated.len(), 3);

    (dir, store, agent_id)
}

fn recorded_result(
    store: &Store,
    head: &shenko_types::subject::SubjectBranchHead,
    mut step: shenko_types::subject::SubjectStep,
) -> shenko_types::subject::SubjectStep {
    use shenko_types::{ids::*, subject::*};
    step.state = SubjectStepState::Preparing;
    step.record_revision = RecordRevision(1);
    step.input_plan_ref = Some(
        store
            .cas
            .seal_object(b"plan", "text/plain", "test", "raw", 1)
            .unwrap()
            .object,
    );
    store.upsert_step(&step, head, RecordRevision(0)).unwrap();
    step.state = SubjectStepState::AwaitingModel;
    step.record_revision = RecordRevision(2);
    step.active_request_id = Some(EntityId::new());
    store.upsert_step(&step, head, RecordRevision(1)).unwrap();
    step.state = SubjectStepState::ResultRecorded;
    step.record_revision = RecordRevision(3);
    step.model_result_ref = Some(
        store
            .cas
            .seal_object("继续先前关注".as_bytes(), "text/plain", "test", "raw", 1)
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
    store.upsert_step(&step, head, RecordRevision(2)).unwrap();
    step
}

fn committed_candidate(
    store: &Store,
    mut step: shenko_types::subject::SubjectStep,
    next: shenko_types::ids::EntityId,
) -> shenko_types::subject::SubjectStep {
    use shenko_types::{ids::*, subject::*};
    let decision = StepDecision {
        schema_version: 3,
        output: shenko_types::output::ParsedOutput {
            schema_version: 1,
            step_id: step.step_id,
            raw_ref: step.model_result_ref.clone().unwrap(),
            text: "继续先前关注".into(),
            controls: vec![],
        },
        identity_intent: None,
        step_id: step.step_id,
        input_plan_ref: step.input_plan_ref.clone().unwrap(),
        model_result_ref: step.model_result_ref.clone(),
        applied_steering_refs: vec![],
        event_dispositions: vec![EventDisposition {
            event_id: step.step_id,
            disposition: Disposition::Acknowledged,
        }],
        delegated_task_intents: vec![],
        child_dialogue_intents: vec![],
        memory_intents: vec![],
        policy_intents: vec![],
        publication_intents: vec![],
        pending_obligations: vec![],
        evidence_refs: vec![],
        successor_step_id: next,
    };
    step.decision_ref = Some(
        store
            .cas
            .seal_object(
                &serde_json::to_vec(&decision).unwrap(),
                "application/json",
                "test",
                "StepDecision",
                1,
            )
            .unwrap()
            .object,
    );
    step.state = SubjectStepState::Committed;
    step.record_revision = RecordRevision(step.record_revision.0 + 1);
    step
}

#[test]
fn s1_commit_advances_branch_head() {
    use shenko_types::{ids::*, subject::*};
    let (dir, store, agent_id) = initialized_subject();
    let (head, first_step) = store.load_subject(agent_id).unwrap();
    let first_id = first_step.step_id;
    let ledger_id = store.ledger.ledger_id();
    let genesis_json: String = rusqlite::Connection::open(dir.join("head.db"))
        .unwrap()
        .query_row(
            "SELECT data FROM subject_genesis WHERE subject_id=?1",
            [agent_id.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    let genesis: SubjectGenesis = serde_json::from_str(&genesis_json).unwrap();
    assert_eq!(head.committed_at, genesis.created_commit);
    assert_eq!(first_step.expected_parent_commit, genesis.created_commit);
    assert!(genesis.created_commit.seq.0 > 0);
    let second_step_id = EntityId::new();
    let result = recorded_result(&store, &head, first_step);
    let step = committed_candidate(&store, result, second_step_id);
    assert!(
        store
            .event_disposition_commit(agent_id, first_id)
            .unwrap()
            .is_none()
    );
    let receipt = store
        .commit_step_and_advance_head(&step, &head, second_step_id)
        .unwrap();
    assert_eq!(receipt.updated.len(), 3);
    assert_eq!(
        receipt
            .updated
            .iter()
            .find(|u| u.entity_id == agent_id)
            .unwrap()
            .record_revision,
        RecordRevision(1)
    );
    drop(store);

    // 真实重开并读回，而不是只检查回执声称写了什么。
    let reopened = Store::open(dir.join("head.db"), dir.join("cas")).unwrap();
    assert_eq!(reopened.ledger.ledger_id(), ledger_id);
    assert_eq!(
        reopened
            .event_disposition_commit(agent_id, first_id)
            .unwrap(),
        Some(receipt.commit_ref)
    );
    let (restored, successor) = reopened.load_subject(agent_id).unwrap();
    assert_eq!(restored.branch_id, head.branch_id);
    assert_eq!(restored.subject_root_task_id, head.subject_root_task_id);
    assert_eq!(restored.genesis_ref.genesis_id, head.genesis_ref.genesis_id);
    assert_eq!(restored.genesis_ref.ledger_id, head.genesis_ref.ledger_id);
    assert_eq!(restored.controller_epoch, head.controller_epoch);
    assert_eq!(restored.software_generation, head.software_generation);
    assert_eq!(restored.committed_step_no, 1);
    assert_eq!(restored.committed_step_id, Some(first_id));
    assert_eq!(restored.committed_at, receipt.commit_ref);
    assert_eq!(restored.next_step_id, second_step_id);
    assert_eq!(successor.step_id, second_step_id);
    assert_eq!(successor.step_no, 2);
    assert_eq!(successor.state, SubjectStepState::Planned);
    assert_eq!(successor.expected_parent_commit, receipt.commit_ref);
    assert_eq!(successor.created_by_step_id, Some(first_id));
    let committed = reopened
        .get_step(&head.branch_id, first_id)
        .unwrap()
        .unwrap();
    assert_eq!(committed.state, SubjectStepState::Committed);
    assert_eq!(committed.commit_ref, Some(receipt.commit_ref));
    let conn = rusqlite::Connection::open(dir.join("head.db")).unwrap();
    let count: i64 = conn
        .query_row(
            "SELECT count(*) FROM subject_steps WHERE branch_id=?1 AND step_no=2",
            [head.branch_id.0.to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let next = reopened.commit(WriteBatch::new()).unwrap();
    assert!(next.commit_ref.seq.0 > receipt.commit_ref.seq.0);
}

#[test]
fn competing_s1_rejects_stale_writer_without_partial_commit() {
    use shenko_types::{error::ShenkoError, ids::*};
    use std::sync::{Arc, Barrier};
    let (dir, store, agent) = initialized_subject();
    let (head, planned) = store.load_subject(agent).unwrap();
    let result = recorded_result(&store, &head, planned);
    let next_a = EntityId::new();
    let next_b = EntityId::new();
    let step_a = committed_candidate(&store, result.clone(), next_a);
    let step_b = committed_candidate(&store, result, next_b);
    // 同一竞争前提下，错 parent 和陈旧 epoch 都不能占用提交槽。
    let mut wrong_parent = step_a.clone();
    wrong_parent.expected_parent_commit.seq = LedgerSeq(0);
    assert!(matches!(
        store.commit_step_and_advance_head(&wrong_parent, &head, next_a),
        Err(ShenkoError::Conflict(_))
    ));
    let mut stale_head = head.clone();
    stale_head.controller_epoch = ControllerEpoch(head.controller_epoch.0 + 1);
    assert!(matches!(
        store.commit_step_and_advance_head(&step_a, &stale_head, next_a),
        Err(ShenkoError::Conflict(_))
    ));
    let other = Store::open(dir.join("head.db"), dir.join("cas")).unwrap();
    let conn = rusqlite::Connection::open(dir.join("head.db")).unwrap();
    let before: i64 = conn
        .query_row("SELECT count(*) FROM commit_log", [], |r| r.get(0))
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let barrier_a = barrier.clone();
    let head_a = head.clone();
    let a = std::thread::spawn(move || {
        barrier_a.wait();
        store.commit_step_and_advance_head(&step_a, &head_a, next_a)
    });
    let b = std::thread::spawn(move || {
        barrier.wait();
        other.commit_step_and_advance_head(&step_b, &head, next_b)
    });
    let a = a.join().unwrap();
    let b = b.join().unwrap();
    assert_ne!(a.is_ok(), b.is_ok());
    let (winner, loser) = if a.is_ok() {
        (next_a, next_b)
    } else {
        (next_b, next_a)
    };
    assert!(matches!(
        a.as_ref().err().or(b.as_ref().err()).unwrap(),
        ShenkoError::Conflict(_)
    ));
    let reopened = Store::open(dir.join("head.db"), dir.join("cas")).unwrap();
    let (head, successor) = reopened.load_subject(agent).unwrap();
    assert_eq!(head.next_step_id, winner);
    assert_eq!(successor.step_id, winner);
    assert!(reopened.get_step(&head.branch_id, loser).unwrap().is_none());
    let after: i64 = conn
        .query_row("SELECT count(*) FROM commit_log", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, before + 1);
    let steps: i64 = conn
        .query_row("SELECT count(*) FROM subject_steps", [], |r| r.get(0))
        .unwrap();
    assert_eq!(steps, 2);
    let committed = reopened
        .get_step(&head.branch_id, head.committed_step_id.unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(committed.commit_ref, Some(head.committed_at));
}

#[test]
fn task_roundtrip() {
    use shenko_types::commit::CommitRef;
    use shenko_types::ids::*;
    use shenko_types::object::{Digest, ObjectRef, SchemaRef};
    use shenko_types::task::{
        DispatchEligibility, ImplementationRef, TaskOwner, TaskRecord, TaskState,
    };

    let dir = tmp_dir();
    let db = dir.join("task.db");
    let cas = dir.join("cas");
    let store = Store::open(db, cas).unwrap();

    let agent_id = EntityId::new();
    let conv_id = EntityId::new();
    let task_id = EntityId::new();
    let commit_ref = CommitRef::new(store.ledger.ledger_id(), shenko_types::ids::LedgerSeq(1));

    let codec = SchemaRef {
        name: "raw".into(),
        major: 1,
        digest: Digest::blake3(b"raw"),
    };
    let input_ref = ObjectRef {
        digest: Digest::blake3(b"task input"),
        bytes: 10,
        media_type: "text/plain".into(),
        store_namespace: "test".into(),
        locator: "file:///test".into(),
        codec,
    };

    let task = TaskRecord {
        record_schema_version: 1,
        task_id,
        task_revision: TaskRevision(0),
        record_revision: RecordRevision(0),
        agent_id,
        conversation_id: conv_id,
        owner: TaskOwner::Conversation { id: conv_id },
        background: false,
        kind: "test".into(),
        definition_version: 1,
        implementation_ref: ImplementationRef {
            plugin_id: EntityId::new(),
            artifact_digest: Digest::blake3(b"plugin"),
            registry_revision: 1,
        },
        input_ref,
        state: TaskState::Pending {
            checkpoint_ref: None,
        },
        abort_request: None,
        memos: Default::default(),
        residual_case_ids: vec![],
        created_by_step_id: None,
        dispatch_eligibility: DispatchEligibility::Ready,
        no_progress_crashes: 0,
        first_run_at: None,
        terminal_at: None,
        created_commit: commit_ref,
        updated_commit: commit_ref,
    };

    let mut batch = WriteBatch::new();
    batch.task_upserts.push(task);
    let receipt = store.commit(batch).unwrap();

    assert!(!receipt.updated.is_empty());
    assert_eq!(receipt.updated[0].entity_id, task_id);
}
