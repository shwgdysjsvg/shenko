//! Shenko — personal agent framework entry point.
//! SPEC §44: seed/bootstrap startup with real subject loop + broker.
use anyhow::Result;
use clap::{Parser, Subcommand};
use shenko_identity::IdentityService;
use shenko_types::identity::{IdentityChange, IdentityProfile, IdentitySource};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(
    name = "shenko",
    about = "Shenko（深刻）— personal agent framework",
    version = "0.1.0"
)]
struct Cli {
    #[arg(long, env = "SHENKO_DATA_DIR")]
    data_dir: Option<PathBuf>,
    #[arg(long, default_value = "info", env = "SHENKO_LOG")]
    log: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start Shenko agent with UI and continuous thought loop
    Start {
        #[arg(long, default_value = "7860")]
        port: u16,
        /// Instance name given by the user. Omit it to let the agent name
        /// itself. Existing instances are edited with an explicit revision CAS.
        #[arg(long)]
        name: Option<String>,
        /// NapCat WebSocket URL (e.g. ws://127.0.0.1:3001)
        #[arg(long, env = "NAPCAT_WS_URL")]
        napcat: Option<String>,
        /// Account bound to the persistent ingress database (not an admin grant).
        #[arg(long, env = "NAPCAT_BOT_QQ", requires = "napcat")]
        napcat_account: Option<u64>,
    },
    /// Show or edit the instance's name, soul and personality — SPEC §6.
    /// Every edit is a new revision of the same agent; history is kept.
    Identity {
        #[arg(long)]
        set_name: Option<String>,
        #[arg(long)]
        set_soul: Option<String>,
        #[arg(long)]
        set_personality: Option<String>,
        /// Revision read by the editor; required for any mutation.
        #[arg(long)]
        expected_revision: Option<u64>,
        /// Print every revision, oldest first
        #[arg(long)]
        history: bool,
    },
    /// Validate configuration and exit
    Check,
    /// Print version info
    Version,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    fmt()
        .with_env_filter(EnvFilter::new(&cli.log))
        .with_target(true)
        .compact()
        .init();

    let data_dir = cli.data_dir.unwrap_or_else(|| {
        dirs_next::data_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("shenko")
    });

    // Reuse the actual published C03 host: one native root, one install.
    let supervisor =
        shenko_kernel::supervisor::Supervisor::new(shenko_types::ids::RuntimeGeneration::default());
    let policy: Arc<dyn shenko_identity::IdentityPolicy> =
        Arc::new(shenko_identity::DefaultIdentityPolicy);
    let _identity_plugin = supervisor
        .spawn(shenko_identity::IdentityPlugin, policy)
        .await?;
    let identity_context = supervisor.context().clone();
    let identity_service = identity_context.try_service::<IdentityService>()?;
    let result: Result<()> = async {
    match cli.command {
        Command::Version => {
            println!("Shenko（深刻）0.1.0  SPEC v0.6.2");
            println!("Model: claude-opus-5-5");
            println!(
                "ANTHROPIC_API_KEY: {}",
                if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                    "set"
                } else {
                    "not set (stub mode)"
                }
            );
        }

        Command::Check => {
            info!(data_dir=%data_dir.display(), "check");
            tokio::fs::create_dir_all(&data_dir).await?;
            let db_path = data_dir.join("shenko.db");
            let cas_dir = data_dir.join("cas");
            let store = shenko_store::store::Store::open(db_path, cas_dir)?;
            println!(
                "Store: OK (SQLite {}, WAL={}, FTS5={})",
                store.capabilities.sqlite_version,
                store.capabilities.wal_mode,
                store.capabilities.fts5_available,
            );
            println!(
                "API key: {}",
                if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                    "present"
                } else {
                    "absent — stub mode"
                }
            );
            println!("OK");
        }

        Command::Identity {
            set_name,
            set_soul,
            set_personality,
            expected_revision,
            history,
        } => {
            tokio::fs::create_dir_all(&data_dir).await?;
            let store =
                shenko_store::store::Store::open(data_dir.join("shenko.db"), data_dir.join("cas"))?;
            let agent_id = store
                .get_existing_agent_id()?
                .ok_or_else(|| anyhow::anyhow!("no instance yet; run `shenko start` first"))?;
            let mut current = store
                .latest_identity(agent_id)?
                .ok_or_else(|| anyhow::anyhow!("identity not initialized"))?;
            let changes = [
                set_name.map(IdentityChange::Name),
                set_soul.map(IdentityChange::Soul),
                set_personality.map(IdentityChange::Personality),
            ];
            let changes: Vec<_> = changes.into_iter().flatten().collect();
            if !changes.is_empty() {
                let expected = expected_revision.ok_or_else(|| {
                    anyhow::anyhow!(
                        "editing requires --expected-revision from the profile you read"
                    )
                })?;
                anyhow::ensure!(
                    current.revision == expected,
                    "IDENTITY_REVISION_CONFLICT: expected {expected}, actual {}",
                    current.revision
                );
                if let Some(next) =
                    identity_service.revise(&current, &changes, IdentitySource::Operator)
                {
                    store.append_identity(&next, expected)?;
                    current = next;
                }
            }
            let shown = if history {
                store.identity_history(agent_id)?
            } else {
                vec![current]
            };
            for profile in shown {
                println!("{}", serde_json::to_string_pretty(&profile)?);
            }
        }

        Command::Start {
            port,
            name,
            napcat,
            napcat_account,
        } => {
            if napcat.is_some() {
                anyhow::ensure!(
                    napcat_account.is_some_and(|account| account > 0),
                    "NapCat persistent ingress requires --napcat-account or NAPCAT_BOT_QQ"
                );
            }
            info!(
                data_dir=%data_dir.display(), port,
                name=name.as_deref().unwrap_or("(unnamed)"),
                "Shenko starting"
            );

            // 1. Open persistent store
            tokio::fs::create_dir_all(&data_dir).await?;
            let db_path = data_dir.join("shenko.db");
            let cas_dir = data_dir.join("cas");
            let store = Arc::new(shenko_store::store::Store::open(db_path, cas_dir)?);
            info!(
                sqlite=%store.capabilities.sqlite_version,
                wal=store.capabilities.wal_mode,
                fts5=store.capabilities.fts5_available,
                "store ready"
            );

            // 2. Bootstrap genesis if first run — SPEC §44.6
            let agent_id = bootstrap_genesis(&store, name.as_deref())?;
            let identity = adopt_identity(&store, agent_id, name.as_deref(), &identity_service)?;
            info!(
                revision = identity.revision,
                name = identity.display_name.as_deref().unwrap_or("(unnamed)"),
                naming = ?identity.naming,
                "identity ready"
            );

            // 3. Start inference broker
            let (broker, broker_handle) = shenko_broker::broker::Broker::new()?;
            tokio::spawn(broker.run());

            // 4. Start subject loop — the continuous main agent. SPEC §5.1
            let cancel = tokio_util::sync::CancellationToken::new();
            let subject_inbox = Arc::new(shenko_subject::inbox::Inbox::new(1024));
            // Establish recovery consumers BEFORE ResultRecorded can take fast S1.
            store.start_subject_dispatcher()?;
            store.settle_subject_memory()?;
            store.reconcile_subject_publications()?;
            let consumer_cancel = tokio_util::sync::CancellationToken::new();
            let dispatch_store = Arc::clone(&store);
            let dispatch_stop = consumer_cancel.clone();
            let failure_cancel = cancel.clone();
            let dispatcher_task = tokio::spawn(async move {
                let result = shenko_subject::dispatch::run_committed_dispatcher(dispatch_store, dispatch_stop).await;
                if result.is_err() { failure_cancel.cancel(); }
                result
            });
            let hook_registry = Arc::new(shenko_kernel::hooks::HookRegistry::new());
            // Outbound path: subject model output → UI timeline. SPEC §5.1, §21.12
            let (outbox_tx, _outbox_rx) =
                tokio::sync::broadcast::channel::<shenko_subject::outbox::OutboxMessage>(64);
            let loop_state = shenko_subject::loop_runner::SubjectLoopState::new(
                agent_id,
                Arc::clone(&subject_inbox),
                Arc::clone(&store),
                identity_context.clone(),
            )?
            .with_broker(broker_handle)
            .with_hooks(Arc::clone(&hook_registry))
            .with_outbox(outbox_tx.clone());

            // 5. Start NapCat if configured — SPEC §16, §35.6
            if let Some(ws_url) = napcat {
                use shenko_channel_napcat::outbound::{
                    OutboundKind, OutboundLedger, OutboundOperation,
                };

                let napcat_config = shenko_channel_napcat::config::NapCatConfig {
                    ws_url,
                    bot_qq: napcat_account.expect("NapCat account validated before startup"),
                    ..Default::default()
                };
                // 独立接收域先落盘；打开失败停止启动，绝不降级到内存。
                let (napcat_client, mut napcat_event_rx, napcat_action_rx) =
                    shenko_channel_napcat::client::NapCatClient::open_persistent(
                        napcat_config.clone(),
                        data_dir.join("napcat-ingress.sqlite"),
                    )?;
                let client = Arc::new(napcat_client);
                let spool = client.spool.clone();
                let event_tx = client.event_sender();
                let pending_actions = client.pending_actions.clone();
                let cancel_napcat = consumer_cancel.clone();

                // A storage failure closes the connector and stops this runtime;
                // silently leaving an apparently healthy UI would hide lost reception.
                let connection_spool = Arc::clone(&spool);
                let connection_config = napcat_config.clone();
                let connection_cancel = cancel_napcat.clone();
                tokio::spawn(async move {
                    if let Err(error) =
                        shenko_channel_napcat::client::NapCatClient::run_connection_loop(
                            connection_config,
                            event_tx,
                            napcat_action_rx,
                            pending_actions,
                            connection_spool,
                            connection_cancel.clone(),
                        )
                        .await
                    {
                        tracing::error!(err=%error, "NapCat ingress failed — stopping runtime for recovery");
                        connection_cancel.cancel();
                    }
                });

                // Keep the ingress identity across replay. L's committed S1 receipt
                // wins over a stale I acknowledgement mirror after a crash.
                let inbox_tx = subject_inbox.sender();
                let event_store = Arc::clone(&store);
                let event_spool = Arc::clone(&spool);
                let cancel_events = cancel_napcat.clone();
                tokio::spawn(async move {
                    let result: Result<()> = async {
                        loop {
                            let ev = tokio::select! {
                                ev = napcat_event_rx.recv() => match ev {
                                    Some(ev) => ev,
                                    None => break,
                                },
                                _ = cancel_events.cancelled() => break,
                            };
                            let event_id = ev.ingress_id.ok_or_else(|| anyhow::anyhow!("NapCat event missing durable ingress identity"))?;
                            if let Some(commit) = event_store.event_disposition_commit(agent_id, event_id)? {
                                event_spool.ack(event_id, &commit.to_string())?;
                                continue;
                            }
                            let inbox_ev = napcat_inbox_event(ev)?;
                            tokio::select! {
                                sent = inbox_tx.send(inbox_ev) => sent.map_err(|_| anyhow::anyhow!("subject inbox closed; ingress remains pending"))?,
                                _ = cancel_events.cancelled() => break,
                            }
                        }
                        Ok(())
                    }.await;
                    if let Err(error) = result {
                        tracing::error!(err=%error, "NapCat admission failed — ingress remains pending");
                        cancel_events.cancel();
                    }
                });

                // Poll bounded pages: cross-node watchers and one-shot callbacks
                // are not recovery truth. No S1 receipt means no ACK, even if delivered.
                let ack_store = Arc::clone(&store);
                let cancel_acks = cancel_napcat.clone();
                tokio::spawn(async move {
                    let result: Result<()> = async {
                        let mut after = 0;
                        let mut tick = tokio::time::interval(std::time::Duration::from_millis(250));
                        loop {
                            tokio::select! {
                                _ = tick.tick() => {},
                                _ = cancel_acks.cancelled() => break,
                            }
                            let page = spool.pending_after(after, 256)?;
                            if page.is_empty() {
                                after = 0;
                            }
                            for event in page {
                                after = event.seq;
                                if let Some(commit) =
                                    ack_store.event_disposition_commit(agent_id, event.spool_id)?
                                {
                                    spool.ack(event.spool_id, &commit.to_string())?;
                                }
                            }
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(error) = result {
                        tracing::error!(err=%error, "NapCat acknowledgement mirror failed — reopen required");
                        cancel_acks.cancel();
                    }
                });

                // Outbound replies: the model emits an explicit target so we
                // never guess who to answer. Every send is recorded in the
                // durable operation ledger *before* it is written, and a
                // missing echo is recorded as unknown for reconciliation.
                // SPEC §35.6, §41.4 X03.
                let ledger = Arc::new(
                    OutboundLedger::with_store(Arc::clone(&store))
                        .with_account(napcat_config.bot_qq),
                );
                let reply_client = Arc::clone(&client);
                let reply_store = Arc::clone(&store);
                let cancel_replies = consumer_cancel.clone();
                let reply_failure = cancel.clone();
                tokio::spawn(async move {
                    let result: Result<()> = async {
                    let mut tick=tokio::time::interval(std::time::Duration::from_millis(250));
                    loop {
                        tokio::select! { biased;
                            _ = cancel_replies.cancelled() => break,
                            _ = tick.tick() => {}
                        }
                        // Ready index, not the broadcast buffer, owns the work.
                        for work in reply_store.committed_intents(Some("publication"), Some("ready"), 32)? {
                            let publication: shenko_types::subject::PublicationIntent = serde_json::from_value(work.payload)?;

                            let (kind, target) = match publication.target {
                                shenko_types::output::PublicationTarget::QqGroup(id) => {
                                    (OutboundKind::SendGroup, id)
                                }
                                shenko_types::output::PublicationTarget::QqPrivate(id) => {
                                    (OutboundKind::SendPrivate, id)
                                }
                            };
                            let mut op =
                                OutboundOperation::new(kind, target, publication.content.clone());
                            op.operation_id = publication.intent_id;
                            op.echo_token = publication.intent_id.to_string();
                            let op_id = op.operation_id;
                            if !ledger.record_intent(&op)? { continue; }

                            let send_result = match kind {
                                OutboundKind::SendGroup => {
                                    reply_client
                                        .send_group_message_with_echo(
                                            target,
                                            &op.message,
                                            op.echo_token.clone(),
                                        )
                                        .await
                                }
                                OutboundKind::SendPrivate => {
                                    reply_client
                                        .send_private_message_with_echo(
                                            target,
                                            &op.message,
                                            op.echo_token.clone(),
                                        )
                                        .await
                                }
                            };

                            match send_result {
                                Ok(receipt) => {
                                    ledger.record_response(op_id, &receipt)?;
                                    if receipt.is_success() {
                                        tracing::info!(op=%op_id, target, kind=kind.as_str(), "QQ reply sent");
                                    } else {
                                        tracing::warn!(
                                            op=%op_id, status=%receipt.status, retcode=receipt.retcode,
                                            "NapCat rejected QQ reply"
                                        );
                                    }
                                }
                                Err(e) => {
                                    // The request may have been executed. Record the
                                    // unknown effect and leave resolution to
                                    // reconciliation; never blind-resend.
                                    tracing::warn!(err=%e, op=%op_id, "QQ reply outcome unknown");
                                    ledger.record_unknown(op_id, &e.to_string())?;
                                }
                            }
                        }
                    }
                    Ok(())
                    }.await;
                    if let Err(e)=result { tracing::error!(err=%e,"publication consumer stopped; reopen writer and reconcile"); reply_failure.cancel(); }
                });
                info!("NapCat connector started");
            }

            // Connector/ACK storage failures also stop admission in the subject.
            let connector_failure = cancel.clone();
            let connector_stop = consumer_cancel.clone();
            tokio::spawn(async move { connector_stop.cancelled().await; connector_failure.cancel(); });
            let cancel_subject = cancel.clone();
            let mut subject_task = tokio::spawn(shenko_subject::loop_runner::run_subject_loop(
                loop_state,
                cancel_subject,
            ));
            info!("subject loop started");


            // 6. Start WebUI
            let ui = shenko_ui::server::UiServer::new(port)
                .with_inbox(subject_inbox.sender())
                .with_outbox(outbox_tx)
                .with_store(Arc::clone(&store))
                .with_data_dir(data_dir.clone());
            println!("Shenko UI: http://0.0.0.0:{port}");
            println!("  Subject loop: running (continuous thought)");
            println!(
                "  Inference: {}",
                if std::env::var("ANTHROPIC_API_KEY").is_ok() {
                    "claude-opus-5-5"
                } else {
                    "stub (no ANTHROPIC_API_KEY)"
                }
            );

            // Supervise the actual subject task, including panics. Stop claiming
            // availability before waiting for the current durable boundary.
            let live = ui.ws_state().live;
            let mut subject_finished = false;
            let runtime_result: Result<()> = tokio::select! {
                result = &mut subject_task => {
                    subject_finished = true;
                    match result {
                        Ok(Err(error)) => Err(error.into()),
                        Err(error) => Err(error.into()),
                        Ok(Ok(())) => Err(anyhow::anyhow!("subject stopped unexpectedly")),
                    }
                }
                result = ui.run() => result,
                result = tokio::signal::ctrl_c() => {
                    result?;
                    info!("shutdown signal; settling subject at a recoverable boundary");
                    Ok(())
                }
                _ = cancel.cancelled() => Err(anyhow::anyhow!("runtime stopped after connector failure; reopen for recovery")),
            };
            live.set_wired(false);
            subject_inbox.close().await;
            cancel.cancel();
            if !subject_finished {
                // Do not drop an admitted request merely because Ctrl-C arrived.
                // A timeout is an observation limit, never proof the provider stopped.
                match tokio::time::timeout(std::time::Duration::from_secs(15), &mut subject_task)
                    .await
                {
                    Ok(Ok(result)) => result?,
                    Ok(Err(error)) => return Err(error.into()),
                    Err(_) => {
                        consumer_cancel.cancel();
                        let (_, step) = store.load_subject(agent_id)?;
                        anyhow::bail!(
                            "shutdown settlement timed out: durable step {} is {:?}, request {:?}; remote outcome is NOT proven stopped, reopen/reconcile before dispatch",
                            step.step_id,
                            step.state,
                            step.active_request_id
                        );
                    }
                }
            }
            // S1 has settled or left a durable unresolved step. Do not drop the
            // consumers first. Their unfinished work remains queryable on reopen.
            consumer_cancel.cancel();
            match tokio::time::timeout(std::time::Duration::from_secs(2), dispatcher_task).await {
                Ok(result) => result??,
                Err(_) => tracing::warn!("dispatcher stop pending; durable work retained"),
            }
            runtime_result?;
            info!("Shenko stopped");
        }
    }

    Ok(())
    }.await;
    supervisor.stop().await?;
    result
}

fn napcat_inbox_event(
    event: shenko_channel_napcat::client::OneBotEvent,
) -> Result<shenko_subject::inbox::InboxEvent> {
    Ok(shenko_subject::inbox::InboxEvent {
        event_id: event
            .ingress_id
            .ok_or_else(|| anyhow::anyhow!("NapCat event missing durable ingress identity"))?,
        kind: shenko_subject::inbox::InboxEventKind::Message,
        payload: serde_json::to_value(&event)?,
        created_at: chrono::Utc::now(),
        source: "napcat".to_string(),
    })
}

/// Make sure the agent has an identity revision, and apply a user-given
/// `--name` only for initialization. Existing edits require an explicit CAS. SPEC §6, R117.
fn adopt_identity(
    store: &shenko_store::store::Store,
    agent_id: shenko_types::ids::EntityId,
    name: Option<&str>,
    identity: &IdentityService,
) -> Result<IdentityProfile> {
    let genesis = identity.genesis(agent_id, name);
    let current = store.initialize_identity(&genesis)?;
    anyhow::ensure!(
        name.is_none() || current.display_name == genesis.display_name,
        "IDENTITY_INITIAL_NAME_CONFLICT: start --name only initializes; use identity --expected-revision {} to edit",
        current.revision
    );
    Ok(current)
}

/// Ensure genesis exists for this installation. Returns agent_id. SPEC §44.6
fn bootstrap_genesis(
    store: &shenko_store::store::Store,
    name: Option<&str>,
) -> Result<shenko_types::ids::EntityId> {
    use shenko_types::commit::CommitRef;
    use shenko_types::ids::*;
    use shenko_types::object::ObjectRef;
    use shenko_types::subject::*;

    // Check if genesis exists already
    let existing_agent_id = store.get_existing_agent_id()?;

    if let Some(agent_id) = existing_agent_id {
        info!(agent_id=%agent_id, "existing genesis found — resuming");
        return Ok(agent_id);
    }

    // First run — create genesis. SPEC §44.6
    let agent_id = EntityId::new();
    let bootstrap_id = EntityId::new();
    let launch_id = EntityId::new();
    let genesis_id = EntityId::new();
    let ledger_id = store.ledger.ledger_id();

    // Seal the real seed artifacts into the CAS so the genesis object closure is
    // materialized and re-readable. SPEC §44.6.
    let seal_json = |value: &serde_json::Value| -> Result<ObjectRef> {
        let bytes = serde_json::to_vec(value)?;
        Ok(store
            .cas
            .seal_object(&bytes, "application/json", "seed", "json", 1)?
            .object)
    };

    let seed_manifest = seal_json(&serde_json::json!({
        "seed_version": env!("CARGO_PKG_VERSION"),
        "platform": std::env::consts::OS,
        "created": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "spec_version": "0.6.2",
    }))?;
    let initial_profile = seal_json(&serde_json::json!({
        "model": "claude-opus-5-5",
        "max_tokens": 4096,
        "source": "default",
    }))?;
    let home_dir = std::env::var("SHENKO_HOME")
        .map(|h| format!("{h}/home"))
        .unwrap_or_else(|_| "$SHENKO_HOME/home".to_string());
    let home_manifest = seal_json(&serde_json::json!({
        "home": home_dir,
        "wasm": true,
        "mounts": [],
    }))?;
    let bootstrap_event = seal_json(&serde_json::json!({
        "event": "bootstrap",
        "timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        "kind": "genesis",
    }))?;

    let genesis = SubjectGenesis {
        genesis_id,
        subject_id: agent_id,
        bootstrap_id,
        launch_id,
        seed_manifest_ref: seed_manifest,
        initial_profile_ref: initial_profile,
        home_manifest_ref: home_manifest,
        bootstrap_event_ref: bootstrap_event,
        created_commit: CommitRef {
            ledger_id,
            seq: LedgerSeq(0),
        },
    };

    let first_step_id = EntityId::new();
    let head = SubjectBranchHead {
        agent_id,
        branch_id: BranchId::new(),
        subject_root_task_id: EntityId::new(),
        controller_epoch: ControllerEpoch(1),
        software_generation: RuntimeGeneration::default(),
        genesis_ref: GenesisRef {
            ledger_id,
            genesis_id,
        },
        committed_step_no: 0,
        committed_step_id: None,
        committed_at: CommitRef {
            ledger_id,
            seq: LedgerSeq(0),
        },
        next_step_id: first_step_id,
        mode: SubjectMode::Active,
        mode_reason_ref: None,
        record_revision: RecordRevision(0),
    };

    let first_step = shenko_types::subject::SubjectStep {
        step_id: first_step_id,
        branch_id: head.branch_id.clone(),
        step_no: 1,
        expected_parent_commit: genesis.created_commit,
        controller_epoch: ControllerEpoch(1),
        software_generation: RuntimeGeneration::default(),
        state: shenko_types::subject::SubjectStepState::Planned,
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

    let receipt = store.initialize_genesis(&genesis, &head, &first_step)?;
    info!(agent_id=%agent_id, name=name.unwrap_or("(unnamed)"), commit=%receipt.commit_ref, "genesis created");

    Ok(agent_id)
}

#[cfg(test)]
mod recovery_integration_tests {
    use super::*;
    use shenko_channel_napcat::{NapCatClient, NapCatConfig, spool::IngressSpool};
    use shenko_types::{ids::*, subject::*};

    // Local I/L/CAS simulation: crash after S1, before I mirrors its ACK.
    // No provider, external account, model inference or production data is used.
    #[tokio::test]
    async fn reopen_reconciles_spool_from_committed_subject_disposition() {
        let dir = std::env::temp_dir().join(format!("shenko-continuity-{}", EntityId::new()));
        std::fs::create_dir(&dir).unwrap();
        let spool_path = dir.join("ingress.sqlite");
        let spool = IngressSpool::open(&spool_path, 9).unwrap();
        let received = spool
            .ingest(serde_json::json!({
                "post_type":"message", "message_type":"group", "group_id":7,
                "self_id":9, "user_id":8, "message_id":42, "raw_message":"continue"
            }))
            .unwrap()
            .unwrap();
        drop(spool);

        let config = NapCatConfig {
            bot_qq: 9,
            ws_url: "ws://127.0.0.1:0".into(),
            ..Default::default()
        };
        let (client, mut rx, actions) =
            NapCatClient::open_persistent(config.clone(), &spool_path).unwrap();
        let cancel = tokio_util::sync::CancellationToken::new();
        let connector = tokio::spawn(NapCatClient::run_connection_loop(
            config,
            client.event_sender(),
            actions,
            client.pending_actions.clone(),
            client.spool.clone(),
            cancel.clone(),
        ));
        // Delivery now runs independently of connection establishment.
        let replayed = tokio::time::timeout(std::time::Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        let inbox_event = napcat_inbox_event(replayed).unwrap();
        cancel.cancel();
        connector.await.unwrap().unwrap();
        assert_eq!(inbox_event.event_id, received.spool_id);

        let store =
            shenko_store::store::Store::open(dir.join("control.sqlite"), dir.join("cas")).unwrap();
        let agent = bootstrap_genesis(&store, None).unwrap();
        let (head, mut step) = store.load_subject(agent).unwrap();
        assert!(
            store
                .event_disposition_commit(agent, inbox_event.event_id)
                .unwrap()
                .is_none()
        );
        assert!(!client.spool.event(received.spool_id).unwrap().consumed);
        let object = store
            .cas
            .seal_object(b"continue", "text/plain", "test", "raw", 1)
            .unwrap()
            .object;
        step.input_plan_ref = Some(object.clone());
        step.state = SubjectStepState::Preparing;
        step.record_revision = RecordRevision(1);
        store.upsert_step(&step, &head, RecordRevision(0)).unwrap();
        step.state = SubjectStepState::AwaitingModel;
        step.active_request_id = Some(EntityId::new());
        step.record_revision = RecordRevision(2);
        store.upsert_step(&step, &head, RecordRevision(1)).unwrap();
        step.state = SubjectStepState::ResultRecorded;
        step.model_result_ref = Some(object.clone());
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
        let next = EntityId::new();
        let identity_context = cordis_core::Context::new();
        let _publication = identity_context
            .provide(Arc::new(IdentityService::new(Arc::new(
                shenko_identity::DefaultIdentityPolicy,
            ))))
            .unwrap();
        let identity_service = identity_context.try_service::<IdentityService>().unwrap();
        let decision = StepDecision {
            schema_version: 3,
            output: shenko_subject::output::parse_output(
                step.step_id,
                object.clone(),
                "continue",
                &identity_service,
            )
            .unwrap(),
            identity_intent: None,
            step_id: step.step_id,
            input_plan_ref: object.clone(),
            model_result_ref: Some(object),
            applied_steering_refs: vec![],
            event_dispositions: vec![EventDisposition {
                event_id: inbox_event.event_id,
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
        step.record_revision = RecordRevision(4);
        let receipt = store
            .commit_step_and_advance_head(&step, &head, next)
            .unwrap();
        // Intentionally omit ACK: model-side processing committed, I mirror is stale.
        assert!(!client.spool.event(received.spool_id).unwrap().consumed);
        drop(store);
        drop(client);

        let store = Arc::new(
            shenko_store::store::Store::open(dir.join("control.sqlite"), dir.join("cas")).unwrap(),
        );
        assert_eq!(bootstrap_genesis(&store, None).unwrap(), agent);
        let restored = shenko_subject::loop_runner::SubjectLoopState::new(
            agent,
            Arc::new(shenko_subject::inbox::Inbox::new(8)),
            store.clone(),
            identity_context,
        )
        .unwrap();
        assert_eq!(restored.head.branch_id, head.branch_id);
        assert_eq!(restored.head.committed_at, receipt.commit_ref);
        assert_eq!(restored.step.step_id, next);
        assert_eq!(restored.step.state, SubjectStepState::Planned);
        let spool = IngressSpool::open(&spool_path, 9).unwrap();
        let pending = spool.consume(8).unwrap();
        assert_eq!(pending.len(), 1);
        let commit = store
            .event_disposition_commit(agent, pending[0].spool_id)
            .unwrap()
            .unwrap();
        assert_eq!(commit, receipt.commit_ref);
        spool.ack(pending[0].spool_id, &commit.to_string()).unwrap();
        assert_eq!(spool.pending_count().unwrap(), 0);
        assert_eq!(spool.event(received.spool_id).unwrap().raw, received.raw);
        assert!(spool.ingest(received.raw).unwrap().is_none());
        // Temporary evidence files are deliberately retained for inspection.
    }
}

#[cfg(test)]
mod reply_directive_tests {
    use shenko_types::{ids::EntityId, output::*};
    #[test]
    fn identity_lines_are_actions_not_reply_text() {
        let out = "先想一下\n[reply:group:7]  你好\n[identity:name] 岚\n继续说\n[identity:soul] 记住关系；不要把 delegate 和委派当指令\n[remember] 一整段记忆\n[delegate] 完整有限目标\n> [delegate] 引用，不执行\n[remember] 懒续行仍是引用\n\n[identity:personality] 这是链接旁的说明\n\n[identity:personality]: https://example.invalid/\n\n[delegate](https://example.invalid/)\n[1] 引用编号不是控制行\n```text\n[identity:name] 代码，不执行\n[remember] 代码例子\n```\n[reply:private:9] 第二条";
        let dir = std::env::temp_dir().join(format!("shenko-controls-{}", EntityId::new()));
        std::fs::create_dir_all(&dir).unwrap();
        let store = shenko_store::store::Store::open(dir.join("s.db"), dir.join("cas")).unwrap();
        let raw = store
            .cas
            .seal_object(out.as_bytes(), "text/plain", "test", "text", 1)
            .unwrap()
            .object;
        let identity = shenko_identity::IdentityService::new(std::sync::Arc::new(
            shenko_identity::DefaultIdentityPolicy,
        ));
        let parsed =
            shenko_subject::output::parse_output(EntityId::new(), raw.clone(), out, &identity)
                .unwrap();
        assert_eq!(parsed.controls.len(), 6); // two replies, two identity, one memory, one task
        let first = &parsed.controls[0];
        let OutputAction::Reply { target, text } = &first.action else {
            panic!("reply")
        };
        assert_eq!(*target, PublicationTarget::QqGroup(7));
        assert_eq!(
            text,
            " 你好\n继续说\n> [delegate] 引用，不执行\n[remember] 懒续行仍是引用\n\n[identity:personality] 这是链接旁的说明\n\n[identity:personality]: https://example.invalid/\n\n[delegate](https://example.invalid/)\n[1] 引用编号不是控制行\n```text\n[identity:name] 代码，不执行\n[remember] 代码例子\n```\n"
        );
        assert!(!parsed.text.contains("一整段记忆") && !parsed.text.contains("完整有限目标"));
        assert_eq!(
            &out[first.spans[0].start..first.spans[0].end],
            "[reply:group:7]  你好\n"
        );
        assert_eq!(
            store.cas.read_object(&parsed.raw_ref.digest).unwrap(),
            out.as_bytes()
        );
        // Unsupported old syntax is a diagnosed protocol error, not a fallback.
        let obsolete = store
            .cas
            .seal_object(b"[memory] obsolete", "text/plain", "test", "text", 1)
            .unwrap()
            .object;
        assert!(
            shenko_subject::output::parse_output(
                EntityId::new(),
                obsolete,
                "[memory] obsolete",
                &identity
            )
            .unwrap_err()
            .to_string()
            .contains("CONTROL_PROTOCOL_INVALID")
        );
    }
}
