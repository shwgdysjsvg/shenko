//! Inference broker — dispatches to Anthropic API, local model, or stub. SPEC §13, §22.1, §47
//!
//! Requests are held in a BinaryHeap keyed by (InferencePriority desc, arrival seq asc) so that
//! Urgent(3) > Subject(2) > Normal(1) > Low(0), with FIFO ordering within the same priority.
//!
//! Provider selection (checked once at startup):
//!   SHENKO_USE_LOCAL_MODEL=1  → LocalModelProvider (Qwen3.8-27B via Tailnet, §22.1)
//!   ANTHROPIC_API_KEY set     → AnthropicProvider
//!   neither                   → stub
use anyhow::Result;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::admission::AdmissionController;
use crate::local_provider::LocalModelProvider;
use crate::provider::AnthropicProvider;
use crate::request::{InferenceRequest, InferenceResponse, InferenceResult, InferenceStatus};

enum ActiveProvider {
    Anthropic(AnthropicProvider),
    Local(LocalModelProvider),
    Stub,
}

impl ActiveProvider {
    fn binding(&self) -> crate::request::ProviderBinding {
        use crate::request::ProviderBinding;
        match self {
            Self::Stub => ProviderBinding::Stub { codec: 1 },
            Self::Local(p) => ProviderBinding::Local {
                base_url: p.base_url.clone(),
                model: p.model.clone(),
                allow_invalid_certs: p.allow_invalid_certs,
                serializer_revision: 1,
                default_max_tokens: crate::request::DEFAULT_MAX_TOKENS,
            },
            Self::Anthropic(p) => ProviderBinding::Anthropic {
                base_url: p.base_url.clone(),
                model: p.model.clone(),
                api_version: crate::provider::ANTHROPIC_API_VERSION.into(),
                serializer_revision: 1,
                default_max_tokens: crate::request::DEFAULT_MAX_TOKENS,
            },
        }
    }

    fn from_env() -> Self {
        if std::env::var("SHENKO_USE_LOCAL_MODEL").as_deref() == Ok("1") {
            let p = LocalModelProvider::from_env();
            info!(model=%p.model, "local model provider selected (§22.1)");
            return Self::Local(p);
        }
        if let Some(p) = AnthropicProvider::from_env() {
            info!("Anthropic API key found — real inference enabled");
            return Self::Anthropic(p);
        }
        info!("No ANTHROPIC_API_KEY and SHENKO_USE_LOCAL_MODEL not set — stub mode");
        Self::Stub
    }

    async fn complete(&self, req: &InferenceRequest) -> InferenceResult {
        match self {
            Self::Anthropic(p) => match p.complete(&req.payload).await {
                Ok(response) => InferenceResult {
                    request_id: req.request_id,
                    status: InferenceStatus::Success,
                    response: Some(response),
                },
                Err(e) => {
                    warn!(err=%e, "anthropic inference failed");
                    InferenceResult {
                        request_id: req.request_id,
                        status: failure_status(&e),
                        response: None,
                    }
                }
            },
            Self::Local(p) => match p.complete(&req.payload).await {
                Ok(response) => InferenceResult {
                    request_id: req.request_id,
                    status: InferenceStatus::Success,
                    response: Some(response),
                },
                Err(e) => {
                    warn!(err=%e, "local inference failed");
                    InferenceResult {
                        request_id: req.request_id,
                        status: failure_status(&e),
                        response: None,
                    }
                }
            },
            Self::Stub => InferenceResult {
                request_id: req.request_id,
                status: InferenceStatus::Success,
                response: Some(InferenceResponse {
                    content: "[stub: set ANTHROPIC_API_KEY or SHENKO_USE_LOCAL_MODEL=1]"
                        .to_string(),
                    stop_reason: "end_turn".to_string(),
                    raw_response: r#"{"provider":"stub","stop_reason":"end_turn"}"#.into(),
                    usage: None,
                }),
            },
        }
    }
}

fn failure_status(error: &anyhow::Error) -> InferenceStatus {
    match error.downcast_ref::<crate::request::ProviderRejection>() {
        Some(rejection) => InferenceStatus::Rejected {
            http_status: rejection.http_status,
        },
        None => InferenceStatus::Failed,
    }
}

#[derive(Clone)]
pub struct BrokerHandle {
    tx: mpsc::UnboundedSender<InferenceRequest>,
    binding: crate::request::ProviderBinding,
}

impl BrokerHandle {
    pub fn binding(&self) -> &crate::request::ProviderBinding {
        &self.binding
    }

    pub fn submit(&self, req: InferenceRequest) -> Result<()> {
        anyhow::ensure!(
            req.provider_binding == self.binding,
            "MODEL_PROVIDER_BINDING_MISMATCH: request not admitted"
        );
        self.tx
            .send(req)
            .map_err(|e| anyhow::anyhow!("broker: {e}"))
    }
}

/// Wraps an InferenceRequest so that BinaryHeap (max-heap) dequeues by highest priority first,
/// then by lowest sequence number (earliest arrival) for equal-priority ties.
struct Queued {
    req: InferenceRequest,
    seq: u64,
}

impl Queued {
    #[inline]
    fn priority_val(&self) -> u8 {
        self.req.priority as u8
    }
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.priority_val() == other.priority_val() && self.seq == other.seq
    }
}
impl Eq for Queued {}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority wins; for equal priority, the earlier-arriving request (lower seq) wins.
        self.priority_val()
            .cmp(&other.priority_val())
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct Broker {
    rx: mpsc::UnboundedReceiver<InferenceRequest>,
    admission: AdmissionController,
    provider: ActiveProvider,
}

impl Broker {
    pub fn new() -> Result<(Self, BrokerHandle)> {
        let provider = ActiveProvider::from_env();
        provider.binding().validate_endpoint()?;
        Ok(Self::with_provider(provider))
    }

    /// Explicit offline construction; does not inspect or use ambient credentials.
    pub fn offline() -> (Self, BrokerHandle) {
        Self::with_provider(ActiveProvider::Stub)
    }

    fn with_provider(provider: ActiveProvider) -> (Self, BrokerHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let binding = provider.binding();
        (
            Self {
                rx,
                admission: AdmissionController::nominal(),
                provider,
            },
            BrokerHandle { tx, binding },
        )
    }

    pub async fn run(mut self) {
        info!("inference broker running");
        let mut queue: BinaryHeap<Queued> = BinaryHeap::new();
        let mut seq: u64 = 0;

        loop {
            // Drain all immediately available requests into the priority queue.
            loop {
                match self.rx.try_recv() {
                    Ok(req) => {
                        queue.push(Queued { req, seq });
                        seq += 1;
                    }
                    Err(_) => break,
                }
            }

            // If nothing is queued yet, block until at least one request arrives.
            if queue.is_empty() {
                match self.rx.recv().await {
                    Some(req) => {
                        queue.push(Queued { req, seq });
                        seq += 1;
                    }
                    None => break, // channel closed — all handles dropped
                }
                // Drain any requests that arrived concurrently while we were waiting.
                loop {
                    match self.rx.try_recv() {
                        Ok(req) => {
                            queue.push(Queued { req, seq });
                            seq += 1;
                        }
                        Err(_) => break,
                    }
                }
            }

            // Dequeue the highest-priority (earliest-within-tier) request.
            let req = queue.pop().expect("queue non-empty").req;

            let guard = match self.admission.try_admit() {
                Some(g) => g,
                None => {
                    let _ = req.result_tx.send(InferenceResult {
                        request_id: req.request_id,
                        status: InferenceStatus::QueuedCapacityWait,
                        response: None,
                    });
                    continue;
                }
            };

            let result = self.provider.complete(&req).await;
            if matches!(
                result.status,
                InferenceStatus::Failed | InferenceStatus::Cancelled
            ) {
                // No verified remote stop: keep the allocation charged. The
                // subject's AwaitingModel record also blocks blind reopen replay.
                drop(guard); // deliberately NOT released
            } else {
                guard.release_terminal();
            }
            let _ = req.result_tx.send(result);
        }
        info!("inference broker stopped");
    }
}
