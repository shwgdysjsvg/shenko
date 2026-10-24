use serde::{Deserialize, Serialize};
use shenko_types::ids::{ControllerEpoch, EntityId};
use tokio::sync::oneshot;

pub(crate) const DEFAULT_MAX_TOKENS: u64 = 8192;

/// Non-secret effective transport/model contract, fixed by broker construction.
/// It is sealed into S0; a recovered request must not route through a new binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProviderBinding {
    Stub {
        codec: u32,
    },
    Local {
        base_url: String,
        model: String,
        allow_invalid_certs: bool,
        serializer_revision: u32,
        default_max_tokens: u64,
    },
    Anthropic {
        base_url: String,
        model: String,
        api_version: String,
        serializer_revision: u32,
        default_max_tokens: u64,
    },
}

impl ProviderBinding {
    pub fn model(&self) -> &str {
        match self {
            Self::Stub { .. } => "stub",
            Self::Local { model, .. } | Self::Anthropic { model, .. } => model,
        }
    }

    pub(crate) fn validate_endpoint(&self) -> anyhow::Result<()> {
        let endpoint = match self {
            Self::Stub { .. } => return Ok(()),
            Self::Local { base_url, .. } | Self::Anthropic { base_url, .. } => base_url,
        };
        let url = reqwest::Url::parse(endpoint)
            .map_err(|_| anyhow::anyhow!("invalid provider endpoint"))?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "provider endpoint must be HTTP(S) without credentials/query/fragment; use separate credentials"
        );
        Ok(())
    }
}

pub struct InferenceRequest {
    pub request_id: EntityId,
    pub agent_id: EntityId,
    pub conversation_id: EntityId,
    pub provider_binding: ProviderBinding,
    pub priority: InferencePriority,
    pub epoch: ControllerEpoch,
    pub payload: InferencePayload,
    pub result_tx: oneshot::Sender<InferenceResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferencePriority {
    Low = 0,
    Normal = 1,
    Subject = 2,
    Urgent = 3,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferencePayload {
    pub system_prompt: String,
    pub messages: Vec<InferenceMessage>,
    pub max_tokens: Option<u64>,
    pub tools: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResult {
    pub request_id: EntityId,
    pub status: InferenceStatus,
    pub response: Option<InferenceResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InferenceStatus {
    Success,
    /// Outcome/remote execution may be unknown. Never retry based on this alone.
    Failed,
    Cancelled,
    /// Broker did not dispatch this request.
    QueuedCapacityWait,
    /// Explicit provider rejection, not inferred from error text.
    Rejected {
        http_status: u16,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("provider rejected request with HTTP {http_status}")]
pub struct ProviderRejection {
    pub http_status: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub content: String,
    pub stop_reason: String,
    pub raw_response: String,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: Option<u64>,
    pub cache_write_tokens: Option<u64>,
}
