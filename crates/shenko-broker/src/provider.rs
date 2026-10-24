//! Anthropic Claude API provider — SPEC §47 PV01/PV02
//! Wire contract: Anthropic Messages API 2026-10-21 (no temperature/top_p/top_k)
use anyhow::{Result, anyhow};
use tracing::{info, instrument};

use crate::request::{InferencePayload, InferenceResponse, TokenUsage};

pub(crate) const ANTHROPIC_API_VERSION: &str = "2026-10-21";
const DEFAULT_MODEL: &str = "claude-opus-5-5";

#[derive(Debug, Clone)]
pub struct AnthropicProvider {
    pub api_key: String,
    pub base_url: String,
    pub model: String,
}

impl AnthropicProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            base_url: "https://api.anthropic.com".to_string(),
            model: DEFAULT_MODEL.to_string(),
        }
    }

    pub fn from_env() -> Option<Self> {
        std::env::var("ANTHROPIC_API_KEY").ok().map(Self::new)
    }

    pub fn with_model(mut self, model: impl Into<String>) -> Self {
        self.model = model.into();
        self
    }

    #[instrument(skip(self, payload))]
    pub async fn complete(&self, payload: &InferencePayload) -> Result<InferenceResponse> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;

        let messages: Vec<serde_json::Value> = payload
            .messages
            .iter()
            .map(|m| serde_json::json!({ "role": m.role, "content": m.content }))
            .collect();

        let mut body = serde_json::json!({
            "model": self.model,
            "max_tokens": payload.max_tokens.unwrap_or(crate::request::DEFAULT_MAX_TOKENS),
            "messages": messages,
        });

        if !payload.system_prompt.is_empty() {
            body["system"] = serde_json::Value::String(payload.system_prompt.clone());
        }
        if !payload.tools.is_empty() {
            body["tools"] = serde_json::Value::Array(payload.tools.clone());
        }
        let resp = client
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", ANTHROPIC_API_VERSION)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(crate::request::ProviderRejection {
                    http_status: status.as_u16(),
                }
                .into());
            }
            let text = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Anthropic API error {status}: {text}"));
        }

        let raw_response = resp.text().await?;
        let raw: serde_json::Value = serde_json::from_str(&raw_response)?;

        let content = raw["content"]
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|block| block["type"] == "text")
                    .filter_map(|block| block["text"].as_str())
                    .collect::<String>()
            })
            .unwrap_or_default();

        let stop_reason = raw["stop_reason"].as_str().unwrap_or("unknown").to_string();

        let usage = raw["usage"].as_object().map(|u| TokenUsage {
            input_tokens: u.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            output_tokens: u.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            cache_read_tokens: u.get("cache_read_input_tokens").and_then(|v| v.as_u64()),
            cache_write_tokens: u
                .get("cache_creation_input_tokens")
                .and_then(|v| v.as_u64()),
        });

        info!(model=%self.model, stop_reason=%stop_reason, in_tokens=?usage.as_ref().map(|u|u.input_tokens), out_tokens=?usage.as_ref().map(|u|u.output_tokens), "inference complete");
        Ok(InferenceResponse {
            content,
            stop_reason,
            raw_response,
            usage,
        })
    }
}
