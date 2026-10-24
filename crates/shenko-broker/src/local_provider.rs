//! Local model provider — OpenAI-compatible /v1/chat/completions. SPEC §22.1
//!
//! Targets the user's Qwen3.8-27B via Tailnet vLLM endpoint. Auth is not required
//! (the Tailnet address is a private network address, not a secret).
use anyhow::{Result, anyhow};
use tracing::{info, instrument};

use crate::request::{InferencePayload, InferenceResponse, TokenUsage};

const DEFAULT_BASE_URL: &str = "https://doesvm.icefish-exponential.ts.net:8446";
const DEFAULT_MODEL: &str = "qwen3.8-27b";

#[derive(Debug, Clone)]
pub struct LocalModelProvider {
    pub base_url: String,
    pub model: String,
    pub allow_invalid_certs: bool,
}

impl LocalModelProvider {
    pub fn new(base_url: String, model: String) -> Self {
        Self {
            base_url,
            model,
            allow_invalid_certs: false,
        }
    }

    /// Build from environment. SHENKO_LOCAL_MODEL_URL overrides the Tailnet default.
    pub fn from_env() -> Self {
        let base_url = std::env::var("SHENKO_LOCAL_MODEL_URL")
            .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string());
        Self {
            base_url,
            model: DEFAULT_MODEL.to_string(),
            allow_invalid_certs: std::env::var("SHENKO_LOCAL_INSECURE").as_deref() == Ok("1"),
        }
    }

    #[instrument(skip(self, payload))]
    pub async fn complete(&self, payload: &InferencePayload) -> Result<InferenceResponse> {
        let mut builder = reqwest::Client::builder().timeout(std::time::Duration::from_secs(180));

        if self.allow_invalid_certs {
            builder = builder.danger_accept_invalid_certs(true);
        }

        let client = builder.build()?;

        // Convert InferencePayload to OpenAI chat messages.
        let mut messages: Vec<serde_json::Value> = Vec::new();

        if !payload.system_prompt.is_empty() {
            messages.push(serde_json::json!({
                "role": "system",
                "content": payload.system_prompt,
            }));
        }

        for m in &payload.messages {
            messages.push(serde_json::json!({
                "role": m.role,
                "content": m.content,
            }));
        }

        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": payload.max_tokens.unwrap_or(crate::request::DEFAULT_MAX_TOKENS),
            "messages": messages,
        });

        let resp = client
            .post(format!("{}/v1/chat/completions", self.base_url))
            // Tailnet endpoint requires no real key; an empty string is accepted.
            .header("x-api-key", "")
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
            return Err(anyhow!("local model API error {status}: {text}"));
        }

        let raw_response = resp.text().await?;
        let raw: serde_json::Value = serde_json::from_str(&raw_response)?;

        let content = raw["choices"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|c| c["message"]["content"].as_str())
            .unwrap_or("")
            .to_string();

        let finish_reason = raw["choices"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|c| c["finish_reason"].as_str())
            .unwrap_or("unknown")
            .to_string();

        // OpenAI usage shape: prompt_tokens / completion_tokens.
        let usage = raw["usage"].as_object().map(|u| TokenUsage {
            input_tokens: u.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0),
            output_tokens: u
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .unwrap_or(0),
            cache_read_tokens: None,
            cache_write_tokens: None,
        });

        info!(
            model = %self.model,
            finish_reason = %finish_reason,
            in_tokens = ?usage.as_ref().map(|u| u.input_tokens),
            out_tokens = ?usage.as_ref().map(|u| u.output_tokens),
            "local inference complete"
        );

        Ok(InferenceResponse {
            content,
            stop_reason: finish_reason,
            raw_response,
            usage,
        })
    }
}
