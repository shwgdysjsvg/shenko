//! HistorySource over committed `session_events` (feature `store`).
//!
//! Mapping convention until the session event writer (#6) fixes a payload:
//! `event_type` is one of system / user / assistant / assistant_cancelled /
//! tool_call / tool_result; `data` is JSON `{text, call_id?, tool?, args_ref?,
//! status?}` with `status` in [`CallStatus`]'s serde form. A call or result
//! without `status` is Unknown, never success; one without `call_id` cannot be
//! paired and is kept as [`ItemKind::Other`]; any other event type is kept as
//! Other too. Non-JSON `data` becomes the text. Nothing is dropped.
//! Authoritative call status belongs to the execution ledger (operations /
//! attempts); joining it is a later step.
use std::sync::Arc;

use serde::Deserialize;
use shenko_store::{Store, StoredSessionEvent};
use shenko_types::ids::EntityId;

use crate::history::{CallStatus, HistoryError, HistoryItem, HistorySource, ItemKind, ItemRef};

#[derive(Default, Deserialize)]
struct Payload {
    #[serde(default)]
    text: String,
    call_id: Option<String>,
    tool: Option<String>,
    args_ref: Option<String>,
    status: Option<CallStatus>,
}

/// Tokenizer of the currently bound model.
pub type TokenCounter = Arc<dyn Fn(&str) -> u32 + Send + Sync>;

pub struct StoreHistory {
    store: Arc<Store>,
    session_id: EntityId,
    count: TokenCounter,
}

impl StoreHistory {
    pub fn new(store: Arc<Store>, session_id: EntityId, count: TokenCounter) -> Self {
        Self { store, session_id, count }
    }

    fn item(&self, event: StoredSessionEvent) -> Result<HistoryItem, HistoryError> {
        let seq = u64::try_from(event.seq)
            .map_err(|_| HistoryError(format!("event {} has negative seq", event.event_id)))?;
        let payload = serde_json::from_str::<Payload>(&event.data)
            .unwrap_or_else(|_| Payload { text: event.data.clone(), ..Payload::default() });
        let status = payload.status.unwrap_or(CallStatus::Unknown);
        let kind = match (event.event_type.as_str(), payload.call_id) {
            ("system", _) => ItemKind::System,
            ("user", _) => ItemKind::User,
            ("assistant", _) => ItemKind::Assistant { cancelled: false },
            ("assistant_cancelled", _) => ItemKind::Assistant { cancelled: true },
            ("tool_call", Some(call_id)) => ItemKind::ToolCall {
                call_id,
                tool: payload.tool.unwrap_or_default(),
                args_ref: payload.args_ref.unwrap_or_default(),
                status,
            },
            ("tool_result", Some(call_id)) => ItemKind::ToolResult { call_id, status },
            (other, _) => ItemKind::Other { event_type: other.to_owned() },
        };
        let tokens = (self.count)(&payload.text);
        Ok(HistoryItem {
            at: ItemRef { seq, event_id: event.event_id },
            kind,
            text: payload.text,
            tokens,
        })
    }
}

fn storage(error: impl std::fmt::Display) -> HistoryError {
    HistoryError(error.to_string())
}

impl HistorySource for StoreHistory {
    fn read_range(&self, from: u64, to: u64) -> Result<Vec<HistoryItem>, HistoryError> {
        let to = i64::try_from(to).unwrap_or(i64::MAX);
        let from = i64::try_from(from).map_err(storage)?;
        self.store
            .session_events_range(self.session_id, from, to)
            .map_err(storage)?
            .into_iter()
            .map(|event| self.item(event))
            .collect()
    }

    fn read_event(&self, event_id: EntityId) -> Result<Option<HistoryItem>, HistoryError> {
        self.store
            .session_event(event_id)
            .map_err(storage)?
            .filter(|event| event.session_id == self.session_id)
            .map(|event| self.item(event))
            .transpose()
    }

    fn leaf_seq(&self) -> Result<Option<u64>, HistoryError> {
        self.store
            .session_leaf_seq(self.session_id)
            .map_err(storage)?
            .map(|seq| u64::try_from(seq).map_err(storage))
            .transpose()
    }
}
