//! Durable history and window chain over the session store (feature `store`).
//!
//! `StoreHistory` reads committed `session_events`; `StoreWindowLog` keeps the
//! committed window chain in `context_windows`.
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
use shenko_store::{Store, StoredContextWindow, StoredDerivative, StoredSessionEvent};
use shenko_types::ids::EntityId;

use crate::history::{CallStatus, HistoryItem, HistorySource, ItemKind, ItemRef, StorageError};
use crate::policy::Derivative;
use crate::window::{CommittedWindow, WindowLog};

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

    fn item(&self, event: StoredSessionEvent) -> Result<HistoryItem, StorageError> {
        let seq = u64::try_from(event.seq)
            .map_err(|_| StorageError(format!("event {} has negative seq", event.event_id)))?;
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

fn storage(error: impl std::fmt::Display) -> StorageError {
    StorageError(error.to_string())
}

impl HistorySource for StoreHistory {
    fn read_range(&self, from: u64, to: u64) -> Result<Vec<HistoryItem>, StorageError> {
        let to = i64::try_from(to).unwrap_or(i64::MAX);
        let from = i64::try_from(from).map_err(storage)?;
        self.store
            .session_events_range(self.session_id, from, to)
            .map_err(storage)?
            .into_iter()
            .map(|event| self.item(event))
            .collect()
    }

    fn read_event(&self, event_id: EntityId) -> Result<Option<HistoryItem>, StorageError> {
        self.store
            .session_event(event_id)
            .map_err(storage)?
            .filter(|event| event.session_id == self.session_id)
            .map(|event| self.item(event))
            .transpose()
    }

    fn leaf_seq(&self) -> Result<Option<u64>, StorageError> {
        self.store
            .session_leaf_seq(self.session_id)
            .map_err(storage)?
            .map(|seq| u64::try_from(seq).map_err(storage))
            .transpose()
    }
}

/// WindowLog over the store's `context_windows` chain for one scope. The
/// body is the committed window as JSON without derivative bytes; frames are
/// stored beside it in the same transaction and verified on read.
pub struct StoreWindowLog {
    store: Arc<Store>,
    scope_id: EntityId,
}

impl StoreWindowLog {
    pub fn new(store: Arc<Store>, scope_id: EntityId) -> Self {
        Self { store, scope_id }
    }

    fn encode(window: &CommittedWindow) -> Result<StoredContextWindow, StorageError> {
        let body = CommittedWindow { derivatives: Vec::new(), ..window.clone() };
        Ok(StoredContextWindow {
            window_id: window.id,
            generation: i64::try_from(window.generation).map_err(storage)?,
            body: serde_json::to_string(&body).map_err(storage)?,
            derivatives: window
                .derivatives
                .iter()
                .map(|d| StoredDerivative {
                    media_type: d.media_type.clone(),
                    blake3: d.blake3.clone(),
                    bytes: d.bytes.clone(),
                })
                .collect(),
        })
    }

    fn decode(stored: StoredContextWindow) -> Result<CommittedWindow, StorageError> {
        let mut window: CommittedWindow = serde_json::from_str(&stored.body).map_err(storage)?;
        if window.id != stored.window_id {
            return Err(storage(format!("window row {} holds body of {}", stored.window_id, window.id)));
        }
        window.derivatives = stored
            .derivatives
            .into_iter()
            .map(|d| Derivative { media_type: d.media_type, blake3: d.blake3, bytes: d.bytes })
            .collect();
        Ok(window)
    }
}

impl WindowLog for StoreWindowLog {
    fn head(&self) -> Result<CommittedWindow, StorageError> {
        if let Some(head) = self.all()?.pop() {
            return Ok(head);
        }
        // Lazily persist genesis. Losing the race to another writer is fine:
        // whoever landed first is the genesis everyone then reads.
        let genesis = CommittedWindow::genesis();
        self.store
            .append_context_window(self.scope_id, None, &Self::encode(&genesis)?)
            .map_err(storage)?;
        self.all()?.pop().ok_or_else(|| storage("genesis window missing after append"))
    }

    fn all(&self) -> Result<Vec<CommittedWindow>, StorageError> {
        self.store
            .context_windows(self.scope_id)
            .map_err(storage)?
            .into_iter()
            .map(Self::decode)
            .collect()
    }

    fn append(&self, expected: EntityId, window: &CommittedWindow) -> Result<bool, StorageError> {
        self.store
            .append_context_window(self.scope_id, Some(expected), &Self::encode(window)?)
            .map_err(storage)
    }

    fn model_generation(&self) -> Result<u64, StorageError> {
        let generation = self.store.context_model_generation(self.scope_id).map_err(storage)?;
        u64::try_from(generation).map_err(storage)
    }

    fn set_model_generation(&self, generation: u64) -> Result<(), StorageError> {
        let generation = i64::try_from(generation).map_err(storage)?;
        self.store.set_context_model_generation(self.scope_id, generation).map_err(storage)
    }
}
