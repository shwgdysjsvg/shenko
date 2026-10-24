//! Stable history references and call units.
//!
//! The raw session ledger (#6) and history child (#14) own the real storage;
//! policies only see this read-only view, addressed by sequence and event id.
use std::collections::{HashMap, HashSet};
use std::ops::Range;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use shenko_types::ids::EntityId;

/// Stable address of one committed raw event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ItemRef {
    pub seq: u64,
    pub event_id: EntityId,
}

/// Execution-ledger status of a tool call. Only the ledger may change it; a
/// window must never present Pending/Unknown/Cancelled as success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallStatus {
    Pending,
    Executed { ok: bool },
    Cancelled,
    Unknown,
}

impl CallStatus {
    pub fn is_resolved(self) -> bool {
        matches!(self, Self::Executed { .. } | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemKind {
    System,
    User,
    /// A cancelled attempt stays marked so it is never read as an executed action.
    Assistant { cancelled: bool },
    /// `args_ref` addresses the complete arguments in the execution ledger;
    /// `text` may be a bounded rendering of them.
    ToolCall { call_id: String, tool: String, args_ref: String, status: CallStatus },
    ToolResult { call_id: String, status: CallStatus },
    /// An event type this contract version does not interpret. Kept, never
    /// dropped, so a window cannot silently lose part of the raw record.
    Other { event_type: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryItem {
    pub at: ItemRef,
    pub kind: ItemKind,
    pub text: String,
    /// Tokenizer count for the current model, supplied by the caller.
    pub tokens: u32,
}

/// A failed read. It must fail the window operation: treating it as "no
/// history" would cut a window from a silently truncated record.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[error("history read failed: {0}")]
pub struct HistoryError(pub String);

/// Read access to raw history, independent of any policy plugin being alive.
pub trait HistorySource: Send + Sync {
    /// Items with `from <= seq <= to`, in order.
    fn read_range(&self, from: u64, to: u64) -> Result<Vec<HistoryItem>, HistoryError>;
    fn read_event(&self, event_id: EntityId) -> Result<Option<HistoryItem>, HistoryError>;
    fn leaf_seq(&self) -> Result<Option<u64>, HistoryError>;
}

/// In-process append-only history used until the durable ledger adapter lands.
#[derive(Default)]
pub struct MemoryHistory {
    items: RwLock<Vec<HistoryItem>>,
}

impl MemoryHistory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one raw event with the next sequence number.
    pub fn append(&self, kind: ItemKind, text: impl Into<String>, tokens: u32) -> ItemRef {
        let mut items = self.items.write();
        let at = ItemRef { seq: items.len() as u64, event_id: EntityId::new() };
        items.push(HistoryItem { at, kind, text: text.into(), tokens });
        at
    }

    pub fn snapshot(&self) -> Vec<HistoryItem> {
        self.items.read().clone()
    }
}

impl HistorySource for MemoryHistory {
    fn read_range(&self, from: u64, to: u64) -> Result<Vec<HistoryItem>, HistoryError> {
        let items = self.items.read();
        let end = (to.saturating_add(1)).min(items.len() as u64) as usize;
        let start = (from as usize).min(end);
        Ok(items[start..end].to_vec())
    }

    fn read_event(&self, event_id: EntityId) -> Result<Option<HistoryItem>, HistoryError> {
        Ok(self.items.read().iter().find(|i| i.at.event_id == event_id).cloned())
    }

    fn leaf_seq(&self) -> Result<Option<u64>, HistoryError> {
        Ok(self.items.read().last().map(|i| i.at.seq))
    }
}

/// Split items into indivisible units: a tool call, its result and everything
/// interleaved between them form one unit, so no window cuts a call in half.
pub fn units(items: &[HistoryItem]) -> Vec<Range<usize>> {
    let mut result_at = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        if let ItemKind::ToolResult { call_id, .. } = &item.kind {
            result_at.insert(call_id.as_str(), i);
        }
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let mut end = i;
        let mut j = i;
        while j <= end {
            if let ItemKind::ToolCall { call_id, .. } = &items[j].kind {
                if let Some(&r) = result_at.get(call_id.as_str()) {
                    end = end.max(r);
                }
            }
            j += 1;
        }
        out.push(i..end + 1);
        i = end + 1;
    }
    out
}

/// Calls whose outcome is not yet resolved by the execution ledger.
pub fn unresolved_calls(items: &[HistoryItem]) -> HashSet<String> {
    let mut resolved = HashSet::new();
    for item in items {
        match &item.kind {
            ItemKind::ToolResult { call_id, status } | ItemKind::ToolCall { call_id, status, .. }
                if status.is_resolved() =>
            {
                resolved.insert(call_id.clone());
            }
            _ => {}
        }
    }
    items
        .iter()
        .filter_map(|item| match &item.kind {
            ItemKind::ToolCall { call_id, .. } if !resolved.contains(call_id) => Some(call_id.clone()),
            _ => None,
        })
        .collect()
}

/// Whether a unit holds any unresolved call and must therefore stay verbatim.
pub fn unit_is_unfinished(items: &[HistoryItem], unresolved: &HashSet<String>) -> bool {
    items.iter().any(|item| match &item.kind {
        ItemKind::ToolCall { call_id, .. } | ItemKind::ToolResult { call_id, .. } => {
            unresolved.contains(call_id)
        }
        _ => false,
    })
}
