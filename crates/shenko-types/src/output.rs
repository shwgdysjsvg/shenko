//! One parsed model result shared by every consumer. Raw bytes remain in CAS.
use crate::{identity::IdentityChange, ids::EntityId, object::ObjectRef};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputSpan {
    pub start: usize,
    pub end: usize,
    pub line: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum PublicationTarget {
    QqGroup(u64),
    QqPrivate(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputAction {
    Reply {
        target: PublicationTarget,
        text: String,
    },
    Remember {
        text: String,
    },
    Delegate {
        text: String,
    },
    Identity {
        change: IdentityChange,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputControl {
    pub action_id: EntityId,
    pub spans: Vec<OutputSpan>,
    pub action: OutputAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParsedOutput {
    pub schema_version: u32,
    pub step_id: EntityId,
    pub raw_ref: ObjectRef,
    /// Natural-language text, with active control syntax removed. No rewriting.
    pub text: String,
    pub controls: Vec<OutputControl>,
}
