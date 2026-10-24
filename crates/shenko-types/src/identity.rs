//! Versioned instance identity — SPEC §6, R053/R054/R104/R105/R117.
//!
//! The framework is Shenko; an instance has its own display name, soul and
//! personality. All three are revisions of one profile bound to the stable
//! `agent_id`: renaming never creates another subject or detaches history.
//! The profile only feeds the persona section that follows the fixed role/tool
//! contract; it carries no tool schema, permission or resource field (R054).
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::EntityId;

/// Who chose the current display name. SPEC §6: user naming and self naming
/// are both first-class initialisation paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamingOrigin {
    /// No name yet; the subject is invited to choose one.
    Unnamed,
    UserGiven,
    SelfChosen,
}

/// Provenance of one profile revision, so every persona change is traceable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IdentitySource {
    /// Created at genesis or first adoption by an older installation.
    Genesis,
    /// The human operator (CLI flag / `shenko identity`).
    Operator,
    /// The subject itself, from a committed step's model output.
    Subject { step_id: EntityId, step_no: u64 },
}

/// One field change requested against the current revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "field", content = "value", rename_all = "snake_case")]
pub enum IdentityChange {
    Name(String),
    Soul(String),
    Personality(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityProfile {
    pub agent_id: EntityId,
    /// Starts at 1 and increases by exactly one per change.
    pub revision: u64,
    pub display_name: Option<String>,
    pub naming: NamingOrigin,
    /// One or a few sentences of the instance's own philosophy (R105).
    pub soul: Option<String>,
    pub personality: Option<String>,
    pub source: IdentitySource,
    pub created_at: DateTime<Utc>,
}

/// One step proposes one atomic profile revision, based only on its S0 snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityIntent {
    pub intent_id: EntityId,
    pub action_ids: Vec<EntityId>,
    pub expected_revision: u64,
    pub provider_revision: String,
    pub proposed: IdentityProfile,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum IdentityOutcome {
    Unchanged {
        revision: Option<u64>,
    },
    Applied {
        expected_revision: u64,
        revision: u64,
    },
    Conflict {
        expected_revision: u64,
        actual_revision: Option<u64>,
    },
}

impl IdentityOutcome {
    pub fn revision(&self) -> Option<u64> {
        match self {
            Self::Unchanged { revision } => *revision,
            Self::Applied { revision, .. } => Some(*revision),
            Self::Conflict {
                actual_revision, ..
            } => *actual_revision,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityReceipt {
    pub step_id: EntityId,
    pub intent_id: Option<EntityId>,
    pub outcome: IdentityOutcome,
    pub committed_at: crate::commit::CommitRef,
}
