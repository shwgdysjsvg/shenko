//! Committed windows and the commit transaction (§12.3.2).
//!
//! In-process ledger until the durable store adapter (#6) exists. A committed
//! window records its decoder (policy id/version), source refs and derivatives,
//! so it can be materialized without the plugin that produced it.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use shenko_types::ids::EntityId;

use crate::history::{HistoryError, HistorySource, ItemRef};
use crate::policy::{CostReport, Derivative, PreparedWindow, WindowBase, WindowEntry};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedWindow {
    pub id: EntityId,
    pub generation: u64,
    /// None for the genesis window, which shows the full raw history.
    pub base: Option<WindowBase>,
    pub policy_id: Option<String>,
    pub policy_version: Option<String>,
    pub entries: Vec<WindowEntry>,
    pub derivatives: Vec<Derivative>,
    /// Raw events after this seq are appended verbatim when materialized.
    pub covers_through: Option<u64>,
    /// Model/template/encoder binding the window's input was built for.
    pub model_generation: u64,
    pub cost: CostReport,
}

/// What actually reaches the model for one entry. Equal identities produce
/// equal input; anything else (including an index into a different
/// derivative list) is a different input.
#[derive(PartialEq)]
enum InputIdentity<'a> {
    Raw(ItemRef, u32),
    Pointer(u64, u64, &'a str),
    Text(u64, u64, &'a str),
    Image(u64, u64, &'a str, &'a str),
}

fn identities<'a>(
    entries: &'a [WindowEntry],
    derivatives: &'a [Derivative],
) -> Vec<Option<InputIdentity<'a>>> {
    entries
        .iter()
        .map(|entry| match entry {
            WindowEntry::Verbatim { at, tokens } => Some(InputIdentity::Raw(*at, *tokens)),
            WindowEntry::Pointer { from, to, note, .. } => {
                Some(InputIdentity::Pointer(*from, *to, note))
            }
            WindowEntry::Text { from, to, text, .. } => Some(InputIdentity::Text(*from, *to, text)),
            WindowEntry::Image { from, to, derivative, .. } => derivatives
                .get(*derivative)
                .map(|d| InputIdentity::Image(*from, *to, &d.media_type, &d.blake3)),
        })
        .collect()
}

impl CommittedWindow {
    fn genesis() -> Self {
        Self {
            id: EntityId::new(),
            generation: 0,
            base: None,
            policy_id: None,
            policy_version: None,
            entries: Vec::new(),
            derivatives: Vec::new(),
            covers_through: None,
            model_generation: 0,
            cost: CostReport::default(),
        }
    }

    /// The model-visible sequence now: stored entries plus every raw event
    /// that arrived after the window was cut.
    pub fn materialize(&self, history: &dyn HistorySource) -> Result<Vec<WindowEntry>, HistoryError> {
        let mut out = self.entries.clone();
        let from = self.covers_through.map_or(0, |seq| seq + 1);
        if let Some(leaf) = history.leaf_seq()? {
            if from <= leaf {
                out.extend(
                    history
                        .read_range(from, leaf)?
                        .into_iter()
                        .map(|item| WindowEntry::Verbatim { at: item.at, tokens: item.tokens }),
                );
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum CommitError {
    #[error("window was prepared against {prepared}, but {current} is current")]
    StaleSource { prepared: EntityId, current: EntityId },
    #[error("model generation moved from {prepared} to {current}")]
    ModelGenerationChanged { prepared: u64, current: u64 },
    #[error("policy {policy} epoch {epoch} is no longer installed")]
    StalePolicy { policy: String, epoch: u64 },
    #[error("prepared leaf {prepared} is ahead of the history leaf {leaf:?}")]
    LeafAhead { prepared: u64, leaf: Option<u64> },
    #[error(transparent)]
    History(#[from] HistoryError),
}

struct LedgerState {
    windows: Vec<CommittedWindow>,
    model_generation: u64,
}

pub struct WindowLedger {
    state: Mutex<LedgerState>,
}

impl Default for WindowLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowLedger {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LedgerState {
                windows: vec![CommittedWindow::genesis()],
                model_generation: 0,
            }),
        }
    }

    pub fn current(&self) -> CommittedWindow {
        self.state.lock().windows.last().cloned().expect("ledger always has genesis")
    }

    /// Every committed window, oldest first; none is ever discarded.
    pub fn all(&self) -> Vec<CommittedWindow> {
        self.state.lock().windows.clone()
    }

    pub fn model_generation(&self) -> u64 {
        self.state.lock().model_generation
    }

    /// Record a rebind of model weights, chat template, tokenizer or vision
    /// encoder/processor. Windows prepared for the old binding cannot commit,
    /// and no prefix built under it counts as reusable.
    pub fn set_model_generation(&self, generation: u64) {
        self.state.lock().model_generation = generation;
    }

    /// Verify the base still holds and commit atomically. Events appended after
    /// the prepared leaf stay visible because materialization carries them.
    pub fn commit(
        &self,
        prepared: PreparedWindow,
        history: &dyn HistorySource,
    ) -> Result<CommittedWindow, CommitError> {
        let mut state = self.state.lock();
        let current = state.windows.last().expect("ledger always has genesis");
        if prepared.base.source_window != current.id {
            return Err(CommitError::StaleSource {
                prepared: prepared.base.source_window,
                current: current.id,
            });
        }
        if prepared.base.model_generation != state.model_generation {
            return Err(CommitError::ModelGenerationChanged {
                prepared: prepared.base.model_generation,
                current: state.model_generation,
            });
        }
        let leaf = history.leaf_seq()?;
        if leaf.is_none_or(|leaf| prepared.base.leaf_seq > leaf) {
            return Err(CommitError::LeafAhead { prepared: prepared.base.leaf_seq, leaf });
        }

        let previous = current.materialize(history)?;
        let mut window = CommittedWindow {
            id: EntityId::new(),
            generation: current.generation + 1,
            base: Some(prepared.base),
            policy_id: Some(prepared.policy.id),
            policy_version: Some(prepared.policy.version),
            entries: prepared.entries,
            derivatives: prepared.derivatives,
            covers_through: Some(prepared.base.leaf_seq),
            model_generation: prepared.base.model_generation,
            cost: prepared.cost,
        };
        let next = window.materialize(history)?;
        // Reusable only when the binding is unchanged and the final input of
        // each leading entry is identical, derivative bytes included.
        let reused: u32 = if current.model_generation == window.model_generation {
            identities(&previous, &current.derivatives)
                .into_iter()
                .zip(identities(&next, &window.derivatives))
                .zip(&next)
                .take_while(|((a, b), _)| a.is_some() && a == b)
                .map(|(_, entry)| entry.tokens())
                .sum()
        } else {
            0
        };
        let total: u32 = next.iter().map(WindowEntry::tokens).sum();
        window.cost.reusable_prefix_tokens = reused;
        window.cost.recompute_tokens = total - reused;
        state.windows.push(window.clone());
        Ok(window)
    }
}
