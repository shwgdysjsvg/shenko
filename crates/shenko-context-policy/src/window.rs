//! Committed windows and the commit transaction (§12.3.2).
//!
//! A committed window records its decoder (policy id/version), source refs and
//! derivatives, so it can be materialized without the plugin that produced it.
//! Where the chain lives is a [`WindowLog`]: in-process, or the durable store.
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use shenko_types::ids::EntityId;

use crate::history::{StorageError, HistorySource, ItemRef};
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
    pub fn genesis() -> Self {
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
    pub fn materialize(&self, history: &dyn HistorySource) -> Result<Vec<WindowEntry>, StorageError> {
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
    Storage(#[from] StorageError),
}

/// Durable home of one scope's window chain. The ledger validates; the log
/// stores and enforces the head compare-and-append, so two writers cannot
/// both extend the same head.
pub trait WindowLog: Send + Sync {
    /// Current head; an empty log creates and returns its genesis window.
    fn head(&self) -> Result<CommittedWindow, StorageError>;
    /// Every committed window, oldest first; none is ever discarded.
    fn all(&self) -> Result<Vec<CommittedWindow>, StorageError>;
    /// Append `window` only while `expected` is the head; Ok(false) if it moved.
    fn append(&self, expected: EntityId, window: &CommittedWindow) -> Result<bool, StorageError>;
    fn model_generation(&self) -> Result<u64, StorageError>;
    fn set_model_generation(&self, generation: u64) -> Result<(), StorageError>;
}

/// In-process log for strategies and tests without a durable store.
pub struct MemoryWindowLog {
    state: Mutex<(Vec<CommittedWindow>, u64)>,
}

impl Default for MemoryWindowLog {
    fn default() -> Self {
        Self { state: Mutex::new((vec![CommittedWindow::genesis()], 0)) }
    }
}

impl WindowLog for MemoryWindowLog {
    fn head(&self) -> Result<CommittedWindow, StorageError> {
        Ok(self.state.lock().0.last().cloned().expect("log always has genesis"))
    }

    fn all(&self) -> Result<Vec<CommittedWindow>, StorageError> {
        Ok(self.state.lock().0.clone())
    }

    fn append(&self, expected: EntityId, window: &CommittedWindow) -> Result<bool, StorageError> {
        let mut state = self.state.lock();
        if state.0.last().map(|w| w.id) != Some(expected) {
            return Ok(false);
        }
        state.0.push(window.clone());
        Ok(true)
    }

    fn model_generation(&self) -> Result<u64, StorageError> {
        Ok(self.state.lock().1)
    }

    fn set_model_generation(&self, generation: u64) -> Result<(), StorageError> {
        self.state.lock().1 = generation;
        Ok(())
    }
}

pub struct WindowLedger {
    log: Arc<dyn WindowLog>,
    /// Serializes this process's validate-then-append; the log's
    /// compare-and-append covers any other writer.
    commit: Mutex<()>,
}

impl Default for WindowLedger {
    fn default() -> Self {
        Self::new(Arc::new(MemoryWindowLog::default()))
    }
}

impl WindowLedger {
    pub fn new(log: Arc<dyn WindowLog>) -> Self {
        Self { log, commit: Mutex::new(()) }
    }

    pub fn current(&self) -> Result<CommittedWindow, StorageError> {
        self.log.head()
    }

    /// Every committed window, oldest first; none is ever discarded.
    pub fn all(&self) -> Result<Vec<CommittedWindow>, StorageError> {
        self.log.all()
    }

    pub fn model_generation(&self) -> Result<u64, StorageError> {
        self.log.model_generation()
    }

    /// Record a rebind of model weights, chat template, tokenizer or vision
    /// encoder/processor. Windows prepared for the old binding cannot commit,
    /// and no prefix built under it counts as reusable.
    pub fn set_model_generation(&self, generation: u64) -> Result<(), StorageError> {
        self.log.set_model_generation(generation)
    }

    /// Verify the base still holds and commit atomically. Events appended after
    /// the prepared leaf stay visible because materialization carries them.
    pub fn commit(
        &self,
        prepared: PreparedWindow,
        history: &dyn HistorySource,
    ) -> Result<CommittedWindow, CommitError> {
        let _serial = self.commit.lock();
        let current = self.log.head()?;
        if prepared.base.source_window != current.id {
            return Err(CommitError::StaleSource {
                prepared: prepared.base.source_window,
                current: current.id,
            });
        }
        let model_generation = self.log.model_generation()?;
        if prepared.base.model_generation != model_generation {
            return Err(CommitError::ModelGenerationChanged {
                prepared: prepared.base.model_generation,
                current: model_generation,
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
        if !self.log.append(current.id, &window)? {
            let current = self.log.head()?.id;
            return Err(CommitError::StaleSource { prepared: prepared.base.source_window, current });
        }
        Ok(window)
    }
}
