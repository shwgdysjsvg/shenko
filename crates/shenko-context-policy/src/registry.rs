//! Installed strategies and window commits, published as one Cordis Service.
//!
//! Each strategy plugin installs itself here from its own `apply` and removes
//! itself in cleanup, so install/update/uninstall/replace are ordinary Cordis
//! lifecycle operations on that plugin alone. The registry holds no strategy.
use std::borrow::Cow;
use std::sync::Arc;

use cordis_core::{Context, Plugin, Service};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::history::{HistoryError, HistoryItem, HistorySource};
use crate::policy::{
    Availability, ContextInput, ContextPolicy, ModelCaps, PolicyError, PolicyManifest,
    PreparedWindow, WindowBase, WindowEntry,
};
use crate::window::{CommitError, CommittedWindow, WindowLedger};

struct Installed {
    policy: Arc<dyn ContextPolicy>,
    manifest: PolicyManifest,
    epoch: u64,
}

#[derive(Default)]
struct RegistryState {
    installed: Vec<Installed>,
    preference: Vec<String>,
    next_epoch: u64,
}

/// One strategy's outcome while selecting a window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub policy: String,
    pub outcome: Result<(), PolicyError>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum PrepareError {
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error("no installed context policy produced a usable window: {attempts:?}")]
    NoUsablePolicy { attempts: Vec<Attempt> },
}

impl PrepareError {
    /// Strategy outcomes, empty when history could not be read at all.
    pub fn attempts(&self) -> &[Attempt] {
        match self {
            Self::History(_) => &[],
            Self::NoUsablePolicy { attempts } => attempts,
        }
    }
}

#[derive(Default)]
pub struct PolicyRegistry {
    state: Mutex<RegistryState>,
    ledger: WindowLedger,
}

impl Service for PolicyRegistry {
    const NAME: &'static str = "shenko/context-policy";
}

impl PolicyRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn ledger(&self) -> &WindowLedger {
        &self.ledger
    }

    /// Install or replace the strategy with this manifest id. Returns the new
    /// epoch; windows prepared by any earlier epoch can no longer commit.
    pub fn install(&self, policy: Arc<dyn ContextPolicy>) -> u64 {
        let manifest = policy.manifest();
        let mut state = self.state.lock();
        state.next_epoch += 1;
        let epoch = state.next_epoch;
        state.installed.retain(|i| i.manifest.id != manifest.id);
        state.installed.push(Installed { policy, manifest, epoch });
        epoch
    }

    /// Remove only that exact installation, so a late cleanup of a replaced
    /// generation cannot uninstall its successor.
    pub fn uninstall(&self, id: &str, epoch: u64) -> bool {
        let mut state = self.state.lock();
        let before = state.installed.len();
        state.installed.retain(|i| !(i.manifest.id == id && i.epoch == epoch));
        state.installed.len() != before
    }

    pub fn installed(&self) -> Vec<(PolicyManifest, u64)> {
        let state = self.state.lock();
        state.installed.iter().map(|i| (i.manifest.clone(), i.epoch)).collect()
    }

    pub fn availability(&self, caps: &ModelCaps) -> Vec<(String, Availability)> {
        self.candidates()
            .into_iter()
            .map(|(policy, manifest, _)| (manifest.id, policy.availability(caps)))
            .collect()
    }

    /// Strategy order chosen by the subject or configuration; ids not listed
    /// follow in installation order.
    pub fn set_preference(&self, ids: Vec<String>) {
        self.state.lock().preference = ids;
    }

    fn candidates(&self) -> Vec<(Arc<dyn ContextPolicy>, PolicyManifest, u64)> {
        let state = self.state.lock();
        let rank = |id: &str| {
            state.preference.iter().position(|p| p == id).unwrap_or(usize::MAX)
        };
        let mut out: Vec<_> = state
            .installed
            .iter()
            .map(|i| (i.policy.clone(), i.manifest.clone(), i.epoch))
            .collect();
        out.sort_by_key(|(_, m, _)| rank(&m.id));
        out
    }

    fn base(&self, items: &[HistoryItem], epoch: u64) -> WindowBase {
        WindowBase {
            source_window: self.ledger.current().id,
            leaf_seq: items.last().map_or(0, |i| i.at.seq),
            model_generation: self.ledger.model_generation(),
            policy_epoch: epoch,
        }
    }

    /// Whether the first usable strategy wants a new window now.
    pub fn should_compact(
        &self,
        history: &dyn HistorySource,
        caps: &ModelCaps,
        used_tokens: u32,
    ) -> Result<bool, HistoryError> {
        let items = raw_items(history)?;
        Ok(self.candidates()
            .into_iter()
            .find(|(p, _, _)| !matches!(p.availability(caps), Availability::Unavailable(_)))
            .is_some_and(|(policy, _, epoch)| {
                let input =
                    ContextInput { base: self.base(&items, epoch), items: &items, caps, used_tokens };
                policy.should_compact(&input)
            }))
    }

    /// Prepare with the preferred usable strategy, falling back on unavailable,
    /// non-shrinking, over-limit or unreadable results. No registry lock is
    /// held while strategy code runs.
    ///
    /// Gain is judged against the window the model sees now, not the raw
    /// history: a window must be at least [`MIN_GAIN_RATIO`] smaller than the
    /// current materialized window. Re-cutting the same raw history into the
    /// same (or a marginally smaller) window costs a full prefill for no
    /// benefit, so it is refused instead of repeated whenever the trigger
    /// still fires.
    pub fn prepare(
        &self,
        history: &dyn HistorySource,
        caps: &ModelCaps,
        used_tokens: u32,
    ) -> Result<(PreparedWindow, Vec<Attempt>), PrepareError> {
        let items = raw_items(history)?;
        let items = &items[..];
        let current: u32 =
            self.ledger.current().materialize(history)?.iter().map(WindowEntry::tokens).sum();
        let ceiling = current - (current as f32 * MIN_GAIN_RATIO) as u32;
        let mut attempts = Vec::new();
        for (policy, manifest, epoch) in self.candidates() {
            if let Availability::Unavailable(reason) = policy.availability(caps) {
                attempts.push(Attempt {
                    policy: manifest.id,
                    outcome: Err(PolicyError::Unavailable(reason)),
                });
                continue;
            }
            let input = ContextInput { base: self.base(items, epoch), items, caps, used_tokens };
            let result = policy.prepare(&input).and_then(|window| {
                let limit = if window.derivatives.is_empty() {
                    caps.max_context
                } else {
                    caps.image_request_context().unwrap_or(0)
                };
                if window.cost.window_tokens > limit {
                    Err(PolicyError::OverBudget { window: window.cost.window_tokens, limit })
                } else if window.cost.window_tokens > ceiling {
                    Err(PolicyError::NoReduction)
                } else {
                    Ok(window)
                }
            });
            match result {
                Ok(window) => {
                    attempts.push(Attempt { policy: manifest.id, outcome: Ok(()) });
                    return Ok((window, attempts));
                }
                Err(error) => attempts.push(Attempt { policy: manifest.id, outcome: Err(error) }),
            }
        }
        Err(PrepareError::NoUsablePolicy { attempts })
    }

    /// Commit only if the preparing installation is still current, then let
    /// the ledger verify source window, model generation and leaf.
    pub fn commit(
        &self,
        prepared: PreparedWindow,
        history: &dyn HistorySource,
    ) -> Result<CommittedWindow, CommitError> {
        // Held across the ledger commit so uninstall/replace cannot interleave.
        let state = self.state.lock();
        let live = state
            .installed
            .iter()
            .any(|i| i.manifest.id == prepared.policy.id && i.epoch == prepared.base.policy_epoch);
        if !live {
            return Err(CommitError::StalePolicy {
                policy: prepared.policy.id,
                epoch: prepared.base.policy_epoch,
            });
        }
        self.ledger.commit(prepared, history)
    }
}

/// Smallest share of the current window a new window must save.
pub const MIN_GAIN_RATIO: f32 = 0.05;

fn raw_items(history: &dyn HistorySource) -> Result<Vec<HistoryItem>, HistoryError> {
    match history.leaf_seq()? {
        Some(leaf) => history.read_range(0, leaf),
        None => Ok(Vec::new()),
    }
}

/// Install `policy` into the registry for the lifetime of this Fiber
/// generation. Restart/update gives a new epoch; dispose uninstalls it.
pub fn install_on(ctx: &Context, policy: Arc<dyn ContextPolicy>) -> Result<u64, std::io::Error> {
    let registry = ctx.try_service::<PolicyRegistry>().map_err(std::io::Error::other)?;
    let id = policy.manifest().id;
    let epoch = registry.install(policy);
    ctx.effect_sync(move || {
        registry.uninstall(&id, epoch);
    })
    .map_err(std::io::Error::other)?;
    Ok(epoch)
}

/// Publishes a caller-owned registry. The registry (and its window ledger)
/// outlives this plugin's restarts; strategies require it by name.
pub struct ContextPolicyHost;

impl Plugin for ContextPolicyHost {
    type Config = Arc<PolicyRegistry>;
    type Input = Arc<PolicyRegistry>;
    type PrepareError = std::convert::Infallible;
    type ApplyError = std::io::Error;

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("context-policy-host")
    }

    fn prepare(&self, config: Self::Config) -> Result<Self::Input, Self::PrepareError> {
        Ok(config)
    }

    async fn apply(&self, ctx: Context, input: &Self::Input) -> Result<(), Self::ApplyError> {
        // Generation-owned: Cordis withdraws the publication on dispose.
        let _publication = ctx.provide(input.clone()).map_err(std::io::Error::other)?;
        Ok(())
    }
}
