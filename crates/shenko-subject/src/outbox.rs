//! Subject outbox — the outbound half of the subject timeline. SPEC §5.1, §21.12
//!
//! The subject loop publishes each completed model output here so transports
//! (the Web UI, other channels) can render it without reaching into loop state.
use serde::{Deserialize, Serialize};

/// One outbound subject message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxMessage {
    /// What kind of output this is, e.g. `"thought"` or `"reply"`.
    pub kind: String,
    /// The subject step number that produced it.
    pub step: u64,
    pub step_id: shenko_types::ids::EntityId,
    pub commit_ref: shenko_types::commit::CommitRef,
    /// The model output text.
    pub text: String,
    /// Already validated and committed publication actions; never reparse text.
    pub publications: Vec<shenko_types::subject::PublicationIntent>,
    /// Identity revision in effect after this step (SPEC §6), so transports
    /// can refresh the displayed name/soul when it changes.
    pub identity_revision: Option<u64>,
}

/// Replayable committed-only projection. Stable IDs let transports de-duplicate
/// a repeated snapshot independently of effect dispatch.
pub fn committed_after(
    store: &shenko_store::Store,
    seq: u64,
    limit: usize,
) -> anyhow::Result<Vec<OutboxMessage>> {
    store
        .committed_decisions_after(seq, limit)?
        .into_iter()
        .map(|(step, decision)| {
            let identity_revision = store
                .identity_receipt(step.step_id)?
                .and_then(|r| r.outcome.revision());
            Ok(OutboxMessage {
                kind: "thought".into(),
                step: step.step_no,
                step_id: step.step_id,
                commit_ref: step
                    .commit_ref
                    .ok_or_else(|| anyhow::anyhow!("missing committed ref"))?,
                text: decision.output.text,
                publications: decision.publication_intents,
                identity_revision,
            })
        })
        .collect()
}
