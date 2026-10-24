//! Budget compact through the real pinned Cordis runtime: install, window
//! commit, replacement fencing and raw-history recovery after uninstall.
use std::sync::Arc;

use shenko_compact_budget::{BudgetCompactPlugin, BudgetConfig, POLICY_ID};
use shenko_context_policy::{
    CallStatus, CommitError, ContextPolicyHost, HistorySource, ItemKind, MemoryHistory, ModelCaps,
    PolicyError, PolicyRegistry, WindowEntry,
};
use shenko_kernel::PluginRegistry;
use tokio_util::sync::CancellationToken;

fn caps() -> ModelCaps {
    ModelCaps { model_id: "text-only".into(), max_context: 4000, vision: None }
}

#[tokio::test(flavor = "multi_thread")]
async fn budget_window_keeps_unfinished_calls_and_fences_replaced_generations() {
    let history = MemoryHistory::new();
    history.append(ItemKind::System, "system prompt", 100);
    // A large old call still Unknown in the execution ledger (review numbers:
    // max 4000, system 100, call 2300): the first window stays above trigger.
    history.append(
        ItemKind::ToolCall {
            call_id: "ssh-1".into(),
            tool: "ssh".into(),
            args_ref: "ledger://call/ssh-1".into(),
            status: CallStatus::Unknown,
        },
        "ssh host deploy",
        2300,
    );
    for n in 0..30 {
        history.append(ItemKind::User, format!("question {n}"), 60);
        history.append(ItemKind::Assistant { cancelled: false }, format!("answer {n}"), 60);
    }
    let used: u32 = history.snapshot().iter().map(|i| i.tokens).sum();

    let host = PluginRegistry::new(CancellationToken::new());
    let registry = Arc::new(PolicyRegistry::new());
    host.spawn(ContextPolicyHost, registry.clone()).await.unwrap();
    let budget = host.spawn(BudgetCompactPlugin, BudgetConfig::default()).await.unwrap();
    assert_eq!(registry.installed()[0].0.id, POLICY_ID);
    assert!(registry.should_compact(&history, &caps(), used).unwrap());

    let (prepared, _) = registry.prepare(&history, &caps(), used).unwrap();
    assert_eq!(prepared.carried_calls, vec!["ssh-1".to_string()]);
    assert!(prepared.cost.window_tokens < prepared.cost.source_tokens);
    assert_eq!(prepared.cost.model_calls, 0);

    // Replacing the plugin generation invalidates the window it prepared.
    host.restart(&budget).await.unwrap();
    let stale = registry.commit(prepared, &history).unwrap_err();
    assert!(matches!(stale, CommitError::StalePolicy { .. }));

    let (prepared, _) = registry.prepare(&history, &caps(), used).unwrap();
    // A message arriving between prepare and commit is carried, not lost.
    let late = history.append(ItemKind::User, "late message", 10);
    let window = registry.commit(prepared, &history).unwrap();
    assert_eq!(window.policy_id.as_deref(), Some(POLICY_ID));
    // System prompt + the carried call are an identical prefix; the rest is recomputed.
    assert_eq!(window.cost.reusable_prefix_tokens, 2400);

    // Still above the trigger, but re-cutting the same raw history gains
    // nothing against the window the model sees now: refused, not repeated.
    let now: u32 = window.materialize(&history).unwrap().iter().map(WindowEntry::tokens).sum();
    assert!(registry.should_compact(&history, &caps(), now).unwrap());
    let again = registry.prepare(&history, &caps(), now).unwrap_err();
    assert_eq!(again.attempts()[0].outcome, Err(PolicyError::NoReduction));

    // Uninstall the strategy; the committed window still resolves to raw events.
    budget.dispose().await.unwrap();
    assert!(registry.installed().is_empty());
    let visible = registry.ledger().current().unwrap().materialize(&history).unwrap();
    assert!(visible.iter().any(|e| matches!(e, WindowEntry::Verbatim { at, .. } if *at == late)));
    let unknown_call = visible.iter().find_map(|e| match e {
        WindowEntry::Verbatim { at, .. } => history.read_event(at.event_id).unwrap().filter(|i| {
            matches!(i.kind, ItemKind::ToolCall { status: CallStatus::Unknown, .. })
        }),
        _ => None,
    });
    assert!(unknown_call.is_some(), "unfinished call stays whole and keeps its status");
    let (from, to) = visible
        .iter()
        .find_map(|e| match e {
            WindowEntry::Pointer { from, to, .. } => Some((*from, *to)),
            _ => None,
        })
        .unwrap();
    assert_eq!(history.read_range(from, to).unwrap().len() as u64, to - from + 1);
    host.stop().await.unwrap();
}
