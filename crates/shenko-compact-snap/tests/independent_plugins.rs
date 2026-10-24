//! Snapcompact and budget compact as two independent Cordis plugins: honest
//! capability gating, fallback, and uninstalling one without the other.
use std::sync::Arc;

use shenko_compact_budget::{BudgetCompactPlugin, BudgetConfig};
use shenko_compact_snap::{SnapCompact, SnapCompactPlugin, SnapConfig};
use shenko_context_policy::{
    Availability, ContextInput, ContextPolicy, ContextPolicyHost, ItemKind, MemoryHistory,
    ModelCaps, PolicyError, PolicyRegistry, StoreWindowLog, VisionCaps, WindowBase, WindowEntry,
    WindowLedger,
};
use shenko_kernel::{FiberHandle, PluginRegistry};
use shenko_store::Store;
use shenko_types::ids::EntityId;
use tokio_util::sync::CancellationToken;

fn history(text: impl Fn(usize) -> String) -> MemoryHistory {
    let history = MemoryHistory::new();
    history.append(ItemKind::System, "system prompt", 100);
    for n in 0..60 {
        history.append(ItemKind::User, text(n), 200);
        history.append(ItemKind::ToolCall {
            call_id: format!("c{n}"),
            tool: "shell".into(),
            args_ref: format!("ledger://call/c{n}"),
            status: shenko_context_policy::CallStatus::Executed { ok: true },
        }, "cargo test -p shenko-store", 20);
        history.append(ItemKind::ToolResult {
            call_id: format!("c{n}"),
            status: shenko_context_policy::CallStatus::Executed { ok: true },
        }, "x".repeat(5000), 1200);
    }
    history
}

const VISION: ModelCaps = ModelCaps {
    model_id: String::new(),
    max_context: 131072,
    vision: Some(VisionCaps { max_image_context: 32768, patch_px: 28 }),
};

/// Host plus both strategies; returns snapcompact's Fiber for disposal.
async fn host_with_both() -> (PluginRegistry, Arc<PolicyRegistry>, FiberHandle) {
    let host = PluginRegistry::new(CancellationToken::new());
    let registry = Arc::new(PolicyRegistry::new());
    host.spawn(ContextPolicyHost, registry.clone()).await.unwrap();
    let snap = host.spawn(SnapCompactPlugin, SnapConfig::default()).await.unwrap();
    host.spawn(BudgetCompactPlugin, BudgetConfig::default()).await.unwrap();
    registry.set_preference(vec!["snapcompact".into(), "budget-compact".into()]);
    (host, registry, snap)
}

#[tokio::test(flavor = "multi_thread")]
async fn without_usable_vision_snapcompact_reports_and_budget_takes_over() {
    let (host, registry, _snap) = host_with_both().await;
    let text_only = ModelCaps { model_id: "text-only".into(), max_context: 131072, vision: None };
    let h = history(|n| format!("step {n}: fix path crates/shenko-store/src/lib.rs:{n}"));
    let items = h.snapshot();
    let used = items.iter().map(|i| i.tokens).sum();

    let (window, attempts) = registry.prepare(&h, &text_only, used).unwrap();
    assert!(matches!(&attempts[0].outcome, Err(PolicyError::Unavailable(_))));
    assert_eq!(window.policy.id, "budget-compact");

    // Vision present, but records are mostly outside the BMP (no Unifont glyph):
    // reported unreadable, never shipped as a frame, budget falls in.
    let emoji = history(|n| format!("{n} 🚀🔥✅🎉🙏🤖"));
    let (window, attempts) = registry.prepare(&emoji, &VISION, used).unwrap();
    assert!(matches!(&attempts[0].outcome, Err(PolicyError::FrameUnreadable(_))));
    assert_eq!(window.policy.id, "budget-compact");
    host.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn snapcompact_renders_frames_and_uninstalls_independently() {
    let (host, registry, snap) = host_with_both().await;
    // Mixed Chinese / English / digits / paths, as real group chat and tool logs are.
    let h = history(|n| format!("第{n}步：修复 crates/shenko-store/src/lib.rs:{n} 的恢复路径，见 #6"));
    let items = h.snapshot();
    let used = items.iter().map(|i| i.tokens).sum();
    assert!(registry.availability(&VISION).iter().all(|(_, a)| *a == Availability::Available));

    let (prepared, _) = registry.prepare(&h, &VISION, used).unwrap();
    assert_eq!(prepared.policy.id, "snapcompact");
    assert!(prepared.cost.window_tokens <= 32768, "fits the image-request envelope");
    assert!(prepared.cost.truncated_chars > 0, "bounded archive records truncation");
    assert!(prepared.cost.vision_tokens_estimate > 0 && prepared.cost.image_bytes > 0);
    assert_eq!(prepared.cost.unrenderable_chars, 0, "every CJK glyph is drawn, none boxed");
    let frame = &prepared.derivatives[0];
    assert_eq!(&frame.bytes[1..4], b"PNG");
    // Every frame is introduced by a pointer back to its raw range.
    let first_image = prepared.entries.iter().position(|e| matches!(e, WindowEntry::Image { .. }));
    assert!(matches!(prepared.entries[first_image.unwrap() - 1], WindowEntry::Pointer { .. }));
    let window = registry.commit(prepared, &h).unwrap();

    // Uninstalling snapcompact leaves budget compact and the committed frames intact.
    snap.dispose().await.unwrap();
    let ids: Vec<_> = registry.installed().into_iter().map(|(m, _)| m.id).collect();
    assert_eq!(ids, vec!["budget-compact".to_string()]);
    assert_eq!(registry.ledger().current().unwrap().derivatives, window.derivatives);
    h.append(ItemKind::User, "after uninstall", 10);
    // Only budget compact is consulted now, and it is judged against the
    // committed frame window: its text-only window would be larger, so it
    // is refused rather than swapped in.
    let refused = registry.prepare(&h, &VISION, used).unwrap_err();
    assert_eq!(refused.attempts().len(), 1);
    assert_eq!(refused.attempts()[0].policy, "budget-compact");
    assert_eq!(refused.attempts()[0].outcome, Err(PolicyError::NoReduction));
    host.stop().await.unwrap();

    // Prefix reuse follows the actual input: identical entries whose PNG bytes
    // differ (2000 vs 1999 output chars) are not reusable, nor is anything
    // built under a previous model/template/encoder generation. The chain and
    // its frames live in a real SQLite store and survive a reopen.
    let raw = h.snapshot();
    let dir = std::env::temp_dir().join(format!("shenko-c20-{}", EntityId::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let (db, cas, scope) = (dir.join("shenko.db"), dir.join("cas"), EntityId::new());
    let store = Arc::new(Store::open(db.clone(), cas.clone()).unwrap());
    let ledger = WindowLedger::new(Arc::new(StoreWindowLog::new(store.clone(), scope)));
    let cut = |chars, source_window, model_generation| {
        let config = SnapConfig { max_tool_output_chars: chars, ..SnapConfig::default() };
        let base = WindowBase {
            source_window,
            leaf_seq: raw.last().unwrap().at.seq,
            model_generation,
            policy_epoch: 0,
        };
        let input = ContextInput { base, items: &raw, caps: &VISION, used_tokens: used };
        SnapCompact::new(config).prepare(&input).unwrap()
    };
    let a = ledger.commit(cut(2000, ledger.current().unwrap().id, 0), &h).unwrap();
    let b = cut(1999, a.id, 0);
    assert_eq!(b.entries, a.entries, "same ranges, indices and token counts");
    assert_ne!(b.derivatives, a.derivatives, "different pixels");
    let b = ledger.commit(b, &h).unwrap();
    let first_image = b.entries.iter().position(|e| matches!(e, WindowEntry::Image { .. })).unwrap();
    let text_prefix: u32 = b.entries[..first_image].iter().map(WindowEntry::tokens).sum();
    assert_eq!(b.cost.reusable_prefix_tokens, text_prefix);
    ledger.set_model_generation(1).unwrap();
    let c = ledger.commit(cut(1999, b.id, 1), &h).unwrap();
    assert_eq!((c.entries == b.entries, c.derivatives == b.derivatives), (true, true));
    assert_eq!(c.cost.reusable_prefix_tokens, 0, "new binding: nothing reusable");
    drop((ledger, store));

    let store = Arc::new(Store::open(db, cas).unwrap());
    let reopened = WindowLedger::new(Arc::new(StoreWindowLog::new(store, scope)));
    let chain = reopened.all().unwrap();
    let ids: Vec<_> = chain.iter().skip(1).map(|w| w.id).collect();
    assert_eq!(ids, vec![a.id, b.id, c.id], "genesis + three windows, in order");
    assert_eq!(chain[3], c, "frames and body read back byte-identical");
    assert_eq!(reopened.model_generation().unwrap(), 1);
    let _ = std::fs::remove_dir_all(dir);
}
