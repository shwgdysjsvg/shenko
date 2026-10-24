//! Budget compact over committed SQLite session events and a SQLite window
//! chain: after the store is closed and reopened, the committed window is the
//! head and still resolves to every raw event, without plugin or registry.
use std::sync::Arc;

use shenko_compact_budget::{BudgetCompact, BudgetConfig};
use shenko_context_policy::{
    CallStatus, HistorySource, ItemKind, ModelCaps, PolicyRegistry, StoreHistory, StoreWindowLog,
    TokenCounter, WindowEntry, WindowLedger,
};
use shenko_store::Store;
use shenko_types::ids::EntityId;
use shenko_types::store::{RawSessionEvent, WriteBatch};

#[test]
fn window_over_store_survives_reopen_and_keeps_every_raw_event() {
    let dir = std::env::temp_dir().join(format!("shenko-c20-{}", EntityId::new()));
    std::fs::create_dir_all(&dir).unwrap();
    let (db, cas) = (dir.join("shenko.db"), dir.join("cas"));
    let session = EntityId::new();
    let count: TokenCounter = Arc::new(|text: &str| 20 + text.len() as u32);

    let store = Arc::new(Store::open(db.clone(), cas.clone()).unwrap());
    // No WriteBatch path creates raw_sessions yet (#6); stand in for that writer.
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute(
            "INSERT INTO raw_sessions (session_id, agent_id, started_at, data) VALUES (?1, ?2, '0', '{}')",
            [session.to_string(), EntityId::new().to_string()],
        )
        .unwrap();
    let mut events = vec![
        ("system", r#"{"text":"system prompt"}"#.to_string()),
        // No status in the payload: must read back as Unknown, not success.
        ("tool_call", r#"{"text":"ssh host deploy","call_id":"ssh-1","tool":"ssh","args_ref":"ledger://call/ssh-1"}"#.into()),
        ("napcat_image", r#"{"text":"[image 1.png]"}"#.into()),
        ("tool_result", r#"{"text":"result without call id"}"#.into()),
        ("user", "plain text, not JSON".into()),
    ];
    for n in 0..30 {
        events.push(("user", serde_json::json!({ "text": format!("question {n}") }).to_string()));
        events.push(("assistant", serde_json::json!({ "text": format!("answer {n}") }).to_string()));
    }
    let mut batch = WriteBatch::new();
    for (seq, (event_type, data)) in events.into_iter().enumerate() {
        batch.session_event_upserts.push(RawSessionEvent {
            event_id: EntityId::new(),
            session_id: session,
            seq: seq as i64,
            event_type: event_type.into(),
            role: event_type.into(),
            data,
        });
    }
    store.commit(batch).unwrap();

    let history = StoreHistory::new(store.clone(), session, count.clone());
    let raw = history.read_range(0, u64::MAX).unwrap();
    assert!(matches!(raw[1].kind, ItemKind::ToolCall { status: CallStatus::Unknown, .. }));
    assert_eq!(raw[2].kind, ItemKind::Other { event_type: "napcat_image".into() });
    assert_eq!(raw[3].kind, ItemKind::Other { event_type: "tool_result".into() });
    assert_eq!((raw[4].kind.clone(), raw[4].text.as_str()), (ItemKind::User, "plain text, not JSON"));

    let caps = ModelCaps { model_id: "text-only".into(), max_context: 1000, vision: None };
    let log = Arc::new(StoreWindowLog::new(store.clone(), session));
    let registry = PolicyRegistry::with_ledger(WindowLedger::new(log));
    registry.install(Arc::new(BudgetCompact::new(BudgetConfig::default())));
    let used = raw.iter().map(|i| i.tokens).sum();
    let (prepared, _) = registry.prepare(&history, &caps, used).unwrap();
    let window = registry.commit(prepared, &history).unwrap();
    drop((registry, history, store));

    // Reopen: no plugin, no registry, only the store.
    let store = Arc::new(Store::open(db, cas).unwrap());
    let history = StoreHistory::new(store.clone(), session, count);
    let head = WindowLedger::new(Arc::new(StoreWindowLog::new(store, session))).current().unwrap();
    assert_eq!(head, window, "committed window is the durable head");
    let visible = head.materialize(&history).unwrap();
    let mut seen = Vec::new();
    for entry in &visible {
        match entry {
            WindowEntry::Verbatim { at, .. } => {
                assert_eq!(history.read_event(at.event_id).unwrap().unwrap().at, *at);
                seen.push(at.seq);
            }
            WindowEntry::Pointer { from, to, .. } => {
                seen.extend(history.read_range(*from, *to).unwrap().iter().map(|i| i.at.seq));
            }
            other => panic!("budget compact made a derived entry: {other:?}"),
        }
    }
    seen.sort_unstable();
    assert_eq!(seen, (0..raw.len() as u64).collect::<Vec<_>>(), "every raw event addressable");
    let _ = std::fs::remove_dir_all(dir);
}
