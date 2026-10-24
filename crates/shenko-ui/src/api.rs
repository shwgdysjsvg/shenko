//! Honest read-only JSON endpoints behind the WebUI 任务 / 记忆 / 配置 / 自身状态 pages.
//! SPEC §21 (single subject timeline / UI), §43 (playground & configuration).
//!
//! These handlers only report state that actually exists on this node. When the
//! store handle has not been wired (main.rs still calls `UiServer::new`), the
//! task endpoint answers `available: false` instead of inventing rows. Memory
//! intents are not persisted to the store yet, so that endpoint says so.
//!
//! No secrets are ever returned: `/api/config` reports only
//! `api_key_configured: true|false`.

use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    routing::get,
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};

use shenko_store::store::Store;

use crate::playground::PlaygroundState;
use crate::ws::LiveStatus;

/// How recent an observed subject outbox message must be for `/api/self` to
/// call the subject `thinking`. Longer silence is reported as `idle`; the
/// window matches the maximum semantic wait (600 s), so a deliberate wait is
/// not mislabelled as idle.
const STATUS_ACTIVE_WINDOW_SECS: i64 = 600;

/// Shared state for the `/api/*` routes. Every field is optional so the router
/// stays usable in tests and in early deployments with no store attached.
#[derive(Clone, Default)]
pub struct ApiState {
    /// Persistent store; `None` means the task list cannot be read yet.
    pub store: Option<Arc<Store>>,
    /// Resolved data directory for `/api/config`. Never includes credentials.
    pub data_dir: Option<PathBuf>,
    /// Live Playground registry (shared Arc), so `/api/self` can list the real
    /// artifacts without a second in-memory index. None = not wired yet.
    pub playground: Option<PlaygroundState>,
    /// Real observations about the subject loop and WebSocket clients.
    pub live: Option<Arc<LiveStatus>>,
}

impl ApiState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_store(mut self, store: Arc<Store>) -> Self {
        self.store = Some(store);
        self
    }

    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        self.data_dir = Some(data_dir.into());
        self
    }

    /// Share the Playground registry created in `UiServer::run`.
    pub fn with_playground(mut self, playground: PlaygroundState) -> Self {
        self.playground = Some(playground);
        self
    }

    /// Share the live subject/client facts tracked by the WS layer.
    pub fn with_live(mut self, live: Arc<LiveStatus>) -> Self {
        self.live = Some(live);
        self
    }
}

/// Mount the JSON API. Registered from [`crate::server::UiServer::run`] so
/// `bin/shenko/src/main.rs` needs no change for the routes themselves.
pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/api/tasks", get(tasks_handler))
        .route("/api/memory", get(memory_handler))
        .route("/api/config", get(config_handler))
        .route("/api/self", get(self_status_handler))
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct TasksQuery {
    limit: Option<usize>,
}

/// `GET /api/tasks` → `{ items: [{task_id, kind, state, created_at}] }`.
/// Empty table → `items: []`; no store handle → `available: false`.
async fn tasks_handler(
    State(state): State<ApiState>,
    Query(query): Query<TasksQuery>,
) -> Json<Value> {
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    match state.store.as_ref() {
        Some(store) => match store.list_recent_tasks(limit) {
            Ok(items) => Json(json!({ "available": true, "items": items })),
            Err(e) => Json(json!({
                "available": false,
                "items": [],
                "note": format!("任务读取失败: {e}"),
            })),
        },
        None => Json(json!({
            "available": false,
            "items": [],
            "note": "store 尚未接入 UI server (UiServer::with_store)",
        })),
    }
}

/// `GET /api/memory` → honest empty result: memory intents are not persisted
/// into `memory_entries` yet, so there is no real data to show.
async fn memory_handler() -> Json<Value> {
    Json(json!({
        "items": [],
        "intents": [],
        "note": "memory intents 尚未持久化",
    }))
}

/// Resolved configuration shared by `/api/config` and `/api/self`.
/// Returns `(model, data_dir, api_key_configured, generation)`.
fn config_values(state: &ApiState) -> (String, Option<String>, bool, String) {
    // Only presence is reported; the key value is never read into a response.
    let api_key_configured = std::env::var("ANTHROPIC_API_KEY").is_ok();
    // Mirrors the model label main.rs prints for this deployment. There is no
    // model negotiator wired into the UI yet, so this is the known binding.
    let model = std::env::var("SHENKO_MODEL").unwrap_or_else(|_| {
        if api_key_configured {
            "claude-opus-5-5".to_string()
        } else {
            "stub".to_string()
        }
    });
    let data_dir = state
        .data_dir
        .clone()
        .or_else(|| std::env::var_os("SHENKO_DATA_DIR").map(PathBuf::from))
        .map(|p| p.display().to_string());
    // RuntimeGeneration is a label whose documented default is "seed"
    // (shenko-types::ids::RuntimeGeneration::default).
    let generation =
        std::env::var("SHENKO_GENERATION").unwrap_or_else(|_| "seed".to_string());
    (model, data_dir, api_key_configured, generation)
}

/// `GET /api/config` → `{ model, data_dir, generation, api_key_configured }`.
/// Secrets are reduced to a boolean; the key itself is never echoed.
async fn config_handler(State(state): State<ApiState>) -> Json<Value> {
    let (model, data_dir, api_key_configured, generation) = config_values(&state);

    Json(json!({
        "model": model,
        "data_dir": data_dir,
        "generation": generation,
        "api_key_configured": api_key_configured,
    }))
}

/// Derive the subject status from real observations:
/// - no outbox wired → the UI cannot see the loop at all → `starting`;
/// - outbox wired but nothing observed yet → `starting` (fresh process);
/// - an outbox message within [`STATUS_ACTIVE_WINDOW_SECS`] → `thinking`;
/// - observed activity, but none recently → `idle`.
fn subject_status(live: Option<&LiveStatus>) -> &'static str {
    let Some(live) = live else {
        return "starting";
    };
    if !live.is_wired() {
        return "starting";
    }
    match live.seconds_since_activity() {
        None => "starting",
        Some(secs) if secs <= STATUS_ACTIVE_WINDOW_SECS => "thinking",
        Some(_) => "idle",
    }
}

/// `GET /api/self` → honest node/self status for the new WebUI.
///
/// Known today:
/// - `connected_clients`: counted live by the WS handler;
/// - `step`: the step number of the last committed thought observed on the
///   subject outbox (the outbox is published only after the S1 commit, so the
///   number is always a durably committed step). TODO: the newest *committed*
///   step is not exposed to the UI crate; when it is, report that instead.
/// - `artifacts`: the real Playground registry, if it was wired.
///
/// Unknown values are `null`, never invented. The local host is the only
/// device this node actually knows about; scheduled/interval activities do not
/// exist yet, so `activities` is empty.
async fn self_status_handler(State(state): State<ApiState>) -> Json<Value> {
    let (model, data_dir, _api_key_configured, _generation) = config_values(&state);

    let status = subject_status(state.live.as_deref());
    let connected_clients = state.live.as_ref().map(|live| live.connections());
    let step = state.live.as_ref().and_then(|live| live.last_step());

    let artifacts: Vec<Value> = match state.playground.as_ref() {
        Some(playground) => playground
            .list()
            .into_iter()
            .map(|record| {
                json!({
                    "id": record.artifact_id.to_string(),
                    "title": record.title,
                    "kind": "file",
                })
            })
            .collect(),
        None => Vec::new(),
    };

    // Instance identity (SPEC §6): the name is the instance's, not the brand.
    let identity = state.store.as_ref().and_then(|store| {
        let agent_id = store.get_existing_agent_id().ok()??;
        store.latest_identity(agent_id).ok()?
    });

    Json(json!({
        "status": status,
        "identity": identity.map(|p| json!({
            "agent_id": p.agent_id.to_string(),
            "revision": p.revision,
            "display_name": p.display_name,
            "naming": p.naming,
            "soul": p.soul,
        })),
        "connected_clients": connected_clients,
        "step": step,
        "model": model,
        "data_dir": data_dir,
        // Honest: only the local node is known. SPEC §5.4 multi-device
        // discovery is not wired into the UI yet.
        "devices": [
            { "id": "local", "label": "本机", "online": true, "kind": "host" }
        ],
        "activities": [],
        "artifacts": artifacts,
    }))
}
