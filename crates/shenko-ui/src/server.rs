use anyhow::Result;
use axum::{Extension, Router, routing::get};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::api::{self, ApiState};
use crate::playground::{self, PlaygroundState};
use crate::routes::*;
use crate::ws::{WsState, ws_handler};

pub struct UiServer {
    pub bind: SocketAddr,
    pub state: WsState,
    /// Root under which Playground artifacts persist (SHENKO_DATA_DIR/playground).
    pub playground_root: PathBuf,
    /// Data directory reported by `/api/config`. None = unknown.
    data_dir: Option<PathBuf>,
    /// Read-only store handle for `/api/tasks`. None = endpoint reports
    /// `available: false` instead of inventing task rows.
    store: Option<std::sync::Arc<shenko_store::store::Store>>,
    inbox_tx: Option<tokio::sync::mpsc::Sender<shenko_subject::inbox::InboxEvent>>,
}

impl UiServer {
    pub fn new(port: u16) -> Self {
        Self {
            bind: SocketAddr::from(([0, 0, 0, 0], port)),
            state: WsState::new(),
            playground_root: default_playground_root(),
            data_dir: None,
            store: None,
            inbox_tx: None,
        }
    }

    pub fn ws_state(&self) -> WsState {
        self.state.clone()
    }

    /// Point the persistent Playground at `<data_dir>/playground`. SPEC §43.2.
    pub fn with_data_dir(mut self, data_dir: impl Into<PathBuf>) -> Self {
        let data_dir = data_dir.into();
        self.playground_root = data_dir.join("playground");
        self.data_dir = Some(data_dir);
        self
    }

    /// Attach the persistent store for the read-only WebUI task list.
    /// Without this, `/api/tasks` honestly serves `available: false`.
    pub fn with_store(mut self, store: std::sync::Arc<shenko_store::store::Store>) -> Self {
        self.state.store = Some(Arc::clone(&store));
        self.store = Some(store);
        self
    }

    /// Wire the subject inbox so UI messages reach the loop. SPEC §5.1.
    pub fn with_inbox(
        mut self,
        inbox_tx: tokio::sync::mpsc::Sender<shenko_subject::inbox::InboxEvent>,
    ) -> Self {
        self.state = self.state.with_inbox(inbox_tx.clone());
        self.inbox_tx = Some(inbox_tx);
        self
    }

    /// Wire the subject outbox so model output reaches the timeline. SPEC §21.12.
    pub fn with_outbox(
        mut self,
        outbox_tx: tokio::sync::broadcast::Sender<shenko_subject::outbox::OutboxMessage>,
    ) -> Self {
        self.state = self.state.with_outbox(outbox_tx);
        self
    }

    pub async fn run(self) -> Result<()> {
        // Playground shares the inbox the WS handler already writes to, so
        // artifact changes are perceived by the same subject loop. SPEC §43.2.
        let mut playground_state = PlaygroundState::new(self.playground_root);
        if let Some(inbox_tx) = self.inbox_tx.clone() {
            playground_state = playground_state.with_inbox(inbox_tx);
        }
        let playground_root = playground_state.root().to_path_buf();

        // Observe the subject outbox independently of any browser client, so
        // `/api/self` knows the last committed thought step and whether the
        // loop is active. A broadcast subscriber never consumes messages meant
        // for WS clients. SPEC §21.12.
        if let Some(outbox_tx) = self.state.outbox_tx.as_ref() {
            self.state.live.set_wired(true);
            let live = Arc::clone(&self.state.live);
            let mut outbox_rx = outbox_tx.subscribe();
            tokio::spawn(async move {
                while let Ok(msg) = outbox_rx.recv().await {
                    live.record_outbox(msg.step);
                }
            });
        }

        let mut api_state = ApiState::new();
        if let Some(store) = self.store.clone() {
            api_state = api_state.with_store(store);
        }
        if let Some(data_dir) = self.data_dir.clone() {
            api_state = api_state.with_data_dir(data_dir);
        }
        // Shared Arc registries: `/api/self` reports the same live objects the
        // Playground routes and WS handler use, not a second copy.
        api_state = api_state.with_playground(playground_state.clone());
        api_state = api_state.with_live(Arc::clone(&self.state.live));

        let app = Router::new()
            .route("/", get(index_handler))
            .route("/health", get(health_handler))
            .route("/ws", get(ws_handler))
            .merge(playground::router(playground_state))
            .merge(api::router(api_state))
            .layer(Extension(self.state))
            .layer(CorsLayer::permissive());
        info!(addr=%self.bind, playground=%playground_root.display(), "UI server starting");
        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}

/// Fallback used by `UiServer::new` when the caller does not pass a data dir.
fn default_playground_root() -> PathBuf {
    std::env::var_os("SHENKO_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("shenko"))
        .join("playground")
}
