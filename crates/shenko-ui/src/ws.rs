//! WebSocket handler — routes UI messages to subject inbox, streams subject output.
//! SPEC §21: single subject timeline, real-time streaming.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

use axum::{
    extract::{
        Extension,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use shenko_subject::inbox::{InboxEvent, InboxEventKind};
use shenko_subject::outbox::OutboxMessage;
use shenko_types::ids::EntityId;
use tokio::sync::{broadcast, mpsc::Sender};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Message on the subject timeline
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineMessage {
    pub role: String,
    pub content: String,
    pub message_id: String,
    pub timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_ref: Option<String>,
}

/// Live facts shared by the WS layer and the read-only `/api/self` endpoint.
///
/// Every value is recorded from a real event (a client socket opening, a
/// subject outbox message). Unknown values keep their sentinel until observed,
/// so `/api/self` can answer `null` instead of guessing.
#[derive(Debug)]
pub struct LiveStatus {
    connections: AtomicUsize,
    wired: AtomicBool,
    /// Unix seconds of the last observed subject outbox message; -1 = never.
    last_outbox_unix: AtomicI64,
    /// Step number carried by the last observed outbox message; -1 = unknown.
    last_step: AtomicI64,
}

impl LiveStatus {
    pub fn new() -> Self {
        Self {
            connections: AtomicUsize::new(0),
            wired: AtomicBool::new(false),
            last_outbox_unix: AtomicI64::new(-1),
            last_step: AtomicI64::new(-1),
        }
    }

    /// True once the subject outbox is attached, i.e. the loop can be observed.
    pub fn set_wired(&self, wired: bool) {
        self.wired.store(wired, Ordering::Relaxed);
    }

    pub fn is_wired(&self) -> bool {
        self.wired.load(Ordering::Relaxed)
    }

    /// Register a live WebSocket client. The returned guard decrements the
    /// counter when the socket closes (including every early-exit path).
    pub fn connection_added(self: &Arc<Self>) -> ConnectionGuard {
        self.connections.fetch_add(1, Ordering::Relaxed);
        ConnectionGuard {
            live: Arc::clone(self),
        }
    }

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::Relaxed)
    }

    /// Record one committed thought published on the subject outbox.
    pub fn record_outbox(&self, step: u64) {
        self.last_step.store(step as i64, Ordering::Relaxed);
        self.last_outbox_unix
            .store(Utc::now().timestamp(), Ordering::Relaxed);
    }

    pub fn last_step(&self) -> Option<u64> {
        match self.last_step.load(Ordering::Relaxed) {
            s if s < 0 => None,
            s => Some(s as u64),
        }
    }

    /// Seconds since the last observed outbox message; None = never observed.
    pub fn seconds_since_activity(&self) -> Option<i64> {
        match self.last_outbox_unix.load(Ordering::Relaxed) {
            t if t < 0 => None,
            t => Some(Utc::now().timestamp().saturating_sub(t)),
        }
    }
}

impl Default for LiveStatus {
    fn default() -> Self {
        Self::new()
    }
}

/// Decrements the live connection count when a WS socket task ends.
#[derive(Debug)]
pub struct ConnectionGuard {
    live: Arc<LiveStatus>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.live.connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Shared broadcast state
#[derive(Clone)]
pub struct WsState {
    pub subject_tx: std::sync::Arc<broadcast::Sender<TimelineMessage>>,
    /// Forward UI messages into the subject inbox.
    /// TODO(main.rs): wire by passing UiServer::new_with_inbox(inbox.sender()) after S2 subject loop init.
    pub inbox_tx: Option<std::sync::Arc<Sender<InboxEvent>>>,
    /// Subject outbound output. Each connected client subscribes here so model
    /// output reaches every open timeline. SPEC §5.1, §21.12.
    pub outbox_tx: Option<std::sync::Arc<broadcast::Sender<OutboxMessage>>>,
    /// Live connection/subject facts shared with `/api/self`.
    pub live: Arc<LiveStatus>,
    pub store: Option<Arc<shenko_store::Store>>,
}

impl WsState {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            subject_tx: std::sync::Arc::new(tx),
            inbox_tx: None,
            outbox_tx: None,
            live: Arc::new(LiveStatus::new()),
            store: None,
        }
    }

    pub fn new_with_inbox(inbox_tx: Sender<InboxEvent>) -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            subject_tx: std::sync::Arc::new(tx),
            inbox_tx: Some(std::sync::Arc::new(inbox_tx)),
            outbox_tx: None,
            live: Arc::new(LiveStatus::new()),
            store: None,
        }
    }

    pub fn with_inbox(mut self, inbox_tx: Sender<InboxEvent>) -> Self {
        self.inbox_tx = Some(std::sync::Arc::new(inbox_tx));
        self
    }

    /// Attach the subject's outbox broadcaster — SPEC §5.1, §21.12.
    pub fn with_outbox(mut self, outbox_tx: broadcast::Sender<OutboxMessage>) -> Self {
        self.outbox_tx = Some(std::sync::Arc::new(outbox_tx));
        self
    }

    pub fn broadcast(&self, msg: TimelineMessage) {
        let _ = self.subject_tx.send(msg);
    }
}

impl Default for WsState {
    fn default() -> Self {
        Self::new()
    }
}

pub async fn ws_handler(ws: WebSocketUpgrade, Extension(state): Extension<WsState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum SubmissionStage {
    Queued,
    Rejected,
}

#[derive(Serialize)]
struct SubmissionReceipt<'a> {
    schema_version: u32,
    #[serde(rename = "type")]
    kind: &'static str,
    submission_id: &'a str,
    stage: SubmissionStage,
    detail: &'static str,
}

#[derive(Deserialize)]
struct UserSubmission {
    submission_id: String,
    message_id: String,
    content: String,
}

async fn handle_socket(mut socket: WebSocket, state: WsState) {
    info!("ws client connected");
    // Counted for `/api/self`; dropped on every exit path from this task.
    let _connection = state.live.connection_added();
    let mut rx = state.subject_tx.subscribe();
    let mut outbox_rx = state.outbox_tx.as_ref().map(|tx| tx.subscribe());

    // Send welcome
    let welcome = TimelineMessage {
        role: "system".to_string(),
        content: "已连接传输服务；处理状态以持久结果为准。".to_string(),
        message_id: Uuid::new_v4().to_string(),
        timestamp: Utc::now().to_rfc3339(),
        stream: None,
        step: None,
        step_id: None,
        commit_ref: None,
    };
    if let Ok(json) = serde_json::to_string(&welcome) {
        let _ = socket.send(Message::Text(json.into())).await;
    }

    let mut identity_revision: Option<u64> = None;
    let mut committed_seq = 0u64;
    let mut history = std::collections::VecDeque::new();
    loop {
        tokio::select! {
            // Subject broadcasts → all WS clients
            msg = rx.recv() => {
                match msg {
                    Ok(m) => {
                        if let Ok(json) = serde_json::to_string(&m) {
                            if socket.send(Message::Text(json.into())).await.is_err() { break; }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // client fell behind; skip old messages
                    }
                    Err(_) => break,
                }
            }
            // Subject model output → every connected client. SPEC §5.1, §21.12
            out = recv_committed(&state, &mut committed_seq, &mut history, &mut outbox_rx) => {
                match out {
                    Some(out) => {
                        // Identity revisions change mid-session (self naming,
                        // CLI rename); tell the page so it re-reads /api/self.
                        if out.identity_revision.is_some() && out.identity_revision != identity_revision {
                            identity_revision = out.identity_revision;
                            let note = serde_json::json!({ "type": "identity", "revision": identity_revision });
                            if socket.send(Message::Text(note.to_string().into())).await.is_err() { break; }
                        }
                        let msg = TimelineMessage {
                            role: "subject".to_string(),
                            content: out.text,
                            message_id: out.step_id.to_string(),
                            timestamp: Utc::now().to_rfc3339(),
                            stream: None,
                            step: Some(out.step.to_string()),
                            step_id: Some(out.step_id.to_string()),
                            commit_ref: Some(out.commit_ref.to_string()),
                        };
                        if let Ok(json) = serde_json::to_string(&msg) {
                            if socket.send(Message::Text(json.into())).await.is_err() { break; }
                        }
                    }
                    None => break,
                }
            }
            // Client → subject inbox
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        debug!("ws recv {} bytes", text.len());
                        if let Ok(user_msg) = serde_json::from_str::<UserSubmission>(&text) {
                            // Route the user message into the subject inbox (SPEC §5.1).
                            let event = InboxEvent {
                                event_id: EntityId::new(),
                                kind: InboxEventKind::Message,
                                payload: serde_json::json!({
                                    "content": user_msg.content,
                                    "message_id": user_msg.message_id,
                                    "submission_id": user_msg.submission_id,
                                    "role": "user",
                                }),
                                created_at: Utc::now(),
                                source: "web_ui".to_string(),
                            };
                            // A successful try_send is only volatile queue admission,
                            // not persistence or a subject decision. Never echo success
                            // when the bounded inbox is full or its consumer has stopped.
                            let (stage, detail) = match &state.inbox_tx {
                                Some(tx) => match tx.try_send(event) {
                                    Ok(()) => (SubmissionStage::Queued, "已进入内存队列；尚未持久化或处理，服务退出前仍可能丢失。"),
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => (SubmissionStage::Rejected, "主体输入队列已满，请稍后重试。"),
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => (SubmissionStage::Rejected, "主体已停止，需要恢复。"),
                                },
                                None => (SubmissionStage::Rejected, "主体输入未连接。"),
                            };
                            // This wire receipt is what the actual page consumes.
                            // Queued is deliberately NOT durable acceptance.
                            let reply = SubmissionReceipt {
                                schema_version: 1, kind: "receipt", submission_id: &user_msg.submission_id,
                                stage, detail,
                            };
                            let json = serde_json::to_string(&reply).expect("receipt serialization");
                            if socket.send(Message::Text(json.into())).await.is_err() { break; }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => { info!("ws disconnected"); break; }
                    Some(Err(e)) => { warn!(err=%e, "ws error"); break; }
                    _ => {}
                }
            }
        }
    }
}

/// Await the next subject outbox message. Pends forever when no outbox is wired,
/// so the select loop stays idle instead of spinning. SPEC §21.12.
async fn recv_outbox(rx: &mut Option<broadcast::Receiver<OutboxMessage>>) -> Option<OutboxMessage> {
    match rx {
        Some(rx) => loop {
            match rx.recv().await {
                Ok(m) => return Some(m),
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        },
        None => std::future::pending().await,
    }
}

async fn recv_committed(
    state: &WsState,
    seq: &mut u64,
    history: &mut std::collections::VecDeque<OutboxMessage>,
    wake: &mut Option<broadcast::Receiver<OutboxMessage>>,
) -> Option<OutboxMessage> {
    let Some(store) = &state.store else {
        return recv_outbox(wake).await;
    };
    loop {
        if let Some(out) = history.pop_front() {
            *seq = out.commit_ref.seq.0;
            state.live.record_outbox(out.step);
            return Some(out);
        }
        match shenko_subject::outbox::committed_after(store, *seq, 64) {
            Ok(messages) => history.extend(messages),
            Err(e) => {
                tracing::error!(err=%e,"committed timeline read failed; no guessed replay");
                return None;
            }
        }
        if history.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        }
    }
}
