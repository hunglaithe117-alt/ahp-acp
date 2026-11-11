//! AHP WebSocket server demonstrating multi-client coordination.
//!
//! Implements the server side of AHP (Agent Host Protocol) — the `ahp` crate
//! is client-only, so this server speaks the same wire format using `ahp-types`
//! and Axum WebSocket handlers. It demonstrates:
//!
//! - Channel management (subscribe / unsubscribe)
//! - Redux-style state per channel
//! - Server-sequenced action broadcasting
//! - Multi-client session coordination

use std::{
    collections::HashMap,
    sync::Arc,
};

use ahp_types::{
    actions::{ActionEnvelope, ActionOrigin, StateAction},
    commands::{
        DispatchActionParams, InitializeParams, InitializeResult,
        SubscribeParams, SubscribeResult, UnsubscribeParams,
    },
    common::{ROOT_RESOURCE_URI, Uri},
    messages::{
        JsonRpcErrorResponse, JsonRpcMessage,
        JsonRpcRequest, JsonRpcSuccessResponse, JsonRpcVersion,
    },
    state::{RootState, SessionState, Snapshot, SnapshotState, SessionLifecycle},
    PROTOCOL_VERSION,
};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State as AxumState, WebSocketUpgrade,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

/// Global server state shared across all WebSocket connections.
struct ServerState {
    /// Monotonically increasing server sequence number.
    server_seq: RwLock<u64>,
    /// Per-channel state snapshots (channel URI → JSON state).
    channels: RwLock<HashMap<Uri, Value>>,
    /// Per-channel subscriber broadcasts (channel URI → broadcast sender).
    subscribers: RwLock<HashMap<Uri, broadcast::Sender<ActionEnvelope>>>,
}

impl ServerState {
    fn new() -> Self {
        let root_state = RootState {
            agents: vec![],
            active_sessions: Some(0),
            terminals: None,
            config: None,
            meta: None,
        };
        let mut channels = HashMap::new();
        channels.insert(
            ROOT_RESOURCE_URI.to_string(),
            serde_json::to_value(root_state).unwrap(),
        );
        Self {
            server_seq: RwLock::new(0),
            channels: RwLock::new(channels),
            subscribers: RwLock::new(HashMap::new()),
        }
    }

    /// Allocate the next server sequence number.
    async fn next_seq(&self) -> u64 {
        let mut seq = self.server_seq.write().await;
        *seq += 1;
        *seq
    }

    /// Get the current server sequence number.
    async fn current_seq(&self) -> u64 {
        *self.server_seq.read().await
    }

    /// Get or create channel state.
    async fn get_or_create_channel(&self, channel: &str) -> Value {
        let mut channels = self.channels.write().await;
        channels
            .entry(channel.to_string())
            .or_insert_with(|| create_default_state(channel))
            .clone()
    }

    /// Apply a state action to a channel (Redux-style reducer).
    async fn reduce(&self, channel: &str, action: &StateAction, seq: u64) -> Value {
        let mut channels = self.channels.write().await;
        let state = channels
            .entry(channel.to_string())
            .or_insert_with(|| create_default_state(channel));

        // Apply action to state — simplified reducer for demo purposes.
        // In a full implementation, this would use the ahp reducers.
        apply_action(state, action);

        // Attach serverSeq to the state for observability.
        if let Some(obj) = state.as_object_mut() {
            obj.insert("serverSeq".into(), serde_json::Value::from(seq));
        }
        state.clone()
    }

    /// Subscribe a broadcast sender for a channel, returning the receiver.
    async fn subscribe_channel(
        &self,
        channel: &str,
    ) -> broadcast::Receiver<ActionEnvelope> {
        let mut subs = self.subscribers.write().await;
        let tx = subs
            .entry(channel.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(256);
                tx
            })
            .clone();
        tx.subscribe()
    }

    /// Broadcast an action envelope to all subscribers of a channel.
    async fn broadcast_action(
        &self,
        envelope: ActionEnvelope,
    ) {
        let channel = envelope.channel.clone();
        let subs = self.subscribers.read().await;
        if let Some(tx) = subs.get(&channel) {
            let _ = tx.send(envelope);
        }
    }

    /// Remove a client's subscription from a channel.
    async fn unsubscribe_channel(&self, _channel: &str) {
        // In a full implementation, we'd track per-client subscriptions.
        // For this demo, subscriptions live until the connection drops.
    }
}

/// Create a default state for an unknown channel.
fn create_default_state(channel: &str) -> Value {
    if channel.starts_with("ahp-session:") {
        serde_json::json!({
            "provider": "demo",
            "title": "Demo Session",
            "status": 1,
            "lifecycle": "ready",
            "chats": [],
            "activeClients": [],
        })
    } else if channel.starts_with("ahp-chat:") {
        serde_json::json!({
            "resource": channel,
            "title": "Demo Chat",
            "status": 1,
            "turns": [],
            "modifiedAt": chrono_now(),
        })
    } else {
        serde_json::json!({})
    }
}

/// Simplified reducer — applies an action to a state value in-place.
fn apply_action(state: &mut Value, action: &StateAction) {
    match action {
        StateAction::SessionTitleChanged(a) => {
            if let Some(obj) = state.as_object_mut() {
                obj.insert("title".into(), Value::String(a.title.clone()));
            }
        }
        StateAction::ChatTurnStarted(a) => {
            if let Some(obj) = state.as_object_mut() {
                if let Some(turns) = obj.get_mut("turns").and_then(|v| v.as_array_mut()) {
                    turns.push(serde_json::json!({
                        "turnId": a.turn_id,
                        "startedAt": a.started_at,
                        "message": a.message,
                    }));
                }
                obj.insert("status".into(), Value::from(8)); // InProgress
            }
        }
        StateAction::ChatTurnComplete(_a) => {
            if let Some(obj) = state.as_object_mut() {
                obj.insert("status".into(), Value::from(1)); // Idle
            }
        }
        StateAction::ChatDelta(a) => {
            // Delta actions are streamed, not stored in state.
            tracing::debug!(turn_id = %a.turn_id, part_id = %a.part_id, "chat delta received");
        }
        _ => {
            tracing::debug!(action = ?action, "unhandled action in reducer");
        }
    }
}

fn chrono_now() -> String {
    // Simple ISO 8601 timestamp without pulling in chrono.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{now}")
}

/// Build a typed `SnapshotState` based on the channel URI prefix.
fn build_snapshot_state(channel: &str) -> SnapshotState {
    if channel == ROOT_RESOURCE_URI {
        SnapshotState::Root(Box::new(RootState {
            agents: vec![],
            active_sessions: Some(0),
            terminals: None,
            config: None,
            meta: None,
        }))
    } else if channel.starts_with("ahp-session:") {
        SnapshotState::Session(Box::new(SessionState {
            provider: "demo".into(),
            title: "Demo Session".into(),
            status: 1,
            activity: None,
            project: None,
            working_directory: None,
            annotations: None,
            lifecycle: SessionLifecycle::Ready,
            creation_error: None,
            server_tools: None,
            active_clients: vec![],
            chats: vec![],
            default_chat: None,
            config: None,
            customizations: None,
            changesets: None,
            input_needed: None,
            meta: None,
        }))
    } else {
        // Fallback: return an empty root state for unknown channels.
        SnapshotState::Root(Box::new(RootState {
            agents: vec![],
            active_sessions: Some(0),
            terminals: None,
            config: None,
            meta: None,
        }))
    }
}

fn rpc_response(id: u64, result: Value) -> JsonRpcMessage {
    JsonRpcMessage::SuccessResponse(JsonRpcSuccessResponse {
        jsonrpc: JsonRpcVersion::V2,
        id,
        result: ahp_types::common::AnyValue::from(result),
    })
}

fn rpc_error(id: u64, code: i32, message: impl Into<String>) -> JsonRpcMessage {
    JsonRpcMessage::ErrorResponse(JsonRpcErrorResponse {
        jsonrpc: JsonRpcVersion::V2,
        id,
        error: ahp_types::messages::JsonRpcError {
            code,
            message: message.into(),
            data: None,
        },
    })
}

fn rpc_notification(method: &str, params: Value) -> JsonRpcMessage {
    JsonRpcMessage::Notification(ahp_types::messages::JsonRpcNotification {
        jsonrpc: JsonRpcVersion::V2,
        method: method.into(),
        params: Some(ahp_types::common::AnyValue::from(params)),
    })
}

async fn send_json(ws: &mut WebSocket, msg: &JsonRpcMessage) {
    if let Ok(json) = serde_json::to_string(msg) {
        if let Err(e) = ws.send(Message::Text(json.into())).await {
            tracing::warn!(?e, "ws send failed");
        }
    }
}

async fn handle_connection(
    mut ws: WebSocket,
    state: Arc<ServerState>,
) {
    let client_id = Uuid::new_v4().to_string();
    tracing::info!(client_id = %client_id, "client connected");

    // Channel to forward broadcast events to the WebSocket send loop.
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<String>(64);

    // Process incoming messages and spawn broadcast forwarders on subscribe.
    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        let text_str: &str = &text;
                        match handle_message(
                            text_str,
                            &state,
                            &client_id,
                            &event_tx,
                        ).await {
                            Some(response) => send_json(&mut ws, &response).await,
                            None => {} // notification, no response
                        }
                    }
                    Some(Ok(Message::Close(_))) => break,
                    Some(Err(e)) => {
                        tracing::warn!(?e, "ws error");
                        break;
                    }
                    None => break,
                    _ => {} // Binary, Ping, Pong
                }
            }
            event = event_rx.recv() => {
                if let Some(json) = event {
                    let _ = ws.send(Message::Text(json.into())).await;
                } else {
                    break; // event_tx dropped — server shut down
                }
            }
        }
    }

    tracing::info!(client_id = %client_id, "client disconnected");
}

/// Process a single inbound JSON-RPC message and return an optional response.
async fn handle_message(
    text: &str,
    state: &Arc<ServerState>,
    client_id: &str,
    event_tx: &tokio::sync::mpsc::Sender<String>,
) -> Option<JsonRpcMessage> {
    let msg: JsonRpcMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(?e, "failed to parse JSON-RPC");
            return Some(rpc_error(0, -32700, "parse error"));
        }
    };

    match msg {
        JsonRpcMessage::Request(req) => handle_request(req, state, client_id, event_tx).await,
        JsonRpcMessage::Notification(n) => {
            handle_notification(n, state, client_id).await;
            None
        }
        _ => {
            tracing::debug!("ignoring non-request/notification message");
            None
        }
    }
}

/// Handle an incoming JSON-RPC request.
async fn handle_request(
    req: JsonRpcRequest,
    state: &Arc<ServerState>,
    _client_id: &str,
    event_tx: &tokio::sync::mpsc::Sender<String>,
) -> Option<JsonRpcMessage> {
    match req.method.as_str() {
        "initialize" => {
            let params: InitializeParams = match parse_params(&req.params) {
                Some(p) => p,
                None => return Some(rpc_error(req.id, -32602, "invalid params")),
            };

            tracing::info!(
                client_id = %params.client_id,
                versions = ?params.protocol_versions,
                "initialize"
            );

            // Subscribe to initial channels if requested.
            if let Some(subs) = &params.initial_subscriptions {
                for uri in subs {
                    let mut rx = state.subscribe_channel(uri).await;
                    let tx = event_tx.clone();
                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(env) => {
                                    let wire = rpc_notification(
                                        "action",
                                        serde_json::to_value(&env).unwrap(),
                                    );
                                    if let Ok(json) = serde_json::to_string(&wire) {
                                        if tx.send(json).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                                Err(broadcast::error::RecvError::Closed) => break,
                            }
                        }
                    });
                }
            }

            let result = InitializeResult {
                protocol_version: PROTOCOL_VERSION.to_string(),
                server_seq: state.current_seq().await as i64,
                server_info: Some(ahp_types::commands::Implementation {
                    name: "ahp-host-rust".into(),
                    version: Some(env!("CARGO_PKG_VERSION").into()),
                    title: Some("AHP Host (Rust)".into()),
                }),
                snapshots: vec![], // Simplified — no snapshots in this demo.
                default_directory: None,
                completion_trigger_characters: None,
                terminal_command_prefix: None,
                telemetry: None,
            };

            let val = serde_json::to_value(result).unwrap();
            Some(rpc_response(req.id, val))
        }

        "subscribe" => {
            let params: SubscribeParams = match parse_params(&req.params) {
                Some(p) => p,
                None => return Some(rpc_error(req.id, -32602, "invalid params")),
            };

            tracing::info!(channel = %params.channel, "subscribe");

            // Ensure channel state exists.
            let _snapshot_json = state.get_or_create_channel(&params.channel).await;
            let mut rx = state.subscribe_channel(&params.channel).await;

            // Spawn a task that forwards broadcast events to the WebSocket.
            let tx = event_tx.clone();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(env) => {
                            // Serialize the action envelope as a JSON-RPC notification.
                            let wire = rpc_notification(
                                "action",
                                serde_json::to_value(&env).unwrap(),
                            );
                            if let Ok(json) = serde_json::to_string(&wire) {
                                if tx.send(json).await.is_err() {
                                    break; // WebSocket closed
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });

            let from_seq = state.current_seq().await as i64;

            // Build a proper Snapshot with typed state based on the channel URI.
            let snapshot_state = build_snapshot_state(&params.channel);
            let result = SubscribeResult {
                snapshot: Some(Snapshot {
                    resource: params.channel.clone(),
                    state: snapshot_state,
                    from_seq,
                }),
            };

            let val = serde_json::to_value(result).unwrap();
            Some(rpc_response(req.id, val))
        }

        "unsubscribe" => {
            let params: UnsubscribeParams = match parse_params(&req.params) {
                Some(p) => p,
                None => return Some(rpc_error(req.id, -32602, "invalid params")),
            };

            tracing::info!(channel = %params.channel, "unsubscribe");
            state.unsubscribe_channel(&params.channel).await;

            Some(rpc_response(req.id, serde_json::json!({})))
        }

        "ping" => {
            Some(rpc_response(req.id, serde_json::json!({})))
        }

        method => {
            tracing::warn!(%method, "unknown method");
            Some(rpc_error(req.id, -32601, format!("unknown method: {method}")))
        }
    }
}

/// Handle an incoming JSON-RPC notification (fire-and-forget).
async fn handle_notification(
    n: ahp_types::messages::JsonRpcNotification,
    state: &Arc<ServerState>,
    client_id: &str,
) {
    match n.method.as_str() {
        "dispatchAction" => {
            let params: DispatchActionParams = match parse_params(&n.params) {
                Some(p) => p,
                None => {
                    tracing::warn!("invalid dispatchAction params");
                    return;
                }
            };

            let seq = state.next_seq().await;
            tracing::info!(
                channel = %params.channel,
                seq,
                client_seq = params.client_seq,
                "dispatchAction"
            );

            // Apply action to channel state.
            let _state = state.reduce(&params.channel, &params.action, seq).await;

            // Broadcast the action envelope to all subscribers.
            let envelope = ActionEnvelope {
                channel: params.channel,
                action: params.action,
                server_seq: seq,
                origin: Some(ActionOrigin {
                    client_id: client_id.to_string(),
                    client_seq: params.client_seq,
                }),
                rejection_reason: None,
            };

            state.broadcast_action(envelope).await;
        }

        "unsubscribe" => {
            let params: UnsubscribeParams = match parse_params(&n.params) {
                Some(p) => p,
                None => return,
            };
            state.unsubscribe_channel(&params.channel).await;
        }

        method => {
            tracing::debug!(%method, "unhandled notification");
        }
    }
}

fn parse_params<T: serde::de::DeserializeOwned>(
    params: &Option<ahp_types::common::AnyValue>,
) -> Option<T> {
    params.as_ref().and_then(|v| {
        let val: &Value = v;
        serde_json::from_value(val.clone()).ok()
    })
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    AxumState(state): AxumState<Arc<ServerState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_connection(socket, state))
}

async fn health() -> &'static str {
    "ok"
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ahp_host=info,tower_http=info".into()),
        )
        .init();

    let state = Arc::new(ServerState::new());

    let app = Router::new()
        .route("/ws/ahp", get(ws_handler))
        .route("/health", get(health))
        .with_state(state);

    let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3001".into());
    let listener = tokio::net::TcpListener::bind(&bind).await.unwrap();
    tracing::info!(addr = %bind, "AHP host listening");
    axum::serve(listener, app).await.unwrap();
}
