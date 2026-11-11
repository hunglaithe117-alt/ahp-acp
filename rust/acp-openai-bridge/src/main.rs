//! OpenAI-compatible REST wrapper that bridges HTTP requests to an in-process ACP echo agent.
//!
//! Architecture per request:
//! 1. Create a `Channel::duplex()` pair (in-process JSON-RPC transport)
//! 2. Spawn the ACP echo agent on one endpoint
//! 3. Drive the agent lifecycle on the other endpoint via `Client.builder().connect_with()`
//! 4. Collect streaming `SessionNotification` text chunks
//! 5. Format as OpenAI-compatible response (non-streaming JSON or streaming SSE)

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AgentCapabilities, ContentBlock, ContentChunk, InitializeRequest, InitializeResponse,
    NewSessionRequest, NewSessionResponse, PromptRequest, PromptResponse, SessionId,
    SessionNotification, SessionUpdate, StopReason, TextContent,
};
use agent_client_protocol::{Agent, Channel, Client, ConnectionTo};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::info;

// ---------------------------------------------------------------------------
// OpenAI-compatible request/response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(default)]
    stream: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Serialize)]
struct ChatCompletionResponse {
    id: String,
    object: &'static str,
    created: i64,
    model: String,
    choices: Vec<Choice>,
    usage: Usage,
}

#[derive(Debug, Serialize)]
struct Choice {
    index: u32,
    message: ChatMessage,
    finish_reason: Option<&'static str>,
}

#[derive(Debug, Serialize)]
struct Usage {
    prompt_tokens: u32,
    completion_tokens: u32,
    total_tokens: u32,
}

#[derive(Debug, Serialize)]
struct ModelObject {
    id: String,
    object: &'static str,
    owned_by: &'static str,
}

#[derive(Debug, Serialize)]
struct ModelListResponse {
    object: &'static str,
    data: Vec<ModelObject>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    agent: &'static str,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: ErrorDetail,
}

#[derive(Debug, Serialize)]
struct ErrorDetail {
    message: String,
    r#type: &'static str,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn make_completion_id() -> String {
    format!("chatcmpl-{}", uuid::Uuid::new_v4())
}

/// Extract the user prompt from the last user message in the request.
fn extract_prompt(request: &ChatCompletionRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.clone())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// ACP echo agent — in-process agent built per request via Channel::duplex()
// ---------------------------------------------------------------------------

/// Spawn the echo agent on the given channel endpoint. The agent handles:
/// - `initialize` → negotiates protocol version
/// - `session/new` → creates a session
/// - `session/prompt` → echoes text back via `SessionNotification` chunks
fn spawn_echo_agent(agent_channel: Channel) {
    tokio::spawn(async move {
        let result = Agent
            .builder()
            .name("acp-echo")
            .on_receive_request(
                async |req: InitializeRequest, responder, _connection| {
                    info!(
                        protocol_version = ?req.protocol_version,
                        "Received initialize request"
                    );
                    responder.respond(
                        InitializeResponse::new(req.protocol_version)
                            .agent_capabilities(AgentCapabilities::new()),
                    )
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async |_req: NewSessionRequest, responder, _connection| {
                    let session_id = SessionId::new("session-1");
                    info!(?session_id, "Created new session");
                    responder.respond(NewSessionResponse::new(session_id))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async |req: PromptRequest, responder, connection| {
                    info!(
                        session_id = ?req.session_id,
                        blocks = req.prompt.len(),
                        "Received prompt"
                    );

                    // Stream a prefix chunk
                    connection.send_notification(SessionNotification::new(
                        req.session_id.clone(),
                        SessionUpdate::AgentMessageChunk(ContentChunk::new(
                            ContentBlock::Text(TextContent::new("Echo: ")),
                        )),
                    ))?;

                    // Echo each text content block back
                    for block in &req.prompt {
                        if let ContentBlock::Text(text_content) = block {
                            connection.send_notification(SessionNotification::new(
                                req.session_id.clone(),
                                SessionUpdate::AgentMessageChunk(ContentChunk::new(
                                    ContentBlock::Text(TextContent::new(&text_content.text)),
                                )),
                            ))?;
                        }
                    }

                    responder.respond(PromptResponse::new(StopReason::EndTurn))
                },
                agent_client_protocol::on_receive_request!(),
            )
            .connect_to(agent_channel)
            .await;

        if let Err(e) = result {
            tracing::error!("Echo agent error: {e}");
        }
    });
}

// ---------------------------------------------------------------------------
// HTTP handlers
// ---------------------------------------------------------------------------

/// `POST /v1/chat/completions` — accepts OpenAI ChatCompletion format.
async fn chat_completions(Json(request): Json<ChatCompletionRequest>) -> Response {
    let prompt = extract_prompt(&request);
    let stream = request.stream.unwrap_or(false);

    if stream {
        streaming_response(&prompt, &request.model).await
    } else {
        non_streaming_response(&prompt, &request.model).await
    }
}

/// Non-streaming: run the full ACP lifecycle, collect text, return JSON.
async fn non_streaming_response(prompt: &str, model: &str) -> Response {
    let collected = Arc::new(Mutex::new(String::new()));
    let collected_clone = collected.clone();

    let (client_channel, agent_channel) = Channel::duplex();
    spawn_echo_agent(agent_channel);

    // Register notification handler that collects text chunks, then drive the lifecycle.
    let result = Client
        .builder()
        .name("http-bridge")
        .on_receive_notification(
            async move |notification: SessionNotification, _cx| {
                if let SessionUpdate::AgentMessageChunk(ContentChunk {
                    content: ContentBlock::Text(text_content),
                    ..
                }) = notification.update
                {
                    collected_clone.lock().unwrap().push_str(&text_content.text);
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(client_channel, |cx: ConnectionTo<Agent>| async move {
            let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));

            cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                .block_task()
                .await?;

            let session = cx
                .send_request(NewSessionRequest::new(cwd))
                .block_task()
                .await?;

            cx.send_request(PromptRequest::new(
                session.session_id.clone(),
                vec![ContentBlock::Text(TextContent::new(prompt))],
            ))
            .block_task()
            .await?;

            Ok::<_, agent_client_protocol::Error>(())
        })
        .await;

    match result {
        Ok(()) => {
            let collected_text = collected.lock().unwrap().clone();
            Json(ChatCompletionResponse {
                id: make_completion_id(),
                object: "chat.completion",
                created: now_timestamp(),
                model: model.to_owned(),
                choices: vec![Choice {
                    index: 0,
                    message: ChatMessage {
                        role: "assistant".into(),
                        content: collected_text,
                    },
                    finish_reason: Some("stop"),
                }],
                usage: Usage {
                    prompt_tokens: 0,
                    completion_tokens: 0,
                    total_tokens: 0,
                },
            })
            .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: ErrorDetail {
                    message: format!("ACP error: {e}"),
                    r#type: "server_error",
                },
            }),
        )
            .into_response(),
    }
}

/// Streaming: spawn the ACP interaction in the background, return an SSE stream.
async fn streaming_response(prompt: &str, model: &str) -> Response {
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let prompt = prompt.to_owned();
    let model = model.to_owned();

    // Spawn the ACP client. Text chunks arrive as notification handler messages
    // and are forwarded to `tx`. When the lifecycle completes, `tx` is dropped
    // and the SSE stream ends.
    tokio::spawn(async move {
        let (client_channel, agent_channel) = Channel::duplex();
        spawn_echo_agent(agent_channel);

        let _ = Client
            .builder()
            .name("http-bridge-stream")
            .on_receive_notification(
                async move |notification: SessionNotification, _cx| {
                    if let SessionUpdate::AgentMessageChunk(ContentChunk {
                        content: ContentBlock::Text(text_content),
                        ..
                    }) = notification.update
                    {
                        let _ = tx.send(text_content.text);
                    }
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
            .connect_with(client_channel, |cx: ConnectionTo<Agent>| async move {
                let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));

                cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;

                let session = cx
                    .send_request(NewSessionRequest::new(cwd))
                    .block_task()
                    .await?;

                cx.send_request(PromptRequest::new(
                    session.session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new(prompt))],
                ))
                .block_task()
                .await?;

                Ok::<_, agent_client_protocol::Error>(())
            })
            .await;
        // `tx` is dropped here → stream closes
    });

    // Convert the receiver into an SSE event stream.
    // Each text chunk becomes a chat.completion.chunk; the stream ends with
    // a finish_reason chunk and the `[DONE]` sentinel.
    let model_for_chunks = model.clone();
    let model_for_stop = model.clone();
    let stream = UnboundedReceiverStream::new(rx)
        .map(move |text| {
            let chunk = serde_json::json!({
                "id": make_completion_id(),
                "object": "chat.completion.chunk",
                "created": now_timestamp(),
                "model": model_for_chunks,
                "choices": [{
                    "index": 0,
                    "delta": { "content": text },
                    "finish_reason": null,
                }],
            });
            Ok::<_, Infallible>(Event::default().data(serde_json::to_string(&chunk).unwrap()))
        })
        .chain(stream::once(async move {
            let stop = serde_json::json!({
                "id": make_completion_id(),
                "object": "chat.completion.chunk",
                "created": now_timestamp(),
                "model": model_for_stop,
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }],
            });
            Ok::<_, Infallible>(Event::default().data(serde_json::to_string(&stop).unwrap()))
        }))
        .chain(stream::once(async {
            Ok::<_, Infallible>(Event::default().data("[DONE]"))
        }));

    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// `GET /v1/models` — lists available models.
async fn list_models() -> Json<ModelListResponse> {
    Json(ModelListResponse {
        object: "list",
        data: vec![ModelObject {
            id: "acp-echo".into(),
            object: "model",
            owned_by: "acp-bridge",
        }],
    })
}

/// `GET /health` — health check.
async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        agent: "acp-echo",
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "acp_openai_bridge=info".into()),
        )
        .init();

    let app = Router::new()
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/models", get(list_models))
        .route("/health", get(health));

    let addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    info!("Listening on {addr}");

    axum::Server::from_tcp(listener.into_std().unwrap())
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
}
