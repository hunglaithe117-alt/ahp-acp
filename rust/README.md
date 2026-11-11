# Rust ACP/AHP Examples

Rust implementations of the **Agent Communication Protocol (ACP)** and the **Agent Host
Protocol (AHP)** built on the official `agent-client-protocol`, `ahp`, `ahp-ws`, and
`ahp-types` crates.

## Workspace

```
rust/
├── Cargo.toml              # workspace: resolver 2, edition 2024
├── acp-echo-agent/         # ACP agent over stdio
├── acp-openai-bridge/      # OpenAI-compatible HTTP API in front of the agent
├── ahp-host/               # AHP JSON-RPC WebSocket server
└── ahp-client-demo/        # AHP demo client (ahp + ahp-ws)
```

```bash
cd rust && cargo build
```

Requires a Rust toolchain with edition 2024 support (1.85+).

## `acp-echo-agent`

Echoes each text content block back to the client over **stdio** using JSON-RPC. It responds
to the three core lifecycle methods:

| Method | Behavior |
|---|---|
| `initialize` | Echoes the requested protocol version, empty `AgentCapabilities` |
| `session/new` | Returns the fixed session ID `session-1` |
| `session/prompt` | Sends `"Echo: "` followed by each text block as `AgentMessageChunk` notifications, then `StopReason::EndTurn` |

```bash
cargo run -p acp-echo-agent
```

### Patterns used

**Role markers.** `Agent` is a zero-sized value that pins the protocol direction at compile
time; `.builder()` starts a connection from that role.

```rust
Agent
    .builder()
    .name("acp-echo-agent")
    .on_receive_request(/* handler */, agent_client_protocol::on_receive_request!())
    .connect_to(Stdio::new())
    .await?;
```

**Typed handlers.** Each handler receives the typed request, a `Responder` for the response,
and a `ConnectionTo` for notifications. The parameter type selects the JSON-RPC method; the
`on_receive_request!()` macro adapts async closures into boxed futures. Handlers return
`Result<(), agent_client_protocol::Error>`.

**Streaming.** Notifications go out through the connection before the final response:

```rust
connection.send_notification(SessionNotification::new(
    req.session_id.clone(),
    SessionUpdate::AgentMessageChunk(ContentChunk::new(
        ContentBlock::Text(TextContent::new("Echo: ")),
    )),
))?;
```

## `acp-openai-bridge`

An Axum server exposing an OpenAI-compatible REST API, backed by an ACP echo agent running
in the same Tokio runtime through `Channel::duplex()`.

```bash
cargo run -p acp-openai-bridge
# 0.0.0.0:3000, override with BIND_ADDR
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/v1/chat/completions` | Chat completion, streaming or not |
| `GET` | `/v1/models` | Lists the single model `acp-echo` |
| `GET` | `/health` | `{"status":"ok","agent":"acp-echo"}` |

Request body: `model`, `messages` (each `{role, content}`), optional `stream` (default
`false`). The prompt sent to the agent is the `content` of the last message with
`role: "user"`.

A failing ACP lifecycle returns `500` with `{"error": {"message": "ACP error: …", "type":
"server_error"}}`.

### Non-streaming

```bash
curl http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "acp-echo", "messages": [{"role": "user", "content": "Hello from curl!"}], "stream": false}'
```

```json
{
  "id": "chatcmpl-<uuid>",
  "object": "chat.completion",
  "created": 1234567890,
  "model": "acp-echo",
  "choices": [{
    "index": 0,
    "message": {"role": "assistant", "content": "Echo: Hello from curl!"},
    "finish_reason": "stop"
  }],
  "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
}
```

The content is the concatenation of every chunk the agent sent, including the `"Echo: "`
prefix. `usage` is always zero — the agent reports no token accounting.

### Streaming (SSE)

```bash
curl -N http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "acp-echo", "messages": [{"role": "user", "content": "Hello streaming!"}], "stream": true}'
```

```
data:{"id":"chatcmpl-<uuid>","object":"chat.completion.chunk","created":...,"model":"acp-echo","choices":[{"index":0,"delta":{"content":"Echo: "},"finish_reason":null}]}

data:{"id":"chatcmpl-<uuid>","object":"chat.completion.chunk","created":...,"model":"acp-echo","choices":[{"index":0,"delta":{"content":"Hello streaming!"},"finish_reason":null}]}

data:{"id":"chatcmpl-<uuid>","object":"chat.completion.chunk","created":...,"model":"acp-echo","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data:[DONE]
```

Each event is built independently, so `id` and `created` differ per chunk. There is no
role-only opening chunk (the Python bridge sends one).

### OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:3000/v1", api_key="not-needed")

response = client.chat.completions.create(
    model="acp-echo",
    messages=[{"role": "user", "content": "Hello from Python!"}],
)
print(response.choices[0].message.content)

stream = client.chat.completions.create(
    model="acp-echo",
    messages=[{"role": "user", "content": "Hello streaming Python!"}],
    stream=True,
)
for chunk in stream:
    delta = chunk.choices[0].delta
    if delta.content:
        print(delta.content, end="", flush=True)
```

### Architecture

Every request constructs a fresh agent in-process:

```
HTTP request
    │
    ▼
Axum handler ──→ Channel::duplex()
    │                  │
    │        ┌─────────┴──────────┐
    │        ▼                    ▼
    │   Agent side           Client side
    │   Echo agent           initialize → session/new → session/prompt
    │   (spawned task)           │
    │        │                   ▼
    │        └──→ SessionNotification text chunks
    │                            │
    ▼                            ▼
JSON response / SSE stream ◄── collected or forwarded text
```

`Channel::duplex()` returns two linked endpoints with no I/O and no subprocess. The client
side drives the ACP lifecycle inside `connect_with`; notifications accumulate into a shared
buffer (non-streaming) or forward into an `mpsc` channel feeding the SSE response (streaming).

## `ahp-host`

A JSON-RPC WebSocket server implementing the host side of AHP on top of `ahp-types` 0.6 —
`ahp` itself is client-only, so the server code speaks the wire format directly.

```bash
cargo run -p ahp-host
# 0.0.0.0:3001, override with BIND_ADDR
```

WebSocket: `ws://localhost:3001/ws/ahp` · Health: `http://localhost:3001/health` (`ok`)

### Methods

| Kind | Method | Purpose |
|---|---|---|
| request | `initialize` | Handshake: requires `channel`, `clientId`, `protocolVersions`; optional `clientInfo`, `initialSubscriptions`, `locale`, `capabilities` |
| request | `subscribe` | Registers the connection for a channel and returns a snapshot |
| request | `unsubscribe` | Accepted; forwarding lives until the socket closes |
| request | `ping` | Returns an empty result |
| notification | `dispatchAction` | Applies `params.action` to `params.channel` and broadcasts it |

`initialize` returns the negotiated protocol version, current `serverSeq`, server info, and
always an empty `snapshots` list; clients receive state through `subscribe`. Unknown methods
answer JSON-RPC error `-32601`, malformed params `-32602`, unparseable JSON `-32700`.

### Wire format

```jsonc
→ {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
     "channel": "ahp-root://", "clientId": "probe", "protocolVersions": ["0.6.0"]
   }}
← {"jsonrpc": "2.0", "id": 1, "result": {
     "protocolVersion": "0.6.0", "serverSeq": 0,
     "serverInfo": {"name": "ahp-host-rust", "version": "0.1.0", "title": "AHP Host (Rust)"},
     "snapshots": []
   }}

→ {"jsonrpc": "2.0", "id": 2, "method": "subscribe", "params": {"channel": "ahp-root://"}}
← {"jsonrpc": "2.0", "id": 2, "result": {"snapshot": {
     "resource": "ahp-root://",
     "state": {"agents": [], "activeSessions": 0},
     "fromSeq": 0
   }}}

→ {"jsonrpc": "2.0", "method": "dispatchAction", "params": {
     "channel": "ahp-session:/demo", "clientSeq": 1,
     "action": {"type": "session/titleChanged", "payload": {"title": "t"}}
   }}
← {"jsonrpc": "2.0", "method": "action", "params": {
     "channel": "ahp-session:/demo",
     "action": {"type": "session/titleChanged", "payload": {"title": "t"}},
     "serverSeq": 1,
     "origin": {"clientId": "<connection uuid>", "clientSeq": 1}
   }}
```

`dispatchAction` is a notification (no response). Broadcasts are `action` notifications whose
params are the `ahp-types::actions::ActionEnvelope`, carrying the originating `clientId` and
`clientSeq`.

### Channels and reducer

`ahp-root://` is pre-populated with an empty `RootState`. Other channels are created on first
subscribe:

- `ahp-session:` prefix → demo session state: `{"provider": "demo", "title": "Demo
  Session", "status": 1, "lifecycle": "ready", "activeClients": [], "chats": []}`
- `ahp-chat:` prefix → demo chat state: `{"resource": <channel>, "title": "Demo Chat",
  "status": 1, "turns": [], "modifiedAt": "<unix seconds>"}`
- anything else → `{}`, and the snapshot falls back to an empty root-shaped state

`apply_action` handles a subset of `ahp-types` actions:

| Action | Effect |
|---|---|
| `SessionTitleChanged` | Sets `title` |
| `ChatTurnStarted` | Appends the turn and sets status to in-progress |
| `ChatTurnComplete` | Sets status back to idle |
| `ChatDelta` | Logged only; deltas are not stored in state |

Other actions are logged and ignored. The allocation of `serverSeq` is global across channels
and is also written into the reduced state for observability. Subscription bookkeeping is
per-connection and lives until the socket closes; `unsubscribe` is accepted but does not tear
down the forwarding task.

### Architecture

```
WebSocket ──→ axum ws_handler ──→ handle_connection
                                   │  select! on incoming text and event channel
                                   ▼
                              handle_message
                     ┌─────────────┼──────────────┐
                     ▼             ▼              ▼
                 initialize    subscribe      dispatchAction
                 (handshake)   (snapshot)     (reduce + broadcast)
                     │             │              │
                     ▼             ▼              ▼
              InitializeResult  Snapshot     ActionEnvelope
                                               │
                                               ▼
                            broadcast::Sender → per-subscription forwarder → "action"
```

Each subscription spawns a task that forwards `ActionEnvelope`s from a `tokio::sync::broadcast`
channel into an `mpsc` channel consumed by the connection's send loop.

## `ahp-client-demo`

Connects two clients (Alice and Bob) to a running host through the official `ahp` client and
`ahp-ws` transport.

```bash
cargo run -p ahp-host          # terminal 1
cargo run -p ahp-client-demo   # terminal 2
```

The URL is fixed at `ws://localhost:3001/ws/ahp` in the demo source.

### What it does

1. Alice and Bob each call `Client::connect` over `WebSocketTransport`
2. Each calls `initialize` with `PROTOCOL_VERSION` and initial subscriptions for
   `ahp-root://` and `ahp-session:/demo`
3. Each calls `subscribe` for both channels and logs the returned snapshot
4. A spawned task logs every `SubscriptionEvent` for the session channel
5. Alice dispatches `ChatTurnStarted`, `ChatDelta`, then `ChatTurnComplete` on
   `ahp-session:/demo`
6. Both clients shut down after the actions propagate

Note that the demo dispatches chat actions on the **session** channel; it never subscribes to
a separate `ahp-chat:` channel.

```
ahp-client-demo
    │
    ├─ Alice ──→ ahp::Client ──→ ahp_ws::WebSocketTransport ──→ ahp-host
    │                │                    │
    │            initialize           subscribe
    │            dispatch             ActionEnvelope events
    │
    └─ Bob   ──→ ahp::Client ──→ ahp_ws::WebSocketTransport ──→ ahp-host
                     │                    │
                 initialize           subscribe
                 (no dispatch)        ActionEnvelope events
```
