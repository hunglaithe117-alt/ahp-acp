# Rust ACP/AHP Examples

Rust implementations of the **Agent Communication Protocol (ACP)** and **Agent Host Protocol (AHP)**
using the official crates from Microsoft.

## Building

```bash
cd rust
cargo build
```

Requires Rust 1.88+ (edition 2024).

## Echo Agent

The simplest ACP agent: echoes back whatever the client sends.

```bash
cargo run -p acp-echo-agent
```

The agent communicates over **stdio** using JSON-RPC messages. It implements core ACP
lifecycle methods:

| Method | Purpose |
|---|---|
| `initialize` | Negotiates protocol version and advertises capabilities |
| `new_session` | Creates a session and returns a session ID |
| `prompt` | Receives user content blocks, streams agent responses back |

### How it works

1. A client (e.g., VS Code with Copilot) spawns the agent as a subprocess
2. The client sends `initialize`, then `new_session`, then `prompt` messages over stdin
3. The agent echoes each `Text` content block back as an `AgentMessageChunk` via `session_update`
4. The response is streamed to the client in real-time

## ACP Rust SDK patterns

### Builder pattern with role markers

The SDK uses zero-sized type role markers (`Agent`, `Client`) to enforce protocol
direction at compile time. Call `.builder()` on the role instance:

```rust
use agent_client_protocol::{Agent, Stdio};

Agent.builder()
    .name("my-agent")
    .on_receive_request(/* handler */, agent_client_protocol::on_receive_request!())
    .connect_to(Stdio::new())
    .await?;
```

### Request handlers

Each handler receives three arguments:

1. **Request** — the typed JSON-RPC request (e.g., `PromptRequest`)
2. **Responder** — a `Responder<ResponseType>` for sending the response
3. **Connection** — a `ConnectionTo<Client>` for sending notifications or requests back

Handlers must return `Result<(), agent_client_protocol::Error>`.

### Streaming with notifications

Use `connection.send_notification()` to push content chunks to the client before
sending the final response:

```rust
use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, SessionNotification, SessionUpdate, TextContent,
};

connection.send_notification(SessionNotification::new(
    session_id.clone(),
    SessionUpdate::AgentMessageChunk(ContentChunk::new(
        ContentBlock::Text(TextContent::new("Hello")),
    )),
))?;
```

### Type-driven dispatch

The `on_receive_request!()` macro converts async closures into boxed futures.
The handler's parameter type determines which JSON-RPC method it handles — no
runtime dispatch needed.

## Comparison with the Python version

| Aspect | Python (`acp` SDK) | Rust (`agent-client-protocol`) |
|---|---|---|
| Transport | `Stdio()` | `Stdio::new()` |
| Agent creation | `Agent()` class | `Agent.builder()` (zero-sized type) |
| Handler registration | `on_prompt`, `on_initialize` methods | `.on_receive_request(handler, on_receive_request!())` |
| Content blocks | `TextContentBlock(text=...)` | `ContentBlock::Text(TextContent::new(...))` |
| Streaming | `session_update` via connection | `connection.send_notification(SessionNotification::new(...))` |
| Type safety | Runtime validation (Pydantic) | Compile-time dispatch via role markers |
| Error handling | Exceptions | `Result<T, Error>` |

## Workspace structure

```
rust/
├── Cargo.toml              # workspace root
├── README.md
├── acp-echo-agent/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── acp-openai-bridge/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
├── ahp-host/
│   ├── Cargo.toml
│   └── src/
│       └── main.rs
└── ahp-client-demo/
    ├── Cargo.toml
    └── src/
        └── main.rs
```

## OpenAI Bridge

An Axum HTTP server that exposes an **OpenAI-compatible REST API** and bridges to
the ACP echo agent over an in-process `Channel::duplex()` transport. No subprocess
spawning — the agent runs inside the same Tokio runtime.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/chat/completions` | OpenAI ChatCompletion (streaming & non-streaming) |
| `GET` | `/v1/models` | List available models |
| `GET` | `/health` | Health check |

### Running

```bash
cargo run -p acp-openai-bridge
# Server starts on 0.0.0.0:3000 (override with BIND_ADDR env var)
```

### Example: non-streaming

```bash
curl http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "acp-echo",
    "messages": [{"role": "user", "content": "Hello from curl!"}],
    "stream": false
  }'
```

Response:

```json
{
  "id": "chatcmpl-...",
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

### Example: streaming (SSE)

```bash
curl http://localhost:3000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "acp-echo",
    "messages": [{"role": "user", "content": "Hello streaming!"}],
    "stream": true
  }'
```

Output:

```
data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"delta":{"content":"Echo: "},"index":0}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hello streaming!"},"index":0}]}

data: {"id":"chatcmpl-...","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop","index":0}]}

data: [DONE]
```

### Example: OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:3000/v1", api_key="not-needed")

# Non-streaming
response = client.chat.completions.create(
    model="acp-echo",
    messages=[{"role": "user", "content": "Hello from Python!"}],
)
print(response.choices[0].message.content)

# Streaming
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

Each HTTP request creates a fresh ACP agent instance in-process:

```
HTTP Request
    │
    ▼
Axum Handler ──→ Channel::duplex()
    │                    │
    │         ┌──────────┴──────────┐
    │         ▼                     ▼
    │   Agent side              Client side
    │   (spawned)               (connect_with)
    │   Echo agent              Lifecycle driver
    │   handles JSON-RPC        initialize → new_session → prompt
    │                              │
    │                              ▼
    │                        SessionNotification
    │                        text chunks via channel
    │                              │
    ▼                              ▼
OpenAI Response ◄── collected text / SSE stream
```

The `Channel::duplex()` creates a pair of in-process transports. Messages sent
through one endpoint are received by the other — no I/O, no subprocess overhead.
This makes the bridge suitable as a reference architecture for wrapping any
ACP agent behind an HTTP API.

## AHP Host

A WebSocket server implementing the **Agent Host Protocol (AHP)** — the multi-client
coordination layer that sits on top of ACP agents. Built with `axum` for WebSocket
handling and `ahp-types` for wire-format compliance.

### Concepts

| Concept | Implementation |
|---|---|
| **Channels** | URI-identified resources (`ahp-root://`, `ahp-session:/<id>`, `ahp-chat:/<id>`) |
| **State** | Redux-like JSON state per channel, mutated by typed `StateAction` dispatches |
| **Actions** | Typed mutations (`chat/turnStarted`, `session/titleChanged`, etc.) from `ahp-types` |
| **Server sequencing** | Monotonic `serverSeq` on every action for ordering and gap detection |
| **Subscriptions** | Clients subscribe to channels; receive typed `Snapshot` on join, `ActionEnvelope` streams |

### Running

```bash
cargo run -p ahp-host
# Server starts on 0.0.0.0:3001 (override with BIND_ADDR env var)
```

WebSocket endpoint: `ws://localhost:3001/ws/ahp`
Health check: `http://localhost:3001/health`

### Wire format

The server speaks the same JSON-RPC wire format defined by `ahp-types`:

```
→ Client sends:     {"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}
← Server responds:  {"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"0.6.0",...}}

→ Client sends:     {"jsonrpc":"2.0","id":2,"method":"subscribe","params":{"channel":"ahp-session:/demo"}}
← Server responds:  {"jsonrpc":"2.0","id":2,"result":{"snapshot":{"resource":"ahp-session:/demo",...}}}

→ Client sends:     {"jsonrpc":"2.0","method":"dispatchAction","params":{"channel":"ahp-session:/demo","action":{...}}}
← Server broadcasts: {"jsonrpc":"2.0","method":"action","params":{"channel":"ahp-session:/demo","action":{...},"serverSeq":1}}
```

### Architecture

Since the `ahp` crate is client-only, the server implements the server side directly:

```
WebSocket Connection
    │
    ▼
Axum WS Handler ──→ JSON-RPC Router
    │                      │
    │         ┌────────────┼────────────┐
    │         ▼            ▼            ▼
    │    initialize    subscribe    dispatchAction
    │    (handshake)   (snapshot)   (reduce + broadcast)
    │         │            │            │
    │         ▼            ▼            ▼
    │    InitializeResult  Snapshot   ActionEnvelope
    │                      (typed)   (server-sequenced)
    │         │            │            │
    ▼         ▼            ▼            ▼
Client ◄── responses + broadcast actions
```

## AHP Client Demo

A demo client that connects two clients (Alice and Bob) to the AHP host,
subscribes to channels, dispatches actions, and observes broadcasts.

### Prerequisites

Start the AHP host first:

```bash
cargo run -p ahp-host
```

### Running

```bash
cargo run -p ahp-client-demo
```

### What it demonstrates

1. Two clients connect to the same AHP host via WebSocket
2. Each client initializes with `ahp-types::PROTOCOL_VERSION`
3. Both subscribe to `ahp-root://` (global state) and `ahp-session:/demo` (session state)
4. Alice dispatches a sequence of chat actions:
   - `chat/turnStarted` — begins a new turn
   - `chat/delta` — streams a text chunk
   - `chat/turnComplete` — finishes the turn
5. Both clients observe the server-sequenced action broadcasts

### Architecture

Uses the official `ahp` client SDK with `ahp-ws` WebSocket transport:

```
ahp-client-demo
    │
    ├─ Alice ──→ ahp::Client ──→ ahp_ws::WebSocketTransport ──→ ahp-host
    │                │                    │
    │            initialize           subscribe
    │            dispatch             recv actions
    │
    └─ Bob   ──→ ahp::Client ──→ ahp_ws::WebSocketTransport ──→ ahp-host
                     │                    │
                 initialize           subscribe
                 (observes only)      recv actions
```
