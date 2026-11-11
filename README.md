# ACP/AHP Examples

Working examples of the **Agent Communication Protocol (ACP)** and **Agent Host Protocol (AHP)** — Microsoft's standards for agent-to-client and multi-client session coordination.

```
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  ACP Agent   │◄────►│  ACP Bridge  │◄────►│  OpenAI API  │
│  (stdio)     │      │  (in-process)│      │  (port 8000) │
└──────────────┘      └──────────────┘      └──────────────┘

┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│  Client A    │◄────►│  AHP Host    │◄────►│  Client B    │
│  (WebSocket) │      │  (port 8000) │      │  (WebSocket) │
└──────────────┘      └──────────────┘      └──────────────┘
       ▲                     │                     ▲
       └─────────────────────┴─────────────────────┘
              Shared session state (Redux-like)
```

## Setup

```bash
uv sync
```

Requires Python 3.11+.

## Echo Agent

The simplest ACP agent: echoes back whatever the client sends.

```bash
uv run python src/acp_echo_agent.py
```

The agent communicates over **stdio** using JSON-RPC messages. It implements core ACP lifecycle methods:

| Method | Purpose |
|---|---|
| `initialize` | Negotiates protocol version and advertises capabilities |
| `new_session` | Creates a session and returns a session ID |
| `prompt` | Receives user content blocks, streams agent responses back |

### How it works

1. A client (e.g., VS Code with Copilot) spawns the agent as a subprocess
2. The client sends `initialize`, then `new_session`, then `prompt` messages over stdin
3. The agent echoes each prompt content block back as an `AgentMessageChunk` via `session_update`
4. The response is streamed to the client in real-time

### ACP concepts demonstrated

- **Agent protocol**: implementing the `Agent` interface from the `acp` SDK
- **Connection lifecycle**: `on_connect` receives the client connection for bidirectional messaging
- **Session management**: creating and tracking sessions with unique IDs
- **Streaming responses**: using `session_update` to push message chunks to the client
- **Content blocks**: constructing `TextContentBlock` and wrapping them in `AgentMessageChunk`

## OpenAI-Compatible REST Bridge

A FastAPI server that exposes an OpenAI-compatible `/v1/chat/completions` endpoint backed by the ACP echo agent. Any tool that speaks the OpenAI API (LangChain, LlamaIndex, OpenAI SDK, etc.) can talk to an ACP agent through this bridge.

### Start the server

```bash
uv run python -m src.openai_server
```

The server listens on `http://localhost:8000`.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Health check |
| `GET` | `/v1/models` | List available models |
| `POST` | `/v1/chat/completions` | Chat completion (supports streaming) |

### Example — curl

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "acp-echo",
    "messages": [{"role": "user", "content": "Hello from ACP!"}]
  }'
```

### Example — OpenAI Python SDK

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8000/v1", api_key="not-needed")

response = client.chat.completions.create(
    model="acp-echo",
    messages=[{"role": "user", "content": "Hello from ACP!"}],
)
print(response.choices[0].message.content)
```

### Streaming

Set `"stream": true` in the request body to receive Server-Sent Events (SSE):

```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "acp-echo",
    "messages": [{"role": "user", "content": "Hello!"}],
    "stream": true
  }'
```

### Architecture

The bridge works without spawning subprocesses or setting up JSON-RPC transports. It calls the ACP agent's methods directly in-process:

1. An `AdapterClient` implements the `Client` protocol and captures `session_update` calls
2. The `EchoAgent` is instantiated and connected to the adapter via `on_connect`
3. `initialize()`, `new_session()`, and `prompt()` are called directly on the agent
4. The adapter collects the text from `AgentMessageChunk` updates
5. The server formats the result as an OpenAI-compatible response

```
HTTP Request → FastAPI → bridge.chat() → EchoAgent → AdapterClient → HTTP Response
```

## AHP Multi-Client Host

A simplified Python implementation of AHP (Agent Host Protocol) concepts: WebSocket-based multi-client session coordination with shared, immutable state.

### Start the server

The AHP host runs on the same FastAPI server as the OpenAI bridge (port 8000):

```bash
uv run python -m src.openai_server
```

The WebSocket endpoint is available at `ws://localhost:8000/ws/ahp`.

### Connect multiple clients

Run the built-in demo client to see two clients (Alice and Bob) connect to the same session:

```bash
uv run python -m src.ahp_client
```

This demonstrates:
- Both clients subscribe to the same session channel
- Each client sees the shared state snapshot
- Actions dispatched by one client are broadcast to all subscribers
- State updates are server-sequenced with monotonic `serverSeq` numbers

### AHP concepts demonstrated

| Concept | Implementation |
|---|---|
| **Channels** | URI-identified resources (`ahp-session:/<id>`, `ahp-chat:/<id>`) |
| **State** | Immutable Redux-like dict per channel, mutated only by actions |
| **Actions** | Typed mutations (`chat/turnStarted`, `chat/turnCompleted`, etc.) |
| **Server sequencing** | Monotonic `serverSeq` on every action for ordering and gap detection |
| **Subscriptions** | Clients subscribe to channels; receive snapshots on join, actions as they happen |
| **Multi-client coordination** | N clients observe and participate in the same session state |

### WebSocket message format

```json
// Subscribe to a session
→ {"method": "subscribe", "params": {"channel": "ahp-session:/demo"}}

// Receive current state
← {"method": "snapshot", "params": {"channel": "ahp-session:/demo", "state": {...}, "serverSeq": 0}}

// Dispatch a chat action
→ {"method": "dispatchAction", "params": {
    "channel": "ahp-chat:/demo",
    "action": {"type": "chat/turnStarted", "payload": {"text": "Hello", "origin": "user"}}
}}

// Broadcast to all subscribers
← {"method": "action", "params": {
    "channel": "ahp-chat:/demo",
    "action": {"type": "chat/turnStarted", "payload": {...}, "serverSeq": 1},
    "serverSeq": 1
}}
```

### Session state shape

```json
{
  "title": "New Session",
  "sessionId": "demo",
  "status": "ready",
  "chats": ["ahp-chat:/demo"],
  "activeClients": ["uuid-1", "uuid-2"]
}
```

## Rust Examples

The [`rust/`](./rust/) directory contains Rust implementations of both ACP and AHP:

| Crate | Description |
|---|---|
| `acp-echo-agent` | ACP echo agent binary (stdio) using `agent-client-protocol` v2.0 |
| `acp-openai-bridge` | Axum HTTP server bridging OpenAI format to ACP agent |
| `ahp-host` | AHP WebSocket server using `ahp-types` v0.6 wire format |
| `ahp-client-demo` | Demo client using the official `ahp` + `ahp-ws` crates |

```bash
cd rust && cargo build
```

See [`rust/README.md`](./rust/README.md) for details on each crate.

## Dependencies

- `agent-client-protocol` — the official ACP Python SDK
- `fastapi` / `uvicorn` — HTTP server for the OpenAI-compatible bridge and AHP WebSocket host
- `pydantic` — request/response validation
- `websockets` — async WebSocket client for the AHP demo
