# ACP/AHP Examples

Runnable examples of the **Agent Communication Protocol (ACP)** and the **Agent Host
Protocol (AHP)**, in Python and Rust.

| Path | Contents |
|---|---|
| `src/` | Python: ACP echo agent, OpenAI-compatible HTTP bridge, AHP WebSocket host, demo client |
| `rust/` | Rust: `acp-echo-agent`, `acp-openai-bridge`, `ahp-host`, `ahp-client-demo` (see [`rust/README.md`](./rust/README.md)) |

## Setup

```bash
uv sync          # Python 3.11+, creates .venv from uv.lock
```

```bash
cd rust && cargo build   # Rust with edition 2024 support
```

## Python — Echo Agent

The smallest ACP agent: it echoes back every text content block it receives.

```bash
uv run python src/acp_echo_agent.py
```

The agent speaks JSON-RPC over **stdio** and runs until stdin closes. It overrides three
`acp.Agent` methods:

| Method | Behavior |
|---|---|
| `initialize` | Returns `InitializeResponse` with the requested protocol version |
| `new_session` | Returns a `NewSessionResponse` with the fixed ID `session-1` |
| `prompt` | Emits each text block back as an `AgentMessageChunk` via `session_update`, then returns `stop_reason="end_turn"` |

`EchoAgent.on_connect(conn)` stores the client connection, which is the object used to push
`session_update` notifications while a prompt is in flight. Non-text blocks (image, audio,
resource, embedded resource) are accepted by the signature and skipped.

## Python — OpenAI-Compatible REST Bridge

`src/openai_server.py` is a FastAPI app that exposes an OpenAI-shaped HTTP API in front of
the ACP echo agent. It also hosts the AHP WebSocket endpoint described below.

```bash
uv run python -m src.openai_server   # http://localhost:8000
```

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Returns `{"status": "ok"}` |
| `GET` | `/v1/models` | Lists the single model `acp-echo` |
| `POST` | `/v1/chat/completions` | Chat completion, streaming or not |
| `WS` | `/ws/ahp` | AHP multi-client session coordination |

Request body (`ChatCompletionRequest`): `model` (default `acp-echo`), `messages`
(`role`, `content`), `stream` (default `false`), `temperature`, `max_tokens`.
Only `messages` is required.

Errors:

- `400` — body is not valid JSON, or `messages` is empty
- `422` — body fails `ChatCompletionRequest` validation

### curl

```bash
curl http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "acp-echo", "messages": [{"role": "user", "content": "Hello from ACP!"}]}'
```

### OpenAI Python SDK

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

With `"stream": true` the endpoint returns `text/event-stream` chunks:

1. a chunk carrying `delta.role = "assistant"`
2. one chunk per text chunk produced by the agent
3. a chunk with `finish_reason = "stop"`
4. the `data: [DONE]` sentinel

```bash
curl -N http://localhost:8000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model": "acp-echo", "messages": [{"role": "user", "content": "Hello!"}], "stream": true}'
```

### How the bridge works

No subprocess and no stdio: the agent runs inside the FastAPI process.

```
HTTP request → openai_server.chat()/chat_stream() → bridge.py
             → EchoAgent (in-process ACP agent)
             → AdapterClient.session_update() → text queue → response / SSE
```

`src/bridge.py` connects `EchoAgent` to an `AdapterClient` that implements the ACP `Client`
side. `chat_stream()` calls `initialize(protocol_version=1)`, `new_session(cwd="/")`, and
`prompt()` directly, yielding text from the adapter's queue as it arrives. `chat()` joins
those chunks. `AdapterClient` implements the rest of the `Client` surface (terminals, file
access, permissions, elicitation, extensions) as `NotImplementedError` — the echo agent never
calls them.

## Python — AHP Host

`src/ahp_host.py` implements the host side of an AHP-style coordination layer: URI-addressed
channels, Redux-like state, and server-sequenced actions broadcast to subscribers. It is a
plain WebSocket protocol (not JSON-RPC) served on the same FastAPI app at
`ws://localhost:8000/ws/ahp`.

### Channels and state

Channel state is created on first subscribe:

| Channel URI | Initial state |
|---|---|
| `ahp-session:/<id>` | `{"title": "New Session", "sessionId": "<id>", "status": "ready", "chats": [], "activeClients": []}` |
| `ahp-chat:/<id>` | `{"title": "New Chat", "chatId": "<id>", "sessionId": "<id>", "turns": [], "status": "active"}` |
| anything else | `{}` |

Subscribing to a session channel does not add chats to `chats`; the server never appends to
`chats` or `activeClients` on its own. Those lists change only through actions.

### Messages

All messages are JSON objects with a `method` and `params`. A `id` field is optional; when
present, the host replies with a matching `result` object in addition to any broadcast.

```jsonc
// subscribe → snapshot of the current state
→ {"method": "subscribe", "params": {"channel": "ahp-session:/demo"}, "id": 1}
← {"method": "snapshot", "params": {"channel": "ahp-session:/demo", "state": {...}, "serverSeq": 0}}
← {"id": 1, "result": {"subscribed": true}}

// unsubscribe
→ {"method": "unsubscribe", "params": {"channel": "ahp-session:/demo"}, "id": 2}
← {"id": 2, "result": {"unsubscribed": true}}

// dispatchAction → the action is stamped with serverSeq, reduced into state, broadcast
→ {"method": "dispatchAction", "params": {
     "channel": "ahp-chat:/demo",
     "action": {"type": "chat/turnStarted", "payload": {"text": "Hello", "origin": "user"}}
   }, "id": 3}
← {"method": "action", "params": {
     "channel": "ahp-chat:/demo",
     "action": {"type": "chat/turnStarted", "payload": {...}, "serverSeq": 1},
     "serverSeq": 1
   }}
← {"id": 3, "result": {"serverSeq": 1}}
```

Unparseable JSON answers `{"error": "Invalid JSON"}`; an unknown method answers
`{"error": "Unknown method: <name>"}`. Both include `id` when the request carried one.

### Actions

`_apply_action` is the reducer. Every action produces a fresh state object; `serverSeq` is
attached to the action, not stored in a dedicated state field.

| Action type | Effect |
|---|---|
| `chat/turnStarted` | Appends a turn `{id: "turn-<8 hex>", message: {text, origin}, responseParts: [], state: "in_progress"}` to `turns` |
| `chat/responseChunk` | Appends the payload to `responseParts` of the most recent in-progress turn |
| `chat/turnCompleted` | Sets `responseParts` and `state: "complete"` on the most recent in-progress turn |
| `session/titleChanged` | Sets `title` from `payload.title` |
| `session/chatAdded` | Appends `payload.chatUri` to `chats` if not already present |
| `session/clientJoined` | Appends `payload.clientId` to `activeClients` if not already present |
| `session/clientLeft` | Removes `payload.clientId` from `activeClients` |

The reducer is shared by session and chat state: it operates on whatever keys exist and
creates `turns` when an action needs it.

On disconnect, the host removes the client from every subscribed channel and, for
`ahp-session:/` channels, dispatches `session/clientLeft` with a new `serverSeq` so other
subscribers observe the departure.

### Demo client

```bash
uv run python -m src.ahp_client   # requires the server from src.openai_server
```

`src/ahp_client.py` opens two WebSocket clients (Alice and Bob) against
`ws://localhost:8000/ws/ahp`. Each subscribes to `ahp-session:/demo` and `ahp-chat:/demo`
(consuming one snapshot per subscription), dispatches one `chat/turnStarted` action, logs the
broadcast it receives, then reads further updates with a 2 second per-message timeout before
exiting.

## Rust

Four crates in a Cargo workspace under [`rust/`](./rust/), covering the same ground plus a
typed AHP server and an SDK-based demo client:

| Crate | Listens on | Description |
|---|---|---|
| `acp-echo-agent` | stdio | ACP echo agent on `agent-client-protocol` 2.0 |
| `acp-openai-bridge` | `0.0.0.0:3000` (`BIND_ADDR`) | Axum server in front of an in-process ACP agent |
| `ahp-host` | `0.0.0.0:3001` (`BIND_ADDR`) | JSON-RPC WebSocket server built on `ahp-types` 0.6 |
| `ahp-client-demo` | — | Demo client using `ahp` + `ahp-ws`, expects the host on `ws://localhost:3001/ws/ahp` |

```bash
cd rust && cargo build
```

See [`rust/README.md`](./rust/README.md) for endpoints, message formats, and per-crate
details.

## Python Dependencies

| Package | Used for |
|---|---|
| `agent-client-protocol` | ACP agent and client interfaces, schema types, stdio transport |
| `fastapi` / `uvicorn` | HTTP endpoints, WebSocket endpoint, server process |
| `pydantic` | Request and response models for the OpenAI-compatible API |
| `websockets` | WebSocket client in `src/ahp_client.py` |
| `httpx` (dev) | HTTP client for local testing |
