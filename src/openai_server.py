from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncGenerator

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse

from src.ahp_host import AHPHost
from src.bridge import chat, chat_stream

app = FastAPI(title="ACP OpenAI Bridge", version="0.1.0")

ahp_host = AHPHost()

MODEL_CREATED = int(time.time())


# -- OpenAI-compatible request / response schemas --


class ChatMessage(BaseModel):
    role: str
    content: str | None = None


class ChatCompletionRequest(BaseModel):
    model: str = "acp-echo"
    messages: list[ChatMessage]
    stream: bool = False
    temperature: float | None = None
    max_tokens: int | None = None


class Usage(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChoiceMessage(BaseModel):
    role: str = "assistant"
    content: str


class Choice(BaseModel):
    index: int = 0
    message: ChoiceMessage
    finish_reason: str | None = "stop"


class ChatCompletionResponse(BaseModel):
    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: list[Choice]
    usage: Usage = Usage()


class ChoiceDelta(BaseModel):
    role: str | None = None
    content: str | None = None


class StreamChoice(BaseModel):
    index: int = 0
    delta: ChoiceDelta
    finish_reason: str | None = None


class ChatCompletionChunk(BaseModel):
    id: str
    object: str = "chat.completion.chunk"
    created: int
    model: str
    choices: list[StreamChoice]


# -- Endpoints --


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/v1/models")
async def list_models() -> dict[str, object]:
    return {
        "object": "list",
        "data": [
            {
                "id": "acp-echo",
                "object": "model",
                "created": MODEL_CREATED,
                "owned_by": "acp-bridge",
            }
        ],
    }


@app.post("/v1/chat/completions", response_model=None)
async def chat_completions(request: Request) -> JSONResponse | StreamingResponse:
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    try:
        req = ChatCompletionRequest.model_validate(body)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    if not req.messages:
        raise HTTPException(status_code=400, detail="messages must not be empty")

    messages_payload = [{"role": m.role, "content": m.content or ""} for m in req.messages]

    if req.stream:
        async def safe_stream() -> AsyncGenerator[str, None]:
            try:
                async for chunk in _stream_chunks(req.model, messages_payload):
                    yield chunk
            except asyncio.CancelledError:
                pass  # Client disconnected; ACP session will be garbage collected

        return StreamingResponse(safe_stream(), media_type="text/event-stream")

    response_text = await chat(messages_payload)
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    response = ChatCompletionResponse(
        id=completion_id,
        created=MODEL_CREATED,
        model=req.model,
        choices=[Choice(message=ChoiceMessage(content=response_text))],
    )
    return JSONResponse(content=response.model_dump())


async def _stream_chunks(model: str, messages: list[dict[str, str]]) -> AsyncGenerator[str, None]:
    completion_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())

    # First chunk: set role
    first_chunk = ChatCompletionChunk(
        id=completion_id,
        created=created,
        model=model,
        choices=[StreamChoice(delta=ChoiceDelta(role="assistant"))],
    )
    yield f"data: {json.dumps(first_chunk.model_dump(exclude_none=True))}\n\n"

    # Content chunks from real-time streaming
    async for text in chat_stream(messages):
        chunk = ChatCompletionChunk(
            id=completion_id,
            created=created,
            model=model,
            choices=[StreamChoice(delta=ChoiceDelta(content=text))],
        )
        yield f"data: {json.dumps(chunk.model_dump(exclude_none=True))}\n\n"

    # Final chunk: stop
    done_chunk = ChatCompletionChunk(
        id=completion_id,
        created=created,
        model=model,
        choices=[StreamChoice(delta=ChoiceDelta(), finish_reason="stop")],
    )
    yield f"data: {json.dumps(done_chunk.model_dump(exclude_none=True))}\n\n"
    yield "data: [DONE]\n\n"


@app.websocket("/ws/ahp")
async def ahp_websocket(websocket: WebSocket) -> None:
    """WebSocket endpoint for AHP-style multi-client session coordination."""
    await websocket.accept()
    client_id = str(uuid.uuid4())
    try:
        await ahp_host.handle_client(client_id, websocket)
    except WebSocketDisconnect:
        pass


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
