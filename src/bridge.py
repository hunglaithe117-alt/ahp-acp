from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from acp import Agent, Client
from acp.helpers import text_block
from acp.schema import (
    AgentMessageChunk,
    AgentPlanContentUpdate,
    AgentPlanRemovedUpdate,
    AgentPlanUpdate,
    AgentThoughtChunk,
    AvailableCommandsUpdate,
    ConfigOptionUpdate,
    CreateElicitationResponse,
    CreateTerminalResponse,
    CurrentModeUpdate,
    ElicitationMode,
    KillTerminalResponse,
    PermissionOption,
    ReleaseTerminalResponse,
    RequestPermissionResponse,
    SessionInfoUpdate,
    TerminalOutputResponse,
    ToolCallProgress,
    ToolCallStart,
    ToolCallUpdate,
    UsageUpdate,
    UserMessageChunk,
    WaitForTerminalExitResponse,
    WriteTextFileResponse,
)
from src.acp_echo_agent import EchoAgent


class AdapterClient:
    """Captures session_update calls from an ACP agent for in-process bridging."""

    _queue: asyncio.Queue[str | None]
    _agent_ref: Agent | None

    def __init__(self) -> None:
        self._queue = asyncio.Queue()
        self._agent_ref = None

    def on_connect(self, conn: Agent) -> None:
        self._agent_ref = conn

    async def session_update(
        self,
        session_id: str,
        update: UserMessageChunk
        | AgentMessageChunk
        | AgentThoughtChunk
        | ToolCallStart
        | ToolCallProgress
        | AgentPlanUpdate
        | AgentPlanContentUpdate
        | AgentPlanRemovedUpdate
        | AvailableCommandsUpdate
        | CurrentModeUpdate
        | ConfigOptionUpdate
        | SessionInfoUpdate
        | UsageUpdate,
        **kwargs: Any,
    ) -> None:
        if isinstance(update, AgentMessageChunk) and update.content.type == "text":
            await self._queue.put(update.content.text)

    async def signal_done(self) -> None:
        """Push sentinel to close the iterator."""
        await self._queue.put(None)

    async def text_chunks(self) -> AsyncIterator[str]:
        """Yield text chunks as they arrive from the agent."""
        while True:
            chunk = await self._queue.get()
            if chunk is None:
                break
            yield chunk

    async def request_permission(
        self, session_id: str, tool_call: ToolCallUpdate, options: list[PermissionOption], **kwargs: Any
    ) -> RequestPermissionResponse:
        raise NotImplementedError("request_permission not supported in bridge")

    async def write_text_file(
        self, session_id: str, path: str, content: str, **kwargs: Any
    ) -> WriteTextFileResponse | None:
        raise NotImplementedError("write_text_file not supported in bridge")

    async def read_text_file(
        self, session_id: str, path: str, line: int | None = None, limit: int | None = None, **kwargs: Any
    ) -> Any:
        raise NotImplementedError("read_text_file not supported in bridge")

    async def create_terminal(
        self,
        session_id: str,
        command: str,
        args: list[str] | None = None,
        env: Any | None = None,
        cwd: str | None = None,
        output_byte_limit: int | None = None,
        **kwargs: Any,
    ) -> CreateTerminalResponse:
        raise NotImplementedError("create_terminal not supported in bridge")

    async def terminal_output(self, session_id: str, terminal_id: str, **kwargs: Any) -> TerminalOutputResponse:
        raise NotImplementedError("terminal_output not supported in bridge")

    async def release_terminal(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> ReleaseTerminalResponse | None:
        raise NotImplementedError("release_terminal not supported in bridge")

    async def wait_for_terminal_exit(
        self, session_id: str, terminal_id: str, **kwargs: Any
    ) -> WaitForTerminalExitResponse:
        raise NotImplementedError("wait_for_terminal_exit not supported in bridge")

    async def kill_terminal(self, session_id: str, terminal_id: str, **kwargs: Any) -> KillTerminalResponse | None:
        raise NotImplementedError("kill_terminal not supported in bridge")

    async def create_elicitation(
        self, message: str, mode: ElicitationMode, **kwargs: Any
    ) -> CreateElicitationResponse:
        raise NotImplementedError("create_elicitation not supported in bridge")

    async def complete_elicitation(self, elicitation_id: str, **kwargs: Any) -> None:
        raise NotImplementedError("complete_elicitation not supported in bridge")

    async def ext_method(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError("ext_method not supported in bridge")

    async def ext_notification(self, method: str, params: dict[str, Any]) -> None:
        raise NotImplementedError("ext_notification not supported in bridge")


async def chat_stream(messages: list[dict[str, str]]) -> AsyncIterator[str]:
    """Drive the EchoAgent in-process and yield text chunks as they arrive."""
    agent = EchoAgent()
    adapter = AdapterClient()
    agent.on_connect(adapter)

    await agent.initialize(protocol_version=1)
    session = await agent.new_session(cwd="/")
    session_id = session.session_id

    prompt_blocks = [text_block(m["content"]) for m in messages if m.get("content")]
    if not prompt_blocks:
        prompt_blocks = [text_block("")]

    prompt_task = asyncio.create_task(agent.prompt(session_id=session_id, prompt=prompt_blocks))

    async for chunk in adapter.text_chunks():
        yield chunk

    await prompt_task


async def chat(messages: list[dict[str, str]]) -> str:
    """Collect all chunks into a single string (non-streaming convenience)."""
    chunks = []
    async for chunk in chat_stream(messages):
        chunks.append(chunk)
    return "".join(chunks)
