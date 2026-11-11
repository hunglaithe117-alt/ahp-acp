from __future__ import annotations

import asyncio
from typing import Any

from acp import (
    Agent,
    Client,
    InitializeResponse,
    NewSessionResponse,
    PromptResponse,
    run_agent,
    update_agent_message_text,
)
from acp.schema import (
    AudioContentBlock,
    EmbeddedResourceContentBlock,
    ImageContentBlock,
    ResourceContentBlock,
    TextContentBlock,
)


class EchoAgent(Agent):
    """Minimal ACP agent that echoes back prompt content."""

    _conn: Client

    def on_connect(self, conn: Client) -> None:
        self._conn = conn

    async def initialize(
        self,
        protocol_version: int,
        client_capabilities: Any = None,
        client_info: Any = None,
        **kwargs: Any,
    ) -> InitializeResponse:
        return InitializeResponse(protocol_version=protocol_version)

    async def new_session(self, cwd: str, **kwargs: Any) -> NewSessionResponse:
        return NewSessionResponse(session_id="session-1")

    async def prompt(
        self,
        session_id: str,
        prompt: list[TextContentBlock | ImageContentBlock | AudioContentBlock
                     | ResourceContentBlock | EmbeddedResourceContentBlock],
        **kwargs: Any,
    ) -> PromptResponse:
        for block in prompt:
            if block.type != "text":
                continue  # minimal echo only handles text blocks
            await self._conn.session_update(
                session_id=session_id,
                update=update_agent_message_text(block.text),
            )
        return PromptResponse(stop_reason="end_turn")


async def main() -> None:
    await run_agent(EchoAgent())


if __name__ == "__main__":
    asyncio.run(main())
