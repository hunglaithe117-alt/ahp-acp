from __future__ import annotations

import asyncio
import json
import logging
import sys

import websockets

logger = logging.getLogger(__name__)


async def connect_and_demo(uri: str, session_id: str, client_name: str) -> None:
    """Connect to an AHP host, subscribe to a session, and send a chat message."""
    async with websockets.connect(uri) as ws:
        session_channel = f"ahp-session:/{session_id}"
        chat_channel = f"ahp-chat:/{session_id}"

        # Subscribe to the session and chat channels
        for ch in (session_channel, chat_channel):
            await ws.send(json.dumps({
                "method": "subscribe",
                "params": {"channel": ch},
            }))
            # Consume snapshot for each subscription
            await ws.recv()
        logger.info("[%s] Subscribed to %s and %s", client_name, session_channel, chat_channel)

        # Dispatch a chat message
        await ws.send(json.dumps({
            "method": "dispatchAction",
            "params": {
                "channel": chat_channel,
                "action": {
                    "type": "chat/turnStarted",
                    "payload": {"text": f"Hello from {client_name}!", "origin": "user"},
                },
            },
        }))
        logger.info("[%s] Dispatched chat/turnStarted", client_name)

        # Read the action broadcast
        action_msg = json.loads(await ws.recv())
        logger.info(
            "[%s] Received action: type=%s, serverSeq=%d",
            client_name,
            action_msg["params"]["action"]["type"],
            action_msg["params"]["serverSeq"],
        )

        # Listen for any further updates for a short time
        try:
            for _ in range(5):
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                data = json.loads(msg)
                logger.info("[%s] Update: %s", client_name, data["method"])
        except asyncio.TimeoutError:
            pass

        logger.info("[%s] Demo complete", client_name)


async def main() -> None:
    """Run two clients concurrently to demonstrate multi-client state sync."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )

    ws_uri = "ws://localhost:8000/ws/ahp"

    async with asyncio.TaskGroup() as tg:
        tg.create_task(connect_and_demo(ws_uri, "demo", "Alice"))
        tg.create_task(connect_and_demo(ws_uri, "demo", "Bob"))


if __name__ == "__main__":
    asyncio.run(main())
