from __future__ import annotations

import copy
import json
import logging
import uuid
from dataclasses import dataclass, field

from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)


@dataclass
class Channel:
    """A subscribable resource with immutable state mutated by actions.

    Each channel maintains a Redux-like state tree. Actions are the sole
    mechanism for state change; the server assigns a monotonic sequence
    number to every action so clients can detect ordering and gaps.
    """

    uri: str
    state: dict = field(default_factory=dict)
    subscribers: set[WebSocket] = field(default_factory=set)
    action_seq: int = 0

    def next_seq(self) -> int:
        self.action_seq += 1
        return self.action_seq


@dataclass
class ClientInfo:
    """Tracks a connected WebSocket client and its subscriptions."""

    client_id: str
    ws: WebSocket
    subscriptions: set[str] = field(default_factory=set)


def _initial_session_state(session_id: str) -> dict:
    return {
        "title": "New Session",
        "sessionId": session_id,
        "status": "ready",
        "chats": [],
        "activeClients": [],
    }


def _initial_chat_state(chat_id: str, session_id: str) -> dict:
    return {
        "title": "New Chat",
        "chatId": chat_id,
        "sessionId": session_id,
        "turns": [],
        "status": "active",
    }


def _apply_action(state: dict, action: dict) -> dict:
    """Produce a new state dict by applying the given action (Redux-style).

    This is a simplified reducer — each action type mutates state immutably
    by returning a fresh dict rather than modifying the original.
    """
    state = copy.deepcopy(state)
    action_type = action.get("type", "")
    payload = action.get("payload", {})

    # Chat-level actions operate on state shapes that include a "turns" list
    # (ChatState). Session-level state has no "turns" — these actions are
    # applied regardless; the reducer is resilient to missing keys.
    turns = state.setdefault("turns", [])

    if action_type == "chat/turnStarted":
        turn = {
            "id": f"turn-{uuid.uuid4().hex[:8]}",
            "message": {
                "text": payload.get("text", ""),
                "origin": payload.get("origin", "user"),
            },
            "responseParts": [],
            "state": "in_progress",
        }
        turns.append(turn)

    elif action_type == "chat/turnCompleted":
        for turn in reversed(turns):
            if turn["state"] == "in_progress":
                turn["responseParts"] = payload.get("responseParts", [])
                turn["state"] = "complete"
                break

    elif action_type == "chat/responseChunk":
        for turn in reversed(turns):
            if turn["state"] == "in_progress":
                turn["responseParts"].append(payload)
                break

    elif action_type == "session/titleChanged":
        state["title"] = payload.get("title", state.get("title", ""))

    elif action_type == "session/chatAdded":
        chat_uri = payload.get("chatUri", "")
        chats = state.setdefault("chats", [])
        if chat_uri and chat_uri not in chats:
            chats.append(chat_uri)

    elif action_type == "session/clientJoined":
        client_id = payload.get("clientId", "")
        active = state.setdefault("activeClients", [])
        if client_id and client_id not in active:
            active.append(client_id)

    elif action_type == "session/clientLeft":
        client_id = payload.get("clientId", "")
        active = state.get("activeClients", [])
        state["activeClients"] = [c for c in active if c != client_id]

    return state


class AHPHost:
    """Simplified AHP host managing channels, state, and multi-client subscriptions.

    Demonstrates core AHP concepts:
    - URI-identified channels (ahp-session:/<id>, ahp-chat:/<id>)
    - Immutable Redux-style state per channel
    - Server-sequenced actions as the sole state mutation mechanism
    - Multi-client subscription with snapshot and action broadcasting
    """

    def __init__(self) -> None:
        self._channels: dict[str, Channel] = {}
        self._clients: dict[str, ClientInfo] = {}

    def _get_or_create_channel(self, uri: str, initial_state: dict | None = None) -> Channel:
        if uri not in self._channels:
            if initial_state is None:
                if uri.startswith("ahp-session:/"):
                    session_id = uri.split("/")[-1]
                    initial_state = _initial_session_state(session_id)
                elif uri.startswith("ahp-chat:/"):
                    chat_id = uri.split("/")[-1]
                    initial_state = _initial_chat_state(chat_id, chat_id)
                else:
                    initial_state = {}
            self._channels[uri] = Channel(uri=uri, state=initial_state)
        return self._channels[uri]

    async def handle_client(self, client_id: str, websocket: WebSocket) -> None:
        """Main loop for a connected WebSocket client."""
        info = ClientInfo(client_id=client_id, ws=websocket)
        self._clients[client_id] = info
        logger.info("Client %s connected", client_id)

        try:
            while True:
                raw = await websocket.receive_text()
                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    await self._send_error(websocket, "Invalid JSON")
                    continue

                method = msg.get("method", "")
                params = msg.get("params", {})
                req_id = msg.get("id")

                if method == "subscribe":
                    await self._handle_subscribe(info, params, req_id)
                elif method == "unsubscribe":
                    await self._handle_unsubscribe(info, params, req_id)
                elif method == "dispatchAction":
                    await self._handle_dispatch_action(info, params, req_id)
                else:
                    await self._send_error(websocket, f"Unknown method: {method}", req_id)

        except WebSocketDisconnect:
            logger.info("Client %s disconnected", client_id)
        finally:
            await self._cleanup_client(info)

    async def _handle_subscribe(self, info: ClientInfo, params: dict, req_id: str | None) -> None:
        uri = params.get("channel", "")
        if not uri:
            await self._send_error(info.ws, "Missing channel URI", req_id)
            return

        channel = self._get_or_create_channel(uri)
        channel.subscribers.add(info.ws)
        info.subscriptions.add(uri)

        await self._send_to(info.ws, {
            "method": "snapshot",
            "params": {
                "channel": uri,
                "state": channel.state,
                "serverSeq": channel.action_seq,
            },
        })
        if req_id:
            await self._send_to(info.ws, {"id": req_id, "result": {"subscribed": True}})

        logger.info("Client %s subscribed to %s", info.client_id, uri)

    async def _handle_unsubscribe(self, info: ClientInfo, params: dict, req_id: str | None) -> None:
        uri = params.get("channel", "")
        if uri in self._channels:
            self._channels[uri].subscribers.discard(info.ws)
        info.subscriptions.discard(uri)

        if req_id:
            await self._send_to(info.ws, {"id": req_id, "result": {"unsubscribed": True}})

    async def _handle_dispatch_action(self, info: ClientInfo, params: dict, req_id: str | None) -> None:
        uri = params.get("channel", "")
        action = params.get("action", {})

        if not uri or not action:
            await self._send_error(info.ws, "Missing channel or action", req_id)
            return

        channel = self._get_or_create_channel(uri)
        server_seq = channel.next_seq()
        action_with_seq = {**action, "serverSeq": server_seq}

        channel.state = _apply_action(channel.state, action_with_seq)

        await self._broadcast_action(channel, action_with_seq, server_seq)

        if req_id:
            await self._send_to(info.ws, {"id": req_id, "result": {"serverSeq": server_seq}})

    async def _broadcast_action(self, channel: Channel, action: dict, server_seq: int) -> None:
        """Send the action to every subscriber of the channel."""
        message = {
            "method": "action",
            "params": {
                "channel": channel.uri,
                "action": action,
                "serverSeq": server_seq,
            },
        }
        stale: list[WebSocket] = []
        for ws in channel.subscribers:
            try:
                await ws.send_json(message)
            except Exception:
                stale.append(ws)
        for ws in stale:
            channel.subscribers.discard(ws)

    async def _send_to(self, ws: WebSocket, msg: dict) -> None:
        try:
            await ws.send_json(msg)
        except Exception:
            pass

    async def _send_error(self, ws: WebSocket, message: str, req_id: str | None = None) -> None:
        payload: dict = {"error": message}
        if req_id:
            payload["id"] = req_id
        await self._send_to(ws, payload)

    async def _cleanup_client(self, info: ClientInfo) -> None:
        """Remove client from all channels and notify remaining subscribers."""
        for uri in list(info.subscriptions):
            if uri in self._channels:
                ch = self._channels[uri]
                ch.subscribers.discard(info.ws)
                # Only session channels track activeClients
                if uri.startswith("ahp-session:/"):
                    leave_action = {
                        "type": "session/clientLeft",
                        "payload": {"clientId": info.client_id},
                        "serverSeq": ch.next_seq(),
                    }
                    ch.state = _apply_action(ch.state, leave_action)
                    await self._broadcast_action(ch, leave_action, leave_action["serverSeq"])

        info.subscriptions.clear()
        self._clients.pop(info.client_id, None)
        logger.info("Client %s cleaned up", info.client_id)
