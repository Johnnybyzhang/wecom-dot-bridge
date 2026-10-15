"""Push notifications for dot rooms over ChatGPT's per-user socket.

Protocol as implemented by pi-plugins (pi-desk/src/host/dot-live.ts) and corroborated
by beeper/chatgpt-dots (pkg/chatgpt/signals.go); both are unofficial. Events only
*trigger* authoritative REST reads; nothing here is treated as message content.
The socket URL carries authentication material and is never logged.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from urllib.parse import urlsplit

from .openai_auth import AuthRequired

log = logging.getLogger(__name__)

TOPIC = "calpico-chatgpt-messaging"
ROOM_EVENTS = frozenset({"calpico-message-add", "calpico-message-update", "calpico-room-update",
                         "calpico-room-metadata-update"})
SUBSCRIBE_DEADLINE = 15.0
HEARTBEAT_SECONDS = 30.0
MAX_FRAME_BYTES = 8 * 1024 * 1024
AUTH_RETRY_SECONDS = 30.0
_PATH = re.compile(r"/p\d+/ws/user/[^/]+\Z")


def valid_socket_url(url) -> bool:
    if not isinstance(url, str):
        return False
    parts = urlsplit(url)
    return (parts.scheme == "wss" and parts.hostname == "ws.chatgpt.com"
            and parts.username is None and parts.port is None and bool(_PATH.match(parts.path)))


class DotLive:
    """Keeps one subscription alive; calls `on_event(room_id)` / `on_resync()`."""

    def __init__(self, dot, on_event, on_resync, *, validate=valid_socket_url):
        self.dot, self.on_event, self.on_resync, self.validate = dot, on_event, on_resync, validate
        self.connected = False
        self.offset: str | None = None
        self._attempts = 0

    def commands(self) -> str:
        subscribe = {"type": "subscribe", "topic_id": TOPIC}
        if self.offset:
            subscribe["offset"] = self.offset
        return json.dumps([
            {"id": 1, "command": {"type": "connect",
                                  "presence": {"type": "presence", "state": "background"}}},
            {"id": 2, "command": subscribe}])

    def handle(self, raw) -> bool:
        """Process one frame (a JSON array). Returns True once the subscription is confirmed."""
        try:
            rows = json.loads(raw)
        except (TypeError, ValueError):
            return False
        if not isinstance(rows, list):
            return False
        subscribed = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            reply = row.get("reply") if isinstance(row.get("reply"), dict) else {}
            if (row.get("id") == 2 and reply.get("type") == "subscribe"
                    and reply.get("topic_id") == TOPIC):
                subscribed = True
                if reply.get("recovered") is True and isinstance(reply.get("catchups"), list):
                    for event in reply["catchups"]:
                        self._event(event)
                if isinstance(reply.get("last_offset"), str):
                    self.offset = reply["last_offset"]
                # Replay retention is not guaranteed: always resync from REST after (re)subscribe.
                self.on_resync()
            else:
                self._event(row)
        return subscribed

    def _event(self, row) -> None:
        if not isinstance(row, dict) or row.get("type") != "message" or row.get("topic_id") != TOPIC:
            return
        if isinstance(row.get("offset"), str):
            self.offset = row["offset"]
        event = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        kind = event.get("type")
        if kind not in ROOM_EVENTS:
            return  # typing heartbeats, read receipts, other rooms' noise
        room = payload.get("room") if kind == "calpico-room-update" else None
        room_id = room.get("id") if isinstance(room, dict) else payload.get("room_id")
        if isinstance(room_id, str):
            self.on_event(room_id)

    async def run(self) -> None:
        import aiohttp
        while True:
            try:
                url = await self.dot.live_url()
                if not self.validate(url):
                    log.error("unexpected live socket origin; live updates disabled for now")
                    raise ConnectionError("bad socket url")
                async with aiohttp.ClientSession(trust_env=False) as session:
                    ws = await asyncio.wait_for(
                        session.ws_connect(url, heartbeat=HEARTBEAT_SECONDS, autoping=True,
                                           max_msg_size=MAX_FRAME_BYTES), SUBSCRIBE_DEADLINE)
                    try:
                        await ws.send_str(self.commands())
                        await self._listen(ws, aiohttp)
                    finally:
                        await ws.close()
            except asyncio.CancelledError:
                self.connected = False
                raise
            except AuthRequired:
                self.connected = False
                await asyncio.sleep(AUTH_RETRY_SECONDS)
                continue
            except Exception as exc:
                log.info("live updates disconnected: %s", type(exc).__name__)
            was_connected, self.connected = self.connected, False
            if was_connected:
                self._attempts = 0
                self.on_resync()  # read now, and let the poller fall back to its normal cadence
            delay = min(30.0, 2 ** min(self._attempts, 5)) * (0.8 + random.random() * 0.4)
            self._attempts += 1
            await asyncio.sleep(delay)

    async def _listen(self, ws, aiohttp) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + SUBSCRIBE_DEADLINE
        while True:
            timeout = None if self.connected else max(0.1, deadline - loop.time())
            msg = await asyncio.wait_for(ws.receive(), timeout)
            if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                if self.handle(msg.data) and not self.connected:
                    self.connected = True
                    log.info("live updates subscribed")
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                              aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                raise ConnectionError("live socket closed")
