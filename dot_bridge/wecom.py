"""Minimal WeCom AI-bot client over the official openws WebSocket gateway.

Wire protocol, reconnect/kick handling and QR enrollment are adapted from the
Hermes Agent WeCom adapter (MIT, Copyright (c) 2025 Nous Research); see
THIRD_PARTY_NOTICES.md. Only text in, markdown out; no media.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import re
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

WS_URL = "wss://openws.work.weixin.qq.com"
CMD_SUBSCRIBE = "aibot_subscribe"
CMD_CALLBACK = {"aibot_msg_callback", "aibot_callback"}
CMD_EVENT = "aibot_event_callback"
CMD_PING = "ping"
CMD_SEND = "aibot_send_msg"
CMD_RESPOND = "aibot_respond_msg"
CMD_UPLOAD_INIT = "aibot_upload_media_init"
CMD_UPLOAD_CHUNK = "aibot_upload_media_chunk"
CMD_UPLOAD_FINISH = "aibot_upload_media_finish"
UPLOAD_CHUNK_BYTES = 512 * 1024
MAX_UPLOAD_CHUNKS = 100
IMAGE_MAX_BYTES = 10 * 1024 * 1024
IMAGE_MIMES = frozenset({"image/png", "image/jpeg", "image/gif"})
MAX_CHUNK_CHARS = 4000
MAX_CHUNK_BYTES = 12000
HEARTBEAT_SECONDS = 30.0
TIMEOUT_SECONDS = 20.0
STABLE_SECONDS = 60.0
RECONNECT_BACKOFF = (2, 5, 10, 30, 60)
MIN_SEND_SPACING = 2.1  # WeCom limit: 30 messages/minute/chat

OK, REJECTED, UNAVAILABLE = "ok", "rejected", "unavailable"

QR_GENERATE_URL = "https://work.weixin.qq.com/ai/qc/generate"
QR_QUERY_URL = "https://work.weixin.qq.com/ai/qc/query_result"
QR_PAGE_URL = "https://work.weixin.qq.com/ai/qc/gen?source=hermes&scode="


@dataclass(frozen=True)
class Inbound:
    msgid: str
    req_id: str
    chat_id: str
    sender: str
    is_group: bool
    msgtype: str
    text: str = field(repr=False)
    media: tuple = field(default=(), repr=False)  # ({"kind", "url", "aeskey"}, ...)
    quote: dict | None = field(default=None, repr=False)  # {"type", "text"} of a quoted message


def _content(container: dict, key: str) -> str:
    value = container.get(key)
    return str(value.get("content") or "").strip() if isinstance(value, dict) else ""


def parse_callback(payload: dict) -> Inbound | None:
    body = payload.get("body")
    if not isinstance(body, dict):
        return None
    headers = payload.get("headers") if isinstance(payload.get("headers"), dict) else {}
    req_id = str(headers.get("req_id") or "")
    sender = body.get("from") if isinstance(body.get("from"), dict) else {}
    sender_id = str(sender.get("userid") or "").strip()
    msgid = str(body.get("msgid") or req_id).strip()
    chat_id = str(body.get("chatid") or sender_id).strip()
    if not msgid or not sender_id or not chat_id:
        return None
    msgtype = str(body.get("msgtype") or "").lower()
    if msgtype == "mixed":
        mixed = body.get("mixed") if isinstance(body.get("mixed"), dict) else {}
        items = mixed.get("msg_item") if isinstance(mixed.get("msg_item"), list) else []
        parts = [_content(i, "text") for i in items
                 if isinstance(i, dict) and str(i.get("msgtype") or "").lower() == "text"]
    else:
        parts = [_content(body, "text"), _content(body, "voice") if msgtype == "voice" else ""]
    return Inbound(msgid, req_id, chat_id, sender_id,
                   str(body.get("chattype") or "").lower() == "group", msgtype,
                   "\n".join(p for p in parts if p).strip(), tuple(_media_refs(body, msgtype)),
                   _quote(body))


def _quote(body: dict) -> dict | None:
    """The quoted message of a quote-reply: its type and text (WeCom sends no message id)."""
    quote = body.get("quote") if isinstance(body.get("quote"), dict) else None
    if not quote:
        return None
    kind = str(quote.get("msgtype") or "").lower()
    if kind == "mixed":
        mixed = quote.get("mixed") if isinstance(quote.get("mixed"), dict) else {}
        items = mixed.get("msg_item") if isinstance(mixed.get("msg_item"), list) else []
        text = "\n".join(_content(i, "text") for i in items
                         if isinstance(i, dict) and str(i.get("msgtype") or "").lower() == "text")
    else:
        text = _content(quote, kind) if kind in ("text", "voice", "markdown") else ""
    return {"type": kind or "text", "text": text.strip()}


def _media_refs(body: dict, msgtype: str) -> list[dict]:
    """Image/file references of a callback, as Hermes extracts them (quotes excluded)."""
    found = []

    def take(kind, container):
        ref = container.get(kind) if isinstance(container, dict) else None
        if isinstance(ref, dict) and isinstance(ref.get("url"), str) and ref["url"]:
            found.append({"kind": kind, "url": ref["url"],
                          "aeskey": urllib.parse.unquote(str(ref.get("aeskey") or ""))})
            return True
        return False

    if msgtype == "mixed":
        mixed = body.get("mixed") if isinstance(body.get("mixed"), dict) else {}
        for item in mixed.get("msg_item") if isinstance(mixed.get("msg_item"), list) else []:
            if isinstance(item, dict) and str(item.get("msgtype") or "").lower() == "image":
                take("image", item)
    elif msgtype == "image":
        take("image", body)
    elif msgtype == "file":
        take("file", body)
    elif msgtype == "appmsg":
        take("file", body.get("appmsg")) or take("image", body.get("appmsg"))
    return found


def decrypt_media(data: bytes, aeskey: str) -> bytes:
    """WeCom media: AES-256-CBC, key = base64(aeskey) (unpadded), IV = key[:16], PKCS#7/32."""
    if not aeskey:
        return data
    key = base64.b64decode(aeskey + "=" * (-len(aeskey) % 4))
    if len(key) != 32 or not data or len(data) % 16:
        raise ValueError("invalid WeCom media encryption")
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    plain = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
    plain = plain.update(data) + plain.finalize()
    pad = plain[-1]
    if not 1 <= pad <= 32 or plain[-pad:] != bytes([pad]) * pad:
        raise ValueError("invalid WeCom media padding")
    return plain[:-pad]


_IMAGE_MAGIC = ((b"\x89PNG\r\n\x1a\n", ".png"), (b"\xff\xd8\xff", ".jpg"), (b"GIF8", ".gif"))


def media_name(kind: str, data: bytes, headers: dict) -> str:
    """Filename from Content-Disposition (RFC 5987 or plain, percent-decoded), else by type."""
    disposition = headers.get("content-disposition") or ""
    match = (re.search(r"filename\*\s*=\s*[^']*'[^']*'([^;]+)", disposition)
             or re.search(r'filename\s*=\s*"?([^";]+)"?', disposition))
    name = urllib.parse.unquote(match.group(1).strip()) if match else ""
    name = re.sub(r'[\x00-\x1f\x7f/\\<>:"|?*]', "_", name).strip(" .")
    if not name:
        ext = next((e for magic, e in _IMAGE_MAGIC if data.startswith(magic)), "")
        name = ("image" if kind == "image" else "file") + (ext or ".bin")
    stem, dot, ext = name.rpartition(".")
    if len(name) > 120:  # keep the extension, bound the length
        name = (stem[:110] + "." + ext[:9]) if dot and len(ext) <= 9 else name[:120]
    return name


def guess_mime(name: str, fallback: str | None = None) -> str:
    return mimetypes.guess_type(name)[0] or (fallback if fallback and "/" in fallback else None) \
        or "application/octet-stream"


def _fits(text: str) -> bool:
    return len(text) <= MAX_CHUNK_CHARS and len(text.encode("utf-8")) <= MAX_CHUNK_BYTES


def chunks(text: str) -> list[str]:
    """Split into WeCom-sized parts, preferring line breaks; never loses characters."""
    text = text.strip() or "(empty)"
    out = []
    while not _fits(text):
        lo, hi = 1, min(len(text), MAX_CHUNK_CHARS)
        while lo < hi:  # largest prefix that fits
            mid = (lo + hi + 1) // 2
            lo, hi = (mid, hi) if _fits(text[:mid]) else (lo, mid - 1)
        cut = text.rfind("\n", 0, lo)
        cut = cut if cut > lo // 2 else lo
        out.append(text[:cut])
        text = text[cut:]
    out.append(text)
    return out


class WeComClient:
    def __init__(self, bot_id: str, secret: str, on_message, *, ws_url: str = WS_URL):
        self.bot_id, self._secret, self.on_message, self.ws_url = bot_id, secret, on_message, ws_url
        self.connected = False
        self._ws = self._session = None
        self._pending: dict[str, asyncio.Future] = {}
        self._tasks: set[asyncio.Task] = set()
        self._device_id = uuid.uuid4().hex
        self._last_send: dict[str, float] = {}
        self._send_locks: dict[str, asyncio.Lock] = {}
        self._last_req_id: dict[str, str] = {}
        self._kicked = False

    async def run(self) -> None:
        """Connect and reconnect until cancelled, or until WeCom reports that another
        connection took over this bot. Then return for good: reconnecting would start a
        mutual-kick loop with the other client, so an operator must resolve it (as Hermes does).
        """
        import aiohttp
        backoff = 0
        while True:
            started = time.monotonic()
            try:
                await self._connect(aiohttp)
                log.info("wecom connected")
                await self._listen(aiohttp)
            except asyncio.CancelledError:
                await self._close()
                raise
            except Exception as exc:
                log.warning("wecom connection error: %s", type(exc).__name__)
            self.connected = False
            self._fail_pending()
            await self._close()
            if self._kicked:
                log.error("wecom: another connection took over this bot; stopping")
                return
            if time.monotonic() - started >= STABLE_SECONDS:
                backoff = 0
            await asyncio.sleep(RECONNECT_BACKOFF[min(backoff, len(RECONNECT_BACKOFF) - 1)])
            backoff += 1

    @property
    def kicked(self) -> bool:
        return self._kicked

    async def _connect(self, aiohttp) -> None:
        import ssl
        self._session = aiohttp.ClientSession(
            trust_env=False, connector=aiohttp.TCPConnector(ssl=ssl.create_default_context()))
        self._ws = await asyncio.wait_for(
            self._session.ws_connect(self.ws_url, heartbeat=HEARTBEAT_SECONDS * 2),
            timeout=TIMEOUT_SECONDS)
        req_id = f"subscribe-{uuid.uuid4().hex}"
        await self._ws.send_json({"cmd": CMD_SUBSCRIBE, "headers": {"req_id": req_id},
                                  "body": {"bot_id": self.bot_id, "secret": self._secret,
                                           "device_id": self._device_id}})
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while (remaining := deadline - time.monotonic()) > 0:
            msg = await asyncio.wait_for(self._ws.receive(), timeout=remaining)
            if msg.type != aiohttp.WSMsgType.TEXT:
                raise RuntimeError("websocket closed during subscribe")
            payload = self._parse(msg.data)
            if payload and (payload.get("headers") or {}).get("req_id") == req_id:
                if payload.get("errcode") not in (0, None):
                    raise RuntimeError(f"subscribe rejected errcode={payload.get('errcode')}")
                self.connected = True
                return
        raise TimeoutError("subscribe timed out")

    async def _listen(self, aiohttp) -> None:
        heartbeat = asyncio.create_task(self._heartbeat())
        try:
            while not self._kicked:
                msg = await self._ws.receive()
                if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                    payload = self._parse(msg.data)
                    if payload:
                        self._dispatch(payload)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED,
                                  aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR):
                    raise RuntimeError("websocket closed")
        finally:
            heartbeat.cancel()

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_SECONDS)
            try:
                await self._ws.send_json({"cmd": CMD_PING,
                                          "headers": {"req_id": f"ping-{uuid.uuid4().hex}"},
                                          "body": {}})
            except Exception:
                return

    @staticmethod
    def _parse(raw) -> dict | None:
        try:
            text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
            payload = json.JSONDecoder(strict=False).decode(text)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def _dispatch(self, payload: dict) -> None:
        cmd = str(payload.get("cmd") or "")
        req_id = str((payload.get("headers") or {}).get("req_id") or "")
        if cmd in CMD_CALLBACK:
            inbound = parse_callback(payload)
            if inbound:
                if inbound.req_id:
                    self._last_req_id[inbound.chat_id] = inbound.req_id
                task = asyncio.create_task(self._deliver(inbound))
                self._tasks.add(task)
                task.add_done_callback(self._tasks.discard)
        elif cmd == CMD_EVENT:
            body = payload.get("body") if isinstance(payload.get("body"), dict) else {}
            if body.get("event_type") == "disconnected_event":
                self._kicked = True
        elif req_id in self._pending:
            future = self._pending.pop(req_id)
            if not future.done():
                future.set_result(payload)

    async def _deliver(self, inbound: Inbound) -> None:
        try:
            await self.on_message(inbound)
        except Exception:
            log.exception("inbound handler failed")

    def _fail_pending(self) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionError("wecom disconnected"))
        self._pending.clear()

    async def _close(self) -> None:
        for attr in ("_ws", "_session"):
            live = getattr(self, attr)
            if live is not None and not live.closed:
                try:
                    await live.close()
                except Exception:
                    pass
            setattr(self, attr, None)

    async def _request(self, cmd: str, req_id: str, body: dict) -> dict:
        if not self.connected or self._ws is None or self._ws.closed:
            raise ConnectionError("wecom not connected")
        future = self._pending[req_id] = asyncio.get_running_loop().create_future()
        try:
            await self._ws.send_json({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})
            return await asyncio.wait_for(future, timeout=15)
        finally:
            self._pending.pop(req_id, None)

    async def send_chunk(self, chat_id: str, part: str) -> str:
        """Send one pre-sized markdown part. Returns OK, REJECTED or UNAVAILABLE.

        Proactive send first; if WeCom rejects it, fall back to a passive reply on the
        chat's latest inbound req_id (the order Hermes uses, reversed for unprompted replies).
        """
        markdown = {"msgtype": "markdown", "markdown": {"content": part}}
        lock = self._send_locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            wait = self._last_send.get(chat_id, 0) + MIN_SEND_SPACING - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_send[chat_id] = time.monotonic()
            try:
                resp = await self._request(CMD_SEND, f"{CMD_SEND}-{uuid.uuid4().hex}",
                                           {"chatid": chat_id, **markdown})
                if resp.get("errcode") in (0, None):
                    return OK
                log.warning("wecom proactive send rejected errcode=%s", resp.get("errcode"))
                reply_to = self._last_req_id.get(chat_id)
                if not reply_to:
                    return REJECTED
                resp = await self._request(CMD_RESPOND, reply_to, markdown)
                if resp.get("errcode") in (0, None):
                    return OK
                log.warning("wecom passive reply rejected errcode=%s", resp.get("errcode"))
                return REJECTED
            except (ConnectionError, asyncio.TimeoutError):
                return UNAVAILABLE

    async def send_file(self, chat_id: str, data: bytes, name: str, mime: str) -> str:
        """Upload (init/chunks/finish) then send as image or file. OK, REJECTED or UNAVAILABLE."""
        kind = "image" if mime in IMAGE_MIMES and len(data) <= IMAGE_MAX_BYTES else "file"
        chunks_total = (len(data) + UPLOAD_CHUNK_BYTES - 1) // UPLOAD_CHUNK_BYTES
        if not data or chunks_total > MAX_UPLOAD_CHUNKS:
            return REJECTED
        try:
            init = await self._request(CMD_UPLOAD_INIT, f"{CMD_UPLOAD_INIT}-{uuid.uuid4().hex}", {
                "type": kind, "filename": name, "total_size": len(data),
                "total_chunks": chunks_total, "md5": hashlib.md5(data).hexdigest()})
            upload_id = (init.get("body") or {}).get("upload_id") if init.get("errcode") in (0, None) else None
            if not upload_id:
                log.warning("wecom media upload init rejected errcode=%s", init.get("errcode"))
                return REJECTED
            for index in range(chunks_total):
                part = data[index * UPLOAD_CHUNK_BYTES:(index + 1) * UPLOAD_CHUNK_BYTES]
                resp = await self._request(CMD_UPLOAD_CHUNK, f"{CMD_UPLOAD_CHUNK}-{uuid.uuid4().hex}", {
                    "upload_id": upload_id, "chunk_index": index,
                    "base64_data": base64.b64encode(part).decode("ascii")})
                if resp.get("errcode") not in (0, None):
                    log.warning("wecom media chunk rejected errcode=%s", resp.get("errcode"))
                    return REJECTED
            done = await self._request(CMD_UPLOAD_FINISH, f"{CMD_UPLOAD_FINISH}-{uuid.uuid4().hex}",
                                       {"upload_id": upload_id})
            media_id = (done.get("body") or {}).get("media_id") if done.get("errcode") in (0, None) else None
            if not media_id:
                log.warning("wecom media upload finish rejected errcode=%s", done.get("errcode"))
                return REJECTED
        except (ConnectionError, asyncio.TimeoutError):
            return UNAVAILABLE
        return await self._send_paced(chat_id, {"msgtype": kind, kind: {"media_id": media_id}})

    async def _send_paced(self, chat_id: str, body: dict) -> str:
        lock = self._send_locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            wait = self._last_send.get(chat_id, 0) + MIN_SEND_SPACING - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_send[chat_id] = time.monotonic()
            try:
                resp = await self._request(CMD_SEND, f"{CMD_SEND}-{uuid.uuid4().hex}",
                                           {"chatid": chat_id, **body})
            except (ConnectionError, asyncio.TimeoutError):
                return UNAVAILABLE
            if resp.get("errcode") in (0, None):
                return OK
            log.warning("wecom media send rejected errcode=%s", resp.get("errcode"))
            return REJECTED

    async def send(self, chat_id: str, text: str) -> bool:
        """Best-effort multi-part message (notices); stops at the first failed part."""
        for part in chunks(text):
            if await self.send_chunk(chat_id, part) != OK:
                return False
        return True


def qr_enroll(http_get_json, *, timeout_seconds: int = 300, show=print, sleep=time.sleep) -> dict | None:
    """Create a WeCom AI bot by QR scan; returns {"bot_id", "secret"} or None.

    The ai/qc endpoints back the WeCom admin console and may change.
    """
    raw = http_get_json(f"{QR_GENERATE_URL}?source=hermes")
    data = raw.get("data") if isinstance(raw, dict) and isinstance(raw.get("data"), dict) else {}
    scode, auth_url = str(data.get("scode") or ""), str(data.get("auth_url") or "")
    if not scode or not auth_url:
        raise RuntimeError("unexpected WeCom QR response")
    try:
        import qrcode
        qr = qrcode.QRCode()
        qr.add_data(auth_url)
        qr.make(fit=True)
        qr.print_ascii(invert=True)
    except ImportError:
        pass
    show(f"\nScan with WeCom, or open on your phone:\n  {QR_PAGE_URL}{urllib.parse.quote(scode)}\n",
         flush=True)
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            result = http_get_json(f"{QR_QUERY_URL}?scode={urllib.parse.quote(scode)}")
        except Exception:
            result = {}
        data = result.get("data") if isinstance(result, dict) and isinstance(result.get("data"), dict) else {}
        if str(data.get("status") or "").lower() == "success":
            info = data.get("bot_info") if isinstance(data.get("bot_info"), dict) else {}
            bot_id = str(info.get("botid") or info.get("bot_id") or "").strip()
            secret = str(info.get("secret") or "").strip()
            return {"bot_id": bot_id, "secret": secret} if bot_id and secret else None
        sleep(3)
    return None
