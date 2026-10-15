"""ChatGPT dot messaging client. Endpoints are undocumented; shapes follow chatgpt-dot-cli v0.2.3."""
from __future__ import annotations

import datetime
import re

from .http import MAX_FILE_BYTES, FileTooLarge, Response, Upload
from .openai_auth import Auth, AuthRequired

BASE = "https://chatgpt.com/backend-api"
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,199}\Z")
PAGE_LIMIT = 32


class DotError(Exception):
    def __init__(self, message: str = "", code: str | None = None):
        super().__init__(message)
        self.code = code  # stable key for owner-facing translation, when there is one


class AccountChanged(DotError):
    """The logged-in account is not the one this operation was started for; nothing was sent."""


def ident(value) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise DotError("invalid identifier")
    return value


def members(dot: dict) -> list[dict]:
    preview = dot.get("messaging_room_preview")
    found = preview.get("members") if isinstance(preview, dict) else None
    return [m for m in found or [] if isinstance(m, dict)]


def binding_for(dot: dict, account_id: str) -> dict:
    """Bind an EXISTING dot and room. Rooms are never created here.

    `dot` must come from the /tbo list: only it carries messaging_room_preview.members
    (GET /tbo/{id} returns messaging_room_preview: null). The dot's member is the one
    with an aeon_id; every message has role "user", so role cannot identify it.
    """
    dot_id = ident(dot.get("id"))
    room = dot.get("messaging_room_id")
    if not isinstance(room, str) or not room:
        raise DotError("dot has no messaging room yet; open it once in ChatGPT", code="no_room")
    dot_members = sorted({m["account_user_id"] for m in members(dot)
                          if m.get("aeon_id") and isinstance(m.get("account_user_id"), str)})
    humans = sorted({m["account_user_id"] for m in members(dot)
                     if not m.get("aeon_id") and isinstance(m.get("account_user_id"), str)})
    if not dot_members:
        raise DotError("room has no dot member", code="no_member")
    preview = dot.get("messaging_room_preview") if isinstance(dot.get("messaging_room_preview"), dict) else {}
    name = next((n.strip() for n in (dot.get("display_name"), preview.get("name"))
                 if isinstance(n, str) and n.strip()), "dot")
    return {"account_id": account_id, "dot_id": dot_id, "room_id": ident(room),
            "dot_members": dot_members, "human_members": humans, "name": name}


def message_text(item: dict) -> str:
    content = item.get("content")
    text = content.get("text") if isinstance(content, dict) else None
    return text if isinstance(text, str) else ""


def has_attachments(item: dict) -> bool:
    content = item.get("content")
    return isinstance(content, dict) and bool(content.get("attachments"))


def attachments_of(item: dict) -> list[dict]:
    """Downloadable files of a message: [{file_id, name, mime}] (observed live shape:
    {type: "file", file_id, file: {id, name, mime_type, download_url, status}})."""
    content = item.get("content") if isinstance(item.get("content"), dict) else {}
    out = []
    for att in content.get("attachments") or []:
        if not isinstance(att, dict) or (att.get("type") or "file") != "file":
            continue
        file = att.get("file") if isinstance(att.get("file"), dict) else {}
        file_id = att.get("file_id") or file.get("id") or file.get("file_id")
        if isinstance(file_id, str) and file_id:
            name = file.get("name") or att.get("name") or "attachment"
            out.append({"file_id": file_id, "name": name if isinstance(name, str) else "attachment",
                        "mime": file.get("mime_type") if isinstance(file.get("mime_type"), str) else None})
    return out


def file_host_ok(host: str) -> bool:
    return host.lower().endswith(".oaiusercontent.com")


def timestamp_of(item: dict) -> float | None:
    """created_at as epoch seconds; accepts seconds, milliseconds or ISO-8601."""
    stamp = item.get("created_at")
    if isinstance(stamp, bool):
        return None
    if isinstance(stamp, (int, float)):
        return float(stamp) / 1000 if stamp > 1e11 else float(stamp)
    if isinstance(stamp, str):
        try:
            parsed = datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.timestamp()
    return None


def oldest_first(items: list[dict]) -> list[dict]:
    def key(pair):
        index, item = pair
        stamp = timestamp_of(item)
        return (0, stamp, index) if stamp is not None else (1, 0.0, index)
    return [item for _, item in sorted(enumerate(items), key=key)]


class DotClient:
    def __init__(self, http, auth: Auth):
        self.http, self.auth = http, auth

    async def _call(self, method: str, path: str, *, account_id: str | None = None,
                    body=None, params=None, upload=None, timeout: float = 30) -> Response:
        """One request. `account_id` pins the account: it is re-checked after every token
        fetch or refresh, immediately before the request is issued."""
        def pinned(account: str) -> None:
            if account_id is not None and account != account_id:
                raise AccountChanged("logged-in account changed")

        access, account = await self.auth.access()
        pinned(account)
        refreshed = False
        while True:
            headers = {"Authorization": "Bearer " + access, "ChatGPT-Account-Id": account,
                       "Accept": "application/json", "User-Agent": "Codex Desktop",
                       "originator": "Codex Desktop", "OAI-Product-Sku": "CODEX"}
            extra = {"upload": upload} if upload is not None else {}
            resp = await self.http.request(method, BASE + path, headers=headers, json_body=body,
                                           params=params, timeout=timeout, **extra)
            if resp.status != 401:
                return resp
            # A 401 is a definitive rejection before processing, so one refresh+retry is safe.
            fresh = None if refreshed else await self.auth.refreshed_access(access)
            if fresh is None:  # a fresh token was refused too: the owner must log in again
                await self.auth.reject(access)
                raise AuthRequired()
            (access, account), refreshed = fresh, True
            pinned(account)

    async def list_dots(self, account_id: str | None = None) -> list[dict]:
        resp = await self._call("GET", "/tbo", account_id=account_id)
        items = resp.body.get("items") if isinstance(resp.body, dict) else None
        if resp.status != 200 or not isinstance(items, list):
            raise DotError(f"GET /tbo HTTP {resp.status}")
        return [d for d in items if isinstance(d, dict) and d.get("id")]

    async def live_url(self) -> str:
        """The per-user live-update socket URL. It carries auth material: never log it."""
        resp = await self._call("GET", "/celsius/ws/user")
        url = resp.body.get("websocket_url") if isinstance(resp.body, dict) else None
        if resp.status != 200 or not isinstance(url, str):
            raise DotError(f"GET live socket HTTP {resp.status}")
        return url

    async def find_dot(self, dot_id: str, account_id: str | None = None) -> dict:
        """The dot as listed by /tbo, which (unlike /tbo/{id}) includes its room members."""
        ident(dot_id)
        for dot in await self.list_dots(account_id=account_id):
            if dot.get("id") == dot_id:
                return dot
        raise DotError("this account has no dot with that id; send /dots", code="not_found")

    async def messages(self, room_id: str, limit: int = PAGE_LIMIT,
                       before: str | None = None, account_id: str | None = None) -> list[dict]:
        params = {"limit": str(limit)}
        if before is not None:
            params["before"] = ident(before)
        resp = await self._call("GET", f"/messaging/rooms/{ident(room_id)}/messages",
                                account_id=account_id, params=params)
        items = resp.body.get("items") if isinstance(resp.body, dict) else None
        if resp.status != 200 or not isinstance(items, list):
            raise DotError(f"GET messages HTTP {resp.status}")
        return [m for m in items if isinstance(m, dict)]

    async def send(self, room_id: str, text: str, request_id: str,
                   account_id: str | None = None, file_ids: list[str] | None = None,
                   reply_to: str | None = None) -> Response:
        """One POST (plus one after a definitive 401). Caller classifies the outcome.
        `reply_to` makes it a native reply (verified live: {"reply_to": {"message_id": ...}})."""
        attachments = [{"type": "file", "file_id": ident(f)} for f in file_ids or []]
        body = {"content": {"text": text, "attachments": attachments},
                "request_id": request_id, "idempotency_token": request_id}
        if reply_to:
            body["reply_to"] = {"message_id": ident(reply_to)}
        return await self._call("POST", f"/messaging/rooms/{ident(room_id)}/messages",
                                account_id=account_id, body=body, timeout=45)

    async def upload_file(self, room_id: str, name: str, mime: str, data: bytes,
                          account_id: str | None = None) -> str:
        """Upload one file into the room; returns its file_id (reusable across retries)."""
        if not data or len(data) > MAX_FILE_BYTES:
            raise FileTooLarge()
        resp = await self._call("POST", f"/messaging/rooms/{ident(room_id)}/files",
                                account_id=account_id, upload=Upload("file", name, mime, data),
                                timeout=120)
        file_id = resp.body.get("id") if isinstance(resp.body, dict) else None
        if resp.status not in (200, 201) or not isinstance(file_id, str) or not file_id:
            raise DotError(f"file upload HTTP {resp.status}")
        return ident(file_id)

    async def download_file(self, room_id: str, file_id: str,
                            account_id: str | None = None) -> tuple[bytes, str | None, str | None]:
        """Fresh metadata (signed URLs expire), then the bytes from OpenAI file storage."""
        resp = await self._call("GET", f"/messaging/rooms/{ident(room_id)}/files/{ident(file_id)}",
                                account_id=account_id)
        meta = resp.body if isinstance(resp.body, dict) else {}
        url = meta.get("download_url")
        if (resp.status != 200 or str(meta.get("status") or "").lower() == "error"
                or meta.get("id", file_id) != file_id or not isinstance(url, str)):
            raise DotError(f"file metadata HTTP {resp.status}")
        data, _ = await self.http.fetch(url, host_ok=file_host_ok)
        name, mime = meta.get("name"), meta.get("mime_type")
        return data, name if isinstance(name, str) else None, mime if isinstance(mime, str) else None
