"""Bridge orchestration: owner-only WeCom DM <-> one existing dot room.

Designed for exactly one person. Owner changes happen only with the service stopped
(`dot-bridge reset-owner` refuses otherwise), so no in-flight work can span an owner change.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time

from .dot import (AccountChanged, DotClient, DotError, attachments_of, binding_for,
                  has_attachments, message_text, oldest_first, timestamp_of)
from .http import MAX_FILE_BYTES, ConnectError, FetchError, FileTooLarge, NetworkError
from .openai_auth import Auth, AuthRequired, AuthUnavailable
from . import i18n
from .store import Store
from .wecom import OK, REJECTED, UNAVAILABLE, chunks, decrypt_media, guess_mime, media_name

log = logging.getLogger(__name__)

ACTIVE_POLL_SECONDS = 2
IDLE_POLL_SECONDS = 60
RECOVERY_POLL_SECONDS = 120  # while push updates are live; only a safety net
POLL_RETRY_MAX_SECONDS = 30
ACTIVE_WINDOW_SECONDS = 15 * 60
RECONCILE_DELAY_SECONDS = 4
SETTLED_AGE_SECONDS = 0.5  # dot messages are written whole (updated_at ≈ created_at + 50 ms)
EMPTY_SETTLE_SECONDS = 120
PENDING_GIVE_UP_SECONDS = 6 * 3600
MAX_SWEEP_PAGES = 10
MAX_TEXT_CHARS = 8000
SEND_SPACING_SECONDS = 1.0
NOT_ACCEPTED_DELAY = 30
AFTER_UNCERTAIN_PAUSE = 30
MAX_REPLY_REJECTIONS = 5
MAX_UPLOAD_FAILURES = 3
REPLY_ORDER_HOLD_SECONDS = 600
IN_PROGRESS_STATUSES = frozenset({"in_progress", "streaming", "pending", "generating"})
COMMANDS = frozenset({"/help", "/start", "/login", "/status", "/dots", "/use", "/pending",
                      "/resend", "/discard", "/claim", "/lang"})

def hash_code(code: str) -> str:
    return hashlib.sha256(code.strip().encode()).hexdigest()


def preview(text: str, limit: int = 40) -> str:
    flat = " ".join(text.split())
    return flat if len(flat) <= limit else flat[:limit - 1] + "…"


def _ids(messages) -> set[str]:
    return {m["id"] for m in messages if isinstance(m.get("id"), str)}


class Bridge:
    def __init__(self, store: Store, auth: Auth, dot: DotClient, *, clock=time.time,
                 sleep=asyncio.sleep):
        self.store, self.auth, self.dot, self.clock, self.sleep = store, auth, dot, clock, sleep
        self.wecom = None
        self.live = None
        self._wake = asyncio.Event()
        self._reply_wake = asyncio.Event()
        self._poll_wake = asyncio.Event()
        self._login_task: asyncio.Task | None = None
        self._reply_rejections: dict[str, int] = {}
        self._upload_failures: dict[int, int] = {}
        self._notified: set[str] = set()
        self._autobind_login = None
        self._last_activity = 0.0
        self._recovered = 0

    # ---- identity / notices ---------------------------------------------
    @property
    def owner(self) -> dict | None:
        return self.store.get("owner")

    @property
    def binding(self) -> dict | None:
        return self.store.get("dot")

    def current_binding(self) -> dict | None:
        """The binding, but only if it belongs to the account that is logged in now."""
        binding = self.binding
        if not binding:
            return None
        state = self.auth.state()
        if state["logged_in"] and state["account_id"] != binding["account_id"]:
            log.warning("logged-in account differs from the bound dot's account; unbinding")
            self.store.put("dot", None)
            return None
        return binding

    def tr(self, key: str, **values) -> str:
        """An owner-facing notice in the owner's chosen language (default en-US)."""
        return i18n.t(self.store.get("lang", i18n.DEFAULT), key, **values)

    async def notify(self, text: str) -> bool:
        owner = self.owner
        if not owner or self.wecom is None:
            return False
        return await self.wecom.send(owner["chat_id"], text)

    async def notify_once(self, key: str, text: str) -> None:
        """One notice per condition until the condition clears (see clear_notice)."""
        if key not in self._notified and await self.notify(text):
            self._notified.add(key)

    def clear_notice(self, key: str) -> None:
        self._notified.discard(key)

    # ---- WeCom inbound --------------------------------------------------
    async def on_message(self, msg) -> None:
        if msg.is_group:
            return
        owner = self.owner
        text = msg.text.strip()
        if owner is None:
            await self._try_claim(msg, text)
            return
        if msg.sender != owner["userid"]:
            log.info("ignored message from non-owner")
            return
        if owner.get("chat_id") != msg.chat_id:
            self.store.put("owner", {**owner, "chat_id": msg.chat_id})
        command = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
        if command in COMMANDS:
            if self.store.first_time(msg.msgid):  # WeCom may redeliver; run commands once
                await self._command(command, text[len(command):].strip())
            return
        media = tuple(getattr(msg, "media", ()) or ())
        if (not text and not media) or len(text) > MAX_TEXT_CHARS:
            if self.store.first_time(msg.msgid):
                await self.notify(self.tr("unsupported") if not text else
                                  self.tr("too_long", max=MAX_TEXT_CHARS))
            return
        reply_to = None
        quote = getattr(msg, "quote", None)
        if quote:
            # Native reply when the quote matches text exchanged with a dot message; otherwise
            # the quote travels as context (English: the dot conversation's language).
            reply_to = self.store.find_quote(quote["text"]) if quote["text"] else None
            if not reply_to:
                excerpt = preview(quote["text"], 200) if quote["text"] else f"[{quote['type']}]"
                text = f"> {excerpt}\n\n{text}" if text else f"> {excerpt}"
            log.info("quote-reply (%s, %d chars) %s", quote["type"], len(quote["text"]),
                     "mapped to a dot message" if reply_to else "sent as context")
        _, new = self.store.enqueue(msg.msgid, msg.chat_id, msg.sender, text, files=media,
                                    reply_to=reply_to)
        if new:
            self._last_activity = self.clock()
            self._wake.set()
            if not self.binding:
                await self.notify_once("queued_unbound",
                                       self.tr("queued_unbound"))

    async def _try_claim(self, msg, text: str) -> None:
        claim = self.store.get("claim")
        parts = text.split()
        if (not claim or len(parts) != 2 or parts[0] != "/claim"
                or self.clock() > claim.get("expires", 0)
                or not hmac.compare_digest(hash_code(parts[1]), claim.get("code_hash", ""))):
            return
        self.store.first_time(msg.msgid)  # a redelivery must not reach the command/text path
        self.store.put("owner", {"userid": msg.sender, "chat_id": msg.chat_id})
        self.store.put("claim", None)
        log.info("owner claimed")
        await self.notify(self.tr("claimed") + "\n\n" + self.tr("help"))
        if not self.auth.state()["logged_in"]:
            await self.notify(self.tr("next_login"))

    async def _command(self, name: str, arg: str) -> None:
        if name in ("/help", "/start"):
            await self.notify(self.tr("help"))
        elif name == "/claim":
            await self.notify(self.tr("already_claimed"))
        elif name == "/lang":
            if arg.lower() not in i18n.LANGS:
                await self.notify(self.tr("lang_usage"))
            else:
                self.store.put("lang", arg.lower())
                await self.notify(self.tr("lang_set"))
        elif name == "/login":
            await self._start_login()
        elif name == "/status":
            await self.notify(self.status_text())
        elif name == "/dots":
            await self._list_dots()
        elif name == "/use":
            await self._use_dot(arg)
        elif name == "/pending":
            room = (self.binding or {}).get("room_id")
            items = self.store.unsent()
            lines = [f"{i}. [{self.tr('pending_uncertain' if it.state == 'uncertain' else 'pending_rejected')}"
                     f"{self.tr('pending_other_dot') if it.dest_room not in (None, room) else ''}] "
                     f"{preview(it.text)}" for i, it in enumerate(items, 1)]
            await self.notify(self.tr("pending_title") + "\n" + "\n".join(lines) + "\n\n"
                              + self.tr("pending_footer") if lines else self.tr("pending_none"))
        elif name == "/resend":
            count = self.store.resend_all((self.binding or {}).get("room_id"))
            self._wake.set()
            await self.notify(self.tr("resend_n", n=count) if count else self.tr("resend_none"))
        elif name == "/discard":
            count = self.store.discard_unsent()
            await self.notify(self.tr("discard_n", n=count) if count else self.tr("discard_none"))

    # ---- login / binding ------------------------------------------------
    async def _start_login(self) -> None:
        # /login is only for re-authorizing after OpenAI invalidated the grant; the stored
        # login is removed exactly then (see openai_auth), so while it exists, refuse.
        if self.auth.state()["logged_in"]:
            await self.notify(self.tr("login_valid"))
            return
        # Claim the login slot before any await so concurrent /login calls cannot race.
        if self._login_task and not self._login_task.done():
            await self.notify(self.tr("login_running"))
            return
        self._login_task = asyncio.create_task(self._login_flow())

    async def _login_flow(self) -> None:
        try:
            challenge = await self.auth.device_start()
        except AuthUnavailable:
            await self.notify(self.tr("login_unavailable"))
            return
        await self.notify(self.tr("login_prompt", url=challenge.verification_url,
                                  code=challenge.user_code))
        try:
            account = await self.auth.device_complete(challenge)
        except TimeoutError:
            await self.notify(self.tr("login_expired"))
            return
        except Exception as exc:
            log.warning("device login failed: %s", type(exc).__name__)
            await self.notify(self.tr("login_failed"))
            return
        self.clear_notice("auth")
        binding = self.binding
        if binding and binding["account_id"] != account:
            self.store.put("dot", None)
            await self.notify(self.tr("login_other_account"))
        await self.notify(self.tr("login_ok"))
        if not self.binding:
            self._autobind_login = self.auth.state().get("login_id")
            await self._auto_bind()
        self._wake.set()

    async def _auto_bind(self) -> None:
        try:
            _, account = await self.auth.access()
            dots = await self.dot.list_dots(account_id=account)
        except (DotError, NetworkError, AuthRequired, AuthUnavailable):
            await self.notify(self.tr("dots_unreadable_retry"))
            return
        if len(dots) == 1:
            await self._use_dot(dots[0]["id"])
        elif not dots:
            await self.notify(self.tr("dots_none"))
        else:
            await self._show_dots(dots)

    async def _list_dots(self) -> None:
        try:
            _, account = await self.auth.access()
            dots = await self.dot.list_dots(account_id=account)
        except AuthRequired:
            await self.notify(self.tr("not_logged_in"))
            return
        except (DotError, NetworkError, AuthUnavailable):
            await self.notify(self.tr("dots_unreadable"))
            return
        await self._show_dots(dots)

    async def _show_dots(self, dots: list[dict]) -> None:
        current = (self.binding or {}).get("dot_id")
        lines = [f"- `{d['id']}` {d.get('display_name') or 'dot'}" + (" ✅" if d["id"] == current else "")
                 for d in dots]
        await self.notify(self.tr("dots_title") + "\n" + "\n".join(lines or [self.tr("dots_empty")])
                          + "\n\n" + self.tr("dots_footer"))

    async def _use_dot(self, dot_id: str) -> None:
        if not dot_id:
            await self.notify(self.tr("use_usage"))
            return
        try:
            _, account = await self.auth.access()
            binding = binding_for(await self.dot.find_dot(dot_id, account_id=account), account)
            current = self.current_binding()
            changed = not current or current.get("room_id") != binding["room_id"]
            page = await self.dot.messages(binding["room_id"], account_id=account) if changed else []
            # Existing history of a newly bound room is not replayed into WeCom.
            self.store.bind(binding, _ids(page), room_changed=changed)
        except AuthRequired:
            await self.notify(self.tr("not_logged_in"))
            return
        except DotError as exc:
            await self.notify(self.tr("bind_failed", error=self.tr(f"bind_{exc.code or 'other'}")))
            return
        except (NetworkError, AuthUnavailable):
            await self.notify(self.tr("chatgpt_unreachable"))
            return
        self.clear_notice("queued_unbound")
        log.info("bound dot")
        await self.notify(self.tr("bound", name=binding["name"]))
        self._wake.set()

    # ---- inbound delivery ----------------------------------------------
    async def deliver_once(self) -> str:
        binding = self.current_binding()
        if not binding:
            # Covers a terminal `dot-bridge login`: try binding once per login.
            login = self.auth.state().get("login_id")
            if self.owner and login and login != self._autobind_login:
                self._autobind_login = login
                await self._auto_bind()
            return "unbound"
        item = self.store.head()
        if item is None:
            return "idle"
        if item.available_at > self.clock():
            return "waiting"
        room, account = binding["room_id"], binding["account_id"]
        try:
            await self.auth.access()
        except AuthRequired:
            await self.notify_once("auth", self.tr("need_login"))
            return "auth"
        except AuthUnavailable:
            return "auth_unavailable"
        self.clear_notice("auth")
        if item.dest_room is not None and item.dest_room != room:
            # Attempted earlier in another dot's room; never redirect it to this one.
            self.store.failed(item.seq, "dot_changed")
            await self.notify(self.tr("dot_changed", preview=preview(item.text)))
            return "failed"
        if item.files and not item.dot_files:
            outcome = await self._upload_files(item, room, account)
            if outcome != "uploaded":
                return outcome
            item = self.store.item(item.seq)
        if not self.store.begin_send(item.seq, room):
            return "raced"
        item = self.store.item(item.seq)
        try:
            resp = await self.dot.send(room, item.text, item.request_id, account_id=account,
                                       file_ids=list(item.dot_files), reply_to=item.reply_to)
        except AccountChanged:
            self.store.not_accepted(item.seq, "account_changed", 0)
            self.current_binding()
            return "account_changed"
        except ConnectError:
            self.store.not_accepted(item.seq, "connect", NOT_ACCEPTED_DELAY)
            return "retry_later"
        except NetworkError:
            return await self._uncertain(item, binding, "network")
        except AuthUnavailable:
            # Raised only by token refresh, which happens before any POST or after a definitive 401.
            self.store.not_accepted(item.seq, "auth_unavailable", NOT_ACCEPTED_DELAY)
            return "retry_later"
        except AuthRequired:
            self.store.not_accepted(item.seq, "auth", 0)
            await self.notify_once("auth", self.tr("login_invalid"))
            return "auth"
        except Exception:
            log.exception("unexpected send error")
            return await self._uncertain(item, binding, "internal")
        body = resp.body if isinstance(resp.body, dict) else {}
        if resp.status in (200, 201):
            remote = body.get("id") if isinstance(body.get("id"), str) else None
            self.store.delivered(item.seq, remote)
            self._remember_sent(item.text, remote)
            self._last_activity = self.clock()
            self._poll_wake.set()  # start watching for the dot's reply right away
            return "delivered"
        if resp.status == 429:
            self.store.not_accepted(item.seq, "http_429", NOT_ACCEPTED_DELAY)
            return "retry_later"
        if 400 <= resp.status < 500:
            self.store.failed(item.seq, f"http_{resp.status}")
            await self.notify(self.tr("rejected", preview=preview(item.text), status=resp.status))
            return "failed"
        return await self._uncertain(item, binding, f"http_{resp.status}")

    def _remember_sent(self, text: str, remote_id: str | None) -> None:
        """Map text the owner sees in WeCom to its dot message, for later quote-replies.
        A leading quote block ("> ...") is not part of what the owner typed or reads."""
        if not remote_id or not text:
            return
        self.store.remember_quote(text, remote_id)
        if text.startswith("> ") and "\n\n" in text:
            self.store.remember_quote(text.split("\n\n", 1)[1], remote_id)

    async def _upload_files(self, item, room: str, account: str) -> str:
        """Pre-send step for WeCom images/files: download, decrypt, upload to the dot room,
        persist the file_ids. Nothing is posted here, so failures are safely retried."""
        file_ids, names = [], []
        try:
            for ref in item.files:
                # Encrypted media carries up to 32 bytes of padding; the limit is on the file.
                data, headers = await self.dot.http.fetch(ref["url"], host_ok=lambda host: True,
                                                          max_bytes=MAX_FILE_BYTES + 32)
                data = decrypt_media(data, ref.get("aeskey") or "")
                if len(data) > MAX_FILE_BYTES:
                    raise FileTooLarge()
                name = media_name(ref.get("kind", "file"), data, headers)
                file_ids.append(await self.dot.upload_file(
                    room, name, guess_mime(name, headers.get("content-type")), data, account_id=account))
                names.append(name)
        except FileTooLarge:
            self.store.failed(item.seq, "file_too_large")
            await self.notify(self.tr("file_too_big"))
            return "failed"
        except AuthRequired:
            await self.notify_once("auth", self.tr("need_login"))
            return "auth"
        except AccountChanged:
            self.current_binding()
            return "account_changed"
        except Exception as exc:  # download/decrypt/upload: retry, then give up visibly
            count = self._upload_failures[item.seq] = self._upload_failures.get(item.seq, 0) + 1
            log.warning("file upload to dot failed (%s), attempt %s", type(exc).__name__, count)
            if count < MAX_UPLOAD_FAILURES:
                self.store.defer(item.seq, "upload", NOT_ACCEPTED_DELAY)
                return "retry_later"
            self._upload_failures.pop(item.seq, None)
            self.store.failed(item.seq, "upload_failed")
            await self.notify(self.tr("upload_failed"))
            return "failed"
        self._upload_failures.pop(item.seq, None)
        text = item.text or "📎 " + "、".join(names)
        self.store.set_dot_files(item.seq, file_ids, text)
        return "uploaded"

    async def _uncertain(self, item, binding, reason: str) -> str:
        """Reconcile an unknown outcome. The room returns the request_id of every message
        we post (verified live), so only an exact request_id match counts as evidence."""
        await self.sleep(RECONCILE_DELAY_SECONDS)
        try:
            page = await self.dot.messages(binding["room_id"], account_id=binding["account_id"])
        except Exception:
            page = []
        humans = set(binding.get("human_members") or [])
        for msg in page:
            corr = [msg[k] for k in ("request_id", "idempotency_token") if msg.get(k)]
            if (corr and all(c == item.request_id for c in corr)
                    and msg.get("account_user_id") in humans and isinstance(msg.get("id"), str)):
                self.store.delivered(item.seq, msg["id"])
                self._remember_sent(item.text, msg["id"])
                self._poll_wake.set()
                return "delivered"
        self.store.uncertain(item.seq, reason)
        await self.notify(self.tr("uncertain", preview=preview(item.text)))
        return "uncertain"

    # ---- dot replies: poll (always) -> durable outbox -> WeCom (when connected) ----
    async def poll_replies_once(self) -> int:
        binding = self.current_binding()
        if not binding or not self.owner:
            return 0
        room, account = binding["room_id"], binding["account_id"]
        latest = await self.dot.messages(room, account_id=account)
        pages, vanished, next_sweep = await self._sweep(room, account, latest)
        observed: dict[str, dict] = {}
        for page in pages:  # oldest page first
            for msg in oldest_first(page):
                if isinstance(msg.get("id"), str):
                    observed.setdefault(msg["id"], msg)
        ops = self._decide(observed, set(binding["dot_members"]))
        ops += [("seen", rid) for rid in vanished]
        queued = self.store.apply_reply_ops(room, ops, next_sweep)
        if queued:
            self._last_activity = self.clock()
            self._reply_wake.set()
        return queued

    async def _sweep(self, room: str, account: str, latest: list[dict]):
        """Walk older pages when the latest page does not cover everything we track.

        Needed when nothing on the latest page is known (a gap), or a pending dot message
        has left the latest page. Bounded per poll; an unfinished sweep is persisted and
        resumed next poll. Returns (pages oldest-first, vanished pending ids, next sweep).
        """
        pending = set(self.store.pending())
        latest_ids = _ids(latest)
        sweep = self.store.get("sweep")
        if sweep and sweep.get("room") != room:
            sweep = None
        gap = bool(latest_ids) and not self.store.ledger_empty() and not self.store.known_any(latest_ids)
        if not latest_ids or not (gap or (pending - latest_ids) or sweep):
            return [latest], set(), None
        if sweep:
            cursor, reached_known = sweep["cursor"], sweep["reached_known"]
            found = set(sweep["found"]) | (pending & latest_ids)
        else:
            with_ids = [m for m in latest if isinstance(m.get("id"), str)]
            cursor, reached_known = oldest_first(with_ids)[0]["id"], not gap
            found = pending & latest_ids
        pages, fetched = [latest], set(latest_ids)
        for _ in range(MAX_SWEEP_PAGES):
            older = await self.dot.messages(room, before=cursor, account_id=account)
            if not older:  # beginning of the room: unfound pending messages were deleted
                return list(reversed(pages)), pending - found, None
            fresh = [m for m in older if isinstance(m.get("id"), str) and m["id"] not in fetched]
            if not fresh:
                log.error("message pagination did not advance; reply sweep abandoned")
                return list(reversed(pages)), set(), None
            pages.append(fresh)
            fetched |= _ids(fresh)
            cursor = oldest_first(fresh)[0]["id"]
            reached_known = reached_known or self.store.known_any(_ids(fresh))
            found |= pending & _ids(fresh)
            if reached_known and pending <= found:
                return list(reversed(pages)), set(), None
        log.info("reply sweep continues next poll")
        return list(reversed(pages)), set(), {
            "room": room, "cursor": cursor, "reached_known": reached_known,
            "found": sorted(found)}

    def _decide(self, observed: dict[str, dict], dot_members: set[str]) -> list[tuple]:
        """Turn this poll's observations into ledger ops (pure; nothing is written here)."""
        pending = self.store.pending()
        now = self.clock()
        ops = []
        last_human = None  # observed is chronological: track what the dot was answering
        for rid, msg in observed.items():
            from_dot = msg.get("account_user_id") in dot_members
            quote = self._dot_quote(msg, last_human) if from_dot else ""
            if not from_dot:
                last_human = rid
            if self.store.reply_state(rid) not in (None, "pending"):
                continue
            if not from_dot:
                ops.append(("seen", rid))
                continue
            row = pending.get(rid)
            text = message_text(msg)
            files = attachments_of(msg)
            if not text.strip() and not files and has_attachments(msg):
                text = self.tr("attachment_notice")  # a non-file attachment (link, card)
            # What must stay unchanged to count as final: the text and the attached files.
            signature = text + "\0" + ",".join(f["file_id"] for f in files)
            has_content = bool(text.strip() or files)
            in_progress = str(msg.get("status") or "").lower() in IN_PROGRESS_STATUSES
            if row and now - row.pending_since >= PENDING_GIVE_UP_SECONDS:
                log.warning("dot message unfinished for too long; settling it")
                ops.append(("queue", rid, quote + text + self.tr("unfinished_notice"), files) if has_content
                           else ("seen", rid))
            elif in_progress:
                ops.append(("pending", rid, signature, False))
            elif not has_content:
                # The settle clock runs only while the message is empty AND not generating.
                if row and row.empty_since is not None and now - row.empty_since >= EMPTY_SETTLE_SECONDS:
                    ops.append(("seen", rid))
                else:
                    ops.append(("pending", rid, "", True))
            elif (row and row.text == signature) or self._settled(msg):
                # Final: unchanged across two observations, or not modified for a moment
                # (dot messages are written whole; no streaming status exists).
                ops.append(("queue", rid, quote + text, files))
            else:
                ops.append(("pending", rid, signature, False))
        return ops

    @staticmethod
    def _dot_quote(msg: dict, last_human: str | None) -> str:
        """A "> excerpt" line when the dot replies to something other than the owner's
        latest message (replying to that is the norm and needs no quote)."""
        reply = msg.get("reply_to") if isinstance(msg.get("reply_to"), dict) else {}
        target = reply.get("message_id")
        if not isinstance(target, str) or not target or target == last_human:
            return ""
        quoted = reply.get("message") if isinstance(reply.get("message"), dict) else {}
        return f"> {preview(message_text(quoted), 120) or '[attachment]'}\n\n"

    def _settled(self, msg: dict) -> bool:
        updated = timestamp_of({"created_at": msg.get("updated_at") or msg.get("created_at")})
        return updated is not None and self.clock() - updated >= SETTLED_AGE_SECONDS

    async def forward_once(self) -> str:
        owner = self.owner
        if not owner or self.wecom is None or not self.wecom.connected:
            return "unavailable"
        reply = self.store.next_reply(REPLY_ORDER_HOLD_SECONDS)
        if reply is None:
            return "idle"
        parts = chunks(reply.text) if reply.text.strip() or not reply.files else []
        for index in range(reply.parts_sent, len(parts)):
            result = await self.wecom.send_chunk(owner["chat_id"], parts[index])
            if result == OK:
                self.store.reply_progress(reply.remote_id, index + 1)
                self._remember_sent(parts[index], reply.remote_id)
                continue
            if result == REJECTED:
                count = self._reply_rejections[reply.remote_id] = \
                    self._reply_rejections.get(reply.remote_id, 0) + 1
                if count >= MAX_REPLY_REJECTIONS:
                    log.warning("dropped dot reply after %s WeCom rejections", count)
                    self.store.reply_done(reply.remote_id, "dropped")
                    self._reply_rejections.pop(reply.remote_id, None)
                    return "dropped"
            return result
        binding = self.current_binding() if reply.files[reply.files_sent:] else None
        for index in range(reply.files_sent, len(reply.files)):
            if binding is None:
                return UNAVAILABLE
            result = await self._forward_file(owner["chat_id"], reply.files[index], binding,
                                              f"{reply.remote_id}:{index}")
            if result not in (OK, "skip"):
                return result
            self.store.reply_file_progress(reply.remote_id, index + 1)
        self.store.reply_done(reply.remote_id, "forwarded")
        self._reply_rejections.pop(reply.remote_id, None)
        return "forwarded"

    async def _forward_file(self, chat_id: str, ref: dict, binding: dict, key: str) -> str:
        """One dot file to WeCom: OK, "skip" (the owner was told instead), REJECTED or
        UNAVAILABLE. A file is only skipped once its replacement notice was delivered."""
        name = ref.get("name") or "attachment"
        if self._reply_rejections.get(key, 0) >= MAX_REPLY_REJECTIONS:  # notice still owed
            return await self._file_notice(chat_id, key, self.tr("dot_file_failed", name=name))
        try:
            data, real_name, mime = await self.dot.download_file(
                binding["room_id"], ref["file_id"], account_id=binding["account_id"])
            name = real_name or name
            result = await self.wecom.send_file(chat_id, data, name, guess_mime(name, mime or ref.get("mime")))
        except FileTooLarge:
            return await self._file_notice(chat_id, key, self.tr("dot_file_too_big", name=name))
        except (AuthRequired, AuthUnavailable):
            return UNAVAILABLE
        except Exception as exc:  # metadata/download problems: retry a few times, then point to ChatGPT
            log.warning("dot file download failed: %s", type(exc).__name__)
            result = REJECTED
        if result != REJECTED:
            self._reply_rejections.pop(key, None)
            return result
        count = self._reply_rejections[key] = self._reply_rejections.get(key, 0) + 1
        if count < MAX_REPLY_REJECTIONS:
            return REJECTED
        return await self._file_notice(chat_id, key, self.tr("dot_file_failed", name=name))

    async def _file_notice(self, chat_id: str, key: str, text: str) -> str:
        if not await self.wecom.send(chat_id, text):
            return UNAVAILABLE  # retried later; the file is not skipped silently
        self._reply_rejections.pop(key, None)
        return "skip"

    # ---- push: live events only trigger the authoritative REST read ----
    def on_live_event(self, room_id: str) -> None:
        binding = self.binding
        if binding and room_id == binding["room_id"]:
            self._poll_wake.set()

    def on_live_resync(self) -> None:
        self._poll_wake.set()  # after every (re)subscribe: catch anything missed meanwhile

    def _poll_interval(self) -> float:
        now = self.clock()
        if any(now - p.pending_since < ACTIVE_WINDOW_SECONDS for p in self.store.pending().values()):
            return ACTIVE_POLL_SECONDS  # a dot message is still settling: re-check soon
        if self.live is not None and self.live.connected:
            return RECOVERY_POLL_SECONDS  # push wakes us; this is only a safety net
        active = now - self._last_activity < ACTIVE_WINDOW_SECONDS
        return ACTIVE_POLL_SECONDS if active else IDLE_POLL_SECONDS

    # ---- status ---------------------------------------------------------
    def status_text(self) -> str:
        auth = self.auth.state()
        binding = self.binding
        counts = self.store.counts()
        shown = {k: v for k, v in counts.items() if k != "discarded"}
        lines = [self.tr("status_title"),
                 self.tr("status_wecom", state=self.tr(
                     "connected" if self.wecom and self.wecom.connected else "disconnected")),
                 self.tr("status_live", state=self.tr(
                     "connected" if self.live and self.live.connected else "polling")),
                 self.tr("status_chatgpt", state=self.tr("signed_in" if auth["logged_in"] else "signed_out")),
                 self.tr("status_dot", state=binding["name"] if binding else self.tr("unbound")),
                 self.tr("status_queue", state=", ".join(f"{self.tr('state_' + k)} {v}"
                                                         for k, v in sorted(shown.items()))
                         or self.tr("none")),
                 self.tr("status_backlog", n=self.store.reply_backlog())]
        if counts.get("uncertain") or counts.get("failed"):
            lines.append(self.tr("status_pending"))
        return "\n".join(lines)

    def snapshot(self) -> dict:
        auth = self.auth.state()
        return {"at": self.clock(), "wecom_connected": bool(self.wecom and self.wecom.connected),
                "logged_in": auth["logged_in"], "owner_bound": self.owner is not None,
                "dot_bound": self.binding is not None, "queue": self.store.counts(),
                "reply_backlog": self.store.reply_backlog(),
                "live_connected": bool(self.live and self.live.connected),
                "replies_pending": len(self.store.pending())}

    # ---- loops ----------------------------------------------------------
    async def _wait(self, event: asyncio.Event, timeout: float) -> None:
        """Sleep until `timeout` or until `event` is set (also if it was set while busy)."""
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        event.clear()

    async def delivery_loop(self) -> None:
        while True:
            try:
                result = await self.deliver_once()
            except Exception:
                log.exception("delivery error")
                result = "error"
            if result in ("delivered", "failed", "raced"):
                await self.sleep(SEND_SPACING_SECONDS)  # pace POSTs to the dot account
                continue
            timeout = 15.0
            if result == "uncertain":
                timeout = AFTER_UNCERTAIN_PAUSE
            elif result == "waiting":
                head = self.store.head()
                if head:
                    timeout = min(15.0, max(0.5, head.available_at - self.clock()))
            await self._wait(self._wake, timeout)

    def _retry_delay(self, failures: int) -> float:
        """After a failed read: 2, 4, 8, 16, 30 s, never the long recovery/idle interval,
        so a reply whose push event was consumed by a failed read is not left waiting."""
        return min(float(POLL_RETRY_MAX_SECONDS), 2.0 ** failures)

    async def reply_loop(self) -> None:
        failures = 0
        while True:
            try:
                await self.poll_replies_once()
                failures = 0
            except (AuthRequired, AuthUnavailable, DotError, NetworkError) as exc:
                failures += 1
                log.info("reply poll skipped: %s", type(exc).__name__)
            except Exception:
                failures += 1
                log.exception("reply poll error")
            delay = self._retry_delay(failures) if failures else self._poll_interval()
            await self._wait(self._poll_wake, delay)

    async def forward_loop(self) -> None:
        while True:
            try:
                result = await self.forward_once()
            except Exception:
                log.exception("forward error")
                result = "error"
            if result in ("forwarded", "dropped"):
                continue
            if result == "unavailable":
                await self._wait(self._reply_wake, 2.0)
            elif result == REJECTED:
                await self.sleep(30.0)  # spread retries so a transient limit cannot drop a reply
            else:
                await self._wait(self._reply_wake, 30.0)

    async def status_loop(self) -> None:
        while True:
            try:
                self.store.put("service_status", self.snapshot())
                self.store.prune()
                if self._recovered and self.wecom and self.wecom.connected:
                    if await self.notify(self.tr("recovered", n=self._recovered)):
                        self._recovered = 0
            except Exception:
                log.exception("status update failed")
            await self.sleep(30)

    def recover(self) -> None:
        self._recovered = len(self.store.recover())
