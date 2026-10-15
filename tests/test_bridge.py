import asyncio
import base64
import contextlib
import io
import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path

from dot_bridge import store as store_mod
from dot_bridge.dot import DotClient, DotError, binding_for, oldest_first, timestamp_of
from dot_bridge.http import ConnectError, NetworkError, Response
from dot_bridge.openai_auth import Auth, AuthRequired, TokenFile, account_id_of
from dot_bridge.service import ACTIVE_POLL_SECONDS, IDLE_POLL_SECONDS, Bridge, hash_code
from dot_bridge.store import Store
from dot_bridge.wecom import (MAX_CHUNK_BYTES, MAX_CHUNK_CHARS, OK, REJECTED, UNAVAILABLE,
                              Inbound, WeComClient, chunks, parse_callback)

ACCOUNT = "acct-1"
ROOM = "room-1"
DOT = {"id": "dot-1", "display_name": "Dot", "messaging_room_id": ROOM,
       "messaging_room_preview": {"members": [
           {"account_user_id": "u-dot", "aeon_id": "dot-1"},
           {"account_user_id": "u-me", "aeon_id": None}]}}


def jwt(exp, account=ACCOUNT):
    def enc(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).rstrip(b"=").decode()
    claims = {"https://api.openai.com/auth": {"chatgpt_account_id": account}}
    if exp is not None:
        claims["exp"] = exp
    return ".".join([enc({"alg": "none"}), enc(claims), "sig"])


class Clock:
    def __init__(self, now=1_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


class FakeHttp:
    def __init__(self):
        self.calls = []
        self.routes = {}
        self.history = []

    def on(self, method, suffix, handler):
        self.routes[(method, suffix)] = handler

    async def request(self, method, url, *, headers=None, json_body=None, form=None,
                      params=None, timeout=30, upload=None):
        self.calls.append((method, url, json_body, form, headers, params))
        for (m, suffix), handler in self.routes.items():
            if m == method and url.endswith(suffix):
                result = handler(json_body=json_body, form=form, headers=headers, params=params,
                                 upload=upload)
                if isinstance(result, Exception):
                    raise result
                return result
        return Response(404, None)

    def posts(self, suffix):
        return [c for c in self.calls if c[0] == "POST" and c[1].endswith(suffix)]


class FakeWeCom:
    def __init__(self):
        self.sent = []
        self.connected = True
        self.results = []  # queued send_chunk results; default OK

    async def send_chunk(self, chat_id, part):
        if not self.connected:
            return UNAVAILABLE
        result = self.results.pop(0) if self.results else OK
        if result == OK:
            self.sent.append((chat_id, part))
        return result

    async def send(self, chat_id, text):
        for part in chunks(text):
            if await self.send_chunk(chat_id, part) != OK:
                return False
        return True

    def texts(self):
        return [t for _, t in self.sent]


async def no_sleep(_):
    return None


def msg(text, msgid="m1", sender="owner", chat="owner", group=False):
    return Inbound(msgid, "req-" + msgid, chat, sender, group, "text", text)


class Harness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.clock = Clock()
        self.http = FakeHttp()
        self.tokens = TokenFile(root / "openai.json")
        self.store = Store(root / "bridge.sqlite3", clock=self.clock)
        self.auth = Auth(self.tokens, self.http, clock=self.clock, sleep=no_sleep)
        self.bridge = Bridge(self.store, self.auth, DotClient(self.http, self.auth),
                             clock=self.clock, sleep=no_sleep)
        self.wecom = self.bridge.wecom = FakeWeCom()
        self.http.on("GET", "/tbo", lambda **_: Response(200, {"items": [DOT]}))
        # Real schema: only the /tbo list carries room members; GET /tbo/{id} has none.
        self.http.on("GET", "/tbo/dot-1",
                     lambda **_: Response(200, {**DOT, "messaging_room_preview": None}))
        self.http.on("GET", f"/rooms/{ROOM}/messages", self.page)

    def page(self, params=None, **_):
        """Newest-32 page of history (list is oldest-first); `before` pages backwards."""
        items = list(self.http.history)
        before = (params or {}).get("before")
        if before is not None:
            ids = [m["id"] for m in items]
            items = items[:ids.index(before)] if before in ids else []
        return Response(200, {"items": items[-32:]})

    async def asyncTearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def login(self, exp_offset=3600):
        self.tokens.save({"access_token": jwt(self.clock.now + exp_offset), "refresh_token": "r1",
                          "account_id": ACCOUNT, "obtained_at": self.clock.now - 1000})

    def own(self):
        self.store.put("owner", {"userid": "owner", "chat_id": "owner"})

    def bind(self):
        self.store.put("dot", binding_for(DOT, ACCOUNT))

    def ready(self):
        self.login()
        self.own()
        self.bind()

    def send_route(self, handler):
        self.http.on("POST", f"/rooms/{ROOM}/messages", handler)

    def refresh_ok(self):
        self.http.on("POST", "/oauth/token", lambda **_: Response(
            200, {"access_token": jwt(self.clock.now + 3600), "refresh_token": "r2"}))

    def sent_texts(self):
        return [p[2]["content"]["text"] for p in self.http.posts("/messages")]


class StoreTests(Harness):
    async def test_store_file_private_and_dedup(self):
        mode = stat.S_IMODE(os.stat(Path(self.tmp.name) / "bridge.sqlite3").st_mode)
        self.assertEqual(mode, 0o600)
        a, new_a = self.store.enqueue("m1", "c", "s", "hi")
        b, new_b = self.store.enqueue("m1", "c", "s", "hi")
        self.assertTrue(new_a)
        self.assertFalse(new_b)
        self.assertEqual(a.request_id, b.request_id)

    async def test_crash_mid_send_becomes_uncertain(self):
        item, _ = self.store.enqueue("m1", "c", "s", "hi")
        self.store.begin_send(item.seq, ROOM)
        self.assertEqual(self.store.recover(), [item.seq])
        self.assertEqual(self.store.item(item.seq).state, "uncertain")
        self.assertIsNone(self.store.head())

    async def test_resend_keeps_request_id_and_order(self):
        a, _ = self.store.enqueue("m1", "c", "s", "a")
        b, _ = self.store.enqueue("m2", "c", "s", "b")
        for item in (a, b):
            self.store.begin_send(item.seq, ROOM)
            self.store.uncertain(item.seq, "x")
        self.assertEqual(self.store.resend_all(ROOM), 2)
        head = self.store.head()
        self.assertEqual((head.seq, head.request_id), (a.seq, a.request_id))

    async def test_reply_ledger_is_not_age_pruned(self):
        self.store.mark_seen(["old"])
        self.clock.now += 400 * 86400
        self.store.prune()
        self.assertEqual(self.store.reply_state("old"), "seen")

    async def test_reply_ledger_bounded_by_rows(self):
        original = store_mod.REPLY_LEDGER_ROWS
        store_mod.REPLY_LEDGER_ROWS = 3
        try:
            self.store.mark_seen(["a", "b", "c", "d", "e"])
            self.store.prune()
        finally:
            store_mod.REPLY_LEDGER_ROWS = original
        self.assertEqual([self.store.reply_state(x) for x in "abcde"],
                         [None, None, "seen", "seen", "seen"])

    async def test_reset_identity(self):
        self.ready()
        self.store.enqueue("m1", "owner", "owner", "x")
        self.assertEqual(self.store.reset_identity(), 1)
        self.assertIsNone(self.store.get("owner"))
        self.assertIsNone(self.store.get("dot"))
        self.assertIsNone(self.store.head())


class AuthTests(Harness):
    async def test_no_login_requires_auth(self):
        with self.assertRaises(AuthRequired):
            await self.auth.access()

    async def test_refresh_rotates_atomically(self):
        self.login(exp_offset=10)
        new_access = jwt(self.clock.now + 3600)
        self.http.on("POST", "/oauth/token",
                     lambda **_: Response(200, {"access_token": new_access, "refresh_token": "r2"}))
        access, account = await self.auth.access()
        self.assertEqual((access, account), (new_access, ACCOUNT))
        self.assertEqual(self.tokens.load()["refresh_token"], "r2")
        self.assertEqual(stat.S_IMODE(os.stat(self.tokens.path).st_mode), 0o600)
        self.assertEqual(self.http.posts("/oauth/token")[0][3]["refresh_token"], "r1")

    async def test_revoked_refresh_clears_login(self):
        self.login(exp_offset=10)
        self.http.on("POST", "/oauth/token", lambda **_: Response(400, {"error": "invalid_grant"}))
        with self.assertRaises(AuthRequired):
            await self.auth.access()
        self.assertIsNone(self.tokens.load())

    async def test_html_403_on_refresh_keeps_login(self):
        self.login(exp_offset=10)
        self.http.on("POST", "/oauth/token", lambda **_: Response(403, None))
        with self.assertRaises(Exception) as ctx:
            await self.auth.access()
        self.assertNotIsInstance(ctx.exception, AuthRequired)
        self.assertIsNotNone(self.tokens.load())

    async def test_refresh_cannot_switch_account(self):
        self.login(exp_offset=10)
        self.http.on("POST", "/oauth/token", lambda **_: Response(
            200, {"access_token": jwt(self.clock.now + 3600, account="other")}))
        with self.assertRaises(AuthRequired):
            await self.auth.access()

    async def test_missing_exp_does_not_refresh_every_call(self):
        self.tokens.save({"access_token": jwt(None), "refresh_token": "r1", "account_id": ACCOUNT,
                          "obtained_at": self.clock.now})
        await self.auth.access()
        await self.auth.access()
        self.assertEqual(self.http.posts("/oauth/token"), [])

    async def test_refresh_adopts_concurrent_terminal_login(self):
        self.login(exp_offset=10)
        stale = self.tokens.load()
        fresh = {**stale, "access_token": jwt(self.clock.now + 3600), "refresh_token": "new"}
        self.tokens.save(fresh)
        result = await self.auth._refresh(stale)
        self.assertEqual(result["refresh_token"], "new")
        self.assertEqual(self.http.posts("/oauth/token"), [])

    async def test_device_flow_persists_tokens(self):
        polls = iter([Response(403), Response(200, {"authorization_code": "c", "code_verifier": "v"})])
        self.http.on("POST", "/deviceauth/usercode",
                     lambda **_: Response(200, {"user_code": "ABCD-1234", "device_auth_id": "priv", "interval": 1}))
        self.http.on("POST", "/deviceauth/token", lambda **_: next(polls))
        self.http.on("POST", "/oauth/token", lambda **_: Response(
            200, {"access_token": jwt(self.clock.now + 3600), "refresh_token": "r", "id_token": ""}))
        challenge = await self.auth.device_start()
        self.assertNotIn("priv", repr(challenge))
        self.assertEqual(await self.auth.device_complete(challenge), ACCOUNT)
        self.assertEqual(self.tokens.load()["account_id"], ACCOUNT)

    async def test_account_id_from_id_token(self):
        self.assertEqual(account_id_of({"access_token": "x", "id_token": jwt(1)}), ACCOUNT)


class InboundTests(Harness):
    async def test_unclaimed_bot_ignores_everyone_until_claim(self):
        await self.bridge.on_message(msg("hello"))
        self.assertIsNone(self.store.get("owner"))
        self.store.put("claim", {"code_hash": hash_code("12345678"), "expires": self.clock.now + 60})
        await self.bridge.on_message(msg("/claim 00000000", msgid="m2"))
        self.assertIsNone(self.store.get("owner"))
        await self.bridge.on_message(msg("/claim 12345678", msgid="m3"))
        self.assertEqual(self.store.get("owner")["userid"], "owner")
        self.assertIsNone(self.store.get("claim"))

    async def test_expired_claim_rejected(self):
        self.store.put("claim", {"code_hash": hash_code("1"), "expires": self.clock.now - 1})
        await self.bridge.on_message(msg("/claim 1"))
        self.assertIsNone(self.store.get("owner"))

    async def test_non_owner_and_groups_ignored(self):
        self.ready()
        await self.bridge.on_message(msg("hi", sender="intruder", chat="intruder"))
        await self.bridge.on_message(msg("hi", msgid="m2", group=True))
        self.assertEqual(self.store.counts(), {})
        self.assertEqual(self.wecom.sent, [])

    async def test_unknown_slash_text_goes_to_dot(self):
        self.ready()
        await self.bridge.on_message(msg("/etc/hosts 是什么"))
        self.assertEqual(self.store.counts(), {"ready": 1})

    async def test_delivers_once_with_stable_request_id(self):
        self.ready()
        self.send_route(lambda json_body, **_: Response(200, {"id": "remote-1", **json_body}))
        await self.bridge.on_message(msg("你好 dot"))
        await self.bridge.on_message(msg("你好 dot"))  # WeCom redelivery
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(await self.bridge.deliver_once(), "idle")
        posts = self.http.posts("/messages")
        self.assertEqual(len(posts), 1)
        body = posts[0][2]
        self.assertEqual(body["request_id"], body["idempotency_token"])
        self.assertEqual(body["content"]["text"], "你好 dot")
        self.assertEqual(self.store.counts(), {"delivered": 1})

    async def test_new_identical_text_without_request_id_is_not_evidence(self):
        self.ready()

        def lost(json_body, **_):
            self.http.history.append({"id": "remote-9", "account_user_id": "u-me",
                                      "created_at": self.clock.now + 1,
                                      "content": {"text": json_body["content"]["text"]}})
            return NetworkError("timeout")
        self.send_route(lost)
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")
        self.assertEqual(len(self.http.posts("/messages")), 1)

    async def test_reconcile_by_correlation_id(self):
        self.ready()

        def lost(json_body, **_):
            self.http.history.append({"id": "r", "account_user_id": "u-me",
                                      "request_id": json_body["request_id"],
                                      "content": {"text": json_body["content"]["text"]}})
            return NetworkError("timeout")
        self.send_route(lost)
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")

    async def test_old_identical_text_is_not_evidence(self):
        self.ready()
        self.http.history.append({"id": "old", "account_user_id": "u-me",
                                  "created_at": self.clock.now - 3600, "content": {"text": "ok"}})
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("ok"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")

    async def test_preexisting_untimestamped_identical_text_is_not_evidence(self):
        self.ready()
        self.http.history.append({"id": "x", "account_user_id": "u-me", "content": {"text": "ok"}})
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("ok"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")

    async def test_uncertain_never_auto_resent_and_does_not_block(self):
        self.ready()
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("hello"))
        await self.bridge.on_message(msg("next", msgid="m2"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")
        self.assertIn("hello", self.wecom.texts()[-1])
        self.send_route(lambda **_: Response(200, {"id": "r2"}))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(await self.bridge.deliver_once(), "idle")
        self.assertEqual(self.sent_texts(), ["hello", "next"])
        await self.bridge.on_message(msg("/resend", msgid="m3"))
        await self.bridge.on_message(msg("/resend", msgid="m3"))  # redelivered command
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(await self.bridge.deliver_once(), "idle")
        self.assertEqual(self.sent_texts(), ["hello", "next", "hello"])
        resend_bodies = [p[2] for p in self.http.posts("/messages")]
        self.assertEqual(resend_bodies[0]["request_id"], resend_bodies[2]["request_id"])

    async def test_discard_only_drops_unsent(self):
        self.ready()
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("a"))
        await self.bridge.deliver_once()
        await self.bridge.on_message(msg("b", msgid="m2"))
        await self.bridge.on_message(msg("/discard", msgid="m3"))
        self.assertEqual(self.store.counts(), {"discarded": 1, "ready": 1})

    async def test_connect_error_is_not_accepted_not_uncertain(self):
        self.ready()
        self.send_route(lambda **_: ConnectError("dns"))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "retry_later")
        self.assertEqual(self.store.counts(), {"ready": 1})
        self.assertEqual(self.wecom.sent, [])

    async def test_server_error_is_uncertain(self):
        self.ready()
        self.send_route(lambda **_: Response(502, None))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")

    async def test_unexpected_exception_after_intent_is_uncertain(self):
        self.ready()
        self.send_route(lambda **_: ValueError("boom"))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")
        self.assertEqual(self.store.counts(), {"uncertain": 1})

    async def test_401_refreshes_and_retries_once(self):
        self.ready()
        statuses = iter([Response(401), Response(200, {"id": "r1"})])
        self.send_route(lambda **_: next(statuses))
        self.refresh_ok()
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(len(self.http.posts("/messages")), 2)

    async def test_persistent_401_requires_login_without_loop(self):
        self.ready()
        self.send_route(lambda **_: Response(401))
        self.refresh_ok()
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "auth")
        self.assertIsNone(self.tokens.load())
        for _ in range(5):
            self.clock.now += 60
            self.assertEqual(await self.bridge.deliver_once(), "auth")
        self.assertEqual(len(self.http.posts("/messages")), 2)
        self.assertEqual(len(self.http.posts("/oauth/token")), 1)
        self.assertEqual(len(self.wecom.sent), 1)
        self.assertEqual(self.store.counts(), {"ready": 1})

    async def test_429_keeps_fifo_order(self):
        self.ready()
        replies = iter([Response(429), Response(200, {"id": "1"}), Response(200, {"id": "2"})])
        self.send_route(lambda **_: next(replies))
        await self.bridge.on_message(msg("A"))
        await self.bridge.on_message(msg("B", msgid="m2"))
        self.assertEqual(await self.bridge.deliver_once(), "retry_later")
        self.assertEqual(await self.bridge.deliver_once(), "waiting")
        self.clock.now += 31
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(self.sent_texts(), ["A", "A", "B"])

    async def test_rejected_4xx_reported(self):
        self.ready()
        self.send_route(lambda **_: Response(422, {"detail": "bad"}))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "failed")
        self.assertIn("422", self.wecom.texts()[-1])

    async def test_no_login_holds_queue_and_prompts_once(self):
        self.own()
        self.bind()
        await self.bridge.on_message(msg("hello"))
        for _ in range(4):
            self.clock.now += 600
            self.assertEqual(await self.bridge.deliver_once(), "auth")
        self.assertEqual(len(self.wecom.sent), 1)
        self.assertEqual(self.store.counts(), {"ready": 1})


class BindingTests(Harness):
    def device_routes(self):
        self.http.on("POST", "/deviceauth/usercode",
                     lambda **_: Response(200, {"user_code": "C", "device_auth_id": "d", "interval": 1}))
        self.http.on("POST", "/deviceauth/token",
                     lambda **_: Response(200, {"authorization_code": "c", "code_verifier": "v"}))
        self.http.on("POST", "/oauth/token", lambda **_: Response(
            200, {"access_token": jwt(self.clock.now + 3600), "refresh_token": "r"}))

    async def test_binding_requires_existing_room_and_dot_member(self):
        with self.assertRaises(DotError):
            binding_for({**DOT, "messaging_room_id": None}, ACCOUNT)
        with self.assertRaises(DotError):
            binding_for({**DOT, "messaging_room_preview": {"members": []}}, ACCOUNT)

    async def test_login_auto_binds_single_dot_without_replaying_history(self):
        self.own()
        self.http.history.append({"id": "old", "account_user_id": "u-dot", "content": {"text": "old"}})
        self.device_routes()
        await self.bridge.on_message(msg("/login"))
        await self.bridge._login_task
        self.assertEqual(self.store.get("dot")["dot_id"], "dot-1")
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.assertNotIn("old", self.wecom.texts())

    async def test_terminal_login_autobinds_once_even_with_many_dots(self):
        self.own()
        other = {**DOT, "id": "dot-2", "messaging_room_id": "room-2"}
        self.http.on("GET", "/tbo", lambda **_: Response(200, {"items": [DOT, other]}))
        self.login()
        for _ in range(5):
            self.clock.now += 600
            self.assertEqual(await self.bridge.deliver_once(), "unbound")
        self.assertEqual(len([t for t in self.wecom.texts() if "Your dots" in t]), 1)

    async def test_rebinding_same_room_keeps_inflight_reply(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": "a"}}]
        await self.bridge.poll_replies_once()
        await self.bridge.on_message(msg("/use dot-1"))
        self.assertEqual(await self.bridge.poll_replies_once(), 1)
        self.assertEqual(await self.bridge.forward_once(), "forwarded")

    async def test_no_room_creation_endpoint_is_ever_called(self):
        self.ready()
        await self.bridge.on_message(msg("/use dot-1"))
        self.assertFalse([c for c in self.http.calls if c[1].endswith("/messaging-room")])


class ReplyTests(Harness):
    async def poll_and_forward(self, polls=2):
        for _ in range(polls):
            await self.bridge.poll_replies_once()
        results = []
        while (r := await self.bridge.forward_once()) not in ("idle", "unavailable", REJECTED):
            results.append(r)
        return results

    async def test_dot_reply_forwarded_once_after_stable(self):
        self.ready()
        self.http.history[:] = [{"id": "h1", "account_user_id": "u-me", "content": {"text": "q"}},
                                {"id": "d1", "account_user_id": "u-dot", "content": {"text": "par"}}]
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.http.history[1]["content"]["text"] = "partial answer"
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.assertEqual(await self.bridge.poll_replies_once(), 1)
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(await self.bridge.forward_once(), "idle")
        self.assertEqual(self.wecom.sent, [("owner", "partial answer")])

    async def test_in_progress_status_waits(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "status": "in_progress",
                                 "content": {"text": "a"}}]
        for _ in range(3):
            self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.http.history[0]["status"] = "finished_successfully"
        self.assertEqual(await self.bridge.poll_replies_once(), 1)

    async def test_empty_in_progress_reply_is_kept_until_text_arrives(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "status": "in_progress",
                                 "content": {"text": ""}}]
        await self.bridge.poll_replies_once()
        self.clock.now += 600
        await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "pending")
        self.http.history[0] = {"id": "d1", "account_user_id": "u-dot", "content": {"text": "final"}}
        self.assertEqual(await self.poll_and_forward(), ["forwarded"])
        self.assertEqual(self.wecom.texts(), ["final"])

    async def test_empty_reply_without_status_waits_before_settling(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": ""}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "pending")
        self.http.history[0]["content"]["text"] = "late text"
        self.assertEqual(await self.poll_and_forward(), ["forwarded"])

    async def test_empty_dot_items_settle_silently(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": ""}}]
        await self.bridge.poll_replies_once()
        self.clock.now += 121
        await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "seen")
        self.assertEqual(await self.bridge.forward_once(), "idle")
        self.assertEqual(self.wecom.sent, [])

    async def test_replies_collected_while_wecom_down_survive_32_newer_messages(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": "keep me"}}]
        self.wecom.connected = False
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.http.history += [{"id": f"h{i}", "account_user_id": "u-me", "content": {"text": str(i)}}
                              for i in range(40)]
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "unavailable")
        self.wecom.connected = True
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["keep me"])

    async def test_backfill_finds_reply_beyond_latest_page(self):
        self.ready()
        self.http.history[:] = [{"id": "h0", "account_user_id": "u-me", "content": {"text": "q"}}]
        await self.bridge.poll_replies_once()  # ledger knows h0
        self.http.history += [{"id": "d1", "account_user_id": "u-dot", "content": {"text": "missed"}}]
        self.http.history += [{"id": f"h{i}", "account_user_id": "u-me", "content": {"text": str(i)}}
                              for i in range(1, 60)]
        self.assertEqual(await self.poll_and_forward(), ["forwarded"])
        self.assertEqual(self.wecom.texts(), ["missed"])
        self.assertTrue(any((c[5] or {}).get("before") for c in self.http.calls if c[0] == "GET"))

    async def test_queued_text_is_frozen_against_later_edits(self):
        self.ready()
        first = "x" * (MAX_CHUNK_CHARS + 10)
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": first}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.wecom.results = [OK, REJECTED]
        self.assertEqual(await self.bridge.forward_once(), REJECTED)
        self.http.history[0]["content"]["text"] = "y" * (MAX_CHUNK_CHARS + 10)
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual("".join(self.wecom.texts()), first)

    async def test_part_progress_survives_restart(self):
        self.ready()
        text = "z" * (MAX_CHUNK_CHARS * 2 + 5)
        self.store.queue_reply("d1", text)
        self.wecom.results = [OK, REJECTED]
        await self.bridge.forward_once()
        bridge2 = Bridge(self.store, self.auth, DotClient(self.http, self.auth),
                         clock=self.clock, sleep=no_sleep)
        bridge2.wecom = self.wecom
        self.assertEqual(await bridge2.forward_once(), "forwarded")
        self.assertEqual("".join(self.wecom.texts()), text)

    async def test_rejected_reply_dropped_after_limit_then_next_delivered(self):
        self.ready()
        self.store.queue_reply("d1", "a")
        self.store.queue_reply("d2", "b")
        self.wecom.results = [REJECTED] * 5
        results = [await self.bridge.forward_once() for _ in range(5)]
        self.assertEqual(results, [REJECTED] * 4 + ["dropped"])
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["b"])

    async def test_pending_reply_polling_cadence_and_vanished_message(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "status": "in_progress",
                                 "content": {"text": "a"}}]
        await self.bridge.poll_replies_once()
        self.assertEqual(self.bridge._poll_interval(), ACTIVE_POLL_SECONDS)
        self.clock.now += 3600
        self.assertEqual(self.bridge._poll_interval(), IDLE_POLL_SECONDS)
        self.clock.now -= 3600
        # The message is deleted: a sweep reaches the start of the room without finding it.
        self.http.history[:] = [{"id": "h9", "account_user_id": "u-me", "content": {"text": "x"}}]
        await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "seen")
        self.assertEqual(self.store.pending(), {})


class FindingRegressionTests(Harness):
    """Regressions for the GPT 6.1 Sol pre-deploy review findings."""

    def device_routes(self, approve):
        async def yielding_sleep(_):
            await asyncio.sleep(0)
        self.auth.sleep = yielding_sleep  # let the test run while the login polls
        self.http.on("POST", "/deviceauth/usercode",
                     lambda **_: Response(200, {"user_code": "C", "device_auth_id": "d", "interval": 1}))
        self.http.on("POST", "/deviceauth/token", lambda **_: approve())
        self.http.on("POST", "/oauth/token", lambda **_: Response(
            200, {"access_token": jwt(self.clock.now + 3600), "refresh_token": "r"}))

    async def test_identical_message_30s_before_attempt_is_not_evidence(self):
        self.ready()
        self.http.history.append({"id": "old", "account_user_id": "u-me",
                                  "created_at": self.clock.now - 30, "content": {"text": "ok"}})
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("ok"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")
        self.assertEqual(len(self.store.unsent()), 1)

    async def test_terminal_login_to_other_account_unbinds_before_any_post(self):
        self.ready()
        self.tokens.save({"access_token": jwt(self.clock.now + 3600, account="acct-B"),
                          "refresh_token": "rB", "account_id": "acct-B", "obtained_at": self.clock.now})
        self.http.on("GET", "/tbo", lambda **_: Response(200, {"items": []}))
        self.send_route(lambda **_: Response(200, {"id": "x"}))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "unbound")
        self.assertIsNone(self.store.get("dot"))
        self.assertEqual(self.http.posts("/messages"), [])

    async def test_concurrent_login_commands_start_one_flow(self):
        self.own()
        self.device_routes(lambda: Response(403))
        await asyncio.gather(*(self.bridge.on_message(msg("/login", msgid=f"l{i}")) for i in range(10)))
        await asyncio.sleep(0)
        self.assertEqual(len(self.http.posts("/deviceauth/usercode")), 1)
        self.bridge._login_task.cancel()

    async def test_redelivered_claim_is_never_forwarded(self):
        self.store.put("claim", {"code_hash": hash_code("12345678"), "expires": self.clock.now + 60})
        await self.bridge.on_message(msg("/claim 12345678", msgid="c1"))
        self.login()
        self.bind()
        await self.bridge.on_message(msg("/claim 12345678", msgid="c1"))   # redelivery
        await self.bridge.on_message(msg("/claim 99999999", msgid="c2"))   # new attempt
        self.assertEqual(self.store.counts(), {})

    async def test_reset_drops_undelivered_replies(self):
        self.ready()
        self.store.queue_reply("d1", "private")
        self.store.reset_identity()
        self.assertIsNone(self.store.next_reply(600))


class Round2RegressionTests(Harness):
    """Regressions for the GPT 6.1 Sol verification-pass findings (single-owner scope)."""

    async def test_reset_owner_refuses_while_service_runs(self):
        from dot_bridge import __main__ as cli
        root = Path(self.tmp.name) / "state"
        paths = cli.Paths(str(root))
        paths.prepare()
        held = cli.single_instance(paths)  # the running service holds this
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                code = cli.main(["--state", str(root), "reset-owner"])
        finally:
            os.close(held)
        self.assertEqual(code, 1)
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(["--state", str(root), "reset-owner"]), 0)

    def dot_msg(self, rid, text, **extra):
        return {"id": rid, "account_user_id": "u-dot", "content": {"text": text}, **extra}

    def humans(self, n, start=0):
        return [{"id": f"h{i}", "account_user_id": "u-me", "content": {"text": str(i)}}
                for i in range(start, start + n)]

    async def test_unfinished_reply_tracked_after_leaving_latest_page(self):
        self.ready()
        self.http.history[:] = [self.dot_msg("d1", "", status="in_progress")]
        await self.bridge.poll_replies_once()
        self.http.history += self.humans(40)  # pushes d1 off the latest page
        for _ in range(3):
            await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "pending")
        self.http.history[0] = self.dot_msg("d1", "finished answer")
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["finished answer"])

    async def test_backfilled_unfinished_reply_is_kept_not_seen(self):
        self.ready()
        self.http.history[:] = self.humans(1)
        await self.bridge.poll_replies_once()
        self.http.history += [self.dot_msg("d1", "draft", status="in_progress")] + self.humans(40, 1)
        await self.bridge.poll_replies_once()  # gap -> sweep finds d1 unfinished
        self.assertEqual(self.store.reply_state("d1"), "pending")
        self.http.history[1] = self.dot_msg("d1", "done")
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["done"])

    async def test_account_swap_before_post_never_posts_with_other_account(self):
        self.ready()
        original = self.auth.access
        calls = {"n": 0}

        async def access_then_swap():
            result = await original()
            calls["n"] += 1
            if calls["n"] == 1:  # terminal login to account B lands right after the pre-check
                self.tokens.save({"access_token": jwt(self.clock.now + 3600, account="acct-B"),
                                  "refresh_token": "rB", "account_id": "acct-B",
                                  "obtained_at": self.clock.now})
            return result
        self.auth.access = access_then_swap
        self.send_route(lambda **_: Response(200, {"id": "x"}))
        await self.bridge.on_message(msg("hello"))
        self.assertEqual(await self.bridge.deliver_once(), "account_changed")
        self.assertEqual(self.http.posts("/messages"), [])
        self.assertEqual(self.store.counts(), {"ready": 1})
        self.assertIsNone(self.store.get("dot"))

    async def test_sweep_resumes_across_polls_past_page_limit(self):
        self.ready()
        self.http.history[:] = self.humans(1)
        await self.bridge.poll_replies_once()
        self.http.history += [self.dot_msg("d1", "old but unseen")] + self.humans(400, 1)
        for _ in range(4):
            await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["old but unseen"])
        self.assertIsNone(self.store.get("sweep"))

    async def test_empty_settle_clock_excludes_in_progress_time(self):
        self.ready()
        self.http.history[:] = [self.dot_msg("d1", "", status="in_progress")]
        await self.bridge.poll_replies_once()
        self.clock.now += 600
        self.http.history[0] = self.dot_msg("d1", "")  # status gone, still empty
        await self.bridge.poll_replies_once()
        self.assertEqual(self.store.reply_state("d1"), "pending")
        self.clock.now += 30
        self.http.history[0] = self.dot_msg("d1", "final text")
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")

class AstraRegressionTests(Harness):
    """Regressions for the GPT-6 Astra single-user review findings."""

    async def test_replies_delivered_in_order_while_earlier_one_generates(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "status": "in_progress",
                                 "content": {"text": "Fi"}},
                                {"id": "d2", "account_user_id": "u-dot", "content": {"text": "Second"}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()  # d2 queued, d1 still generating
        self.assertEqual(await self.bridge.forward_once(), "idle")
        self.http.history[0] = {"id": "d1", "account_user_id": "u-dot", "content": {"text": "First"}}
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["First", "Second"])

    async def test_long_generating_reply_stops_blocking_after_hold(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "status": "in_progress",
                                 "content": {"text": "..."}},
                                {"id": "d2", "account_user_id": "u-dot", "content": {"text": "Second"}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.clock.now += 601
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["Second"])

    async def test_resend_never_redirects_to_another_dot(self):
        self.ready()
        self.send_route(lambda **_: NetworkError("timeout"))
        await self.bridge.on_message(msg("meant for A"))
        self.assertEqual(await self.bridge.deliver_once(), "uncertain")
        other = {**DOT, "id": "dot-2", "messaging_room_id": "room-2"}
        self.http.on("GET", "/tbo", lambda **_: Response(200, {"items": [DOT, other]}))
        self.http.on("GET", "/rooms/room-2/messages", lambda **_: Response(200, {"items": []}))
        self.http.on("POST", "/rooms/room-2/messages", lambda **_: Response(200, {"id": "b"}))
        await self.bridge.on_message(msg("/use dot-2", msgid="u1"))
        await self.bridge.on_message(msg("/resend", msgid="r1"))
        self.assertEqual(await self.bridge.deliver_once(), "idle")
        self.assertEqual(self.http.posts("/rooms/room-2/messages"), [])
        self.assertIn("for another dot", (await self._pending_text()))

    async def test_held_item_for_old_room_is_failed_not_redirected(self):
        self.ready()
        item, _ = self.store.enqueue("m1", "owner", "owner", "x")
        self.store.begin_send(item.seq, "room-old")
        self.store.not_accepted(item.seq, "http_429", 0)  # ready again, destination fixed
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        self.assertEqual(await self.bridge.deliver_once(), "failed")
        self.assertEqual(self.http.posts("/messages"), [])

    async def _pending_text(self):
        await self.bridge.on_message(msg("/pending", msgid="p1"))
        return self.wecom.texts()[-1]


class RealSchemaTests(Harness):
    """Shapes observed from the live account on 2026-10-15 (values replaced)."""

    REAL = {"id": "aeon-1", "display_name": "", "messaging_room_id": "room1", "status": "active",
            "messaging_room_preview": {
                "id": "room1", "name": "Bob", "type": "DM", "aeon_id": "aeon-1",
                "creator_account_user_id": "user-HUMAN",
                "members": [
                    {"role": "user", "account_user_id": "user-HUMAN", "username": "me"},
                    {"role": "user", "account_user_id": "aeon-" + "x" * 148, "aeon_id": "aeon-1",
                     "username": ""}]}}

    async def test_use_binds_from_list_when_get_has_no_members(self):
        self.login()
        self.own()
        self.http.on("GET", "/tbo", lambda **_: Response(200, {"items": [self.REAL]}))
        self.http.on("GET", "/tbo/aeon-1",
                     lambda **_: Response(200, {**self.REAL, "messaging_room_preview": None}))
        self.http.on("GET", "/rooms/room1/messages", lambda **_: Response(
            200, {"items": [], "prev_cursor": None, "next_cursor": None}))
        await self.bridge.on_message(msg("/use aeon-1"))
        binding = self.store.get("dot")
        self.assertEqual(binding["dot_members"], ["aeon-" + "x" * 148])
        self.assertEqual(binding["human_members"], ["user-HUMAN"])
        self.assertEqual(binding["name"], "Bob")
        self.assertIn("Bob", self.wecom.texts()[-1])

    async def test_unknown_dot_id_reports_clearly(self):
        self.login()
        self.own()
        await self.bridge.on_message(msg("/use nope"))
        self.assertIsNone(self.store.get("dot"))
        self.assertIn("/dots", self.wecom.texts()[-1])


class LatencyTests(Harness):
    """Reply latency fixes after the first live test (reply seen 20.7 s after creation)."""

    def iso(self, t):
        import datetime
        return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).isoformat().replace("+00:00", "Z")

    async def test_settled_reply_forwarded_on_first_observation(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": "hi"},
                                 "created_at": self.iso(self.clock.now - 3),
                                 "updated_at": self.iso(self.clock.now - 3)}]
        self.assertEqual(await self.bridge.poll_replies_once(), 1)

    async def test_just_written_reply_waits_for_second_observation(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot", "content": {"text": "hi"},
                                 "created_at": self.iso(self.clock.now - 0.2),
                                 "updated_at": self.iso(self.clock.now - 0.2)}]
        self.assertEqual(await self.bridge.poll_replies_once(), 0)
        self.assertEqual(await self.bridge.poll_replies_once(), 1)

    async def test_delivery_wakes_reply_poller_and_needs_no_snapshot(self):
        self.ready()
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        await self.bridge.on_message(msg("hello"))
        self.assertFalse(self.bridge._poll_wake.is_set())
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertTrue(self.bridge._poll_wake.is_set())
        gets = [c for c in self.http.calls if c[0] == "GET" and c[1].endswith("/messages")]
        self.assertEqual(gets, [])
        self.assertEqual(self.bridge._poll_interval(), ACTIVE_POLL_SECONDS)


class LoginPolicyTests(Harness):
    async def test_login_refused_while_oauth_is_valid(self):
        self.ready()
        await self.bridge.on_message(msg("/login"))
        self.assertIsNone(self.bridge._login_task)
        self.assertEqual(self.http.posts("/deviceauth/usercode"), [])
        self.assertIn("isn't needed", self.wecom.texts()[-1])

    async def test_login_allowed_after_grant_invalidated(self):
        self.ready()
        self.tokens.save({**self.tokens.load(), "access_token": jwt(self.clock.now + 10)})
        self.http.on("POST", "/oauth/token", lambda **_: Response(400, {"error": "invalid_grant"}))
        self.http.on("POST", "/deviceauth/usercode", lambda **_: Response(503))
        with self.assertRaises(AuthRequired):
            await self.auth.access()            # OpenAI invalidated the grant: login cleared
        await self.bridge.on_message(msg("/login", msgid="l2"))
        await self.bridge._login_task
        self.assertEqual(len(self.http.posts("/deviceauth/usercode")), 1)


class WeComTests(unittest.IsolatedAsyncioTestCase):
    def test_parse_text_and_voice(self):
        base = {"cmd": "aibot_msg_callback", "headers": {"req_id": "r"}}
        text = parse_callback({**base, "body": {"msgid": "1", "from": {"userid": "u"},
                                                "msgtype": "text", "text": {"content": " hi "}}})
        self.assertEqual((text.chat_id, text.sender, text.text, text.is_group), ("u", "u", "hi", False))
        voice = parse_callback({**base, "body": {"msgid": "2", "from": {"userid": "u"},
                                                 "msgtype": "voice", "voice": {"content": "说话"}}})
        self.assertEqual(voice.text, "说话")
        self.assertIsNone(parse_callback({**base, "body": {"msgid": "3"}}))

    def test_chunks_respect_char_and_byte_limits_lossless(self):
        for text in ("a" * 9000, "中" * 9000, ("第一行\n" + "字" * 3000) * 4):
            parts = chunks(text)
            self.assertTrue(all(len(p) <= MAX_CHUNK_CHARS and
                                len(p.encode()) <= MAX_CHUNK_BYTES for p in parts))
            self.assertEqual("".join(parts), text.strip())

    async def test_proactive_rejection_falls_back_to_passive_reply(self):
        client = WeComClient("bot", "secret", None)
        client.connected = True
        client._last_req_id["owner"] = "req-in"
        calls = []

        async def fake_request(cmd, req_id, body):
            calls.append((cmd, req_id))
            return {"errcode": 1} if cmd == "aibot_send_msg" else {"errcode": 0}
        client._request = fake_request
        self.assertEqual(await client.send_chunk("owner", "hi"), OK)
        self.assertEqual([c for c, _ in calls], ["aibot_send_msg", "aibot_respond_msg"])
        self.assertEqual(calls[1][1], "req-in")

    async def test_send_when_disconnected_is_unavailable(self):
        client = WeComClient("bot", "secret", None)
        self.assertEqual(await client.send_chunk("owner", "hi"), UNAVAILABLE)


class DotParsingTests(unittest.TestCase):
    def test_timestamps(self):
        self.assertEqual(timestamp_of({"created_at": 1_700_000_000}), 1_700_000_000.0)
        self.assertEqual(timestamp_of({"created_at": 1_700_000_000_000}), 1_700_000_000.0)
        self.assertEqual(timestamp_of({"created_at": "2023-11-14T22:13:20Z"}), 1_700_000_000.0)
        self.assertIsNone(timestamp_of({"created_at": True}))

    def test_oldest_first(self):
        items = [{"id": "b", "created_at": 2}, {"id": "a", "created_at": "1970-01-01T00:00:01Z"}]
        self.assertEqual([i["id"] for i in oldest_first(items)], ["a", "b"])


if __name__ == "__main__":
    unittest.main()
