"""Quote replies in both directions, and the owner-notice language setting."""
import unittest

from test_bridge import DOT, ROOM, Harness, msg
from dot_bridge.http import Response
from dot_bridge.store import Store
from dot_bridge.wecom import Inbound, parse_callback


def quoted(text, quote_text, quote_type="text", msgid="q1"):
    return Inbound(msgid, "req-" + msgid, "owner", "owner", False, "text", text, (),
                   {"type": quote_type, "text": quote_text})


class QuoteParsingTests(unittest.TestCase):
    def test_quote_block_is_parsed(self):
        cb = parse_callback({"cmd": "aibot_msg_callback", "headers": {"req_id": "r"}, "body": {
            "msgid": "1", "from": {"userid": "u"}, "msgtype": "text", "text": {"content": "why?"},
            "quote": {"msgtype": "text", "text": {"content": "The meeting moved to 3pm."}}}})
        self.assertEqual(cb.quote, {"type": "text", "text": "The meeting moved to 3pm."})
        img = parse_callback({"cmd": "aibot_msg_callback", "headers": {"req_id": "r"}, "body": {
            "msgid": "2", "from": {"userid": "u"}, "msgtype": "text", "text": {"content": "this"},
            "quote": {"msgtype": "image", "image": {"url": "https://x/i"}}}})
        self.assertEqual(img.quote, {"type": "image", "text": ""})

    def test_quote_key_ignores_markdown_and_spacing(self):
        self.assertEqual(Store.quote_key("**Hello**,  _world_!\n> ok"), Store.quote_key("Hello world ok"))


class QuoteReplyTests(Harness):
    async def test_quoting_a_forwarded_dot_reply_becomes_a_native_reply(self):
        self.ready()
        self.http.history[:] = [{"id": "d1", "account_user_id": "u-dot",
                                 "content": {"text": "**The meeting** moved to 3pm."}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.send_route(lambda json_body, **_: Response(200, {"id": "r1", **json_body}))
        await self.bridge.on_message(quoted("Can we do 4pm?", "The meeting moved to 3pm."))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        body = self.http.posts("/messages")[0][2]
        self.assertEqual(body["reply_to"], {"message_id": "d1"})
        self.assertEqual(body["content"]["text"], "Can we do 4pm?")

    async def test_quoting_own_earlier_message_maps_to_its_dot_message(self):
        self.ready()
        replies = iter([Response(200, {"id": "u1"}), Response(200, {"id": "u2"})])
        self.send_route(lambda **_: next(replies))
        await self.bridge.on_message(msg("Remind me to buy milk", msgid="m1"))
        await self.bridge.deliver_once()
        await self.bridge.on_message(quoted("and eggs", "Remind me to buy milk", msgid="m2"))
        await self.bridge.deliver_once()
        self.assertEqual(self.http.posts("/messages")[1][2]["reply_to"], {"message_id": "u1"})

    async def test_unmatched_quote_travels_as_context(self):
        self.ready()
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        await self.bridge.on_message(quoted("What about this?", "something said elsewhere"))
        await self.bridge.deliver_once()
        body = self.http.posts("/messages")[0][2]
        self.assertNotIn("reply_to", body)
        self.assertEqual(body["content"]["text"], "> something said elsewhere\n\nWhat about this?")

    async def test_quoted_image_without_text_is_labelled(self):
        self.ready()
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        await self.bridge.on_message(quoted("Is this the right one?", "", quote_type="image"))
        await self.bridge.deliver_once()
        self.assertEqual(self.http.posts("/messages")[0][2]["content"]["text"],
                         "> [image]\n\nIs this the right one?")

    async def test_dot_reply_to_older_message_shows_quote_line(self):
        self.ready()
        self.http.history[:] = [
            {"id": "h1", "account_user_id": "u-me", "content": {"text": "first question"}},
            {"id": "h2", "account_user_id": "u-me", "content": {"text": "second question"}},
            {"id": "d1", "account_user_id": "u-dot", "content": {"text": "answer to first"},
             "reply_to": {"message_id": "h1", "message": {"content": {"text": "first question"}}}},
            {"id": "d2", "account_user_id": "u-dot", "content": {"text": "answer to second"},
             "reply_to": {"message_id": "h2", "message": {"content": {"text": "second question"}}}}]
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        await self.bridge.forward_once()
        await self.bridge.forward_once()
        self.assertEqual(self.wecom.texts(), ["> first question\n\nanswer to first", "answer to second"])


class AstraQuoteFindingTests(Harness):
    async def test_shorter_later_message_does_not_capture_the_quote(self):
        self.ready()
        replies = iter([Response(200, {"id": "u1"}), Response(200, {"id": "u2"}), Response(200, {"id": "u3"})])
        self.send_route(lambda **_: next(replies))
        await self.bridge.on_message(msg("Please summarize the deployment logs", msgid="m1"))
        await self.bridge.deliver_once()
        await self.bridge.on_message(msg("Please summarize", msgid="m2"))
        await self.bridge.deliver_once()
        await self.bridge.on_message(quoted("the logs one", "Please summarize the deployment logs", msgid="m3"))
        await self.bridge.deliver_once()
        self.assertEqual(self.http.posts("/messages")[2][2]["reply_to"], {"message_id": "u1"})

    async def test_truncated_quote_maps_only_when_unambiguous(self):
        self.ready()
        self.store.remember_quote("The deployment finished at 3pm with two warnings", "d1")
        self.assertEqual(self.store.find_quote("The deployment finished"), "d1")
        self.store.remember_quote("The deployment finished at 5pm without warnings", "d2")
        self.assertIsNone(self.store.find_quote("The deployment finished"))   # ambiguous
        self.assertEqual(self.store.find_quote("The deployment finished at 5pm without warnings"), "d2")
        self.assertIsNone(self.store.find_quote("The deployment finished at 3pm with two warnings and more"))

    async def test_identical_texts_for_two_messages_fall_back_to_context(self):
        self.ready()
        self.store.remember_quote("ok", "d1")
        self.store.remember_quote("okay then", "d1")
        self.store.remember_quote("okay then", "d2")
        self.assertIsNone(self.store.find_quote("okay then"))


class LanguageTests(Harness):
    async def test_default_english_and_switch_to_chinese(self):
        self.ready()
        await self.bridge.on_message(msg("/help", msgid="h1"))
        self.assertIn("dot bridge commands", self.wecom.texts()[-1])
        await self.bridge.on_message(msg("/lang zh", msgid="l1"))
        self.assertIn("中文", self.wecom.texts()[-1])
        await self.bridge.on_message(msg("/status", msgid="s1"))
        self.assertIn("桥接状态", self.wecom.texts()[-1])
        await self.bridge.on_message(msg("/lang en", msgid="l2"))
        await self.bridge.on_message(msg("/status", msgid="s2"))
        self.assertIn("dot bridge status", self.wecom.texts()[-1])

    async def test_chinese_mode_translates_errors_and_states(self):
        self.ready()
        self.store.put("lang", "zh")
        await self.bridge.on_message(msg("/use nonexistent", msgid="u1"))
        self.assertIn("没有该 id 的 dot", self.wecom.texts()[-1])
        self.store.enqueue("x1", "owner", "owner", "hi")
        await self.bridge.on_message(msg("/status", msgid="s1"))
        self.assertIn("待发送 1", self.wecom.texts()[-1])

    async def test_unknown_language_shows_usage(self):
        self.ready()
        await self.bridge.on_message(msg("/lang fr"))
        self.assertIn("/lang en", self.wecom.texts()[-1])
        self.assertIsNone(self.store.get("lang"))

    async def test_every_message_has_both_languages(self):
        from dot_bridge import i18n
        for key, pair in i18n.M.items():
            self.assertEqual(len(pair), 2, key)
            self.assertTrue(all(pair), key)
