"""Push listener: frame handling, URL validation, bridge wake-ups, and a real socket cycle."""
import asyncio
import json
import socket
import unittest

from dot_bridge.live import TOPIC, DotLive, valid_socket_url

try:
    import aiohttp
    from aiohttp import web
except ImportError:  # pragma: no cover - the container always has python3-aiohttp
    aiohttp = None


def event(kind, room, offset=None, **payload):
    row = {"type": "message", "topic_id": TOPIC,
           "payload": {"type": kind, "payload": {"room_id": room, **payload}}}
    if offset is not None:
        row["offset"] = offset
    return row


class Recorder:
    def __init__(self):
        self.events, self.resyncs = [], 0

    def on_event(self, room):
        self.events.append(room)

    def on_resync(self):
        self.resyncs += 1


class FrameTests(unittest.TestCase):
    def setUp(self):
        self.rec = Recorder()
        self.live = DotLive(None, self.rec.on_event, self.rec.on_resync)

    def test_commands_connect_background_then_subscribe(self):
        cmds = json.loads(self.live.commands())
        self.assertEqual(cmds[0]["command"], {"type": "connect",
                                              "presence": {"type": "presence", "state": "background"}})
        self.assertEqual(cmds[1], {"id": 2, "command": {"type": "subscribe", "topic_id": TOPIC}})
        self.live.offset = "42"
        self.assertEqual(json.loads(self.live.commands())[1]["command"]["offset"], "42")

    def test_subscribe_reply_with_recovered_catchups(self):
        frame = [{"id": 1, "reply": {"type": "connect"}},
                 {"id": 2, "reply": {"type": "subscribe", "topic_id": TOPIC, "recovered": True,
                                     "catchups": [event("calpico-message-add", "room1", "7")],
                                     "last_offset": "9"}}]
        self.assertTrue(self.live.handle(json.dumps(frame)))
        self.assertEqual(self.rec.events, ["room1"])
        self.assertEqual(self.rec.resyncs, 1)
        self.assertEqual(self.live.offset, "9")

    def test_room_events_trigger_and_noise_is_ignored(self):
        frame = [event("calpico-message-add", "room1", "10"),
                 event("calpico-message-update", "room2", "11"),
                 event("calpico-is-responding-heartbeat", "room1", "12", typing=True),
                 {"type": "message", "topic_id": "other-topic",
                  "payload": {"type": "calpico-message-add", "payload": {"room_id": "room1"}}},
                 {"type": "message", "topic_id": TOPIC,
                  "payload": {"type": "calpico-room-update", "payload": {"room": {"id": "room3"}}}}]
        self.assertFalse(self.live.handle(json.dumps(frame)))
        self.assertEqual(self.rec.events, ["room1", "room2", "room3"])
        self.assertEqual(self.live.offset, "12")
        self.assertEqual(self.rec.resyncs, 0)

    def test_garbage_frames_are_harmless(self):
        for raw in ("not json", "{}", "[1, 2]", json.dumps([{"type": "message"}])):
            self.assertFalse(self.live.handle(raw))
        self.assertEqual(self.rec.events, [])

    def test_socket_url_validation(self):
        good = "wss://ws.chatgpt.com/p36/ws/user/user-abc?verify=x"
        self.assertTrue(valid_socket_url(good))
        self.assertTrue(valid_socket_url("wss://ws.chatgpt.com/p0/ws/user/user-abc"))
        for bad in ("ws://ws.chatgpt.com/p36/ws/user/u", "wss://evil.example/p36/ws/user/u",
                    "wss://x@ws.chatgpt.com/p36/ws/user/u", "wss://ws.chatgpt.com:444/p36/ws/user/u",
                    "wss://ws.chatgpt.com/p36/ws/user/u/extra", None):
            self.assertFalse(valid_socket_url(bad), bad)


class BridgeWakeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import tempfile
        from pathlib import Path
        from dot_bridge.dot import DotClient
        from dot_bridge.openai_auth import Auth, TokenFile
        from dot_bridge.service import Bridge
        from dot_bridge.store import Store
        self.tmp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.tmp.name) / "b.sqlite3")
        auth = Auth(TokenFile(Path(self.tmp.name) / "o.json"), None)
        self.bridge = Bridge(self.store, auth, DotClient(None, auth))
        self.store.put("dot", {"room_id": "room1", "account_id": "a", "dot_id": "d",
                               "dot_members": ["u-dot"], "human_members": ["u-me"], "name": "dot"})

    async def asyncTearDown(self):
        self.store.close()
        self.tmp.cleanup()

    async def test_only_bound_room_events_wake_the_poller(self):
        self.bridge.on_live_event("room2")
        self.assertFalse(self.bridge._poll_wake.is_set())
        self.bridge.on_live_event("room1")
        self.assertTrue(self.bridge._poll_wake.is_set())

    async def test_cadence_with_live_connected(self):
        from dot_bridge import service
        self.bridge.live = DotLive(None, None, None)
        self.bridge._last_activity = self.bridge.clock()
        self.assertEqual(self.bridge._poll_interval(), service.ACTIVE_POLL_SECONDS)
        self.bridge.live.connected = True
        self.assertEqual(self.bridge._poll_interval(), service.RECOVERY_POLL_SECONDS)
        self.store.apply_reply_ops("room1", [("pending", "d1", "x", False)], None)
        self.assertEqual(self.bridge._poll_interval(), service.ACTIVE_POLL_SECONDS)


class PollRetryTests(BridgeWakeTests):
    async def test_failed_read_after_event_retries_fast_then_returns_to_recovery(self):
        from dot_bridge import service
        from dot_bridge.http import NetworkError
        self.bridge.live = DotLive(None, None, None)
        self.bridge.live.connected = True
        outcomes = iter([NetworkError("blip"), NetworkError("blip"), None])
        delays = []

        async def poll():
            result = next(outcomes)
            if result is not None:
                raise result

        async def wait(event, timeout):
            delays.append(timeout)
            if len(delays) == 3:
                raise asyncio.CancelledError

        self.bridge.poll_replies_once = poll
        self.bridge._wait = wait
        with self.assertRaises(asyncio.CancelledError):
            await self.bridge.reply_loop()
        self.assertEqual(delays, [2.0, 4.0, service.RECOVERY_POLL_SECONDS])
        self.assertEqual(self.bridge._retry_delay(10), service.POLL_RETRY_MAX_SECONDS)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipIf(aiohttp is None, "aiohttp not installed")
class LiveSocketIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_event_reconnect_resumes_from_offset(self):
        subscribes = []

        async def handler(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            async for msg in ws:
                cmds = json.loads(msg.data)
                subscribes.append(cmds[1]["command"])
                await ws.send_str(json.dumps([
                    {"id": 1, "reply": {"type": "connect"}},
                    {"id": 2, "reply": {"type": "subscribe", "topic_id": TOPIC,
                                        "recovered": False, "last_offset": "10"}}]))
                await ws.send_str(json.dumps([event("calpico-message-add", "room1", "11"),
                                              event("calpico-message-add", "room-x", "12")]))
                if len(subscribes) == 1:
                    await ws.close()  # force a reconnect
            return ws

        app = web.Application()
        app.router.add_get("/p36/ws/user/u", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        port = free_port()
        await web.TCPSite(runner, "127.0.0.1", port).start()

        class FakeDot:
            async def live_url(self):
                return f"ws://127.0.0.1:{port}/p36/ws/user/u?verify=secret"

        rec = Recorder()
        live = DotLive(FakeDot(), rec.on_event, rec.on_resync,
                       validate=lambda u: u.startswith("ws://127.0.0.1:"))
        task = asyncio.create_task(live.run())
        try:
            for _ in range(100):
                if len(subscribes) >= 2 and live.connected and rec.events.count("room1") >= 2:
                    break
                await asyncio.sleep(0.05)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runner.cleanup()
        self.assertEqual(subscribes[0], {"type": "subscribe", "topic_id": TOPIC})
        self.assertEqual(subscribes[1].get("offset"), "12")  # resumed from the last seen offset
        self.assertGreaterEqual(rec.resyncs, 2)              # each (re)subscribe resyncs
        self.assertIn("room-x", rec.events)                  # the bridge, not the listener, filters rooms


if __name__ == "__main__":
    unittest.main()
