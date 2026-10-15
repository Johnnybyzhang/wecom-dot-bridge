"""Real aiohttp client code against local aiohttp servers (skipped without aiohttp)."""
import asyncio
import json
import socket
import unittest

try:
    import aiohttp
    from aiohttp import web
except ImportError:  # pragma: no cover - the container always has python3-aiohttp
    aiohttp = None

from dot_bridge.http import AiohttpHttp, ConnectError
from dot_bridge.wecom import OK, WeComClient


class LocalHttp(AiohttpHttp):
    @staticmethod
    def check_url(url):
        if not url.startswith("http://127.0.0.1:"):
            raise ValueError("test only")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipIf(aiohttp is None, "aiohttp not installed")
class HttpIntegration(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.big = {"items": [{"id": f"m{i}", "content": {"text": "字" * 200}} for i in range(300)]}

        async def chunked(request):
            resp = web.StreamResponse()
            resp.enable_chunked_encoding()
            await resp.prepare(request)
            raw = json.dumps(self.big).encode()
            for i in range(0, len(raw), 1000):
                await resp.write(raw[i:i + 1000])
                await asyncio.sleep(0)
            await resp.write_eof()
            return resp

        async def redirect(request):
            raise web.HTTPFound("http://example.invalid/")

        app = web.Application()
        app.router.add_get("/big", chunked)
        app.router.add_get("/redirect", redirect)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.port = free_port()
        await web.TCPSite(self.runner, "127.0.0.1", self.port).start()
        self.http = LocalHttp()

    async def asyncTearDown(self):
        await self.http.close()
        await self.runner.cleanup()

    async def test_chunked_body_read_completely(self):
        resp = await self.http.request("GET", f"http://127.0.0.1:{self.port}/big")
        self.assertEqual(resp.status, 200)
        self.assertEqual(resp.body, self.big)

    async def test_redirects_not_followed(self):
        resp = await self.http.request("GET", f"http://127.0.0.1:{self.port}/redirect")
        self.assertEqual(resp.status, 302)

    async def test_refused_connection_is_connect_error(self):
        with self.assertRaises(ConnectError):
            await self.http.request("GET", f"http://127.0.0.1:{free_port()}/x")

    async def test_production_allowlist(self):
        with self.assertRaises(ValueError):
            await AiohttpHttp().request("GET", "https://example.com/")
        with self.assertRaises(ValueError):
            await AiohttpHttp().request("GET", "http://chatgpt.com/")


@unittest.skipIf(aiohttp is None, "aiohttp not installed")
class WeComIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_subscribe_callback_send_and_kick(self):
        frames, connections = [], []

        async def ws_handler(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            connections.append(ws)
            async for msg in ws:
                payload = json.loads(msg.data)
                frames.append(payload)
                req_id = payload["headers"]["req_id"]
                if payload["cmd"] == "aibot_subscribe":
                    await ws.send_json({"headers": {"req_id": req_id}, "errcode": 0})
                    await ws.send_json({"cmd": "aibot_msg_callback", "headers": {"req_id": "in-1"},
                                        "body": {"msgid": "m1", "from": {"userid": "u"},
                                                 "msgtype": "text", "text": {"content": "hi"}}})
                elif payload["cmd"] == "aibot_send_msg":
                    await ws.send_json({"headers": {"req_id": req_id}, "errcode": 0})
                    await ws.send_json({"cmd": "aibot_event_callback", "headers": {"req_id": "e"},
                                        "body": {"event_type": "disconnected_event"}})
            return ws

        app = web.Application()
        app.router.add_get("/", ws_handler)
        runner = web.AppRunner(app)
        await runner.setup()
        port = free_port()
        await web.TCPSite(runner, "127.0.0.1", port).start()
        received = asyncio.Queue()

        async def on_message(inbound):
            await received.put(inbound)

        client = WeComClient("bot-1", "s3cret", on_message, ws_url=f"ws://127.0.0.1:{port}/")
        task = asyncio.create_task(client.run())
        try:
            inbound = await asyncio.wait_for(received.get(), 5)
            self.assertEqual((inbound.sender, inbound.text, inbound.req_id), ("u", "hi", "in-1"))
            self.assertTrue(client.connected)
            self.assertEqual(frames[0]["body"]["bot_id"], "bot-1")
            self.assertEqual(await client.send_chunk("u", "**hello**"), OK)
            send = [f for f in frames if f["cmd"] == "aibot_send_msg"][0]
            self.assertEqual(send["body"], {"chatid": "u", "msgtype": "markdown",
                                            "markdown": {"content": "**hello**"}})
            # Kicked by another client: run() returns for good and never reconnects.
            await asyncio.wait_for(task, 5)
            self.assertTrue(client.kicked)
            self.assertFalse(client.connected)
            self.assertEqual(len(connections), 1)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await runner.cleanup()


if __name__ == "__main__":
    unittest.main()
