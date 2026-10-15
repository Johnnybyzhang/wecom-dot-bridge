"""Two-way file transfer: WeCom images/files -> dot uploads, dot attachments -> WeCom."""
import base64
import json
import os
import socket
import sqlite3
import tempfile
import unittest
from pathlib import Path

from test_bridge import ACCOUNT, ROOM, FakeHttp, FakeWeCom, Harness, msg
from dot_bridge.http import FetchError, FileTooLarge, NetworkError, Response
from dot_bridge.service import Bridge
from dot_bridge.store import Store
from dot_bridge.wecom import (MAX_UPLOAD_CHUNKS, OK, REJECTED, UPLOAD_CHUNK_BYTES, Inbound,
                              WeComClient, decrypt_media, parse_callback)

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
except ImportError:  # pragma: no cover
    Cipher = None
try:
    import aiohttp
    from aiohttp import web
except ImportError:  # pragma: no cover
    aiohttp = None


def encrypt(data: bytes, key: bytes) -> bytes:
    pad = 32 - len(data) % 32
    enc = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    return enc.update(data + bytes([pad]) * pad) + enc.finalize()


class FileHttp(FakeHttp):
    """FakeHttp plus `fetch` for unauthenticated file downloads."""

    def __init__(self):
        super().__init__()
        self.files = {}  # url -> (bytes, headers) | Exception
        self.fetched = []

    async def fetch(self, url, *, host_ok, max_bytes=20 * 1024 * 1024, timeout=120):
        self.fetched.append(url)
        result = self.files.get(url, FetchError("404"))
        if isinstance(result, Exception):
            raise result
        return result


class FileWeCom(FakeWeCom):
    def __init__(self):
        super().__init__()
        self.files_sent = []
        self.file_results = []

    async def send_file(self, chat_id, data, name, mime):
        if not self.connected:
            return "unavailable"
        result = self.file_results.pop(0) if self.file_results else OK
        if result == OK:
            self.files_sent.append((chat_id, name, mime, data))
        return result


def media_msg(media, text="", msgid="f1", msgtype="file"):
    return Inbound(msgid, "req-" + msgid, "owner", "owner", False, msgtype, text, tuple(media))


class FileHarness(Harness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        files_http = FileHttp()
        files_http.routes, files_http.history = self.http.routes, self.http.history
        self.http = files_http
        self.bridge.dot.http = files_http
        self.auth.http = files_http
        self.wecom = self.bridge.wecom = FileWeCom()
        self.uploads = []

        def upload(**kw):
            self.uploads.append(kw)
            return Response(200, {"id": f"file-{len(self.uploads)}", "status": "uploaded"})
        self.http.on("POST", f"/rooms/{ROOM}/files", upload)


class SchemaMigrationTests(unittest.TestCase):
    def test_v1_database_gains_file_columns_in_place(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "b.sqlite3"
            db = sqlite3.connect(path)
            db.executescript("""
                CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, msgid TEXT NOT NULL UNIQUE,
                  chat_id TEXT NOT NULL, sender TEXT NOT NULL, text TEXT NOT NULL,
                  request_id TEXT NOT NULL UNIQUE, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                  available_at REAL NOT NULL, sending_at REAL, dest_room TEXT, remote_id TEXT, error TEXT,
                  created_at REAL NOT NULL, updated_at REAL NOT NULL);
                CREATE TABLE replies (remote_id TEXT PRIMARY KEY, state TEXT NOT NULL, text TEXT,
                  parts_sent INTEGER NOT NULL DEFAULT 0, pending_since REAL, empty_since REAL,
                  updated_at REAL NOT NULL);
                CREATE TABLE handled (msgid TEXT PRIMARY KEY, at REAL NOT NULL);
                INSERT INTO inbox(msgid,chat_id,sender,text,request_id,state,available_at,created_at,updated_at)
                  VALUES('m1','c','s','old','r1','delivered',0,0,0);
                PRAGMA user_version=1;""")
            db.close()
            os.chmod(path, 0o600)
            store = Store(path)
            try:
                self.assertEqual(store.db.execute("PRAGMA user_version").fetchone()[0], 3)
                self.assertIsNone(store.item(1).reply_to)
                self.assertEqual(store.item(1).text, "old")
                self.assertEqual(store.item(1).files, ())
                store.queue_reply("d1", "x")
                self.assertEqual(store.next_reply(600).files, ())
            finally:
                store.close()


class WeComMediaParsingTests(unittest.TestCase):
    def cb(self, body):
        return parse_callback({"cmd": "aibot_msg_callback", "headers": {"req_id": "r"},
                               "body": {"msgid": "1", "from": {"userid": "u"}, **body}})

    def test_image_file_appmsg_and_mixed(self):
        img = self.cb({"msgtype": "image", "image": {"url": "https://x.qq.com/i", "aeskey": "k%2F1"}})
        self.assertEqual(img.media, ({"kind": "image", "url": "https://x.qq.com/i", "aeskey": "k/1"},))
        f = self.cb({"msgtype": "file", "file": {"url": "https://x.qq.com/f", "aeskey": "k"}})
        self.assertEqual(f.media[0]["kind"], "file")
        app = self.cb({"msgtype": "appmsg", "appmsg": {"title": "a.pdf", "file": {"url": "https://x/f"}}})
        self.assertEqual(app.media[0]["kind"], "file")
        mixed = self.cb({"msgtype": "mixed", "mixed": {"msg_item": [
            {"msgtype": "text", "text": {"content": "看图"}},
            {"msgtype": "image", "image": {"url": "https://x/1"}},
            {"msgtype": "image", "image": {"url": "https://x/2"}}]}})
        self.assertEqual((mixed.text, len(mixed.media)), ("看图", 2))
        self.assertEqual(self.cb({"msgtype": "text", "text": {"content": "hi"}}).media, ())


@unittest.skipIf(Cipher is None, "cryptography not installed")
class DecryptTests(unittest.TestCase):
    def test_round_trip_and_rejects_bad_key(self):
        key = os.urandom(32)
        aeskey = base64.b64encode(key).decode().rstrip("=")
        data = "文件内容".encode() * 100
        self.assertEqual(decrypt_media(encrypt(data, key), aeskey), data)
        with self.assertRaises(ValueError):
            decrypt_media(encrypt(data, key), base64.b64encode(b"short").decode())
        self.assertEqual(decrypt_media(b"plain", ""), b"plain")


class InboundFileTests(FileHarness):
    async def test_file_uploaded_then_posted_with_attachment(self):
        self.ready()
        self.http.files["https://x.qq.com/f"] = (b"%PDF-1.7 data", {
            "content-disposition": "attachment; filename*=UTF-8''%E5%91%A8%E6%8A%A5.pdf"})
        self.send_route(lambda json_body, **_: Response(200, {"id": "r", **json_body}))
        await self.bridge.on_message(media_msg([{"kind": "file", "url": "https://x.qq.com/f", "aeskey": ""}]))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(self.uploads[0]["upload"].filename, "周报.pdf")
        self.assertEqual(self.uploads[0]["upload"].mime, "application/pdf")
        body = self.http.posts("/messages")[0][2]
        self.assertEqual(body["content"]["attachments"], [{"type": "file", "file_id": "file-1"}])
        self.assertEqual(body["content"]["text"], "📎 周报.pdf")
        self.assertEqual(self.store.item(1).files, ())  # media refs (with keys) dropped after delivery

    async def test_text_and_images_together(self):
        self.ready()
        png = b"\x89PNG\r\n\x1a\n...."
        self.http.files.update({"https://x/1": (png, {}), "https://x/2": (png, {})})
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        await self.bridge.on_message(media_msg([{"kind": "image", "url": "https://x/1", "aeskey": ""},
                                                {"kind": "image", "url": "https://x/2", "aeskey": ""}],
                                               text="看这两张", msgtype="mixed"))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        body = self.http.posts("/messages")[0][2]
        self.assertEqual(body["content"]["text"], "看这两张")
        self.assertEqual(len(body["content"]["attachments"]), 2)
        self.assertEqual(self.uploads[0]["upload"].mime, "image/png")

    async def test_retry_after_429_reuses_uploaded_files(self):
        self.ready()
        self.http.files["https://x/f"] = (b"data", {})
        replies = iter([Response(429), Response(200, {"id": "r"})])
        self.send_route(lambda **_: next(replies))
        await self.bridge.on_message(media_msg([{"kind": "file", "url": "https://x/f", "aeskey": ""}]))
        self.assertEqual(await self.bridge.deliver_once(), "retry_later")
        self.clock.now += 31
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(len(self.uploads), 1)
        self.assertEqual(len(self.http.fetched), 1)

    async def test_upload_failures_retry_then_fail_visibly(self):
        self.ready()
        self.http.files["https://x/f"] = NetworkError("blip")
        await self.bridge.on_message(media_msg([{"kind": "file", "url": "https://x/f", "aeskey": ""}]))
        self.assertEqual(await self.bridge.deliver_once(), "retry_later")
        self.clock.now += 31
        self.assertEqual(await self.bridge.deliver_once(), "retry_later")
        self.clock.now += 31
        self.assertEqual(await self.bridge.deliver_once(), "failed")
        self.assertEqual(self.http.posts("/messages"), [])
        self.assertIn("couldn't be uploaded", self.wecom.texts()[-1])

    async def test_oversize_wecom_file_is_refused(self):
        self.ready()
        self.http.files["https://x/f"] = FileTooLarge()
        await self.bridge.on_message(media_msg([{"kind": "file", "url": "https://x/f", "aeskey": ""}]))
        self.assertEqual(await self.bridge.deliver_once(), "failed")
        self.assertIn("20 MB", self.wecom.texts()[-1])

    async def test_unsupported_message_without_media_still_answers(self):
        self.ready()
        await self.bridge.on_message(Inbound("v1", "r", "owner", "owner", False, "video", ""))
        self.assertEqual(self.store.counts(), {})
        self.assertIn("images and files", self.wecom.texts()[-1])


class OutboundFileTests(FileHarness):
    def dot_file_msg(self, rid="d1", text="", files=(("f1", "report.pdf"),)):
        return {"id": rid, "account_user_id": "u-dot", "content": {"text": text, "attachments": [
            {"attachment_id": "a-" + fid, "type": "file", "file_id": fid,
             "file": {"id": fid, "name": name, "mime_type": "application/octet-stream",
                      "status": "ROOM_PUBLISHED"}} for fid, name in files]}}

    def serve_dot_file(self, fid, data, name):
        url = f"https://sdmntpr.oaiusercontent.com/{fid}"
        self.http.on("GET", f"/rooms/{ROOM}/files/{fid}", lambda **_: Response(
            200, {"id": fid, "file_id": fid, "status": "ROOM_PUBLISHED", "download_url": url,
                  "name": name, "mime_type": "application/octet-stream"}))
        self.http.files[url] = (data, {})

    async def poll_twice(self):
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()

    async def test_file_only_reply_forwarded_as_file(self):
        self.ready()
        self.http.history[:] = [self.dot_file_msg()]
        self.serve_dot_file("f1", b"%PDF", "report.pdf")
        await self.poll_twice()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.sent, [])  # no empty text part
        self.assertEqual(self.wecom.files_sent, [("owner", "report.pdf", "application/pdf", b"%PDF")])

    async def test_text_then_files_in_order(self):
        self.ready()
        self.http.history[:] = [self.dot_file_msg(text="给你文件", files=(("f1", "a.txt"), ("f2", "b.png")))]
        self.serve_dot_file("f1", b"A", "a.txt")
        self.serve_dot_file("f2", b"\x89PNG\r\n\x1a\n", "b.png")
        await self.poll_twice()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts(), ["给你文件"])
        self.assertEqual([f[1:3] for f in self.wecom.files_sent],
                         [("a.txt", "text/plain"), ("b.png", "image/png")])

    async def test_file_progress_survives_restart(self):
        self.ready()
        self.http.history[:] = [self.dot_file_msg(files=(("f1", "a.txt"), ("f2", "b.txt")))]
        self.serve_dot_file("f1", b"A", "a.txt")
        self.serve_dot_file("f2", b"B", "b.txt")
        await self.poll_twice()
        self.wecom.file_results = [OK, "unavailable"]
        self.assertEqual(await self.bridge.forward_once(), "unavailable")
        restarted = Bridge(self.store, self.auth, self.bridge.dot, clock=self.clock)
        restarted.wecom = self.wecom
        self.assertEqual(await restarted.forward_once(), "forwarded")
        self.assertEqual([f[1] for f in self.wecom.files_sent], ["a.txt", "b.txt"])

    async def test_oversize_dot_file_becomes_a_notice(self):
        self.ready()
        self.http.history[:] = [self.dot_file_msg(files=(("f1", "huge.zip"),))]
        self.http.on("GET", f"/rooms/{ROOM}/files/f1", lambda **_: Response(
            200, {"id": "f1", "download_url": "https://x.oaiusercontent.com/f1", "name": "huge.zip"}))
        self.http.files["https://x.oaiusercontent.com/f1"] = FileTooLarge()
        await self.poll_twice()
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertIn("huge.zip", self.wecom.texts()[-1])
        self.assertIn("20 MB", self.wecom.texts()[-1])

    async def test_unavailable_dot_file_retried_then_notice(self):
        self.ready()
        self.http.history[:] = [self.dot_file_msg(), self.dot_file_msg("d2", text="next", files=())]
        self.http.on("GET", f"/rooms/{ROOM}/files/f1", lambda **_: Response(404, {"detail": "gone"}))
        await self.poll_twice()
        results = [await self.bridge.forward_once() for _ in range(5)]
        self.assertEqual(results, [REJECTED] * 4 + ["forwarded"])
        self.assertIn("couldn't be forwarded", self.wecom.texts()[-1])
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(self.wecom.texts()[-1], "next")


class AstraFileFindingTests(FileHarness):
    def dot_msg(self):
        return {"id": "d1", "account_user_id": "u-dot", "content": {"text": "", "attachments": [
            {"type": "file", "file_id": "f1", "file": {"id": "f1", "name": "huge.zip"}}]}}

    async def test_file_not_skipped_until_its_notice_is_delivered(self):
        self.ready()
        self.http.history[:] = [self.dot_msg()]
        self.http.on("GET", f"/rooms/{ROOM}/files/f1", lambda **_: Response(
            200, {"id": "f1", "download_url": "https://x.oaiusercontent.com/f1", "name": "huge.zip"}))
        self.http.files["https://x.oaiusercontent.com/f1"] = FileTooLarge()
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        self.wecom.connected = True
        self.wecom.results = [REJECTED]          # the notice itself fails once
        self.assertEqual(await self.bridge.forward_once(), "unavailable")
        self.assertEqual(self.store.reply_state("d1"), "queued")
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertIn("huge.zip", self.wecom.texts()[-1])

    async def test_failed_file_notice_retried_without_redownloading(self):
        self.ready()
        self.http.history[:] = [self.dot_msg()]
        self.http.on("GET", f"/rooms/{ROOM}/files/f1", lambda **_: Response(404))
        await self.bridge.poll_replies_once()
        await self.bridge.poll_replies_once()
        for _ in range(4):
            await self.bridge.forward_once()
        self.wecom.results = [REJECTED]          # 5th failure -> notice, which fails
        self.assertEqual(await self.bridge.forward_once(), "unavailable")
        gets = len([c for c in self.http.calls if c[1].endswith("/files/f1")])
        self.assertEqual(await self.bridge.forward_once(), "forwarded")
        self.assertEqual(len([c for c in self.http.calls if c[1].endswith("/files/f1")]), gets)
        self.assertIn("couldn't be forwarded", self.wecom.texts()[-1])

    @unittest.skipIf(Cipher is None, "cryptography not installed")
    async def test_exactly_20_mib_encrypted_wecom_file_is_accepted(self):
        from dot_bridge.http import MAX_FILE_BYTES
        self.ready()
        key = os.urandom(32)
        plain = b"x" * MAX_FILE_BYTES
        enc = encrypt(plain, key)
        self.assertGreater(len(enc), MAX_FILE_BYTES)
        limits = []

        async def fetch(url, *, host_ok, max_bytes=MAX_FILE_BYTES, timeout=120):
            limits.append(max_bytes)
            if len(enc) > max_bytes:
                raise FileTooLarge()
            return enc, {}
        self.http.fetch = fetch
        self.send_route(lambda **_: Response(200, {"id": "r"}))
        await self.bridge.on_message(media_msg([{"kind": "file", "url": "https://x/f",
                                                 "aeskey": base64.b64encode(key).decode().rstrip("=")}]))
        self.assertEqual(await self.bridge.deliver_once(), "delivered")
        self.assertEqual(len(self.uploads[0]["upload"].data), MAX_FILE_BYTES)


class WeComSendFileTests(unittest.IsolatedAsyncioTestCase):
    async def test_upload_protocol_and_kind(self):
        client = WeComClient("bot", "s", None)
        client.connected = True
        calls = []

        async def fake_request(cmd, req_id, body):
            calls.append((cmd, body))
            if cmd == "aibot_upload_media_init":
                return {"errcode": 0, "body": {"upload_id": "up1"}}
            if cmd == "aibot_upload_media_finish":
                return {"errcode": 0, "body": {"media_id": "m1"}}
            return {"errcode": 0}
        client._request = fake_request
        data = b"x" * (UPLOAD_CHUNK_BYTES + 10)
        self.assertEqual(await client.send_file("owner", data, "a.bin", "application/octet-stream"), OK)
        cmds = [c for c, _ in calls]
        self.assertEqual(cmds, ["aibot_upload_media_init", "aibot_upload_media_chunk",
                                "aibot_upload_media_chunk", "aibot_upload_media_finish", "aibot_send_msg"])
        self.assertEqual(calls[0][1]["total_chunks"], 2)
        self.assertEqual(calls[2][1]["chunk_index"], 1)
        self.assertEqual(calls[-1][1], {"chatid": "owner", "msgtype": "file", "file": {"media_id": "m1"}})
        calls.clear()
        await client.send_file("owner", b"\x89PNG", "a.png", "image/png")
        self.assertEqual(calls[-1][1]["msgtype"], "image")

    async def test_rejections_and_size_cap(self):
        client = WeComClient("bot", "s", None)
        client.connected = True

        async def refuse(cmd, req_id, body):
            return {"errcode": 40001}
        client._request = refuse
        self.assertEqual(await client.send_file("owner", b"x", "a", "text/plain"), REJECTED)
        too_big = b"x" * (UPLOAD_CHUNK_BYTES * MAX_UPLOAD_CHUNKS + 1)
        self.assertEqual(await client.send_file("owner", too_big, "a", "text/plain"), REJECTED)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipIf(aiohttp is None, "aiohttp not installed")
class FetchAndUploadIntegration(unittest.IsolatedAsyncioTestCase):
    """Real AiohttpHttp.fetch/upload against a local server (https checks relaxed for http)."""

    async def asyncSetUp(self):
        from dot_bridge import http as http_mod
        self.received = {}

        async def big(request):
            return web.Response(body=b"z" * 3000)

        async def redirect(request):
            raise web.HTTPFound(f"http://127.0.0.1:{self.port}/big")

        async def offsite(request):
            raise web.HTTPFound("http://evil.example/x")

        async def upload(request):
            form = await request.post()
            part = form["file"]
            self.received = {"name": part.filename, "type": part.content_type, "data": part.file.read()}
            return web.json_response({"id": "file-9"})

        app = web.Application()
        app.router.add_get("/big", big)
        app.router.add_get("/redirect", redirect)
        app.router.add_get("/offsite", offsite)
        app.router.add_post("/upload", upload)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.port = free_port()
        await web.TCPSite(self.runner, "127.0.0.1", self.port).start()

        class LocalHttp(http_mod.AiohttpHttp):
            @staticmethod
            def check_url(url):
                pass
        self.http = LocalHttp()
        self.http_mod = http_mod
        self._urlsplit = http_mod.urlsplit
        # Treat local http as https for the scheme check inside fetch.
        http_mod.urlsplit = lambda u: self._urlsplit(u.replace("http://", "https://", 1))

    async def asyncTearDown(self):
        self.http_mod.urlsplit = self._urlsplit
        await self.http.close()
        await self.runner.cleanup()

    async def test_fetch_follows_allowed_redirects_and_enforces_limits(self):
        local = lambda host: host == "127.0.0.1"  # noqa: E731
        data, _ = await self.http.fetch(f"http://127.0.0.1:{self.port}/redirect", host_ok=local)
        self.assertEqual(len(data), 3000)
        with self.assertRaises(FetchError):
            await self.http.fetch(f"http://127.0.0.1:{self.port}/offsite", host_ok=local)
        with self.assertRaises(FileTooLarge):
            await self.http.fetch(f"http://127.0.0.1:{self.port}/big", host_ok=local, max_bytes=100)

    async def test_multipart_upload(self):
        from dot_bridge.http import Upload
        resp = await self.http.request("POST", f"http://127.0.0.1:{self.port}/upload",
                                       upload=Upload("file", "周报.pdf", "application/pdf", b"%PDF"))
        self.assertEqual(resp.body, {"id": "file-9"})
        self.assertEqual(self.received, {"name": "周报.pdf", "type": "application/pdf", "data": b"%PDF"})


if __name__ == "__main__":
    unittest.main()
