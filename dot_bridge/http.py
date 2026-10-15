"""Single HTTP seam. Tests inject a fake; production uses aiohttp."""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from urllib.parse import urlsplit

MAX_BODY_BYTES = 4 * 1024 * 1024
MAX_FILE_BYTES = 20 * 1024 * 1024  # both WeCom and dot cap attachments at 20 MB
ALLOWED_HOSTS = frozenset({"chatgpt.com", "auth.openai.com"})


class NetworkError(Exception):
    """The request may or may not have reached the server."""


class ConnectError(NetworkError):
    """No connection was established (DNS, refused, TLS); nothing was sent."""


class FileTooLarge(Exception):
    """The file exceeds MAX_FILE_BYTES."""


class FetchError(Exception):
    """A file download was refused (bad host, redirect loop, HTTP error)."""


@dataclass(frozen=True)
class Upload:
    """One multipart file part."""
    field: str
    filename: str
    mime: str
    data: bytes = field(repr=False)


@dataclass(frozen=True)
class Response:
    status: int
    body: object = field(default=None, repr=False)


class AiohttpHttp:
    def __init__(self):
        self._session = None

    async def _get_session(self):
        if self._session is None or self._session.closed:
            import aiohttp
            self._session = aiohttp.ClientSession(trust_env=False)
        return self._session

    @staticmethod
    def check_url(url: str) -> None:
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.hostname not in ALLOWED_HOSTS:
            raise ValueError("URL host is not allowlisted")

    async def request(self, method: str, url: str, *, headers: dict | None = None,
                      json_body: dict | None = None, form: dict | None = None,
                      upload: Upload | None = None, params: dict | None = None,
                      timeout: float = 30) -> Response:
        self.check_url(url)
        import aiohttp
        session = await self._get_session()
        kwargs = {"headers": headers or {}, "params": params, "allow_redirects": False,
                  "timeout": aiohttp.ClientTimeout(total=timeout, connect=min(15, timeout))}
        if json_body is not None:
            kwargs["json"] = json_body
        elif form is not None:
            kwargs["data"] = form
        elif upload is not None:
            data = aiohttp.FormData(quote_fields=False)  # raw UTF-8 filename, as browsers send
            data.add_field(upload.field, upload.data, filename=upload.filename,
                           content_type=upload.mime)
            kwargs["data"] = data
        try:
            async with session.request(method, url, **kwargs) as resp:
                raw = bytearray()
                async for chunk in resp.content.iter_chunked(65536):
                    raw += chunk
                    if len(raw) > MAX_BODY_BYTES:
                        return Response(resp.status, None)
                status = resp.status
        except (aiohttp.ClientConnectorError, aiohttp.ConnectionTimeoutError):
            raise ConnectError("connect failed") from None
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            raise NetworkError(type(exc).__name__) from None
        try:
            return Response(status, json.loads(raw.decode("utf-8")) if raw else None)
        except (UnicodeDecodeError, ValueError):
            return Response(status, None)

    async def fetch(self, url: str, *, host_ok, max_bytes: int = MAX_FILE_BYTES,
                    timeout: float = 120) -> tuple[bytes, dict]:
        """Download a file WITHOUT account headers. Every hop (up to 3 redirects) must be
        https to a host accepted by `host_ok`. Returns (bytes, lower-cased headers)."""
        import aiohttp
        session = await self._get_session()
        for _ in range(4):
            parts = urlsplit(url)
            if parts.scheme != "https" or parts.username or not parts.hostname or not host_ok(parts.hostname):
                raise FetchError(f"download host not allowed: {parts.hostname}")
            try:
                async with session.get(url, allow_redirects=False, headers={"Accept": "*/*"},
                                       timeout=aiohttp.ClientTimeout(total=timeout, connect=15)) as resp:
                    if resp.status in (301, 302, 303, 307, 308) and resp.headers.get("Location"):
                        from yarl import URL
                        url = str(resp.url.join(URL(resp.headers["Location"])))
                        continue
                    if resp.status != 200:
                        raise FetchError(f"download HTTP {resp.status}")
                    if (resp.content_length or 0) > max_bytes:
                        raise FileTooLarge()
                    data = bytearray()
                    async for chunk in resp.content.iter_chunked(65536):
                        data += chunk
                        if len(data) > max_bytes:
                            raise FileTooLarge()
                    return bytes(data), {k.lower(): v for k, v in resp.headers.items()}
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                raise NetworkError(type(exc).__name__) from None
        raise FetchError("too many redirects")

    async def close(self):
        if self._session is not None:
            await self._session.close()
