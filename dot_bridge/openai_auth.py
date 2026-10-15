"""Independent ChatGPT (Codex OAuth) login for the bridge: device flow, refresh, atomic storage.

Endpoints and client ID follow the official Codex device flow (as Hermes Agent also
uses). This bridge owns its own token family; never copy tokens in.
The service is the only refresher; the token file is flock-serialized so a concurrent
terminal `dot-bridge login` is never overwritten.
"""
from __future__ import annotations

import asyncio
import base64
import fcntl
import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from .http import NetworkError

ISSUER = "https://auth.openai.com"
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
TOKEN_URL = ISSUER + "/oauth/token"
VERIFY_URL = ISSUER + "/codex/device"
REFRESH_SKEW_SECONDS = 300
NO_EXP_REFRESH_SECONDS = 3600
MIN_FORCED_REFRESH_SECONDS = 30
DEVICE_TIMEOUT_SECONDS = 15 * 60
AUTH_CLAIM = "https://api.openai.com/auth"
RELOGIN_CODES = frozenset({"invalid_grant", "invalid_token", "invalid_request",
                           "refresh_token_reused", "refresh_token_expired",
                           "refresh_token_invalidated"})


class AuthRequired(Exception):
    """No usable login; the owner must run /login."""


class AuthUnavailable(Exception):
    """Temporary failure; the stored login is still valid."""



def jwt_claims(token: str) -> dict:
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload))
    except (IndexError, ValueError, TypeError):
        return {}
    return claims if isinstance(claims, dict) else {}


def account_id_of(tokens: dict) -> str | None:
    for key in ("access_token", "id_token"):
        claim = jwt_claims(tokens.get(key) or "").get(AUTH_CLAIM)
        if isinstance(claim, dict) and isinstance(claim.get("chatgpt_account_id"), str):
            return claim["chatgpt_account_id"]
    return None


def expires_at(tokens: dict) -> float:
    exp = jwt_claims(tokens.get("access_token") or "").get("exp")
    if isinstance(exp, (int, float)) and not isinstance(exp, bool):
        return float(exp)
    # Without an exp claim, refresh hourly rather than on every call.
    issued = tokens.get("refreshed_at") or tokens.get("obtained_at") or 0
    return float(issued) + NO_EXP_REFRESH_SECONDS


def _error_code(body) -> str | None:
    if not isinstance(body, dict):
        return None
    err = body.get("error")
    if isinstance(err, dict):
        err = err.get("code") or err.get("type")
    return err if isinstance(err, str) else None


class TokenFile:
    """Mode-0600 JSON file, atomically replaced, flock-serialized across processes."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(".lock")

    @contextmanager
    def locked(self):
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def load(self) -> dict | None:
        try:
            data = json.loads(self.path.read_text("utf-8"))
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) and data.get("access_token") else None

    def save(self, tokens: dict) -> None:
        tmp = self.path.with_suffix(".tmp")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(tokens, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.path)
        dfd = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(dfd)
        finally:
            os.close(dfd)

    def clear(self) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


@dataclass(frozen=True)
class DeviceChallenge:
    user_code: str
    verification_url: str
    interval: int
    expires: float
    device_auth_id: str = field(repr=False)


class Auth:
    def __init__(self, tokens: TokenFile, http, *, clock=time.time, sleep=asyncio.sleep):
        self.tokens, self.http, self.clock, self.sleep = tokens, http, clock, sleep
        self._lock = asyncio.Lock()

    def state(self) -> dict:
        stored = self.tokens.load()
        if not stored or not stored.get("account_id"):
            return {"logged_in": False}
        return {"logged_in": True, "account_id": stored["account_id"],
                "login_id": stored.get("obtained_at"), "access_expires": expires_at(stored)}

    async def access(self) -> tuple[str, str]:
        async with self._lock:
            stored = self.tokens.load()
            if not stored or not stored.get("account_id"):
                raise AuthRequired()
            if expires_at(stored) - self.clock() < REFRESH_SKEW_SECONDS:
                stored = await self._refresh(stored)
            return stored["access_token"], stored["account_id"]

    async def refreshed_access(self, rejected_access: str) -> tuple[str, str] | None:
        """A token newer than one the server just refused, or None if none can be had."""
        async with self._lock:
            stored = self.tokens.load()
            if not stored or not stored.get("account_id"):
                raise AuthRequired()
            if stored["access_token"] != rejected_access:  # already refreshed or re-logged in
                return stored["access_token"], stored["account_id"]
            last = float(stored.get("refreshed_at") or stored.get("obtained_at") or 0)
            if self.clock() - last < MIN_FORCED_REFRESH_SECONDS:
                return None  # just obtained and still refused; another rotation cannot help
            stored = await self._refresh(stored)
            return stored["access_token"], stored["account_id"]

    async def reject(self, access_token: str) -> None:
        """A freshly refreshed token was still refused: the owner must log in again."""
        async with self._lock:
            with self.tokens.locked():
                current = self.tokens.load()
                if current and current.get("access_token") == access_token:
                    self.tokens.clear()

    async def _refresh(self, stored: dict) -> dict:
        with self.tokens.locked():
            current = self.tokens.load()
            if not current or current.get("refresh_token") != stored.get("refresh_token"):
                # A terminal login replaced (or removed) the tokens meanwhile; adopt that.
                if current and current.get("account_id"):
                    return current
                raise AuthRequired()
            if not stored.get("refresh_token"):
                raise AuthRequired()
            try:
                resp = await self.http.request(
                    "POST", TOKEN_URL, form={"grant_type": "refresh_token", "client_id": CLIENT_ID,
                                             "refresh_token": stored["refresh_token"]}, timeout=20)
            except NetworkError:
                raise AuthUnavailable("refresh network error") from None
            body = resp.body if isinstance(resp.body, dict) else None
            if resp.status == 200 and body and isinstance(body.get("access_token"), str):
                updated = {**stored, "access_token": body["access_token"],
                           "refresh_token": body.get("refresh_token") or stored["refresh_token"],
                           "refreshed_at": self.clock()}
                if body.get("id_token"):
                    updated["id_token"] = body["id_token"]
                account = account_id_of(updated)
                if account and account != stored["account_id"]:
                    self.tokens.clear()  # refresh must never switch accounts
                    raise AuthRequired()
                self.tokens.save(updated)
                return updated
            # Only a real OAuth error document proves the grant is dead; an HTML 403 from
            # an edge proxy must not wipe a valid login.
            if body is not None and (_error_code(body) in RELOGIN_CODES
                                     or resp.status in (401, 403)):
                self.tokens.clear()
                raise AuthRequired()
            raise AuthUnavailable(f"refresh HTTP {resp.status}")

    async def device_start(self) -> DeviceChallenge:
        try:
            resp = await self.http.request("POST", ISSUER + "/api/accounts/deviceauth/usercode",
                                           json_body={"client_id": CLIENT_ID}, timeout=20)
        except NetworkError:
            raise AuthUnavailable("device code network error") from None
        body = resp.body if isinstance(resp.body, dict) else {}
        if resp.status != 200 or not body.get("user_code") or not body.get("device_auth_id"):
            raise AuthUnavailable(f"device code HTTP {resp.status}")
        try:
            interval = max(3, int(body.get("interval") or 5))
        except (TypeError, ValueError):
            interval = 5
        return DeviceChallenge(str(body["user_code"]), VERIFY_URL, interval,
                               self.clock() + DEVICE_TIMEOUT_SECONDS, str(body["device_auth_id"]))

    async def device_complete(self, challenge: DeviceChallenge) -> str:
        """Poll until approved, exchange, persist. Returns the ChatGPT account ID."""
        code = None
        while self.clock() < challenge.expires:
            await self.sleep(challenge.interval)
            try:
                resp = await self.http.request(
                    "POST", ISSUER + "/api/accounts/deviceauth/token",
                    json_body={"device_auth_id": challenge.device_auth_id,
                               "user_code": challenge.user_code}, timeout=15)
            except NetworkError:
                continue
            if resp.status == 200 and isinstance(resp.body, dict):
                code = resp.body
                break
            if resp.status not in (403, 404):
                raise AuthUnavailable(f"device poll HTTP {resp.status}")
        if code is None:
            raise TimeoutError("device login expired")
        if not code.get("authorization_code") or not code.get("code_verifier"):
            raise AuthUnavailable("device poll incomplete")
        try:
            resp = await self.http.request("POST", TOKEN_URL, form={
                "grant_type": "authorization_code", "code": code["authorization_code"],
                "redirect_uri": ISSUER + "/deviceauth/callback", "client_id": CLIENT_ID,
                "code_verifier": code["code_verifier"]}, timeout=20)
        except NetworkError:
            raise AuthUnavailable("token exchange network error") from None
        body = resp.body if isinstance(resp.body, dict) else {}
        if resp.status != 200 or not body.get("access_token") or not body.get("refresh_token"):
            raise AuthUnavailable(f"token exchange HTTP {resp.status}")
        tokens = {"access_token": body["access_token"], "refresh_token": body["refresh_token"],
                  "id_token": body.get("id_token", ""), "obtained_at": self.clock()}
        account = account_id_of(tokens)
        if not account:
            raise AuthUnavailable("token has no ChatGPT account")
        tokens["account_id"] = account
        async with self._lock:
            with self.tokens.locked():
                self.tokens.save(tokens)
        return account
