"""dot-bridge CLI: serve | enroll-wecom | claim | login | status | reset-owner."""
from __future__ import annotations

import argparse
import asyncio
import fcntl
import json
import logging
import os
import secrets
import signal
import sys
import time
import urllib.request
from pathlib import Path

from .dot import DotClient
from .http import AiohttpHttp
from .live import DotLive
from .openai_auth import Auth, TokenFile
from .service import Bridge, hash_code
from .store import Store
from .wecom import WeComClient, qr_enroll

DEFAULT_STATE = "/var/lib/dot-bridge"
CLAIM_SECONDS = 15 * 60
STALE_STATUS_SECONDS = 90
EX_DO_NOT_RESTART = 75  # dot-bridge.service: RestartPreventExitStatus=75
log = logging.getLogger("dot_bridge")


class Paths:
    def __init__(self, root: str):
        self.root = Path(root)
        self.secrets = self.root / "secrets"
        self.db = self.root / "bridge.sqlite3"
        self.wecom = self.secrets / "wecom.json"
        self.openai = self.secrets / "openai.json"

    def prepare(self) -> None:
        self.root.mkdir(mode=0o700, exist_ok=True)
        self.secrets.mkdir(mode=0o700, exist_ok=True)


def write_secret(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    dfd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)


def read_wecom(path: Path) -> dict | None:
    try:
        creds = json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):
        return None
    if isinstance(creds, dict) and isinstance(creds.get("bot_id"), str) and \
            isinstance(creds.get("secret"), str) and creds["bot_id"] and creds["secret"]:
        return creds
    return None


def _get_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "wecom-dot-bridge/1.0"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read(1 << 20).decode("utf-8"))


def single_instance(paths: Paths):
    """Hold an exclusive lock for the daemon's lifetime: one bridge per state directory."""
    fd = os.open(paths.root / "serve.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


async def serve(paths: Paths) -> int:
    lock = single_instance(paths)
    if lock is None:
        log.error("another dot-bridge is already running on this state directory")
        return EX_DO_NOT_RESTART
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    store = Store(paths.db)
    store.put("fatal", None)
    http = AiohttpHttp()
    auth = Auth(TokenFile(paths.openai), http)
    dot = DotClient(http, auth)
    bridge = Bridge(store, auth, dot)
    bridge.recover()
    try:
        creds = read_wecom(paths.wecom)
        while creds is None:
            log.warning("waiting for a valid WeCom enrollment (run: dot-bridge enroll-wecom)")
            store.put("service_status", bridge.snapshot())
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
                return 0
            except asyncio.TimeoutError:
                creds = read_wecom(paths.wecom)
        bridge.wecom = WeComClient(creds["bot_id"], creds["secret"], bridge.on_message)
        bridge.live = DotLive(dot, bridge.on_live_event, bridge.on_live_resync)
        tasks = [asyncio.create_task(c) for c in (
            bridge.wecom.run(), bridge.live.run(), bridge.delivery_loop(), bridge.reply_loop(),
            bridge.forward_loop(), bridge.status_loop())]
        log.info("dot-bridge started")
        waiter = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait([waiter, *tasks], return_when=asyncio.FIRST_COMPLETED)
        waiter.cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if waiter in done:
            log.info("dot-bridge stopped")
            return 0
        if bridge.wecom.kicked:
            store.put("fatal", {"reason": "wecom_kicked", "at": time.time()})
            log.error("stopped: another client connected with this WeCom bot's credentials. "
                      "Stop the other client, then: systemctl restart dot-bridge")
            return EX_DO_NOT_RESTART
        log.error("a bridge loop exited unexpectedly")
        return 1
    finally:
        await http.close()
        store.close()


async def terminal_login(paths: Paths) -> int:
    http = AiohttpHttp()
    try:
        auth = Auth(TokenFile(paths.openai), http)
        if auth.state()["logged_in"]:
            print("Already logged in. Login is only needed after OpenAI invalidates it "
                  "(the bridge then asks the owner to /login).")
            return 1
        challenge = await auth.device_start()
        print(f"Open {challenge.verification_url} and enter code: {challenge.user_code}", flush=True)
        print("Waiting up to 15 minutes...", flush=True)
        account = await auth.device_complete(challenge)
        print(f"Logged in. ChatGPT account ...{account[-6:]}")
        print("The running service binds the dot automatically if exactly one exists; "
              "otherwise send /dots and /use <dot_id> from WeCom.")
        return 0
    finally:
        await http.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="dot-bridge")
    parser.add_argument("--state", default=os.environ.get("DOT_BRIDGE_STATE", DEFAULT_STATE))
    parser.add_argument("command", choices=("serve", "enroll-wecom", "claim", "login",
                                            "status", "reset-owner"))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    paths = Paths(args.state)
    paths.prepare()
    if args.command == "serve":
        return asyncio.run(serve(paths))
    if args.command == "enroll-wecom":
        if paths.wecom.exists():
            print("A WeCom bot is already enrolled. Stop the service and delete "
                  "secrets/wecom.json to re-enroll.")
            return 1
        creds = qr_enroll(_get_json)
        if not creds:
            print("QR enrollment did not complete.")
            return 1
        write_secret(paths.wecom, creds)
        print("WeCom bot enrolled; the service connects within 10 s. Next: dot-bridge claim")
        return 0
    if args.command == "login":
        return asyncio.run(terminal_login(paths))
    store = Store(paths.db)
    try:
        if args.command == "claim":
            if store.get("owner"):
                print("Owner already bound. Use reset-owner first to change it.")
                return 1
            code = f"{secrets.randbelow(10**8):08d}"
            store.put("claim", {"code_hash": hash_code(code), "expires": time.time() + CLAIM_SECONDS})
            print(f"Within 15 minutes, send this to the bot in a WeCom private chat:\n\n  /claim {code}\n")
        elif args.command == "reset-owner":
            # Only while the service is stopped, so no in-flight work can outlive the reset.
            lock = single_instance(paths)
            if lock is None:
                print("Stop the service first: systemctl stop dot-bridge")
                return 1
            discarded = store.reset_identity()
            tokens = TokenFile(paths.openai)
            with tokens.locked():
                tokens.clear()
            print(f"Cleared owner, dot binding and ChatGPT login; discarded {discarded} unsent "
                  "message(s). Next: dot-bridge claim")
        else:
            status = store.get("service_status") or {}
            status["state_counts"] = store.counts()
            status["status_age_seconds"] = round(time.time() - status.get("at", 0), 1)
            status["fatal"] = store.get("fatal")
            print(json.dumps(status, indent=2, sort_keys=True))
            healthy = status["status_age_seconds"] < STALE_STATUS_SECONDS and not status["fatal"]
            return 0 if healthy else 3
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
