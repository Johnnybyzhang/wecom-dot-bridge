# wecom-dot-bridge: WeCom ⇄ your existing ChatGPT dot

A dedicated WeCom (企业微信) AI bot that forwards one owner's private-chat messages
(text, images, files, quote replies) into an **existing** ChatGPT dot messaging room,
and sends the dot's replies and files back to the same WeCom chat.

> **Unofficial.** This project is not affiliated with OpenAI or Tencent. It uses
> undocumented ChatGPT endpoints, observed from the ChatGPT apps and two unofficial
> clients, that can change or break at any time, and automated use may conflict with
> the services' terms. Use it with your own accounts, at your own risk.

## What it is

- One Python 3.13 asyncio process (`python3 -m dot_bridge serve`). Its dependencies
  are Debian's `python3-aiohttp` and `python3-cryptography` (WeCom media decryption).
- **WeCom:** an AI bot over the official `wss://openws.work.weixin.qq.com`
  long connection. No public callback, no inbound port. The protocol and QR
  enrollment are adapted from Hermes Agent (MIT, see `THIRD_PARTY_NOTICES.md`).
- **ChatGPT:** an independent Codex OAuth device login owned by this bridge.
  Do not copy tokens from other Codex clients; refresh tokens are single-use.
- **dot:** the undocumented `chatgpt.com/backend-api` messaging endpoints
  (`/tbo`, `/messaging/rooms/{room}/messages`), plus the live-update socket from
  `/celsius/ws/user`. Its protocol follows two unofficial clients:
  [pi-plugins dot-live.ts](https://github.com/gunba/pi-plugins/blob/b4c064050fbc0f445cd56ca838b590920fecf29f/pi-desk/src/host/dot-live.ts)
  and [beeper chatgpt-dots signals.go](https://github.com/beeper/chatgpt-dots/blob/0c75b6ffae91d36e9e637307b8d0adc8c2eb0ad2/pkg/chatgpt/signals.go).
  The socket URL carries authentication material and is never logged. Rooms are
  never created. If OpenAI changes these endpoints, the bridge stops forwarding and
  reports it. It does not guess.

## Behaviour

| Situation | Result |
| --- | --- |
| Anyone but the bound owner, any group chat | Ignored silently |
| Owner text or voice transcript | Durably queued (deduplicated by WeCom `msgid`), then posted once with a stable `request_id`/`idempotency_token`. Strict FIFO. |
| HTTP 200/201 | Delivered |
| No connection (DNS, refused, TLS, connect timeout) | Nothing was sent; the queue pauses 30 s and keeps order |
| HTTP 401 | Refresh once, retry once (401 means not accepted). If a fresh token is still refused, the login is cleared, the message waits, and the owner is asked once to `/login` |
| HTTP 429 | Not accepted; the queue pauses 30 s and keeps order |
| Other 4xx | Rejected; the owner is told, with `/resend` or `/discard` available |
| Timeout after connecting, 5xx, unexpected error, crash mid-send | Outcome unknown. The room is checked for **positive evidence**: a message by the bound human carrying our exact `request_id` (the live API returns it on every message we post). If found, the message is delivered. Otherwise it is marked **uncertain**, the owner is told (with a preview), and it is **never auto-resent**. Later messages are not blocked; the queue pauses 30 s. |
| dot reply | Push-triggered: a listener on ChatGPT's per-user socket (`/celsius/ws/user`, topic `calpico-chatgpt-messaging`, background presence) wakes an authoritative REST read of the room on every message add/update event, on every (re)subscribe and on disconnect. While the socket is live, routine polling is only a 120 s recovery check (2 s while a dot message is still settling). If the socket is down, the bridge falls back to polling: immediately after each delivered message, every 2 s for 15 min after activity, otherwise every 60 s. Messages are filtered by the dot's aeon member ID. Unfinished dot messages are tracked persistently until they finish, even after they leave the latest page. When the latest page does not cover what is tracked, older pages are walked back (10 per poll, resumed on the next poll). A finished reply (not modified for 0.5 s, or text stable across two polls; not `in_progress`; empty items wait up to 2 min) is frozen into a durable outbox. The outbox is delivered in order when WeCom is connected (a finished reply waits up to 10 min behind an earlier one that is still generating), in ≤4000-character / ≤12000-byte parts paced to WeCom's 30 messages/min/chat limit. Part progress is persisted, so a failure or restart resumes at the failed part; at most that one part can repeat. A reply is abandoned only after 5 explicit WeCom rejections spaced 30 s apart. |
| Owner sends an image, file, or text+images (WeCom) | Queued like text. Before the send intent, each file is downloaded from WeCom, decrypted (AES-256-CBC with the message's key), named from `Content-Disposition` (percent-decoded and length-bounded), uploaded to the dot room (`POST /messaging/rooms/{room}/files`), and its `file_id` persisted. Retries reuse the IDs. The message is then posted with `attachments: [{type: "file", file_id}]`; without text, it is named after the files ("📎 name"). A download/upload failure is retried 3 times, 30 s apart, then reported. Files over 20 MB are refused with a notice. |
| dot sends files | The outbox keeps the file references with the frozen text. After the text (if any), each file is fetched fresh (`GET /messaging/rooms/{room}/files/{id}`, then the signed `*.oaiusercontent.com` URL with no account headers), uploaded to WeCom in 512 KB chunks, and sent as an `image` (PNG/JPEG/GIF up to 10 MB) or a `file`. Per-file progress is persisted. Over 20 MB, or still failing after 5 tries, the file becomes a notice pointing to ChatGPT, so later replies are not blocked. |
| Owner quote-replies in WeCom | WeCom sends the quoted *content*, not an ID. The bridge remembers the text exchanged with each dot message (forwarded dot replies and the owner's delivered messages; letters and digits only, so markdown and spacing don't matter) and maps the quote by prefix. On a match it sends a native dot reply (`"reply_to": {"message_id": …}`, verified live). Otherwise the quote travels as context: `> excerpt` (or `> [image]`/`> [file]`) above the message. |
| dot replies to an older message | If the dot's reply quotes something other than the owner's latest message, the WeCom message starts with a `> excerpt` line. |

Bridge notices are in US English by default; `/lang zh` switches them to Simplified Chinese
(`/lang en` back). Text that reaches the dot (quote context, file placeholders) is always
English. WeCom commands (owner private chat, each executed once per `msgid`):
`/login` (only when the bridge reports the ChatGPT login invalidated; refused
while it is valid), `/status`, `/dots`, `/use <dot_id>`, `/lang en|zh`, `/pending` (list uncertain or
rejected messages), `/resend` (resend them in order with their original
idempotency tokens, only to the dot each was first sent to), `/discard`, `/help`. Other text, including unknown `/…`,
goes to the dot.

## Isolation (reference deployment: Incus)

`deploy/` targets an Incus host (developed on Flatcar with the Incus and ZFS system
extensions). The bridge gets its own environment and shares nothing with other workloads:

- Incus project `dot`: restricted, isolated idmap, no nesting, managed disks and
  NICs only, no proxy devices, one container, 2 CPU / 1 GiB / 1024 processes /
  20 GiB on pool `agents`.
- Network `dot0` (10.251.0.1/24) with its own ACL `dot-egress`. It denies all
  private, link-local and multicast ranges, allows public TCP 80/443 and DNS to
  223.5.5.5 / 1.1.1.1, and accepts no ingress.
- Container `dot-bridge` (Debian 13) on profile `dot-bridge`: 1 CPU, 512 MiB,
  8 GiB root, and custom ZFS volume `dot-state` (2 GiB, snapshotted daily at
  03:00, kept 30 days) at `/var/lib/dot-bridge`.
- Unit `dot-bridge.service` runs as the unprivileged `dot-bridge` user with a
  strict systemd sandbox (no capabilities, read-only system, writes only to
  `/var/lib/dot-bridge`, 256 MiB).

State in `/var/lib/dot-bridge` (mode 0700):

- `bridge.sqlite3`: queue, reply ledger, owner/dot binding. Message text is
  plaintext; delivered/failed rows are pruned after 30 days.
- `secrets/wecom.json`, `secrets/openai.json`: mode 0600, never logged.

## Install / upgrade

From a workstation with this checkout (`HOST` is the Incus host, reachable over SSH):

```sh
scp deploy/incus-create.sh HOST:/tmp/
ssh HOST sudo sh /tmp/incus-create.sh
tar czf - dot_bridge/*.py deploy | ssh HOST \
  'sudo incus exec dot-bridge --project dot -- sh -c "rm -rf /root/src && mkdir /root/src && tar xzf - -C /root/src && sh /root/src/deploy/provision.sh"'
```

- `incus-create.sh` expects a ZFS-backed storage pool named `agents` (edit `POOL`
  otherwise). It needs a free 65,536-ID block in root's `/etc/subuid` and
  `/etc/subgid` for the container's isolated ID map; it checks for this and
  explains if the block is missing.
- `provision.sh` is idempotent and keeps the previous code in `/opt/dot-bridge.old`.
- Without Incus, the service runs anywhere with Python 3.11+, aiohttp and
  cryptography: `DOT_BRIDGE_STATE=/path python3 -m dot_bridge serve` (see
  `deploy/dot-bridge.service` for the sandboxing used in the reference deployment).

## First-time setup (owner)

All commands run inside the container (`sudo incus exec dot-bridge --project dot -- …`).
The wrapper drops to the `dot-bridge` user automatically.

1. `dot-bridge enroll-wecom`: scan the QR (or open the printed link) in WeCom.
   This creates a new AI bot. The running service picks it up within 10 s.
2. `dot-bridge claim`: send the printed `/claim <code>` to the new bot in a
   WeCom private chat within 15 minutes. That user becomes the only owner.
3. Send `/login` to the bot, open the link, sign in with the ChatGPT account that
   owns the dot, and enter the code. With exactly one dot it binds
   automatically; otherwise use `/dots` and `/use <dot_id>`.
   (Alternative: `dot-bridge login` in the terminal.)

## Operations

- Health: `dot-bridge status`. It exits 0 when the service heartbeat is under
  90 s old and prints WeCom/login/binding state and queue counts. Logs:
  `journalctl -u dot-bridge`. Message text and tokens are never logged.
- Change owner: `systemctl stop dot-bridge`, then `dot-bridge reset-owner`. It refuses
  while the service runs. It clears the owner, dot binding and ChatGPT login, and
  discards unsent messages and undelivered replies, so a new owner inherits nothing.
  Then start the service and run `dot-bridge claim`.
- Exit code 75 (not restarted by systemd) means either WeCom reported that another
  client connected with this bot's credentials, or a second bridge was started on
  the same state directory. `dot-bridge status` shows `fatal`. Stop the other
  client, then run `systemctl restart dot-bridge`. Reconnecting automatically would
  start a mutual-kick loop.
- Re-enroll the bot: stop the service, delete `secrets/wecom.json`, run
  `enroll-wecom`, start the service.
- Revoke ChatGPT access: delete `secrets/openai.json` (the bridge asks for
  `/login`), and sign out of the session in ChatGPT account settings.
- Backup: the ZFS volume snapshots are crash-consistent (SQLite WAL with
  `synchronous=FULL`). For an application-consistent copy, stop the service and
  run `incus storage volume export agents dot-state --project dot <file>`.
  Restore into a new volume and test it first.
- Rollback code: in the container, `mv /opt/dot-bridge /opt/dot-bridge.bad &&
  mv /opt/dot-bridge.old /opt/dot-bridge && systemctl restart dot-bridge`.
- Remove everything: `incus delete -f dot-bridge --project dot`, then delete
  the `dot-state` volume, profile, project, network `dot0` and ACL `dot-egress`.
  Nothing else on the host depends on them.

## Tests

```sh
python3 -m unittest discover -s tests -v
```

The suite runs offline. Most tests use fake HTTP and WeCom transports. The
integration tests (`test_aiohttp_integration.py`, plus parts of `test_live.py` and
`test_files.py`) run the real aiohttp code against local servers: chunked bodies,
redirects, multipart uploads, and WeCom and live-socket protocol cycles. They are
skipped where aiohttp or cryptography is missing.

## Known limits

- The dot endpoints are undocumented and can change. Their behaviour was verified
  against a live account on 2026-10-15 (see `docs/dot-api.md`). In particular,
  room members, and therefore the dot's identity, come only from the `/tbo` list,
  not from `GET /tbo/{id}`.
- Files up to 20 MB each way (both platforms' limit). Voice is forwarded as its transcript;
  video is not forwarded. Non-file dot attachments (links, cards) are announced.
- Built for exactly one person: one owner, private chat only. There are no multi-user,
  capacity or adversarial-load features by design.
- Egress is limited to public TCP 80/443 and two DNS resolvers. It is not a
  per-domain allowlist: the
  peers (Cloudflare-fronted chatgpt.com, Tencent WeCom endpoints, Debian mirrors)
  sit on large, changing CDN ranges. The container runs only this service; it
  runs no agent and no untrusted code. A filtering egress proxy would be the next
  step if that changes.
