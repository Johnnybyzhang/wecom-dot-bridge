# Observed ChatGPT dot API behaviour

Undocumented and subject to change. Observed against a live account on 2026-10-15,
cross-checked with two unofficial clients:
[pi-plugins](https://github.com/gunba/pi-plugins/tree/b4c064050fbc0f445cd56ca838b590920fecf29f/pi-desk/src/host)
and [beeper/chatgpt-dots](https://github.com/beeper/chatgpt-dots/tree/0c75b6ffae91d36e9e637307b8d0adc8c2eb0ad2/pkg/chatgpt).
All calls go to `https://chatgpt.com/backend-api` with a ChatGPT (Codex OAuth) bearer
token and the `ChatGPT-Account-Id` header.

## Dots and rooms

- `GET /tbo` lists dots. Only these list items carry `messaging_room_preview.members`;
  `GET /tbo/{id}` returns `messaging_room_preview: null`.
- Every member and every message has `role: "user"`. The dot is the room member with
  an `aeon_id`; the human members have none.
- `display_name` can be empty; the room preview's `name` is a usable fallback.

## Messages

- `GET /messaging/rooms/{room}/messages?limit=N` returns the newest page, ordered
  oldest first, with ISO-8601 `created_at`/`updated_at`. `prev_cursor`/`next_cursor`
  were null. `before=<message id>` returns strictly older messages with no overlap.
- `POST` to the same path with
  `{content: {text, attachments}, request_id, idempotency_token}` creates a message.
  The response, and later reads, carry the same `request_id`, which makes delivery
  reconcilable after a timeout. The dot's own messages have `request_id: null` and
  `message_metadata.turn_trigger` (`chatgpt_messaging`, `clock`).
- Dot messages are written whole: `updated_at` is about `created_at` + 50 ms. No
  streaming `status` field was observed.
- Replies: `"reply_to": {"message_id": <id>}` in the POST body produces a native reply,
  echoed with `reply_root_message_id`. A top-level `reply_to_message_id` is ignored.
- Message IDs were 122 characters long (`[A-Za-z0-9_~]`); the dot member's
  `account_user_id` was 153 characters long.

## Files

- Attachment shape: `{attachment_id, type: "file", file_id, file: {id, status:
  "ROOM_PUBLISHED", download_url, mime_type, name}}`. `download_url` is a signed
  `https://*.oaiusercontent.com/...` URL, fetched without account headers, and
  `mime_type` is often `application/octet-stream`.
- `GET /messaging/rooms/{room}/files/{file_id}` returns the same object, with a fresh URL.
- Upload: `POST /messaging/rooms/{room}/files` as multipart with one `file` part
  returns `{id}`. Then reference it with `attachments: [{type: "file", file_id}]`.
  Non-ASCII filenames must be sent as raw UTF-8: aiohttp's `FormData` percent-encodes
  them unless `quote_fields=False`.

## Live updates

- `GET /celsius/ws/user` returns `{websocket_url}`: `wss://ws.chatgpt.com/p<n>/ws/user/<user>?...`.
  The URL carries authentication material; never log it.
- After connecting, send one frame:
  `[{"id":1,"command":{"type":"connect","presence":{"type":"presence","state":"background"}}},
  {"id":2,"command":{"type":"subscribe","topic_id":"calpico-chatgpt-messaging"}}]`.
  Without it, nothing is pushed.
- The subscribe reply (`id: 2`) carries `recovered`, `catchups` and `last_offset`.
  Events look like `{type: "message", topic_id, offset?, payload: {type:
  "calpico-message-add" | "calpico-message-update" | room events, payload: {room_id, ...}}}`.
  On the observed account, frames carried no `offset`.
- Use events only as triggers for authoritative REST reads, and resync after every
  (re)subscribe. Observed latency was about 0.8 s from POST to event.
