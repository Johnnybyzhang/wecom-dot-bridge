"""Durable SQLite state: settings, inbound WeCom -> dot queue, dot -> WeCom reply outbox.

Inbound states:
  ready -> sending -> delivered
                   -> uncertain   (outcome unknown; never auto-resent, /resend is explicit)
                   -> failed      (dot definitively rejected it)
  sending -> ready only for outcomes that prove nothing was accepted (no connection, 401, 429).
  uncertain|failed -> ready (owner /resend) or discarded (owner /discard).

Reply ledger, one row per dot-room message ever observed:
  pending    a dot message not yet finished (in progress, still changing, or empty);
             re-fetched every poll until it settles, even after leaving the latest page
  seen       not for WeCom (human message, pre-binding history, settled empty item)
  queued     a finished dot reply whose text is frozen here until delivered to WeCom
  forwarded  every part delivered (text cleared)
  dropped    abandoned after repeated WeCom rejections, or by an owner reset (text cleared)

"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA_VERSION = 3  # v2: file transfer columns; v3: quote map + inbox.reply_to (in place)
QUOTE_MAP_ROWS = 2000
INBOX_RETENTION_SECONDS = 30 * 86400
HANDLED_RETENTION_SECONDS = 7 * 86400
REPLY_LEDGER_ROWS = 5000


@dataclass(frozen=True)
class Item:
    seq: int
    msgid: str
    chat_id: str
    sender: str
    text: str = field(repr=False)
    request_id: str
    state: str
    attempts: int
    available_at: float
    sending_at: float | None
    remote_id: str | None
    created_at: float
    dest_room: str | None = None
    files: tuple = field(default=(), repr=False)      # WeCom media refs to upload
    dot_files: tuple = ()                              # dot file_ids, once uploaded
    reply_to: str | None = None                        # dot message this quote-replies to


@dataclass(frozen=True)
class Reply:
    remote_id: str
    text: str = field(repr=False)
    parts_sent: int
    files: tuple = ()        # ({"file_id", "name", "mime"}, ...)
    files_sent: int = 0


@dataclass(frozen=True)
class Pending:
    remote_id: str
    text: str = field(repr=False)
    pending_since: float
    empty_since: float | None


class Store:
    def __init__(self, path: str | Path, *, clock=time.time):
        self.clock = clock
        path = Path(path)
        if not path.exists():
            os.close(os.open(path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600))
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        version = self.db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, 1, 2, SCHEMA_VERSION):
            raise RuntimeError("unsupported state schema version")
        with self._tx():
            self.db.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            self.db.execute("""CREATE TABLE IF NOT EXISTS inbox (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                msgid TEXT NOT NULL UNIQUE,
                chat_id TEXT NOT NULL,
                sender TEXT NOT NULL,
                text TEXT NOT NULL,
                request_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL CHECK(state IN
                    ('ready','sending','delivered','uncertain','failed','discarded')),
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL,
                sending_at REAL,
                dest_room TEXT,
                remote_id TEXT,
                error TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL)""")
            self.db.execute("CREATE INDEX IF NOT EXISTS inbox_state ON inbox(state, seq)")
            self.db.execute("""CREATE TABLE IF NOT EXISTS replies (
                remote_id TEXT PRIMARY KEY,
                state TEXT NOT NULL CHECK(state IN
                    ('pending','seen','queued','forwarded','dropped')),
                text TEXT,
                parts_sent INTEGER NOT NULL DEFAULT 0,
                pending_since REAL,
                empty_since REAL,
                updated_at REAL NOT NULL)""")
            self.db.execute("CREATE INDEX IF NOT EXISTS replies_state ON replies(state)")
            self.db.execute("""CREATE TABLE IF NOT EXISTS handled (
                msgid TEXT PRIMARY KEY, at REAL NOT NULL)""")
            self.db.execute("""CREATE TABLE IF NOT EXISTS quote_map (
                norm TEXT NOT NULL, remote_id TEXT NOT NULL, at REAL NOT NULL)""")
            for table, column, decl in (("inbox", "files", "TEXT"), ("inbox", "dot_files", "TEXT"),
                                        ("inbox", "reply_to", "TEXT"),
                                        ("replies", "files", "TEXT"),
                                        ("replies", "files_sent", "INTEGER NOT NULL DEFAULT 0")):
                if column not in {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}:
                    self.db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
            self.db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    @contextmanager
    def _tx(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    def close(self):
        self.db.close()

    # settings
    def get(self, key: str, default=None):
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return default if row is None else json.loads(row["value"])

    def put(self, key: str, value) -> None:
        with self._tx():
            self._put(key, value)

    def _put(self, key: str, value) -> None:
        if value is None:
            self.db.execute("DELETE FROM kv WHERE key=?", (key,))
        else:
            self.db.execute("INSERT INTO kv(key,value) VALUES(?,?) ON CONFLICT(key) "
                            "DO UPDATE SET value=excluded.value",
                            (key, json.dumps(value, ensure_ascii=False, sort_keys=True)))

    def first_time(self, msgid: str) -> bool:
        """True exactly once per WeCom msgid; used for commands, which are not queued."""
        with self._tx():
            return self.db.execute("INSERT OR IGNORE INTO handled(msgid, at) VALUES(?,?)",
                                   (msgid, self.clock())).rowcount == 1

    # inbound
    @staticmethod
    def _item(row) -> Item | None:
        if row is None:
            return None
        return Item(row["seq"], row["msgid"], row["chat_id"], row["sender"], row["text"],
                    row["request_id"], row["state"], row["attempts"], row["available_at"],
                    row["sending_at"], row["remote_id"], row["created_at"], row["dest_room"],
                    tuple(json.loads(row["files"] or "[]")), tuple(json.loads(row["dot_files"] or "[]")),
                    row["reply_to"])

    def item(self, seq: int) -> Item | None:
        return self._item(self.db.execute("SELECT * FROM inbox WHERE seq=?", (seq,)).fetchone())

    def enqueue(self, msgid: str, chat_id: str, sender: str, text: str,
                files=(), reply_to: str | None = None) -> tuple[Item, bool]:
        now = self.clock()
        with self._tx():
            old = self.db.execute("SELECT * FROM inbox WHERE msgid=?", (msgid,)).fetchone()
            if old is not None:
                return self._item(old), False
            cur = self.db.execute("""INSERT INTO inbox(msgid,chat_id,sender,text,request_id,
                state,available_at,created_at,updated_at,files,reply_to)
                VALUES(?,?,?,?,?,'ready',?,?,?,?,?)""",
                (msgid, chat_id, sender, text, str(uuid.uuid4()), now, now, now,
                 json.dumps(list(files)) if files else None, reply_to))
            return self.item(cur.lastrowid), True

    def set_dot_files(self, seq: int, file_ids: list[str], text: str) -> None:
        """Persist uploaded dot file_ids (and the text, named after the files if it was empty)
        before the message POST, so retries never re-upload."""
        with self._tx():
            self.db.execute("UPDATE inbox SET dot_files=?, text=?, updated_at=? "
                            "WHERE seq=? AND state='ready'",
                            (json.dumps(file_ids), text, self.clock(), seq))

    def defer(self, seq: int, error: str, delay: float) -> None:
        """A pre-send step (file upload) failed; nothing was posted. Retry the head later."""
        with self._tx():
            self._move(seq, ("ready",), "ready", error=error, available_at=self.clock() + delay)

    def head(self) -> Item | None:
        """Oldest ready item, due or not. Strict FIFO: a delayed head blocks later items."""
        return self._item(self.db.execute(
            "SELECT * FROM inbox WHERE state='ready' ORDER BY seq LIMIT 1").fetchone())

    def _move(self, seq: int, frm: tuple[str, ...], to: str, **extra) -> bool:
        sets = ["state=?", "updated_at=?"] + [f"{k}=?" for k in extra]
        marks = ",".join("?" * len(frm))
        changed = self.db.execute(
            f"UPDATE inbox SET {','.join(sets)} WHERE seq=? AND state IN ({marks})",
            (to, self.clock(), *extra.values(), seq, *frm)).rowcount
        return changed == 1

    def begin_send(self, seq: int, room_id: str) -> bool:
        """Commit the durable send intent before the first POST byte.

        The first attempt fixes the destination room; later attempts (retries, /resend)
        are only ever made to that same room.
        """
        with self._tx():
            row = self.db.execute("SELECT dest_room FROM inbox WHERE seq=?", (seq,)).fetchone()
            if row is None or (row[0] is not None and row[0] != room_id):
                return False
            if not self._move(seq, ("ready",), "sending", sending_at=self.clock(),
                              dest_room=room_id):
                return False
            self.db.execute("UPDATE inbox SET attempts=attempts+1 WHERE seq=?", (seq,))
            return True

    def delivered(self, seq: int, remote_id: str | None) -> None:
        with self._tx():
            # Delivered: drop the WeCom media refs (they include decryption keys).
            self._move(seq, ("sending", "uncertain"), "delivered", remote_id=remote_id, error=None,
                       files=None)

    def uncertain(self, seq: int, error: str) -> None:
        with self._tx():
            self._move(seq, ("sending",), "uncertain", error=error)

    def failed(self, seq: int, error: str) -> None:
        with self._tx():
            self._move(seq, ("sending", "ready"), "failed", error=error)

    def not_accepted(self, seq: int, error: str, delay: float) -> None:
        """Only for outcomes proving the server did not accept the message."""
        with self._tx():
            self._move(seq, ("sending",), "ready", error=error,
                       available_at=self.clock() + delay)

    def unsent(self) -> list[Item]:
        return [self._item(r) for r in self.db.execute(
            "SELECT * FROM inbox WHERE state IN ('uncertain','failed') ORDER BY seq")]

    def resend_all(self, room_id: str | None) -> int:
        """Owner-approved resend of uncertain/failed messages meant for `room_id` (or not yet
        attempted anywhere), in order, with their original request IDs."""
        with self._tx():
            return self.db.execute(
                "UPDATE inbox SET state='ready', error=NULL, available_at=?, updated_at=? "
                "WHERE state IN ('uncertain','failed') AND (dest_room IS NULL OR dest_room=?)",
                (self.clock(), self.clock(), room_id)).rowcount

    def discard_unsent(self) -> int:
        with self._tx():
            return self.db.execute(
                "UPDATE inbox SET state='discarded', updated_at=? "
                "WHERE state IN ('uncertain','failed')", (self.clock(),)).rowcount

    def delivered_remote_ids(self) -> set[str]:
        return {r[0] for r in self.db.execute(
            "SELECT remote_id FROM inbox WHERE remote_id IS NOT NULL")}

    def recover(self) -> list[int]:
        """After a crash, anything mid-POST has an unknown outcome."""
        with self._tx():
            rows = [r[0] for r in self.db.execute("SELECT seq FROM inbox WHERE state='sending'")]
            self.db.execute("UPDATE inbox SET state='uncertain', error='interrupted', "
                            "updated_at=? WHERE state='sending'", (self.clock(),))
            return rows

    def prune(self) -> None:
        now = self.clock()
        with self._tx():
            self.db.execute("DELETE FROM inbox WHERE state IN ('delivered','failed','discarded') "
                            "AND updated_at<?", (now - INBOX_RETENTION_SECONDS,))
            self.db.execute("DELETE FROM handled WHERE at<?", (now - HANDLED_RETENTION_SECONDS,))
            # Never age-prune the reply ledger (a quiet room keeps old replies in the latest
            # page) and never drop queued replies; only cap the number of settled rows.
            self.db.execute("DELETE FROM replies WHERE state NOT IN ('queued','pending') "
                            "AND rowid NOT IN "
                            "(SELECT rowid FROM replies ORDER BY rowid DESC LIMIT ?)",
                            (REPLY_LEDGER_ROWS,))

    # quote map: text exchanged with each dot message, to resolve WeCom quote-replies
    @staticmethod
    def quote_key(text: str) -> str:
        """Letters/digits only (markdown, punctuation and whitespace dropped), bounded."""
        return re.sub(r"[\W_]+", "", text or "")[:160]

    def remember_quote(self, text: str, remote_id: str) -> None:
        key = self.quote_key(text)
        if len(key) >= 4 and remote_id:
            with self._tx():
                self.db.execute("INSERT INTO quote_map(norm, remote_id, at) VALUES(?,?,?)",
                                (key, remote_id, self.clock()))
                self.db.execute("DELETE FROM quote_map WHERE rowid NOT IN "
                                "(SELECT rowid FROM quote_map ORDER BY rowid DESC LIMIT ?)",
                                (QUOTE_MAP_ROWS,))

    def find_quote(self, text: str) -> str | None:
        """The dot message a quote refers to, or None when it is unknown or ambiguous.

        An exact (normalized) match wins. Otherwise the quote may be a shortened form of the
        stored text, never the reverse. If several distinct dot messages remain plausible,
        return None so the caller sends the quote as context rather than guessing.
        """
        quoted = self.quote_key(text)
        if len(quoted) < 4:
            return None
        exact, shortened = set(), set()
        for norm, remote_id in self.db.execute("SELECT norm, remote_id FROM quote_map"):
            if norm == quoted:
                exact.add(remote_id)
            elif norm.startswith(quoted):
                shortened.add(remote_id)
        candidates = exact or shortened
        return next(iter(candidates)) if len(candidates) == 1 else None

    def counts(self) -> dict[str, int]:
        return dict(self.db.execute("SELECT state, COUNT(*) FROM inbox GROUP BY state").fetchall())

    # reply ledger / outbox
    def reply_state(self, remote_id: str) -> str | None:
        row = self.db.execute("SELECT state FROM replies WHERE remote_id=?", (remote_id,)).fetchone()
        return None if row is None else row[0]

    def known_any(self, remote_ids) -> bool:
        ids = [r for r in remote_ids if isinstance(r, str)]
        if not ids:
            return False
        marks = ",".join("?" * len(ids))
        return self.db.execute(f"SELECT 1 FROM replies WHERE remote_id IN ({marks}) LIMIT 1",
                               ids).fetchone() is not None

    def ledger_empty(self) -> bool:
        return self.db.execute("SELECT 1 FROM replies LIMIT 1").fetchone() is None

    def pending(self) -> dict[str, Pending]:
        return {r[0]: Pending(r[0], r[1] or "", r[2], r[3]) for r in self.db.execute(
            "SELECT remote_id, text, pending_since, empty_since FROM replies WHERE state='pending'")}

    def apply_reply_ops(self, room_id: str, ops: list[tuple], sweep) -> int:
        """Commit one poll's observations atomically (skipped if the dot was rebound meanwhile).

        ops: ("seen", id) | ("pending", id, text, empty_clock) | ("queue", id, text).
        Each op only moves a message from absent/pending; settled rows are never touched.
        Returns the number of newly queued replies.
        """
        now = self.clock()
        queued = 0
        with self._tx():
            if (self.get("dot") or {}).get("room_id") != room_id:
                return 0
            for op in ops:
                if op[0] == "seen":
                    self.db.execute(
                        "INSERT INTO replies(remote_id,state,updated_at) VALUES(?,'seen',?) "
                        "ON CONFLICT(remote_id) DO UPDATE SET state='seen', text=NULL, "
                        "updated_at=excluded.updated_at WHERE replies.state='pending'",
                        (op[1], now))
                elif op[0] == "pending":
                    _, rid, text, empty_clock = op
                    self.db.execute(
                        "INSERT INTO replies(remote_id,state,text,pending_since,empty_since,"
                        "updated_at) VALUES(?,'pending',?,?,?,?) "
                        "ON CONFLICT(remote_id) DO UPDATE SET text=excluded.text, "
                        "empty_since=CASE WHEN ? THEN COALESCE(replies.empty_since, ?) "
                        "ELSE NULL END, updated_at=excluded.updated_at "
                        "WHERE replies.state='pending'",
                        (rid, text, now, now if empty_clock else None, now, empty_clock, now))
                elif op[0] == "queue":
                    files = json.dumps(list(op[3])) if len(op) > 3 and op[3] else None
                    queued += self.db.execute(
                        "INSERT INTO replies(remote_id,state,text,parts_sent,files,files_sent,updated_at) "
                        "VALUES(?,'queued',?,0,?,0,?) ON CONFLICT(remote_id) DO UPDATE SET "
                        "state='queued', text=excluded.text, parts_sent=0, files=excluded.files, "
                        "files_sent=0, empty_since=NULL, updated_at=excluded.updated_at "
                        "WHERE replies.state='pending'",
                        (op[1], op[2], files, now)).rowcount
            self._put("sweep", sweep)
        return queued

    def bind(self, binding: dict, seen_ids, *, room_changed: bool) -> None:
        now = self.clock()
        with self._tx():
            if room_changed:
                # A new room: its history is not replayed, and old-room tracking ends.
                self.db.execute("UPDATE replies SET state='seen', text=NULL, updated_at=? "
                                "WHERE state='pending'", (now,))
                self.db.executemany("INSERT OR IGNORE INTO replies(remote_id,state,updated_at) "
                                    "VALUES(?,'seen',?)", [(r, now) for r in seen_ids])
                self._put("sweep", None)
            self._put("dot", binding)

    def mark_seen(self, remote_ids) -> None:
        now = self.clock()
        with self._tx():
            self.db.executemany("INSERT OR IGNORE INTO replies(remote_id,state,updated_at) "
                                "VALUES(?,'seen',?)", [(r, now) for r in remote_ids])

    def queue_reply(self, remote_id: str, text: str) -> bool:
        """Freeze a finished dot reply for delivery. Never re-queues a known message."""
        with self._tx():
            return self.db.execute(
                "INSERT OR IGNORE INTO replies(remote_id,state,text,parts_sent,updated_at) "
                "VALUES(?,'queued',?,0,?)", (remote_id, text, self.clock())).rowcount == 1

    def next_reply(self, hold_seconds: float) -> Reply | None:
        """Oldest queued reply, unless an earlier-observed dot message is still pending and
        has been pending for less than `hold_seconds` (keeps replies in order without letting
        one long-running message block the rest indefinitely)."""
        row = self.db.execute(
            "SELECT remote_id, text, parts_sent, files, files_sent FROM replies q WHERE q.state='queued' "
            "AND NOT EXISTS (SELECT 1 FROM replies p WHERE p.state='pending' "
            "AND p.rowid < q.rowid AND p.pending_since > ?) ORDER BY q.rowid LIMIT 1",
            (self.clock() - hold_seconds,)).fetchone()
        return None if row is None else Reply(row[0], row[1] or "", row[2],
                                              tuple(json.loads(row[3] or "[]")), row[4] or 0)

    def reply_file_progress(self, remote_id: str, files_sent: int) -> None:
        with self._tx():
            self.db.execute("UPDATE replies SET files_sent=?, updated_at=? "
                            "WHERE remote_id=? AND state='queued'",
                            (files_sent, self.clock(), remote_id))

    def reply_progress(self, remote_id: str, parts_sent: int) -> None:
        with self._tx():
            self.db.execute("UPDATE replies SET parts_sent=?, updated_at=? "
                            "WHERE remote_id=? AND state='queued'",
                            (parts_sent, self.clock(), remote_id))

    def reply_done(self, remote_id: str, state: str) -> None:
        with self._tx():
            self.db.execute("UPDATE replies SET state=?, text=NULL, files=NULL, updated_at=? "
                            "WHERE remote_id=? AND state='queued'",
                            (state, self.clock(), remote_id))

    def reply_backlog(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM replies WHERE state='queued'").fetchone()[0]

    def reset_identity(self) -> int:
        """Owner change (service stopped): forget owner, binding, unsent and undelivered."""
        with self._tx():
            self.db.execute("DELETE FROM kv WHERE key IN ('owner','dot','claim','sweep')")
            self.db.execute("UPDATE replies SET state='dropped', text=NULL, updated_at=? "
                            "WHERE state IN ('queued','pending')", (self.clock(),))
            return self.db.execute(
                "UPDATE inbox SET state='discarded', updated_at=? "
                "WHERE state IN ('ready','sending','uncertain','failed')", (self.clock(),)).rowcount
