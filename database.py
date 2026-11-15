import asyncio
import time
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS guests (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    name_key TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mac TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    vendor TEXT NOT NULL DEFAULT '',
    ip TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT 'unknown' CHECK(category IN ('unknown','resident','guest','ignored')),
    owner_name TEXT NOT NULL DEFAULT '',
    owner_user_id INTEGER,
    guest_id INTEGER REFERENCES guests(id),
    status TEXT NOT NULL DEFAULT 'unknown' CHECK(status IN ('unknown','online','offline')),
    last_seen INTEGER,
    last_arrival INTEGER,
    last_departure INTEGER,
    negative_since INTEGER,
    positive_since INTEGER,
    positive_count INTEGER NOT NULL DEFAULT 0,
    first_alert_pending INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS devices_guest ON devices(guest_id, status);
CREATE TABLE IF NOT EXISTS device_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id INTEGER NOT NULL REFERENCES devices(id),
    mac TEXT NOT NULL,
    label TEXT NOT NULL,
    ip TEXT NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('arrival','departure')),
    observed_at INTEGER NOT NULL,
    confirmed_at INTEGER NOT NULL,
    UNIQUE(device_id, kind, confirmed_at)
);
CREATE INDEX IF NOT EXISTS events_time ON device_events(confirmed_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS notification_settings (
    device_id INTEGER PRIMARY KEY REFERENCES devices(id),
    monitor INTEGER NOT NULL DEFAULT 0,
    keys_enabled INTEGER NOT NULL DEFAULT 0,
    presence_alerts INTEGER NOT NULL DEFAULT 0,
    recipient_chat_id INTEGER
);
CREATE TABLE IF NOT EXISTS bot_users (
    user_id INTEGER PRIMARY KEY,
    display_name TEXT NOT NULL,
    private_started INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pending_registrations (
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    prompt_id INTEGER NOT NULL,
    menu_message_id INTEGER,
    action TEXT NOT NULL,
    device_id INTEGER REFERENCES devices(id),
    revision INTEGER,
    expires_at INTEGER NOT NULL,
    PRIMARY KEY(user_id, chat_id, prompt_id)
);
CREATE INDEX IF NOT EXISTS pending_expiry ON pending_registrations(expires_at);
CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS night_holds (
    device_id INTEGER PRIMARY KEY REFERENCES devices(id) ON DELETE CASCADE,
    morning_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS scan_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    last_scan INTEGER,
    reference_mac TEXT
);
INSERT OR IGNORE INTO scan_state(singleton) VALUES (1);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY,
    event_key TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    body TEXT NOT NULL,
    review_device_id INTEGER REFERENCES devices(id),
    review_revision INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    UNIQUE(event_key, chat_id, body)
);
CREATE INDEX IF NOT EXISTS outbox_due ON outbox(status, available_at, id);
"""

DEVICE_SELECT = """
SELECT d.*, g.name AS guest_name, COALESCE(n.monitor, 0) AS monitor,
       COALESCE(n.keys_enabled, 0) AS keys_enabled,
       COALESCE(n.presence_alerts, 0) AS presence_alerts, n.recipient_chat_id,
       h.morning_at AS night_morning_at
FROM devices d
LEFT JOIN guests g ON g.id = d.guest_id
LEFT JOIN notification_settings n ON n.device_id = d.id
LEFT JOIN night_holds h ON h.device_id = d.id
"""

RESET_CANDIDATES = """
UPDATE devices SET negative_since=NULL,
    positive_since=NULL, positive_count=0
"""


async def one(conn, query, args=()):
    async with conn.execute(query, args) as cursor:
        return await cursor.fetchone()


async def all_rows(conn, query, args=()):
    async with conn.execute(query, args) as cursor:
        return await cursor.fetchall()


async def add_device(conn, mac, name, vendor="", ip="") -> int:
    async with conn.execute(
        "INSERT INTO devices(mac, name, vendor, ip) VALUES (?, ?, ?, ?)",
        (mac, name, vendor, ip),
    ) as cursor:
        return cursor.lastrowid


async def device_by_id(conn, device_id):
    return await one(conn, DEVICE_SELECT + " WHERE d.id=?", (device_id,))


async def configured_chat(conn, settings, role):
    override = await get_setting(conn, role)
    allowed = settings.admins | settings.group_ids if role == "admin" else settings.recipients
    if override in allowed:
        return override
    return settings.default_chats[role]


async def get_setting(conn, key, default=None):
    row = await one(conn, "SELECT value FROM bot_settings WHERE key=?", (key,))
    return default if row is None else row["value"]


async def set_setting(conn, key, value):
    await conn.execute(
        "INSERT INTO bot_settings(key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )


async def set_night_mode(conn, enabled):
    await set_setting(conn, "night_mode", int(enabled))
    if not enabled:
        await conn.execute(RESET_CANDIDATES + " WHERE id IN (SELECT device_id FROM night_holds)")
        await conn.execute("DELETE FROM night_holds")
        await conn.execute("DELETE FROM bot_settings WHERE key='night_capture'")


async def guest_by_name(conn, name):
    await conn.execute(
        "INSERT OR IGNORE INTO guests(name, name_key) VALUES (?, ?)", (name, name.casefold())
    )
    return await one(conn, "SELECT * FROM guests WHERE name_key=?", (name.casefold(),))


async def assign_category(conn, device_id, category, *, name=None, guest_id=None):
    await conn.execute("DELETE FROM night_holds WHERE device_id=?", (device_id,))
    await conn.execute(
        """UPDATE devices SET category=?, name=COALESCE(?, name), guest_id=?,
           owner_name='', owner_user_id=NULL, revision=revision+1,
           status='unknown', negative_since=NULL,
           positive_since=NULL, positive_count=0 WHERE id=?""",
        (category, name, guest_id, device_id),
    )
    await conn.execute(
        """INSERT INTO notification_settings(device_id, monitor) VALUES (?, ?)
           ON CONFLICT(device_id) DO UPDATE SET monitor=excluded.monitor,
             keys_enabled=0, presence_alerts=0, recipient_chat_id=NULL""",
        (device_id, int(category == "guest")),
    )


async def rebind_mac(conn, device, mac):
    if device["mac"] == mac:
        return
    target = await one(conn, "SELECT * FROM devices WHERE mac=?", (mac,))
    if target is not None:
        has_history = await one(conn, "SELECT 1 FROM device_events WHERE device_id=? LIMIT 1", (target["id"],))
        if target["category"] not in {"unknown", "ignored"} or has_history:
            raise ValueError("MAC уже належить іншому запису з налаштуваннями або історією.")
        await conn.execute("DELETE FROM pending_registrations WHERE device_id=?", (target["id"],))
        await conn.execute(
            "UPDATE outbox SET status='cancelled' WHERE review_device_id=? AND status='pending'",
            (target["id"],),
        )
        await conn.execute("UPDATE outbox SET review_device_id=NULL WHERE review_device_id=?", (target["id"],))
        await conn.execute("DELETE FROM notification_settings WHERE device_id=?", (target["id"],))
        await conn.execute("DELETE FROM devices WHERE id=?", (target["id"],))
    await conn.execute("DELETE FROM night_holds WHERE device_id=?", (device["id"],))
    await conn.execute(
        """UPDATE devices SET mac=?, vendor=?, ip=?, last_seen=?, revision=revision+1,
           status='unknown', negative_since=NULL,
           positive_since=NULL, positive_count=0 WHERE id=?""",
        (mac, target["vendor"] if target else "", target["ip"] if target else "",
         target["last_seen"] if target else None, device["id"]),
    )
    previous_id = await add_device(conn, device["mac"], device["name"], device["vendor"], device["ip"])
    await conn.execute("UPDATE devices SET category='ignored' WHERE id=?", (previous_id,))


class Database:
    """Serialize short SQLite transactions on one background-thread connection."""

    def __init__(self, conn):
        self.conn = conn
        self.lock = asyncio.Lock()

    @classmethod
    @asynccontextmanager
    async def open(cls, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(path, isolation_level=None) as conn:
            conn.row_factory = aiosqlite.Row
            await conn.execute("PRAGMA foreign_keys=ON")
            await conn.execute("PRAGMA busy_timeout=5000")
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.executescript(SCHEMA)
            db = cls(conn)
            async with db.transaction():
                columns = await all_rows(conn, "PRAGMA table_info(pending_registrations)")
                if not any(column["name"] == "menu_message_id" for column in columns):
                    await conn.execute("ALTER TABLE pending_registrations ADD COLUMN menu_message_id INTEGER")
            yield db

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
                yield self.conn
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise

    async def prepare_runtime(self, reference_mac: str):
        async with self.transaction() as conn:
            await conn.execute(RESET_CANDIDATES)
            # A previous send may have reached Telegram before the process stopped.
            await conn.execute("UPDATE outbox SET status='uncertain' WHERE status='sending'")
            await conn.execute("DELETE FROM pending_registrations WHERE expires_at<=?", (int(time.time()),))
            await conn.execute(
                "UPDATE scan_state SET last_scan=NULL WHERE reference_mac IS NOT NULL AND reference_mac<>?",
                (reference_mac,),
            )
            await conn.execute("UPDATE scan_state SET reference_mac=?", (reference_mac,))
