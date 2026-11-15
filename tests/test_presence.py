import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, MagicMock, patch
from zoneinfo import ZoneInfo

import aiohttp
from aiogram.exceptions import TelegramNetworkError
from aiogram.methods import SendMessage

from config import PresenceRules
from database import (
    Database, add_device, all_rows, assign_category, device_by_id, guest_by_name, rebind_mac, set_night_mode,
)
from netalertx_client import NetAlertXClient, NetAlertXError, parse_snapshot
from notifications import deliver_one
from presence_tracker import PresenceTracker
from telegram_bot import review_keyboard

ANCHOR = "AA:00:00:00:00:01"
PHONE = "AA:00:00:00:00:02"
GUEST = "AA:00:00:00:00:03"
OTHER = "AA:00:00:00:00:04"
START = 1794636000
ADMIN = 11001
MEMBER = 11002
GROUP = -1001234567890


def settings(**kwargs):
    return SimpleNamespace(**{
        "reference_mac": ANCHOR, "rules": PresenceRules(), "admins": frozenset({ADMIN}),
        "members": frozenset({MEMBER}), "recipients": frozenset({ADMIN, MEMBER, GROUP}),
        "group_ids": frozenset({GROUP}), "default_chats": {"admin": ADMIN, "guest": GROUP, "discovery": GROUP},
        "announce_initial": False, "poll_seconds": 30, "scan_seconds": 60, "timezone": timezone.utc,
        **kwargs,
    })


def raw_device(mac, seen, present=True, ip="192.168.0.42", **extra):
    return {
        "devMac": mac.lower(), "devLastIP": ip, "devName": "Unknown", "devVendor": "Apple",
        "devPresentLastScan": int(present), "devIsArchived": 0, "devScan": 1,
        "devForceStatus": "dont_force", "devParentRelType": "", **extra,
        "devLastConnection": datetime.fromtimestamp(seen, timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def payload(*rows):
    return {"success": True, "devices": list(rows)}


class PresenceTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = settings()
        self.start = START
        self.stack = AsyncExitStack()
        self.addAsyncCleanup(self.stack.aclose)
        root = self.stack.enter_context(TemporaryDirectory())
        self.path = Path(root) / "test.sqlite3"
        await self.open_db()

    async def open_db(self):
        self.connection = Database.open(self.path)
        self.db = await self.connection.__aenter__()
        self.tracker = PresenceTracker(self.db, self.config)
        await self.db.prepare_runtime(ANCHOR)
        self.stack.push_async_callback(self.connection.__aexit__, None, None, None)

    async def register(self, mac=PHONE, *, kind="resident", user=MEMBER, recipient=None,
                       name="Аня", presence_alerts=False):
        async with self.db.transaction() as conn:
            device_id = await add_device(conn, mac, "Телефон")
            if kind == "guest":
                person = await guest_by_name(conn, name)
                await assign_category(conn, device_id, kind, guest_id=person["id"])
            else:
                await assign_category(conn, device_id, kind)
                await conn.execute(
                    "UPDATE devices SET owner_user_id=?, owner_name=? WHERE id=?", (user, name, device_id)
                )
                await conn.execute(
                    """UPDATE notification_settings SET monitor=1, keys_enabled=1,
                       presence_alerts=?, recipient_chat_id=? WHERE device_id=?""",
                    (int(presence_alerts), recipient, device_id),
                )
        return device_id

    async def scan(self, minute, *rows, now_offset=0, control=True):
        instant = self.start + minute * 60
        if control:
            rows = (raw_device(ANCHOR, instant, ip="192.168.0.1"), *rows)
        return await self.tracker.process(parse_snapshot(payload(*rows), ANCHOR), instant + now_offset)

    async def device(self, device_id):
        async with self.db.transaction() as conn:
            return await device_by_id(conn, device_id)

    async def records(self, table):
        assert table in {"device_events", "outbox"}
        async with self.db.transaction() as conn:
            return await all_rows(conn, f"SELECT * FROM {table} ORDER BY id")

    async def positive(self, minute, mac=PHONE, **kwargs):
        return await self.scan(minute, raw_device(mac, self.start + minute * 60, **kwargs))

    async def absence(self, first, end, mac=PHONE, last_seen=1):
        for minute in range(first, end):
            await self.scan(minute, raw_device(mac, self.start + last_seen * 60, False))

    def start_before_night(self):
        self.config.timezone = ZoneInfo("Europe/Kyiv")
        self.start = int(datetime(2026, 11, 14, 22, 50, tzinfo=self.config.timezone).timestamp())

    async def test_two_real_scans_and_ip_changes(self):
        device_id = await self.register()
        self.assertTrue(await self.positive(0))
        self.assertFalse(await self.scan(0, raw_device(PHONE, START), now_offset=30))
        self.assertEqual((await self.device(device_id))["status"], "unknown")
        await self.positive(1, ip="192.168.0.109")
        device = await self.device(device_id)
        self.assertEqual((device["status"], device["ip"], device["mac"]), ("online", "192.168.0.109", PHONE))
        self.assertEqual(device["last_seen"], START + 60)
        self.assertFalse(await self.records("outbox"))

    async def test_long_absence_and_duplicate_event(self):
        device_id = await self.register(recipient=GROUP)
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 7)
        self.assertEqual((await self.device(device_id))["status"], "online")
        await self.absence(7, 22)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        await self.positive(22)
        self.assertEqual(len(await self.records("device_events")), 1)
        await self.positive(23)
        self.assertFalse(await self.positive(23))
        self.assertEqual([row["kind"] for row in await self.records("device_events")], ["departure", "arrival"])
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Аня повернулася додому"), (GROUP, "Аня, не забудь перевірити ключі!"),
        ])

    async def test_keys_reminder_after_five_hour_absence_with_false_positive(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 302)

        await self.positive(302)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertFalse(await self.records("outbox"))
        await self.absence(303, 304, last_seen=302)

        await self.positive(304)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertFalse(await self.records("outbox"))
        await self.positive(305)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.positive(305))
        await self.positive(306)

        self.assertEqual([row["kind"] for row in await self.records("device_events")], ["departure", "arrival"])
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Аня повернулася додому"), (MEMBER, "Аня, не забудь перевірити ключі!"),
        ])

    async def test_brief_drop_and_twenty_minute_floor(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 5)
        await self.positive(5)
        self.assertFalse(await self.records("device_events"))
        await self.absence(6, 25, last_seen=5)
        await self.positive(25)
        await self.positive(26)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertEqual(len(await self.records("device_events")), 2)
        self.assertFalse(await self.records("outbox"))

    async def test_admin_reminder_only(self):
        await self.register(user=ADMIN, name="Андрій")
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 22)
        await self.positive(22)
        await self.positive(23)
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")],
                         [(ADMIN, "З поверненням, перевір ключі")])

    async def test_individually_disabled_reminder(self):
        device_id = await self.register()
        async with self.db.transaction() as conn:
            await conn.execute("UPDATE notification_settings SET keys_enabled=0 WHERE device_id=?", (device_id,))
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 22)
        await self.positive(22)
        await self.positive(23)
        self.assertEqual(len(await self.records("device_events")), 2)
        self.assertFalse(await self.records("outbox"))

    async def test_restart_with_previously_confirmed_absence(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 22)
        await self.positive(22)
        await self.connection.__aexit__(None, None, None)
        await self.open_db()
        self.assertFalse(await self.positive(22))
        await self.positive(23)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertFalse(await self.records("outbox"))
        await self.positive(24)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertEqual([row["kind"] for row in await self.records("device_events")], ["departure", "arrival"])
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Аня повернулася додому"), (MEMBER, "Аня, не забудь перевірити ключі!"),
        ])

    async def test_missing_stale_and_incomplete_data_are_not_absence(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 7)
        # No phone in the API response, then no control, then a stale positive flag.
        await self.scan(7)
        self.assertFalse(await self.scan(8, raw_device(PHONE, START, False), control=False))
        await self.scan(9, raw_device(PHONE, START + 60))
        self.assertFalse(await self.scan(9, raw_device(PHONE, START + 60), now_offset=120))
        await self.absence(12, 17)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("device_events"))

    async def test_api_outage_breaks_positive_confirmation(self):
        device_id = await self.register()
        await self.positive(0)
        client = SimpleNamespace(snapshot=AsyncMock(side_effect=NetAlertXError("NetAlertX HTTP 503")))
        with patch("presence_tracker.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.tracker.run(client, lambda: None)
        self.assertEqual(self.tracker.status.error, "NetAlertX HTTP 503")
        await self.positive(1)
        self.assertEqual((await self.device(device_id))["status"], "unknown")
        await self.positive(2)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("outbox"))

    async def test_guest_alerts_once_per_visit_across_multiple_macs_at_night(self):
        self.start_before_night()
        for mac in (GUEST, OTHER):
            await self.register(mac, kind="guest", name="Олександр")
        last_seen = {GUEST: -1, OTHER: -1}
        for minute in range(29):
            rows = []
            for mac, present in ((GUEST, 11 <= minute <= 12), (OTHER, 11 <= minute <= 22)):
                if present:
                    last_seen[mac] = minute
                rows.append(raw_device(mac, self.start + last_seen[mac] * 60, present))
            await self.scan(minute, *rows)
            if minute == 18:
                self.assertEqual(len(await self.records("outbox")), 2)
        self.assertFalse(await self.scan(28, *rows))
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Олександр прийшов у гості"), (GROUP, "Олександр прийшов у гості"), (ADMIN, "Олександр пішов"),
        ])
        self.assertEqual(len(await self.records("device_events")), 4)

        self.config.default_chats["guest"] = ADMIN
        await self.positive(29, GUEST)
        await self.positive(30, GUEST)
        self.assertEqual([(row["chat_id"], row["body"]) for row in (await self.records("outbox"))[3:]], [
            (ADMIN, "Олександр прийшов у гості"),
        ])

    async def test_night_hold_survives_restart_and_morning_recovery_is_silent(self):
        self.start_before_night()
        device_id = await self.register(presence_alerts=True)
        await self.positive(8)
        await self.positive(9)
        await self.absence(10, 20, last_seen=9)
        self.assertEqual((await self.device(device_id))["status"], "online")

        await self.connection.__aexit__(None, None, None)
        await self.open_db()
        await self.absence(60, 67, last_seen=9)
        self.assertEqual((await self.device(device_id))["status"], "online")
        await self.absence(610, 616, last_seen=9)  # 09:00 through 09:05
        self.assertEqual((await self.device(device_id))["status"], "online")
        await self.positive(616)
        await self.absence(617, 626, last_seen=616)
        await self.positive(626)
        await self.positive(627)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("device_events"))
        self.assertFalse(await self.records("outbox"))

    async def test_morning_departure_uses_fresh_scans_and_preserves_keys_reminder(self):
        self.start_before_night()
        device_id = await self.register(presence_alerts=True)
        await self.positive(8)
        await self.positive(9)
        await self.absence(10, 20, last_seen=9)
        await self.absence(610, 625, last_seen=9)
        self.assertFalse(await self.records("outbox"))

        await self.absence(625, 626, last_seen=9)  # 09:15
        device = await self.device(device_id)
        self.assertEqual((device["status"], device["last_departure"]), ("offline", self.start + 610 * 60))
        await self.absence(626, 630, last_seen=9)
        await self.positive(630)
        self.assertEqual(len(await self.records("outbox")), 1)
        await self.positive(631)
        self.assertFalse(await self.positive(631))
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Телефон · поза мережею"), (ADMIN, "Телефон · у мережі"),
            (ADMIN, "Аня повернулася додому"), (MEMBER, "Аня, не забудь перевірити ключі!"),
        ])

    async def test_night_hold_uses_only_residents_confirmed_home_at_start(self):
        self.start_before_night()
        device_id = await self.register(presence_alerts=True)
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 22)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        await self.positive(22)
        await self.positive(23)
        await self.absence(24, 30, last_seen=23)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertEqual([(row["chat_id"], row["body"]) for row in await self.records("outbox")], [
            (ADMIN, "Телефон · поза мережею"), (ADMIN, "Телефон · у мережі"),
            (ADMIN, "Аня повернулася додому"), (MEMBER, "Аня, не забудь перевірити ключі!"),
            (ADMIN, "Телефон · поза мережею"),
        ])

    async def test_disabling_night_mode_releases_hold(self):
        self.start_before_night()
        device_id = await self.register(presence_alerts=True)
        await self.positive(8)
        await self.positive(9)
        await self.absence(10, 20, last_seen=9)
        self.assertEqual((await self.device(device_id))["status"], "online")

        async with self.db.transaction() as conn:
            await set_night_mode(conn, False)
        await self.absence(20, 26, last_seen=9)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertEqual([row["body"] for row in await self.records("outbox")], ["Телефон · поза мережею"])

    async def test_discovery_alert_and_classification_survive_replays(self):
        await self.scan(0)
        await self.positive(1, mac=OTHER)
        self.assertFalse(await self.positive(1, mac=OTHER))
        discovery, = await self.records("outbox")
        self.assertIn("Виробник: Apple", discovery["body"])
        bot = SimpleNamespace(send_message=AsyncMock())
        self.assertTrue(await deliver_one(self.db, self.config, bot, review_keyboard, now=START + 60))
        markup = bot.send_message.call_args.kwargs["reply_markup"]
        self.assertEqual(sum(len(row) for row in markup.inline_keyboard), 4)
        async with self.db.transaction() as conn:
            await assign_category(conn, discovery["review_device_id"], "ignored")
        await self.positive(2, mac=OTHER)
        await self.positive(3, mac=OTHER)
        self.assertFalse(await deliver_one(self.db, self.config, bot, review_keyboard, now=START + 180))
        self.assertEqual(len(await self.records("outbox")), 1)

    async def test_no_retry_on_ambiguous_delivery(self):
        await self.scan(0)
        await self.positive(1, mac=OTHER)
        error = TelegramNetworkError(method=SendMessage(chat_id=ADMIN, text="test"), message="timeout")
        bot = SimpleNamespace(send_message=AsyncMock(side_effect=error))
        self.assertTrue(await deliver_one(self.db, self.config, bot, review_keyboard, now=START + 60))
        self.assertFalse(await deliver_one(self.db, self.config, bot, review_keyboard, now=START + 61))
        self.assertEqual((await self.records("outbox"))[0]["status"], "uncertain")

    async def test_rebind_keeps_logical_id_and_history(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 8)
        await self.positive(8, mac=OTHER)
        async with self.db.transaction() as conn:
            original = await device_by_id(conn, device_id)
            await rebind_mac(conn, original, OTHER)
        await self.positive(9, mac=OTHER)
        await self.positive(10, mac=OTHER)
        device = await self.device(device_id)
        self.assertEqual((device["mac"], device["status"], device["keys_enabled"]), (OTHER, "online", 1))
        self.assertEqual((await self.records("device_events"))[0]["mac"], PHONE)

    async def test_rebind_does_not_use_previous_mac_departure_for_reminder(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 8)
        async with self.db.transaction() as conn:
            await rebind_mac(conn, await device_by_id(conn, device_id), OTHER)

        await self.absence(22, 28, mac=OTHER, last_seen=21)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertEqual([row["kind"] for row in await self.records("device_events")], ["departure"])
        await self.positive(28, mac=OTHER)
        await self.positive(29, mac=OTHER)
        self.assertFalse(await self.records("outbox"))


class ClientTests(IsolatedAsyncioTestCase):
    async def test_authorization_and_unavailable_netalertx(self):
        response = SimpleNamespace(status=200, json=AsyncMock(return_value=payload(
            raw_device(ANCHOR, START, ip="192.168.0.1")
        )))
        request = MagicMock()
        request.__aenter__.return_value = response
        session = SimpleNamespace(get=MagicMock(return_value=request))
        client = NetAlertXClient(session, "http://example.invalid:20212", "private", ANCHOR, 10)
        self.assertEqual((await client.snapshot()).scan_time, START)
        self.assertEqual(session.get.call_args.args, ("http://example.invalid:20212/devices",))
        self.assertEqual(session.get.call_args.kwargs["headers"]["Authorization"], "Bearer private")
        response.status = 503
        with self.assertRaisesRegex(NetAlertXError, "503"):
            await client.snapshot()
        request.__aenter__.side_effect = aiohttp.ClientError("private")
        with self.assertRaises(NetAlertXError) as error:
            await client.snapshot()
        self.assertNotIn("private", str(error.exception))


class ContractTests(TestCase):
    def test_reports_reject_duplicate_or_malformed_records(self):
        reference = raw_device(ANCHOR, START)
        for rows in ([reference, reference], [reference, {"devMac": PHONE}], []):
            with self.subTest(rows=rows), self.assertRaises(NetAlertXError):
                parse_snapshot(payload(*rows), ANCHOR)

    def test_unsupported_presence_is_indeterminate(self):
        reference = raw_device(ANCHOR, START)
        for row in (raw_device(PHONE, START, False),
                    raw_device(PHONE, START - 60, True),
                    raw_device(PHONE, START, True, devForceStatus="online")):
            snapshot = parse_snapshot(payload(reference, row), ANCHOR)
            self.assertIsNone(snapshot.devices[PHONE].observed_at(START))
