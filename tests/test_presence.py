import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, Mock, patch
from zoneinfo import ZoneInfo

from aiogram.exceptions import TelegramNetworkError
from aiogram.methods import SendMessage

from config import Config, PresenceRules
from database import Database, add_device, all_rows, assign_category, device_by_id, guest_by_name, rebind_mac
from notifications import deliver_one
from presence_tracker import PresenceTracker
from telegram_bot import review_keyboard
from tenda_client import TendaError, parse_online_list

PHONE = "AA:00:00:00:00:02"
GUEST = "AA:00:00:00:00:03"
OTHER = "AA:00:00:00:00:04"
START = 1794636000
ADMIN = 11001
MEMBER = 11002
GROUP = -1001234567890


def settings(**kwargs):
    return SimpleNamespace(**{
        "rules": PresenceRules(), "admins": frozenset({ADMIN}),
        "members": frozenset({MEMBER}), "recipients": frozenset({ADMIN, MEMBER, GROUP}),
        "group_ids": frozenset({GROUP}), "default_chats": {"admin": ADMIN, "guest": GROUP, "discovery": GROUP},
        "announce_initial": False, "timezone": timezone.utc, "poll_seconds": 60,
        **kwargs,
    })


def raw_device(mac, ip="192.168.0.42"):
    return {"deviceId": mac.lower(), "ip": ip, "devName": "Unknown", "vendor": "Apple"}


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
        await self.db.prepare_runtime()
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

    async def poll(self, minute, *rows, seconds=0):
        return await self.tracker.process(parse_online_list(list(rows)), self.start + minute * 60 + seconds)

    async def device(self, device_id):
        async with self.db.transaction() as conn:
            return await device_by_id(conn, device_id)

    async def records(self, table):
        assert table in {"device_events", "outbox"}
        async with self.db.transaction() as conn:
            return await all_rows(conn, f"SELECT * FROM {table} ORDER BY id")

    async def positive(self, minute, mac=PHONE, **kwargs):
        return await self.poll(minute, raw_device(mac, **kwargs))

    async def absence(self, first, end):
        for minute in range(first, end):
            await self.poll(minute)
            await self.poll(minute, seconds=30)

    def start_before_night(self):
        self.config.timezone = ZoneInfo("Europe/Kyiv")
        self.start = int(datetime(2026, 11, 14, 22, 50, tzinfo=self.config.timezone).timestamp())

    async def test_configured_poll_intervals_allow_jitter_but_reset_long_gaps(self):
        device_id = await self.register()
        devices = parse_online_list([raw_device(PHONE)])
        for interval in (30, 60, 300):
            with self.subTest(poll_seconds=interval), patch.dict("os.environ", {
                "TELEGRAM_BOT_TOKEN": "12345:testing", "TENDA_PASSWORD": "test",
                "TENDA_URL": "http://192.168.0.1", "ADMIN_USER_IDS": str(ADMIN),
                "POLL_SECONDS": str(interval),
            }, clear=True):
                self.config.rules = Config.from_env().rules
                self.config.poll_seconds = interval
                async with self.db.transaction() as conn:
                    await conn.execute("UPDATE devices SET status='unknown' WHERE id=?", (device_id,))
                    await conn.execute("UPDATE poll_state SET last_poll=NULL")
                await self.tracker.invalidate_candidates()
                await self.tracker.process(devices, START)
                await self.tracker.process(devices, START + interval + 1)
                self.assertEqual((await self.device(device_id))["status"], "online")
                # A long gap interrupts the pending absence instead of confirming departure.
                await self.tracker.process({}, START + 2 * interval + 1)
                resumed = START + 4 * interval + 2
                await self.tracker.process({}, resumed)
                self.assertEqual((await self.device(device_id))["negative_since"], resumed)
                self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("device_events"))

    async def test_sixty_second_polls_confirm_departure_after_300_seconds(self):
        device_id = await self.register(presence_alerts=True)
        await self.positive(0)
        await self.positive(1)
        for minute in range(2, 7):
            await self.poll(minute)
            self.assertEqual((await self.device(device_id))["status"], "online")
        await self.poll(7)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        event, = await self.records("device_events")
        self.assertEqual((event["observed_at"], event["confirmed_at"]), (START + 120, START + 420))

    async def test_run_sleeps_for_configured_interval_minus_request_time(self):
        client = SimpleNamespace(snapshot=AsyncMock(return_value={}))
        for interval in (30, 60, 300):
            with self.subTest(poll_seconds=interval):
                self.config.poll_seconds = interval
                heartbeat = Mock()
                with (
                    patch.object(self.tracker, "process", new=AsyncMock()),
                    patch("presence_tracker.time.monotonic", side_effect=[100, 107]),
                    patch("presence_tracker.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)) as sleep,
                ):
                    with self.assertRaises(asyncio.CancelledError):
                        await self.tracker.run(client, heartbeat)
                sleep.assert_awaited_once_with(interval - 7)
                heartbeat.assert_called_once_with()

    async def test_api_failure_preserves_online_device_and_poll_timestamp(self):
        device_id = await self.register(presence_alerts=True)
        await self.positive(0)
        await self.positive(1)
        await self.poll(2)
        client = SimpleNamespace(snapshot=AsyncMock(side_effect=TendaError("Tenda HTTP 503")))
        with patch("presence_tracker.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.tracker.run(client, lambda: None)
        device = await self.device(device_id)
        self.assertEqual(device["status"], "online")
        self.assertIsNone(device["negative_since"])
        self.assertEqual(self.tracker.status.last_poll, START + 120)
        self.assertFalse(await self.records("device_events"))
        self.assertFalse(await self.records("outbox"))

    async def test_reopen_preserves_bindings_guests_history_and_telegram_settings(self):
        await self.register(presence_alerts=True, recipient=GROUP)
        await self.register(GUEST, kind="guest", name="Олександр")
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 8)
        await self.tracker.invalidate_candidates()
        async with self.db.transaction() as conn:
            await conn.execute("INSERT INTO bot_settings VALUES ('guest', ?)", (ADMIN,))
            await conn.execute("INSERT INTO bot_users VALUES (?, 'Аня', 1)", (MEMBER,))
            tables = ("guests", "devices", "notification_settings", "device_events",
                      "bot_settings", "bot_users", "outbox", "poll_state")
            before = {table: [tuple(row) for row in await all_rows(conn, f"SELECT * FROM {table}")]
                      for table in tables}
        await self.connection.__aexit__(None, None, None)
        await self.open_db()
        async with self.db.transaction() as conn:
            after = {table: [tuple(row) for row in await all_rows(conn, f"SELECT * FROM {table}")]
                     for table in tables}
        self.assertEqual(after, before)

    async def test_two_distinct_polls_and_ip_changes(self):
        device_id = await self.register()
        self.assertTrue(await self.positive(0))
        self.assertFalse(await self.positive(0))
        self.assertEqual((await self.device(device_id))["status"], "unknown")
        await self.poll(0, raw_device(PHONE, ip="192.168.0.109"), seconds=30)
        device = await self.device(device_id)
        self.assertEqual((device["status"], device["ip"], device["mac"]), ("online", "192.168.0.109", PHONE))
        self.assertEqual(device["last_seen"], START + 30)
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
        await self.absence(303, 304)

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
        await self.absence(6, 25)
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

    async def test_invalid_response_breaks_absence_confirmation(self):
        device_id = await self.register()
        await self.positive(0)
        await self.positive(1)
        await self.absence(2, 7)
        # The next valid poll would confirm departure if the invalid response counted.
        client = SimpleNamespace(snapshot=AsyncMock(side_effect=lambda: parse_online_list({})))
        with patch("presence_tracker.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.tracker.run(client, lambda: None)
        await self.absence(7, 12)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("device_events"))
        await self.poll(12)
        self.assertEqual((await self.device(device_id))["status"], "offline")

    async def test_api_outage_breaks_positive_confirmation(self):
        device_id = await self.register()
        await self.positive(0)
        client = SimpleNamespace(snapshot=AsyncMock(side_effect=TendaError("Tenda HTTP 503")))
        with patch("presence_tracker.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)):
            with self.assertRaises(asyncio.CancelledError):
                await self.tracker.run(client, lambda: None)
        self.assertEqual(self.tracker.status.error, "Tenda HTTP 503")
        await self.positive(1)
        self.assertEqual((await self.device(device_id))["status"], "unknown")
        await self.positive(2)
        self.assertEqual((await self.device(device_id))["status"], "online")
        self.assertFalse(await self.records("outbox"))

    async def test_guest_alerts_once_per_visit_across_multiple_macs_at_night(self):
        self.start_before_night()
        for mac in (GUEST, OTHER):
            await self.register(mac, kind="guest", name="Олександр")
        for minute in range(29):
            rows = []
            for mac, present in ((GUEST, 11 <= minute <= 12), (OTHER, 11 <= minute <= 22)):
                if present:
                    rows.append(raw_device(mac))
            await self.poll(minute, *rows)
            if minute == 18:
                self.assertEqual(len(await self.records("outbox")), 2)
        self.assertFalse(await self.poll(28, *rows))
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

    async def test_resident_departure_is_confirmed_after_23_00(self):
        self.start_before_night()
        device_id = await self.register(presence_alerts=True)
        await self.positive(8)
        await self.positive(9)
        await self.absence(10, 15)
        self.assertEqual((await self.device(device_id))["status"], "online")
        await self.poll(15)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertEqual([row["body"] for row in await self.records("outbox")], ["Телефон · поза мережею"])

    async def test_discovery_alert_and_classification_survive_replays(self):
        await self.poll(0)
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
        await self.poll(0)
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

        await self.absence(22, 28)
        self.assertEqual((await self.device(device_id))["status"], "offline")
        self.assertEqual([row["kind"] for row in await self.records("device_events")], ["departure"])
        await self.positive(28, mac=OTHER)
        await self.positive(29, mac=OTHER)
        self.assertFalse(await self.records("outbox"))
