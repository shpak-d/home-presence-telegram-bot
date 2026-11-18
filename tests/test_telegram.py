from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import AnswerCallbackQuery, EditMessageText, SendMessage
from aiogram.types import ForceReply, Message, Update

from database import DEVICE_SELECT, Database, all_rows
from presence_tracker import PresenceTracker
from telegram_bot import build_router
from test_presence import ADMIN, GROUP, MEMBER, PHONE, START, settings


class BotTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.path = Path(folder.name) / "test.sqlite3"
        context = Database.open(self.path)
        self.db = await context.__aenter__()
        self.addAsyncCleanup(context.__aexit__, None, None, None)
        self.config = settings()
        self.dispatcher = Dispatcher(disable_fsm=True)
        self.dispatcher.include_router(build_router(self.db, self.config, PresenceTracker(self.db, self.config)))
        self.session = AiohttpSession()
        self.addAsyncCleanup(self.session.close)
        self.session.make_request = AsyncMock(side_effect=self.respond)
        self.bot = Bot("12345:testing", session=self.session)
        self.group = {"type": "supergroup", "id": GROUP, "title": "Дім"}
        self.sent = []
        self.update_id = 0

    def bot_user(self):
        return {"id": self.bot.id, "first_name": "Presence", "is_bot": True, "username": "PresenceBot"}

    def respond(self, bot, method, timeout=None):
        self.sent.append(method)
        if isinstance(method, AnswerCallbackQuery):
            return True
        if isinstance(method, EditMessageText):
            return True
        if isinstance(method, SendMessage):
            return Message.model_validate({
                "message_id": 100 + len(self.sent), "date": START, "chat": self.group,
                "from": self.bot_user(), "text": method.text,
            }, context={"bot": bot})
        raise AssertionError(type(method))

    async def feed(self, **item):
        self.update_id += 1
        update = Update.model_validate({"update_id": self.update_id, **item}, context={"bot": self.bot})
        return await self.dispatcher.feed_update(self.bot, update)

    async def send(self, user, text, reply_to=None):
        message = {
            "message_id": self.update_id + 1, "date": START, "chat": self.group,
            "from": {"id": user, "first_name": "Тест", "is_bot": False}, "text": text,
        }
        if reply_to is not None:
            message["reply_to_message"] = {
                "message_id": reply_to, "date": START, "chat": self.group,
                "from": self.bot_user(), "text": "Запит",
            }
        return await self.feed(message=message)

    async def click(self, user, data, menu_id=99):
        await self.feed(callback_query={
            "id": str(self.update_id + 1), "chat_instance": "test", "data": data,
            "from": {"id": user, "first_name": "Тест", "is_bot": False},
            "message": {"message_id": menu_id, "date": START, "chat": self.group, "from": self.bot_user()},
        })
        return 100 + len(self.sent)

    async def registered(self):
        async with self.db.transaction() as conn:
            return await all_rows(conn, DEVICE_SELECT)

    async def test_group_registration_owner_and_notification_settings(self):
        await self.send(ADMIN, "/devices")
        self.assertIsInstance(self.sent[-1], SendMessage)
        add_prompt = await self.click(ADMIN, "h:add")
        self.assertIsInstance(self.sent[-1].reply_markup, ForceReply)
        await self.send(MEMBER, PHONE, reply_to=add_prompt)
        self.assertFalse(await self.registered())
        await self.send(ADMIN, PHONE, reply_to=add_prompt)
        self.assertIsInstance(self.sent[-1], EditMessageText)
        self.assertEqual(self.sent[-1].message_id, 99)
        device, = await self.registered()
        device_id = device["id"]
        category_prompt = await self.click(ADMIN, f"h:category:{device_id}:0:resident")
        await self.send(ADMIN, "iPhone Ані", reply_to=category_prompt)
        owner_prompt = await self.click(ADMIN, f"h:edit:{device_id}:1:owner")
        await self.send(ADMIN, f"Аня | {MEMBER}", reply_to=owner_prompt)
        await self.click(ADMIN, f"h:toggle:{device_id}:2:monitor")
        await self.click(ADMIN, f"h:toggle:{device_id}:3:keys_enabled")
        recipient_prompt = await self.click(ADMIN, f"h:edit:{device_id}:4:recipient")
        await self.send(ADMIN, str(GROUP), reply_to=recipient_prompt)
        device, = await self.registered()
        self.assertEqual(
            (device["name"], device["owner_user_id"], device["monitor"], device["keys_enabled"],
             device["recipient_chat_id"]),
            ("iPhone Ані", MEMBER, 1, 1, GROUP),
        )

    async def test_unknown_user_and_member_cannot_change_categories(self):
        await self.send(77777, "/devices")
        self.assertEqual(self.sent, [])
        add_prompt = await self.click(ADMIN, "h:add")
        await self.send(ADMIN, PHONE, reply_to=add_prompt)
        device, = await self.registered()
        await self.click(MEMBER, f"h:category:{device['id']}:0:ignored")
        self.assertTrue(self.sent[-1].show_alert)
        changed, = await self.registered()
        self.assertEqual(changed["category"], "unknown")

    async def test_menu_navigation_edits_the_original_message(self):
        await self.send(ADMIN, "/start")
        menu_id = 100 + len(self.sent)
        for page in ("devices", "settings", "start"):
            await self.click(ADMIN, f"h:page:{page}:0", menu_id)

        edits = [method for method in self.sent if isinstance(method, EditMessageText)]
        self.assertEqual([edit.message_id for edit in edits], [menu_id] * 3)
        self.assertEqual([edit.text.splitlines()[0] for edit in edits], ["Пристрої", "Сповіщення", "Home Presence"])
        self.assertEqual(sum(isinstance(method, SendMessage) for method in self.sent), 1)

    async def test_unchanged_menu_does_not_send_an_extra_message(self):
        def unchanged(bot, method, timeout=None):
            if isinstance(method, EditMessageText):
                raise TelegramBadRequest(method, "Bad Request: message is not modified")
            return self.respond(bot, method, timeout)

        self.session.make_request.side_effect = unchanged
        await self.click(ADMIN, "h:page:start:0")
        self.assertEqual(len(self.sent), 1)
        self.assertIsInstance(self.sent[0], AnswerCallbackQuery)

    async def test_status_reports_successful_tenda_poll(self):
        async with self.db.transaction() as conn:
            await conn.execute("UPDATE poll_state SET last_poll=? WHERE singleton=1", (START,))
        await self.send(ADMIN, "/status")
        self.assertIn("Останній прийнятий список Tenda:", self.sent[-1].text)
        self.assertIn("Інтервал опитування: 60 с", self.sent[-1].text)

    async def test_status_reports_custom_poll_interval(self):
        self.config.poll_seconds = 300
        await self.send(ADMIN, "/status")
        self.assertIn("Інтервал опитування: 300 с", self.sent[-1].text)
