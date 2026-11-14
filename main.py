import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import aiohttp
from aiogram import Bot, Dispatcher
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.exceptions import TelegramConflictError, TelegramUnauthorizedError
from aiogram.methods import GetUpdates

from config import Config
from database import Database
from netalertx_client import NetAlertXClient
from notifications import run_outbox
from presence_tracker import PresenceTracker
from telegram_bot import bot_commands, build_router, review_keyboard

HEALTH_FILE = Path("/tmp/home-presence.alive")
HEALTH_MAX_AGE = 120
log = logging.getLogger(__name__)


class RedactSecrets(logging.Filter):
    def __init__(self, *values):
        super().__init__()
        self.values = tuple(value for value in values if value)

    def filter(self, record):
        message = record.getMessage()
        for value in self.values:
            message = message.replace(value, "[приховано]")
        record.msg, record.args = message, ()
        record.exc_info = record.exc_text = record.stack_info = None
        return True


def setup_logging(config):
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    handler.addFilter(RedactSecrets(config.telegram_token, config.api_token))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    logging.getLogger("aiogram.event").setLevel(logging.WARNING)


async def supervise_health(ticks, poll_seconds):
    while True:
        now = time.monotonic()
        if "telegram_fatal" in ticks:
            raise RuntimeError("Telegram відхилив long polling. Перевірте токен, webhook і кількість запущених копій.")
        if ticks.keys() >= {"scan", "outbox", "telegram"}:
            if (now - ticks["scan"] < poll_seconds + 30
                    and now - ticks["outbox"] < 60
                    and now - ticks["telegram"] < 60):
                HEALTH_FILE.touch()
        await asyncio.sleep(5)


async def serve(config):
    HEALTH_FILE.unlink(missing_ok=True)
    ticks = {}

    def tick(key):
        ticks[key] = time.monotonic()

    async def telegram_heartbeat(make_request, bot, method):
        try:
            result = await make_request(bot, method)
        except (TelegramConflictError, TelegramUnauthorizedError):
            if isinstance(method, GetUpdates):
                ticks["telegram_fatal"] = time.monotonic()
            raise
        if isinstance(method, GetUpdates):
            tick("telegram")
        return result

    telegram_session = AiohttpSession(timeout=20)
    telegram_session.middleware(telegram_heartbeat)
    async with (
        Bot(config.telegram_token, session=telegram_session) as bot,
        aiohttp.ClientSession(trust_env=False) as session,
        Database.open(config.database_path) as db,
    ):
        await db.prepare_runtime(config.reference_mac)
        webhook = await bot.get_webhook_info()
        if webhook.url:
            raise RuntimeError("Вимкніть активний webhook Telegram для використання long polling.")
        await bot.set_my_commands(bot_commands())
        tracker = PresenceTracker(db, config)
        client = NetAlertXClient(session, config.api_url, config.api_token, config.reference_mac, config.api_timeout)
        dispatcher = Dispatcher(disable_fsm=True)
        dispatcher.include_router(build_router(db, config, tracker))
        log.info("Бот запущено. Інтервал читання API %s с.", config.poll_seconds)
        tasks = [
            asyncio.create_task(dispatcher.start_polling(
                bot, polling_timeout=20, handle_as_tasks=False, close_bot_session=False,
            ), name="telegram"),
            asyncio.create_task(tracker.run(client, lambda: tick("scan")), name="presence"),
            asyncio.create_task(run_outbox(db, config, bot, review_keyboard, lambda: tick("outbox")), name="outbox"),
            asyncio.create_task(supervise_health(ticks, config.poll_seconds), name="health"),
        ]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
                if task.get_name() != "telegram":
                    raise RuntimeError(f"Зупинився процес {task.get_name()}.")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            HEALTH_FILE.unlink(missing_ok=True)


def healthcheck() -> int:
    try:
        return int(not 0 <= time.time() - HEALTH_FILE.stat().st_mtime < HEALTH_MAX_AGE)
    except OSError:
        return 1


def main() -> int:
    if sys.argv[1:] == ["--healthcheck"]:
        return healthcheck()
    os.umask(0o077)
    try:
        config = Config.from_env()
    except ValueError as exc:
        print(f"Помилка конфігурації. {exc}", file=sys.stderr)
        return 2
    except (OSError, KeyError):
        print("Перевірте .env, доступ до файлів і наявність часового поясу Europe/Kyiv.", file=sys.stderr)
        return 2
    setup_logging(config)
    try:
        asyncio.run(serve(config))
    except KeyboardInterrupt:
        return 0
    except RuntimeError as exc:
        log.error("Робота бота перервана. %s", exc)
        return 1
    except Exception as exc:
        log.error("Робота бота перервана (%s)", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
