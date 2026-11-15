import asyncio
import logging
import time

from aiogram.exceptions import (
    TelegramAPIError, TelegramBadRequest, TelegramForbiddenError,
    TelegramMigrateToChat, TelegramRetryAfter, TelegramUnauthorizedError,
)

from database import configured_chat, one

log = logging.getLogger(__name__)


async def enqueue(conn, key, target, body, now, *, review=None, revision=None, ttl=900):
    if target is None:
        return
    await conn.execute(
        """INSERT OR IGNORE INTO outbox(event_key, chat_id, body, review_device_id,
           review_revision, available_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (key, target, body, review, revision, now, now + ttl),
    )


async def announce_device(conn, config, device_id, reading, now):
    target = await configured_chat(conn, config, "discovery")
    if target is None:
        target = await configured_chat(conn, config, "admin")
    lines = [
        "Виявлено новий пристрій у мережі!", "",
        f"MAC: {reading.mac}", f"IP: {reading.ip or '—'}", f"Назва: {reading.name}",
    ]
    if reading.vendor.lower() not in {"", "unknown", "none", "null", "(unknown)", "(unknown: locally administered)"}:
        lines.append(f"Виробник: {reading.vendor}")
    lines.extend(("", "Хто це?"))
    await enqueue(conn, f"discovery:{device_id}", target, "\n".join(lines), now,
                  review=device_id, revision=0, ttl=86400)


async def announce_transition(conn, config, device, kind, event_id, absence, now):
    key = f"event:{event_id}"
    admin_chat = await configured_chat(conn, config, "admin")
    target = device["recipient_chat_id"]
    owner_id = device["owner_user_id"]
    owner = device["owner_name"] or device["name"]
    if device["category"] == "guest":
        if kind == "arrival":
            body = f"{device['guest_name']} прийшов у гості"
            await enqueue(conn, key, admin_chat, body, now)
            guest_chat = await configured_chat(conn, config, "guest")
            if guest_chat != admin_chat:
                await enqueue(conn, key, guest_chat, body, now)
        else:
            await enqueue(conn, key, admin_chat, f"{device['guest_name']} пішов", now)
        return

    if device["presence_alerts"]:
        state = "у мережі" if kind == "arrival" else "поза мережею"
        await enqueue(conn, key, admin_chat, f"{device['name']} · {state}", now)
    if (kind != "arrival" or not device["keys_enabled"]
            or owner_id not in (config.admins | config.members)
            or absence < config.rules.reminder_threshold):
        return
    if owner_id in config.admins:
        admin_recipient = target if target in (config.admins | config.group_ids) else admin_chat
        await enqueue(conn, key, admin_recipient, "З поверненням, перевір ключі", now)
    else:
        admin_text = f"{owner} повернулася додому" if owner.casefold() == "аня" else f"{owner} знову вдома"
        await enqueue(conn, key, admin_chat, admin_text, now)
        member_recipient = target if target in config.group_ids else owner_id
        await enqueue(conn, key, member_recipient, f"{owner}, не забудь перевірити ключі!", now)


async def claim_message(db, config, now):
    async with db.transaction() as conn:
        await conn.execute("UPDATE outbox SET status='expired' WHERE status='pending' AND expires_at<=?", (now,))
        while row := await one(
            conn, "SELECT * FROM outbox WHERE status='pending' AND available_at<=? ORDER BY id LIMIT 1", (now,)
        ):
            allowed = row["chat_id"] in config.recipients
            if allowed and row["review_device_id"] is not None:
                allowed = await one(
                    conn, "SELECT 1 FROM devices WHERE id=? AND category='unknown' AND revision=?",
                    (row["review_device_id"], row["review_revision"]),
                ) is not None
            status = "sending" if allowed else "cancelled"
            await conn.execute("UPDATE outbox SET status=?, attempts=attempts+1 WHERE id=?", (status, row["id"]))
            if allowed:
                return row
    return None


async def deliver_one(db, config, bot, review_keyboard, now=None):
    now = int(time.time()) if now is None else now
    row = await claim_message(db, config, now)
    if row is None:
        return False
    markup = None
    if row["review_device_id"] is not None:
        markup = review_keyboard(row["review_device_id"], row["review_revision"])
    status = "sent"
    available = now
    try:
        await bot.send_message(row["chat_id"], row["body"], reply_markup=markup)
    except TelegramRetryAfter as exc:
        # Telegram rejected this request before delivery. It is safe to retry.
        status = "pending"
        available = int(time.time()) + max(1, exc.retry_after)
    except (TelegramBadRequest, TelegramForbiddenError, TelegramMigrateToChat, TelegramUnauthorizedError) as exc:
        status = "failed"
        log.warning("Telegram відхилив повідомлення %s (%s)", row["id"], type(exc).__name__)
    except TelegramAPIError as exc:
        # The server may have accepted a timed-out request. Avoid duplicate reminders.
        status = "uncertain"
        log.warning("Невизначена доставка повідомлення %s (%s)", row["id"], type(exc).__name__)
    async with db.transaction() as conn:
        await conn.execute("UPDATE outbox SET status=?, available_at=? WHERE id=?", (status, available, row["id"]))
        if status == "pending":
            await conn.execute(
                "UPDATE outbox SET available_at=MAX(available_at, ?) WHERE status='pending' AND chat_id=?",
                (available, row["chat_id"]),
            )
    return True


async def run_outbox(db, config, bot, review_keyboard, heartbeat):
    while True:
        heartbeat()
        sent = await deliver_one(db, config, bot, review_keyboard)
        await asyncio.sleep(0.1 if sent else 2)
