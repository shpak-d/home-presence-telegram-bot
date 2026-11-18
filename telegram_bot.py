import logging
import time
from datetime import datetime

from aiogram import BaseMiddleware, F, Router
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import (
    BotCommand, CallbackQuery, ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Message,
)

from config import chat_id, normalize_mac
from database import (
    DEVICE_SELECT, add_device, all_rows, assign_category, configured_chat, device_by_id,
    guest_by_name, one, rebind_mac, set_setting,
)

log = logging.getLogger(__name__)

COMMANDS = {
    "start": "Головне меню", "home": "Хто зараз удома", "devices": "Пристрої",
    "guests": "Гості", "unknown": "Невідомі пристрої", "history": "Історія",
    "settings": "Сповіщення", "status": "Стан системи", "help": "Довідка",
}
MENU_COMMANDS = list(COMMANDS)[1:]
CATEGORIES = {"resident": "Мій пристрій", "guest": "Гість", "ignored": "Ігнорувати", "unknown": "Невідомий"}
STATES = {"unknown": "очікує підтвердження", "online": "у мережі", "offline": "поза мережею"}
CHAT_ROLES = {"admin": "Адмін", "discovery": "Нові MAC", "guest": "Візити гостей"}
PAGE_SIZE = 8
PROMPT_TTL = 900


def keyboard(rows):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=title, callback_data=f"h:{action}") for title, action in row]
        for row in rows
    ])


def review_keyboard(device_id, revision, *, back=False):
    rows = [
        [(CATEGORIES[key], f"category:{device_id}:{revision}:{key}") for key in ("resident", "guest")],
        [(CATEGORIES[key], f"category:{device_id}:{revision}:{key}") for key in ("ignored", "unknown")],
    ]
    if back:
        rows.append([("До картки", f"device:{device_id}")])
    return keyboard(rows)


async def edit_menu(message, body, markup, *, menu_id=None):
    try:
        await message.bot.edit_message_text(
            chat_id=message.chat.id, message_id=menu_id or message.message_id, text=body, reply_markup=markup,
        )
    except TelegramBadRequest as exc:
        error = exc.message.lower()
        if "message is not modified" in error:
            return
        if "message to edit not found" not in error and "message can't be edited" not in error:
            raise
        await message.answer(body, reply_markup=markup)


def bot_commands():
    return [BotCommand(command=key, description=value) for key, value in COMMANDS.items()]


class AccessMiddleware(BaseMiddleware):
    def __init__(self, config):
        self.config = config

    async def __call__(self, handler, event, data):
        user = event.from_user
        message = event.message if isinstance(event, CallbackQuery) else event
        permitted = (
            user and not user.is_bot and isinstance(message, Message)
            and (user.id in self.config.admins or user.id in self.config.members)
            and (
                (message.chat.type == ChatType.PRIVATE and message.chat.id == user.id)
                or message.chat.id in self.config.group_ids
            )
        )
        if permitted:
            data["is_admin"] = user.id in self.config.admins
            return await handler(event, data)
        if isinstance(event, CallbackQuery):
            await event.answer("Доступ обмежено.", show_alert=True)
        return None


def device_view(device, is_admin, format_time):
    def toggle(field, title):
        state = "увімкнено" if device[field] else "вимкнено"
        return title + " · " + state, f"toggle:{device['id']}:{device['revision']}:{field}"

    owner = device["guest_name"] or device["owner_name"] or "—"
    status = STATES[device["status"]] if device["monitor"] else "моніторинг вимкнено"
    lines = [
        device["name"], device["mac"], f"Категорія: {CATEGORIES[device['category']]}",
        f"Власник: {owner}", f"IP: {device['ip'] or '—'}", f"Стан: {status}",
        f"Останнє виявлення: {format_time(device['last_seen'])}",
        f"Прихід: {format_time(device['last_arrival'])}",
        f"Вихід: {format_time(device['last_departure'])}",
    ]
    rows = []
    if is_admin:
        token = f"{device['id']}:{device['revision']}"
        rows.extend([
            [("Назва", f"edit:{token}:name"), ("MAC", f"edit:{token}:mac")],
            [("Категорія", f"review:{token}")],
        ])
        if device["category"] in {"resident", "guest"}:
            rows.append([toggle("monitor", "Моніторинг")])
        if device["category"] == "resident":
            lines.append(f"Chat ID нагадування: {device['recipient_chat_id'] or 'автоматично'}")
            rows.extend([
                [("Власник", f"edit:{token}:owner"), ("Chat ID", f"edit:{token}:recipient")],
                [toggle("keys_enabled", "Ключі")],
                [toggle("presence_alerts", "Повідомлення про прихід і вихід")],
            ])
    rows.append([("До списку", "page:devices:0"), ("Меню", "page:start:0")])
    return "\n".join(lines), keyboard(rows)


def build_router(db, config, tracker):
    router = Router()
    access = AccessMiddleware(config)
    router.message.outer_middleware(access)
    router.callback_query.outer_middleware(access)

    def date_text(timestamp):
        if timestamp is None:
            return "—"
        return datetime.fromtimestamp(timestamp, config.timezone).strftime("%d.%m.%Y %H:%M:%S %Z")

    def main_menu():
        return keyboard([[(COMMANDS[key], f"page:{key}:0") for key in MENU_COMMANDS[i:i + 2]]
                         for i in range(0, len(MENU_COMMANDS), 2)])

    async def view_page(page, index, is_admin):
        if page == "start":
            return "Home Presence", main_menu()
        if page == "help":
            return (
                "Команди\n" + "\n".join(f"/{command} · {label}" for command, label in COMMANDS.items())
                + "\n\nНалаштування пристроїв доступні адміністратору. "
                  "Для введення даних відповідайте на конкретний запит бота. /cancel скасовує відкриті запити.",
                main_menu(),
            )
        async with db.transaction() as conn:
            if page == "status":
                state = tracker.status
                poll = await one(conn, "SELECT last_poll FROM poll_state WHERE singleton=1")
                counts = await all_rows(conn, "SELECT status, COUNT(*) AS total FROM outbox GROUP BY status")
                statuses = {row["status"]: row["total"] for row in counts}
                return "\n".join([
                    "Бот відповідає",
                    f"Останній прийнятий список Tenda: {date_text(poll['last_poll'])}",
                    f"Інтервал опитування: {config.poll_seconds} с",
                    f"Останній виміряний інтервал: {state.observed_gap or '—'} с",
                    f"Стан: {state.error or 'дані актуальні'}",
                    f"Сповіщення: очікують {statuses.get('pending', 0)}, "
                    f"прострочені {statuses.get('expired', 0)}, відхилені {statuses.get('failed', 0)}, "
                    f"доставка невідома {statuses.get('uncertain', 0)}",
                ]), main_menu()
            if page == "settings":
                rules = config.rules
                lines = [
                    "Сповіщення", f"Підтвердження відсутності: {rules.offline_seconds} с",
                    f"Нагадування після відсутності від {rules.reminder_threshold} с",
                    "Моніторинг, нагадування та одержувач телефону задаються в його картці.",
                ]
                buttons = [[("Картки пристроїв", "page:devices:0")]]
                if is_admin:
                    buttons.append([("Дізнатися ID цього чату", "chatid")])
                    for role, label in CHAT_ROLES.items():
                        target = await configured_chat(conn, config, role)
                        lines.append(f"{label}: {target or 'за маршрутом адміністратора'}")
                        buttons.append([(f"Одержувач · {label}", f"chat:{role}")])
                buttons.append([("Меню", "page:start:0")])
                return "\n".join(lines), keyboard(buttons)

            limit = (PAGE_SIZE + 1, PAGE_SIZE * index)
            title = COMMANDS[page]
            buttons = []
            lines = [title]
            if page == "history":
                rows = await all_rows(
                    conn, "SELECT * FROM device_events ORDER BY confirmed_at DESC, id DESC LIMIT ? OFFSET ?", limit
                )
                for row in rows[:PAGE_SIZE]:
                    verb = "поява" if row["kind"] == "arrival" else "вихід"
                    lines.append(f"{date_text(row['observed_at'])} · {verb} · {row['label']} · {row['mac']}")
            elif page == "guests":
                rows = await all_rows(conn, """
                    SELECT g.id, g.name, COUNT(*) AS devices, SUM(d.status='online' AND n.monitor=1) AS online
                    FROM guests g JOIN devices d ON d.guest_id=g.id
                    LEFT JOIN notification_settings n ON n.device_id=d.id
                    GROUP BY g.id ORDER BY g.name_key, g.id LIMIT ? OFFSET ?""", limit)
                for row in rows[:PAGE_SIZE]:
                    state = "у мережі" if row["online"] else "поза мережею або ще не підтверджено"
                    lines.append(f"{row['name']} · {state} · MAC: {row['devices']}")
            else:
                filters = {
                    "devices": "d.category<>'unknown'",
                    "unknown": "d.category='unknown'",
                    "home": "d.status='online' AND n.monitor=1",
                }
                rows = await all_rows(
                    conn, DEVICE_SELECT + f" WHERE {filters[page]} ORDER BY d.name, d.id LIMIT ? OFFSET ?", limit
                )
                if page == "home":
                    state = tracker.status
                    if state.error or not state.last_poll:
                        lines.append("Показано останній підтверджений стан. Очікується свіжа відповідь Tenda.")
                for row in rows[:PAGE_SIZE]:
                    owner = row["guest_name"] or row["owner_name"]
                    label = f"{owner} · {row['name']}" if owner else row["name"]
                    buttons.append([(f"{label[:47]} · {row['mac']}", f"device:{row['id']}")])
                if page == "devices" and is_admin:
                    buttons.append([("Додати за MAC", "add")])
            if not rows:
                lines.append("Список порожній.")
            navigation = []
            if index:
                navigation.append(("‹", f"page:{page}:{index - 1}"))
            if len(rows) > PAGE_SIZE:
                navigation.append(("›", f"page:{page}:{index + 1}"))
            if navigation:
                buttons.append(navigation)
            buttons.append([("Меню", "page:start:0")])
            return "\n".join(lines), keyboard(buttons)

    @router.message(Command(*COMMANDS))
    async def on_command(message: Message, command, is_admin):
        if command.command == "start":
            async with db.transaction() as conn:
                await conn.execute(
                    """INSERT INTO bot_users(user_id, display_name, private_started) VALUES (?, ?, ?)
                       ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name,
                       private_started=MAX(bot_users.private_started, excluded.private_started)""",
                    (message.from_user.id, message.from_user.full_name, int(message.chat.type == ChatType.PRIVATE)),
                )
        body, markup = await view_page(command.command, 0, is_admin)
        await message.answer(body, reply_markup=markup)

    @router.message(Command("cancel"))
    async def cancel_prompt(message: Message):
        async with db.transaction() as conn:
            await conn.execute(
                "DELETE FROM pending_registrations WHERE user_id=? AND chat_id=?", (message.from_user.id, message.chat.id)
            )
        await message.answer("Запити введення в цьому чаті скасовано.")

    async def start_prompt(query, action, body, device_id=None, revision=None):
        prompt = await query.message.answer(f"{query.from_user.full_name}, {body}", reply_markup=ForceReply(force_reply=True))
        async with db.transaction() as conn:
            now = int(time.time())
            await conn.execute("DELETE FROM pending_registrations WHERE expires_at<=?", (now,))
            await conn.execute(
                """INSERT INTO pending_registrations(
                   user_id, chat_id, prompt_id, menu_message_id, action, device_id, revision, expires_at
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (query.from_user.id, prompt.chat.id, prompt.message_id, query.message.message_id,
                 action, device_id, revision, now + PROMPT_TTL),
            )

    @router.callback_query(F.data.startswith("h:"))
    async def on_callback(query: CallbackQuery, is_admin):
        action, *parts = query.data.removeprefix("h:").split(":")
        if action == "page" and len(parts) == 2 and parts[0] in COMMANDS and parts[1].isdigit():
            body, markup = await view_page(parts[0], min(int(parts[1]), 100000), is_admin)
            await query.answer()
            await edit_menu(query.message, body, markup)
            return
        if action == "device" and len(parts) == 1 and parts[0].isdigit():
            async with db.transaction() as conn:
                device = await device_by_id(conn, int(parts[0]))
            await query.answer()
            if device:
                body, markup = device_view(device, is_admin, date_text)
                await edit_menu(query.message, body, markup)
            return
        if not is_admin:
            await query.answer("Ця дія доступна адміністратору.", show_alert=True)
            return
        if action == "chatid":
            await query.answer()
            await edit_menu(
                query.message, f"Chat ID: {query.message.chat.id}\nВаш user ID: {query.from_user.id}",
                keyboard([[("До налаштувань", "page:settings:0")]]),
            )
            return
        if action == "chat" and len(parts) == 1 and parts[0] in CHAT_ROLES:
            await query.answer()
            await start_prompt(query, f"chat_{parts[0]}", "введіть дозволений chat ID або 0 для маршруту з .env.")
            return
        if action == "add":
            await query.answer()
            await start_prompt(query, "add", "введіть MAC, наприклад AA:BB:CC:DD:EE:FF.")
            return
        if len(parts) < 2 or not all(value.isdigit() for value in parts[:2]):
            await query.answer("Невідома дія.")
            return
        device_id, revision = map(int, parts[:2])
        option = parts[2] if len(parts) == 3 else ""
        error = ""
        prompt = None
        async with db.transaction() as conn:
            device = await device_by_id(conn, device_id)
            if not device or device["revision"] != revision:
                error = "Картка змінилася. Відкрийте її з актуального списку."
            elif action == "category" and option in CATEGORIES:
                if option in {"resident", "guest"}:
                    label = "введіть назву пристрою." if option == "resident" else "введіть ім’я гостя."
                    prompt = option, label
                else:
                    await assign_category(conn, device_id, option)
            elif action == "edit":
                fields = {
                    "name": "введіть нову назву пристрою.",
                    "mac": "введіть нову MAC-адресу.",
                    "owner": "введіть ім’я та Telegram user ID у форматі Аня | 123456789. Для скидання введіть 0.",
                    "recipient": "введіть дозволений chat ID або 0 для автоматичного маршруту.",
                }
                if option not in fields or (option in {"owner", "recipient"} and device["category"] != "resident"):
                    error = "Поле недоступне."
                else:
                    prompt = option, fields[option]
            elif action == "toggle":
                valid = {"monitor"} if device["category"] == "guest" else {"monitor", "keys_enabled", "presence_alerts"}
                if device["category"] not in {"resident", "guest"} or option not in valid:
                    error = "Налаштування недоступне."
                elif option == "keys_enabled" and not device[option] and device["owner_user_id"] not in (config.admins | config.members):
                    error = "Спочатку вкажіть дозволений user ID власника."
                else:
                    await conn.execute(
                        f"UPDATE notification_settings SET {option}=1-{option} WHERE device_id=?", (device_id,)
                    )
                    await conn.execute("UPDATE devices SET revision=revision+1 WHERE id=?", (device_id,))
                    if option == "monitor":
                        await conn.execute(
                            """UPDATE devices SET status='unknown', negative_since=NULL, positive_count=0,
                               positive_since=NULL WHERE id=?""", (device_id,)
                        )
            elif action != "review":
                error = "Невідома дія."
            if not error and not prompt:
                device = await device_by_id(conn, device_id)
        if error:
            await query.answer(error, show_alert=True)
            return
        await query.answer()
        if prompt:
            await start_prompt(query, *prompt, device_id, revision)
        elif action == "review":
            await edit_menu(query.message, "Оберіть категорію.", review_keyboard(device_id, revision, back=True))
        else:
            body, markup = device_view(device, is_admin, date_text)
            await edit_menu(query.message, body, markup)

    def label(text):
        clean = " ".join(text.split())
        if not 1 <= len(clean) <= 80:
            raise ValueError("Введіть текст довжиною від 1 до 80 символів.")
        return clean

    def target_chat(text):
        if text == "0":
            return None
        value = chat_id(text)
        if value not in config.recipients:
            raise ValueError("Цього chat ID немає у списках дозволених у .env.")
        return value

    async def apply_prompt(conn, pending, device, text):
        action = pending["action"]
        if action.startswith("chat_"):
            role = action.removeprefix("chat_")
            target = target_chat(text)
            if role == "admin" and target in config.members:
                raise ValueError("Вкажіть приватний chat ID адміністратора або дозволену групу.")
            if target is None:
                await conn.execute("DELETE FROM bot_settings WHERE key=?", (role,))
            else:
                await set_setting(conn, role, target)
            return None
        if action == "add":
            mac = normalize_mac(text)
            row = await one(conn, "SELECT id FROM devices WHERE mac=?", (mac,))
            return row["id"] if row else await add_device(conn, mac, "Unknown")
        device_id = device["id"]
        if action == "resident":
            await assign_category(conn, device_id, "resident", name=label(text))
        elif action == "guest":
            guest = await guest_by_name(conn, label(text))
            await assign_category(conn, device_id, "guest", guest_id=guest["id"])
        elif action == "mac":
            mac = normalize_mac(text)
            await rebind_mac(conn, device, mac)
        elif action == "name":
            await conn.execute("UPDATE devices SET name=?, revision=revision+1 WHERE id=?", (label(text), device_id))
        elif action == "owner":
            owner, separator, raw_id = text.partition("|")
            if text == "0":
                owner_name, owner_id = "", None
                await conn.execute("UPDATE notification_settings SET keys_enabled=0 WHERE device_id=?", (device_id,))
            else:
                if not separator:
                    raise ValueError("Введіть ім’я та ID у форматі Аня | 123456789.")
                owner_name, owner_id = label(owner), chat_id(raw_id)
                if owner_id not in (config.admins | config.members):
                    raise ValueError("Додайте user ID власника до дозволених у .env.")
            if owner_id != device["owner_user_id"]:
                await conn.execute(
                    "UPDATE notification_settings SET recipient_chat_id=NULL WHERE device_id=?", (device_id,)
                )
            await conn.execute(
                "UPDATE devices SET owner_name=?, owner_user_id=?, revision=revision+1 WHERE id=?",
                (owner_name, owner_id, device_id),
            )
        elif action == "recipient":
            target = target_chat(text)
            owner_id = device["owner_user_id"]
            allowed_private = config.admins if owner_id in config.admins else {owner_id}
            if target is not None and target not in (allowed_private | config.group_ids):
                raise ValueError("Вкажіть chat ID власника, адміністратора для адмінського телефона або дозволеної групи.")
            await conn.execute(
                "UPDATE notification_settings SET recipient_chat_id=? WHERE device_id=?",
                (target, device_id),
            )
            await conn.execute("UPDATE devices SET revision=revision+1 WHERE id=?", (device_id,))
        return device_id

    @router.message(F.text, F.reply_to_message)
    async def on_reply(message: Message, is_admin, bot):
        if not is_admin or not message.reply_to_message.from_user or message.reply_to_message.from_user.id != bot.id:
            return
        address = (message.from_user.id, message.chat.id, message.reply_to_message.message_id)
        keep_prompt = False
        menu_id = None
        refresh_settings = False
        async with db.transaction() as conn:
            pending = await one(
                conn, "SELECT * FROM pending_registrations WHERE user_id=? AND chat_id=? AND prompt_id=?", address
            )
            if pending is None:
                return
            device_id = pending["device_id"]
            device = await device_by_id(conn, device_id) if device_id is not None else None
            if pending["expires_at"] <= int(time.time()):
                response = "Час введення минув. Відкрийте дію знову.", None
            elif device_id is not None and (device is None or device["revision"] != pending["revision"]):
                response = "Картка змінилася. Відкрийте дію знову.", None
            else:
                try:
                    device_id = await apply_prompt(conn, pending, device, message.text.strip())
                except ValueError as exc:
                    keep_prompt = True
                    response = f"{exc} Дайте нову відповідь на початковий запит.", None
                else:
                    menu_id = pending["menu_message_id"]
                    if device_id is None:
                        response = "Одержувача збережено.", main_menu()
                        refresh_settings = menu_id is not None
                    else:
                        device = await device_by_id(conn, device_id)
                        response = device_view(device, True, date_text)
            if not keep_prompt:
                await conn.execute("DELETE FROM pending_registrations WHERE user_id=? AND chat_id=? AND prompt_id=?", address)
        if refresh_settings:
            response = await view_page("settings", 0, is_admin)
        if menu_id is not None:
            await edit_menu(message, *response, menu_id=menu_id)
        else:
            await message.answer(response[0], reply_markup=response[1])

    @router.errors()
    async def report_error(event):
        log.warning("Помилка обробки Telegram-оновлення (%s)", type(event.exception).__name__)

    return router
