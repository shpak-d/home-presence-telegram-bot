import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

MAC_PATTERN = re.compile(r"[0-9a-f]{2}(?::[0-9a-f]{2}){5}", re.IGNORECASE | re.ASCII)


def normalize_mac(value: str) -> str:
    mac = value.strip().upper().replace("-", ":")
    if (
        not MAC_PATTERN.fullmatch(mac)
        or int(mac[:2], 16) & 1
        or mac == "00:00:00:00:00:00"
    ):
        raise ValueError("Введіть коректну unicast MAC-адресу.")
    return mac


def chat_id(value: str) -> int:
    if not re.fullmatch(r"-?[0-9]{1,16}", value.strip()):
        raise ValueError("Введіть числовий Telegram chat ID.")
    number = int(value)
    if number == 0 or abs(number) >= 2**52:
        raise ValueError("Введіть коректний Telegram chat ID.")
    return number


@dataclass(frozen=True)
class PresenceRules:
    positive_scans: int = 2
    offline_seconds: int = 300
    reminder_absence_seconds: int = 600
    reminder_quiet_seconds: int = 1200
    max_scan_gap: int = 105

    @property
    def reminder_threshold(self) -> int:
        return max(self.reminder_absence_seconds, self.reminder_quiet_seconds)


@dataclass(frozen=True, repr=False)
class Config:
    telegram_token: str
    api_token: str
    api_url: str
    reference_mac: str
    admins: frozenset[int]
    members: frozenset[int]
    group_ids: frozenset[int]
    default_chats: dict[str, int | None]
    database_path: Path
    timezone: ZoneInfo
    poll_seconds: int
    scan_seconds: int
    api_timeout: int
    rules: PresenceRules
    announce_initial: bool

    @classmethod
    def from_env(cls) -> "Config":
        load_dotenv()

        def required(key: str) -> str:
            value = os.getenv(key, "").strip()
            if not value:
                raise ValueError(f"Заповніть {key} у .env.")
            return value

        def number(key: str, default: int, minimum: int, maximum: int) -> int:
            value = os.getenv(key) or str(default)
            if not re.fullmatch(r"[0-9]+", value) or not minimum <= int(value) <= maximum:
                raise ValueError(f"{key} має бути цілим числом від {minimum} до {maximum}.")
            return int(value)

        def ids(key: str, *, groups: bool = False) -> frozenset[int]:
            values = frozenset(chat_id(item) for item in os.getenv(key, "").split(",") if item.strip())
            if any((value < 0) != groups for value in values):
                raise ValueError(f"Перевірте знак Telegram ID у {key}.")
            return values

        admins = ids("ADMIN_USER_IDS")
        if not admins:
            raise ValueError("Заповніть ADMIN_USER_IDS.")
        members = ids("MEMBER_USER_IDS") - admins
        groups = ids("ALLOWED_GROUP_IDS")
        chats = {}
        for role in ("admin", "discovery", "guest"):
            raw = os.getenv(f"{role.upper()}_CHAT_ID", "").strip()
            target = chat_id(raw) if raw else None
            allowed = admins | groups if role == "admin" else admins | members | groups
            if target is not None and target not in allowed:
                raise ValueError(f"{role.upper()}_CHAT_ID має бути в списку дозволених ID.")
            chats[role] = target
        chats["admin"] = chats["admin"] or min(admins)

        url_value = required("NETALERTX_URL")
        try:
            url = urlsplit(url_value)
            port = url.port
        except ValueError:
            raise ValueError("Перевірте хост і порт NETALERTX_URL.") from None
        if (
            url.scheme not in {"http", "https"} or not url.hostname
            or url.username or url.password or url.query or url.fragment
            or url.path.rstrip("/") or any(char.isspace() for char in url_value)
        ):
            raise ValueError("NETALERTX_URL має містити схему, хост і порт API.")
        if port == 0:
            raise ValueError("NETALERTX_URL містить некоректний порт.")
        token = required("TELEGRAM_BOT_TOKEN")
        if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
            raise ValueError("Перевірте формат TELEGRAM_BOT_TOKEN.")
        announce = os.getenv("ANNOUNCE_INITIAL_DEVICES", "false").lower()
        if announce not in {"true", "false"}:
            raise ValueError("ANNOUNCE_INITIAL_DEVICES має бути true або false.")

        poll = number("POLL_SECONDS", 30, 5, 300)
        cadence = number("SCAN_SECONDS", 60, 60, 3600)
        if poll > cadence / 2:
            raise ValueError("POLL_SECONDS має бути щонайбільше половиною SCAN_SECONDS.")
        default_gap = cadence + min(45, cadence // 2 + 15)
        rules = PresenceRules(
            positive_scans=number("POSITIVE_SCANS", 2, 2, 10),
            offline_seconds=number("OFFLINE_SECONDS", 300, 60, 86400),
            reminder_absence_seconds=number("REMINDER_ABSENCE_SECONDS", 600, 0, 86400),
            reminder_quiet_seconds=number("REMINDER_QUIET_SECONDS", 1200, 0, 86400),
            max_scan_gap=number("MAX_SCAN_GAP_SECONDS", default_gap, cadence, 7200),
        )
        return cls(
            telegram_token=token,
            api_token=required("API_TOKEN"),
            api_url=url.geturl().rstrip("/"),
            reference_mac=normalize_mac(required("REFERENCE_MAC")),
            admins=admins,
            members=members,
            group_ids=groups,
            default_chats=chats,
            database_path=Path(os.getenv("DATABASE_PATH", "data/presence.sqlite3")),
            timezone=ZoneInfo("Europe/Kyiv"),
            poll_seconds=poll,
            scan_seconds=cadence,
            api_timeout=number("API_TIMEOUT_SECONDS", 10, 2, poll),
            rules=rules,
            announce_initial=announce == "true",
        )

    @property
    def recipients(self) -> frozenset[int]:
        return self.admins | self.members | self.group_ids
