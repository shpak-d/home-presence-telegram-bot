import asyncio
import base64
import hashlib
import json
import re
import time
from dataclasses import dataclass
from ipaddress import ip_address
from urllib.parse import urlsplit

import aiohttp
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from yarl import URL

from config import normalize_mac

AES_IV = b"EU5H62G9ICGRNI43"
ONLINE_PATH = "/goform/getOnlineList"
KEY_PATH = "/goform/stokCfg"
MAX_BODY_BYTES = 1024 * 1024


class TendaError(Exception):
    """Safe diagnostic without router response contents or credentials."""


class _SessionExpired(TendaError):
    def __init__(self):
        super().__init__("Сесія Tenda завершилася")


class _EndpointMissing(TendaError):
    def __init__(self):
        super().__init__("Tenda HTTP 404")


@dataclass(frozen=True)
class DeviceReading:
    mac: str
    name: str
    ip: str
    vendor: str = ""


def _login_location(value):
    if not isinstance(value, str):
        return False
    try:
        return urlsplit(value).path.lower().rsplit("/", 1)[-1] == "login.html"
    except ValueError:
        return False


def _check_payload(payload):
    if not isinstance(payload, dict):
        return
    if _login_location(payload.get("redirect")):
        raise _SessionExpired()
    if (any(key in payload and str(payload[key]) != "0" for key in ("errCode", "err_code"))
            or ("success" in payload and payload["success"] is not True)):
        raise TendaError("Tenda повернула помилку API")


def _flag(record, field, default):
    if field not in record:
        return default
    value = record[field]
    if type(value) in (int, str, bool) and str(value).lower() in ("0", "1", "false", "true"):
        return str(value).lower() in ("1", "true")
    raise ValueError


def parse_online_list(payload):
    _check_payload(payload)
    rows = payload.get("onlineList", payload.get("getOnlineList")) if isinstance(payload, dict) else payload
    guests = payload.get("guestList", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list) or not isinstance(guests, list):
        raise TendaError("Tenda не повернула повний список пристроїв")
    # The UI can include a leading record about the requester's host.
    if (rows and isinstance(rows[0], dict) and "localhostMac" in rows[0]
            and "deviceId" not in rows[0] and "mac" not in rows[0]):
        rows = rows[1:]
    devices = {}
    seen = set()
    try:
        for row in rows + guests:
            if not isinstance(row, dict):
                raise ValueError
            raw_mac = row.get("deviceId", row.get("mac"))
            if not isinstance(raw_mac, str):
                raise ValueError
            mac = normalize_mac(raw_mac)
            if mac in seen:
                raise ValueError
            seen.add(mac)
            online = _flag(row, "online", True)
            offline = _flag(row, "offline", False)
            blacklisted = _flag(row, "black", False) and not _flag(row, "isGuestClient", False)
            if online and not offline and not blacklisted:
                name = row.get("devName") or row.get("hostname") or row.get("hostName")
                name = name.strip()[:100] if isinstance(name, str) else ""
                vendor = row.get("vendor") or row.get("manufacturer")
                ip = row.get("ip")
                devices[mac] = DeviceReading(
                    mac,
                    name or "Unknown",
                    str(ip_address(ip)) if ip else "",
                    vendor.strip()[:100] if isinstance(vendor, str) else "",
                )
    except (TypeError, ValueError):
        raise TendaError("Tenda повернула некоректний запис пристрою") from None
    return devices


def _parse_json(body):
    try:
        return json.loads(body)
    except (UnicodeError, ValueError, RecursionError):
        raise TendaError("Tenda повернула некоректну відповідь") from None


class TendaClient:
    def __init__(self, session: aiohttp.ClientSession, url: str, password: str, timeout: int):
        self._session = session
        self._url = url.rstrip("/")
        self._password_hash = hashlib.md5(password.encode("utf-8"), usedforsecurity=False).hexdigest().upper()
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._lock = asyncio.Lock()
        self._key = None
        self._stok = None
        self._online_path = ONLINE_PATH

    def _has_cookie(self):
        return any(
            cookie.value for cookie in self._session.cookie_jar.filter_cookies(URL(self._url + KEY_PATH)).values()
        )

    def _clear_session(self):
        self._session.cookie_jar.clear()
        self._key = self._stok = None
        self._online_path = ONLINE_PATH

    async def _request(self, method, path, **kwargs):
        login = method == "POST"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self._url + ("/login.html" if login else "/main.html"),
            "Cache-Control": "no-cache",
        }
        if login:
            headers["Origin"] = self._url
        else:
            kwargs["params"] = {"rand": str(time.time_ns())}
        try:
            async with self._session.request(
                method, self._url + path, headers=headers, timeout=self._timeout,
                allow_redirects=False, **kwargs,
            ) as response:
                location = response.headers.get("Location")
                if not login and (
                    response.status in (401, 403)
                    or (300 <= response.status < 400 and _login_location(location))
                ):
                    raise _SessionExpired()
                if response.status == 404:
                    raise _EndpointMissing()
                if response.status != 200 and not (login and response.status in (204, 302, 303)):
                    raise TendaError(f"Tenda HTTP {response.status}")
                if login:
                    # Older web servers omit Path even though the cookie is for the goform API.
                    for name, cookie in response.cookies.items():
                        if not cookie["path"]:
                            cookie["path"] = "/"
                            self._session.cookie_jar.update_cookies({name: cookie}, response.url)
                try:
                    await response.content.readexactly(MAX_BODY_BYTES + 1)
                except asyncio.IncompleteReadError as exc:
                    body = exc.partial
                else:
                    raise TendaError("Відповідь Tenda завелика")
        except (aiohttp.ClientError, asyncio.TimeoutError):
            raise TendaError("Помилка з’єднання з Tenda") from None

        if not login:
            html = body.lstrip().lower()
            if html.startswith(b"<") and any(
                marker in html for marker in (b"login.html", b"/login/auth", b"login.js")
            ):
                raise _SessionExpired()
        return body

    async def _login(self):
        self._clear_session()
        await self._request(
            "POST", "/login/Auth", json={"userName": "admin", "password": self._password_hash}
        )
        if not self._has_cookie():
            raise TendaError("Tenda не видала cookie сесії. Перевірте TENDA_PASSWORD.")

    async def _fetch_key(self):
        payload = _parse_json(await self._request("GET", KEY_PATH))
        _check_payload(payload)
        config = payload.get("stokCfg", payload) if isinstance(payload, dict) else None
        sign = config.get("sign") if isinstance(config, dict) else None
        try:
            key = sign.encode("utf-8") if isinstance(sign, str) else b""
        except UnicodeEncodeError:
            key = b""
        if len(key) != 16:
            raise TendaError("Tenda не повернула 16-байтовий ключ сесії")
        stok = config.get("stok")
        if stok is not None and (not isinstance(stok, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", stok)):
            raise TendaError("Tenda повернула некоректний токен сесії")
        self._key = key
        self._stok = stok

    def _decrypt_list(self, body):
        body = body.strip()
        if body.startswith((b"{", b'"', b"[")):
            data = _parse_json(body)
            _check_payload(data)
            encrypted = data.get("data") if isinstance(data, dict) else data
        else:
            encrypted = body
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            cipher = AES.new(self._key, AES.MODE_CBC, AES_IV)
            plaintext = unpad(cipher.decrypt(ciphertext), AES.block_size, style="pkcs7")
        except (TypeError, ValueError):
            raise TendaError("Не вдалося розшифрувати список пристроїв Tenda") from None
        return parse_online_list(_parse_json(plaintext))

    async def _read_online_list(self, path):
        return self._decrypt_list(await self._request("GET", path))

    async def _read_devices(self):
        if self._key is None:
            await self._fetch_key()
        try:
            return await self._read_online_list(self._online_path)
        except (_EndpointMissing, _SessionExpired) as direct_error:
            if not self._stok or self._online_path != ONLINE_PATH:
                raise
            # Some Tenda web interfaces require the stok prefix in addition to the cookie.
            path = f"/;stok={self._stok}{ONLINE_PATH}"
            try:
                devices = await self._read_online_list(path)
            except _EndpointMissing:
                raise direct_error from None
            self._online_path = path
            return devices

    async def snapshot(self) -> dict[str, DeviceReading]:
        async with self._lock:
            if not self._has_cookie():
                await self._login()
            try:
                return await self._read_devices()
            except _SessionExpired:
                await self._login()
            try:
                return await self._read_devices()
            except _SessionExpired:
                self._clear_session()
                raise
