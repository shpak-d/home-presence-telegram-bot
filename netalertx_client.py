import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from ipaddress import ip_address

import aiohttp

from config import normalize_mac


class NetAlertXError(Exception):
    """A safe diagnostic that never contains the API response or credentials."""


@dataclass(frozen=True)
class DeviceReading:
    mac: str
    ip: str
    name: str
    vendor: str
    present: bool
    last_seen: int | None
    eligible: bool = True

    def observed_at(self, scan_time: int) -> bool | None:
        if not self.eligible or self.last_seen is None:
            return None
        if self.present:
            return True if self.last_seen == scan_time else None
        return False if self.last_seen < scan_time else None


@dataclass(frozen=True)
class Snapshot:
    scan_time: int | None
    devices: dict[str, DeviceReading]


def parse_timestamp(value: str | None) -> int | None:
    if value in (None, "", "null"):
        return None
    instant = datetime.fromisoformat(value)
    # The REST /devices route returns raw Devices rows. Naive DB dates are UTC.
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return int(instant.timestamp())


def parse_ip(value) -> str:
    try:
        return str(ip_address(value)) if isinstance(value, str) else ""
    except ValueError:
        return ""


def parse_snapshot(payload: object, reference_mac: str) -> Snapshot:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise NetAlertXError("API не підтвердив успішне читання пристроїв")
    rows = payload.get("devices")
    if not isinstance(rows, list) or not rows:
        raise NetAlertXError("API повернув порожній або неповний список пристроїв")

    devices = {}
    try:
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("devMac"), str):
                raise ValueError
            try:
                mac = normalize_mac(row["devMac"])
            except ValueError:
                # The upstream inventory may include Internet, synthetic, and broadcast IDs.
                continue
            flag = row["devPresentLastScan"]
            if type(flag) is not int or flag not in (0, 1) or mac in devices:
                raise ValueError
            last_seen = parse_timestamp(row["devLastConnection"])
            ip = parse_ip(row.get("devLastIP"))
            eligible = (
                row["devIsArchived"] == 0
                and row["devScan"] == 1
                and row.get("devForceStatus") in (None, "", "dont_force")
                and str(row.get("devParentRelType", "")).lower() != "nic"
            )
            devices[mac] = DeviceReading(
                mac=mac,
                ip=ip,
                name=str(row.get("devName") or "Unknown")[:100],
                vendor=str(row.get("devVendor") or "")[:100],
                present=bool(flag),
                last_seen=last_seen,
                eligible=eligible,
            )
    except (KeyError, ValueError, TypeError, OverflowError, OSError):
        raise NetAlertXError("API повернув неповні або некоректні поля пристроїв") from None

    reference = devices.get(reference_mac)
    scan_time = (
        reference.last_seen
        if reference and reference.eligible and reference.present and reference.ip else None
    )
    return Snapshot(scan_time, devices)


class NetAlertXClient:
    def __init__(self, session: aiohttp.ClientSession, url: str, token: str, reference: str, timeout: int):
        self.session = session
        self.url = url
        self.headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        self.reference = reference
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def _get(self, endpoint: str) -> object:
        try:
            async with self.session.get(
                self.url + endpoint, headers=self.headers, timeout=self.timeout, allow_redirects=False
            ) as response:
                if response.status != 200:
                    raise NetAlertXError(f"NetAlertX HTTP {response.status}")
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
            raise NetAlertXError("Помилка з’єднання або формату відповіді NetAlertX") from None

    async def snapshot(self) -> Snapshot:
        # No limit/offset: this route reads one SQLite snapshot instead of multiple pages.
        return parse_snapshot(await self._get("/devices"), self.reference)
