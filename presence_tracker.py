import asyncio
import logging
import time
from dataclasses import dataclass

from config import POLL_SECONDS
from database import DEVICE_SELECT, RESET_CANDIDATES, add_device, all_rows, device_by_id, one
from notifications import announce_device, announce_transition
from tenda_client import DeviceReading, TendaError

log = logging.getLogger(__name__)


@dataclass
class TrackerStatus:
    last_poll: int | None = None
    observed_gap: int | None = None
    error: str = "Очікування першого запиту"


class PresenceTracker:
    def __init__(self, db, config):
        self.db = db
        self.config = config
        self.status = TrackerStatus()

    async def invalidate_candidates(self):
        async with self.db.transaction() as conn:
            await conn.execute(
                RESET_CANDIDATES + " WHERE negative_since IS NOT NULL OR positive_count<>0"
            )

    async def process(self, devices: dict[str, DeviceReading], now: int) -> bool:
        rules = self.config.rules
        async with self.db.transaction() as conn:
            previous = (await one(conn, "SELECT last_poll FROM poll_state WHERE singleton=1"))["last_poll"]
            if previous is not None and now <= previous:
                self.status.last_poll = previous
                self.status.error = "" if now == previous else "Системний час змінився"
                if now < previous:
                    await conn.execute(RESET_CANDIDATES)
                return False
            if previous is not None:
                self.status.observed_gap = now - previous
                if now - previous > rules.max_poll_gap:
                    await conn.execute(RESET_CANDIDATES)

            rows = await all_rows(conn, DEVICE_SELECT + " ORDER BY d.id")
            known = {row["mac"]: row for row in rows}
            announce_new = previous is not None or bool(rows) or self.config.announce_initial
            guests_before = {
                row["guest_id"] for row in rows
                if row["category"] == "guest" and row["monitor"] and row["status"] == "online"
            }
            changes = []
            for mac, reading in devices.items():
                row = known.get(mac)
                if row is None:
                    device_id = await add_device(conn, mac, reading.name, reading.vendor, reading.ip)
                    row = await device_by_id(conn, device_id)
                    if announce_new:
                        await announce_device(conn, self.config, device_id, reading, now)
                if row["first_alert_pending"] and row["category"] == "unknown" and not row["revision"]:
                    await announce_device(conn, self.config, row["id"], reading, now)
                await conn.execute(
                    """UPDATE devices SET ip=COALESCE(NULLIF(?, ''), ip),
                       vendor=COALESCE(NULLIF(?, ''), vendor), last_seen=?, first_alert_pending=0,
                       name=CASE WHEN category='unknown' AND revision=0 THEN ? ELSE name END
                       WHERE id=?""",
                    (reading.ip, reading.vendor, now, reading.name, row["id"]),
                )
                if row["monitor"]:
                    event = await self._advance(conn, row, True, now, reading.ip)
                    if event:
                        changes.append((row, *event))

            for row in rows:
                if row["monitor"] and row["mac"] not in devices:
                    event = await self._advance(conn, row, False, now, row["ip"])
                    if event:
                        changes.append((row, *event))

            guests_after = set()
            if any(row["category"] == "guest" for row, *_ in changes):
                guests_after = {
                    guest["guest_id"] for guest in await all_rows(conn, """
                        SELECT DISTINCT d.guest_id FROM devices d
                        JOIN notification_settings n ON n.device_id=d.id
                        WHERE d.category='guest' AND d.status='online' AND n.monitor=1""")
                }
            announced_guests = set()
            for row, kind, event_id, absence in changes:
                if row["category"] == "guest":
                    guest_id = row["guest_id"]
                    still_present = guests_before if kind == "arrival" else guests_after
                    if guest_id in still_present or guest_id in announced_guests:
                        continue
                    announced_guests.add(guest_id)
                await announce_transition(conn, self.config, row, kind, event_id, absence, now)
            await conn.execute("UPDATE poll_state SET last_poll=? WHERE singleton=1", (now,))

        self.status.last_poll = now
        self.status.error = ""
        return True

    async def _advance(self, conn, row, present, now, ip):
        rules = self.config.rules
        status = row["status"]
        negative_since = row["negative_since"]
        positive_since = row["positive_since"]
        positives = row["positive_count"]
        event = None
        if present:
            if status == "online":
                negative_since = positive_since = None
                positives = 0
            else:
                positive_since = positive_since if positive_since is not None else now
                positives += 1
                if positives >= rules.positive_checks:
                    if status == "offline":
                        departure = row["last_departure"]
                        if departure is None:
                            # Preserve the fallback for pre-existing offline rows without a departure.
                            departure = negative_since
                        if departure is not None:
                            event = ("arrival", positive_since, positive_since - departure)
                    status = "online"
                    negative_since = positive_since = None
                    positives = 0
        else:
            if negative_since is None or positives:
                negative_since = now
            positive_since = None
            positives = 0
            if status != "offline" and now - negative_since >= rules.offline_seconds:
                if status == "online":
                    event = ("departure", negative_since, 0)
                else:
                    # Establish the current absence when the first confirmed state is offline.
                    await conn.execute(
                        "UPDATE devices SET last_departure=? WHERE id=?", (negative_since, row["id"])
                    )
                status = "offline"

        await conn.execute(
            """UPDATE devices SET status=?, negative_since=?,
               positive_since=?, positive_count=? WHERE id=?""",
            (status, negative_since, positive_since, positives, row["id"]),
        )
        if event is None:
            return None
        kind, observed, absence = event
        label = row["guest_name"] or row["owner_name"] or row["name"]
        async with conn.execute(
            """INSERT OR IGNORE INTO device_events(device_id, mac, label, ip, kind,
               observed_at, confirmed_at) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (row["id"], row["mac"], label, ip or row["ip"], kind, observed, now),
        ) as cursor:
            if not cursor.rowcount:
                return None
            event_id = cursor.lastrowid
        column = "last_arrival" if kind == "arrival" else "last_departure"
        await conn.execute(f"UPDATE devices SET {column}=? WHERE id=?", (observed, row["id"]))
        return kind, event_id, absence

    async def run(self, client, heartbeat):
        last_error = ""
        while True:
            heartbeat()
            started = time.monotonic()
            try:
                snapshot = await client.snapshot()
                await self.process(snapshot, int(time.time()))
            except TendaError as exc:
                self.status.error = str(exc)
                await self.invalidate_candidates()
            error = self.status.error
            if error != last_error:
                if error:
                    log.warning("Tenda: %s", error)
                else:
                    log.info("Опитування Tenda відновилося")
                last_error = error
            await asyncio.sleep(max(0, POLL_SECONDS - (time.monotonic() - started)))
