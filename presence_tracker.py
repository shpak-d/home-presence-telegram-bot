import asyncio
import logging
import time
from dataclasses import dataclass

from database import DEVICE_SELECT, RESET_CANDIDATES, add_device, all_rows, device_by_id, one
from netalertx_client import NetAlertXError, Snapshot
from notifications import announce_device, announce_transition

log = logging.getLogger(__name__)


@dataclass
class TrackerStatus:
    api_ok_at: int | None = None
    last_scan: int | None = None
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

    async def process(self, snapshot: Snapshot, now: int) -> bool:
        self.status.api_ok_at = now
        scan = snapshot.scan_time
        rules = self.config.rules
        if scan is None:
            self.status.error = "Контрольна MAC відсутня, вимкнена або поза мережею"
            await self.invalidate_candidates()
            return False
        if scan > now + 10 or now - scan > rules.max_scan_gap:
            self.status.error = "Час контрольного запису застарів або годинники не синхронізовані"
            await self.invalidate_candidates()
            return False

        async with self.db.transaction() as conn:
            previous = (await one(conn, "SELECT last_scan FROM scan_state WHERE singleton=1"))["last_scan"]
            if previous is not None and scan <= previous:
                self.status.last_scan = previous
                self.status.error = "" if scan == previous else "API повернув старіший результат"
                if scan < previous:
                    await conn.execute(RESET_CANDIDATES)
                return False
            if previous is not None:
                self.status.observed_gap = scan - previous
                if scan - previous > rules.max_scan_gap:
                    await conn.execute(RESET_CANDIDATES)

            rows = await all_rows(conn, DEVICE_SELECT + " ORDER BY d.id")
            known = {row["mac"]: row for row in rows}
            guests_before = {
                row["guest_id"] for row in rows
                if row["category"] == "guest" and row["monitor"] and row["status"] == "online"
            }
            changes = []
            for mac, reading in snapshot.devices.items():
                if mac == self.config.reference_mac or not reading.eligible:
                    continue
                row = known.get(mac)
                if row is None:
                    device_id = await add_device(conn, mac, reading.name, reading.vendor, reading.ip)
                    if reading.last_seen is not None and reading.last_seen <= scan:
                        await conn.execute("UPDATE devices SET last_seen=? WHERE id=?", (reading.last_seen, device_id))
                    row = await device_by_id(conn, device_id)
                    if previous is not None or self.config.announce_initial:
                        if reading.observed_at(scan) is True:
                            await announce_device(conn, self.config, device_id, reading, now)
                        elif previous is not None:
                            # An inventory row may precede its first usable positive reading.
                            await conn.execute(
                                "UPDATE devices SET first_alert_pending=1 WHERE id=?", (device_id,)
                            )
                observation = reading.observed_at(scan)
                if observation is True:
                    if row["first_alert_pending"] and row["category"] == "unknown" and not row["revision"]:
                        await announce_device(conn, self.config, row["id"], reading, now)
                    await conn.execute(
                        """UPDATE devices SET ip=COALESCE(NULLIF(?, ''), ip),
                           vendor=COALESCE(NULLIF(?, ''), vendor), last_seen=?, first_alert_pending=0,
                           name=CASE WHEN category='unknown' AND revision=0 THEN ? ELSE name END
                           WHERE id=?""",
                        (reading.ip, reading.vendor, scan, reading.name, row["id"]),
                    )
                if row["monitor"]:
                    event = await self._advance(conn, row, observation, scan, reading.ip)
                    if event:
                        changes.append((row, *event))

            for row in rows:
                reading = snapshot.devices.get(row["mac"])
                if row["monitor"] and (reading is None or not reading.eligible):
                    await self._advance(conn, row, None, scan, row["ip"])

            for row, kind, event_id, absence in changes:
                if row["category"] == "guest" and kind == "arrival":
                    if row["guest_id"] in guests_before:
                        continue
                    guests_before.add(row["guest_id"])
                await announce_transition(conn, self.config, row, kind, event_id, absence, now)
            await conn.execute("UPDATE scan_state SET last_scan=? WHERE singleton=1", (scan,))

        self.status.last_scan = scan
        self.status.error = ""
        return True

    async def _advance(self, conn, row, observation, scan, ip):
        rules = self.config.rules
        status = row["status"]
        negative_since = row["negative_since"]
        positive_since = row["positive_since"]
        positives = row["positive_count"]
        event = None
        if observation is None:
            negative_since = positive_since = None
            positives = 0
        elif observation:
            if status == "online":
                negative_since = positive_since = None
                positives = 0
            else:
                positive_since = positive_since if positive_since is not None else scan
                positives += 1
                if positives >= rules.positive_scans:
                    if status == "offline" and negative_since is not None:
                        absence = positive_since - negative_since
                        event = ("arrival", positive_since, absence)
                    status = "online"
                    negative_since = positive_since = None
                    positives = 0
        else:
            # A single positive while offline ends the observed absence, even
            # if it never becomes a confirmed arrival.
            if negative_since is None or positives:
                negative_since = scan
            positive_since = None
            positives = 0
            if status != "offline" and scan - negative_since >= rules.offline_seconds:
                if status == "online":
                    event = ("departure", negative_since, 0)
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
            (row["id"], row["mac"], label, ip or row["ip"], kind, observed, scan),
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
            except NetAlertXError as exc:
                self.status.error = str(exc)
                if self.status.error != last_error:
                    await self.invalidate_candidates()
            error = self.status.error
            if error != last_error:
                if error:
                    log.warning("NetAlertX: %s", error)
                else:
                    log.info("Сканування NetAlertX відновилося")
                last_error = error
            await asyncio.sleep(max(0, self.config.poll_seconds - (time.monotonic() - started)))
