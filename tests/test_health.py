import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import AsyncMock, patch

import main


class HealthSupervisorTests(IsolatedAsyncioTestCase):
    async def test_heartbeat_stays_fresh_between_configured_polls(self):
        for interval in (30, 60, 300):
            for scan_age in (interval, interval + 29, interval + 31):
                with self.subTest(poll_seconds=interval, scan_age=scan_age):
                    ticks = {"scan": 1000 - scan_age, "outbox": 1000, "telegram": 1000}
                    with (
                        patch("main.time.monotonic", return_value=1000),
                        patch("main.HEALTH_FILE") as health_file,
                        patch("main.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)),
                    ):
                        with self.assertRaises(asyncio.CancelledError):
                            await main.supervise_health(ticks, interval)
                    self.assertEqual(health_file.touch.call_count, int(scan_age < interval + 30))

    async def test_router_heartbeat_does_not_hide_stalled_telegram_or_outbox(self):
        for stalled in ("telegram", "outbox"):
            with self.subTest(stalled=stalled):
                ticks = {"scan": 1000, "outbox": 1000, "telegram": 1000}
                ticks[stalled] -= 61
                with (
                    patch("main.time.monotonic", return_value=1000),
                    patch("main.HEALTH_FILE") as health_file,
                    patch("main.asyncio.sleep", new=AsyncMock(side_effect=asyncio.CancelledError)),
                ):
                    with self.assertRaises(asyncio.CancelledError):
                        await main.supervise_health(ticks, 300)
                health_file.touch.assert_not_called()


class HealthcheckTests(TestCase):
    def test_healthcheck_requires_a_recent_supervisor_heartbeat(self):
        with TemporaryDirectory() as folder, patch("main.HEALTH_FILE", Path(folder) / "alive"):
            self.assertEqual(main.healthcheck(), 1)
            main.HEALTH_FILE.touch()
            timestamp = main.HEALTH_FILE.stat().st_mtime
            for age, expected in ((0, 0), (119, 0), (120, 1), (-1, 1)):
                with self.subTest(age=age), patch("main.time.time", return_value=timestamp + age):
                    self.assertEqual(main.healthcheck(), expected)
