import os
from unittest import TestCase
from unittest.mock import patch

from config import Config


class ConfigTests(TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {
            "TELEGRAM_BOT_TOKEN": "12345:testing",
            "TENDA_URL": "http://192.168.0.1",
            "TENDA_PASSWORD": "test",
            "ADMIN_USER_IDS": "11001",
        }, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def test_poll_defaults_preserve_presence_thresholds(self):
        config = Config.from_env()
        self.assertEqual(config.poll_seconds, 60)
        self.assertEqual(config.rules.max_poll_gap, 120)
        self.assertEqual(config.rules.positive_checks, 2)
        self.assertEqual(config.rules.offline_seconds, 300)

    def test_poll_interval_and_gap_follow_environment(self):
        for value in (30, 60, 120, 300):
            with self.subTest(value=value), patch.dict(os.environ, {"POLL_SECONDS": str(value)}):
                config = Config.from_env()
                self.assertEqual(config.poll_seconds, value)
                self.assertEqual(config.rules.max_poll_gap, 2 * value)

    def test_empty_poll_interval_uses_default(self):
        with patch.dict(os.environ, {"POLL_SECONDS": ""}):
            self.assertEqual(Config.from_env().poll_seconds, 60)

    def test_invalid_poll_intervals_are_rejected(self):
        for value in ("0", "29", "301", "-60", "60.0", "abc", " 60 "):
            with self.subTest(value=value), patch.dict(os.environ, {"POLL_SECONDS": value}):
                with self.assertRaisesRegex(ValueError, "POLL_SECONDS.*30.*300"):
                    Config.from_env()

    def test_api_timeout_cannot_exceed_poll_interval(self):
        with patch.dict(os.environ, {"POLL_SECONDS": "30", "API_TIMEOUT_SECONDS": "31"}):
            with self.assertRaisesRegex(ValueError, "API_TIMEOUT_SECONDS"):
                Config.from_env()
        with patch.dict(os.environ, {"POLL_SECONDS": "120", "API_TIMEOUT_SECONDS": "90"}):
            self.assertEqual(Config.from_env().api_timeout, 90)
