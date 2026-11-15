import asyncio
import base64
import hashlib
import json
from unittest import IsolatedAsyncioTestCase, TestCase

from aiohttp import ClientSession, CookieJar, web
from aiohttp.test_utils import TestServer
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad

from tenda_client import TendaClient, TendaError, parse_online_list

MAC = "AA:00:00:00:00:02"
PASSWORD = " test router password "
KEY = b"0123456789abcdef"
DEVICE = {"deviceId": MAC.lower(), "devName": "iPhone", "ip": "192.168.0.42"}


def encrypted(payload):
    cipher = AES.new(KEY, AES.MODE_CBC, b"EU5H62G9ICGRNI43")
    return base64.b64encode(cipher.encrypt(pad(json.dumps(payload).encode(), 16))).decode()


class TendaProtocolTests(IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.logins = 0
        self.key_requests = 0
        self.online_paths = []
        self.expired = False
        self.use_stok = False
        self.key_error = False
        self.login_reply = lambda: web.json_response({"errCode": 0})
        self.online_reply = lambda: web.json_response({"data": encrypted([DEVICE])})
        app = web.Application()
        app.router.add_post("/login/Auth", self.login)
        app.router.add_get("/goform/stokCfg", self.key)
        app.router.add_get("/goform/getOnlineList", self.online)
        app.router.add_get("/;stok=abcdef/goform/getOnlineList", self.online)
        server = TestServer(app)
        await server.start_server()
        self.addAsyncCleanup(server.close)
        session = ClientSession(cookie_jar=CookieJar(unsafe=True), trust_env=False)
        self.addAsyncCleanup(session.close)
        self.client = TendaClient(session, str(server.make_url("/")), PASSWORD, 2)

    async def login(self, request):
        expected_hash = hashlib.md5(PASSWORD.encode(), usedforsecurity=False).hexdigest()
        self.assertEqual(request.content_type, "application/x-www-form-urlencoded")
        self.assertEqual(dict(await request.post()), {"username": "admin", "password": expected_hash})
        self.assertFalse(request.cookies)
        self.logins += 1
        self.expired = False
        response = self.login_reply()
        response.set_cookie("password", f"login-{self.logins}", path="")
        return response

    async def key(self, request):
        self.key_requests += 1
        self.assertEqual(request.cookies.get("password"), f"login-{self.logins}")
        if self.key_error:
            raise web.HTTPServiceUnavailable()
        payload = {"sign": KEY.decode()}
        if self.use_stok:
            payload["stok"] = "abcdef"
        return web.json_response({"stokCfg": payload})

    async def online(self, request):
        self.online_paths.append(request.path)
        self.assertEqual(request.cookies.get("password"), f"login-{self.logins}")
        if self.expired:
            raise web.HTTPFound("/login.html")
        if self.use_stok and request.path == "/goform/getOnlineList":
            raise web.HTTPNotFound()
        return self.online_reply()

    async def test_cookie_key_and_decrypted_list_are_reused(self):
        first = await self.client.snapshot()
        self.assertEqual((first[MAC].name, first[MAC].ip), ("iPhone", "192.168.0.42"))
        self.online_reply = lambda: web.Response(text=encrypted([]))
        self.assertEqual(await self.client.snapshot(), {})
        self.assertEqual((self.logins, self.key_requests), (1, 1))

    async def test_form_login_accepts_302_cookie_and_fetches_sign(self):
        self.login_reply = lambda: web.Response(status=302, headers={"Location": "/main.html"})
        devices = await self.client.snapshot()
        self.assertEqual(devices[MAC].name, "iPhone")
        self.assertEqual((self.logins, self.key_requests, len(self.online_paths)), (1, 1, 1))

    async def test_login_redirect_refreshes_session_and_retries_list_once(self):
        await self.client.snapshot()
        self.expired = True
        self.assertIn(MAC, await self.client.snapshot())
        self.assertEqual((self.logins, self.key_requests, len(self.online_paths)), (2, 2, 3))

    async def test_concurrent_polls_share_one_login(self):
        first, second = await asyncio.gather(self.client.snapshot(), self.client.snapshot())
        self.assertEqual(first, second)
        self.assertEqual((self.logins, self.key_requests), (1, 1))

    async def test_login_html_and_unauthorized_status_refresh_session(self):
        await self.client.snapshot()
        for reply in (
            lambda: web.Response(text="<html><script src='login.js'></script></html>"),
            lambda: web.Response(status=401),
        ):
            original = self.online_reply

            def reply_once():
                self.online_reply = original
                return reply()

            self.online_reply = reply_once
            self.assertIn(MAC, await self.client.snapshot())
        self.assertEqual((self.logins, self.key_requests), (3, 3))

    async def test_second_session_rejection_does_not_loop(self):
        self.online_reply = lambda: web.Response(status=403)
        with self.assertRaisesRegex(TendaError, "Сесія Tenda"):
            await self.client.snapshot()
        self.assertEqual((self.logins, self.key_requests, len(self.online_paths)), (2, 2, 2))

    async def test_stok_prefixed_route_is_cached_when_required(self):
        self.use_stok = True
        self.assertIn(MAC, await self.client.snapshot())
        self.assertIn(MAC, await self.client.snapshot())
        self.assertEqual(self.online_paths, [
            "/goform/getOnlineList", "/;stok=abcdef/goform/getOnlineList", "/;stok=abcdef/goform/getOnlineList",
        ])
        self.assertEqual(self.logins, 1)

    async def test_failed_key_read_reuses_valid_cookie(self):
        self.key_error = True
        with self.assertRaisesRegex(TendaError, "503"):
            await self.client.snapshot()
        self.key_error = False
        self.assertIn(MAC, await self.client.snapshot())
        self.assertEqual((self.logins, self.key_requests), (1, 2))

    async def test_http_and_corrupt_data_do_not_reauthenticate_or_expose_body(self):
        await self.client.snapshot()
        for status, body in (
            (503, PASSWORD),
            (200, json.dumps({"data": PASSWORD})),
            (200, json.dumps({"data": base64.b64encode(b"x").decode()})),
            (200, json.dumps({"data": encrypted({"onlineList": [DEVICE, {}]})})),
            (200, "<html>unrelated router error</html>"),
        ):
            with self.subTest(status=status, body=body):
                self.online_reply = lambda: web.Response(status=status, text=body)
                with self.assertRaises(TendaError) as failure:
                    await self.client.snapshot()
                self.assertNotIn(PASSWORD, str(failure.exception))
        self.assertEqual((self.logins, self.key_requests), (1, 1))


class TendaListTests(TestCase):
    def test_online_list_handles_header_offline_and_guest_rows(self):
        guest = {"mac": "aa:00:00:00:00:03", "hostname": "Guest", "ip": ""}
        offline = {"mac": "aa:00:00:00:00:04", "offline": "1"}
        self.assertEqual(
            set(parse_online_list({
                "onlineList": [{"localhostMac": "ignored"}, DEVICE, offline],
                "guestList": [guest],
            })),
            {MAC, guest["mac"].upper()},
        )

    def test_rejects_partial_invalid_and_ambiguous_lists(self):
        for payload in (
            {}, {"onlineList": None}, {"onlineList": [], "guestList": None},
            {"errCode": 1, "onlineList": []}, [DEVICE, DEVICE],
            [DEVICE, {}], [{**DEVICE, "offline": "unknown"}],
        ):
            with self.subTest(payload=payload), self.assertRaises(TendaError):
                parse_online_list(payload)
