import json
import time
import ssl

import httpx
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from experiments.public_pilot.gateway import COOKIE, Pilot


@pytest.mark.asyncio
async def test_upstream_reuses_verified_context_but_not_cookies(pilot, monkeypatch):
    contexts, cookies = [], []
    def transport(**kwargs):
        contexts.append(kwargs["verify"])
        assert kwargs["trust_env"] is False
        assert kwargs["local_address"] == "127.0.0.1"
        def respond(request):
            cookies.append(request.headers.get("cookie", ""))
            return httpx.Response(200, headers={"Set-Cookie": f"{COOKIE}=private; Path=/"})
        return httpx.MockTransport(respond)
    monkeypatch.setattr(httpx, "AsyncHTTPTransport", transport)
    await pilot.upstream_request("GET", "/api/session", cookie="explicit")
    await pilot.upstream_request("GET", "/assets/test.js")
    assert cookies == [f"{COOKIE}=explicit", ""]
    assert contexts == [pilot.ssl_context, pilot.ssl_context]
    assert pilot.ssl_context.verify_mode == ssl.CERT_REQUIRED
    assert pilot.ssl_context.check_hostname


@pytest.fixture
def pilot():
    p = Pilot(upstream_origin="http://192.168.10.52:8765")
    p.public_origin = "https://example.trycloudflare.com"
    p.control_origin = "http://127.0.0.1:8770"
    return p


@pytest.mark.parametrize("path", ["/api/client-pairing", "/api/devices/pair", "/api/login", "/api/machines", "/healthz"])
def test_public_no_unauthorized_access_or_loopback_spoof(pilot, path):
    with TestClient(pilot.public, base_url=pilot.public_origin) as client:
        response = client.post(path, headers={"Origin": "http://127.0.0.1:8765",
                                              "X-Forwarded-For": "127.0.0.1",
                                              "CF-Connecting-IP": "127.0.0.1"})
        assert response.status_code == 403
        assert client.get(path).status_code == 401


def test_ws_needs_session_and_exact_origin(pilot):
    with TestClient(pilot.public, base_url=pilot.public_origin) as client:
        with pytest.raises(WebSocketDisconnect):
            with client.websocket_connect("/ws", headers={"Origin": pilot.public_origin}):
                pass


def test_control_rejects_forwarded_or_cross_origin_pairing(pilot):
    with TestClient(pilot.control, base_url=pilot.control_origin) as client:
        assert client.post("/api/client-pairing").status_code == 403
        assert client.post("/api/client-pairing", headers={"Origin": pilot.control_origin,
                                                          "X-Forwarded-For": "127.0.0.1"}).status_code == 403


def test_stop_requires_local_origin_and_public_cannot_stop(pilot):
    with TestClient(pilot.public, base_url=pilot.public_origin) as public:
        assert public.post("/api/pilot/stop").status_code == 401
    with TestClient(pilot.control, base_url=pilot.control_origin) as local:
        assert local.post("/api/pilot/stop").status_code == 403
        assert not pilot.stop.is_set()
        assert local.post("/api/pilot/stop", headers={"Origin": pilot.control_origin}).status_code == 200
        assert pilot.stop.is_set()


def test_lan_cookie_is_not_a_public_session(pilot):
    with TestClient(pilot.public, base_url=pilot.public_origin) as client:
        assert client.get("/api/machines", headers={"Cookie": f"{COOKIE}=private-lan-cookie"}).status_code == 401
        assert client.get("/api/session", headers={"Host": "127.0.0.1:8765"}).status_code == 403


@pytest.mark.asyncio
async def test_stop_revokes_sessions_and_clears_grants(pilot):
    calls = []
    async def upstream(method, path, body=None, cookie="", local=False):
        calls.append((path, cookie))
        return httpx.Response(200, json={"ok": True})
    pilot.upstream_request = upstream
    pilot.sessions["alias"] = ("private", time.time() + 60)
    pilot.grants["grant"] = time.time() + 60
    await pilot.revoke_all()
    assert calls == [("/api/logout", "private")]
    assert not pilot.sessions and not pilot.grants


def test_expired_unknown_and_oversize_grants(pilot):
    pilot.grants["old"] = time.time() - 1
    with TestClient(pilot.public, base_url=pilot.public_origin) as client:
        for token in ("old", "unknown"):
            assert client.post("/api/client-pairing/redeem", json={"token": token}).status_code == 401
        assert client.post("/api/client-pairing/redeem", content=b"x" * 8193).status_code == 400


def test_qr_redeem_alias_single_use_and_logout(pilot):
    calls = []
    async def upstream(method, path, body=None, cookie="", local=False):
        calls.append((path, cookie, local))
        request = httpx.Request(method, pilot.upstream + path)
        if path == "/api/client-pairing":
            return httpx.Response(200, request=request, json={
                "payload": json.dumps({"token": "grant", "relay": "http://old", "machine_id": "test"}),
                "expires_at": time.time() + 600})
        if path.endswith("/redeem"):
            return httpx.Response(200, request=request, json={"ok": True},
                                  headers={"Set-Cookie": f"{COOKIE}=private-lan-cookie; Path=/; HttpOnly"})
        return httpx.Response(200, request=request, json={"ok": True})
    pilot.upstream_request = upstream
    with TestClient(pilot.control, base_url=pilot.control_origin) as local:
        grant = local.post("/api/client-pairing", headers={"Origin": pilot.control_origin}).json()
        assert json.loads(grant["payload"])["relay"] == pilot.public_origin
        assert grant["expires_at"] <= time.time() + 120
    with TestClient(pilot.public, base_url=pilot.public_origin) as public:
        response = public.post("/api/client-pairing/redeem", json={"token": "grant"})
        assert response.status_code == 200
        assert "private-lan-cookie" not in response.headers["set-cookie"]
        assert "Secure" in response.headers["set-cookie"]
        assert public.get("/api/machines").status_code == 200
        assert calls[-1][1] == "private-lan-cookie"
        assert public.post("/api/client-pairing/redeem", json={"token": "grant"}).status_code == 401
        assert public.post("/api/client-pairing", headers={"Origin": pilot.public_origin}).status_code == 403
        assert public.post("/api/logout").status_code == 200
        assert public.get("/api/machines").status_code == 401


def test_stopped_pilot_rejects_new_requests(pilot):
    pilot.stop.set()
    with TestClient(pilot.public, base_url=pilot.public_origin) as public:
        assert public.get("/").status_code == 503
        assert public.post("/api/client-pairing/redeem", json={"token": "grant"}).status_code == 503
    with TestClient(pilot.control, base_url=pilot.control_origin) as local:
        assert local.post("/api/client-pairing", headers={"Origin": pilot.control_origin}).status_code == 503


@pytest.mark.asyncio
async def test_shutdown_closes_live_connections_even_when_relay_unavailable(pilot):
    closed = []
    class Connection:
        async def close(self, code):
            closed.append(code)
    async def unavailable(*args, **kwargs):
        return httpx.Response(503)
    pilot.upstream_request = unavailable
    pilot.connections["alias"] = {Connection()}
    pilot.sessions["alias"] = ("private", time.time() + 60)
    assert await pilot.revoke_all() is False
    assert closed == [1008]
    assert not pilot.sessions and not pilot.connections
    assert pilot.pending_revocations == {"private"}
    async def recovered(*args, **kwargs):
        return httpx.Response(200)
    pilot.upstream_request = recovered
    assert await pilot.revoke_all() is True
    assert not pilot.pending_revocations


def test_logout_invalidates_alias_before_upstream_and_closes_socket(pilot):
    closed = []
    class Connection:
        async def close(self, code):
            closed.append(code)
    async def unavailable(*args, **kwargs):
        assert "alias" not in pilot.sessions
        assert closed == [1008]
        return httpx.Response(503)
    pilot.upstream_request = unavailable
    pilot.sessions["alias"] = ("private", time.time() + 60)
    pilot.connections["alias"] = {Connection()}
    with TestClient(pilot.public, base_url=pilot.public_origin) as public:
        public.cookies.set(COOKIE, "alias")
        assert public.post("/api/logout").status_code == 503
        assert public.get("/api/session").status_code == 401
    assert pilot.pending_revocations == {"private"}


def test_expired_session_cannot_reconnect(pilot):
    pilot.sessions["alias"] = ("private", time.time() - 1)
    with TestClient(pilot.public, base_url=pilot.public_origin) as public:
        public.cookies.set(COOKIE, "alias")
        assert public.get("/api/session").status_code == 401
        with pytest.raises(WebSocketDisconnect):
            with public.websocket_connect("/ws", headers={"Origin": pilot.public_origin}):
                pass
