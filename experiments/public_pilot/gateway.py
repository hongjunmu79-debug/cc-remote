"""Opt-in pilot gateway. Not packaged, no configuration/credential migration.

Public and local-control apps MUST be bound to separate loopback ports. Only the
public port may be tunneled. Public cookies are ephemeral aliases, never LAN
credentials. The relay remains the authority for machine scope and revocation.
"""
import asyncio
import json
import secrets
import time
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import JSONResponse, Response
from websockets.asyncio.client import connect

COOKIE = "cc_remote_session"
SAFE_GET = {"/api/session", "/api/machines", "/api/devices", "/api/push-config"}


class Pilot:
    def __init__(self, upstream="http://127.0.0.1:8765", upstream_origin=None):
        self.upstream = upstream
        self.upstream_origin = upstream_origin
        self.public_origin = ""
        self.control_origin = ""
        self.sessions = {}
        self.connections = {}
        self.pending_revocations = set()
        self.grants = {}
        self.lock = asyncio.Lock()
        self.stop = asyncio.Event()
        self.public = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self.control = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
        self.public.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])(self.public_http)
        self.public.websocket("/ws")(self.websocket)
        self.control.api_route("/{path:path}", methods=["GET", "POST"])(self.control_http)

    async def upstream_request(self, method, path, body=None, cookie="", local=False):
        headers = {"Origin": self.upstream if local else self.upstream_origin,
                   "Content-Type": "application/json"}
        if not local:
            # Never forward caller-controlled proxy identity to the LAN relay.
            headers["X-Forwarded-For"] = "192.0.2.1"
        if cookie:
            headers["Cookie"] = f"{COOKIE}={cookie}"
        transport = httpx.AsyncHTTPTransport(local_address="127.0.0.1")
        async with httpx.AsyncClient(trust_env=False, timeout=8, transport=transport) as client:
            try:
                return await client.request(method, self.upstream + path,
                                            headers=headers, content=body)
            except httpx.HTTPError:
                return httpx.Response(503, json={"error": "local_relay_unavailable"})

    @staticmethod
    def error(code):
        return JSONResponse({"error": "pilot_request_rejected"}, status_code=code,
                            headers={"Cache-Control": "no-store"})

    def allowed_host(self, request, origin):
        return bool(origin) and request.headers.get("host") == urlsplit(origin).netloc

    def session(self, request):
        entry = self.sessions.get(request.cookies.get(COOKIE, ""))
        return entry[0] if entry and entry[1] > time.time() else ""

    @staticmethod
    def static_path(path):
        return path == "/" or (path.startswith("/assets/") and ".." not in path
                                and "\\" not in path)

    async def body(self, request):
        data = bytearray()
        async for chunk in request.stream():
            data.extend(chunk)
            if len(data) > 8192:
                raise ValueError("request too large")
        return bytes(data)

    def response(self, upstream):
        return Response(upstream.content, status_code=upstream.status_code,
                        headers={"Content-Type": upstream.headers.get("content-type", "application/json"),
                                 "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
                                 "Referrer-Policy": "no-referrer"})

    async def control_http(self, request: Request, path: str):
        path = "/" + path
        if (not self.allowed_host(request, self.control_origin)
                or request.client.host not in {"127.0.0.1", "::1", "testclient"}
                or any(h in request.headers for h in ("x-forwarded-for", "forwarded", "cf-connecting-ip"))):
            return self.error(403)
        if self.stop.is_set():
            return self.error(503)
        if request.method == "POST" and path == "/api/pilot/stop":
            if request.headers.get("origin") != self.control_origin:
                return self.error(403)
            self.stop.set()
            return JSONResponse({"ok": True})
        if request.method == "GET" and path == "/" and "pair" not in request.query_params:
            return Response('''<!doctype html><meta charset="utf-8"><title>CC Remote 公网试点</title>
<style>body{background:#0b1020;color:#e6f4ff;font:18px system-ui;max-width:680px;margin:60px auto;padding:24px}button,a{display:inline-block;padding:14px;margin:12px;background:#67e8f9;color:#071421;border:0;border-radius:8px;text-decoration:none}</style>
<h1>CC Remote · 公网试点</h1><p>仅供自己的设备测试。消息经过 Cloudflare，不是端到端加密。入口限时关闭，不会开机自启。</p>
<a href="/?pair=1">显示配对二维码</a><button onclick="this.disabled=true;fetch('/api/pilot/stop',{method:'POST'}).then(r=>{if(!r.ok)throw Error();document.body.textContent='已请求停止公网试点，正在关闭入口和撤销授权。请以控制台结果为准。'}).catch(()=>{this.disabled=false;this.textContent='停止请求未确认，请重试或检查控制台'})">停止公网试点</button>
<p>二维码两分钟内一次有效。临时地址改变后需重新扫码。</p>''', media_type="text/html",
                            headers={"Cache-Control": "no-store"})
        if request.method == "GET" and self.static_path(path):
            return self.response(await self.upstream_request("GET", path, local=True))
        if (request.method != "POST" or path != "/api/client-pairing"
                or request.headers.get("origin") != self.control_origin
                or not self.public_origin):
            return self.error(403)
        async with self.lock:
            if self.stop.is_set():
                return self.error(503)
            self.grants = {k: v for k, v in self.grants.items() if v > time.time()}
            if len(self.grants) >= 8:
                return self.error(429)
            upstream = await self.upstream_request("POST", path, b"{}", local=True)
            if upstream.status_code != 200:
                return self.response(upstream)
            data = upstream.json()
            payload = json.loads(data["payload"])
            payload["relay"] = self.public_origin
            # Pilot QR has a shorter lifetime than the underlying LAN grant.
            data["expires_at"] = min(data["expires_at"], int(time.time()) + 120)
            self.grants[payload["token"]] = data["expires_at"]
            print("PILOT_QR_CREATED", flush=True)
            data["payload"] = json.dumps(payload, separators=(",", ":"))
            return JSONResponse(data, headers={"Cache-Control": "no-store"})

    async def public_http(self, request: Request, path: str):
        path = "/" + path
        if self.stop.is_set():
            return self.error(503)
        if not self.allowed_host(request, self.public_origin):
            return self.error(403)
        origin = request.headers.get("origin")
        if origin and origin != self.public_origin:
            return self.error(403)
        if "authorization" in request.headers:
            return self.error(403)
        if request.method == "GET" and self.static_path(path):
            return self.response(await self.upstream_request("GET", path))
        if request.method == "GET" and path == "/api/auth-config":
            return JSONResponse({"multi_user": False, "password_enabled": False},
                                headers={"Cache-Control": "no-store"})
        if request.method == "POST" and path == "/api/client-pairing/redeem":
            async with self.lock:
                if self.stop.is_set():
                    return self.error(503)
                if len(self.sessions) >= 8:
                    return self.error(429)
                try:
                    body = await asyncio.wait_for(self.body(request), timeout=5)
                    token = json.loads(body).get("token", "")
                    if not isinstance(token, str) or self.grants.get(token, 0) <= time.time():
                        print("PILOT_QR_REJECTED_AT_GATE", flush=True)
                        return self.error(401)
                except (ValueError, AttributeError, TimeoutError):
                    return self.error(400)
                self.grants.pop(token, None)
                upstream = await self.upstream_request("POST", path, body)
                print("PILOT_REDEEM_STATUS", upstream.status_code, flush=True)
                cookie = upstream.cookies.get(COOKIE)
                if upstream.status_code != 200 or not cookie:
                    return self.response(upstream)
                alias = secrets.token_urlsafe(32)
                self.sessions[alias] = (cookie, time.time() + 900)
                result = self.response(upstream)
                result.set_cookie(COOKIE, alias, max_age=900, secure=True,
                                  httponly=True, samesite="strict")
                return result
        cookie = self.session(request)
        if not cookie:
            return self.error(401)
        if request.method == "GET" and path in SAFE_GET:
            return self.response(await self.upstream_request("GET", path, cookie=cookie))
        if request.method == "POST" and path == "/api/logout":
            alias = request.cookies.get(COOKIE)
            self.sessions.pop(alias, None)
            self.pending_revocations.add(cookie)
            await self.close_connections(alias)
            result = self.response(await self.upstream_request("POST", path, b"{}", cookie))
            if result.status_code == 200:
                self.pending_revocations.discard(cookie)
            result.delete_cookie(COOKIE, secure=True, httponly=True, samesite="strict")
            return result
        # No public pairing generation, device enrollment, passwords or health metadata.
        return self.error(403)

    async def websocket(self, websocket: WebSocket):
        alias = websocket.cookies.get(COOKIE, "")
        cookie = self.session(websocket)
        if (self.stop.is_set() or not self.allowed_host(websocket, self.public_origin) or not cookie
                or websocket.headers.get("origin") != self.public_origin
                or "authorization" in websocket.headers):
            await websocket.close(code=1008)
            return
        query = str(websocket.query_params)
        target = self.upstream.replace("http://", "ws://", 1) + "/ws" + ("?" + query if query else "")
        try:
            async with connect(target, origin=self.upstream_origin,
                               additional_headers={"Cookie": f"{COOKIE}={cookie}"},
                               proxy=None, local_addr=("127.0.0.1", 0),
                               max_size=16 * 1024 * 1024, max_queue=16) as upstream:
                # The handshake can race with logout or shutdown.
                if self.stop.is_set() or not self.session(websocket):
                    return
                await websocket.accept()
                self.connections.setdefault(alias, set()).add(websocket)
                async def inbound():
                    while True:
                        await upstream.send(await websocket.receive_text())
                async def outbound():
                    async for message in upstream:
                        if isinstance(message, str):
                            await websocket.send_text(message)
                tasks = [asyncio.create_task(inbound()), asyncio.create_task(outbound())]
                try:
                    remaining = max(0, self.sessions.get(alias, ("", 0))[1] - time.time())
                    await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED, timeout=remaining)
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        except Exception:
            pass  # Never log frames, cookies, URLs or upstream exception headers.
        finally:
            connections = self.connections.get(alias)
            if connections is not None:
                connections.discard(websocket)
                if not connections:
                    self.connections.pop(alias, None)
            try:
                await websocket.close(code=1001)
            except Exception:
                pass

    async def close_connections(self, alias):
        for connection in list(self.connections.pop(alias, ())):
            try:
                await connection.close(code=1008)
            except Exception:
                pass

    async def revoke_all(self):
        self.stop.set()
        # Wait out any redemption already issuing its session before taking it away.
        async with self.lock:
            self.grants.clear()
            self.pending_revocations.update(cookie for cookie, _ in self.sessions.values())
            self.sessions.clear()
        for alias in list(self.connections):
            await self.close_connections(alias)
        for cookie in list(self.pending_revocations):
            try:
                result = await self.upstream_request("POST", "/api/logout", b"{}", cookie)
                if result.status_code == 200:
                    self.pending_revocations.discard(cookie)
            except Exception:
                pass
        return not self.pending_revocations
