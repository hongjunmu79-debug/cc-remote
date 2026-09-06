# CC Remote — opt-in public pilot

Experimental, not included in release installers. No autostart, paid service,
router change, or modification to existing LAN configuration. This is **not yet
accepted for production**: phone WebSocket/command/reconnect tests remain pending.

## Architecture and boundaries

Cloudflare temporary HTTPS entrance -> loopback public gateway -> existing LAN
relay. The gateway exposes only static assets, QR redemption, session status,
machine-scoped read APIs, logout and authenticated WebSocket transport. It does
not expose local pairing creation, password login, device enrollment, wrapper
bearer authentication, health metadata or arbitrary upstream URLs.

A second, separate loopback-only listener serves the local QR page and stop
button. Never point a tunnel at this control listener or directly at port 8765.
Local actions enforce Host, Origin and absence of forwarding headers. Public
requests cannot acquire local privileges by forging forwarding headers.

QR grants expire after at most 120 seconds and are single-use at the gateway.
Public cookies are random in-memory aliases with Secure/HttpOnly/SameSite=Strict,
not the original LAN cookies. Relay machine authorization remains authoritative.
Shutdown drops grants, stops the tunnel and requests revocation of pilot sessions.
No model credentials are read or modified. Cloudflare handles the public TLS
termination; this is not end-to-end encryption.

## Manual launch (operator only)

Use a Python environment with the repository's existing FastAPI, httpx,
websockets and uvicorn dependencies installed. Download cloudflared from its
official release and verify its published SHA256 first.

```powershell
python experiments/public_pilot/run.py --cloudflared "D:\应用程序\CC Remote Public Pilot\cloudflared.exe" --wlan-address 192.168.10.52 --lan-origin http://192.168.10.52:8765 --seconds 900
```

Addresses above are this test machine's observed values, not reusable defaults.
The launcher currently uses explicit official edge IPs and source binding as a
diagnostic workaround for this machine's FlClash path. It is not a permanent DNS
or routing solution. Keep TLS verification enabled. The printed `CONTROL` URL
has the QR and stop buttons. Public hostname changes on restart; scan a new QR.
Each run is limited to 15 minutes. Closing via the stop button is preferred.

## Verification

`python -m pytest tests/test_public_pilot.py -q`

2026-09-05: phone on separate personal hotspot returned HTTP 200 from a synthetic
probe; then image QR redemption against the real scoped gateway succeeded.
Anonymous session request returned HTTP 401. Live task list, command reply,
network recovery and overseas connectivity are not yet accepted. Network TLS
failures were also observed, so no long-term stability claim is made.

2026-09-06 unattended follow-up:

- `tests/test_public_pilot.py` and `tests/test_auth.py`: 76 passed. Pilot-specific
  tests now include logout closing live connections, expiry rejecting reconnect,
  shutdown rejecting new requests, and failed upstream revocation reporting.
- Including `tests/test_devices.py`: 81 passed, 3 failed. The failures concern
  POSIX file mode assertions/checks on Windows; they were not changed or suppressed.
- Logout removes the public alias before awaiting the local relay. Shutdown
  clears aliases even if the local relay is unavailable, and reports upstream
  revocation as unconfirmed instead of claiming success. Existing WebSockets
  cannot extend their original alias expiry by reconnecting.
- ADB no longer reported a connected device. The background Android transport
  probe compiled, but was **not executed**. Phone command/reply, task UI, network
  recovery and overseas tests remain unverified. A later fresh-QR rejection
  reported by Android remains unresolved; initial successful redemption alone
  does not demonstrate reliable pairing.
- No tunnel remained running; the original relay reported its wrapper connected.
  Do not publish this pilot as a production installer or imply full acceptance.
