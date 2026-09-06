"""Manual, bounded pilot launcher. No autostart or installation-side changes."""
import argparse
import asyncio
import contextlib
import re
import socket
import subprocess
import sys
from pathlib import Path

import uvicorn

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from experiments.public_pilot.gateway import Pilot


def listener():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    return sock


async def main(args):
    pilot = Pilot(upstream_origin=args.lan_origin)
    public_socket, control_socket = listener(), listener()
    pilot.control_origin = f"http://127.0.0.1:{control_socket.getsockname()[1]}"
    servers = [uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="warning",
                                           ws_max_size=16 * 1024 * 1024, timeout_graceful_shutdown=2))
               for app in (pilot.public, pilot.control)]
    tasks = [asyncio.create_task(server.serve(sockets=[sock]))
             for server, sock in zip(servers, (public_socket, control_socket))]
    command = [args.cloudflared, "tunnel", "--no-autoupdate", "--protocol", "http2",
               "--edge", "198.41.192.167:7844", "--edge", "198.41.200.13:7844",
               "--edge-bind-address", args.wlan_address,
               "--url", f"http://127.0.0.1:{public_socket.getsockname()[1]}"]
    child = None
    reader = None
    try:
        # Smoke-check the local service before creating a public entrance.
        health = await pilot.upstream_request("GET", "/healthz")
        if health.status_code != 200 or not health.json().get("wrapper_connected"):
            raise RuntimeError("Local wrapper is not ready")
        child = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW)
        async def read_logs():
            async for raw in child.stdout:
                line = raw.decode("utf-8", "replace")
                match = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", line)
                if match:
                    pilot.public_origin = match.group(0)
                    print("PUBLIC " + pilot.public_origin, flush=True)
                if "Registered tunnel connection" in line:
                    print("TUNNEL_CONNECTED", flush=True)
                    print("CONTROL " + pilot.control_origin, flush=True)
                if "failed to request quick Tunnel" in line:
                    print("TUNNEL_ALLOCATION_FAILED", flush=True)
        reader = asyncio.create_task(read_logs())
        stop_task = asyncio.create_task(pilot.stop.wait())
        child_task = asyncio.create_task(child.wait())
        await asyncio.wait([stop_task, child_task], timeout=args.seconds,
                           return_when=asyncio.FIRST_COMPLETED)
        stop_task.cancel()
        child_task.cancel()
        await asyncio.gather(stop_task, child_task, return_exceptions=True)
    finally:
        if child and child.returncode is None:
            child.terminate()
            await child.wait()
        if reader:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        revoked = await pilot.revoke_all()
        for server in servers:
            server.should_exit = True
        await asyncio.gather(*tasks, return_exceptions=True)
        for sock in (public_socket, control_socket):
            with contextlib.suppress(OSError):
                sock.close()
        print("PILOT_STOPPED_AND_SESSIONS_REVOKED" if revoked else
              "PILOT_STOPPED_PUBLIC_ALIASES_CLEARED_UPSTREAM_REVOCATION_UNCONFIRMED", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloudflared", required=True)
    parser.add_argument("--wlan-address", required=True)
    parser.add_argument("--lan-origin", required=True)
    parser.add_argument("--seconds", type=int, default=900)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 900:
        parser.error("seconds must be 1..900")
    asyncio.run(main(args))
