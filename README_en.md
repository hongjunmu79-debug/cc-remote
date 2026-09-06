# CC Remote

**Bring Claude Code / Codex on your machine to your phone and any browser.**

Self-hosted · Dual-engine · Multi-session · Live process · Responsive Web

**Development version: v3.0.0** · Pending distribution `v3.0.0-pager.18` · Wire protocol v19

[中文](README.md) ·
[Journey 1: Windows + Android on a LAN](#journey-1-windows--android-on-a-trusted-lan) ·
[Journey 2: remote access](#journey-2-https-relay--wrapper-for-remote-access) ·
[Security](#security-please-read) ·
[Changelog](CHANGELOG.md) ·
[Upstream provenance](docs/UPSTREAM.md)

cc-remote is an open-source remote control plane. A local `wrapper` drives the

![cc-remote Claude sessions and model controls](assets/readme-model-controls.jpg)
already installed and authenticated `claude` / `codex` CLI, while browsers view
and control its sessions through your self-hosted WebSocket relay. Models,
authentication, and tool execution remain under the local CLI; cc-remote does
not proxy model APIs or bake API keys into the web client.

This repository also ships a native **Android pager** (`dev.ccremote.lan`): one
WebView owns authentication, WebSocket transport, and the chat surface, while a
Jetpack Compose dashboard projects the session state through an exact-origin
bridge. There is no second session state machine.

v3.0.0 is not a visual rebrand. It adds isolated Code / Work spaces on top of
the existing two-engine, multi-session remote control plane, while redesigning
history projection, native-client coordination, multi-device routing, and
release boundaries. The work targets real failures seen with very long
sessions, stale App/CLI state, mobile history jumps, and cross-machine leakage.

![cc-remote Claude sessions and multi-session workspace](assets/readme-claude-multisession.jpg)

---

## Table of contents

- [What changed in v3](#what-changed-in-v3)
- [Prerequisites](#prerequisites)
- [Journey 1: Windows + Android on a trusted LAN](#journey-1-windows--android-on-a-trusted-lan)
- [Journey 2: HTTPS relay + wrapper for remote access](#journey-2-https-relay--wrapper-for-remote-access)
- [Architecture](#architecture)
- [Environment variables](#environment-variables)
- [Auth model](#auth-model)
- [Reliability boundary](#reliability-boundary)
- [Operations](#operations)
- [Security (please read)](#security-please-read)
- [Release maintainers](#release-maintainers)
- [Development](#development)
- [FAQ](#faq)
- [License](#license)

---

## What changed in v3

v3 advances cc-remote from “control a CLI in a browser” into a local-first,
recoverable control plane that can safely connect multiple machines. Compared
with the previous public release, the major changes are:

| Area | v3.0.0 |
|---|---|
| **Code / Work spaces** | Add an independent Cowork surface beside repository-oriented Code sessions. Claude and Codex each receive private projects, file/link/note knowledge sources, reusable templates, schedules, and artifacts. Work and Code isolate directories, sessions, base prompts, and permission boundaries. |
| **History startup and very long sessions** | The browser first paints its last validated IndexedDB projection. A source-fingerprinted SQLite index on the wrapper serves recent turn summaries first, while tool output, reasoning, process logs, and oversized text load per turn. Short sessions no longer wait for a full source scan; long sessions page backward without losing the reader's viewport. |
| **Large Codex rollouts** | Codex history is read backward by turn while preserving app-server-native resume and compaction state; cc-remote never re-uploads the entire rollout to the model. A tightly guarded official HTTP compatibility path is used only for a specific oversized Codex Desktop + OpenAI resume case. |
| **Native App / CLI coordination** | Claude CLI/Desktop/Agent View and Codex shared daemon/App/CLI retain engine-specific ownership models. v3 reconciles running, read-only, interrupt, steer, compact, turn binding, and terminal state so sibling sessions do not lock each other, old turns do not move to the tail, and interrupted work does not leave ghost activity. |
| **Multi-device isolation** | Device Center adds single-use pairing, independently revocable machine credentials, and presence. The relay routes only an account's allowed `machine_id` values. Device, Code / Work, engine, connection generation, and session ownership are isolated so delayed frames cannot mutate the active view. |
| **Mobile and artifact UX** | Loading older history preserves the scroll anchor. Images load on demand and support a lightbox, tap-to-close, and pinch zoom. Markdown, source, HTML, PDF, and Office previews remain within the local security boundary. PWA icons, narrow-screen sheets, error presentation, and process timelines are also aligned. |
| **Rollback-safe releases** | The product version is v3.0.0 and the wire protocol is v19. Builds and deployments validate both values. The VPS uses immutable releases, release-local virtual environments, an atomic `current` switch, and rollback instead of overwriting a live directory. |

> **The trust boundary has not changed:** model accounts, API keys, session
> sources, and tool execution stay on the wrapper machine. The VPS relay stores
> no conversations or artifacts. Browsing history reads only local transcripts,
> rollouts, and rebuildable projections; it never resumes an engine or creates a
> model turn.

See [CHANGELOG.md](CHANGELOG.md) for the complete release notes and upgrade
requirements.

## Prerequisites

cc-remote **does not install or log in an agent CLI for you.** Before starting
either journey you must already have a working installation of **Claude Code**
and/or **Codex CLI**, signed in and able to chat on its own:

- **Claude Code** — a signed-in `claude` that responds in your terminal.
- **Codex CLI** — a signed-in `codex` whose `app-server` can start.

The wrapper drives these existing installations and never replaces them. Model
credentials and the model API stay on the agent machine; cc-remote only moves
the control link (session view + commands) over your self-hosted relay.

Office artifact preview (DOCX/XLSX/PPTX → PDF) additionally requires
**LibreOffice** on the wrapper host. It is optional everywhere else.

---

## Journey 1: Windows + Android on a trusted LAN

The fastest full experience: a **Windows machine** runs the relay and the
wrapper (packaged as a Windows service or portable archive), and an **Android
phone** on the same LAN runs the native pager (`dev.ccremote.lan`) or the
responsive web client. No public VPS, no domain, no TLS needed — traffic stays
inside your LAN.

For a normal install, download exactly two files from
[GitHub Releases](https://github.com/hongjunmu79-debug/cc-remote/releases):
the Windows `*-windows-x64-setup.exe` and Android `app-release.apk`. Double-click
the Windows installer, open the Android app, and scan. No password, IP address,
Python, Node, ADB, or command line is required; manual address/password fields
remain recovery-only fallbacks.

```
Android pager / phone browser ──http://<windows-lan-ip>:8765──▶ Windows relay+wrapper
                                                                  └─ drives local claude / codex
```

![cc-remote multi-session workspace](assets/readme-multi-session.jpg)

### 1) Install the Windows distribution

> **[GitHub Releases: download the Windows x64 one-click installer
> (`*-windows-x64-setup.exe`)](https://github.com/hongjunmu79-debug/cc-remote/releases)**
> · [Download its matching `*-windows-x64-setup.exe.sha256` from the same
> Release](https://github.com/hongjunmu79-debug/cc-remote/releases)

> **The source and installer must match:** this README may be updated before an
> installer is published. Use only a Release whose publication identifies the
> source SHA it contains (or was built from). Do not attribute unreleased branch
> features described here to an older Release asset.

In a directory containing only the two files from that Release, verify the
installer against its sidecar:

```powershell
$setup = @(Get-Item .\cc-remote-v*-windows-x64-setup.exe)
if ($setup.Count -ne 1) { throw "expected exactly one setup.exe" }
Get-FileHash -LiteralPath $setup[0].FullName -Algorithm SHA256
Get-Content -LiteralPath "$($setup[0].FullName).sha256"
```

The portable archive remains available in the same Release for foreground use.
The installer:

- installs to a directory you choose (no fixed path);
- generates strong `SESSION_SECRET` and `WRAPPER_TOKEN`;
- selects safe machine/workspace/LAN defaults without requiring a password;
- detects `claude` and `codex` without copying their credentials;
- writes configuration with restrictive user-only ACLs and refuses placeholder
  values;
- registers scheduled tasks that supervise the long-lived relay/wrapper
  processes with bounded restart-on-failure;
- creates a firewall rule limited to `LocalSubnet` on the selected port;
- adds Start Menu/Desktop **cc-remote console** shortcuts and opens it;
- preserves the existing configuration on upgrade and supports clean
  uninstall/rollback without touching Claude/Codex sessions or credentials.

Unattended/silent installs are supported by passing a config file. See
[cc_portable_control/windows/README.md](cc_portable_control/windows/README.md).

### 2) Display the client pairing QR

The installer opens the local console and generates the QR automatically. The
QR is short-lived, single-use, and scoped to the selected machine and new client.

```bash
curl http://<windows-lan-ip>:8765/healthz
# expect: {"ok":true,"wrapper_connected":true,"clients":0}
```

### 3) Install and open the Android pager

1. Download and install `app-release.apk` from the same GitHub Release.
2. First launch opens the scanner directly; point it at the Windows console QR.
3. The app validates and saves the relay origin, redeems the HttpOnly session,
   and enters directly. Manual origin/password entry remains a fallback.
4. The WebView owns login/WebSocket/chat; the native dashboard projects the
   same session state through the bridge. Keep one session state machine.

### 4) (Web-only alternative) use the responsive web client

Open `http://<windows-lan-ip>:8765/` in any browser on the LAN and log in. The
web client installs as a PWA and works on the phone too; the native pager adds
the bounded dashboard projection on top.

### 5) Local one-machine quick start (source checkout)

For a single machine with Python + Node already installed:

```bash
git clone https://github.com/hongjunmu79-debug/cc-remote.git
cd cc-remote-native-pager

python3 -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --require-hashes --only-binary=:all: -r requirements.lock

npm --prefix web ci
npm --prefix web run build          # produces web/dist/

install -m 600 .env.example .env    # Windows: copy .env.example .env
```

Edit `.env` — at minimum:

```ini
SESSION_SECRET=<openssl rand -hex 32>
WRAPPER_TOKEN=<openssl rand -hex 32>
PUBLIC_ORIGIN=http://127.0.0.1:8765
WEB_STATIC_DIR=web/dist
CC_CWD=C:\path\to\your\project
```

Run in two terminals:

```bash
python -m cc_remote.relay          # serves web + /ws + /api
python -m cc_remote.wrapper        # drives the local claude / codex CLI
```

Then open `http://127.0.0.1:8765` and log in.

![cc-remote new session start](assets/readme-new-session.jpg)

> For LAN use (browsers or the Android pager reaching a non-loopback IP), set
> `RELAY_HOST=0.0.0.0` and `PUBLIC_ORIGIN=http://<lan-ip>:8765`, and ensure the
> firewall allows the port on the LAN subnet. The packaged Windows distribution
> does this for you.

---

## Journey 2: HTTPS relay + wrapper for remote access

Move the relay to a **public VPS** with a domain (Caddy auto-provisions TLS),
keep the wrapper on your agent machine (or on the Windows machine), and reach
everything from anywhere over `wss://`.

```
agent machine wrapper ──wss:443──▶ Caddy(VPS, auto HTTPS) ──▶ relay(127.0.0.1:8765) ◀──wss:443── phone browser
                                                                    └─ serves web/dist (same origin)
```

![cc-remote Claude session](assets/readme-claude-session.jpg)

### 1) Download and verify the bootstrap

Confirm the version and release attestation on GitHub, then download
`install.sh` and `SHA256SUMS` from the same release:

```bash
release=https://github.com/hongjunmu79-debug/cc-remote/releases/download/v3.0.0-pager.18
curl -fLO "$release/install.sh"
curl -fLO "$release/SHA256SUMS"

# Linux
grep ' install.sh$' SHA256SUMS | sha256sum -c -
# On macOS use:
# grep ' install.sh$' SHA256SUMS | shasum -a 256 -c -
chmod +x install.sh
```

The bootstrap detects OS/CPU, downloads only the selected role artifact, and
checks its SHA-256 before extraction or execution.

### 2) Install Relay on the VPS

Point the domain's A/AAAA record at the VPS, open ports 80/443, then run:

```bash
./install.sh relay --domain remote.example.com
```

On Linux the script requests `sudo` itself. A first install asks interactively
for a web password of at least 16 characters, generates Relay secrets, installs
Caddy/systemd, and performs immutable staging, atomic `current` activation, and
rollback under `/opt/cc-remote/releases/`. An existing
`/opt/cc-remote/.env` is preserved.

Open `https://remote.example.com/`, sign in, choose **Allow adding devices** in
Device Center, and copy the one-time pair code.

### 3) Install Wrapper where Claude / Codex runs

First ensure the native `claude` or `codex` CLI is signed in and works on that
machine, then run:

```bash
./install.sh wrapper \
  --relay https://remote.example.com \
  --pair XXXXX-XXXXX-XXXXX-XXXXX \
  --name "Desktop"
```

Run the macOS installer as the logged-in desktop user; it creates a per-user
LaunchAgent. Linux requests `sudo`, while Wrapper and all model/tool descendants
still run as the ordinary user who started installation. The long-lived device
credential is stored only in a mode-`0600` private config. It is never embedded
in a plist, systemd unit, or release directory.

For an upgrade, download the new version's `install.sh` and rerun it. Relay
still needs `--domain`; a previously paired Wrapper needs only:

```bash
./install.sh wrapper
```

Complete protocol upgrades for Relay, Web, and every Wrapper in one maintenance
window, then hard-refresh open browser tabs. The installers retain the previous
release and restore both `current` and the service definition if activation
does not become healthy.

### 4) Use the Android pager remotely

Enter `https://remote.example.com/` on first launch. HTTPS root origins are
always accepted; the WebView then owns login/WebSocket/chat exactly as it does
on the LAN. Public cleartext HTTP is rejected by design.

### 5) Generate tokens manually (source staging path)

The source-staging/manual path remains available for development, custom
deployments, and recovery:

```bash
openssl rand -hex 32   # WRAPPER_TOKEN (must match on relay + wrapper)
openssl rand -hex 32   # SESSION_SECRET (relay)
# also pick a LOGIN_PASSWORD (web login password)
```

Build the web client and upload the staging tree to the VPS, then run
`deploy/setup-vps.sh your-domain.com ~/cc-remote-upload`. The installer builds
an immutable release + venv, atomically switches `/opt/cc-remote/current`, and
rolls back `current`, the Caddyfile, and the relay unit together on failure.
See [deploy/README.md](deploy/README.md).

Verify:

```bash
curl https://remote.example.com/healthz
# expect: {"ok":true,"wrapper_connected":false,"clients":0}
```

---

## Architecture

Two **independent** links:

```
MODEL LINK (cc-remote never touches):  claude / codex ──(their local config)──▶ model service

CONTROL LINK (this repo):              browser ⇄ relay(WebSocket) ⇄ wrapper ⇄ SDK / app-server ⇄ local CLI
```

| Component | Runs where | What it does |
|---|---|---|
| **wrapper** | the machine where `claude` / `codex` runs | Holds a session pool, translates SDK/app-server events to the wire protocol, handles interrupt/drain, reads transcript/rollout history on demand, and temporarily converts Office previews locally. **Outbound-only to the relay — no inbound ports needed.** |
| **relay** | public VPS (or the LAN machine) | Pure WebSocket forwarder (FastAPI). It keeps one wrapper slot per `machine_id`; browsers use an HttpOnly session cookie and receive events only from their selected machine. **Does not persist sessions or artifacts, never imports `claude-agent-sdk`, and never touches the model API.** |
| **web** | the browser / Android WebView | React client; the relay serves its static files (`web/dist`) from the same origin. |
| **Android pager** | the phone | A bounded Jetpack Compose projection of the web reducer's state over an exact-origin WebMessage bridge. One WebView owns auth/WebSocket/chat. |

### How native terminals and Remote cooperate

Code sessions follow each CLI's real control plane without replacing official
commands:

- **Claude:** `claude` always remains the official command and official TUI;
  cc-remote installs no alias, shim, or PATH interception. Sessions opened
  directly by `claude`, Claude Desktop, or Agent View are read-only mirrors in
  Remote by default. To write from Remote, the user explicitly chooses takeover;
  cc-remote sends SIGTERM only to the exact same-user Claude process identity,
  waits for release, and then resumes the same session through the SDK.
- **Codex Code:** prefers Codex's official shared app-server daemon, so native
  Codex clients and Remote share the thread and control state. If the installed
  version cannot provide it, cc-remote explicitly falls back to a private
  app-server. Set `CC_REMOTE_CODEX_DAEMON=off` for troubleshooting.
- **Work:** Claude and Codex Work keep private processes and directories and do
  not join the Code control plane.

### Where artifact preview runs

- HTML is sanitized with DOMPurify in the browser and rendered in a scriptless,
  network-blocked sandbox iframe.
- PNG/JPEG/GIF/WebP/AVIF and PDF are path-, type-, and size-checked by the
  wrapper, then returned only to the requesting browser through the
  authenticated WebSocket.
- DOC/DOCX/ODT/RTF, XLS/XLSX/ODS, and PPT/PPTX/ODP are converted to PDF by
  LibreOffice on the **wrapper host**. On Linux, bubblewrap removes network and
  user-directory access. The directory is deleted immediately after conversion.
- The relay forwards bounded preview frames and stores neither originals nor
  converted files.

![cc-remote process timeline](assets/readme-process-timeline.jpg)

---

## Environment variables

**Relay**

| Var | Default | Notes |
|---|---|---|
| `RELAY_HOST` / `RELAY_PORT` | `127.0.0.1` / `8765` | Listen address (behind Caddy in prod — keep 127.0.0.1; LAN setups set `0.0.0.0`). |
| `LOGIN_PASSWORD` | empty | Optional single-user password fallback; normal client onboarding uses a one-time QR. |
| `LOGIN_USERS_JSON` | empty | Optional multi-user policy; replaces `LOGIN_PASSWORD`. |
| `SESSION_SECRET` | empty | HMAC secret to sign session tokens. **Required** (`openssl rand -hex 32`). |
| `PUBLIC_ORIGIN` | empty | Exact browser origin allowed to connect, e.g. `https://remote.example.com`; **required**, non-loopback origins must use HTTPS unless `ALLOW_INSECURE_HTTP` is enabled. |
| `ALLOW_INSECURE_HTTP` | `0` | Escape hatch for a bare public IPv4 address. Off by default; credentials and all session traffic are unencrypted while enabled. Prefer TLS. |
| `WRAPPER_TOKEN` | placeholder | Wrapper Bearer token for single-machine/compatibility mode; required unless `WRAPPER_TOKENS_JSON` is set. |
| `WRAPPER_TOKENS_JSON` | empty | Optional machine-bound tokens; replaces the relay's wildcard `WRAPPER_TOKEN`. |
| `WEB_STATIC_DIR` | empty | Point at `web/dist` to serve the web client same-origin; empty = API/WS only. |
| `DEVICE_PAIRING_TTL_SECONDS` | `600` | Lifetime of a single-use pairing code in seconds. |

**Wrapper**

| Var | Default | Notes |
|---|---|---|
| `RELAY_URL` | `ws://127.0.0.1:8765/ws` | Relay WebSocket URL (`wss://domain/ws` in prod). |
| `WRAPPER_TOKEN` | `change-me-wrapper` | Same as relay. |
| `CC_REMOTE_MACHINE_ID` | `default` | Stable route id on a multi-machine relay. |
| `CC_CWD` | cwd | Default working directory for new sessions; **must be correct** for Claude `--resume`. |
| `CC_REMOTE_CODEX_DAEMON` | `auto` | Code prefers Codex's official shared daemon; `off` forces private stdio app-server. |
| `MAX_CONCURRENT_SESSIONS` | `20` | Maximum resident agent subprocesses. |
| `CLAUDE_WORK_ROOT` | `~/.claude/cc-remote/work` | Private Claude Work root. |
| `CODEX_WORK_ROOT` | `~/.codex/cc-remote/work` | Private Codex Work root. |

Each message accepts at most 8 attachments, at most 6 MiB each and 8 MiB decoded
in total; oversized input is rejected before a model turn starts.

---

## Auth model

- **Web/Android clients**: a local or already-authenticated console issues a
  single-use QR JSON scoped to `machine_id/client_id`.
  `POST /api/client-pairing/redeem` consumes it and creates the existing
  **HttpOnly, SameSite=Strict** HMAC cookie. The secret never appears in a URL;
  `POST /api/login` remains an optional password fallback.
- **Wrapper ⇄ relay**: the WS handshake carries a machine credential. Manual
  setups use `WRAPPER_TOKEN` / `WRAPPER_TOKENS_JSON`; Device Center issues an
  independent, machine-bound, individually revocable credential. The relay
  stores only its hash, and no credential may announce another device's
  `machine_id`.
- **Android pager**: the embedded WebView shares the same browser session. The
  native bridge is restricted to the exact configured origin; external links
  open in the system browser without bridge access.
- Tokens travel only in cookies/headers, never in URLs or wire-protocol message
  bodies; logging redacts token/password fields.

---

## Reliability boundary

- The Web and TUI attach a stable `cmd_id` to retryable commands and resend them
  after a socket reconnect or wrapper recovery. The wrapper deduplicates them
  and ACKs completion within the same wrapper process lifetime.
- Unacknowledged-command queues and the general command-deduplication table are
  **bounded in-memory state**. A hard browser refresh, TUI exit, or wrapper
  crash does not promise cross-process exactly-once delivery.
- Persisted Claude transcripts and Codex rollouts are the history source of
  truth. The wrapper SQLite summary index and browser IndexedDB are rebuildable
  projections; the live ring only provides bounded reconnect catch-up.
- Work schedules are the exception: schedules, run records, leases, heartbeats,
  retries, and next-run timestamps live in SQLite.

---

## Operations

### Logs

- **Linux/macOS**: `journalctl -u cc-remote-wrapper -f` and
  `journalctl -u cc-remote-relay -f` (VPS); macOS uses `log show --predicate`.
- **Windows**: the packaged scheduled tasks write JSON logs under the install
  directory's `logs/` folder; `Get-Content -Wait logs\wrapper.log` works while
  developing.

### Health

- Relay: `curl <origin>/healthz` → `{"ok":true,"wrapper_connected":...}`.
- Wrapper log line `connected to relay` / `wrapper running`.
- Android pager: the dashboard banner shows bridge/wrapper status.

### Upgrade

1. Stop the wrapper (or let the packaged tasks handle a restart window).
2. Deploy the new Relay + Web bundle (VPS) and the new Wrapper (agent machine)
   in one maintenance window; hard-refresh open browsers.
3. On Windows, run the new installer over the existing install — configuration
   is preserved.

### Rollback

- Linux/macOS releases keep the previous release directory; the installer
  restores `current` and the service definition if activation fails.
- Windows keeps the previous install tree; `uninstall.ps1 -Restore <version>`
  can restore a prior snapshot.
- The Android APK blocks version-code downgrades; roll back the web bundle
  first, then publish a newer APK.

### Uninstall

- Windows: run `uninstall.ps1` from the install directory. It removes scheduled
  tasks, the LocalSubnet firewall rule, and the installed files, but **never
  deletes `~/.cc-remote`, Claude transcripts, Codex rollouts, or CLI
  credentials**.
- Linux/macOS: the release installer's `--uninstall` path removes the service
  and release symlinks, preserving device credentials and session sources.

### Common failure recovery

| Symptom | Recovery |
|---|---|
| `protocol v19` gate errors | Relay/Web/Wrapper versions are mixed. Update all tiers to the same release and hard-refresh. |
| Android pager shows no tasks | Log into the web chat once; the WebView owns the session. |
| Wrapper cannot find `claude`/`codex` | Set `CLAUDE_BIN` or adjust PATH; both CLIs must already be signed in. |
| Firewall blocks LAN access | Confirm the Windows rule is scoped to `LocalSubnet` on the selected port. |
| Connection resets in a loop | Check `RELAY_URL`/`PUBLIC_ORIGIN` match, then check logs for drain/timeouts. |

---

## Security (please read)

> **cc-remote lets a remote person run arbitrary commands on your machine. Treat it like handing someone a shell.**

- Code sessions remain a remote development control plane: Claude defaults to
  `permissionMode: bypassPermissions`, and Codex defaults to approval policy
  `never` while inheriting the machine's Codex sandbox configuration. **Treat
  anyone who can log in and enter Code as holding remote agent/shell authority
  on the wrapper machine.** Work uses a separate private root and does not
  expose external directories.
- `LOGIN_PASSWORD` / `LOGIN_USERS_JSON`, `WRAPPER_TOKEN` /
  `WRAPPER_TOKENS_JSON`, and `SESSION_SECRET` form the authentication boundary:
  use strong random values, never commit or paste them into chats, and rotate
  them. A repository `.env` is for local development only; production wrappers
  must use a root-only environment file.
- Always use TLS (`wss://`) in production. Only set `ALLOW_INSECURE_HTTP=1`
  for a temporary bare-public-IPv4 deployment.
- **LAN HTTP is still plaintext.** On a trusted LAN it is a convenience, not a
  security boundary. Anyone who can sniff the LAN can read login credentials and
  session content. Prefer the packaged firewall's `LocalSubnet` scoping and
  avoid exposing the relay beyond your LAN.
- Recommended: restrict the relay by IP / only run it when needed; login is
  rate-limited (5/min per IP) out of the box.

---

## Release maintainers

Versioning, release-tag validation, asset assembly, signing, and attestation
are documented in [docs/release-hardening/RELEASE_MAINTAINER.md](docs/release-hardening/RELEASE_MAINTAINER.md).
The single source of truth for versions and package defaults is
[`deploy/release-metadata.json`](deploy/release-metadata.json); run
`python -m deploy.validate_release_metadata` before any release.

---

## Development

```
cc_remote/
  protocol.py      # pydantic wire protocol (client/relay/wrapper all depend on it)
  config.py        # env-driven config
  relay/           # FastAPI relay: server / auth / pairing / forward
  wrapper/         # Claude SDK + Codex app-server / pool / stream / ringbuffer / transport
web/               # React client (Vite + TS)
android-native/    # Jetpack Compose pager + WebView shell
cc_portable_control/windows/ # reproducible Windows installer + portable archive
tests/             # zero-token unit tests + e2e scripts
deploy/            # release metadata, Caddyfile / systemd / setup-vps.sh / env examples
```

```bash
python -m pip install -r requirements-dev.txt
pytest                              # unit tests (no model, zero tokens)
npm --prefix web run test:reliability # pure web reliability tests
npm --prefix web run lint           # web static checks
npm --prefix web run build          # web production build

# Explicit live path (requires a running relay + wrapper and calls the model)
CC_REMOTE_RUN_E2E=1 CC_REMOTE_E2E_SCENARIO=smoke \
  RELAY_URL=wss://remote.example/ws LOGIN_PASSWORD='...' \
  pytest -q tests/test_e2e_entry.py
```

Architecture notes & contribution contracts are in [CLAUDE.md](CLAUDE.md).

---

## FAQ

- **Does restarting the wrapper lose history?** Persisted history does not
  disappear; it comes from Claude transcripts / Codex rollouts. A restart loses
  unacknowledged in-memory commands and the live ring.
- **Does restarting the relay drop the session?** It briefly disconnects and
  requires login again. The conversation remains intact on the wrapper machine.
- **Can I replace the VPS or move to a new device?** Yes. The VPS only serves
  the relay and static web bundle. Move the wrapper by copying transcripts,
  rollouts, Work roots, and cc-remote state, then re-authenticate the CLIs.
- **Do I need inbound ports?** No — the wrapper only dials out to the relay. On
  a LAN, the Windows machine must allow inbound connections on the selected
  relay port from the LAN subnet.
- **How expensive is it?** cc-remote itself has zero model cost; browsing /
  refreshing / viewing history spends no tokens.

## License

MIT — see [LICENSE](LICENSE). Upstream provenance and the pager adaptation are
documented in [docs/UPSTREAM.md](docs/UPSTREAM.md).
