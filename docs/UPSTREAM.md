# Upstream provenance

This repository (`hongjunmu79-debug/cc-remote`) is a production
hardening of the **cc-remote** remote-control plane, adapted for the native
Android pager distribution.

## Upstream project

- **Project:** [cc-remote](https://github.com/muggle-stack/cc-remote)
- **Author:** muggle
- **License:** MIT — see [LICENSE](../LICENSE)

The upstream project is the self-hosted remote control plane for Claude Code
and Codex: a local `wrapper` drives the already-installed CLI, and browsers
view/control sessions through a self-hosted WebSocket relay. The core concepts
in `AGENTS.md`, `CLAUDE.md`, the wire protocol in `cc_remote/protocol.py`, the
relay/wrapper architecture, and the web client all originate there and remain
MIT-licensed.

## The pager adaptation

This repository adds a production-ready Android distribution on top of the
upstream control plane, while keeping the shared backend and web client
backward-compatible with upstream cc-remote:

- **Native Android pager** (`android-native/`): a bounded Jetpack Compose
  projection of the web client's session state. One WebView still owns
  authentication, the WebSocket, and the chat surface; the native dashboard
  reads a versioned, exact-origin WebMessage bridge and never duplicates the
  session state machine. Package `dev.ccremote.lan`.
- **Windows packaging** (`cc_portable_control/windows/`): a reproducible Windows
  installer and portable archive with first-run configuration, supervised
  scheduled tasks, and a LocalSubnet firewall rule. No Node LAN proxy and no
  machine-created `.venv` are shipped.
- **Canonical release metadata** (`deploy/release-metadata.json`): one source
  of truth for the product/distribution versions, protocol, repository
  identity, and Android package defaults consumed by every build tier.

The pager adaptation does not change the wire protocol (still v19), the
model link (cc-remote never touches model credentials or the model API), or
the authentication boundaries of the upstream project.

## Attribution

The upstream MIT copyright and license notice are preserved in the repository
root `LICENSE`. New files contributed in this repository follow the same MIT
license. The native-pager architecture notes in
[`docs/native-pager/ARCHITECTURE.md`](native-pager/ARCHITECTURE.md) describe
which behavior is original to this adaptation.
