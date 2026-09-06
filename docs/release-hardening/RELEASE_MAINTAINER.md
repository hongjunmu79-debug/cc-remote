# Release maintainer guide

This document describes how a maintainer cuts a cc-remote native-pager release.
It covers the canonical version source, the tag contract, the CI/build
pipeline, asset signing, checksums/attestations, rollback, and the pre-flight
checklist. Security guidance for operators lives in
[`SECURITY.md`](SECURITY.md); the two user journeys live in the
[`README`](../../README.md).

## Version and identity source of truth

Everything that varies per release lives in one file:
[`deploy/release-metadata.json`](../../deploy/release-metadata.json).

| Key | Current value | Meaning |
| --- | --- | --- |
| `product_version` | `3.0.0` | Base cc-remote backend/web codebase version. |
| `distribution_version` | `3.0.0-pager.8` | Android-pager release line; what release tags and installers target. |
| `protocol` | `19` | Wire protocol. Unchanged by the pager distribution. |
| `repository.owner/name` | `hongjunmu79-debug/cc-remote` | Public repository identity used by URLs and install scripts. |
| `android.application_id` | `dev.ccremote.lan` | Android package id; do not change (signing identity). |
| `android.version_name` | `3.0.0-pager.8` | Mirrors the distribution version. |
| `android.version_code` | `30017` | Android version code; must strictly increase for every APK. |

Build scripts and CI consume this file; `python -m deploy.validate_release_metadata`
asserts the backend, web build manifest, `install.sh` defaults, Android Gradle
defaults, READMEs, and repository-identity scans all agree with it. **Never edit
a version in more than one place.** Change `release-metadata.json`, then run the
validator.

## Release tag contract

The only tag that may publish a release is the canonical distribution tag
(`v3.0.0-pager.8`, derived from `deploy/release-metadata.json` →
`distribution_version`). The workflow triggers only on pre-release-shaped tags
(`v*.*.*-*`) and its `verify` job then rejects anything except the canonical
distribution tag. The bare product tag `v3.0.0` is **not** a trigger pattern and
is rejected, so it can never become a release. A tag push of any other version
fails the `verify` job and cannot publish. The tag must also match
`web/package.json`, `web/package-lock.json`, and the backend `__version__`.

Use the canonical distribution tag for user-facing releases so installers and
Android `version_name` line up.

## Workflows

### `ci.yml` — push/PR gate

Runs on every push to any branch and on pull requests. It is the fast, no-publish
gate: canonical metadata/identity/secret scan, Python tests + Ruff on Ubuntu,
Windows packaging tests on `windows-2025`, web build/reliability/lint, and
Android unit tests + lint (JDK 17). All actions are SHA-pinned. A PR that would
fail a release is caught here first.

### `release.yml` — tag release and manual validation

Triggered by pre-release-shaped distribution tag pushes (`v*.*.*-*`), or
manually via `workflow_dispatch` for a no-publish build validation. Strictly
gated: `publish` `needs: [build, build-windows, build-android]`, and each build
job `needs: verify`. **A failure in any gate makes publication impossible.**

| Job | What it does |
| --- | --- |
| `verify` | Runs the canonical metadata validator, the tag/version contract check, full Python + web tests/lint, `shellcheck`, `bash -n`, and `git diff --check`. |
| `build` (matrix) | Builds deterministic Linux/macOS role bundles (`relay`/`wrapper` × `x86_64`/`arm64`) from source with `deploy.build_release`, then installs the bundle into a scratch venv and imports the packaged role. |
| `build-windows` | Builds exactly three Windows release artifacts with `packaging\windows\build.ps1` (source-built `web/dist`, no shipped `.venv`, no Node LAN proxy): the installer/bootstrap zip, the portable zip, and a genuine Inno Setup installer `.exe`, each with its own `.sha256` sidecar. ISCC (Inno Setup) is a hard dependency — if it is unavailable the build fails closed instead of emitting a fake installer. |
| `build-android` | Runs Android unit tests + `lintDebug`, then `assembleRelease`. A tag push is **fail-closed**: both `PAGER_KEYSTORE_B64` and `PAGER_SIGNING_PROPERTIES` must be present and the assembled APK's signer SHA-256 must equal the canonical fingerprint in `deploy/release-metadata.json`, otherwise the job fails and nothing is published. A manual `workflow_dispatch` run may assemble an unsigned APK for validation only — it has no publish path. |
| `publish` | Downloads all build artifacts, assembles `SHA256SUMS`, runs `actions/attest` for attestations, and creates/uploads/publishes the GitHub Release. Refuses to replace an already-published release. Runs only on tag pushes. |

## Building release assets from source

- **Linux/macOS bundles:** built by `deploy/build_release.py` in CI. They embed a
  pinned `uv`, the hashed role lockfile, the MIT license, and the release
  manifest. Byte-identical for the same git SHA + `SOURCE_DATE_EPOCH` + sources.
- **Windows:** built by `cc_portable_control\windows\build.ps1` → two deterministic zips
  (installer/bootstrap and portable) + one genuine Inno Setup installer `.exe`,
  each with a `.sha256` sidecar, all assembled from one staged, smoke-verified
  payload. The installer build fails closed if ISCC (Inno Setup) is absent.
  See [`cc_portable_control/windows/README.md`](../../cc_portable_control/windows/README.md).
- **Android APK:** built by Gradle in `android-native/`. Release assets come
  from CI only — do not manually upload a machine-built APK.

## Signing

### Android

The release APK must keep the existing signing identity (package
`dev.ccremote.lan`, same signer SHA-256 digest). `assembleRelease` reads signing
configuration from a properties file whose path is exported as
`PAGER_SIGNING_PROPERTIES`; CI decodes the keystore from the
`PAGER_KEYSTORE_B64` secret and co-locates both under `$RUNNER_TEMP/pager-keys/`.

> **Workflow gotcha:** the `secrets` context is **not available in `if:`
> conditions**. Referencing it there makes GitHub fail the whole run at startup
> with `Unrecognized named-value: 'secrets'` and schedules zero jobs. The
> signing step always runs and the bash guard decides from the
> `PAGER_KEYSTORE_B64` / `PAGER_SIGNING_PROPERTIES` environment variables. A tag
> push with either secret absent **fails the job closed** — an unsigned release
> APK is never assembled and publication is impossible. Only a manual
> `workflow_dispatch` validation run may assemble an unsigned APK, and that run
> has no publish path.

> **External dependency:** the keystore, alias, and passwords are not in this
> repository and cannot be reconstructed here. They must be provisioned as
> repository/environment secrets. If that signing material is unavailable to CI,
> a tag release **cannot publish at all** — the job fails closed and nothing is
> uploaded, so an unsigned APK can never be shipped from a tag push. Provision
> the secrets first, then push the tag. Manual validation builds are unsigned
> by design and must never be distributed. Never substitute a different key.

Before distributing an APK, verify with Android SDK `apksigner verify --print-certs`
that the previous and new APKs have identical signer SHA-256 digests, the
application id is `dev.ccremote.lan`, and the version code strictly increased.

### Windows

Authenticode signing is **optional and CI-secret-driven**. The genuine Inno
Setup installer `.exe` is the primary Authenticode target; the `.zip` packages
are integrity-protected by their `.sha256` sidecars and release attestations,
not by code signing. The workflow builds and verifies all Windows artifacts
unsigned. If a real code-signing certificate is later provisioned as a secret,
a separately designed CI step may sign the installer `.exe` — and, separately,
scripts such as `setup.ps1` — but no such step exists today, so current
artifacts ship unsigned. Never commit or invent a certificate. Local builds are
explicitly unsigned and `win_smoke` documents that in the distribution
manifest/notes.

## Checksums and attestations

`publish` computes `SHA256SUMS` over every release asset (`cc-remote-*.tar.gz`,
`cc-remote-*.zip`, `cc-remote-*.zip.sha256`, `cc-remote-*.exe`,
`cc-remote-*.exe.sha256`, `*.apk`, `install.sh`) and attaches `actions/attest`
provenance. Users verify downloads with `sha256sum -c` / `shasum -a 256 -c`.

## Version bump procedure

1. On `main`, bump `deploy/release-metadata.json` (and, when it changes, the
   backend `cc_remote/__init__.py`, `web/package.json`,
   `web/package-lock.json`, and `web/public/cc-remote-build.json`).
2. Run `python deploy/validate_release_metadata.py --root .` and the full
   zero-token test suite.
3. Open the PR through `ci.yml`. Merge to `main`.
4. Tag the merge commit with the distribution tag (`v3.0.0-pager.N`), then
   push the tag. `release.yml` handles the rest.

## Rollback

- **Linux/macOS:** each release keeps the previous immutable release directory
  and `current` symlink; a failed activation restores `current` and the service
  definitions.
- **Windows:** `uninstall.ps1 -Rollback` restores the previous release tree
  recorded in `releases\current.json`; configuration is preserved.
- **Android:** Android blocks version-code downgrades. Roll the web/relay/wrapper
  back first, then publish an APK with a higher version code. See
  [`docs/native-pager/DEPLOYMENT.md`](../native-pager/DEPLOYMENT.md).

## Pre-release checklist

- [ ] `deploy/release-metadata.json` is the only place versions changed, and it
      matches the intended tag.
- [ ] `python deploy/validate_release_metadata.py --root .` passes.
- [ ] `pytest`, `npm --prefix web run test:reliability`, `npm --prefix web run lint`,
      `npm --prefix web run build`, and `tests/test_windows_packaging.py` all pass.
- [ ] Android `./gradlew testDebugUnitTest lintDebug` pass.
- [ ] `git diff --check` passes and `git fsck` is healthy.
- [ ] Release tag pushed; `release.yml` runs `verify` → all `build*` jobs →
      `publish`; no gate fails.
- [ ] GitHub Release contains all assets + `SHA256SUMS`, attestation succeeded,
      and the APK is signed with the real certificate.

## What still requires a human / external action

These cannot be verified inside CI and need the parent acceptance owner or a
maintainer:

- A clean-VM install/upgrade/rollback run of all three Windows artifacts: the
  installer/bootstrap zip, the portable zip, and the genuine Inno Setup
  installer `.exe`.
- On-device Android verification (bridge behavior, endpoint entry, IME, WebView
  enforcement) with a real ADB device.
- Android keystore / signing-password provisioning and a signed-APK verify.
- Windows Authenticode certificate provisioning, if used.
- GitHub repository administration (branch protection, secrets, environment
  approval, release publication rights).
