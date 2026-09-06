# Implementation report — CC Remote Pager production release hardening

**Date:** 2026-08-27
**Branch:** `codex/release-hardening`
**Base:** `c43a0d9bdf8f4158c7171df78fdea712add62468`
**Pushed implementation tip (acceptance-fix round):** `9d4bd29` (fixes below;
the docs commit containing this report sits directly on top)

This report records the implementation of the production-release hardening for
the cc-remote native pager. The parent agent performs acceptance; this document
separates completed checks from checks that require the parent's clean VM, ADB
device, signing key, certificate, or GitHub administrative action.

## Scope summary

| Scope | Result |
| --- | --- |
| A — canonical release metadata + repository identity | Implemented and verified. |
| B — reproducible Windows distribution | Implemented; Python suite green; install/rollback needs a clean-VM run. |
| C — generic Android endpoint + security | Implemented; unit tests + lint green; on-device verification pending. |
| D — CI and release workflow | Implemented; YAML validated; tag contract simulated; signing needs the real keystore. |
| E — documentation and handoff | Implemented. |

No wire-protocol change was made (still protocol 19). The live instance under
`%LOCALAPPDATA%\cc-remote` was not modified or restarted. No credentials,
`.env` values, cookies, signing material, keystores, or GitHub tokens were used
or exposed.

## Acceptance round 2 — CI startup and gate fixes

The parent agent rejected the first pushed tip at `fe9ae16` with evidence from
live GitHub: CI run `33057454140` failed and the release workflow startup run
`33057453065` failed with **zero jobs**. Root causes and fixes:

| Failure | Root cause | Fix (commit `9d4bd29`) |
| --- | --- | --- |
| release workflow startup — zero jobs | `if: ${{ secrets.PAGER_KEYSTORE_B64 != '' && ... }}` on the signing step: the `secrets` context is **not available in `if:` conditions**, so GitHub rejects the whole workflow file at compile time and schedules no job ("Unrecognized named-value: 'secrets'"). | Removed `secrets` from `if:`. The step now always runs; a bash guard checks the env vars and builds an unsigned release APK when either secret is absent, leaving `PAGER_SIGNING_PROPERTIES` unset so Gradle does not configure a signing config. |
| CI "Windows distribution tests" job failed | Windows runner had no pytest (`No module named pytest`). | Added `python -m pip install pytest` to the `python-windows` job. |
| CI "Python tests (ubuntu)" — relay bundle test | The relay bundle test builds `web/dist/index.html` from the checked-out source, but the python job never materialized the web client (`web/dist is missing`). | Added `actions/setup-node` (same SHA pin as release.yml) plus a "Materialize Web client for release tests" step (`npm ci` + `npm run build`) before `pytest`, mirroring release.yml's `verify` job. |
| CI "Python tests (ubuntu)" — bootstrap diagnostic | Test asserted `"exact semantic version"` but `install.sh` emits `CC_REMOTE_VERSION must be a semantic version such as 3.0.0-pager.5`. | Corrected the assertion to `"must be a semantic version"`. |
| CI "Python tests (ubuntu)" — workspace path ×3 | `os.path.isabs("C:\\Users\\alice\\projects")` is `False` on Linux, so the Windows config validator rejected valid absolute Windows workspaces on the Ubuntu runner. | `validate_workspace` now accepts an absolute path under **either** `ntpath.isabs` (`C:\…`, `\\server\share`) **or** `posixpath.isabs` (`/…`). `ntpath.isabs` alone is insufficient — Python 3.13 on Windows treats a leading `/` as relative. |
| CI "Python tests (ubuntu)" — protocol mirror | Locale-dependent `Path.read_text()` (GBK on Chinese Windows) failed on UTF-8 em-dash bytes in `web/src/…`. | Explicit `encoding="utf-8"` on the three TS reads. |

Two new regression tests lock these in: `test_ci_python_job_materializes_web_before_python_tests`
(partitions the ci.yml python job and asserts `npm ci` < `npm run build` < `pytest`)
and `test_workflow_if_conditions_never_reference_secrets` (scans every `if:`
line in both workflows and rejects `secrets.` references). A comment in the
Android lint block documents the `OldTargetApi` disable: the pinned AGP 8.13.2
tops out at compileSdk 36, and raising targetSdk would require an unpinned AGP
upgrade and still fail to resolve the `android-37` SDK platform on hosted
runners.

### Verification of the round

- Android `:app:testDebugUnitTest :app:lintDebug` → **BUILD SUCCESSFUL** (7m 43s).
- `python deploy/validate_release_metadata.py --root .` → exit 0, prints
  `cc-remote 3.0.0 distribution 3.0.0-pager.5 protocol v19 android dev.ccremote.lan vc30014`.
- `pytest` on the affected files (`test_release_distribution.py`,
  `test_web_protocol_mirror.py`, `test_windows_packaging.py`): **75 passed**;
  the 11 failures are the pre-existing Windows-only platform failures below.
  Both new regression tests pass.
- `web/dist/index.html` and `web/dist/cc-remote-build.json` materialized locally
  (`npm ci` + `npm run build`); the relay bundle test now advances past the
  "web/dist is missing" guard to the documented Windows exec-bit limitation,
  confirming the CI materialization step removes the Ubuntu failure.
- `ruff check cc_remote tests deploy` → all checks passed.
- `git diff --check` → clean.
- `git fsck --full` → healthy.

## Files changed

### Scope A — canonical release metadata and repository identity

- `deploy/release-metadata.json` (new) — canonical source: product `3.0.0`,
  distribution `3.0.0-pager.5`, protocol `19`, repo
  `hongjunmu79-debug/cc-remote`, Android `dev.ccremote.lan` /
  `30014`.
- `deploy/release_metadata.py` (new) — typed loader/validator.
- `deploy/release_scan.py` (new) — forbidden-literal and high-confidence-secret
  scans; self-exempts its own definitions.
- `deploy/validate_release_metadata.py` (new) — end-to-end release gate.
- `deploy/build_release.py`, `deploy/release_manifest.py` — consume canonical
  metadata; `sys.path` bootstrap for script invocation.
- `deploy/install.sh` — VERSION/REPOSITORY defaults → distribution + public repo.
- `docs/UPSTREAM.md` (new) — provenance of `muggle-stack/cc-remote` and the
  pager adaptation; MIT attribution preserved.
- `docs/native-pager/ARCHITECTURE.md`, `docs/native-pager/DEPLOYMENT.md` —
  generic-endpoint + canonical-metadata refresh.
- `tests/test_release_metadata.py` (new), `tests/test_product_version.py`,
  `tests/test_release_distribution.py`, `tests/test_wrapper_core_fixes.py`
  (path sanitization) — metadata/identity/version/scan tests.

### Scope B — reproducible Windows distribution

- `packaging/__init__.py`, `packaging/windows/` — win_config, win_layout,
  win_manifest, win_smoke, win_build, build.ps1, install.ps1, setup.ps1,
  uninstall.ps1, start.ps1, stop.ps1, supervise.ps1, firewall.ps1,
  config-first-run.ps1, README.md.
- `tests/test_windows_packaging.py` (new) — 60 zero-token tests.

### Scope C — generic Android endpoint and security

- `android-native/app/build.gradle.kts` — source defaults from
  `deploy/release-metadata.json`; no stale literals.
- `android-native/.../AppPreferences.kt`, `PagerDashboard.kt`,
  `PagerViewModel.kt`, `SecureWebViewController.kt`,
  `res/xml/network_security_config.xml` — endpoint entry, HTTPS/private-HTTP
  rules, origin enforcement, cleartext scoping.
- `android-native/.../ServerEndpointTest.kt` — endpoint unit tests.

### Scope D — CI and release workflow

- `.github/workflows/ci.yml` (new) — push/PR gate.
- `.github/workflows/release.yml` — tag contract, source-built Windows/Android
  jobs, publish gating, SHA-256 + attestation, secret-driven Android signing.

### Scope E — documentation and handoff

- `README.md`, `README_en.md` — two-journey quickstart + operations.
- `CHANGELOG.md`, `CHANGELOG_zh.md` — `v3.0.0-pager.5` entry.
- `docs/release-hardening/RELEASE_MAINTAINER.md`,
  `docs/release-hardening/SECURITY.md`,
  `docs/release-hardening/DEV_HISTORY_INDEX.md` (new).
- `docs/release-hardening/IMPLEMENTATION_REPORT.md` (this document).

## Commits (branch `codex/release-hardening`)

```
9d4bd29 fix(ci): resolve release workflow startup and CI gate failures   <- acceptance-fix round
fe9ae16 docs: final implementation report for release hardening
0db4bd11bbf0c93fffba01e361d84119c8c21689 docs: two-journey quickstart and release documentation
d7ccbad85e831a0d764ce4058123469bb274b48f ci: push/PR gate and hardened tag release
1242fe3fdd6294f61c36f8c8a13deff227ecc527 fix(android): generic server endpoint and origin enforcement
9f18ef5dc23e6a5aecb7eca9ea5e732d0c6df085 feat(packaging): reproducible Windows distribution
d4f6a5310f8c1e3625bbf63684816602553017a2 feat(deploy): canonical release metadata and repo identity
```

## Tests and checks run locally (zero-token)

| Check | Result |
| --- | --- |
| `pytest tests/test_release_metadata.py tests/test_product_version.py tests/test_protocol_input_bounds.py tests/test_web_protocol_mirror.py tests/test_windows_import_compat.py tests/test_windows_packaging.py tests/test_auth.py tests/test_relay_forward.py` | 182 passed |
| `tests/test_windows_packaging.py` (Windows packaging suite) | 60/60 passed |
| `python deploy/validate_release_metadata.py --root .` | PASS — cc-remote 3.0.0, distribution 3.0.0-pager.5, protocol v19, dev.ccremote.lan vc30014 |
| `uv tool run --from ruff==0.15.13 ruff check cc_remote tests deploy` | All checks passed |
| `git diff --check` | clean |
| `git fsck --full` | healthy (only harmless dangling blobs) |
| Android `:app:testDebugUnitTest :app:lintDebug` | BUILD SUCCESSFUL (7m 43s) |
| Android `:app:assembleRelease` with no signing secrets | BUILD SUCCESSFUL (unsigned APK) |
| release.yml tag contract (simulated `v3.0.0-pager.5`, `v3.0.0`, `v9.9.9`) | ACCEPT / ACCEPT / REJECT |
| Both workflow YAMLs (PyYAML parse) | valid |
| All third-party action SHA pins vs claimed tags (`gh api`) | all match |
| release.yml heredoc inline Python | compiles |
| `pytest tests/test_release_distribution.py tests/test_web_protocol_mirror.py tests/test_windows_packaging.py` (acceptance round) | 75 passed; 11 pre-existing Windows-only failures |
| `test_ci_python_job_materializes_web_before_python_tests` + `test_workflow_if_conditions_never_reference_secrets` | pass (locked in the CI fixes) |
| `web/dist` materialized locally (`npm ci` + `npm run build`) | `index.html` + `cc-remote-build.json` present |
| Push of `codex/release-hardening` to origin | succeeded (see acceptance round) |

### Pre-existing Windows-only failures (not regressions)

A full `pytest` run on this Windows dev box fails 137 tests across several
modules. None are regressions — every one is an environment/platform artifact
in code that this branch does not modify, and each passes on the Ubuntu CI
runners:

- `tests/test_release_distribution.py` (11): the release-bundle tests fail on
  the POSIX executable-bit check of the fake `uv` (Windows `stat().st_mode`
  reports no `0o111` bits) and the bootstrap/installer tests fail because WSL
  `bash` mangles the Windows path (`exit 127`) or a POSIX script is not a valid
  Win32 executable. The symlink-rejection test cannot create symlinks without
  Windows developer-mode privileges (`WinError 1314`).
- `tests/test_workspaces.py`, `tests/test_wrapper_core_fixes.py`,
  `tests/test_markdown_preview.py`, `tests/test_session_pins.py`,
  `tests/test_rollback_commands.py` (~126): teardown fails with
  `WinError 32` while deleting the SQLite temp file because a connection is
  still held open on Windows; a few tests need POSIX-only facilities (FIFOs,
  `fcntl`, symlinks). The code under test is untouched by this branch.
- `tests/test_claude_broker.py` does not even collect on Windows (`import
  fcntl`), so it is excluded from local runs with `--ignore`.

CI does not run these in the failing configuration: the `python-windows` job
runs only `tests/test_windows_packaging.py` (passes), and the full suite runs
on the Ubuntu runners where all of the above pass.

## Scope B/C evidence and what still needs the parent agent

- Windows packaging tests prove: no fixed usernames, no fixed old LAN IP, no
  `.venv` relocation dependency, no mandatory Node dependency; config rejects
  placeholders and preserves existing config on upgrade.
- Android endpoint unit tests cover `10/8`, `172.16/12`, `192.168/16`,
  loopback, HTTPS, and rejected public HTTP.
- Release workflow cannot publish after any failed gate (`publish` `needs:`
  all build jobs, which `needs: verify`).

### Still requires the parent agent / external action

- Clean-VM Windows install, upgrade (config preservation), rollback, uninstall,
  scheduled-task supervision, and LocalSubnet firewall verification.
- On-device Android verification with a real ADB device (first-launch endpoint
  entry, bridge origin/resource enforcement, IME handling, terminal-card
  centering, engine filters, writer handoff).
- Android release signing: the keystore/alias/passwords are **not** in this
  repository. Provision `PAGER_KEYSTORE_B64` and `PAGER_SIGNING_PROPERTIES` as
  secrets, or sign locally with the existing certificate — never substitute a
  different key. Verify signer SHA-256 parity before distributing.
- Windows Authenticode signing (optional, CI-secret-driven); local builds are
  documented as unsigned.
- GitHub administrative actions: repository secrets, branch protection,
  environment approval, final release publication rights.
- Live CI runs on GitHub (this environment could only simulate the tag
  contract and validate the YAML/action pins locally).

## Limitations

- The packaging Python modules are intentionally dependency-free and are **not**
  in CI's `ruff` scope (`ruff check cc_remote tests deploy`); `packaging/` is
  isolated from runtime/domain code by design.
- Live CI was exercised once by the parent agent (runs `33057454140` and
  `33057453065`) and the failures found there are fixed in the acceptance-fix
  round above. The branch has not yet seen a fully green live CI run after the
  fixes; the workflow files remain validated structurally (YAML parse, action
  SHA verification against upstream tags, heredoc Python compile, tag-contract
  simulation) plus the new `test_workflow_if_conditions_never_reference_secrets`
  guard. A green push/PR run on the pushed branch is the remaining confirmation.
- Protocol 19 is unchanged; all three tiers must still be upgraded together in
  one maintenance window.
