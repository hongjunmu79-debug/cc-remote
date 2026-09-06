"""Canonical release metadata loader, validator, and static scans."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from deploy.release_metadata import (
    ReleaseMetadataError,
    load_release_metadata,
    repository_root,
)
from deploy.release_scan import (
    scan_forbidden_literals,
    scan_high_confidence_secrets,
)
from deploy.validate_release_metadata import (
    ReleaseValidationError,
    validate_release_metadata,
)


ROOT = Path(__file__).resolve().parents[1]
METADATA_PATH = ROOT / "deploy" / "release-metadata.json"


def test_canonical_metadata_values_match_the_pager_distribution():
    metadata = load_release_metadata(METADATA_PATH)
    assert metadata.product_version == "3.0.0"
    assert metadata.distribution_version == "3.0.0-pager.18"
    assert metadata.protocol == 19
    assert metadata.repository.slug == "hongjunmu79-debug/cc-remote"
    assert metadata.android.application_id == "dev.ccremote.lan"
    assert metadata.android.version_name == "3.0.0-pager.18"
    assert metadata.android.version_code == 30027
    assert metadata.release_tag == "v3.0.0-pager.18"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda raw: raw.__setitem__("protocol", "19"), "positive integer"),
        (lambda raw: raw.__setitem__("distribution_version", "3.0.0"), "extend"),
        (lambda raw: raw.__setitem__("android", None), "android"),
        (
            lambda raw: raw["android"].__setitem__("version_code", -1),
            "positive integer",
        ),
        (
            lambda raw: raw["android"].__setitem__("application_id", "com"),
            "application_id is invalid",
        ),
        (lambda raw: raw.__setitem__("schema", 2), "schema"),
    ],
)
def test_release_metadata_rejects_inconsistent_input(tmp_path, mutate, message):
    raw = json.loads(METADATA_PATH.read_text(encoding="utf-8"))
    mutate(raw)
    target = tmp_path / "release-metadata.json"
    target.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ReleaseMetadataError, match=message):
        load_release_metadata(target)


def test_repository_root_is_discoverable_from_nested_directories():
    assert repository_root(ROOT / "cc_remote" / "wrapper") == ROOT


def test_validate_release_metadata_passes_on_the_working_tree():
    validate_release_metadata(ROOT)


def test_validate_release_metadata_fails_on_drifted_install_sh(tmp_path):
    # Copy the repo tree shape that the validator inspects, then break the
    # install.sh default and confirm the gate refuses to publish.
    fake = tmp_path / "repo"
    for name in (
        "cc_remote/__init__.py",
        "cc_remote/protocol.py",
        "deploy/release-metadata.json",
        "deploy/install.sh",
        "web/package.json",
        "web/package-lock.json",
        "web/public/cc-remote-build.json",
        "android-native/app/build.gradle.kts",
        "README.md",
        "README_en.md",
    ):
        source = ROOT / name
        target = fake / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    fake_install = fake / "deploy" / "install.sh"
    fake_install.write_text(
        fake_install.read_text(encoding="utf-8").replace(
            'VERSION="${CC_REMOTE_VERSION:-3.0.0-pager.18}"',
            'VERSION="${CC_REMOTE_VERSION:-1.2.3}"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ReleaseValidationError, match="distribution_version"):
        validate_release_metadata(fake)


def test_forbidden_literal_scan_finds_machine_identity(tmp_path):
    source = tmp_path / "source.py"
    source.write_text(  # cc-remote-scan-allow: fixture deliberately exercising the forbidden literal
        "ip = 'http://192.168.3.4:8766/'\n",  # cc-remote-scan-allow: fixture
        encoding="utf-8",
    )
    (tmp_path / ".git").mkdir()
    violations = scan_forbidden_literals(tmp_path)
    assert any("192.168.3.4" in violation for violation in violations)  # cc-remote-scan-allow: assertion references the fixture literal


def test_secret_scan_finds_high_confidence_indicators(tmp_path):
    source = tmp_path / "secrets.txt"
    source.write_text(
        "token=ghp_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"  # cc-remote-scan-allow: fixture
        "-----BEGIN RSA PRIVATE KEY-----\nMII\n",  # cc-remote-scan-allow: fixture
        encoding="utf-8",
    )
    (tmp_path / ".git").mkdir()
    violations = scan_high_confidence_secrets(tmp_path)
    assert any("GitHub classic token" in violation for violation in violations)
    assert any("private key" in violation for violation in violations)


def test_secret_scan_honours_the_allow_marker(tmp_path):
    source = tmp_path / "fixture.txt"
    source.write_text(
        "sk-ant-fake-0123456789abcdef0123456789abcdef  # cc-remote-scan-allow\n",
        encoding="utf-8",
    )
    (tmp_path / ".git").mkdir()
    assert scan_high_confidence_secrets(tmp_path) == []


def test_validate_release_metadata_cli_prints_canonical_summary():
    result = subprocess.run(
        [sys.executable, "-m", "deploy.validate_release_metadata"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "3.0.0-pager.18" in result.stdout
    assert "hongjunmu79-debug/cc-remote" in result.stdout
