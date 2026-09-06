from __future__ import annotations

import json
import re
from pathlib import Path

from cc_remote import __version__
from cc_remote.protocol import PROTOCOL_VERSION
from cc_remote.wrapper.codex_handle import _initialize_params
from deploy.release_metadata import load_release_metadata


ROOT = Path(__file__).resolve().parents[1]
METADATA = load_release_metadata(ROOT / "deploy" / "release-metadata.json")


def test_v3_product_version_is_consistent_across_runtime_and_web_metadata():
    assert __version__ == "3.0.0"
    assert METADATA.product_version == __version__
    assert re.fullmatch(r"[1-9]\d*\.\d+\.\d+", __version__)

    package = json.loads((ROOT / "web/package.json").read_text())
    package_lock = json.loads((ROOT / "web/package-lock.json").read_text())
    build_manifest = json.loads(
        (ROOT / "web/public/cc-remote-build.json").read_text()
    )

    assert package["version"] == __version__
    assert package_lock["version"] == __version__
    assert package_lock["packages"][""]["version"] == __version__
    assert build_manifest == {
        "version": __version__,
        "protocol": PROTOCOL_VERSION,
    }
    assert _initialize_params()["clientInfo"]["version"] == __version__
    installer = (ROOT / "deploy/install.sh").read_text()
    assert (
        f'VERSION="${{CC_REMOTE_VERSION:-{METADATA.distribution_version}}}"'
        in installer
    )
    assert (
        f'REPOSITORY="${{CC_REMOTE_GITHUB_REPOSITORY:-{METADATA.repository.slug}}}"'
        in installer
    )


def test_canonical_release_metadata_defines_the_pager_distribution():
    assert METADATA.distribution_version == "3.0.0-pager.18"
    assert METADATA.protocol == PROTOCOL_VERSION == 19
    assert METADATA.android.application_id == "dev.ccremote.lan"
    assert METADATA.android.version_code == 30027
    assert METADATA.android.version_name == METADATA.distribution_version
    assert METADATA.repository.slug == "hongjunmu79-debug/cc-remote"
    assert METADATA.release_tag == "v3.0.0-pager.18"


def test_release_docs_distinguish_product_and_wire_protocol_versions():
    readme = (ROOT / "README.md").read_text()
    readme_en = (ROOT / "README_en.md").read_text()
    changelog = (ROOT / "CHANGELOG.md").read_text()

    assert "当前版本：v3.0.0" in readme
    assert "## v3 架构升级" in readme
    assert "Current release: v3.0.0" in readme_en
    assert "## What changed in v3" in readme_en
    for document in (readme, readme_en, changelog):
        assert "v3.0.0" in document
        assert "protocol v19" in document.lower()
    assert METADATA.distribution_version in readme
    assert METADATA.distribution_version in readme_en


def test_readmes_use_safe_markdown_for_navigation_and_images():
    readme = (ROOT / "README.md").read_text()
    readme_en = (ROOT / "README_en.md").read_text()

    for document in (readme, readme_en):
        assert '<p align="center">' not in document
        assert "<img " not in document
        assert "<a href=" not in document
        assert document.count("](assets/") == 6

    assert "[English](README_en.md)" in readme
    assert "[中文](README.md)" in readme_en


def test_readmes_reference_the_real_repository_and_distribution_release():
    for document_name in ("README.md", "README_en.md"):
        document = (ROOT / document_name).read_text()
        assert f"github.com/{METADATA.repository.slug}" in document
        assert (
            f"github.com/{METADATA.repository.slug}/releases/download/"
            f"{METADATA.release_tag}"
        ) in document
        assert "muggle-stack/cc-remote" not in document
