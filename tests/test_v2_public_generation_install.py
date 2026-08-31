from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from api.v2.generation import BuildContext, SourceLineage, write_distribution_manifest
from api.v2.install_public_generation import (
    PublicGenerationInstallError,
    install_generation,
    rollback_generation,
    verify_package,
)

HEAD = "a" * 40


def _context(build_id: str) -> BuildContext:
    return BuildContext.from_env(
        {
            "V2_SOURCE_HEAD_SHA": HEAD,
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1",
            "V2_BUILD_ID": build_id,
            "V2_GENERATED_AT": "2026-08-31T12:00:00Z",
        }
    )


def _source(build_id: str) -> SourceLineage:
    return SourceLineage.from_mapping(
        {
            "source_id": "consumer_gov_core",
            "source_url": "https://example.test/consumer",
            "fetched_at": "2026-08-31T11:59:00Z",
            "state": "fresh",
            "build_id": build_id,
            "sha256": "b" * 64,
            "snapshot_path": "data/cache/v2/conduct/manifest.json",
        }
    )


def _write_json(path: Path, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"marker": marker}), encoding="utf-8")


def _package(root: Path, build_id: str, marker: str) -> Path:
    package = root / build_id
    for filename in (
        "search_index.json",
        "profile_manifest.json",
        "insurer_explorer.json",
        "explore_index.json",
    ):
        _write_json(package / filename, marker)
    _write_json(package / "profiles" / "entity.json", marker)
    _write_json(package / "leaderboards" / "largest.json", marker)
    _write_json(package / "collections" / "favorable.json", marker)
    write_distribution_manifest(
        public_dir=package,
        context=_context(build_id),
        sources=[_source(build_id)],
    )
    return package


def _link_name(path: Path) -> str:
    assert path.is_symlink()
    return (path.parent / os.readlink(path)).resolve().name


def test_verify_package_rejects_post_manifest_tampering(tmp_path: Path):
    package = _package(tmp_path / "source", "v2-build-1", "one")
    assert verify_package(package)["build"]["build_id"] == "v2-build-1"

    (package / "search_index.json").write_text("{}", encoding="utf-8")
    with pytest.raises(PublicGenerationInstallError, match="byte size mismatch|sha256 mismatch"):
        verify_package(package)


def test_install_switches_current_atomically_and_keeps_previous(tmp_path: Path):
    source = tmp_path / "source"
    target = tmp_path / "installed"
    first = _package(source, "v2-build-1", "one")
    second = _package(source, "v2-build-2", "two")

    first_result = install_generation(first, target)
    assert first_result["build_id"] == "v2-build-1"
    assert _link_name(target / "current") == "v2-build-1"
    assert not (target / "previous").exists()

    second_result = install_generation(second, target)
    assert second_result["build_id"] == "v2-build-2"
    assert _link_name(target / "current") == "v2-build-2"
    assert _link_name(target / "previous") == "v2-build-1"
    assert verify_package(target / "current")["build"]["build_id"] == "v2-build-2"


def test_rollback_swaps_current_and_previous_verified_generations(tmp_path: Path):
    source = tmp_path / "source"
    target = tmp_path / "installed"
    first = _package(source, "v2-build-1", "one")
    second = _package(source, "v2-build-2", "two")
    install_generation(first, target)
    install_generation(second, target)

    result = rollback_generation(target)

    assert result["build_id"] == "v2-build-1"
    assert result["replaced_build_id"] == "v2-build-2"
    assert _link_name(target / "current") == "v2-build-1"
    assert _link_name(target / "previous") == "v2-build-2"
    assert verify_package(target / "current")["build"]["build_id"] == "v2-build-1"
