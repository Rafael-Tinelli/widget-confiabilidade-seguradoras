from __future__ import annotations

import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

from api.v2.generation import BuildContext, SourceLineage, write_distribution_manifest
from api.v2.install_public_generation import (
    PublicGenerationInstallError,
    install_generation,
    rollback_generation,
    verify_package,
    verify_transport_archive,
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


@pytest.mark.parametrize("extra", [
    "rogue.php",
    "national.zip",
    "readme.txt",
    ".hidden",
    "profiles/rogue.json",
    "profiles/distribution_manifest.json",
    "nested/private.php",
])
def test_r4_package_rejects_every_unlisted_physical_file(
    tmp_path: Path, extra: str,
) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    original = verify_package(package)["public_package"]["package_sha256"]
    unexpected = package / extra
    unexpected.parent.mkdir(parents=True, exist_ok=True)
    unexpected.write_bytes(b"INERT TEST PAYLOAD; NOT EXECUTABLE")
    with pytest.raises(PublicGenerationInstallError, match="file set mismatch"):
        verify_package(package)
    # The audit's R4 allowed unchanged logical hash alongside an extra PHP/ZIP.
    # New verification must fail instead of trusting that original digest.
    assert json.loads(
        (package / "distribution_manifest.json").read_text(encoding="utf-8")
    )["public_package"]["package_sha256"] == original


def test_r4_rejects_unlisted_empty_directory(tmp_path: Path) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    (package / "unexpected-empty").mkdir()
    with pytest.raises(PublicGenerationInstallError, match="directory set mismatch"):
        verify_package(package)


@pytest.mark.parametrize("kind", ["file_symlink", "directory_symlink", "manifest_symlink", "hardlink", "fifo"])
def test_r4_package_rejects_links_and_special_types(
    tmp_path: Path, kind: str,
) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    if kind == "file_symlink":
        (package / "profiles" / "linked.json").symlink_to("entity.json")
    elif kind == "directory_symlink":
        (package / "aliased").symlink_to(package / "profiles", target_is_directory=True)
    elif kind == "manifest_symlink":
        real = package / "manifest-real.json"
        (package / "distribution_manifest.json").replace(real)
        (package / "distribution_manifest.json").symlink_to(real)
    elif kind == "hardlink":
        os.link(package / "search_index.json", tmp_path / "outside-file.json")
    else:
        os.mkfifo(package / "rogue-pipe")
    with pytest.raises(PublicGenerationInstallError, match="symlink|hardlink|special file"):
        verify_package(package)


def test_r4_untrusted_root_symlink_is_not_a_generation_pointer(
    tmp_path: Path,
) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    alias = tmp_path / "untrusted-alias"
    alias.symlink_to(package, target_is_directory=True)
    with pytest.raises(PublicGenerationInstallError, match="root symlink"):
        verify_package(alias)


def test_r4_installer_rejection_does_not_touch_current_or_previous(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = tmp_path / "published"
    first = _package(source, "v2-build-1", "one")
    second = _package(source, "v2-build-2", "two")
    third = _package(source, "v2-build-3", "three")
    install_generation(first, target)
    install_generation(second, target)
    assert _link_name(target / "current") == "v2-build-2"
    assert _link_name(target / "previous") == "v2-build-1"

    (third / "unlisted.php").write_text("<?php /* inert */ ?>", encoding="utf-8")
    with pytest.raises(PublicGenerationInstallError):
        install_generation(third, target)
    assert _link_name(target / "current") == "v2-build-2"
    assert _link_name(target / "previous") == "v2-build-1"
    assert not (target / "generations" / "v2-build-3").exists()

    # Reject a previously installed generation whose logical manifest stayed
    # unchanged but whose directory gained an unlisted ZIP.
    (target / "generations" / "v2-build-2" / "leaked.zip").write_bytes(b"test ZIP")
    with pytest.raises(PublicGenerationInstallError):
        install_generation(second, target)
    assert _link_name(target / "current") == "v2-build-2"
    assert _link_name(target / "previous") == "v2-build-1"


def test_r4_rollback_rejects_unlisted_file_in_previous_without_switching_links(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    target = tmp_path / "published"
    install_generation(_package(source, "v2-build-1", "one"), target)
    install_generation(_package(source, "v2-build-2", "two"), target)
    (target / "previous" / "rogue.php").write_text("inert", encoding="utf-8")
    with pytest.raises(PublicGenerationInstallError):
        rollback_generation(target)
    assert _link_name(target / "current") == "v2-build-2"
    assert _link_name(target / "previous") == "v2-build-1"


def _transport_archive(package: Path, archive_path: Path) -> None:
    with tarfile.open(archive_path, mode="w:gz") as archive:
        archive.add(package, arcname=".")


def test_r4_verifies_exact_real_transport_tar_and_cli(tmp_path: Path) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    transport = tmp_path / "out.tar.gz"
    _transport_archive(package, transport)
    assert verify_transport_archive(transport, package)["build"]["build_id"] == "v2-build-r4"
    completed = subprocess.run(
        [
            sys.executable, "-m", "api.v2.install_public_generation",
            "verify-archive", str(transport), str(package),
        ],
        check=True, capture_output=True, text=True,
    )
    assert json.loads(completed.stdout)["action"] == "verify-archive"


@pytest.mark.parametrize("kind", [
    "extra_php", "extra_zip", "extra_json", "nested_manifest",
    "missing_member", "changed_contents", "symlink", "hardlink",
    "fifo", "traversal", "duplicate",
])
def test_r4_rejects_tampered_or_augmented_transport_tar(
    tmp_path: Path, kind: str,
) -> None:
    package = _package(tmp_path / "source", "v2-build-r4", "safe")
    transport = tmp_path / "out.tar.gz"
    with tarfile.open(transport, mode="w:gz") as archive:
        if kind == "changed_contents":
            corrupted = tmp_path / "altered"
            shutil.copytree(package, corrupted)
            # Equal byte count, different SHA. The source manifest stays valid.
            victim = corrupted / "search_index.json"
            raw = victim.read_bytes()
            victim.write_bytes(raw.replace(b"safe", b"evil"))
            archive.add(corrupted, arcname=".")
        elif kind == "missing_member":
            archive.add(package / "search_index.json", arcname="./search_index.json")
        else:
            archive.add(package, arcname=".")
            if kind in ("extra_php", "extra_zip", "extra_json", "nested_manifest"):
                filename = {
                    "extra_php": "./rogue.php",
                    "extra_zip": "./rogue.zip",
                    "extra_json": "./profiles/rogue.json",
                    "nested_manifest": "./profiles/distribution_manifest.json",
                }[kind]
                payload = b"test-rogue"
                item = tarfile.TarInfo(filename)
                item.size = len(payload)
                archive.addfile(item, io.BytesIO(payload))
            elif kind in ("symlink", "hardlink", "fifo"):
                item = tarfile.TarInfo("./unexpected-special")
                item.type = {
                    "symlink": tarfile.SYMTYPE,
                    "hardlink": tarfile.LNKTYPE,
                    "fifo": tarfile.FIFOTYPE,
                }[kind]
                item.linkname = "./search_index.json"
                archive.addfile(item)
            elif kind == "traversal":
                payload = b"inert"
                item = tarfile.TarInfo("./../escape.php")
                item.size = len(payload)
                archive.addfile(item, io.BytesIO(payload))
            else:
                archive.add(
                    package / "search_index.json", arcname="./search_index.json",
                )
    with pytest.raises(PublicGenerationInstallError):
        verify_transport_archive(transport, package)
