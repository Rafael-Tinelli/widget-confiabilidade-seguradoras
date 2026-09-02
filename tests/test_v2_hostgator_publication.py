from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from api.v2.generation import BuildContext, SourceLineage, write_distribution_manifest
from api.v2.install_public_generation import verify_package

HEAD = "a" * 40
REMOTE_INSTALLER = Path("ops/hostgator/install_v2_public_remote.sh")
PYTHON_INSTALLER = Path("api/v2/install_public_generation.py")
PUBLICATION_WORKFLOW = Path(".github/workflows/v2-hostgator-publication.yml")
SCHEDULE_WORKFLOW = Path(".github/workflows/v2-production-generation-schedule.yml")


def _package(root: Path, build_id: str, marker: str) -> Path:
    package = root / build_id
    context = BuildContext.from_env(
        {
            "V2_SOURCE_HEAD_SHA": HEAD,
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1",
            "V2_BUILD_ID": build_id,
            "V2_GENERATED_AT": "2026-09-02T03:00:00Z",
        }
    )
    source = SourceLineage.from_mapping(
        {
            "source_id": "test_source",
            "source_url": "https://example.test/source",
            "fetched_at": "2026-09-02T02:59:00Z",
            "state": "fresh",
            "build_id": build_id,
            "sha256": "b" * 64,
            "snapshot_path": "data/cache/v2/source/test.json",
        }
    )
    for filename in (
        "search_index.json",
        "profile_manifest.json",
        "insurer_explorer.json",
        "explore_index.json",
    ):
        path = package / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"marker": marker}), encoding="utf-8")
    for relative in (
        "profiles/entity.json",
        "leaderboards/test.json",
        "collections/test.json",
    ):
        path = package / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"marker": marker}), encoding="utf-8")
    write_distribution_manifest(
        public_dir=package,
        context=context,
        sources=[source],
    )
    return package


def _package_sha(package: Path) -> str:
    payload = json.loads((package / "distribution_manifest.json").read_text(encoding="utf-8"))
    return str(payload["public_package"]["package_sha256"])


def _run_remote_installer(package: Path, target: Path, public_path: Path) -> None:
    subprocess.run(
        [
            "bash",
            str(REMOTE_INSTALLER),
            str(package),
            str(target),
            str(public_path),
            package.name,
            _package_sha(package),
            str(PYTHON_INSTALLER.resolve()),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_hostgator_remote_installer_keeps_public_path_atomic_and_rollback_ready(tmp_path: Path):
    source = tmp_path / "source"
    target = tmp_path / "publication"
    public_path = tmp_path / "public"
    os.symlink(target / "current", public_path)

    first = _package(source, "v2-build-1", "one")
    second = _package(source, "v2-build-2", "two")

    _run_remote_installer(first, target, public_path)
    assert public_path.resolve().name == "v2-build-1"
    assert verify_package(public_path)["build"]["build_id"] == "v2-build-1"

    _run_remote_installer(second, target, public_path)
    assert public_path.resolve().name == "v2-build-2"
    assert (target / "previous").resolve().name == "v2-build-1"
    assert verify_package(public_path)["build"]["build_id"] == "v2-build-2"

    retained_installer = target / "tools" / "install_public_generation.py"
    assert retained_installer.is_file()
    subprocess.run(
        ["python3", str(retained_installer), "rollback", str(target)],
        check=True,
        capture_output=True,
        text=True,
    )
    assert public_path.resolve().name == "v2-build-1"
    assert (target / "previous").resolve().name == "v2-build-2"


def test_hostgator_remote_installer_refuses_wrong_expected_package_hash(tmp_path: Path):
    package = _package(tmp_path / "source", "v2-build-1", "one")
    target = tmp_path / "publication"
    public_path = tmp_path / "public"
    os.symlink(target / "current", public_path)

    completed = subprocess.run(
        [
            "bash",
            str(REMOTE_INSTALLER),
            str(package),
            str(target),
            str(public_path),
            package.name,
            "0" * 64,
            str(PYTHON_INSTALLER.resolve()),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode != 0
    assert not (target / "current").exists()


def test_publication_workflow_uses_exact_successful_main_generation_only():
    workflow = PUBLICATION_WORKFLOW.read_text(encoding="utf-8")

    for required in (
        "V2 Gate 4 Full Generation Proof",
        "V2_HOSTGATOR_DEPLOY_ENABLED",
        "github.event.workflow_run.head_branch == 'main'",
        "source Full Generation did not conclude successfully",
        "HostGator publication accepts only a Full Generation from main",
        'gh run download "$SOURCE_RUN_ID"',
        "v2-gate4-full-generation-${source_run_id}-a${run_attempt}",
        "install_public_generation verify",
        "StrictHostKeyChecking=yes",
        "V2_HOSTGATOR_KNOWN_HOSTS",
    ):
        assert required in workflow

    assert "gh run list" not in workflow
    assert "latest successful" not in workflow.lower()


def test_production_scheduler_is_gated_and_triggers_canonical_workflow_on_main():
    workflow = SCHEDULE_WORKFLOW.read_text(encoding="utf-8")

    assert "V2_PRODUCTION_AUTOMATION_ENABLED" in workflow
    assert "v2-gate4-full-generation-proof.yml" in workflow
    assert '--ref main' in workflow
    assert "schedule:" in workflow
    assert "actions: write" in workflow
