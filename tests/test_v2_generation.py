from __future__ import annotations

import json
from pathlib import Path

import pytest

from api.v2.generation import (
    BuildContext,
    SourceLineage,
    build_distribution_manifest,
    load_source_lineage,
    write_distribution_manifest,
)

HEAD = "a" * 40
BUILD_ID = "v2-aaaaaaaaaaaa-123-a1"


def _context() -> BuildContext:
    return BuildContext.from_env(
        {
            "V2_SOURCE_HEAD_SHA": HEAD,
            "GITHUB_RUN_ID": "123",
            "GITHUB_RUN_ATTEMPT": "1",
            "V2_BUILD_ID": BUILD_ID,
            "V2_GENERATED_AT": "2026-08-30T18:00:00Z",
        }
    )


def _source(*, build_id: str = BUILD_ID, state: str = "fresh") -> SourceLineage:
    return SourceLineage.from_mapping(
        {
            "source_id": "susep_licensed_entities",
            "source_url": "https://example.test/susep",
            "fetched_at": "2026-08-30T17:59:00-00:00",
            "state": state,
            "build_id": build_id,
            "sha256": "b" * 64 if state != "unavailable" else None,
            "snapshot_path": "data/raw/susep/licensed.json" if state != "unavailable" else None,
        }
    )


def _write_json(path: Path, payload: object | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload or {"ok": True}), encoding="utf-8")


def _public_package(root: Path) -> Path:
    public = root / "public"
    for filename in (
        "search_index.json",
        "profile_manifest.json",
        "insurer_explorer.json",
        "explore_index.json",
    ):
        _write_json(public / filename)
    _write_json(public / "profiles" / "entity.json")
    _write_json(public / "leaderboards" / "largest_by_direct_premium.json")
    _write_json(public / "collections" / "favorable_joint_assessment.json")
    return public


def test_build_context_is_stable_for_same_run_and_attempt():
    context = _context()

    assert context.build_id == BUILD_ID
    assert context.source_head_sha == HEAD
    assert context.generated_at == "2026-08-30T18:00:00Z"
    assert context.workflow_run_id == "123"
    assert context.workflow_run_attempt == 1


def test_source_lineage_requires_hash_for_fresh_or_stale_sources():
    with pytest.raises(ValueError, match="requires content hash"):
        SourceLineage.from_mapping(
            {
                "source_id": "receita",
                "source_url": "https://example.test/receita",
                "fetched_at": "2026-08-30T18:00:00Z",
                "state": "stale",
                "build_id": BUILD_ID,
            }
        )


def test_source_lineage_rejects_cross_generation_mix(tmp_path: Path):
    path = tmp_path / "source_lineage.json"
    path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        **_source(build_id="v2-other").as_dict(),
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="build_id mismatch"):
        load_source_lineage(path, _context())


def test_distribution_manifest_hashes_complete_public_package(tmp_path: Path):
    public = _public_package(tmp_path)
    payload = build_distribution_manifest(
        public_dir=public,
        context=_context(),
        sources=[_source()],
    )

    assert payload["artifact"] == "v2_public_distribution_manifest"
    assert payload["build"]["build_id"] == BUILD_ID
    assert payload["build"]["source_head_sha"] == HEAD
    assert payload["source_lineage"]["state_counts"]["fresh"] == 1
    assert payload["public_package"]["files_count"] == 7
    assert len(payload["public_package"]["package_sha256"]) == 64
    assert payload["publication_policy"] == {
        "atomic_publish_required": True,
        "partial_publish_forbidden": True,
        "manifest_hash_verification_required": True,
        "retain_previous_generation_for_rollback": True,
    }


def test_distribution_manifest_rejects_incomplete_package(tmp_path: Path):
    public = _public_package(tmp_path)
    (public / "explore_index.json").unlink()

    with pytest.raises(ValueError, match="missing required files"):
        build_distribution_manifest(
            public_dir=public,
            context=_context(),
            sources=[_source()],
        )


def test_written_manifest_is_not_self_hashed(tmp_path: Path):
    public = _public_package(tmp_path)
    first = write_distribution_manifest(
        public_dir=public,
        context=_context(),
        sources=[_source()],
    )
    first_payload = json.loads(first.read_text(encoding="utf-8"))

    second = write_distribution_manifest(
        public_dir=public,
        context=_context(),
        sources=[_source()],
    )
    second_payload = json.loads(second.read_text(encoding="utf-8"))

    assert first_payload["public_package"]["package_sha256"] == second_payload[
        "public_package"
    ]["package_sha256"]
    assert all(
        item["path"] != "distribution_manifest.json"
        for item in second_payload["public_package"]["files"]
    )
