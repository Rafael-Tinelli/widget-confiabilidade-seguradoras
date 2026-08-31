import hashlib
import json
from pathlib import Path

import pytest

from api.v2.build_conduct_source_snapshot import (
    CONSUMER_MANIFEST_VERSION,
    ConductSourceSnapshotError,
    _cache_consumer_manifest,
    _cache_matches_current_inputs,
    _manifest_content_key,
    _materialize_consumer_manifest,
    _validate_consumer_manifest,
)


def test_consumer_manifest_content_key_ignores_provenance_timestamp() -> None:
    first = {
        "fetched_at": "2026-08-01T00:00:00Z",
        "aggregate": {"sha256": "a" * 64},
        "months": [
            {"month": "2026-07", "sha256": "b" * 64},
            {"month": "2026-08", "sha256": "c" * 64},
        ],
    }
    second = {**first, "fetched_at": "2026-08-30T00:00:00Z"}
    changed = {
        **first,
        "months": [
            {"month": "2026-07", "sha256": "b" * 64},
            {"month": "2026-08", "sha256": "d" * 64},
        ],
    }

    assert _manifest_content_key(first) == _manifest_content_key(second)
    assert _manifest_content_key(first) != _manifest_content_key(changed)


def _materialized_fixture(tmp_path: Path) -> dict:
    work = tmp_path / "work"
    aggregate = work / "consumidor_gov_agg.json.gz"
    aggregate.parent.mkdir(parents=True, exist_ok=True)
    aggregate.write_bytes(b"aggregate")
    months = []
    for index in range(12):
        path = work / "monthly" / f"consumidor_gov_2026-{index + 1:02d}.json.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"month-{index}".encode())
        months.append(
            {
                "month": f"2026-{index + 1:02d}",
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return {
        "artifact": "v2_consumer_gov_core_source_manifest",
        "version": CONSUMER_MANIFEST_VERSION,
        "aggregate": {
            "path": str(aggregate),
            "sha256": hashlib.sha256(aggregate.read_bytes()).hexdigest(),
        },
        "months": months,
    }


def test_consumer_cache_survives_fresh_workspace_materialization(tmp_path) -> None:
    materialized = _materialized_fixture(tmp_path)
    cache_dir = tmp_path / "cache"
    cached = _cache_consumer_manifest(
        materialized,
        cache_dir=cache_dir,
        fetched_at="2026-08-30T00:00:00Z",
    )
    manifest = cache_dir / "consumer_gov_core_manifest.json"
    manifest.write_text(json.dumps(cached), encoding="utf-8")
    validated = _validate_consumer_manifest(manifest)

    original_paths = [
        Path(materialized["aggregate"]["path"]),
        *(Path(item["path"]) for item in materialized["months"]),
    ]
    expected = {path: path.read_bytes() for path in original_paths}
    for path in original_paths:
        path.unlink()

    _materialize_consumer_manifest(validated)

    for path, content in expected.items():
        assert path.read_bytes() == content


def test_consumer_manifest_requires_fetched_at_version_and_valid_hashes(tmp_path) -> None:
    materialized = _materialized_fixture(tmp_path)
    cache_dir = tmp_path / "cache"
    payload = _cache_consumer_manifest(
        materialized,
        cache_dir=cache_dir,
        fetched_at="2026-08-30T00:00:00Z",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    assert _validate_consumer_manifest(manifest)["fetched_at"] == payload["fetched_at"]

    wrong_version = {**payload, "version": CONSUMER_MANIFEST_VERSION - 1}
    manifest.write_text(json.dumps(wrong_version), encoding="utf-8")
    with pytest.raises(ConductSourceSnapshotError, match="version"):
        _validate_consumer_manifest(manifest)

    payload.pop("fetched_at")
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConductSourceSnapshotError, match="fetched_at"):
        _validate_consumer_manifest(manifest)


def test_receita_cache_reuse_requires_current_schema_universe_and_provider_hashes(
    tmp_path,
) -> None:
    path = tmp_path / "receita.json"
    key = {
        "version": "v2-consumer-receita-identity-1",
        "reference_period": "2026-08",
        "target_universe_hash": "a" * 64,
        "unresolved_provider_hash": "b" * 64,
    }
    path.write_text(
        json.dumps({"meta": {"gate4_cache_key": key}}),
        encoding="utf-8",
    )

    assert _cache_matches_current_inputs(path, "a" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "c" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "a" * 64, "d" * 64)

    incompatible = {
        **key,
        "version": "v2-consumer-receita-identity-0",
    }
    path.write_text(
        json.dumps({"meta": {"gate4_cache_key": incompatible}}),
        encoding="utf-8",
    )
    assert not _cache_matches_current_inputs(path, "a" * 64, "b" * 64)
