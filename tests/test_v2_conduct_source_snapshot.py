import json

import pytest

from api.v2.build_conduct_source_snapshot import (
    ConductSourceSnapshotError,
    _cache_matches_current_inputs,
    _manifest_content_key,
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


def test_consumer_manifest_requires_fetched_at_and_valid_hashes(tmp_path) -> None:
    aggregate = tmp_path / "aggregate.gz"
    aggregate.write_bytes(b"aggregate")
    months = []
    for index in range(12):
        path = tmp_path / f"month-{index:02d}.gz"
        path.write_bytes(f"month-{index}".encode())
        months.append(
            {
                "month": f"2026-{index + 1:02d}",
                "path": str(path),
            }
        )

    import hashlib

    payload = {
        "artifact": "v2_consumer_gov_core_source_manifest",
        "version": 1,
        "fetched_at": "2026-08-30T00:00:00Z",
        "aggregate": {
            "path": str(aggregate),
            "sha256": hashlib.sha256(aggregate.read_bytes()).hexdigest(),
        },
        "months": [
            {
                **item,
                "sha256": hashlib.sha256(
                    open(item["path"], "rb").read()  # noqa: PTH123, SIM115
                ).hexdigest(),
            }
            for item in months
        ],
    }
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    assert _validate_consumer_manifest(manifest)["fetched_at"] == payload["fetched_at"]

    payload.pop("fetched_at")
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConductSourceSnapshotError, match="fetched_at"):
        _validate_consumer_manifest(manifest)


def test_receita_cache_reuse_requires_current_universe_and_provider_hashes(tmp_path) -> None:
    path = tmp_path / "receita.json"
    path.write_text(
        json.dumps(
            {
                "meta": {
                    "gate4_cache_key": {
                        "version": "v2-consumer-receita-identity-1",
                        "reference_period": "2026-08",
                        "target_universe_hash": "a" * 64,
                        "unresolved_provider_hash": "b" * 64,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    assert _cache_matches_current_inputs(path, "a" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "c" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "a" * 64, "d" * 64)
