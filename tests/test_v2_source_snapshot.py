from __future__ import annotations

import json
from pathlib import Path

import pytest

from api.v2.generation import BuildContext, load_source_lineage
from api.v2.source_snapshot import (
    SourceObservation,
    build_source_lineage,
    write_source_lineage,
)

HEAD = "c" * 40
BUILD_ID = "v2-cccccccccccc-456-a1"


def _context() -> BuildContext:
    return BuildContext.from_env(
        {
            "V2_SOURCE_HEAD_SHA": HEAD,
            "GITHUB_RUN_ID": "456",
            "GITHUB_RUN_ATTEMPT": "1",
            "V2_BUILD_ID": BUILD_ID,
            "V2_GENERATED_AT": "2026-08-30T19:00:00Z",
        }
    )


def _observation(path: Path, *, succeeded: bool) -> SourceObservation:
    return SourceObservation(
        source_id="susep_ses_base_completa",
        source_url="https://www2.susep.gov.br/download/estatisticas/BaseCompleta.zip",
        snapshot_path=path,
        fetched_at="2026-08-30T18:55:00Z",
        current_fetch_succeeded=succeeded,
    )


def test_successful_current_fetch_is_fresh_and_hashed(tmp_path: Path):
    snapshot = tmp_path / "BaseCompleta.zip"
    snapshot.write_bytes(b"snapshot-current")

    lineage = _observation(snapshot, succeeded=True).to_lineage(_context())

    assert lineage.state == "fresh"
    assert lineage.freshness_method == "current_fetch"
    assert lineage.build_id == BUILD_ID
    assert lineage.snapshot_path == str(snapshot)
    assert lineage.sha256 is not None
    assert len(lineage.sha256) == 64


def test_current_validation_can_confirm_existing_snapshot_as_fresh(tmp_path: Path):
    snapshot = tmp_path / "receita.json"
    snapshot.write_text('{"records": []}', encoding="utf-8")

    lineage = SourceObservation(
        source_id="receita_cnpj_lifecycle",
        source_url="https://example.test/receita",
        snapshot_path=snapshot,
        fetched_at="2026-08-29T10:00:00Z",
        current_validation_succeeded=True,
        state_reason="official reference period and target hash unchanged",
    ).to_lineage(_context())

    assert lineage.state == "fresh"
    assert lineage.freshness_method == "current_validation"
    assert lineage.fetched_at == "2026-08-29T10:00:00Z"
    assert "target hash" in str(lineage.state_reason)


def test_failed_current_fetch_with_valid_snapshot_is_explicitly_stale(tmp_path: Path):
    snapshot = tmp_path / "BaseCompleta.zip"
    snapshot.write_bytes(b"snapshot-previously-validated")

    lineage = _observation(snapshot, succeeded=False).to_lineage(_context())

    assert lineage.state == "stale"
    assert lineage.freshness_method == "validated_cache_fallback"
    assert lineage.sha256 is not None
    assert lineage.snapshot_path == str(snapshot)


def test_failed_current_fetch_without_snapshot_is_unavailable(tmp_path: Path):
    snapshot = tmp_path / "missing.zip"

    lineage = _observation(snapshot, succeeded=False).to_lineage(_context())

    assert lineage.state == "unavailable"
    assert lineage.freshness_method == "unavailable"
    assert lineage.sha256 is None
    assert lineage.snapshot_path is None


def test_fetch_and_validation_cannot_both_claim_current_freshness(tmp_path: Path):
    snapshot = tmp_path / "source.bin"
    snapshot.write_bytes(b"source")

    observation = SourceObservation(
        source_id="source",
        source_url="https://example.test/source",
        snapshot_path=snapshot,
        fetched_at="2026-08-30T18:55:00Z",
        current_fetch_succeeded=True,
        current_validation_succeeded=True,
    )

    with pytest.raises(ValueError, match="both freshly fetched and cache-validated"):
        observation.to_lineage(_context())


def test_success_claim_without_snapshot_fails_closed(tmp_path: Path):
    snapshot = tmp_path / "missing.zip"

    with pytest.raises(ValueError, match="has no materialized snapshot"):
        _observation(snapshot, succeeded=True).to_lineage(_context())


def test_source_observations_must_have_unique_ids(tmp_path: Path):
    first = tmp_path / "first.bin"
    second = tmp_path / "second.bin"
    first.write_bytes(b"one")
    second.write_bytes(b"two")

    observations = [
        _observation(first, succeeded=True),
        _observation(second, succeeded=True),
    ]

    with pytest.raises(ValueError, match="duplicate source observation"):
        build_source_lineage(observations, _context())


def test_written_source_lineage_round_trips_into_distribution_contract(tmp_path: Path):
    snapshot = tmp_path / "BaseCompleta.zip"
    snapshot.write_bytes(b"snapshot-current")
    output = tmp_path / "source_lineage.json"

    write_source_lineage(output, [_observation(snapshot, succeeded=True)], _context())

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["artifact"] == "v2_source_lineage"
    assert payload["version"] == 2
    assert payload["build"]["build_id"] == BUILD_ID
    assert payload["state_counts"] == {"fresh": 1, "stale": 0, "unavailable": 0}
    assert payload["freshness_method_counts"] == {"current_fetch": 1}

    loaded = load_source_lineage(output, _context())
    assert len(loaded) == 1
    assert loaded[0].source_id == "susep_ses_base_completa"
    assert loaded[0].state == "fresh"
    assert loaded[0].freshness_method == "current_fetch"
