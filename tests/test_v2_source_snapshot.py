from __future__ import annotations

from pathlib import Path

import pytest

from api.v2.generation import BuildContext
from api.v2.source_snapshot import SourceObservation, build_source_lineage

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
    assert lineage.build_id == BUILD_ID
    assert lineage.snapshot_path == str(snapshot)
    assert lineage.sha256 is not None
    assert len(lineage.sha256) == 64


def test_failed_current_fetch_with_valid_snapshot_is_explicitly_stale(tmp_path: Path):
    snapshot = tmp_path / "BaseCompleta.zip"
    snapshot.write_bytes(b"snapshot-previously-validated")

    lineage = _observation(snapshot, succeeded=False).to_lineage(_context())

    assert lineage.state == "stale"
    assert lineage.sha256 is not None
    assert lineage.snapshot_path == str(snapshot)


def test_failed_current_fetch_without_snapshot_is_unavailable(tmp_path: Path):
    snapshot = tmp_path / "missing.zip"

    lineage = _observation(snapshot, succeeded=False).to_lineage(_context())

    assert lineage.state == "unavailable"
    assert lineage.sha256 is None
    assert lineage.snapshot_path is None


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
