"""Offline regression tests for the manual, nonpublishing A09 official rebuild."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from api.v2.build_conduct_source_snapshot import _receita_cache
from scripts.a09_validate_rebuild_proof import A09ProofError, verify_rebuild_proof


def _events() -> list[dict]:
    events = []
    for i, size in ((0, 100), (1, 200)):
        events.append({
            "event": "zip_download", "category": "estabelecimentos",
            "file_index": i, "received_bytes": size,
            "elapsed_download_s": 10 + i, "state": "success",
        })
        events.append({
            "event": "zip_reuse", "category": "estabelecimentos",
            "file_index": i, "expected_bytes": size, "state": "stored",
        })
    events.extend([
        {"event": "release_discovery", "reference_period": "2026-10",
         "release_fingerprint": "f" * 64, "establishment_partitions": 2},
        {"event": "zip_reuse", "category": "estabelecimentos",
         "file_index": 0, "expected_bytes": 100,
         "elapsed_s": 0.4, "state": "verified"},
        {"event": "zip_reuse", "category": "estabelecimentos",
         "file_index": 1, "expected_bytes": 200,
         "elapsed_s": 0.5, "state": "verified"},
        {"event": "identity_complete"},
    ])
    return events


def _source_logs() -> list[dict]:
    return [
        {"event": "end", "source_id": source,
         "state": "fresh", "freshness_method": "current_fetch",
         "current_error": None, "used_cache": False,
         "duration_seconds": 900.0}
        for source in ("receita_cnpj_lifecycle", "receita_consumer_identity")
    ]


def _verify(tmp_path: Path, events: list[dict], sources: list[dict] | None = None) -> dict:
    telemetry = tmp_path / "events.jsonl"
    telemetry.write_text(
        "\n".join(json.dumps(x) for x in events) + "\n", encoding="utf-8"
    )
    dag_log = tmp_path / "dag.log"
    dag_log.write_text(
        "\n".join("V2_SOURCE_ACQUISITION " + json.dumps(x)
                  for x in (sources if sources is not None else _source_logs()))
        + "\n",
        encoding="utf-8",
    )
    return verify_rebuild_proof(telemetry, dag_log)


def test_a09_proof_accepts_single_get_and_two_verified_reuses(tmp_path: Path) -> None:
    report = _verify(tmp_path, _events())
    assert report["status"] == "verified"
    assert report["establishment_partitions"] == 2
    assert report["official_establishment_gets"] == 2
    assert report["reused_without_second_get"] == 2
    assert report["avoided_duplicate_download_bytes"] == 300
    assert report["verified_reuse_seconds"] == 0.9
    assert report["initial_establishment_download_seconds"] == 21


@pytest.mark.parametrize("mutation", [
    "duplicate_download", "missing_store", "missing_reuse",
    "changed_size", "corrupted_source", "late_download",
    "unverifiable", "fake_catalog_count",
])
def test_a09_proof_rejects_incomplete_or_false_savings(
    tmp_path: Path, mutation: str,
) -> None:
    events = deepcopy(_events())
    if mutation == "duplicate_download":
        events.append(dict(events[0]))
    elif mutation == "missing_store":
        events.pop(1)
    elif mutation == "missing_reuse":
        events.pop(-2)
    elif mutation == "changed_size":
        events[-2]["expected_bytes"] = 199
    elif mutation == "corrupted_source":
        events[0]["state"] = "failure"
    elif mutation == "late_download":
        events.append({
            "event": "zip_download", "category": "estabelecimentos",
            "file_index": 1, "received_bytes": 200, "elapsed_download_s": 30,
            "state": "success",
        })
    elif mutation == "unverifiable":
        events.insert(-1, {
            "event": "zip_reuse", "category": "estabelecimentos",
            "file_index": 1, "state": "storage_unavailable",
        })
    elif mutation == "fake_catalog_count":
        events[-4]["establishment_partitions"] = 3
    with pytest.raises(A09ProofError):
        _verify(tmp_path, events)


@pytest.mark.parametrize("mutation", [
    "missing", "cached", "stale", "failed", "duplicate",
])
def test_a09_proof_requires_two_real_fresh_acquisitions(
    tmp_path: Path, mutation: str,
) -> None:
    sources = _source_logs()
    if mutation == "missing":
        sources.pop()
    elif mutation == "cached":
        sources[-1]["used_cache"] = True
    elif mutation == "stale":
        sources[0]["state"] = "stale"
        sources[0]["freshness_method"] = "validated_cache_fallback"
    elif mutation == "failed":
        sources[0]["current_error"] = "Acquisition error"
    elif mutation == "duplicate":
        sources.append(deepcopy(sources[0]))
    with pytest.raises(A09ProofError):
        _verify(tmp_path, _events(), sources)


def test_a09_proof_missing_files_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(A09ProofError, match="missing"):
        verify_rebuild_proof(tmp_path / "absent.jsonl", tmp_path / "absent.log")


def test_a09_identity_cache_was_already_persisted_in_conduct_cache(tmp_path: Path) -> None:
    cache = _receita_cache(tmp_path)
    assert cache.content_path == tmp_path / "receita_consumer_identity.json"
    assert cache.metadata_path == tmp_path / "receita_consumer_identity.meta.json"
    assert cache.source_id == "receita_consumer_identity"


def test_a09_manual_proof_cannot_trigger_hostgator_publisher() -> None:
    proof = Path(".github/workflows/v2-a09-isolated-rebuild-proof.yml").read_text()
    deploy = Path(".github/workflows/v2-hostgator-publication.yml").read_text()
    canonical = Path(".github/workflows/v2-gate4-full-generation-proof.yml").read_text()
    assert "name: V2 A09 Isolated Full Rebuild Proof" in proof
    assert "workflow_dispatch:" in proof and "\n  schedule:" not in proof
    assert "if: inputs.confirm_rebuild == 'REBUILD'" in proof
    assert "actions/cache/restore@" not in proof
    assert "actions/cache/save@" not in proof
    assert "Upload compact A09 proof only" in proof
    assert "actions/upload-artifact@" in proof
    assert "data/derived/v2/public/" not in proof
    assert "gh workflow run" not in proof
    assert "V2 A09 Isolated Full Rebuild Proof" not in deploy
    assert "V2 Gate 4 Full Generation Proof" in deploy
    assert "path: data/cache/v2/conduct" in canonical
    assert "receita_consumer_identity.json" in proof
