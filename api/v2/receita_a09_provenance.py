"""Compact, non-personal Receita identity provenance for A09.1.

This is an operational audit artifact, not a new data source, score or
replacement for the source-lineage contract.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_telemetry import emit, utcnow

DEFAULT_PROVENANCE = Path("data/derived/v2/receita_identity_provenance_a09.json")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_compact_provenance(
    snapshot: Path, lineage: Any, marker: Path,
) -> dict[str, Any]:
    """Check the actual materialized snapshot against its lineage/marker."""
    result: dict[str, Any] = {
        "artifact": "receita_identity_provenance_a09",
        "version": 1,
        "source_id": "receita_consumer_identity",
        "observed_at": utcnow(),
        "build_id": lineage.build_id,
        "source_state": lineage.state,
        "freshness_method": lineage.freshness_method,
        "snapshot_sha256": lineage.sha256,
        "reference_period": None,
        "release_url": None,
        "release_fingerprint": None,
        "release_fingerprint_method": None,
        "release_checked_at": None,
        "observed_catalog_fingerprint": None,
        "catalog_fingerprint_scope": None,
        "last_remote_fetch_at": None,
        "cache_contract_version": None,
        "cache_decision_reason": None,
        "target_universe_hash": None,
        "unresolved_provider_hash": None,
        "marker_consistency": "not_available",
        "snapshot_contract_consistency": "not_available",
    }
    if not snapshot.is_file():
        return result
    actual_digest = _sha256(snapshot)
    if actual_digest != lineage.sha256:
        raise ValueError("Receita provenance snapshot/lineage digest mismatch")
    data = json.loads(snapshot.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError("Receita provenance snapshot must be JSON object")
    source = data.get("source") or {}
    cache_key = (data.get("meta") or {}).get("gate4_cache_key") or {}
    contract_matches = (
        bool(source.get("reference_period"))
        and cache_key.get("reference_period") == source.get("reference_period")
    )
    result["snapshot_contract_consistency"] = (
        "matched" if contract_matches else "mismatch"
    )
    result.update({
        "reference_period": source.get("reference_period"),
        "release_url": source.get("release_url"),
        "release_fingerprint": source.get("release_fingerprint"),
        "release_fingerprint_method": source.get("release_fingerprint_method"),
        "last_remote_fetch_at": source.get("retrieved_at"),
        "cache_contract_version": cache_key.get("version"),
        "target_universe_hash": cache_key.get("target_universe_hash"),
        "unresolved_provider_hash": cache_key.get("unresolved_provider_hash"),
    })
    if not marker.is_file():
        return result
    observed = json.loads(marker.read_text(encoding="utf-8"))
    if not isinstance(observed, dict):
        return result
    consistent = (
        contract_matches
        and observed.get("snapshot_release_fingerprint")
        == result["release_fingerprint"]
        and observed.get("snapshot_reference_period") in (
            None, result["reference_period"]
        )
        and observed.get("observed_release_period") == result["reference_period"]
        and observed.get("observed_release_url") == result["release_url"]
        and observed.get("cache_contract_version") == result["cache_contract_version"]
        and observed.get("target_universe_hash") == result["target_universe_hash"]
        and observed.get("unresolved_provider_hash") == result["unresolved_provider_hash"]
    )
    result["marker_consistency"] = "matched" if consistent else "mismatch"
    if consistent:
        result["release_checked_at"] = observed.get("release_checked_at")
        result["observed_catalog_fingerprint"] = observed.get("catalog_fingerprint")
        result["catalog_fingerprint_scope"] = observed.get("catalog_fingerprint_scope")
        result["cache_decision_reason"] = observed.get("cache_decision")
    return result


def try_write_compact_provenance(
    snapshot: Path, lineage: Any, output: Path = DEFAULT_PROVENANCE,
) -> None:
    """Diagnostics must not introduce a new production failure mode."""
    try:
        result = build_compact_provenance(
            snapshot, lineage, snapshot.with_suffix(".a09.json"),
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        candidate = output.with_suffix(".a09.tmp")
        candidate.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        candidate.replace(output)
        emit(
            "provenance_write", state=result["marker_consistency"],
            reference_period=result["reference_period"],
            cache_decision=result["cache_decision_reason"],
        )
    except (OSError, ValueError, TypeError, AttributeError):
        emit("provenance_write", state="unavailable")
