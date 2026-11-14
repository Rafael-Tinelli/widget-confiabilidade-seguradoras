from __future__ import annotations

import argparse
import hashlib
import json

import requests
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_bulk import (
    _configure_official_session,
    discover_latest_release,
)
from api.sources.receita_cnpj_cache_integrity import (
    checked_release_fingerprint,
    identity_cache_current,
)
from api.sources.receita_cnpj_identity import _release_files, build_filtered_identity_snapshot
from api.sources.receita_cnpj_telemetry import emit, release_fingerprint, utcnow
from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
from api.v2.consumer_gov_receita_resolution import DEFAULT_RECEITA_IDENTITY_SNAPSHOT

CONSUMER_IDENTITY_PATH = Path("data/derived/v2/consumer_gov_identity_experiment.json")
CACHE_KEY_VERSION = "v2-consumer-receita-identity-2"
LEGACY_CACHE_KEY_VERSIONS = frozenset({"v2-consumer-receita-identity-1"})
TARGET_UNIVERSE_HASH_METHOD = "sorted_normalized_cnpjs_v1"


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def target_universe_cnpjs(entities: list[dict[str, Any]]) -> list[str]:
    cnpjs = sorted(
        {
            cnpj
            for entity in entities
            if (cnpj := normalize_cnpj_v2(entity.get("cnpj"))) is not None
        }
    )
    if not cnpjs:
        raise RuntimeError("cannot hash an empty Consumer.gov Receita CNPJ target universe")
    return cnpjs


def target_universe_hash(entities: list[dict[str, Any]]) -> str:
    """Hash exactly the CNPJ population queried in the filtered Receita build."""
    return _stable_hash(target_universe_cnpjs(entities))


def unresolved_provider_hash(providers: list[str]) -> str:
    names = sorted(
        {
            normalize_name_key(provider)
            for provider in providers
            if normalize_name_key(provider)
        }
    )
    if not names:
        raise RuntimeError("cannot hash an empty unresolved Consumer.gov provider set")
    return _stable_hash(names)


def _cache_key(
    *,
    reference_period: str,
    universe_hash: str,
    provider_hash: str,
) -> dict[str, str]:
    return {
        "version": CACHE_KEY_VERSION,
        "reference_period": reference_period,
        "target_universe_hash": universe_hash,
        "target_universe_hash_method": TARGET_UNIVERSE_HASH_METHOD,
        "unresolved_provider_hash": provider_hash,
    }


def _existing_cache_key(path: Path) -> dict[str, str] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    meta = payload.get("meta") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return None
    value = meta.get("gate4_cache_key")
    if not isinstance(value, dict):
        return None
    return {
        "version": str(value.get("version") or ""),
        "reference_period": str(value.get("reference_period") or ""),
        "target_universe_hash": str(value.get("target_universe_hash") or ""),
        "target_universe_hash_method": str(
            value.get("target_universe_hash_method") or ""
        ),
        "unresolved_provider_hash": str(value.get("unresolved_provider_hash") or ""),
    }


def _official_identity_release_fingerprint(release: Any) -> str | None:
    """Only PROPFIND; no official ZIP downloaded while validating cache."""
    with requests.Session() as session:
        _configure_official_session(session)
        files = _release_files(session, release)
    required = [
        *files["companies"], *release.establishment_files,
        files["cnaes"], files["legal_natures"],
    ]
    selected = [files["metadata"][name] for name in required]
    return checked_release_fingerprint(release.period, selected, required)


def _cache_decision(
    output: Path, existing: dict[str, str] | None,
    expected: dict[str, str],
) -> str:
    """Explain existing eligibility exactly; do not change the cache key."""
    if not output.is_file():
        return "miss_snapshot"
    if existing is None:
        return "miss_contract"
    for field, reason in (
        ("version", "miss_contract"),
        ("reference_period", "miss_period"),
        ("target_universe_hash", "miss_universe_hash"),
        ("target_universe_hash_method", "miss_contract"),
        ("unresolved_provider_hash", "miss_provider_hash"),
    ):
        if existing.get(field) != expected.get(field):
            return reason
    return "hit"


def _write_a09_marker(
    output: Path, *, release: Any, decision: str,
    expected_key: dict[str, str], checked_at: str,
    snapshot: dict[str, Any] | None,
) -> None:
    """Optional run-scoped metadata; never a replacement for source content."""
    marker = output.with_suffix(".a09.json")
    try:
        fingerprint = release_fingerprint(
            release.period, list(release.resource_metadata),
        )
        source = (snapshot or {}).get("source") or {}
        payload = {
            "schema": "receita-a09-1-observation-v1",
            "observed_release_period": release.period,
            "observed_release_url": release.release_url,
            "release_checked_at": checked_at,
            "catalog_fingerprint": fingerprint,
            "catalog_fingerprint_scope": "discovery_estabelecimentos_motivos",
            "snapshot_reference_period": source.get("reference_period"),
            "snapshot_release_fingerprint": source.get("release_fingerprint"),
            "cache_decision": decision,
            "cache_contract_version": CACHE_KEY_VERSION,
            "target_universe_hash": expected_key["target_universe_hash"],
            "unresolved_provider_hash": expected_key["unresolved_provider_hash"],
        }
        marker.parent.mkdir(parents=True, exist_ok=True)
        temp = marker.with_suffix(".a09.tmp")
        temp.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp.replace(marker)
    except (OSError, AttributeError, KeyError, TypeError, ValueError):
        emit("provenance_write", state="unavailable")


def _load_inputs() -> tuple[list[dict[str, Any]], list[str]]:
    eligibility = json.loads(ELIGIBILITY_PATH.read_text(encoding="utf-8"))
    consumer = json.loads(CONSUMER_IDENTITY_PATH.read_text(encoding="utf-8"))
    entities = list(eligibility.get("entities") or [])
    providers = [
        str(row.get("provider") or "").strip()
        for row in consumer.get("unresolved_providers") or []
        if str(row.get("provider") or "").strip()
    ]
    if not entities:
        raise RuntimeError("eligibility inventory contains no entities")
    if not providers:
        raise RuntimeError("Consumer.gov identity experiment contains no unresolved providers")
    return entities, providers


def write_snapshot(payload: dict[str, Any], output: Path) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def build_snapshot(
    *,
    output: Path = DEFAULT_RECEITA_IDENTITY_SNAPSHOT,
    force: bool = False,
) -> dict[str, Any] | None:
    marker = output.with_suffix(".a09.json")
    try:
        marker.unlink(missing_ok=True)
    except OSError:
        emit("provenance_write", state="unavailable")
    entities, providers = _load_inputs()
    universe_hash = target_universe_hash(entities)
    provider_hash = unresolved_provider_hash(providers)
    release = discover_latest_release()
    checked_at = utcnow()
    expected_key = _cache_key(
        reference_period=release.period,
        universe_hash=universe_hash,
        provider_hash=provider_hash,
    )
    existing_key = _existing_cache_key(output)
    decision = _cache_decision(output, existing_key, expected_key)
    if force:
        decision = "forced_rebuild"
    emit(
        "cache_decision", cache_decision=decision,
        reference_period=release.period,
        target_count=len(entities), provider_count=len(providers),
    )
    if not force and existing_key == expected_key:
        prior = json.loads(output.read_text(encoding="utf-8"))
        current_fingerprint = _official_identity_release_fingerprint(release)
        if identity_cache_current(prior, release, current_fingerprint):
            _write_a09_marker(
                output, release=release, decision=decision,
                expected_key=expected_key, checked_at=checked_at,
                snapshot=prior,
            )
            emit("cache_integrity", phase="identity", state="verified",
                 reference_period=release.period)
            print(
                "Receita Consumer.gov identity build skipped: cache key and "
                "official metadata fingerprint are unchanged"
            )
            return None
        decision = "miss_release_fingerprint"
        emit("cache_integrity", phase="identity", state="mismatch",
             reference_period=release.period)
        emit("cache_decision", phase="identity", cache_decision=decision,
             reference_period=release.period)

    payload = build_filtered_identity_snapshot(
        entities,
        providers,
        release=release,
    )
    meta = payload.setdefault("meta", {})
    meta["gate4_cache_key"] = expected_key
    meta["target_universe_hash_method"] = TARGET_UNIVERSE_HASH_METHOD
    write_snapshot(payload, output)
    _write_a09_marker(
        output, release=release, decision=decision,
        expected_key=expected_key, checked_at=checked_at,
        snapshot=payload,
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve unresolved Consumer.gov provider labels against filtered official "
            "Receita CNPJ identity/activity data."
        )
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_RECEITA_IDENTITY_SNAPSHOT)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    payload = build_snapshot(output=args.output, force=args.force)
    if payload is None:
        return

    meta = payload["meta"]
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "reference_period": payload["source"]["reference_period"],
                "gate4_cache_key": meta["gate4_cache_key"],
                "target_cnpjs": meta["target_cnpjs"],
                "target_cnpjs_enriched": meta["target_cnpjs_enriched"],
                "provider_queries": meta["provider_queries"],
                "provider_unique_candidates": meta["provider_unique_candidates"],
                "provider_ambiguous_candidates": meta["provider_ambiguous_candidates"],
                "provider_no_candidate": meta["provider_no_candidate"],
                "provider_matches": payload["provider_matches"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {args.output}")


if __name__ == "__main__":
    main()
