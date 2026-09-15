from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2
from api.utils.name_cleaner import normalize_name_key
from api.v2.build_consumer_gov_receita_identity import (
    CACHE_KEY_VERSION,
    _load_inputs,
    target_universe_hash,
    unresolved_provider_hash,
)
from api.v2.source_cache import CachedSource


class ConductReceitaCacheCompatibilityError(ValueError):
    """Raised when stale Receita Conduct identity evidence cannot be reused safely."""


def _current_target_cnpjs(entities: list[dict[str, Any]]) -> set[str]:
    return {
        cnpj
        for entity in entities
        if (cnpj := normalize_cnpj_v2(entity.get("cnpj"))) is not None
    }


def _current_provider_keys(providers: list[str]) -> set[str]:
    return {
        key
        for provider in providers
        if (key := normalize_name_key(str(provider or "")))
    }


def _cache_evidence_sets(
    payload: dict[str, Any],
) -> tuple[set[str], set[str], list[dict[str, Any]], list[str]]:
    canonical_records = [
        dict(row)
        for row in (payload.get("canonical_records") or [])
        if isinstance(row, dict)
    ]
    cached_cnpjs = {
        cnpj
        for row in canonical_records
        if (cnpj := normalize_cnpj_v2(row.get("cnpj"))) is not None
    }
    provider_rows = [
        dict(row)
        for row in (payload.get("provider_matches") or [])
        if isinstance(row, dict)
    ]
    cached_provider_keys: set[str] = set()
    cached_provider_names: list[str] = []
    for row in provider_rows:
        provider = str(row.get("provider") or "").strip()
        key = normalize_name_key(str(row.get("provider_key") or provider))
        if not key:
            continue
        cached_provider_keys.add(key)
        cached_provider_names.append(provider or key)
    return cached_cnpjs, cached_provider_keys, canonical_records, cached_provider_names


def validate_additions_only_cache(
    payload: dict[str, Any],
    *,
    entities: list[dict[str, Any]],
    providers: list[str],
    universe_hash: str,
    provider_hash: str,
) -> dict[str, Any]:
    """Prove that stale Conduct identity evidence is safe under a growing universe.

    The prior snapshot must have been complete for its own target universe and provider
    query set. Reuse is allowed only when every old target CNPJ is still present in the
    current universe and every currently unresolved Consumer.gov provider already has a
    cached Receita query row. New current CNPJs are therefore the only permitted target
    delta; they remain without canonical Receita evidence until a fresh official build.
    """
    if payload.get("artifact") != "v2_receita_cnpj_identity":
        raise ConductReceitaCacheCompatibilityError(
            "unexpected Receita Conduct identity cache artifact"
        )

    key = (payload.get("meta") or {}).get("gate4_cache_key")
    if not isinstance(key, dict) or str(key.get("version") or "") != CACHE_KEY_VERSION:
        raise ConductReceitaCacheCompatibilityError(
            "Receita Conduct identity cache key is missing or unsupported"
        )

    meta = payload.get("meta") or {}
    try:
        target_count = int(meta.get("target_cnpjs"))
        enriched_count = int(meta.get("target_cnpjs_enriched"))
        provider_query_count = int(meta.get("provider_queries"))
    except (TypeError, ValueError) as exc:
        raise ConductReceitaCacheCompatibilityError(
            "Receita Conduct identity cache coverage metadata is invalid"
        ) from exc

    cached_cnpjs, cached_provider_keys, canonical_records, cached_provider_names = (
        _cache_evidence_sets(payload)
    )
    if (
        target_count <= 0
        or enriched_count != target_count
        or len(canonical_records) != target_count
        or len(cached_cnpjs) != target_count
    ):
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache was not complete for its prior target CNPJ universe"
        )
    if provider_query_count <= 0 or len(cached_provider_keys) != provider_query_count:
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache was not complete for its prior provider query set"
        )

    if any(not str(row.get("entity_id") or "").strip() for row in canonical_records):
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct canonical records are missing entity_id"
        )

    reconstructed_universe_hash = target_universe_hash(canonical_records)
    if reconstructed_universe_hash != str(key.get("target_universe_hash") or ""):
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache target hash does not match its canonical records"
        )
    reconstructed_provider_hash = unresolved_provider_hash(cached_provider_names)
    if reconstructed_provider_hash != str(key.get("unresolved_provider_hash") or ""):
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache provider hash does not match its provider rows"
        )

    current_cnpjs = _current_target_cnpjs(entities)
    current_provider_keys = _current_provider_keys(providers)
    if not current_cnpjs or not current_provider_keys:
        raise ConductReceitaCacheCompatibilityError(
            "current Conduct identity inputs are empty"
        )

    removed_cnpjs = sorted(cached_cnpjs - current_cnpjs)
    if removed_cnpjs:
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache contains target CNPJs no longer in the current universe: "
            f"count={len(removed_cnpjs)} sample={removed_cnpjs[:5]}"
        )

    added_cnpjs = sorted(current_cnpjs - cached_cnpjs)
    if universe_hash != str(key.get("target_universe_hash") or "") and not added_cnpjs:
        raise ConductReceitaCacheCompatibilityError(
            "Receita Conduct universe hash changed without an additions-only CNPJ delta"
        )

    missing_provider_keys = sorted(current_provider_keys - cached_provider_keys)
    if missing_provider_keys:
        raise ConductReceitaCacheCompatibilityError(
            "stale Receita Conduct cache does not cover current unresolved providers: "
            f"count={len(missing_provider_keys)} sample={missing_provider_keys[:5]}"
        )

    return {
        "mode": "current_target_additions_only_current_providers_covered",
        "cached_target_count": len(cached_cnpjs),
        "current_target_count": len(current_cnpjs),
        "added_current_cnpj_count": len(added_cnpjs),
        "added_current_cnpjs": added_cnpjs,
        "cached_provider_count": len(cached_provider_keys),
        "current_provider_count": len(current_provider_keys),
        "provider_set_reduced": current_provider_keys != cached_provider_keys,
        "cached_target_universe_hash": str(key.get("target_universe_hash") or ""),
        "current_target_universe_hash": universe_hash,
        "cached_unresolved_provider_hash": str(key.get("unresolved_provider_hash") or ""),
        "current_unresolved_provider_hash": provider_hash,
    }


def materialize_additions_only_cache(
    *,
    cache: CachedSource,
    destination: Path,
    universe_hash: str,
    provider_hash: str,
) -> tuple[str, dict[str, Any]]:
    """Materialize and annotate a provably compatible stale Conduct Receita cache."""
    fetched_at = cache.materialize(destination)
    payload = json.loads(destination.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ConductReceitaCacheCompatibilityError(
            "Receita Conduct identity cache must be a JSON object"
        )
    entities, providers = _load_inputs()
    diagnostic = validate_additions_only_cache(
        payload,
        entities=entities,
        providers=providers,
        universe_hash=universe_hash,
        provider_hash=provider_hash,
    )
    meta = dict(payload.get("meta") or {})
    meta["gate4_partial_cache_compatibility"] = diagnostic
    payload["meta"] = meta

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(destination)
    return fetched_at, diagnostic
