from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.utils.identifiers import normalize_cnpj_v2
from api.v2.build_conduct_source_snapshot import _receita_observation
from api.v2.build_consumer_gov_receita_identity import (
    CACHE_KEY_VERSION,
    LEGACY_CACHE_KEY_VERSIONS,
    TARGET_UNIVERSE_HASH_METHOD,
    target_universe_hash,
    unresolved_provider_hash,
)
from api.v2.conduct_receita_cache_fallback import (
    ConductReceitaCacheCompatibilityError,
    materialize_additions_only_cache,
    validate_additions_only_cache,
)
from api.v2.generation import BuildContext
from api.v2.source_cache import CachedSource


def _entity(index: int, *, cnpj: str | None = None) -> dict[str, Any]:
    return {
        "entity_id": f"fip:{index:06d}",
        "cnpj": cnpj or f"{index + 10_000_000_000_000:014d}",
        "legal_name": f"SEGURADORA {index} S.A.",
    }


def _legacy_target_hash(entities: list[dict[str, Any]]) -> str:
    rows = sorted(
        {
            (
                str(entity.get("entity_id") or entity.get("id") or "").strip(),
                normalize_cnpj_v2(entity.get("cnpj")) or "",
            )
            for entity in entities
            if str(entity.get("entity_id") or entity.get("id") or "").strip()
        }
    )
    encoded = json.dumps(
        rows,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _payload(
    entities: list[dict[str, Any]],
    providers: list[str],
    *,
    contract_version: str = CACHE_KEY_VERSION,
    legacy_hash_entities: list[dict[str, Any]] | None = None,
) -> dict:
    if contract_version in LEGACY_CACHE_KEY_VERSIONS:
        universe_hash = _legacy_target_hash(legacy_hash_entities or entities)
    else:
        universe_hash = target_universe_hash(entities)
    key = {
        "version": contract_version,
        "reference_period": "2026-08",
        "target_universe_hash": universe_hash,
        "unresolved_provider_hash": unresolved_provider_hash(providers),
    }
    if contract_version == CACHE_KEY_VERSION:
        key["target_universe_hash_method"] = TARGET_UNIVERSE_HASH_METHOD
    return {
        "artifact": "v2_receita_cnpj_identity",
        "status": "experimental",
        "source": {"reference_period": "2026-08"},
        "meta": {
            "target_cnpjs": len(entities),
            "target_cnpjs_enriched": len(entities),
            "provider_queries": len(providers),
            "provider_unique_candidates": 0,
            "provider_ambiguous_candidates": 0,
            "provider_no_candidate": len(providers),
            "gate4_cache_key": key,
        },
        "canonical_records": [
            {
                "entity_id": entity["entity_id"],
                "cnpj": entity["cnpj"],
                "project_legal_name": entity["legal_name"],
                "legal_name_receita": entity["legal_name"],
            }
            for entity in entities
        ],
        "provider_matches": [
            {
                "provider": provider,
                "provider_key": provider.casefold(),
                "candidate_state": "no_candidate",
                "match_method": None,
                "candidates": [],
            }
            for provider in providers
        ],
    }


def _cache(tmp_path: Path) -> CachedSource:
    return CachedSource(
        source_id="receita_consumer_identity",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=tmp_path / "cache" / "receita_consumer_identity.json",
        metadata_path=tmp_path / "cache" / "receita_consumer_identity.meta.json",
    )


def _context() -> BuildContext:
    return BuildContext(
        build_id="v2-test-conduct-partial",
        source_head_sha="a" * 40,
        generated_at="2026-09-15T00:00:00Z",
        workflow_run_id="test",
        workflow_run_attempt=1,
    )


def test_additions_only_target_growth_is_compatible_when_current_providers_are_covered():
    cached_entities = [_entity(1), _entity(2)]
    current_entities = [*cached_entities, _entity(3)]
    providers = ["Previsul", "Itaú Unibanco Capitalização"]
    payload = _payload(cached_entities, providers)

    diagnostic = validate_additions_only_cache(
        payload,
        entities=current_entities,
        providers=providers,
        universe_hash=target_universe_hash(current_entities),
        provider_hash=unresolved_provider_hash(providers),
    )

    assert diagnostic["cached_target_count"] == 2
    assert diagnostic["current_target_count"] == 3
    assert diagnostic["added_current_cnpj_count"] == 1
    assert diagnostic["added_current_cnpjs"] == [_entity(3)["cnpj"]]
    assert diagnostic["current_provider_count"] == 2
    assert diagnostic["provider_set_reduced"] is False
    assert diagnostic["legacy_cache_contract"] is False


def test_run_83_legacy_hash_with_non_cnpj_entity_is_migratable():
    legacy_version = next(iter(LEGACY_CACHE_KEY_VERSIONS))
    cached_entities = [_entity(1), _entity(2)]
    legacy_hashed_entities = [
        *cached_entities,
        {
            "entity_id": "legacy:without-cnpj",
            "cnpj": None,
            "legal_name": "ENTIDADE SEM CNPJ UTILIZÁVEL",
        },
    ]
    current_entities = [*cached_entities, _entity(3), _entity(4)]
    providers = ["Previsul", "Itaú Unibanco Capitalização"]
    payload = _payload(
        cached_entities,
        providers,
        contract_version=legacy_version,
        legacy_hash_entities=legacy_hashed_entities,
    )

    assert payload["meta"]["gate4_cache_key"]["target_universe_hash"] != target_universe_hash(
        cached_entities
    )

    diagnostic = validate_additions_only_cache(
        payload,
        entities=current_entities,
        providers=providers,
        universe_hash=target_universe_hash(current_entities),
        provider_hash=unresolved_provider_hash(providers),
    )

    assert diagnostic["legacy_cache_contract"] is True
    assert diagnostic["cache_contract_version"] == legacy_version
    assert diagnostic["added_current_cnpj_count"] == 2
    assert "not_reconstructible" in diagnostic["target_hash_validation"]


def test_removed_cached_target_still_fails_closed_for_legacy_cache():
    legacy_version = next(iter(LEGACY_CACHE_KEY_VERSIONS))
    cached_entities = [_entity(1), _entity(2)]
    current_entities = [_entity(2), _entity(3)]
    providers = ["Previsul"]

    with pytest.raises(
        ConductReceitaCacheCompatibilityError,
        match="no longer in the current universe",
    ):
        validate_additions_only_cache(
            _payload(
                cached_entities,
                providers,
                contract_version=legacy_version,
            ),
            entities=current_entities,
            providers=providers,
            universe_hash=target_universe_hash(current_entities),
            provider_hash=unresolved_provider_hash(providers),
        )


def test_new_current_unresolved_provider_not_in_cache_still_fails_closed():
    entities = [_entity(1), _entity(2)]
    cached_providers = ["Previsul"]
    current_providers = ["Previsul", "Novo Provedor"]

    with pytest.raises(
        ConductReceitaCacheCompatibilityError,
        match="does not cover current unresolved providers",
    ):
        validate_additions_only_cache(
            _payload(entities, cached_providers),
            entities=entities,
            providers=current_providers,
            universe_hash=target_universe_hash(entities),
            provider_hash=unresolved_provider_hash(current_providers),
        )


def test_unknown_future_contract_still_fails_closed():
    entities = [_entity(1), _entity(2)]
    providers = ["Previsul"]
    payload = _payload(entities, providers)
    payload["meta"]["gate4_cache_key"]["version"] = "v2-consumer-receita-identity-999"

    with pytest.raises(ConductReceitaCacheCompatibilityError, match="unsupported"):
        validate_additions_only_cache(
            payload,
            entities=entities,
            providers=providers,
            universe_hash=target_universe_hash(entities),
            provider_hash=unresolved_provider_hash(providers),
        )


def test_current_v2_target_hash_mismatch_still_fails_closed():
    entities = [_entity(1), _entity(2)]
    providers = ["Previsul"]
    payload = _payload(entities, providers)
    payload["meta"]["gate4_cache_key"]["target_universe_hash"] = "f" * 64

    with pytest.raises(ConductReceitaCacheCompatibilityError, match="canonical records"):
        validate_additions_only_cache(
            payload,
            entities=entities,
            providers=providers,
            universe_hash=target_universe_hash(entities),
            provider_hash=unresolved_provider_hash(providers),
        )


def test_materialized_partial_cache_is_annotated_without_rewriting_canonical_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    cached_entities = [_entity(1), _entity(2)]
    current_entities = [*cached_entities, _entity(3)]
    providers = ["Previsul"]
    source = tmp_path / "source.json"
    source.write_text(json.dumps(_payload(cached_entities, providers)), encoding="utf-8")
    cache = _cache(tmp_path)
    cache.store(source, fetched_at="2026-09-08T12:00:00Z")
    cached_before = cache.content_path.read_text(encoding="utf-8")

    monkeypatch.setattr(
        "api.v2.conduct_receita_cache_fallback._load_inputs",
        lambda: (current_entities, providers),
    )
    destination = tmp_path / "materialized.json"
    fetched_at, diagnostic = materialize_additions_only_cache(
        cache=cache,
        destination=destination,
        universe_hash=target_universe_hash(current_entities),
        provider_hash=unresolved_provider_hash(providers),
    )

    materialized = json.loads(destination.read_text(encoding="utf-8"))
    assert fetched_at == "2026-09-08T12:00:00Z"
    assert diagnostic["added_current_cnpj_count"] == 1
    assert materialized["meta"]["gate4_partial_cache_compatibility"] == diagnostic
    assert cache.content_path.read_text(encoding="utf-8") == cached_before


def test_receita_observation_uses_legacy_run_83_cache_as_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    legacy_version = next(iter(LEGACY_CACHE_KEY_VERSIONS))
    cached_entities = [_entity(1), _entity(2)]
    legacy_hashed_entities = [
        *cached_entities,
        {
            "entity_id": "legacy:without-cnpj",
            "cnpj": None,
            "legal_name": "ENTIDADE SEM CNPJ UTILIZÁVEL",
        },
    ]
    current_entities = [*cached_entities, _entity(3)]
    providers = ["Previsul"]
    cache_dir = tmp_path / "cache"
    cache = CachedSource(
        source_id="receita_consumer_identity",
        source_url=RECEITA_CNPJ_OPEN_DATA_URL,
        content_path=cache_dir / "receita_consumer_identity.json",
        metadata_path=cache_dir / "receita_consumer_identity.meta.json",
    )
    source = tmp_path / "source.json"
    source.write_text(
        json.dumps(
            _payload(
                cached_entities,
                providers,
                contract_version=legacy_version,
                legacy_hash_entities=legacy_hashed_entities,
            )
        ),
        encoding="utf-8",
    )
    cache.store(source, fetched_at="2026-09-08T12:00:00Z")

    destination = tmp_path / "receita_consumer_gov_identity.json"
    monkeypatch.setattr(
        "api.v2.build_conduct_source_snapshot.DEFAULT_RECEITA_IDENTITY_SNAPSHOT",
        destination,
    )
    monkeypatch.setattr(
        "api.v2.build_conduct_source_snapshot._current_input_hashes",
        lambda: (
            target_universe_hash(current_entities),
            unresolved_provider_hash(providers),
        ),
    )
    monkeypatch.setattr(
        "api.v2.build_conduct_source_snapshot.build_snapshot",
        lambda **_: (_ for _ in ()).throw(ConnectionError("official manifest unavailable")),
    )
    monkeypatch.setattr(
        "api.v2.conduct_receita_cache_fallback._load_inputs",
        lambda: (current_entities, providers),
    )

    result = _receita_observation(cache_dir=cache_dir)
    lineage = result.to_lineage(_context())
    payload = json.loads(destination.read_text(encoding="utf-8"))

    assert lineage.state == "stale"
    assert lineage.freshness_method == "validated_cache_fallback"
    assert "official manifest unavailable" in (result.state_reason or "")
    assert "added_current_cnpjs=1" in (result.state_reason or "")
    diagnostic = payload["meta"]["gate4_partial_cache_compatibility"]
    assert diagnostic["legacy_cache_contract"] is True
    assert diagnostic["added_current_cnpj_count"] == 1
