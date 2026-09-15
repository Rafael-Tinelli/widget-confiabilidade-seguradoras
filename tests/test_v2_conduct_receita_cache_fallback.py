from __future__ import annotations

import json
from pathlib import Path

import pytest

from api.sources.receita_cnpj import RECEITA_CNPJ_OPEN_DATA_URL
from api.v2.build_conduct_source_snapshot import _receita_observation
from api.v2.build_consumer_gov_receita_identity import (
    CACHE_KEY_VERSION,
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


def _entity(index: int) -> dict[str, str]:
    return {
        "entity_id": f"fip:{index:06d}",
        "cnpj": f"{index + 10_000_000_000_000:014d}",
        "legal_name": f"SEGURADORA {index} S.A.",
    }


def _payload(
    entities: list[dict[str, str]],
    providers: list[str],
) -> dict:
    key = {
        "version": CACHE_KEY_VERSION,
        "reference_period": "2026-08",
        "target_universe_hash": target_universe_hash(entities),
        "unresolved_provider_hash": unresolved_provider_hash(providers),
    }
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


def test_removed_cached_target_still_fails_closed():
    cached_entities = [_entity(1), _entity(2)]
    current_entities = [_entity(2), _entity(3)]
    providers = ["Previsul"]

    with pytest.raises(
        ConductReceitaCacheCompatibilityError,
        match="no longer in the current universe",
    ):
        validate_additions_only_cache(
            _payload(cached_entities, providers),
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


def test_receita_observation_uses_additions_only_cache_as_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    cached_entities = [_entity(1), _entity(2)]
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
    source.write_text(json.dumps(_payload(cached_entities, providers)), encoding="utf-8")
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
    assert payload["meta"]["gate4_partial_cache_compatibility"][
        "added_current_cnpj_count"
    ] == 1
