from pathlib import Path

import pytest

from api.v2.consumer_gov_identity import (
    ConsumerGovIdentityError,
    load_provider_resolution_registry,
    resolve_curated_provider,
)
from api.v2.consumer_gov_temporal_brand import (
    build_temporal_brand_index,
    resolve_temporal_brand,
)


def test_registry_resolves_same_cnpj_historical_alias() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider(
        "Too Seguros (antiga PAN Seguros)",
        {"33245762000107": "fip:006653"},
        registry,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:006653"
    assert result["resolution_kind"] == "historical_legal_name_same_cnpj"


def test_registry_preserves_ambiguous_multi_entity_label() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider(
        "HDI Seguros (e Sompo Consumer)",
        {},
        registry,
    )
    assert result is not None
    assert result["resolution_state"] == "ambiguous"
    assert result["entity_id"] is None


def test_registry_resolves_deactivated_sompo_consumer_to_hdi_successor() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider(
        "Sompo Consumer (DESATIVADO) (Atual HDI SEGUROS)",
        {"29980158000157": "fip:006572"},
        registry,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:006572"
    assert result["resolution_kind"] == (
        "historical_legal_entity_incorporated_into_current_insurer"
    )


def test_registry_resolves_plain_sompo_consumer_to_same_hdi_successor() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider(
        "Sompo Consumer",
        {"29980158000157": "fip:006572"},
        registry,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:006572"


def test_registry_keeps_viver_previdencia_outside_without_transfer_to_insurer() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider("Viver Previdência", {}, registry)
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["resolution_kind"] == "open_pension_entity_not_current_insurer"
    assert result["entity_id"] is None


def test_registry_preserves_outside_universe_state() -> None:
    registry = load_provider_resolution_registry()
    result = resolve_curated_provider("LTI Seguros", {}, registry)
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["entity_id"] is None


def test_unregistered_provider_is_not_guessed() -> None:
    registry = load_provider_resolution_registry()
    assert resolve_curated_provider("Unknown Provider", {}, registry) is None


def test_matched_target_must_exist_in_current_universe() -> None:
    registry = load_provider_resolution_registry()
    with pytest.raises(ConsumerGovIdentityError):
        resolve_curated_provider(
            "Yelum Seguros (antiga Liberty Seguros)",
            {},
            registry,
        )


def test_registry_rejects_non_source_backed_rows(tmp_path: Path) -> None:
    path = tmp_path / "invalid.json"
    path.write_text(
        '{"resolutions":[{"provider_name":"X","resolution_state":"ambiguous",'
        '"resolution_kind":"test","evidence":[]}]}',
        encoding="utf-8",
    )
    with pytest.raises(ConsumerGovIdentityError):
        load_provider_resolution_registry(path)


def _temporal_brand_index() -> dict:
    return build_temporal_brand_index(
        [
            {
                "brand_id": "brand:test",
                "name": "Marca Teste",
                "aliases": ["Teste Seguro"],
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_entity_id": "fip:000001",
                        "effective_from": "2026-02-28",
                        "status": "current",
                    }
                ],
            }
        ],
        {"fip:000001"},
    )


def test_temporal_brand_requires_full_month_coverage() -> None:
    result = resolve_temporal_brand("Marca Teste", "2026-02", _temporal_brand_index())
    assert result is not None
    assert result["resolution_state"] == "unresolved"
    assert result["match_method"] == "verified_brand_partial_month_unresolved"


def test_temporal_brand_resolves_after_effective_month() -> None:
    result = resolve_temporal_brand("Teste Seguro", "2026-03", _temporal_brand_index())
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:000001"
    assert result["match_method"] == "verified_brand_exact_temporal"


def test_temporal_brand_does_not_apply_before_effective_window() -> None:
    result = resolve_temporal_brand("Marca Teste", "2026-01", _temporal_brand_index())
    assert result is not None
    assert result["resolution_state"] == "unresolved"
    assert result["match_method"] == "verified_brand_out_of_window_unresolved"
