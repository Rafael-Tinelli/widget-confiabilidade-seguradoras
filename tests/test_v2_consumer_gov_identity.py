from pathlib import Path

import pytest

from api.v2.consumer_gov_identity import (
    ConsumerGovIdentityError,
    load_provider_resolution_registry,
    resolve_curated_provider,
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
