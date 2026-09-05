from api.v2.consumer_gov_universe_resolution import (
    build_full_universe_provider_index,
    resolve_provider_against_full_universe,
)


def _entity(
    entity_id: str,
    legal_name: str,
    entity_type: str,
    eligible: bool,
    reason_codes: list[str] | None = None,
):
    return {
        "entity_id": entity_id,
        "legal_name": legal_name,
        "entity_type": entity_type,
        "eligibility": {
            "regulatory_universe_eligible": eligible,
            "reason_codes": reason_codes or [],
        },
    }


def test_current_short_core_name_can_match_when_unique_across_full_universe() -> None:
    index = build_full_universe_provider_index(
        [_entity("fip:006572", "HDI SEGUROS S.A.", "insurer", True)]
    )
    result = resolve_provider_against_full_universe("HDI Seguros", index)
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:006572"


def test_capitalization_is_excluded_not_transferred_to_same_group_insurer() -> None:
    index = build_full_universe_provider_index(
        [
            _entity("fip:005142", "ICATU SEGUROS S.A.", "insurer", True),
            _entity(
                "fip:021334",
                "ICATU CAPITALIZAÇÃO S.A.",
                "capitalization_company",
                False,
            ),
        ]
    )
    result = resolve_provider_against_full_universe("Icatu Capitalização", index)
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["matched_canonical_entity_id"] == "fip:021334"
    assert result["entity_id"] is None


def test_sandbox_is_excluded_even_when_label_says_seguradora() -> None:
    index = build_full_universe_provider_index(
        [_entity("cnpj:neo", "Neo Seguradora S.A.", "sandbox_participant", False)]
    )
    result = resolve_provider_against_full_universe("Neo seguradora", index)
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["reason_code"] == "canonical_outside_sandbox_participant"


def test_historical_legal_entity_is_not_transferred_to_successor() -> None:
    index = build_full_universe_provider_index(
        [
            _entity(
                "fip:003182",
                "ITAU SEGUROS DE AUTO E RESIDÊNCIA S.A.",
                "unknown",
                False,
                ["historical_legal_entity"],
            ),
            _entity(
                "fip:005886",
                "PORTO SEGURO COMPANHIA DE SEGUROS GERAIS",
                "insurer",
                True,
            ),
        ]
    )
    result = resolve_provider_against_full_universe(
        "Itaú Seguros Auto e Residência", index
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["matched_canonical_entity_id"] == "fip:003182"
    assert result["entity_id"] is None


def test_sales_channel_with_no_canonical_supervised_entity_stays_unresolved() -> None:
    index = build_full_universe_provider_index(
        [_entity("fip:006572", "HDI SEGUROS S.A.", "insurer", True)]
    )
    assert resolve_provider_against_full_universe("Seguros Honda", index) is None


def test_ambiguous_core_name_is_not_used() -> None:
    index = build_full_universe_provider_index(
        [
            _entity("fip:a", "ABC SEGUROS S.A.", "insurer", True),
            _entity("fip:b", "ABC SEGURADORA S.A.", "insurer", True),
        ]
    )
    assert resolve_provider_against_full_universe("ABC Seguros", index) is None
