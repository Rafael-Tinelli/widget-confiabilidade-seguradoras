from api.v2.consumer_gov_receita_resolution import (
    load_verified_receita_provider_hints,
    resolve_provider_via_receita_payload,
)


def _entity(entity_id: str, cnpj: str, legal_name: str) -> dict:
    return {
        "entity_id": entity_id,
        "cnpj": cnpj,
        "legal_name": legal_name,
        "entity_type": "insurer",
        "eligibility": {
            "regulatory_universe_eligible": True,
            "reason_codes": [],
        },
    }


def _empty_payload(provider: str) -> dict:
    return {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": provider,
                "provider_key": provider.lower(),
                "candidate_state": "no_candidate",
                "match_method": None,
                "candidates": [],
            }
        ],
        "canonical_records": [],
    }


def _ambiguous_payload(provider: str) -> dict:
    return {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": provider,
                "provider_key": provider.lower(),
                "candidate_state": "ambiguous_candidates",
                "match_method": None,
                "candidates": [
                    {
                        "cnpj": "26136748000100",
                        "trade_name": "MG SEGUROS",
                        "primary_cnae_code": "6511101",
                    },
                    {
                        "cnpj": "23846343000177",
                        "trade_name": "MG SEGUROS",
                        "primary_cnae_code": "6622300",
                    },
                ],
            }
        ],
        "canonical_records": [],
    }


def test_ciclic_verified_hint_excludes_broker_without_carrier_transfer() -> None:
    hints = load_verified_receita_provider_hints()
    result = resolve_provider_via_receita_payload(
        "Ciclic",
        _empty_payload("Ciclic"),
        [],
        verified_hints=hints,
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["reason_code"] == "receita_insurance_broker_or_agent_activity"
    assert result["entity_id"] is None


def test_sky_verified_hint_requires_exact_current_susep_cnpj() -> None:
    hints = load_verified_receita_provider_hints()
    sky = _entity(
        "fip:003638",
        "52997050000199",
        "SKY SEGURADORA S.A. - MICROSSEGURADORA",
    )
    result = resolve_provider_via_receita_payload(
        "Sky Seguradora",
        _empty_payload("Sky Seguradora"),
        [sky],
        verified_hints=hints,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:003638"


def test_mg_verified_hint_can_resolve_real_trade_name_despite_generic_homonyms() -> None:
    hints = load_verified_receita_provider_hints()
    bmg = _entity(
        "fip:003417",
        "26136748000100",
        "BMG SEGURADORA S.A.",
    )
    result = resolve_provider_via_receita_payload(
        "MG Seguros",
        _ambiguous_payload("MG Seguros"),
        [bmg],
        verified_hints=hints,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:003417"
    assert result["match_method"] == "verified_receita_provider_hint_cnpj_to_current_susep"


def test_reag_verified_hint_resolves_same_cnpj_current_taamin_entity() -> None:
    hints = load_verified_receita_provider_hints()
    taamin = _entity(
        "fip:001864",
        "53759974000110",
        "TAAMIN SEGURADORA S.A.",
    )
    result = resolve_provider_via_receita_payload(
        "Reag Seguradora",
        _empty_payload("Reag Seguradora"),
        [taamin],
        verified_hints=hints,
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:001864"


def test_sompo_succession_is_not_misrepresented_as_receita_hint() -> None:
    hints = load_verified_receita_provider_hints()
    assert "sompo consumer" not in hints
    assert "sompo consumer desativado atual hdi seguros" not in hints
