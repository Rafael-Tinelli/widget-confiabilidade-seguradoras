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


def test_sompo_succession_is_not_misrepresented_as_receita_hint() -> None:
    hints = load_verified_receita_provider_hints()
    assert "sompo consumer" not in hints
    assert "sompo consumer desativado atual hdi seguros" not in hints
