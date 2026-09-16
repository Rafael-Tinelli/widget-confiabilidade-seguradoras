from api.v2.enrich_market_identity_candidates import (
    enrich_candidate_registry_with_receita,
)


def _registry(query: str):
    return {
        "artifact": "v2_market_identity_candidate_registry",
        "policy": {
            "candidate_assertion_effect": "none",
            "candidate_complaint_transfer_effect": "none",
            "automatic_registry_mutation": "forbidden",
        },
        "candidates": [
            {
                "candidate_id": "market:query:test",
                "candidate_type": "unknown_market_query",
                "review_state": "review_required",
                "lifecycle_state": "review_required",
                "assertion_effect": "none",
                "score_effect": "none",
                "complaint_transfer_effect": "none",
                "automatic_registry_mutation": "forbidden",
                "blocking": False,
                "observations": [
                    {
                        "source": "widget_unknown_search",
                        "sensor_class": "demand",
                        "normalized_query": query,
                        "observed_value": query,
                    }
                ],
            }
        ],
    }


def test_enrichment_preserves_candidate_lifecycle_and_assertion_policy():
    registry = _registry("allsafe")
    receita = {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": "AllSafe",
                "candidate_state": "unique_candidate",
                "match_method": "trade_name_surface_exact",
                "candidates": [
                    {
                        "cnpj": "33897006000162",
                        "legal_name_receita": "PACTA GESTAO DE NEGOCIOS LTDA",
                        "trade_name": "ALLSAFE",
                        "primary_cnae_code": "7490104",
                        "secondary_cnaes": [],
                    }
                ],
            }
        ],
    }

    out = enrich_candidate_registry_with_receita(registry, receita)
    candidate = out["candidates"][0]
    resolution = candidate["investigation"]["resolution"]

    assert candidate["review_state"] == "review_required"
    assert candidate["lifecycle_state"] == "review_required"
    assert candidate["assertion_effect"] == "none"
    assert candidate["automatic_registry_mutation"] == "forbidden"
    assert resolution["identity_state"] == "verified_legal_entity"
    assert resolution["classification_state"] == "role_unresolved"
    assert resolution["public_market_role"] is None
    assert resolution["risk_carrier"] is None


def test_receita_absence_does_not_turn_unknown_query_into_public_identity():
    out = enrich_candidate_registry_with_receita(
        _registry("yelum affinity"),
        {"artifact": "v2_receita_cnpj_identity", "provider_matches": []},
    )

    candidate = out["candidates"][0]
    assert candidate["investigation"] == {
        "source": "receita_cnpj",
        "state": "not_available_for_query",
        "query": "yelum affinity",
        "assertion_effect": "none",
    }
    assert candidate["review_state"] == "review_required"


def test_exact_susep_cnpj_can_verify_insurer_status_but_not_create_relationships():
    registry = _registry("nova seguradora")
    receita = {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": "Nova Seguradora",
                "candidate_state": "unique_candidate",
                "match_method": "trade_name_surface_exact",
                "candidates": [
                    {
                        "cnpj": "11111111000111",
                        "legal_name_receita": "NOVA SEGURADORA S A",
                        "trade_name": "NOVA SEGURADORA",
                        "primary_cnae_code": "6512000",
                    }
                ],
            }
        ],
    }
    licensed = {"entities": [{"cnpj": "11.111.111/0001-11"}]}

    out = enrich_candidate_registry_with_receita(
        registry,
        receita,
        current_licensed_payload=licensed,
    )
    resolution = out["candidates"][0]["investigation"]["resolution"]

    assert resolution["insurer_status"] == "authorized_insurer"
    assert resolution["classification_state"] == "verified_regulatory"
    assert resolution["relationship_state"] == "unresolved"
    assert resolution["risk_carrier"] is None


def test_ambiguous_receita_match_stays_unresolved():
    registry = _registry("affinity")
    receita = {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": "Affinity",
                "candidate_state": "ambiguous_candidates",
                "candidates": [
                    {
                        "cnpj": "17846647000131",
                        "legal_name_receita": "ASPAS TURISMO VIAGENS E ASSISTENCIA INTERNACIONAL S A",
                        "primary_cnae_code": "7912100",
                    },
                    {
                        "cnpj": "13711169000100",
                        "legal_name_receita": "3RI VIAGENS E TURISMO LTDA",
                        "primary_cnae_code": "7911200",
                    },
                ],
            }
        ],
    }

    out = enrich_candidate_registry_with_receita(registry, receita)
    resolution = out["candidates"][0]["investigation"]["resolution"]

    assert resolution["identity_state"] == "unresolved"
    assert resolution["publication_state"] == "private_candidate"
    assert resolution["review_reason"] == "ambiguous_legal_entity_candidates"
