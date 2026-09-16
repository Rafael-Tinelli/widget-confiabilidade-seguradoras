from api.v2.market_role_signals import (
    market_role_signals_from_receita,
    resolve_receita_market_identity_signal,
    resolve_unknown_query_with_receita_candidates,
)


def test_receita_broker_cnae_is_hypothesis_not_public_role():
    candidate = {
        "cnpj": "00027571000110",
        "legal_name_receita": "ASSIST-CARD DO BRASIL LTDA",
        "trade_name": "ASSIST CARD",
        "primary_cnae_code": "6622300",
        "secondary_cnaes": [],
    }
    result = resolve_receita_market_identity_signal(candidate)

    assert result["identity_state"] == "verified_legal_entity"
    assert result["classification_state"] == "role_unresolved"
    assert result["insurer_status"] == "not_verified_as_insurer"
    assert result["public_market_role"] is None
    assert result["risk_carrier"] is None
    assert result["automatic_registry_mutation"] == "forbidden"
    assert result["role_signals"][0]["signal"] == "insurance_broker_or_agent_activity"
    assert result["role_signals"][0]["assertion_effect"] == "none"


def test_general_intermediation_does_not_become_broker():
    candidate = {
        "cnpj": "16813061000164",
        "legal_name_receita": "ITURAN SERVICOS LTDA",
        "trade_name": "ITURAN",
        "primary_cnae_code": "7490104",
        "secondary_cnaes": [],
    }
    signals = market_role_signals_from_receita(candidate)

    assert signals == [
        {
            "source": "receita_cnae",
            "cnae_code": "7490104",
            "position": "primary",
            "signal": "general_business_intermediation",
            "strength": "weak",
            "semantics": "role_hypothesis_only",
            "assertion_effect": "none",
        }
    ]


def test_travel_activity_can_steer_review_without_asserting_market_role():
    candidate = {
        "cnpj": "07139957000162",
        "legal_name_receita": "ASISTBRAS - ASSISTENCIA AO VIAJANTE LTDA",
        "trade_name": "UNIVERSAL ASSISTANCE",
        "primary_cnae_code": "7990200",
        "secondary_cnaes": [
            {"code": "7911200"},
            {"code": "7912100"},
        ],
    }
    result = resolve_receita_market_identity_signal(candidate)

    assert result["identity_state"] == "verified_legal_entity"
    assert result["public_market_role"] is None
    assert {row["signal"] for row in result["role_signals"]} == {
        "other_travel_services_activity",
        "travel_agency_activity",
        "tour_operator_activity",
    }


def test_exact_current_susep_cnpj_wins_over_receita_role_hypothesis():
    candidate = {
        "cnpj": "61198164000160",
        "legal_name_receita": "PORTO SEGURO COMPANHIA DE SEGUROS GERAIS",
        "primary_cnae_code": "6512000",
        "secondary_cnaes": [],
    }
    result = resolve_receita_market_identity_signal(
        candidate,
        current_susep_insurer_cnpjs=["61.198.164/0001-60"],
    )

    assert result["insurer_status"] == "authorized_insurer"
    assert result["classification_state"] == "verified_regulatory"
    assert result["review_reason"] == "current_susep_insurer_exact_cnpj"


def test_program_or_product_name_without_distinct_legal_entity_stays_private_candidate():
    result = resolve_unknown_query_with_receita_candidates(
        "Yelum Affinity",
        [],
    )

    assert result["identity_state"] == "unresolved"
    assert result["publication_state"] == "private_candidate"
    assert result["public_market_role"] is None
    assert result["review_reason"] == "no_distinct_legal_entity_candidate"


def test_ambiguous_brand_name_never_selects_one_legal_entity_by_guessing():
    result = resolve_unknown_query_with_receita_candidates(
        "Affinity",
        [
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
    )

    assert result["identity_state"] == "unresolved"
    assert result["publication_state"] == "private_candidate"
    assert result["candidate_count"] == 2
    assert result["review_reason"] == "ambiguous_legal_entity_candidates"


def test_unique_receita_identity_does_not_create_risk_carrier_relationship():
    result = resolve_unknown_query_with_receita_candidates(
        "AllSafe",
        [
            {
                "cnpj": "33897006000162",
                "legal_name_receita": "PACTA GESTAO DE NEGOCIOS LTDA",
                "trade_name": "ALLSAFE",
                "primary_cnae_code": "7490104",
            }
        ],
    )

    assert result["identity_state"] == "verified_legal_entity"
    assert result["classification_state"] == "role_unresolved"
    assert result["relationship_state"] == "unresolved"
    assert result["risk_carrier"] is None
    assert result["assessment_inheritance"] == "forbidden"
    assert result["complaint_transfer_effect"] == "none"
