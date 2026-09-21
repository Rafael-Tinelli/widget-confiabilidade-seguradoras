from api.v2.public_profile_regulatory_semantics import (
    apply_market_relationship_semantics,
)


def _summary(complaints):
    return {
        "complaints": complaints,
        "responded": complaints,
        "response_rate": 1.0 if complaints else None,
        "finalized": complaints,
        "finalized_rate": 1.0 if complaints else None,
        "satisfaction_count": complaints,
        "average_satisfaction": 4.0 if complaints else None,
        "provider_labels": [],
        "film": {
            "history_state": "observed",
            "months_observed": 12,
            "months_with_complaints": 1 if complaints else 0,
            "satisfaction_trend": {},
        },
    }


def test_public_context_is_not_sandbox_specific_and_preserves_multi_carrier_split():
    contract = {
        "artifact": "v2_public_search_profile_contract",
        "version": "old",
        "profiles": [
            {
                "profile_id": "brand:multi",
                "profile_kind": "brand",
                "identity": {"name": "Multi"},
                "limits": [],
            }
        ],
        "search_index": [],
        "population": {},
        "publication_policy": {},
    }
    market_conduct = {
        "artifact": "v2_market_relationship_conduct_evidence",
        "brands": [
            {
                "brand_id": "brand:multi",
                "market_identity": {
                    "identity_state": "verified",
                    "public_label": "Representante de seguros",
                    "legal_name": "DISTRIBUIDORA MULTI LTDA.",
                    "cnpj": "33333333000133",
                    "distribution_role": "insurance_representative",
                    "distribution_role_state": "verified",
                    "evidence": {"authority": "Fonte"},
                },
                "distribution_role": "insurance_representative",
                "distribution_role_state": "verified",
                "market_identity_conduct_summary": _summary(3),
                "risk_bearers": [
                    {
                        "profile_id": "entity:a",
                        "entity_id": "a",
                        "name": "SEGURADORA A",
                        "cnpj": "11111111000111",
                        "entity_type": "insurer",
                        "regulatory_regime": "ordinary",
                        "regulatory_status": "active_licensed",
                        "regulatory_class": "Seguradora",
                        "relationship_status": "current",
                        "product_scope": "produto A",
                        "evidence": {"authority": "Fonte A"},
                        "conduct_summary": _summary(5),
                    },
                    {
                        "profile_id": "entity:b",
                        "entity_id": "b",
                        "name": "SEGURADORA B",
                        "cnpj": "22222222000122",
                        "entity_type": "sandbox_participant",
                        "regulatory_regime": "sandbox",
                        "regulatory_status": "temporary_authorized",
                        "regulatory_class": "Participante do Sandbox regulatório",
                        "relationship_status": "current",
                        "product_scope": "produto B",
                        "evidence": {"authority": "Fonte B"},
                        "conduct_summary": _summary(7),
                    },
                ],
            }
        ],
    }

    output = apply_market_relationship_semantics(contract, market_conduct)
    profile = output["profiles"][0]
    context = profile["market_relationship_context"]

    assert context["multi_carrier"] is True
    assert context["distributor"]["legal_name"] == "DISTRIBUIDORA MULTI LTDA."
    assert context["distributor_conduct"]["metrics"]["complaints"]["value"] == 3
    assert len(context["risk_bearers"]) == 2
    assert context["risk_bearers"][0]["regulatory_class"] == "Seguradora"
    assert context["risk_bearers"][0]["conduct_context"]["metrics"]["complaints"]["value"] == 5
    assert context["risk_bearers"][1]["regulatory_class"] == (
        "Participante do Sandbox regulatório"
    )
    assert context["risk_bearers"][1]["conduct_context"]["metrics"]["complaints"]["value"] == 7

    policy = output["publication_policy"]
    assert policy["distribution_relationship_model_is_regime_agnostic"] is True
    assert policy["risk_bearer_relation_never_transfers_complaints"] is True
    assert policy["multi_carrier_relationships_must_remain_separate"] is True
    assert output["population"]["multi_risk_bearer_brand_profiles"] == 1


def test_graduated_risk_bearer_context_warns_about_mixed_regulatory_history():
    contract = {
        "artifact": "v2_public_search_profile_contract",
        "version": "old",
        "profiles": [
            {
                "profile_id": "brand:loovi",
                "profile_kind": "brand",
                "identity": {"name": "Loovi"},
                "limits": [],
            }
        ],
        "search_index": [],
        "population": {},
        "publication_policy": {},
    }
    market_conduct = {
        "artifact": "v2_market_relationship_conduct_evidence",
        "brands": [
            {
                "brand_id": "brand:loovi",
                "market_identity": {
                    "identity_state": "verified",
                    "legal_name": "CW TECHNOLOGY LTDA.",
                    "cnpj": "00000000000100",
                    "distribution_role": "insurance_representative",
                    "distribution_role_state": "verified",
                    "evidence": {"authority": "Fonte"},
                },
                "market_identity_conduct_summary": _summary(17),
                "risk_bearers": [
                    {
                        "profile_id": "entity:fip:001988",
                        "entity_id": "fip:001988",
                        "name": "LTI SEGUROS S.A.",
                        "cnpj": "47006254000180",
                        "entity_type": "insurer",
                        "regulatory_regime": "ordinary",
                        "regulatory_status": "active_licensed",
                        "regulatory_class": "Seguradora",
                        "sandbox_history": {
                            "current_outcome": "graduated_to_ordinary_license",
                        },
                        "relationship_status": "current",
                        "conduct_summary": _summary(1329),
                    }
                ],
            }
        ],
    }

    output = apply_market_relationship_semantics(contract, market_conduct)
    bearer = output["profiles"][0]["market_relationship_context"]["risk_bearers"][0]

    assert bearer["regulatory_class"] == "Seguradora"
    assert bearer["regulatory_history"]["sandbox"]["current_outcome"] == (
        "graduated_to_ordinary_license"
    )
    assert "pode incluir observações anteriores" in bearer["conduct_context"][
        "plain_language"
    ]
