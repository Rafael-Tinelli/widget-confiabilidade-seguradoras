from api.v2.build_eligibility_inventory import build_eligibility_inventory


def _payload():
    return {
        "meta": {"entities_total": 3},
        "unresolved": {},
        "groups": [],
        "brands": [],
        "corporate_relationships": [],
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "000001",
                "cnpj": "11111111000191",
                "legal_name": "SEGURADORA A S.A.",
                "entity_type": "insurer",
                "regulatory_regime": "ordinary",
                "regulatory_status": "active_licensed",
                "legal_lifecycle": {"cadastral_status": "active"},
                "relationships": [],
                "query_context": {"filter_bucket": "insurers"},
            },
            {
                "entity_id": "cnpj:43095690000112",
                "cnpj": "43095690000112",
                "legal_name": "SANDBOX TESTE S.A.",
                "entity_type": "sandbox_participant",
                "regulatory_regime": "sandbox",
                "regulatory_status": "sandbox_authorized",
                "relationships": [],
                "query_context": {"filter_bucket": "sandbox"},
            },
            {
                "entity_id": "fip:000003",
                "fip_code": "000003",
                "cnpj": "33333333000193",
                "legal_name": "CAPITALIZACAO TESTE S.A.",
                "entity_type": "capitalization_company",
                "regulatory_regime": "ordinary",
                "regulatory_status": "active_licensed",
                "legal_lifecycle": {"cadastral_status": "active"},
                "relationships": [],
                "query_context": {"filter_bucket": "capitalization"},
            },
        ],
    }


def test_builder_materializes_only_regulatory_gate_as_eligible():
    payload = build_eligibility_inventory(_payload())
    meta = payload["meta"]

    assert payload["artifact"] == "v2_entity_eligibility_inventory"
    assert meta["regulatory_universe_eligible_count"] == 1
    assert meta["assessment_eligible_count"] == 0
    assert meta["ranking_eligible_count"] == 0

    eligible = [
        entity
        for entity in payload["entities"]
        if entity["eligibility"]["regulatory_universe_eligible"]
    ]
    assert [entity["entity_id"] for entity in eligible] == ["fip:000001"]
    assert eligible[0]["eligibility"]["comparison_cohort"] is None
