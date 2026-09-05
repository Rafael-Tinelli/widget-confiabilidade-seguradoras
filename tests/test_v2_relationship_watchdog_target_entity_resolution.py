from api.v2.relationship_watchdog import build_relationship_watchdog


def test_sandbox_registry_resolves_canonical_carrier_from_target_entity_id():
    lifecycle = {
        "artifact": "v2_lifecycle_relationship_inventory",
        "entities": [
            {
                "entity_id": "cnpj:47006254000180",
                "cnpj": "47006254000180",
                "legal_name": "LTI Seguros S.A.",
                "query_context": {
                    "entity_state": "sandbox_experimental_participant",
                    "filter_bucket": "sandbox",
                },
                "relationships": [],
            }
        ],
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros", "Loovi Technology"],
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_entity_id": "cnpj:47006254000180",
                        "status": "current",
                        "evidence": {"authority": "Loovi"},
                    }
                ],
            }
        ],
        "groups": [],
        "corporate_relationships": [],
    }
    sandbox_registry = {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "representative_cnpj": "34504257000100",
                "risk_carrier_cnpj": "47006254000180",
                "risk_carrier_name": "LTI Seguros S.A.",
                "regulatory_scope": "sandbox",
                "conduct_context_policy": (
                    "carrier_level_context_not_brand_exclusive_attribution"
                ),
            }
        ]
    }

    payload = build_relationship_watchdog(
        lifecycle,
        sandbox_registry=sandbox_registry,
    )

    assert payload["summary"]["blocking_registry_drift_count"] == 0
    assert not any(
        candidate["candidate_type"] == "sandbox_brand_carrier_registry_drift"
        for candidate in payload["candidates"]
    )
