from api.v2.build_lifecycle_relationship_inventory import (
    build_lifecycle_relationship_inventory,
)


def _classification_payload():
    return {
        "meta": {"inventory_count": 2},
        "unresolved": {"sandbox": []},
        "entities": [
            {
                "entity_id": "fip:005282",
                "fip_code": "005282",
                "cnpj": "33061813000140",
                "legal_entity_id": "cnpj:33061813000140",
                "legal_name": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A.",
                "entity_type": "unknown",
                "regulatory_regime": "unknown",
                "regulatory_status": "unknown",
                "activities": {},
                "evidence": {},
            },
            {
                "entity_id": "fip:004367",
                "fip_code": "004367",
                "cnpj": "21986074000119",
                "legal_entity_id": "cnpj:21986074000119",
                "legal_name": "PRUDENTIAL DO BRASIL SEGUROS S.A.",
                "entity_type": "insurer",
                "regulatory_regime": "ordinary",
                "regulatory_status": "active_licensed",
                "activities": {},
                "evidence": {},
            },
        ],
    }


def test_historical_incorporated_entity_points_to_current_successor():
    lifecycle = [
        {
            "cnpj": "33061813000140",
            "legal_name": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A.",
            "cadastral_status": "closed",
            "status_date": "2024-11-01",
            "status_reason": "incorporation",
            "raw_status": "BAIXADA",
            "raw_reason": "Incorporação",
            "source_authority": "Receita Federal",
            "source_document": "Comprovante",
            "source_mode": "verified_snapshot",
            "observed_at": "2026-08-17",
        }
    ]
    registry = {
        "corporate_relationships": [
            {
                "relationship_type": "incorporated_into",
                "source_cnpj": "33061813000140",
                "target_cnpj": "21986074000119",
                "effective_date": "2024-11-01",
                "evidence": {"authority": "SUSEP"},
            }
        ],
        "brands": [],
    }
    groups = [
        {
            "fip_code": "004367",
            "group_code": "77",
            "group_name": "GRUPO PRUDENTIAL",
            "observed_period": "202606",
            "is_specific_group": True,
            "source": "SUSEP SES / Ses_grupos_economicos.csv",
        }
    ]

    payload = build_lifecycle_relationship_inventory(
        _classification_payload(), lifecycle, registry, groups
    )
    by_id = {item["entity_id"]: item for item in payload["entities"]}

    old = by_id["fip:005282"]
    current = by_id["fip:004367"]
    assert old["query_context"]["entity_state"] == "historical_incorporated_entity"
    assert old["query_context"]["immediate_successor_entity_id"] == "fip:004367"
    assert old["query_context"]["successor_entity_id"] == "fip:004367"
    assert old["query_context"]["successor_chain"] == ["fip:004367"]
    assert old["query_context"]["score_behavior"] == "do_not_score_historical_entity"
    assert current["query_context"]["entity_state"] == "current_ordinary_insurer"
    assert current["economic_group"]["group_name"] == "GRUPO PRUDENTIAL"


def test_successor_chain_routes_through_intermediate_historical_entity():
    classification = {
        "meta": {"inventory_count": 3},
        "unresolved": {"sandbox": []},
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "000001",
                "cnpj": "11111111000111",
                "legal_entity_id": "cnpj:11111111000111",
                "legal_name": "OLD A",
                "entity_type": "unknown",
                "regulatory_regime": "unknown",
                "regulatory_status": "unknown",
                "activities": {},
                "evidence": {},
            },
            {
                "entity_id": "fip:000002",
                "fip_code": "000002",
                "cnpj": "22222222000122",
                "legal_entity_id": "cnpj:22222222000122",
                "legal_name": "INTERMEDIATE B",
                "entity_type": "unknown",
                "regulatory_regime": "unknown",
                "regulatory_status": "unknown",
                "activities": {},
                "evidence": {},
            },
            {
                "entity_id": "fip:000003",
                "fip_code": "000003",
                "cnpj": "33333333000133",
                "legal_entity_id": "cnpj:33333333000133",
                "legal_name": "CURRENT C",
                "entity_type": "insurer",
                "regulatory_regime": "ordinary",
                "regulatory_status": "active_licensed",
                "activities": {},
                "evidence": {},
            },
        ],
    }
    registry = {
        "corporate_relationships": [
            {
                "relationship_type": "incorporated_into",
                "source_cnpj": "11111111000111",
                "target_cnpj": "22222222000122",
                "effective_date": "2020-01-01",
                "evidence": {"authority": "TEST"},
            },
            {
                "relationship_type": "incorporated_into",
                "source_cnpj": "22222222000122",
                "target_cnpj": "33333333000133",
                "effective_date": "2022-01-01",
                "evidence": {"authority": "TEST"},
            },
        ],
        "brands": [],
    }

    payload = build_lifecycle_relationship_inventory(classification, [], registry, [])
    by_id = {item["entity_id"]: item for item in payload["entities"]}
    context = by_id["fip:000001"]["query_context"]

    assert context["entity_state"] == "historical_incorporated_entity"
    assert context["immediate_successor_entity_id"] == "fip:000002"
    assert context["successor_entity_id"] == "fip:000003"
    assert context["successor_chain"] == ["fip:000002", "fip:000003"]
    assert context["lifecycle_evidence"] == "corporate_relationship"


def test_loovi_brand_materializes_to_sandbox_carrier_without_score_inheritance():
    classification = {
        "meta": {"inventory_count": 1},
        "unresolved": {"sandbox": []},
        "entities": [
            {
                "entity_id": "sandbox:lti",
                "fip_code": None,
                "cnpj": "47006254000180",
                "legal_entity_id": "cnpj:47006254000180",
                "legal_name": "LTI SEGUROS S.A.",
                "display_name": "LTI Seguros",
                "entity_type": "sandbox_participant",
                "regulatory_regime": "sandbox",
                "regulatory_status": "sandbox_authorized",
                "activities": {},
                "evidence": {},
            }
        ],
    }
    registry = {
        "corporate_relationships": [],
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros", "Loovi Technology"],
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_cnpj": "47006254000180",
                        "status": "current",
                        "evidence": {"authority": "Loovi"},
                    }
                ],
            }
        ],
    }

    payload = build_lifecycle_relationship_inventory(classification, [], registry, [])

    brand = payload["brands"][0]
    assert brand["brand_id"] == "brand:loovi"
    assert "Loovi Seguros" in brand["aliases"]
    assert brand["relationships"][0]["target_entity_id"] == "sandbox:lti"
    assert "score" not in brand

    carrier = payload["entities"][0]
    assert carrier["query_context"]["entity_state"] == "sandbox_experimental_participant"
    assert carrier["query_context"]["filter_bucket"] == "sandbox"
    assert carrier["query_context"]["score_behavior"] == (
        "never_compare_with_ordinary_insurers"
    )
