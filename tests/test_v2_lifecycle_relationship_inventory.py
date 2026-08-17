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
    assert old["query_context"]["successor_entity_id"] == "fip:004367"
    assert old["query_context"]["score_behavior"] == "do_not_score_historical_entity"
    assert current["query_context"]["entity_state"] == "current_ordinary_insurer"
    assert current["economic_group"]["group_name"] == "GRUPO PRUDENTIAL"
