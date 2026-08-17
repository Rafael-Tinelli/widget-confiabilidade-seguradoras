import pytest

from api.v2.relationships import (
    RelationshipConflictError,
    apply_corporate_relationships,
    apply_economic_groups,
    materialize_brands,
)


def _entity(entity_id, fip, cnpj, name):
    return {
        "entity_id": entity_id,
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}" if cnpj else None,
        "legal_name": name,
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "activities": {},
        "evidence": {},
    }


def test_incorporation_creates_forward_and_inverse_relationships():
    entities = [
        _entity("fip:005282", "005282", "33061813000140", "OLD PRUDENTIAL"),
        _entity("fip:004367", "004367", "21986074000119", "CURRENT PRUDENTIAL"),
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
        ]
    }

    enriched, resolved = apply_corporate_relationships(entities, registry)
    by_id = {item["entity_id"]: item for item in enriched}

    old_rel = by_id["fip:005282"]["relationships"][0]
    new_rel = by_id["fip:004367"]["relationships"][0]
    assert old_rel["relationship_type"] == "incorporated_into"
    assert old_rel["target_entity_id"] == "fip:004367"
    assert new_rel["relationship_type"] == "successor_of"
    assert new_rel["target_entity_id"] == "fip:005282"
    assert resolved[0]["effective_date"] == "2024-11-01"


def test_verified_relationship_fails_if_target_is_missing():
    entities = [_entity("fip:005282", "005282", "33061813000140", "OLD")]
    registry = {
        "corporate_relationships": [
            {
                "relationship_type": "incorporated_into",
                "source_cnpj": "33061813000140",
                "target_cnpj": "21986074000119",
            }
        ]
    }

    with pytest.raises(RelationshipConflictError, match="could not be resolved"):
        apply_corporate_relationships(entities, registry)


def test_group_membership_is_attached_by_fip():
    entities = [_entity("fip:004367", "004367", "21986074000119", "PRUDENTIAL")]
    groups = [
        {
            "fip_code": "004367",
            "group_code": "123",
            "group_name": "GRUPO PRUDENTIAL",
            "source": "SUSEP SES / Ses_cias.csv",
        }
    ]

    enriched, catalog = apply_economic_groups(entities, groups)

    assert enriched[0]["economic_group"]["group_id"] == "susep-group:123"
    assert enriched[0]["relationships"][0]["relationship_type"] == "member_of_group"
    assert catalog[0]["member_entity_ids"] == ["fip:004367"]


def test_brand_resolves_target_without_inheriting_score():
    entities = [_entity("fip:004367", "004367", "21986074000119", "PRUDENTIAL")]
    registry = {
        "brands": [
            {
                "brand_id": "brand:prudential",
                "name": "Prudential",
                "aliases": ["Prudential do Brasil"],
                "relationships": [
                    {
                        "relationship_type": "brand_of",
                        "target_cnpj": "21986074000119",
                        "status": "current",
                    }
                ],
            }
        ]
    }

    brands = materialize_brands(entities, registry)

    assert brands[0]["relationships"][0]["target_entity_id"] == "fip:004367"
    assert "score" not in brands[0]
