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


def test_specific_group_membership_is_attached_by_fip():
    entities = [_entity("fip:005886", "005886", "61198164000160", "PORTO")]
    groups = [
        {
            "fip_code": "005886",
            "group_code": "00051",
            "group_name": "PORTO SEGURO",
            "observed_period": "202606",
            "is_specific_group": True,
            "source": "SUSEP SES / Ses_grupos_economicos.csv",
        }
    ]

    enriched, catalog = apply_economic_groups(entities, groups)

    assert enriched[0]["economic_group"]["group_id"] == "susep-group:00051"
    assert enriched[0]["economic_group"]["observed_period"] == "202606"
    assert enriched[0]["relationships"][0]["relationship_type"] == "member_of_group"
    assert catalog[0]["member_entity_ids"] == ["fip:005886"]


def test_generic_group_bucket_is_evidence_not_corporate_relationship():
    entities = [_entity("fip:004367", "004367", "21986074000119", "PRUDENTIAL")]
    groups = [
        {
            "fip_code": "004367",
            "group_code": "01225",
            "group_name": "INDEPENDENTE",
            "observed_period": "202606",
            "is_specific_group": False,
            "source": "SUSEP SES / Ses_grupos_economicos.csv",
        }
    ]

    enriched, catalog = apply_economic_groups(entities, groups)

    assert catalog == []
    assert "economic_group" not in enriched[0]
    assert enriched[0]["evidence"]["ses_economic_group"]["group_name"] == "INDEPENDENTE"
    assert enriched[0]["evidence"]["ses_economic_group"]["is_specific_group"] is False


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


def test_verified_market_identity_survives_brand_materialization():
    entities = [
        {
            **_entity("sandbox:lti", None, "47006254000180", "LTI SEGUROS S.A."),
            "entity_type": "sandbox_participant",
            "regulatory_regime": "sandbox",
            "regulatory_status": "temporary_authorized",
        }
    ]
    registry = {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros"],
                "market_identity": {
                    "kind": "insurance_distribution_company",
                    "public_label": "Representante de seguros",
                    "legal_name": "CW TECHNOLOGY LTDA.",
                    "cnpj": "34.504.257/0001-00",
                    "distribution_role": "insurance_representative",
                    "distribution_role_state": "verified",
                    "evidence": {"authority": "Loovi", "fact": "verified"},
                },
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_cnpj": "47006254000180",
                        "status": "current",
                    }
                ],
            }
        ]
    }

    brand = materialize_brands(entities, registry)[0]

    assert brand["market_identity"]["legal_name"] == "CW TECHNOLOGY LTDA."
    assert brand["market_identity"]["cnpj"] == "34504257000100"
    assert brand["market_identity"]["distribution_role"] == "insurance_representative"
    assert brand["market_identity"]["distribution_role_state"] == "verified"
    assert brand["relationships"][0]["target_entity_id"] == "sandbox:lti"
    assert "score" not in brand


def test_verified_distribution_role_requires_evidence():
    entities = [
        {
            **_entity("sandbox:lti", None, "47006254000180", "LTI SEGUROS S.A."),
            "entity_type": "sandbox_participant",
            "regulatory_regime": "sandbox",
        }
    ]
    registry = {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "market_identity": {
                    "legal_name": "CW TECHNOLOGY LTDA.",
                    "cnpj": "34504257000100",
                    "distribution_role": "insurance_representative",
                    "distribution_role_state": "verified",
                },
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_cnpj": "47006254000180",
                    }
                ],
            }
        ]
    }

    with pytest.raises(RelationshipConflictError, match="requires evidence"):
        materialize_brands(entities, registry)
