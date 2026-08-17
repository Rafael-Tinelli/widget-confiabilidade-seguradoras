from api.v2.sandbox_semantics import normalize_sandbox_entity_semantics


def test_sandbox_participant_is_not_typed_as_insurer():
    entities = [
        {
            "entity_id": "cnpj:43095690000112",
            "fip_code": None,
            "cnpj": "43095690000112",
            "entity_type": "insurer",
            "regulatory_regime": "sandbox",
            "regulatory_status": "temporary_authorized",
        }
    ]

    normalized = normalize_sandbox_entity_semantics(entities)

    assert normalized[0]["entity_type"] == "sandbox_participant"


def test_ordinary_insurer_keeps_insurer_type():
    entities = [
        {
            "entity_id": "fip:005886",
            "fip_code": "005886",
            "cnpj": "61198164000160",
            "entity_type": "insurer",
            "regulatory_regime": "ordinary",
            "regulatory_status": "active_licensed",
        }
    ]

    normalized = normalize_sandbox_entity_semantics(entities)

    assert normalized[0]["entity_type"] == "insurer"
