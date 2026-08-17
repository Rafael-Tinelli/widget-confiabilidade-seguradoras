from api.v2.sandbox_identity import materialize_unmatched_sandbox_identities


def _unmatched(*, cnpj="43095690000112", status="temporary_authorized"):
    return {
        "legal_name": "Clubfix Seguradora S.A.",
        "cnpj": cnpj,
        "edition": "2ª edição do Sandbox",
        "regulatory_status": status,
        "raw_status": "Autorizada" if status == "temporary_authorized" else "Autorização temporária cancelada",
        "authorization_start": "2023-03-21",
        "authorization_end": "2026-05-19",
        "authorization_end_raw": "19/05/2026 (Autorização prorrogada)",
        "modalities": "Seguros de Danos",
        "source": "https://www.gov.br/susep/pt-br/assuntos/sandbox-regulatorio/seguradoras-participantes-do-sandbox-1",
        "resolution": "unmatched_cnpj",
        "matched_entity_ids": [],
    }


def test_unmatched_official_sandbox_record_becomes_cnpj_identity():
    entities, unresolved = materialize_unmatched_sandbox_identities([], [_unmatched()])

    assert unresolved == []
    assert len(entities) == 1
    entity = entities[0]
    assert entity["entity_id"] == "cnpj:43095690000112"
    assert entity["fip_code"] is None
    assert entity["legal_entity_id"] == "cnpj:43095690000112"
    assert entity["entity_type"] == "insurer"
    assert entity["regulatory_regime"] == "sandbox"
    assert entity["regulatory_status"] == "temporary_authorized"
    assert entity["evidence"]["ses_present"] is False
    assert entity["evidence"]["identity_origin"] == "susep_sandbox"


def test_cancelled_sandbox_only_record_preserves_cancelled_status():
    entities, unresolved = materialize_unmatched_sandbox_identities(
        [],
        [_unmatched(cnpj="39768897000133", status="sandbox_authorization_cancelled")],
    )

    assert unresolved == []
    assert entities[0]["entity_id"] == "cnpj:39768897000133"
    assert entities[0]["regulatory_status"] == "sandbox_authorization_cancelled"


def test_ambiguous_cnpj_is_not_materialized():
    record = _unmatched()
    record["resolution"] = "ambiguous_cnpj"
    record["matched_entity_ids"] = ["fip:000111", "fip:000222"]

    entities, unresolved = materialize_unmatched_sandbox_identities([], [record])

    assert entities == []
    assert unresolved[0]["resolution"] == "ambiguous_cnpj"


def test_existing_cnpj_blocks_materialization_if_state_changed_after_resolution():
    existing = {
        "entity_id": "fip:000111",
        "fip_code": "000111",
        "cnpj": "43095690000112",
        "legal_entity_id": "cnpj:43095690000112",
        "legal_name": "ENTIDADE EXISTENTE",
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "activities": {},
        "evidence": {},
    }

    entities, unresolved = materialize_unmatched_sandbox_identities(
        [existing],
        [_unmatched()],
    )

    assert len(entities) == 1
    assert unresolved[0]["resolution"] == "cnpj_became_non_unique"
