import pytest

from api.v2.lifecycle import LifecycleConflictError, apply_legal_lifecycle


def _entity(*, cnpj, status="unknown"):
    return {
        "entity_id": "fip:005282",
        "fip_code": "005282",
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}",
        "legal_name": "PRUDENTIAL DO BRASIL SEGUROS DE VIDA S.A.",
        "entity_type": "unknown",
        "regulatory_regime": "unknown",
        "regulatory_status": status,
        "activities": {},
        "evidence": {},
    }


def _record(cnpj="33061813000140"):
    return {
        "cnpj": cnpj,
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


def test_lifecycle_does_not_overwrite_regulatory_status():
    entities, unresolved = apply_legal_lifecycle(
        [_entity(cnpj="33061813000140")],
        [_record()],
    )

    assert unresolved == []
    assert entities[0]["regulatory_status"] == "unknown"
    assert entities[0]["legal_lifecycle"]["cadastral_status"] == "closed"
    assert entities[0]["legal_lifecycle"]["status_reason"] == "incorporation"
    assert entities[0]["evidence"]["receita_cnpj"]["source_authority"] == "Receita Federal"


def test_closed_receita_record_conflicts_with_active_susep_same_cnpj():
    with pytest.raises(LifecycleConflictError, match="active_licensed"):
        apply_legal_lifecycle(
            [_entity(cnpj="33061813000140", status="active_licensed")],
            [_record()],
        )


def test_unmatched_receita_record_is_auditable():
    entities, unresolved = apply_legal_lifecycle([], [_record()])

    assert entities == []
    assert unresolved[0]["resolution"] == "unmatched_cnpj"
