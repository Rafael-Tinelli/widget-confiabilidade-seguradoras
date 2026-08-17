import pytest

from api.v2.classification import (
    ClassificationConflictError,
    apply_sandbox_classification,
)


def _entity(*, fip, cnpj, status="unknown", regime="unknown", entity_type="unknown"):
    return {
        "entity_id": f"fip:{fip}",
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}" if cnpj else None,
        "legal_name": "ENTIDADE TESTE",
        "entity_type": entity_type,
        "regulatory_regime": regime,
        "regulatory_status": status,
        "activities": {
            "insurance": False,
            "pension": False,
            "capitalization": False,
            "reinsurance": False,
        },
        "evidence": {"ses_present": True},
    }


def _sandbox(*, cnpj, status="temporary_authorized"):
    return {
        "legal_name": "SANDBOX TESTE S.A.",
        "cnpj": cnpj,
        "edition": "2ª edição do Sandbox",
        "regulatory_status": status,
        "raw_status": "Autorizada" if status == "temporary_authorized" else "Autorização temporária cancelada",
        "authorization_start": "2023-01-01",
        "authorization_end": "2026-12-31",
        "authorization_end_raw": "31/12/2026",
        "modalities": "Seguros de Danos",
        "source": "https://www.gov.br/susep/sandbox",
    }


def test_active_sandbox_applies_only_by_exact_cnpj():
    entities = [_entity(fip="000111", cnpj="43095690000112")]
    classified, unresolved = apply_sandbox_classification(
        entities,
        [_sandbox(cnpj="43095690000112")],
    )

    assert unresolved == []
    assert classified[0]["entity_id"] == "fip:000111"
    assert classified[0]["entity_type"] == "insurer"
    assert classified[0]["regulatory_regime"] == "sandbox"
    assert classified[0]["regulatory_status"] == "temporary_authorized"
    assert classified[0]["evidence"]["sandbox"]["cnpj"] == "43095690000112"


def test_unmatched_sandbox_cnpj_remains_unresolved_without_fip_invention():
    classified, unresolved = apply_sandbox_classification(
        [],
        [_sandbox(cnpj="43095690000112")],
    )

    assert classified == []
    assert unresolved[0]["resolution"] == "unmatched_cnpj"
    assert unresolved[0]["matched_entity_ids"] == []


def test_duplicate_entity_cnpj_makes_sandbox_match_ambiguous():
    entities = [
        _entity(fip="000111", cnpj="43095690000112"),
        _entity(fip="000222", cnpj="43095690000112"),
    ]
    classified, unresolved = apply_sandbox_classification(
        entities,
        [_sandbox(cnpj="43095690000112")],
    )

    assert len(classified) == 2
    assert unresolved[0]["resolution"] == "ambiguous_cnpj"
    assert set(unresolved[0]["matched_entity_ids"]) == {"fip:000111", "fip:000222"}


def test_cancelled_sandbox_does_not_downgrade_later_ordinary_license():
    entities = [
        _entity(
            fip="000111",
            cnpj="39768897000133",
            status="active_licensed",
            regime="ordinary",
            entity_type="insurer",
        )
    ]
    classified, unresolved = apply_sandbox_classification(
        entities,
        [_sandbox(cnpj="39768897000133", status="sandbox_authorization_cancelled")],
    )

    assert unresolved == []
    assert classified[0]["regulatory_status"] == "active_licensed"
    assert classified[0]["regulatory_regime"] == "ordinary"
    assert classified[0]["evidence"]["sandbox"]["regulatory_status"] == "sandbox_authorization_cancelled"


def test_current_sandbox_and_current_ordinary_license_conflict_fails_closed():
    entities = [
        _entity(
            fip="000111",
            cnpj="43095690000112",
            status="active_licensed",
            regime="ordinary",
            entity_type="insurer",
        )
    ]

    with pytest.raises(ClassificationConflictError, match="Sandbox CNPJ"):
        apply_sandbox_classification(
            entities,
            [_sandbox(cnpj="43095690000112")],
        )
