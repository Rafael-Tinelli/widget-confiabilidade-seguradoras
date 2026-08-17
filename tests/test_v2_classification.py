import pytest

from api.v2.classification import (
    ClassificationConflictError,
    apply_licensed_classification,
)


def _entity(*, fip="004120", cnpj=None, entity_id=None):
    return {
        "entity_id": entity_id or (f"cnpj:{cnpj}" if cnpj else f"fip:{fip}"),
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_name": "88I SEGURADORA DIGITAL S.A.",
        "entity_type": "unknown",
        "regulatory_regime": "unknown",
        "regulatory_status": "unknown",
        "activities": {
            "insurance": True,
            "pension": False,
            "capitalization": False,
            "reinsurance": False,
        },
        "evidence": {"ses_present": True},
    }


def _licensed(*, fip="004120", cnpj="29846286000102", entity_type="insurer"):
    return {
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_name": "88I SEGURADORA DIGITAL S.A.",
        "entity_type": entity_type,
        "source_type_code": "2",
        "source": "https://www2.susep.gov.br/menuatendimento/procura_2011.asp",
    }


def test_official_license_classifies_and_promotes_missing_cnpj():
    result = apply_licensed_classification([_entity()], [_licensed()])[0]

    assert result["entity_id"] == "cnpj:29846286000102"
    assert result["cnpj"] == "29846286000102"
    assert result["entity_type"] == "insurer"
    assert result["regulatory_regime"] == "ordinary"
    assert result["regulatory_status"] == "active_licensed"
    assert result["evidence"]["identity_cnpj_source"] == "susep_licensed_entities"


def test_existing_matching_cnpj_is_preserved():
    result = apply_licensed_classification(
        [_entity(cnpj="29846286000102")],
        [_licensed()],
    )[0]

    assert result["entity_id"] == "cnpj:29846286000102"
    assert result["evidence"].get("identity_cnpj_source") is None


def test_cnpj_conflict_fails_closed():
    with pytest.raises(ClassificationConflictError, match="CNPJ conflict"):
        apply_licensed_classification(
            [_entity(cnpj="11111111000111")],
            [_licensed()],
        )


def test_unmatched_ses_entity_remains_unknown():
    result = apply_licensed_classification([_entity(fip="009999")], [_licensed()])[0]

    assert result["entity_type"] == "unknown"
    assert result["regulatory_regime"] == "unknown"
    assert result["regulatory_status"] == "unknown"


def test_same_fip_in_conflicting_official_types_fails_closed():
    with pytest.raises(ClassificationConflictError, match="Conflicting licensed records"):
        apply_licensed_classification(
            [_entity()],
            [_licensed(entity_type="insurer"), _licensed(entity_type="local_reinsurer")],
        )
