import pytest

from api.v2.classification import (
    ClassificationConflictError,
    apply_licensed_classification,
    apply_special_regime_classification,
)


def _entity(*, fip="004120", cnpj=None):
    return {
        "entity_id": f"fip:{fip}",
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}" if cnpj else None,
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
        "evidence": {
            "ses_present": True,
            "ses_identity": {
                "legal_name": "88I SEGURADORA DIGITAL S.A.",
                "cnpj": cnpj,
            },
        },
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


def test_official_license_classifies_and_fills_missing_cnpj():
    result = apply_licensed_classification([_entity()], [_licensed()])[0]

    assert result["entity_id"] == "fip:004120"
    assert result["cnpj"] == "29846286000102"
    assert result["legal_entity_id"] == "cnpj:29846286000102"
    assert result["entity_type"] == "insurer"
    assert result["regulatory_regime"] == "ordinary"
    assert result["regulatory_status"] == "active_licensed"
    assert result["evidence"]["identity_cnpj_source"] == "susep_licensed_entities"


def test_existing_matching_cnpj_is_preserved_as_current_identity():
    result = apply_licensed_classification(
        [_entity(cnpj="29846286000102")],
        [_licensed()],
    )[0]

    assert result["entity_id"] == "fip:004120"
    assert result["cnpj"] == "29846286000102"
    assert "identity_variances" not in result["evidence"]


def test_official_cnpj_variance_is_recorded_not_silently_discarded():
    entity = _entity(fip="040851", cnpj="09438454000113")
    entity["legal_name"] = "CHUBB TEMPEST REINSURANCE LTD. ESCRITORIO DE REPRESENTACAO"
    entity["evidence"]["ses_identity"]["legal_name"] = entity["legal_name"]
    licensed = {
        **_licensed(
            fip="040851",
            cnpj="10335860000130",
            entity_type="admitted_reinsurer",
        ),
        "legal_name": "CHUBB TEMPEST REINSURANCE LTD.",
        "source_type_code": "4",
    }

    result = apply_licensed_classification([entity], [licensed])[0]

    assert result["entity_id"] == "fip:040851"
    assert result["cnpj"] == "10335860000130"
    assert result["legal_entity_id"] == "cnpj:10335860000130"
    assert result["entity_type"] == "admitted_reinsurer"
    assert result["evidence"]["identity_variances"]["cnpj"] == {
        "ses": "09438454000113",
        "licensed": "10335860000130",
    }


def test_official_licensed_record_missing_from_ses_is_added():
    result = apply_licensed_classification([], [_licensed()])

    assert len(result) == 1
    assert result[0]["entity_id"] == "fip:004120"
    assert result[0]["regulatory_status"] == "active_licensed"
    assert result[0]["evidence"]["ses_present"] is False
    assert result[0]["evidence"]["identity_origin"] == "susep_licensed_entities"


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


def test_special_regime_classifies_existing_unlicensed_record():
    special = {
        "fip_code": "003948",
        "legal_name": "SEGURADORA SA INFINITE",
        "entity_type": "insurer",
        "regulatory_regime": "special",
        "regulatory_status": "extrajudicial_liquidation",
        "source": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_extrajudicial_2011.asp",
    }

    result = apply_special_regime_classification(
        [_entity(fip="003948")],
        [special],
    )[0]

    assert result["entity_type"] == "insurer"
    assert result["regulatory_regime"] == "special"
    assert result["regulatory_status"] == "extrajudicial_liquidation"


def test_special_regime_record_missing_from_ses_is_added():
    special = {
        "fip_code": "007056",
        "legal_name": "COMPANHIA CENTRAL DE SEGUROS",
        "entity_type": "insurer",
        "regulatory_regime": "special",
        "regulatory_status": "ordinary_liquidation",
        "source": "https://www2.susep.gov.br/menuatendimento/regimesespeciais/liq_ordinaria_2011.asp",
    }

    result = apply_special_regime_classification([], [special])[0]

    assert result["entity_id"] == "fip:007056"
    assert result["evidence"]["ses_present"] is False
    assert result["evidence"]["identity_origin"] == "susep_special_regimes"


def test_same_fip_cannot_be_current_ordinary_and_special_regime():
    ordinary = apply_licensed_classification([_entity()], [_licensed()])
    special = {
        "fip_code": "004120",
        "legal_name": "88I SEGURADORA DIGITAL S.A.",
        "entity_type": "insurer",
        "regulatory_regime": "special",
        "regulatory_status": "intervention",
        "source": "special-source",
    }

    with pytest.raises(ClassificationConflictError, match="simultaneously"):
        apply_special_regime_classification(ordinary, [special])
