import pytest

from api.sources.susep_licensed import (
    LicensedEntitiesSourceError,
    _assert_known_official_taxonomy,
)
from api.v2.build_lifecycle_relationship_inventory import _derive_query_context
from api.v2.classification import apply_licensed_classification
from api.v2.regulatory_scope import (
    SPECIAL_PURPOSE_INSURER_SUBTYPE,
    infer_regulatory_subtype,
    is_current_ordinary_consumer_insurer,
)


def _licensed(fip: str, cnpj: str, name: str):
    return {
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_name": name,
        "entity_type": "insurer",
        "source_type_code": "2",
        "source": "https://www2.susep.gov.br/menuatendimento/procura_2011.asp",
    }


def test_sspe_subtype_is_derived_from_official_legal_name_not_fip_allowlist():
    entity = {
        "entity_type": "insurer",
        "legal_name": "NOVA SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.",
    }
    assert infer_regulatory_subtype(entity) == SPECIAL_PURPOSE_INSURER_SUBTYPE


def test_qi_style_new_ordinary_insurer_missing_from_ses_enters_naturally():
    result = apply_licensed_classification(
        [],
        [_licensed("003662", "63851104000120", "QI SEGURADORA S.A.")],
    )[0]

    assert result["entity_id"] == "fip:003662"
    assert result["evidence"]["identity_origin"] == "susep_licensed_entities"
    assert result["evidence"]["ses_present"] is False
    assert result.get("regulatory_subtype") is None
    assert is_current_ordinary_consumer_insurer(result) is True


def test_btg_style_new_sspe_missing_from_ses_is_retained_but_outside_comparator():
    result = apply_licensed_classification(
        [],
        [
            _licensed(
                "003191",
                "63512576000158",
                "BTG PACTUAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.",
            )
        ],
    )[0]

    assert result["entity_type"] == "insurer"
    assert result["regulatory_status"] == "active_licensed"
    assert result["regulatory_subtype"] == SPECIAL_PURPOSE_INSURER_SUBTYPE
    assert result["evidence"]["identity_origin"] == "susep_licensed_entities"
    assert is_current_ordinary_consumer_insurer(result) is False


def test_existing_sspe_with_ses_history_is_also_classified_by_same_rule():
    ses_entity = {
        "entity_id": "fip:002747",
        "fip_code": "002747",
        "cnpj": "52477097000121",
        "legal_entity_id": "cnpj:52477097000121",
        "legal_name": "GALÁPAGOS CAPITAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.",
        "entity_type": "unknown",
        "regulatory_regime": "unknown",
        "regulatory_status": "unknown",
        "activities": {},
        "evidence": {"ses_present": True, "ses_identity": {}},
    }
    result = apply_licensed_classification(
        [ses_entity],
        [
            _licensed(
                "002747",
                "52477097000121",
                "GALÁPAGOS CAPITAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.",
            )
        ],
    )[0]
    assert result["regulatory_subtype"] == SPECIAL_PURPOSE_INSURER_SUBTYPE


def test_sspe_gets_searchable_non_comparator_query_context():
    entity = {
        "entity_id": "fip:003191",
        "fip_code": "003191",
        "cnpj": "63512576000158",
        "legal_name": "BTG PACTUAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A.",
        "entity_type": "insurer",
        "regulatory_subtype": SPECIAL_PURPOSE_INSURER_SUBTYPE,
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "relationships": [],
    }
    context = _derive_query_context([entity])[0]["query_context"]
    assert context["entity_state"] == "special_purpose_insurer"
    assert context["filter_bucket"] == "other"
    assert context["score_behavior"] == "outside_consumer_insurer_comparator"


def test_new_official_susep_type_fails_closed_instead_of_being_ignored():
    document = """
    <select name="tiposempresas">
      <option value="1">Entidade aberta de previdência</option>
      <option value="2">Seguradora</option>
      <option value="9">Nova categoria supervisionada</option>
    </select>
    """
    with pytest.raises(LicensedEntitiesSourceError, match="taxonomy changed"):
        _assert_known_official_taxonomy(document)
