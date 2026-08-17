from __future__ import annotations

import pytest

from api.v2.identity import (
    IdentityConflictError,
    build_canonical_entities,
    build_canonical_identity,
    canonical_fip_code,
)


def test_canonical_fip_code_normalizes_common_ses_forms() -> None:
    assert canonical_fip_code("5177") == "005177"
    assert canonical_fip_code("005177") == "005177"
    assert canonical_fip_code("ses:5177") == "005177"
    assert canonical_fip_code(5177) == "005177"
    assert canonical_fip_code("5177.0") == "005177"


def test_identity_uses_fip_as_stable_regulatory_entity_id() -> None:
    identity = build_canonical_identity(
        {
            "id": "005177",
            "name": "ALLIANZ SEGUROS S.A.",
            "cnpj": "61.573.796/0001-66",
            "sources_found": ["SEGUROS", "PATRIMONIO"],
        }
    )

    assert identity.entity_id == "fip:005177"
    assert identity.fip_code == "005177"
    assert identity.cnpj == "61573796000166"
    assert identity.legal_entity_id == "cnpj:61573796000166"
    assert identity.legal_name == "ALLIANZ SEGUROS S.A."
    assert identity.activities == {
        "insurance": True,
        "pension": False,
        "capitalization": False,
        "reinsurance": False,
    }


def test_activity_evidence_does_not_infer_legal_classification() -> None:
    identity = build_canonical_identity(
        {
            "id": "000123",
            "name": "ENTIDADE EXEMPLO",
            "sources_found": ["PREVIDENCIA", "CAPITALIZACAO"],
        }
    )

    assert identity.entity_type == "unknown"
    assert identity.regulatory_regime == "unknown"
    assert identity.regulatory_status == "unknown"
    assert identity.activities["pension"] is True
    assert identity.activities["capitalization"] is True
    assert identity.activities["insurance"] is False


def test_identity_without_cnpj_still_uses_fip_namespace() -> None:
    identity = build_canonical_identity(
        {
            "id": "123",
            "name": "ENTIDADE SEM CNPJ",
            "cnpj": None,
        }
    )

    assert identity.entity_id == "fip:000123"
    assert identity.fip_code == "000123"
    assert identity.cnpj is None
    assert identity.legal_entity_id is None


def test_same_cnpj_may_belong_to_distinct_regulatory_records() -> None:
    companies = {
        "000111": {
            "id": "000111",
            "name": "REGISTRO A",
            "cnpj": "12.345.678/0001-90",
        },
        "000222": {
            "id": "000222",
            "name": "REGISTRO B",
            "cnpj": "12.345.678/0001-90",
        },
    }

    entities = build_canonical_entities(companies)

    assert [item["entity_id"] for item in entities] == ["fip:000111", "fip:000222"]
    assert all(item["cnpj"] == "12345678000190" for item in entities)


def test_builder_rejects_conflicting_records_for_same_fip() -> None:
    records = [
        {
            "id": "000111",
            "name": "EMPRESA A",
            "cnpj": "12.345.678/0001-90",
        },
        {
            "id": "000111",
            "name": "EMPRESA A ALTERADA",
            "cnpj": "98.765.432/0001-10",
        },
    ]

    with pytest.raises(IdentityConflictError):
        build_canonical_entities(records)


def test_builder_rejects_invalid_record_instead_of_silently_dropping_it() -> None:
    with pytest.raises(ValueError):
        build_canonical_entities(
            {
                "invalid": {
                    "name": "SEM CODIGO FIP",
                    "cnpj": "12.345.678/0001-90",
                }
            }
        )


def test_builder_output_is_deterministic() -> None:
    companies = {
        "000222": {"id": "000222", "name": "EMPRESA B", "cnpj": None},
        "000111": {"id": "000111", "name": "EMPRESA A", "cnpj": None},
    }

    entities = build_canonical_entities(companies)

    assert [item["entity_id"] for item in entities] == ["fip:000111", "fip:000222"]
