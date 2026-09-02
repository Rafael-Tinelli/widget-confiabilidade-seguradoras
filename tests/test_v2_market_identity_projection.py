from __future__ import annotations

import pytest

from api.v2.public_information_projection import (
    PublicInformationProjectionError,
    apply_public_information_projection,
)

MONTHS = [
    "2025-07",
    "2025-08",
    "2025-09",
    "2025-10",
    "2025-11",
    "2025-12",
    "2026-01",
    "2026-02",
    "2026-03",
    "2026-04",
    "2026-05",
    "2026-06",
]


def _inputs():
    contract = {
        "artifact": "v2_public_search_profile_contract",
        "profiles": [
            {
                "profile_id": "brand:azos",
                "profile_kind": "brand",
                "identity": {
                    "brand_id": "brand:azos",
                    "name": "Azos",
                    "aliases": ["Azos Seguros"],
                    "entity_type": "brand",
                },
                "public_summary": {
                    "headline": "legacy brand headline",
                    "quick_answer": "legacy brand wording",
                },
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_profile_id": "entity:fip:005690",
                        "target_name": "COMPANHIA EXCELSIOR DE SEGUROS",
                    }
                ],
                "assessment": {"availability": "not_applicable"},
            }
        ],
        "search_index": [
            {
                "profile_id": "brand:azos",
                "result_kind": "brand",
                "entity_type": "brand",
                "filter_bucket": "brands",
                "name": "Azos",
                "aliases": ["Azos Seguros"],
                "cnpj": None,
                "fip_code": None,
                "disambiguation": "Marca",
                "search_text": "azos azos seguros",
            }
        ],
    }
    explorer = {"artifact": "v2_public_insurer_explorer", "entities": []}
    conduct = {
        "artifact": "v2_conduct_methodology_closure",
        "candidate_entities": [
            {"direct_pressure": {"monthly": [{"month": month} for month in MONTHS]}}
        ],
    }
    sandbox = {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "source": {"months": MONTHS},
    }
    registry = {
        "brands": [
            {
                "brand_id": "brand:azos",
                "market_identity": {
                    "kind": "insurtech_platform",
                    "public_label": "Insurtech / plataforma de seguros",
                    "legal_name": "AZOS TECNOLOGIA E SERVIÇOS LTDA",
                    "cnpj": "39520039000175",
                    "public_note": (
                        "A própria Azos se apresenta como insurtech, não como seguradora."
                    ),
                    "evidence": {"authority": "Azos"},
                },
            }
        ]
    }
    return contract, explorer, conduct, sandbox, registry


def test_verified_market_identity_is_searchable_without_inheriting_assessment():
    contract, explorer, conduct, sandbox, registry = _inputs()

    projected, _ = apply_public_information_projection(
        contract, explorer, conduct, sandbox, registry
    )

    profile = projected["profiles"][0]
    market = profile["identity"]["market_identity"]
    assert market["kind"] == "insurtech_platform"
    assert market["cnpj"] == "39520039000175"
    assert market["assessment_inheritance"] == "forbidden"
    assert "insurtech" in profile["public_summary"]["quick_answer"].lower()
    assert "COMPANHIA EXCELSIOR DE SEGUROS" in profile["public_summary"]["quick_answer"]

    entry = projected["search_index"][0]
    assert entry["cnpj"] == "39520039000175"
    assert entry["entity_type"] == "insurtech_platform"
    assert entry["market_role_label"] == "Insurtech / plataforma de seguros"
    assert "39520039000175" in entry["search_text"]
    assert "azos tecnologia e servicos ltda" in entry["search_text"]

    projection = projected["public_information_projection"]
    assert projection["counts"]["verified_market_identity_profiles"] == 1
    assert (
        projection["policy"]["market_identity_may_inherit_related_entity_assessment"]
        is False
    )


def test_market_identity_projection_rejects_malformed_cnpj():
    contract, explorer, conduct, sandbox, registry = _inputs()
    registry["brands"][0]["market_identity"]["cnpj"] = "123"

    with pytest.raises(PublicInformationProjectionError, match="invalid market identity CNPJ"):
        apply_public_information_projection(
            contract, explorer, conduct, sandbox, registry
        )
