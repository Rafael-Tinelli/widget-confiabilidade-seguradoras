from api.v2.public_profile_regulatory_semantics import (
    COOPERATIVE_ASSESSMENT_REASON,
    COOPERATIVE_LABEL,
    apply_regulatory_profile_semantics,
)


def test_cooperative_profile_is_searchable_without_ordinary_assessment():
    payload = {
        "artifact": "v2_public_search_profile_contract",
        "version": "fixture",
        "population": {},
        "publication_policy": {},
        "profiles": [
            {
                "profile_id": "entity:fip:009999",
                "profile_kind": "entity",
                "identity": {
                    "entity_id": "fip:009999",
                    "legal_name": "COOPERATIVA TESTE DE SEGUROS",
                    "cnpj": "12345678000199",
                    "fip_code": "009999",
                    "entity_type": "insurer",
                },
                "regulatory": {
                    "regime": "ordinary",
                    "status": "active_licensed",
                    "query_state": "insurance_cooperative",
                    "filter_bucket": "other",
                    "label": "Seguradora",
                },
                "public_summary": {},
                "assessment": {"availability": "available"},
                "limits": [],
            }
        ],
        "search_index": [
            {
                "profile_id": "entity:fip:009999",
                "name": "COOPERATIVA TESTE DE SEGUROS",
                "filter_bucket": "other",
                "disambiguation": "Seguradora",
            }
        ],
    }

    result = apply_regulatory_profile_semantics(payload)
    profile = result["profiles"][0]
    search = result["search_index"][0]

    assert profile["regulatory"]["label"] == COOPERATIVE_LABEL
    assert profile["assessment"] == {
        "availability": "not_applicable",
        "reason": COOPERATIVE_ASSESSMENT_REASON,
    }
    assert COOPERATIVE_LABEL in search["disambiguation"]
    assert search["filter_bucket"] == "other"
    assert result["population"]["insurance_cooperative_profiles"] == 1
    assert result["publication_policy"]["insurance_cooperative_enters_ordinary_assessment"] is False
    assert result["publication_policy"]["insurance_cooperative_enters_ordinary_ranking"] is False
