import copy

import pytest

from api.v2.public_information_projection import (
    PublicInformationProjectionError,
    apply_public_information_projection,
)

MONTHS = [f"2025-{month:02d}" for month in range(7, 13)] + [
    f"2026-{month:02d}" for month in range(1, 7)
]


def _payloads():
    contract = {
        "artifact": "v2_public_search_profile_contract",
        "profiles": [
            {
                "profile_id": "entity:fip:1",
                "profile_kind": "entity",
                "assessment": {"availability": "available", "conduct": {"state": "x"}},
                "sandbox_conduct": None,
            },
            {
                "profile_id": "entity:cnpj:2",
                "profile_kind": "entity",
                "assessment": {"availability": "not_applicable"},
                "sandbox_conduct": {"availability": "available"},
            },
            {
                "profile_id": "brand:test",
                "profile_kind": "brand",
                "assessment": {"availability": "not_applicable"},
                "sandbox_conduct_context": {"availability": "available"},
            },
        ],
    }
    explorer = {
        "artifact": "v2_public_insurer_explorer",
        "entities": [{"entity_id": "fip:1", "conduct": {"state": "x"}}],
    }
    conduct = {
        "artifact": "v2_conduct_methodology_closure",
        "candidate_entities": [
            {
                "entity_id": "fip:1",
                "direct_pressure": {
                    "monthly": [{"month": month, "state": "available"} for month in MONTHS]
                },
            }
        ],
        "non_comparable_entities": [],
    }
    sandbox = {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "source": {"months": MONTHS},
    }
    return contract, explorer, conduct, sandbox


def test_reference_window_is_projected_without_frontend_inference():
    contract, explorer, conduct, sandbox = _payloads()
    projected_contract, projected_explorer = apply_public_information_projection(
        contract, explorer, conduct, sandbox
    )

    expected = {
        "start_month": "2025-07",
        "end_month": "2026-06",
        "months": 12,
        "semantics": "preserved_consumer_gov_window_not_inferred_by_frontend",
    }
    assert projected_explorer["entities"][0]["conduct"]["reference_window"] == expected

    profiles = {row["profile_id"]: row for row in projected_contract["profiles"]}
    assert profiles["entity:fip:1"]["assessment"]["conduct"]["reference_window"] == expected
    assert profiles["entity:cnpj:2"]["sandbox_conduct"]["reference_window"] == expected
    assert profiles["brand:test"]["sandbox_conduct_context"]["reference_window"] == expected

    policy = projected_contract["public_information_projection"]["policy"]
    assert policy["frontend_may_infer_conduct_period"] is False
    assert policy["projection_changes_methodology"] is False


def test_projection_rejects_disagreement_between_ordinary_and_sandbox_windows():
    contract, explorer, conduct, sandbox = _payloads()
    broken = copy.deepcopy(sandbox)
    broken["source"]["months"][-1] = "2026-07"

    with pytest.raises(
        PublicInformationProjectionError, match="ordinary/Sandbox Conduct windows differ"
    ):
        apply_public_information_projection(contract, explorer, conduct, broken)


def test_projection_rejects_disagreeing_candidate_windows():
    contract, explorer, conduct, sandbox = _payloads()
    broken = copy.deepcopy(conduct)
    second = copy.deepcopy(broken["candidate_entities"][0])
    second["entity_id"] = "fip:2"
    second["direct_pressure"]["monthly"][-1]["month"] = "2026-07"
    broken["candidate_entities"].append(second)

    with pytest.raises(PublicInformationProjectionError, match="candidate windows disagree"):
        apply_public_information_projection(contract, explorer, broken, sandbox)
