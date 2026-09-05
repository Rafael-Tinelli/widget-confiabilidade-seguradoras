import pytest

from api.v2.merge_market_identity_watchdog import (
    MarketWatchdogMergeError,
    merge_market_candidates_into_watchdog,
)


def _watchdog():
    return {
        "artifact": "v2_relationship_watchdog",
        "status": "clear",
        "policy": {
            "candidate_assertion_effect": "none",
            "candidate_score_effect": "none",
            "candidate_complaint_transfer_effect": "none",
            "automatic_registry_mutation": "forbidden",
        },
        "summary": {
            "candidate_count": 0,
            "blocking_registry_drift_count": 0,
            "review_candidate_count": 0,
            "consumer_provider_review_count": 0,
            "verified_or_official_observation_count": 0,
            "official_economic_group_observation_count": 0,
        },
        "inputs": {},
        "candidates": [],
        "observations": [],
    }


def _market_candidate():
    return {
        "artifact": "v2_market_identity_candidate_registry",
        "status": "observational_non_authoritative",
        "sensor_status": {"widget_unknown_search": "fresh", "gsc_query": "unavailable"},
        "policy": {
            "detection_is_assertion": False,
            "fuzzy_identity_resolution": "forbidden",
            "candidate_assertion_effect": "none",
            "candidate_score_effect": "none",
            "candidate_complaint_transfer_effect": "none",
            "automatic_registry_mutation": "forbidden",
            "sensor_unavailable_invalidates_gate4": False,
        },
        "summary": {"candidate_count": 1, "review_required_count": 1},
        "candidates": [
            {
                "candidate_id": "market:query:test",
                "candidate_type": "unknown_market_query",
                "candidate_types": ["unknown_market_query"],
                "candidate_anchor": "query:nova marca",
                "review_state": "review_required",
                "lifecycle_state": "review_required",
                "priority": "P1",
                "observations": [
                    {
                        "source": "widget_unknown_search",
                        "observed_value": "nova marca",
                    }
                ],
                "assertion_effect": "none",
                "score_effect": "none",
                "complaint_transfer_effect": "none",
                "automatic_registry_mutation": "forbidden",
                "blocking": False,
            }
        ],
    }


def test_market_candidate_enters_single_review_watchdog_without_becoming_blocking():
    result = merge_market_candidates_into_watchdog(_watchdog(), _market_candidate())
    candidate = result["candidates"][0]
    assert result["artifact"] == "v2_relationship_watchdog"
    assert result["status"] == "review_candidates"
    assert result["summary"]["market_identity_review_count"] == 1
    assert result["summary"]["blocking_registry_drift_count"] == 0
    assert candidate["candidate_domain"] == "emerging_market_identity"
    assert candidate["signals"]["sources"] == ["widget_unknown_search"]
    assert candidate["assertion_effect"] == "none"
    assert candidate["score_effect"] == "none"
    assert candidate["complaint_transfer_effect"] == "none"
    assert candidate["automatic_registry_mutation"] == "forbidden"
    assert result["market_sensor_status"]["gsc_query"] == "unavailable"


def test_market_registry_cannot_weaken_watchdog_invariants():
    market = _market_candidate()
    market["candidates"][0]["complaint_transfer_effect"] = "allowed"
    with pytest.raises(MarketWatchdogMergeError, match="complaint transfer"):
        merge_market_candidates_into_watchdog(_watchdog(), market)
