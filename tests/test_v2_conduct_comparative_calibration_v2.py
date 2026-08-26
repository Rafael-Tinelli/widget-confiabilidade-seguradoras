from __future__ import annotations

import pytest

from api.v2.build_conduct_comparative_calibration_v2 import build_calibration_v2


def _conduct_entity(entity_id: str, complaints: list[int], satisfaction: float) -> dict:
    months = ["2026-01", "2026-02"]
    return {
        "entity_id": entity_id,
        "display_name": entity_id,
        "totals": {
            "complaints": sum(complaints),
            "satisfaction_count": 10,
            "average_satisfaction": satisfaction,
        },
        "monthly": [
            {
                "month": month,
                "complaints": count,
                "satisfaction_count": 5,
                "average_satisfaction": satisfaction,
            }
            for month, count in zip(months, complaints, strict=True)
        ],
        "film": {
            "satisfaction_trend": {
                "direction": "stable",
                "first_half_average": satisfaction,
                "second_half_average": satisfaction,
            }
        },
    }


def _ses_month(direct: float, branch: int) -> dict:
    return {
        "insurance_premium_direct": direct,
        "insurance_premium_earned": direct * 0.9,
        "insurance_branches": {
            branch: {
                "premium_direct": direct,
                "premium_earned": direct * 0.9,
                "rows": 1.0,
            }
        },
    }


def test_calibration_v2_aligns_market_population_and_forbids_scoring() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-02"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [5, 5], 3.0),
            _conduct_entity("fip:000002", [5, 5], 4.0),
            _conduct_entity("fip:000003", [100, 100], 2.0),
        ],
    }
    reconciliation = {
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "000001",
                "cnpj": "1",
                "legal_name": "A",
                "complaints_12m": 10,
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "pressure_eligible_candidate": True,
                    "reason_code": None,
                },
            },
            {
                "entity_id": "fip:000002",
                "fip_code": "000002",
                "cnpj": "2",
                "legal_name": "B",
                "complaints_12m": 10,
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "pressure_eligible_candidate": True,
                    "reason_code": None,
                },
            },
            {
                "entity_id": "fip:000003",
                "fip_code": "000003",
                "cnpj": "3",
                "legal_name": "Hybrid",
                "complaints_12m": 200,
                "pressure_comparability": {
                    "state": "hybrid_insurance_pension_requires_product_numerator",
                    "pressure_eligible_candidate": False,
                    "reason_code": "consumer_gov_numerator_not_product_separated_under_p3",
                },
            },
        ]
    }
    ses = {
        "periods": [202601, 202602],
        "entities": {
            "000001": {
                "months": {
                    202601: _ses_month(100.0, 1001),
                    202602: _ses_month(0.0, 1001),
                }
            },
            "000002": {
                "months": {
                    202601: _ses_month(150.0, 2001),
                    202602: _ses_month(150.0, 2001),
                }
            },
        },
    }

    payload = build_calibration_v2(conduct, reconciliation, ses)

    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["denominator"]["candidate"] == "insurance_premium_direct"
    assert payload["denominator"]["final_denominator_approved"] is False
    assert payload["population"]["direct_one_to_one_candidates"] == 2
    assert payload["population"]["excluded_from_pressure_experiment"] == 1
    assert payload["market_12m"]["complaints"] == 20
    assert payload["market_12m"]["premium_direct"] == pytest.approx(400.0)

    by_id = {row["entity_id"]: row for row in payload["entities"]}
    a = by_id["fip:000001"]
    b = by_id["fip:000002"]

    assert a["pressure_12m"]["expected_complaints"] == pytest.approx(5.0)
    assert a["pressure_12m"]["ratio"] == pytest.approx(2.0)
    assert b["pressure_12m"]["expected_complaints"] == pytest.approx(15.0)
    assert b["pressure_12m"]["ratio"] == pytest.approx(2.0 / 3.0)

    feb_market = payload["monthly_market"][1]
    assert feb_market["comparable_entities"] == 1
    assert feb_market["market_complaints"] == 5
    assert feb_market["market_premium_direct"] == pytest.approx(150.0)
    assert a["monthly"][1]["state"] == "not_comparable_non_positive_monthly_premium"
    assert a["monthly"][1]["pressure_ratio"] is None
    assert b["monthly"][1]["pressure_ratio"] == pytest.approx(1.0)


def test_calibration_v2_preserves_mix_and_excluded_reason() -> None:
    conduct = {
        "source": {
            "months": ["2026-01"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [10, 0], 3.0),
            _conduct_entity("fip:000002", [10, 0], 4.0),
        ],
    }
    conduct["entities"][0]["monthly"] = conduct["entities"][0]["monthly"][:1]
    conduct["entities"][1]["monthly"] = conduct["entities"][1]["monthly"][:1]
    reconciliation = {
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "000001",
                "legal_name": "A",
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "pressure_eligible_candidate": True,
                },
            },
            {
                "entity_id": "fip:000002",
                "fip_code": "000002",
                "legal_name": "B",
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "pressure_eligible_candidate": True,
                },
            },
        ]
    }
    ses = {
        "periods": [202601],
        "entities": {
            "000001": {"months": {202601: _ses_month(100.0, 1001)}},
            "000002": {"months": {202601: _ses_month(100.0, 2001)}},
        },
    }

    payload = build_calibration_v2(conduct, reconciliation, ses)
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    assert by_id["fip:000001"]["portfolio_12m"]["positive_branch_mix"] == {"1001": 1.0}
    assert by_id["fip:000001"]["portfolio_12m"]["distance_from_market_mix"] == pytest.approx(0.5)
    assert by_id["fip:000001"]["portfolio_12m"]["nearest_mix_peers"][0]["entity_id"] == "fip:000002"
    assert payload["diagnostics"]["shrinkage_applied"] is False
    assert payload["diagnostics"]["peer_groups_selected"] is False
