from __future__ import annotations

import pytest

from api.v2.build_conduct_comparative_calibration_v2 import (
    ConductComparativeCalibrationV2Error,
    build_calibration_v2,
)


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


def _ses_month(
    direct: float,
    branch: int,
    *,
    earned_missing_rows: int = 0,
) -> dict:
    return {
        "insurance_premium_direct": direct,
        "insurance_premium_earned": direct * 0.9,
        "insurance_premium_direct_missing_rows": 0,
        "insurance_premium_earned_missing_rows": earned_missing_rows,
        "insurance_branches": {
            branch: {
                "premium_direct": direct,
                "premium_earned": direct * 0.9,
                "rows": 1.0,
            }
        },
    }


def _candidate(entity_id: str, fip: str, name: str) -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": fip,
        "cnpj": fip,
        "legal_name": name,
        "insurance_exposure_12m": {
            "insurance_premium_direct_missing_rows": 0,
        },
        "pressure_comparability": {
            "state": "direct_one_to_one_candidate",
            "pressure_eligible_candidate": True,
            "reason_code": None,
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
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
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
    assert payload["denominator"]["currency"] == "BRL"
    assert payload["denominator"]["source_unit_label"] == "R$"
    assert payload["denominator"]["scale_factor_applied"] == 1.0
    assert payload["denominator"]["final_denominator_approved"] is False
    assert payload["source"]["ses_currency"] == "BRL"
    assert payload["population"]["direct_one_to_one_candidates"] == 2
    assert payload["population"]["excluded_from_pressure_experiment"] == 1
    assert payload["market_12m"]["complaints"] == 20
    assert payload["market_12m"]["premium_direct"] == pytest.approx(400.0)

    by_id = {row["entity_id"]: row for row in payload["entities"]}
    a = by_id["fip:000001"]
    b = by_id["fip:000002"]

    assert a["pressure_12m"]["observed_complaints"] == 5
    assert a["pressure_12m"]["expected_complaints"] == pytest.approx(4.0)
    assert a["pressure_12m"]["ratio"] == pytest.approx(1.25)
    assert a["pressure_12m"]["comparable_months"] == 1
    assert a["small_sample_bucket"] == "5_19"
    assert b["pressure_12m"]["observed_complaints"] == 10
    assert b["pressure_12m"]["expected_complaints"] == pytest.approx(11.0)
    assert b["pressure_12m"]["ratio"] == pytest.approx(10.0 / 11.0)

    feb_market = payload["monthly_market"][1]
    assert feb_market["comparable_entities"] == 1
    assert feb_market["market_complaints"] == 5
    assert feb_market["market_premium_direct"] == pytest.approx(150.0)
    assert a["monthly"][1]["state"] == "not_comparable_non_positive_monthly_premium"
    assert a["monthly"][1]["pressure_ratio"] is None
    assert b["monthly"][1]["pressure_ratio"] == pytest.approx(1.0)


def test_calibration_v2_preserves_earned_missingness_as_unavailable() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-02"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [2, 2], 3.0),
            _conduct_entity("fip:000002", [2, 2], 4.0),
        ],
    }
    reconciliation = {
        "entities": [
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
        ]
    }
    ses = {
        "periods": [202601, 202602],
        "entities": {
            "000001": {
                "months": {
                    202601: _ses_month(100.0, 1001, earned_missing_rows=1),
                    202602: _ses_month(100.0, 1001),
                }
            },
            "000002": {
                "months": {
                    202601: _ses_month(100.0, 2001),
                    202602: _ses_month(100.0, 2001),
                }
            },
        },
    }

    payload = build_calibration_v2(conduct, reconciliation, ses)
    row = {item["entity_id"]: item for item in payload["entities"]}["fip:000001"]

    assert row["premium_earned_12m_diagnostic"] is None
    assert row["premium_earned_complete_months"] == 1
    assert row["premium_earned_diagnostic_complete"] is False
    assert row["monthly"][0]["premium_earned_diagnostic"] is None
    assert row["monthly"][0]["premium_earned_missing_rows"] == 1


def test_calibration_v2_rejects_fractional_complaint_count() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-02"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [2, 2], 3.0),
            _conduct_entity("fip:000002", [2, 2], 4.0),
        ],
    }
    conduct["entities"][0]["monthly"][0]["complaints"] = 1.5
    reconciliation = {
        "entities": [
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
        ]
    }
    ses = {
        "periods": [202601, 202602],
        "entities": {
            "000001": {"months": {202601: _ses_month(100.0, 1001), 202602: _ses_month(100.0, 1001)}},
            "000002": {"months": {202601: _ses_month(100.0, 2001), 202602: _ses_month(100.0, 2001)}},
        },
    }

    with pytest.raises(ConductComparativeCalibrationV2Error, match="non-integer complaints"):
        build_calibration_v2(conduct, reconciliation, ses)


def test_calibration_v2_small_sample_uses_aligned_complaints_only() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-02"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [100, 2], 3.0),
            _conduct_entity("fip:000002", [1, 2], 4.0),
        ],
    }
    reconciliation = {
        "entities": [
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
        ]
    }
    ses = {
        "periods": [202601, 202602],
        "entities": {
            "000001": {"months": {202601: _ses_month(0.0, 1001), 202602: _ses_month(100.0, 1001)}},
            "000002": {"months": {202601: _ses_month(100.0, 2001), 202602: _ses_month(100.0, 2001)}},
        },
    }

    payload = build_calibration_v2(conduct, reconciliation, ses)
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    target = by_id["fip:000001"]

    assert target["complaints_12m"] == 102
    assert target["pressure_12m"]["observed_complaints"] == 2
    assert target["small_sample_bucket"] == "0_4"
    bucket = payload["diagnostics"]["small_sample"]["0_4"]
    assert bucket["sample_basis"] == "pressure_12m.observed_complaints"
    assert bucket["complaints_total"] == 5
    assert bucket["total_evidence_complaints"] == 105


def test_calibration_v2_rejects_gapped_comparison_window() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-03"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [],
    }

    with pytest.raises(ConductComparativeCalibrationV2Error, match="must be consecutive"):
        build_calibration_v2(conduct, {"entities": []}, {"periods": [202601, 202603]})


def test_calibration_v2_zero_market_complaint_month_is_not_neutral() -> None:
    conduct = {
        "source": {
            "months": ["2026-01", "2026-02"],
            "core": {"state": "available"},
            "taxonomy_evidence": {"state": "source_unavailable"},
        },
        "entities": [
            _conduct_entity("fip:000001", [0, 2], 3.0),
            _conduct_entity("fip:000002", [0, 2], 4.0),
        ],
    }
    reconciliation = {
        "entities": [
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
        ]
    }
    ses = {
        "periods": [202601, 202602],
        "entities": {
            "000001": {
                "months": {
                    202601: _ses_month(100.0, 1001),
                    202602: _ses_month(100.0, 1001),
                }
            },
            "000002": {
                "months": {
                    202601: _ses_month(100.0, 2001),
                    202602: _ses_month(100.0, 2001),
                }
            },
        },
    }

    payload = build_calibration_v2(conduct, reconciliation, ses)
    january = payload["monthly_market"][0]
    row = next(item for item in payload["entities"] if item["entity_id"] == "fip:000001")

    assert january["market_complaints"] == 0
    assert january["market_premium_direct"] == pytest.approx(200.0)
    assert january["pressure_baseline_state"] == "unavailable_zero_market_complaints"
    assert row["monthly"][0]["state"] == "not_comparable_zero_market_complaints"
    assert row["monthly"][0]["expected_complaints"] == pytest.approx(0.0)
    assert row["monthly"][0]["pressure_ratio"] is None
    assert row["pressure_12m"]["comparable_months"] == 1
    assert row["pressure_12m"]["zero_market_complaint_months"] == 1
    assert row["pressure_12m"]["observed_complaints"] == 2
    assert row["pressure_12m"]["expected_complaints"] == pytest.approx(2.0)
    assert row["pressure_12m"]["ratio"] == pytest.approx(1.0)


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
            _candidate("fip:000001", "000001", "A"),
            _candidate("fip:000002", "000002", "B"),
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
