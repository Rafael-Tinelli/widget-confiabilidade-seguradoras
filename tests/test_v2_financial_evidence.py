from __future__ import annotations

from api.v2.financial_evidence import (
    apply_financial_evidence,
    financial_evidence_summary,
    month_window,
    validate_financial_evidence,
)


def _eligible_entity() -> dict:
    return {
        "entity_id": "fip:000001",
        "fip_code": "000001",
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "eligibility": {"regulatory_universe_eligible": True},
    }


def _source(months: int = 12, cmr: float = 80.0) -> dict:
    periods = month_window(202606, months)
    capital = {
        period: {"period": period, "pla_adjusted": 100.0, "cmr": cmr}
        for period in periods
    }
    balance_values = {
        period: {1479: 100.0, 11160: 0.0, 351: 0.0, 1040: 50.0}
        for period in periods
    }
    return {
        "source": {},
        "reference_periods": {
            "capital": 202606,
            "balance": 202606,
            "insurance_operations": 202606,
        },
        "entities": {
            "000001": {
                "capital_history": capital,
                "balance_periods": set(periods),
                "balance_values": balance_values,
                "insurance_operation_periods": set(periods),
                "nonzero_premium_periods": set(periods),
                "duplicate_capital_rows": 0,
                "duplicate_balance_cmpid_rows": 0,
            }
        },
    }


def test_month_window_crosses_year_boundary() -> None:
    assert month_window(202601, 3) == [202511, 202512, 202601]


def test_complete_core_history_is_readiness_not_assessment() -> None:
    entities = apply_financial_evidence([_eligible_entity()], _source())
    validate_financial_evidence(entities)
    profile = entities[0]["financial_evidence"]

    assert profile["state"] == "complete_core_history"
    assert profile["core_financial_evidence_ready"] is True
    assert profile["capital"]["pla_cmr_ratio"] == 1.25
    assert profile["assessment_eligible"] is False
    assert profile["ranking_eligible"] is False


def test_short_history_remains_limited_not_bad() -> None:
    entities = apply_financial_evidence([_eligible_entity()], _source(months=5))
    profile = entities[0]["financial_evidence"]

    assert profile["state"] == "limited_core_history"
    assert profile["core_financial_evidence_ready"] is False
    assert "capital_adequacy_history_under_12m" in profile["reason_codes"]
    assert "balance_history_under_12m" in profile["reason_codes"]


def test_zero_cmr_is_unavailable_evidence_not_adverse_signal() -> None:
    entities = apply_financial_evidence([_eligible_entity()], _source(cmr=0.0))
    validate_financial_evidence(entities)
    profile = entities[0]["financial_evidence"]

    assert profile["state"] == "capital_metric_unavailable"
    assert profile["capital"]["state"] == "metric_unavailable"
    assert profile["capital"]["current_metric_state"] == "cmr_zero_unusable"
    assert profile["capital"]["pla_cmr_ratio"] is None
    assert profile["capital"]["pla_cmr_ratio_state"] == "unavailable"


def test_negative_cmr_is_source_investigation_case() -> None:
    entities = apply_financial_evidence([_eligible_entity()], _source(cmr=-1.0))
    profile = entities[0]["financial_evidence"]

    assert profile["state"] == "requires_source_investigation"
    assert profile["capital"]["current_metric_state"] == "cmr_negative_invalid"
    assert profile["capital"]["pla_cmr_ratio"] is None


def test_noneligible_entity_is_not_applicable() -> None:
    entity = _eligible_entity()
    entity["eligibility"]["regulatory_universe_eligible"] = False
    entities = apply_financial_evidence([entity], _source())

    assert entities[0]["financial_evidence"]["state"] == "not_applicable"


def test_summary_counts_only_regulatory_universe() -> None:
    eligible = _eligible_entity()
    outside = {
        "entity_id": "fip:000002",
        "fip_code": "000002",
        "eligibility": {"regulatory_universe_eligible": False},
    }
    entities = apply_financial_evidence([eligible, outside], _source())
    summary = financial_evidence_summary(entities)

    assert summary["regulatory_eligible_count"] == 1
    assert summary["core_financial_evidence_ready_count"] == 1
    assert summary["assessment_eligible_count"] == 0
    assert summary["ranking_eligible_count"] == 0
