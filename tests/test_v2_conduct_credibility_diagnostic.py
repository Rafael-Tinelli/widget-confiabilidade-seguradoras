from __future__ import annotations

import pytest

from api.v2.build_conduct_credibility_diagnostic import (
    ConductCredibilityDiagnosticError,
    build_credibility_diagnostic,
)


def _entity(
    entity_id: str,
    complaints: list[int],
    direct: list[float],
    earned: list[float | None],
    *,
    aligned_observed: int,
    aligned_expected: float,
    comparable_months: int,
) -> dict:
    months = [f"2025-{month:02d}" for month in range(1, 13)]
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":", 1)[1],
        "legal_name": entity_id,
        "complaints_12m": sum(complaints),
        "premium_direct_12m": sum(direct),
        "premium_earned_12m_diagnostic": (
            sum(value for value in earned if value is not None)
            if all(value is not None for value in earned)
            else None
        ),
        "pressure_12m": {
            "observed_complaints": aligned_observed,
            "expected_complaints": aligned_expected,
            "ratio": aligned_observed / aligned_expected,
            "comparable_months": comparable_months,
            "aggregation_policy": (
                "sum_monthly_expected_then_observed_divided_by_expected"
            ),
        },
        "monthly": [
            {
                "month": month,
                "complaints": complaints[index],
                "premium_direct": direct[index],
                "premium_earned_diagnostic": earned[index],
            }
            for index, month in enumerate(months)
        ],
    }


def _calibration(entities: list[dict]) -> dict:
    return {
        "version": "upstream-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "denominator": {
            "candidate": "insurance_premium_direct",
            "source_field": "premio_direto",
            "currency": "BRL",
            "source_unit_label": "R$",
            "scale_factor_applied": 1.0,
        },
        "source": {"months": [f"2025-{month:02d}" for month in range(1, 13)]},
        "market_12m": {
            "complaints": sum(row["complaints_12m"] for row in entities),
            "premium_direct": sum(row["premium_direct_12m"] for row in entities),
        },
        "entities": entities,
    }


def test_credibility_reuses_monthly_aligned_direct_pressure() -> None:
    target = _entity(
        "fip:000001",
        complaints=[5] + [0] * 11,
        direct=[0.0] + [10.0] * 11,
        earned=[10.0] * 12,
        aligned_observed=0,
        aligned_expected=2.0,
        comparable_months=11,
    )
    peer = _entity(
        "fip:000002",
        complaints=[95] + [10] * 11,
        direct=[100.0] * 12,
        earned=[100.0] * 12,
        aligned_observed=205,
        aligned_expected=203.0,
        comparable_months=12,
    )

    payload = build_credibility_diagnostic(_calibration([target, peer]))
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    target_out = by_id["fip:000001"]

    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["source"]["currency"] == "BRL"
    assert payload["source"]["source_unit_label"] == "R$"
    assert payload["source"]["scale_factor_applied"] == pytest.approx(1.0)
    assert payload["methodology"]["pressure_definition"] == (
        "sum_monthly_expected_then_observed_divided_by_expected"
    )
    assert target_out["direct_candidate"]["observed_complaints"] == 0
    assert target_out["direct_candidate"]["expected_complaints"] == pytest.approx(2.0)
    assert target_out["direct_candidate"]["ratio"] == pytest.approx(0.0)
    assert target_out["direct_candidate"]["comparable_months"] == 11
    assert target_out["direct_candidate"]["premium_currency"] == "BRL"
    assert target_out["direct_candidate"]["premium_share"] is None
    assert (
        target_out["direct_candidate"]["premium_share_state"]
        == "not_used_under_monthly_aligned_pressure"
    )
    assert (
        target_out["temporal_overlap"]["premium_direct"][
            "complaints_in_non_positive_premium_months"
        ]
        == 5
    )


def test_sparse_or_missing_earned_exposure_cannot_create_sensitivity_veto() -> None:
    target = _entity(
        "fip:000001",
        complaints=[2] * 12,
        direct=[100.0] * 12,
        earned=[None] * 8 + [100.0] * 4,
        aligned_observed=24,
        aligned_expected=12.0,
        comparable_months=12,
    )
    peer = _entity(
        "fip:000002",
        complaints=[2] * 12,
        direct=[100.0] * 12,
        earned=[100.0] * 12,
        aligned_observed=24,
        aligned_expected=36.0,
        comparable_months=12,
    )

    payload = build_credibility_diagnostic(_calibration([target, peer]))
    target_out = next(
        row for row in payload["entities"] if row["entity_id"] == "fip:000001"
    )

    assert target_out["earned_diagnostic"]["comparable_months"] == 4
    assert target_out["earned_diagnostic"]["missing_months"] == 8
    assert target_out["earned_diagnostic"]["eligible_for_sensitivity_guard"] is False
    assert (
        target_out["denominator_sensitivity"]["familywise_state_consistency"]
        == "earned_insufficient_temporal_coverage"
    )
    assert (
        target_out["temporal_overlap"]["premium_earned"]["missing_premium_months"]
        == 8
    )
    assert payload["population"]["earned_diagnostic_entities"] == 1


def test_earned_diagnostic_uses_same_month_population_only() -> None:
    target = _entity(
        "fip:000001",
        complaints=[10] + [0] * 11,
        direct=[50.0] * 12,
        earned=[100.0] * 12,
        aligned_observed=10,
        aligned_expected=5.0,
        comparable_months=12,
    )
    peer = _entity(
        "fip:000002",
        complaints=[0] * 12,
        direct=[50.0] * 12,
        earned=[-5.0] * 12,
        aligned_observed=0,
        aligned_expected=5.0,
        comparable_months=12,
    )

    payload = build_credibility_diagnostic(_calibration([target, peer]))
    by_id = {row["entity_id"]: row for row in payload["entities"]}

    assert by_id["fip:000001"]["earned_diagnostic"]["ratio"] == pytest.approx(1.0)
    assert by_id["fip:000001"]["earned_diagnostic"]["comparable_months"] == 1
    assert (
        by_id["fip:000002"]["earned_diagnostic"]["state"]
        == "unavailable_no_positive_aligned_premium_earned"
    )


def test_credibility_rejects_mismatched_upstream_direct_ratio() -> None:
    entity = _entity(
        "fip:000001",
        complaints=[1] * 12,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
        aligned_observed=12,
        aligned_expected=12.0,
        comparable_months=12,
    )
    entity["pressure_12m"]["ratio"] = 2.0

    with pytest.raises(
        ConductCredibilityDiagnosticError,
        match="upstream direct pressure ratio mismatch",
    ):
        build_credibility_diagnostic(_calibration([entity]))


def test_credibility_rejects_fractional_complaint_count() -> None:
    entity = _entity(
        "fip:000001",
        complaints=[1] * 12,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
        aligned_observed=12,
        aligned_expected=12.0,
        comparable_months=12,
    )
    entity["monthly"][0]["complaints"] = 1.5

    with pytest.raises(
        ConductCredibilityDiagnosticError,
        match="non-integer complaints",
    ):
        build_credibility_diagnostic(_calibration([entity]))


def test_credibility_rejects_rescaled_or_wrong_currency_upstream() -> None:
    entity = _entity(
        "fip:000001",
        complaints=[1] * 12,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
        aligned_observed=12,
        aligned_expected=12.0,
        comparable_months=12,
    )
    calibration = _calibration([entity])
    calibration["denominator"]["scale_factor_applied"] = 1000.0

    with pytest.raises(
        ConductCredibilityDiagnosticError,
        match=r"unit contract must be BRL/R\$/1\.0",
    ):
        build_credibility_diagnostic(calibration)

    calibration = _calibration([entity])
    calibration["denominator"]["currency"] = "USD"
    with pytest.raises(
        ConductCredibilityDiagnosticError,
        match=r"unit contract must be BRL/R\$/1\.0",
    ):
        build_credibility_diagnostic(calibration)
