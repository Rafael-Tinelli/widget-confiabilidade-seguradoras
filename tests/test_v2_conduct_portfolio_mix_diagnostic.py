from __future__ import annotations

import pytest

from api.v2.build_conduct_portfolio_mix_diagnostic import (
    ConductPortfolioMixDiagnosticError,
    build_portfolio_mix_diagnostic,
)


def _entity(
    entity_id: str,
    complaints: int,
    premium: float,
    expected: float,
    mix: dict[str, float],
    *,
    aligned_complaints: int | None = None,
    monthly: list[tuple[str, int, float]] | None = None,
) -> dict:
    aligned = complaints if aligned_complaints is None else aligned_complaints
    month_rows = monthly or [("2026-01", aligned, premium)]
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":", 1)[1],
        "legal_name": entity_id,
        "complaints_12m": complaints,
        "premium_direct_12m": premium,
        "pressure_12m": {
            "observed_complaints": aligned,
            "expected_complaints": expected,
            "ratio": aligned / expected if expected > 0 else None,
            "aggregation_policy": "sum_monthly_expected_then_observed_divided_by_expected",
        },
        "monthly": [
            {
                "month": month,
                "complaints": month_complaints,
                "premium_direct": month_premium,
            }
            for month, month_complaints, month_premium in month_rows
        ],
        "portfolio_12m": {
            "positive_branch_mix": mix,
            "positive_branch_count": len(mix),
            "hhi": sum(value * value for value in mix.values()),
            "top_branch_share": max(mix.values()),
            "distance_from_market_mix": 0.0,
        },
    }


def _calibration(entities: list[dict]) -> dict:
    return {
        "version": "cal-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "denominator": {
            "currency": "BRL",
            "source_unit_label": "R$",
            "scale_factor_applied": 1.0,
        },
        "entities": entities,
    }


def _credibility(entity_ids: list[str]) -> dict:
    return {
        "version": "cred-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "entities": [
            {
                "entity_id": entity_id,
                "direct_candidate": {
                    "familywise_exact_interval": {
                        "state": "not_distinguishable_from_size_proportional_reference"
                    }
                },
                "temporal_overlap": {
                    "premium_direct": {
                        "complaints_in_non_positive_premium_months": 0
                    }
                },
                "denominator_sensitivity": {
                    "familywise_state_consistency": "same_state"
                },
            }
            for entity_id in entity_ids
        ],
    }


def test_portfolio_distance_and_local_population_are_aligned() -> None:
    entities = [
        _entity("fip:000001", 20, 20.0, 10.0, {"A": 1.0}),
        _entity("fip:000002", 10, 80.0, 40.0, {"A": 0.9, "B": 0.1}),
        _entity("fip:000003", 70, 100.0, 50.0, {"B": 1.0}),
    ]
    payload = build_portfolio_mix_diagnostic(
        _calibration(entities),
        _credibility([row["entity_id"] for row in entities]),
    )
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    first = by_id["fip:000001"]

    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["source"]["currency"] == "BRL"
    assert payload["methodology"]["peer_groups_selected"] is False
    assert payload["methodology"]["distance_threshold_selected"] is False
    assert payload["methodology"]["local_pressure_aggregation"] == (
        "sum_monthly_expected_then_observed_divided_by_expected"
    )
    assert first["portfolio"]["nearest_distance"] == pytest.approx(0.1)

    point = next(
        row
        for row in first["peer_distance_curve"]
        if row["max_total_variation_distance"] == 0.1
    )
    local = point["local_aligned_pressure"]
    assert point["peer_count"] == 1
    assert local["group_complaints"] == 30
    assert local["group_premium_direct"] == pytest.approx(100.0)
    assert local["observed_complaints"] == 20
    assert local["expected_complaints"] == pytest.approx(6.0)
    assert local["ratio"] == pytest.approx(20.0 / 6.0)
    assert local["comparable_months"] == 1


def test_local_pressure_excludes_target_non_positive_month_instead_of_annual_aggregation() -> None:
    target = _entity(
        "fip:000001",
        102,
        20.0,
        2.0,
        {"A": 1.0},
        aligned_complaints=2,
        monthly=[
            ("2026-01", 100, 0.0),
            ("2026-02", 2, 20.0),
        ],
    )
    peer = _entity(
        "fip:000002",
        18,
        160.0,
        16.0,
        {"A": 0.95, "B": 0.05},
        aligned_complaints=18,
        monthly=[
            ("2026-01", 10, 80.0),
            ("2026-02", 8, 80.0),
        ],
    )
    far = _entity(
        "fip:000003",
        10,
        100.0,
        10.0,
        {"B": 1.0},
    )
    entities = [target, peer, far]

    payload = build_portfolio_mix_diagnostic(
        _calibration(entities),
        _credibility([row["entity_id"] for row in entities]),
    )
    first = next(row for row in payload["entities"] if row["entity_id"] == "fip:000001")
    point = next(
        row
        for row in first["peer_distance_curve"]
        if row["max_total_variation_distance"] == 0.10
    )
    local = point["local_aligned_pressure"]

    assert point["peer_count"] == 1
    assert local["comparable_months"] == 1
    assert local["non_positive_target_months"] == 1
    assert local["observed_complaints"] == 2
    assert local["group_complaints"] == 10
    assert local["group_premium_direct"] == pytest.approx(100.0)
    assert local["expected_complaints"] == pytest.approx(2.0)
    assert local["ratio"] == pytest.approx(1.0)
    assert first["complaints_12m"] == 102
    assert first["aligned_observed_complaints"] == 2


def test_high_volume_diagnostic_uses_aligned_complaints_not_total_evidence() -> None:
    entities = [
        _entity(
            "fip:000001",
            120,
            20.0,
            2.0,
            {"A": 1.0},
            aligned_complaints=2,
            monthly=[("2026-01", 118, 0.0), ("2026-02", 2, 20.0)],
        ),
        _entity(
            "fip:000002",
            110,
            100.0,
            11.0,
            {"A": 0.9, "B": 0.1},
            aligned_complaints=11,
        ),
        _entity("fip:000003", 10, 100.0, 10.0, {"B": 1.0}),
    ]
    entities[1]["monthly"] = [
        {"month": "2026-01", "complaints": 5, "premium_direct": 50.0},
        {"month": "2026-02", "complaints": 6, "premium_direct": 50.0},
    ]
    entities[2]["monthly"] = [
        {"month": "2026-01", "complaints": 5, "premium_direct": 50.0},
        {"month": "2026-02", "complaints": 5, "premium_direct": 50.0},
    ]

    payload = build_portfolio_mix_diagnostic(
        _calibration(entities),
        _credibility([row["entity_id"] for row in entities]),
    )

    assert payload["population"]["high_volume_100_plus_aligned_complaint_entities"] == 0
    first = next(row for row in payload["entities"] if row["entity_id"] == "fip:000001")
    assert first["complaints_12m"] == 120
    assert first["aligned_observed_complaints"] == 2
    assert payload["methodology"]["pairwise_pressure_similarity"]["sample_basis"] == (
        "pressure_12m.observed_complaints"
    )


def test_nearest_observation_does_not_become_approved_peer_group() -> None:
    entities = [
        _entity("fip:000001", 0, 10.0, 1.0, {"A": 1.0}),
        _entity("fip:000002", 5, 10.0, 1.0, {"B": 1.0}),
        _entity("fip:000003", 5, 10.0, 1.0, {"C": 1.0}),
    ]
    payload = build_portfolio_mix_diagnostic(
        _calibration(entities),
        _credibility([row["entity_id"] for row in entities]),
    )
    first = next(row for row in payload["entities"] if row["entity_id"] == "fip:000001")

    assert first["portfolio"]["nearest_distance"] == pytest.approx(1.0)
    assert all(
        point["peer_count"] == 0 for point in first["peer_distance_curve"]
    )
    assert all(
        point["local_aligned_pressure"]["ratio"] is None
        for point in first["peer_distance_curve"]
    )
    assert payload["diagnostics"]["peer_coverage_curve"][-1][
        "entities_with_at_least_1_peer"
    ] == 0
    guardrails = set(payload["methodology"]["guardrails"])
    assert "no_peer_means_no_local_conclusion_not_neutrality" in guardrails
    assert "nearest_entity_is_not_automatically_an_adequate_peer" in guardrails
    assert "local_pressure_must_preserve_monthly_alignment" in guardrails


def test_portfolio_mix_rejects_wrong_premium_unit_contract() -> None:
    entity = _entity("fip:000001", 5, 10.0, 5.0, {"A": 1.0})
    calibration = _calibration([entity])
    calibration["denominator"]["scale_factor_applied"] = 1000.0

    with pytest.raises(
        ConductPortfolioMixDiagnosticError,
        match="scale factor must be 1.0",
    ):
        build_portfolio_mix_diagnostic(calibration, _credibility([entity["entity_id"]]))
