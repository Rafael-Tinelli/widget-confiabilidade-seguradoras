from __future__ import annotations

import math

from api.v2.liquidity_experiment import (
    build_entity_liquidity_experiment,
    calculate_liquidity_observation,
    liquidity_experiment_summary,
    validate_liquidity_experiment,
)


def _full_values() -> dict[int, float]:
    return {
        1479: 1000.0,
        11160: 10.0,
        351: 5.0,
        1040: 500.0,
        331: 100.0,
        11187: 0.0,
        5503: 0.0,
        6449: 50.0,
    }


def _entity(entity_id: str = "fip:000001") -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":", 1)[1],
        "legal_name": "TESTE SEGURADORA S.A.",
    }


def test_ilc_formula_matches_official_cmpid_expression() -> None:
    result = calculate_liquidity_observation(_full_values(), "ILC")

    assert result["state"] == "derivable"
    assert math.isclose(result["numerator"], 985.0)
    assert math.isclose(result["denominator"], 500.0)
    assert math.isclose(result["value"], 1.97)


def test_ilt_formula_matches_official_cmpid_expression() -> None:
    result = calculate_liquidity_observation(_full_values(), "ILT")

    assert result["state"] == "derivable"
    assert math.isclose(result["numerator"], 1085.0)
    assert math.isclose(result["denominator"], 550.0)
    assert math.isclose(result["value"], 1085.0 / 550.0)


def test_missing_component_is_not_imputed_as_zero() -> None:
    values = _full_values()
    del values[351]

    result = calculate_liquidity_observation(values, "ILC")

    assert result["state"] == "missing_components"
    assert result["value"] is None
    assert result["missing_cmpids"] == [351]


def test_zero_denominator_is_not_divided() -> None:
    values = _full_values()
    values[1040] = 0.0

    result = calculate_liquidity_observation(values, "ILC")

    assert result["state"] == "non_positive_denominator"
    assert result["value"] is None
    assert "denominator_zero" in result["flags"]


def test_negative_denominator_is_not_divided() -> None:
    values = _full_values()
    values[1040] = -1.0

    result = calculate_liquidity_observation(values, "ILC")

    assert result["state"] == "non_positive_denominator"
    assert result["value"] is None
    assert "denominator_negative" in result["flags"]


def test_negative_numerator_remains_derivable_but_flagged() -> None:
    values = _full_values()
    values[1479] = 5.0
    values[11160] = 10.0
    values[351] = 1.0

    result = calculate_liquidity_observation(values, "ILC")

    assert result["state"] == "derivable"
    assert result["value"] < 0
    assert result["flags"] == ["negative_numerator", "negative_ratio"]


def test_duplicate_candidate_cmpids_exclude_entity_from_statistics() -> None:
    source = {
        "balance_values": {202606: _full_values()},
        "duplicate_balance_cmpid_rows": 2,
    }
    experiment = build_entity_liquidity_experiment(_entity(), source, 202606)
    summary = liquidity_experiment_summary([experiment], 202606)

    assert experiment["quality_excluded_from_statistics"] is True
    assert summary["quality_excluded_count"] == 1
    assert (
        summary["metrics"]["ILC"]["current_distribution_excluding_quality_issues"][
            "count"
        ]
        == 0
    )


def test_complete_history_is_measured_without_becoming_score() -> None:
    periods = [
        202507,
        202508,
        202509,
        202510,
        202511,
        202512,
        202601,
        202602,
        202603,
        202604,
        202605,
        202606,
    ]
    source = {
        "balance_values": {period: _full_values() for period in periods},
        "duplicate_balance_cmpid_rows": 0,
    }
    experiment = build_entity_liquidity_experiment(_entity(), source, 202606)

    assert experiment["metrics"]["ILC"]["history"]["12"]["complete_derivable"] is True
    assert experiment["metrics"]["ILT"]["history"]["12"]["complete_derivable"] is True
    assert "score" not in experiment


def test_artifact_validator_rejects_no_valid_experiment() -> None:
    source = {
        "balance_values": {202606: _full_values()},
        "duplicate_balance_cmpid_rows": 0,
    }
    experiment = build_entity_liquidity_experiment(_entity(), source, 202606)
    summary = liquidity_experiment_summary([experiment], 202606)
    payload = {
        "artifact": "v2_liquidity_experiment",
        "status": "experimental",
        "summary": summary,
        "entities": [experiment],
    }

    validate_liquidity_experiment(payload)
