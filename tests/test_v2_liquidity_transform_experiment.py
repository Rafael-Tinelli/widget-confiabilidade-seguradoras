from __future__ import annotations

import math

from api.v2.liquidity_transform_experiment import (
    build_liquidity_transform_experiment,
    geometric_history_ratio,
    hard_log_saturation,
    tanh_log_transform,
)


def _periods() -> list[int]:
    return [
        202407,
        202408,
        202409,
        202410,
        202411,
        202412,
        202501,
        202502,
        202503,
        202504,
        202505,
        202506,
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


def _entity(entity_id: str, base: float, drift: float) -> dict:
    series = []
    for index, period in enumerate(_periods()):
        value = base * (1.0 + drift * index)
        series.append(
            {
                "period": period,
                "state": "derivable",
                "value": value,
                "numerator": value * 100.0,
                "denominator": 100.0,
                "missing_cmpids": [],
                "flags": [],
            }
        )
    return {
        "entity_id": entity_id,
        "legal_name": f"SEGURADORA {entity_id}",
        "metrics": {
            "ILC": {"series_last_36": series, "current": series[-1]},
            "ILT": {"series_last_36": series, "current": series[-1]},
        },
    }


def _payload() -> dict:
    entities = [
        _entity("fip:000001", 0.65, 0.010),
        _entity("fip:000002", 0.90, 0.006),
        _entity("fip:000003", 1.05, 0.004),
        _entity("fip:000004", 1.25, 0.003),
        _entity("fip:000005", 1.70, 0.002),
        _entity("fip:000006", 3.00, 0.001),
    ]
    return {
        "artifact": "v2_liquidity_experiment",
        "status": "experimental",
        "summary": {"reference_period": 202606},
        "entities": entities,
    }


def test_hard_log_saturation_is_centered_and_capped() -> None:
    assert math.isclose(hard_log_saturation(1.0, 3.0), 0.5)
    assert math.isclose(hard_log_saturation(3.0, 3.0), 1.0)
    assert math.isclose(hard_log_saturation(30.0, 3.0), 1.0)
    assert math.isclose(hard_log_saturation(1.0 / 3.0, 3.0), 0.0)
    assert math.isclose(hard_log_saturation(0.01, 3.0), 0.0)


def test_tanh_log_transform_is_bounded_and_monotonic() -> None:
    low = tanh_log_transform(0.5, 1.0)
    parity = tanh_log_transform(1.0, 1.0)
    high = tanh_log_transform(2.0, 1.0)

    assert low is not None and high is not None and parity is not None
    assert 0.0 < low < parity < high < 1.0
    assert math.isclose(parity, 0.5)


def test_geometric_history_blend_has_expected_endpoints() -> None:
    assert math.isclose(geometric_history_ratio(4.0, 1.0, 0.0), 1.0)
    assert math.isclose(geometric_history_ratio(4.0, 1.0, 1.0), 4.0)
    assert math.isclose(geometric_history_ratio(4.0, 1.0, 0.5), 2.0)


def test_transform_experiment_keeps_raw_and_history_families_separate() -> None:
    result = build_liquidity_transform_experiment(_payload())
    ilt = result["metrics"]["ILT"]["transforms"]

    assert result["status"] == "experimental"
    assert ilt["raw_ratio"]["current_distribution"]["count"] == 6
    assert ilt["tanh_log_tau_1_0"]["current_rank_spearman_vs_raw"] == 1.0
    assert ilt["history_geo_current_050"]["current_distribution"]["count"] == 6
    assert (
        ilt["history_geo_current_050"]["rank_stability_vs_current"]["summary"][
            "count"
        ]
        > 0
    )


def test_hard_cap_creates_explicit_saturation_resolution_loss() -> None:
    payload = _payload()
    payload["entities"].append(_entity("fip:000007", 5.0, 0.001))
    result = build_liquidity_transform_experiment(payload)
    item = result["metrics"]["ILT"]["transforms"]["hard_log_cap_2_0"]

    assert item["current_resolution"]["exact_ceiling_count"] >= 2
    assert item["current_resolution"]["unique_values"] < 7


def test_transform_artifact_contains_no_scoring_contract_fields() -> None:
    result = build_liquidity_transform_experiment(_payload())
    serialized = str(result)

    assert "assessment_eligible" not in serialized
    assert "ranking_eligible" not in serialized
    assert "'score'" not in serialized
    assert "'rating'" not in serialized
