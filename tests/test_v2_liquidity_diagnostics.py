from __future__ import annotations

from api.v2.liquidity_diagnostics import build_liquidity_diagnostics
from api.v2.liquidity_experiment import build_entity_liquidity_experiment


def _values(multiplier: float = 1.0) -> dict[int, float]:
    return {
        1479: 1000.0 * multiplier,
        11160: 10.0,
        351: 5.0,
        1040: 500.0,
        331: 100.0,
        11187: 0.0,
        5503: 0.0,
        6449: 50.0,
    }


def _entity(index: int) -> dict:
    return {
        "entity_id": f"fip:{index:06d}",
        "fip_code": f"{index:06d}",
        "legal_name": f"TESTE {index}",
    }


def test_diagnostics_publish_descriptive_bands_and_use_new_pla_for_capital() -> None:
    experiments = []
    sources = {}
    for index, multiplier in enumerate((0.5, 1.0, 2.0), start=1):
        source = {
            "balance_values": {202606: _values(multiplier)},
            "duplicate_balance_cmpid_rows": 0,
            "capital_history": {
                202606: {
                    # Deliberately inverse to prove the diagnostic does not use it.
                    "pla_adjusted": 100.0 / multiplier,
                    "new_pla": 100.0 * multiplier,
                    "cmr": 80.0,
                }
            },
        }
        experiments.append(
            build_entity_liquidity_experiment(_entity(index), source, 202606)
        )
        sources[f"{index:06d}"] = source

    diagnostics = build_liquidity_diagnostics(
        experiments,
        {
            "reference_periods": {"balance": 202606},
            "entities": sources,
        },
    )

    ilc = diagnostics["metrics"]["ILC"]
    assert ilc["descriptive_bands"]["below_1_00_total"] >= 1
    redundancy = ilc["capital_pla_cmr_redundancy"]
    assert redundancy["paired_count"] == 3
    assert redundancy["raw"]["spearman"] == 1.0


def test_zero_denominator_is_described_with_prior_observation() -> None:
    source = {
        "balance_values": {
            202605: _values(),
            202606: {key: 0.0 for key in _values()},
        },
        "duplicate_balance_cmpid_rows": 0,
        "capital_history": {},
    }
    experiment = build_entity_liquidity_experiment(_entity(1), source, 202606)
    diagnostics = build_liquidity_diagnostics(
        [experiment],
        {
            "reference_periods": {"balance": 202606},
            "entities": {"000001": source},
        },
    )

    item = diagnostics["metrics"]["ILC"]["non_derivable_current"][0]
    assert item["state"] == "non_positive_denominator"
    assert item["latest_prior_derivable"]["period"] == 202605
