from __future__ import annotations

from copy import deepcopy

from api.v2.build_financial_methodology_closure import (
    build_financial_methodology_closure,
)


FIXTURE_POPULATION = 123


def _payloads():
    financial_entities = []
    liquidity_entities = []
    operating_entities = []
    for index in range(FIXTURE_POPULATION):
        entity_id = f"fip:{index:06d}"
        financial_entities.append(
            {
                "entity_id": entity_id,
                "fip_code": f"{index:06d}",
                "legal_name": f"Seguradora {index}",
                "eligibility": {"regulatory_universe_eligible": True},
                "financial_evidence": {
                    "state": "complete_core_history",
                    "capital": {
                        "pla_cmr_ratio_state": "derivable",
                        "pla_cmr_ratio": 1.20,
                    },
                },
            }
        )
        liquidity_entities.append(
            {
                "entity_id": entity_id,
                "metrics": {
                    "ILT": {
                        "current": {
                            "state": "derivable",
                            "value": 1.10,
                        }
                    }
                },
            }
        )
        operating_entities.append(
            {
                "entity_id": entity_id,
                "operating_state": {
                    "operating_signal": "balanced_persistent",
                    "history_state": "established",
                    "formula_state": "derivable",
                },
            }
        )

    financial = {
        "meta": {
            "regulatory_eligible_count": FIXTURE_POPULATION,
            "financial_period_maturity": {"selected_period": 202605},
        },
        "entities": financial_entities,
    }
    liquidity = {
        "summary": {"reference_period": 202605},
        "entities": liquidity_entities,
    }
    operating = {
        "summary": {"reference_period": 202605},
        "entities": operating_entities,
    }
    return financial, liquidity, operating


def test_capital_shortfall_cannot_be_offset_by_high_liquidity():
    financial, liquidity, operating = _payloads()
    financial["entities"][0]["financial_evidence"]["capital"]["pla_cmr_ratio"] = 0.90
    liquidity["entities"][0]["metrics"]["ILT"]["current"]["value"] = 8.0

    payload = build_financial_methodology_closure(financial, liquidity, operating)
    first = payload["entities"][0]

    assert payload["population"]["regulatory_universe"] == FIXTURE_POPULATION
    assert first["core_financial_signal"] == "capital_requirement_shortfall_observed"
    assert first["liquidity"]["state"] == "ilt_at_or_above_arithmetic_parity"
    assert (
        payload["methodology_decisions"]["combination"][
            "capital_shortfall_can_be_offset_by_liquidity"
        ]
        is False
    )
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"


def test_excess_capital_does_not_erase_liquidity_pressure_or_create_bonus_tier():
    financial, liquidity, operating = _payloads()
    financial["entities"][1]["financial_evidence"]["capital"]["pla_cmr_ratio"] = 5.0
    liquidity["entities"][1]["metrics"]["ILT"]["current"]["value"] = 0.80

    payload = build_financial_methodology_closure(financial, liquidity, operating)
    row = payload["entities"][1]

    assert row["core_financial_signal"] == "capital_requirement_met_with_liquidity_pressure"
    assert row["capital"]["magnitude_rewarded"] is False
    assert row["liquidity"]["magnitude_rewarded"] is False
    assert payload["methodology_decisions"]["combination"]["weighted_average_selected"] is False
    assert payload["methodology_decisions"]["capital"]["positive_tiers_above_reference_selected"] is False


def test_short_history_changes_confidence_not_current_core_signal():
    financial, liquidity, operating = _payloads()
    financial["entities"][2]["financial_evidence"]["state"] = "limited_core_history"
    baseline = build_financial_methodology_closure(financial, liquidity, operating)

    financial_complete = deepcopy(financial)
    financial_complete["entities"][2]["financial_evidence"]["state"] = "complete_core_history"
    complete = build_financial_methodology_closure(
        financial_complete,
        liquidity,
        operating,
    )

    limited_row = baseline["entities"][2]
    complete_row = complete["entities"][2]
    assert limited_row["core_financial_signal"] == complete_row["core_financial_signal"]
    assert limited_row["evidence_confidence"] == "limited_core_history"
    assert complete_row["evidence_confidence"] == "established_core_history"
    assert baseline["methodology_decisions"]["confidence"]["short_history_penalizes_performance"] is False
