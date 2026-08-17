from __future__ import annotations

from api.v2.build_financial_evidence_inventory import build_financial_evidence_inventory
from api.v2.financial_evidence import month_window


def test_inventory_keeps_assessment_and_ranking_closed() -> None:
    periods = month_window(202606, 12)
    eligibility = {
        "meta": {"regulatory_universe_eligible_count": 1},
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "000001",
                "eligibility": {"regulatory_universe_eligible": True},
            }
        ],
    }
    source = {
        "source": {"source_id": "test"},
        "reference_periods": {
            "capital": 202606,
            "balance": 202606,
            "insurance_operations": 202606,
        },
        "entities": {
            "000001": {
                "capital_history": {
                    period: {
                        "period": period,
                        "pla_adjusted": 100.0,
                        "cmr": 80.0,
                    }
                    for period in periods
                },
                "balance_periods": set(periods),
                "balance_values": {period: {} for period in periods},
                "insurance_operation_periods": set(periods),
                "nonzero_premium_periods": set(periods),
            }
        },
    }

    payload = build_financial_evidence_inventory(eligibility, source)

    assert payload["meta"]["core_financial_evidence_ready_count"] == 1
    assert payload["meta"]["assessment_eligible_count"] == 0
    assert payload["meta"]["ranking_eligible_count"] == 0
    assert payload["entities"][0]["financial_evidence"]["state"] == "complete_core_history"
