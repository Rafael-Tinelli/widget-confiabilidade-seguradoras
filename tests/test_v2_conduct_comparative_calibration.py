from __future__ import annotations

import pytest

from api.v2.build_conduct_comparative_calibration import build_calibration


def _conduct() -> dict:
    return {
        "source": {"months": ["2026-01"]},
        "entities": [
            {
                "entity_id": "fip:000001",
                "fip_code": "1",
                "legal_name": "A Seguradora",
                "totals": {"complaints": 10},
            },
            {
                "entity_id": "fip:000002",
                "fip_code": "2",
                "legal_name": "B Seguradora e Previdencia",
                "totals": {"complaints": 40},
            },
            {
                "entity_id": "fip:000003",
                "fip_code": "3",
                "legal_name": "Fornecedor sem exposicao comparavel",
                "totals": {"complaints": 1367},
            },
            {
                "entity_id": "fip:000004",
                "fip_code": "4",
                "legal_name": "Componente negativo",
                "totals": {"complaints": 5},
            },
        ],
    }


def _ses() -> dict:
    def month(
        direct: float,
        pension: float,
        *,
        product: str | None = None,
    ) -> dict:
        pension_products = {}
        if product is not None:
            pension_products[product] = {"contributions": pension, "rows": 1}
        return {
            "insurance_premium_direct": direct,
            "insurance_premium_earned": direct,
            "pension_contributions": pension,
            "insurance_branches": {
                1001: {
                    "premium_direct": direct,
                    "premium_earned": direct,
                    "rows": 1,
                }
            }
            if direct != 0
            else {},
            "pension_products": pension_products,
        }

    return {
        "periods": [202601],
        "entities": {
            "000001": {"months": {202601: month(100.0, 0.0)}},
            "000002": {
                "months": {202601: month(300.0, 100.0, product="PGBL")}
            },
            "000003": {"months": {202601: month(0.0, 0.0)}},
            "000004": {
                "months": {202601: month(-10.0, 20.0, product="VGBL")}
            },
        },
    }


def _entities_by_id(payload: dict) -> dict[str, dict]:
    return {entity["entity_id"]: entity for entity in payload["entities"]}


def test_calibration_uses_direct_premium_plus_private_pension_contributions() -> None:
    payload = build_calibration(_conduct(), _ses())
    rows = _entities_by_id(payload)

    a = rows["fip:000001"]
    b = rows["fip:000002"]

    assert a["revenue_components_12m"]["combined_revenue_candidate"] == pytest.approx(100)
    assert b["revenue_components_12m"]["insurance_premium_direct"] == pytest.approx(300)
    assert b["revenue_components_12m"]["private_pension_contributions"] == pytest.approx(100)
    assert b["revenue_components_12m"]["combined_revenue_candidate"] == pytest.approx(400)
    assert b["revenue_components_12m"]["private_pension_product_contributions"] == {
        "PGBL": pytest.approx(100)
    }


def test_market_baseline_excludes_mismatch_and_negative_components() -> None:
    payload = build_calibration(_conduct(), _ses())
    baseline = payload["market_baseline"]

    assert baseline["comparable_entities"] == 2
    assert baseline["excluded_entities"] == 2
    assert baseline["market_complaints"] == 50
    assert baseline["market_combined_revenue_candidate"] == pytest.approx(500)
    assert baseline["excluded_by_state"] == {
        "complaints_without_comparable_exposure": 1,
        "negative_exposure_component": 1,
    }

    rows = _entities_by_id(payload)
    assert rows["fip:000003"]["comparability"]["pressure_eligible"] is False
    assert rows["fip:000003"]["pressure"]["pressure_ratio"] is None
    assert rows["fip:000004"]["comparability"]["state"] == "negative_exposure_component"


def test_pressure_is_calculated_only_inside_aligned_population() -> None:
    payload = build_calibration(_conduct(), _ses())
    rows = _entities_by_id(payload)

    assert rows["fip:000001"]["pressure"]["expected_complaints"] == pytest.approx(10)
    assert rows["fip:000001"]["pressure"]["pressure_ratio"] == pytest.approx(1)
    assert rows["fip:000002"]["pressure"]["expected_complaints"] == pytest.approx(40)
    assert rows["fip:000002"]["pressure"]["pressure_ratio"] == pytest.approx(1)
    assert payload["source"]["population_policy"] == (
        "complaints_and_revenue_same_entities_only"
    )


def test_private_pension_is_not_labeled_as_insurance_premium() -> None:
    payload = build_calibration(_conduct(), _ses())
    semantics = payload["candidate_denominator"]["semantics"]

    assert "private-pension" in semantics["private_pension_contributions"].lower()
    assert "not as insurance premiums" in semantics["private_pension_contributions"]
    assert payload["candidate_denominator"]["selected_for_scoring"] is False
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"


def test_calibration_fails_closed_when_ses_window_does_not_cover_conduct() -> None:
    ses = _ses()
    ses["periods"] = []
    with pytest.raises(RuntimeError, match="complete Consumer.gov comparison window"):
        build_calibration(_conduct(), ses)
