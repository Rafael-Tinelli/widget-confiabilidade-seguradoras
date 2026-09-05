from __future__ import annotations

import pytest

from api.v2.build_conduct_coverage_reconciliation import (
    ConductCoverageReconciliationError,
    build_reconciliation,
)


def _entity(
    entity_id: str,
    fip: str,
    cnpj: str,
    *,
    insurance: bool,
    pension: bool = False,
) -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_name": entity_id,
        "activities": {
            "insurance": insurance,
            "pension": pension,
            "capitalization": False,
            "reinsurance": False,
        },
        "eligibility": {"regulatory_universe_eligible": True},
    }


def _conduct(entity_id: str, fip: str, cnpj: str, complaints: int) -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_name": entity_id,
        "display_name": entity_id,
        "totals": {
            "complaints": complaints,
            "responded": complaints,
            "finalized": complaints,
            "satisfaction_count": 0,
            "average_satisfaction": None,
        },
        "film": {"history_state": "synthetic"},
    }


def _ses_entity(
    direct: float,
    earned: float | None = None,
    *,
    direct_missing_rows: int = 0,
) -> dict:
    earned_value = direct if earned is None else earned
    return {
        "months": {
            202601: {
                "insurance_premium_direct": direct,
                "insurance_premium_earned": earned_value,
                "insurance_premium_direct_missing_rows": direct_missing_rows,
                "insurance_premium_earned_missing_rows": 0,
                "insurance_branches": {
                    1001: {
                        "premium_direct": direct,
                        "premium_earned": earned_value,
                        "rows": 1.0,
                    }
                },
            }
        }
    }


def _payloads(
    entities: list[dict], complaints: dict[str, int], ses_values: dict[str, float]
):
    eligibility = {"entities": entities}
    conduct = {
        "source": {"months": ["2026-01"]},
        "entities": [
            _conduct(
                entity["entity_id"],
                entity["fip_code"],
                entity["cnpj"],
                complaints.get(entity["entity_id"], 0),
            )
            for entity in entities
        ],
    }
    ses = {
        "periods": [202601],
        "entities": {
            entity["fip_code"]: _ses_entity(
                ses_values.get(entity["entity_id"], 0.0)
            )
            for entity in entities
        },
    }
    return eligibility, conduct, ses


def _by_id(payload: dict) -> dict[str, dict]:
    return {row["entity_id"]: row for row in payload["entities"]}


def test_pure_insurer_can_be_pressure_candidate_without_noninsurance_amounts() -> None:
    entities = [_entity("pure", "000001", "11111111000191", insurance=True)]
    eligibility, conduct, ses = _payloads(
        entities, {"pure": 10}, {"pure": 1000.0}
    )
    payload = build_reconciliation(eligibility, conduct, ses, {"relationships": []})
    row = _by_id(payload)["pure"]

    assert row["conduct_evidence_state"] == "observed"
    assert row["pressure_comparability"] == {
        "state": "direct_one_to_one_candidate",
        "pressure_eligible_candidate": True,
        "reason_code": None,
        "relationship_ids": [],
    }
    assert row["insurance_exposure_12m"]["private_pension_amount_used"] is False
    assert row["insurance_exposure_12m"]["capitalization_amount_used"] is False
    assert row["insurance_exposure_12m"]["insurance_premium_direct_complete"] is True


def test_missing_direct_premium_blocks_pressure_without_becoming_zero() -> None:
    entity = _entity("incomplete", "000013", "14141414000174", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"incomplete": 7}, {"incomplete": 500.0}
    )
    ses["entities"]["000013"] = _ses_entity(
        500.0, direct_missing_rows=1
    )

    payload = build_reconciliation(eligibility, conduct, ses, {"relationships": []})
    row = _by_id(payload)["incomplete"]

    assert row["insurance_exposure_12m"]["insurance_premium_direct"] == 500.0
    assert row["insurance_exposure_12m"]["insurance_premium_direct_missing_rows"] == 1
    assert row["insurance_exposure_12m"]["insurance_premium_direct_complete"] is False
    assert row["pressure_comparability"] == {
        "state": "insurance_premium_direct_incomplete",
        "pressure_eligible_candidate": False,
        "reason_code": "missing_direct_premium_rows_in_comparison_window",
        "relationship_ids": [],
    }
    assert "incomplete_insurance_premium_direct" in row["universe_audit_flags"]


def test_missing_complaint_total_is_not_imputed_to_zero() -> None:
    entity = _entity("missing", "000014", "15151515000165", insurance=True)
    eligibility, conduct, ses = _payloads([entity], {"missing": 0}, {"missing": 500.0})
    del conduct["entities"][0]["totals"]["complaints"]

    with pytest.raises(
        ConductCoverageReconciliationError,
        match=r"totals\.complaints is required; missing is not zero",
    ):
        build_reconciliation(eligibility, conduct, ses, {"relationships": []})


def test_duplicate_comparison_months_are_rejected() -> None:
    entity = _entity("dup-month", "000015", "16161616000156", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"dup-month": 1}, {"dup-month": 500.0}
    )
    conduct["source"]["months"] = ["2026-01", "2026-01"]

    with pytest.raises(
        ConductCoverageReconciliationError,
        match="duplicate comparison months",
    ):
        build_reconciliation(eligibility, conduct, ses, {"relationships": []})


def test_out_of_order_comparison_months_are_rejected() -> None:
    entity = _entity("order", "000016", "17171717000147", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"order": 1}, {"order": 500.0}
    )
    conduct["source"]["months"] = ["2026-02", "2026-01"]

    with pytest.raises(
        ConductCoverageReconciliationError,
        match="comparison months must be chronological",
    ):
        build_reconciliation(eligibility, conduct, ses, {"relationships": []})


def test_gapped_comparison_months_are_rejected() -> None:
    entity = _entity("gap", "000017", "18181818000138", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"gap": 1}, {"gap": 500.0}
    )
    conduct["source"]["months"] = ["2026-01", "2026-03"]

    with pytest.raises(
        ConductCoverageReconciliationError,
        match="comparison months must be consecutive",
    ):
        build_reconciliation(eligibility, conduct, ses, {"relationships": []})


def test_fractional_exposure_row_count_is_rejected() -> None:
    entity = _entity("fractional", "000018", "19191919000129", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"fractional": 1}, {"fractional": 500.0}
    )
    ses["entities"]["000018"]["months"][202601]["insurance_branches"][1001][
        "rows"
    ] = 1.5

    with pytest.raises(
        ConductCoverageReconciliationError,
        match=r"invalid non-negative integer insurance_branches\[1001\]\.rows",
    ):
        build_reconciliation(eligibility, conduct, ses, {"relationships": []})


def test_hybrid_insurance_pension_keeps_conduct_but_blocks_p3_pressure() -> None:
    entities = [
        _entity("hybrid", "000002", "22222222000182", insurance=True, pension=True)
    ]
    eligibility, conduct, ses = _payloads(
        entities, {"hybrid": 250}, {"hybrid": 5000.0}
    )
    payload = build_reconciliation(eligibility, conduct, ses, {"relationships": []})
    row = _by_id(payload)["hybrid"]

    assert row["conduct_evidence_state"] == "observed"
    assert row["pressure_comparability"]["state"] == (
        "hybrid_insurance_pension_requires_product_numerator"
    )
    assert row["pressure_comparability"]["pressure_eligible_candidate"] is False
    assert row["widget_coverage_state"] == "conduct_observed_pressure_unavailable"


def test_youse_style_relation_keeps_complaints_on_subject_and_blocks_carrier_pressure() -> None:
    subject = _entity("brand", "000003", "33333333000173", insurance=False)
    carrier = _entity("carrier", "000004", "44444444000164", insurance=True)
    eligibility, conduct, ses = _payloads(
        [subject, carrier],
        {"brand": 1367, "carrier": 379},
        {"brand": 0.0, "carrier": 2_000_000.0},
    )
    relationships = {
        "relationships": [
            {
                "relationship_id": "brand-carrier",
                "relationship_type": "consumer_subject_single_risk_carrier",
                "subject_cnpj": subject["cnpj"],
                "target_cnpjs": [carrier["cnpj"]],
                "pressure_policy": "brand_specific_exposure_required",
                "reconciliation_state": (
                    "consumer_subject_single_carrier_exposure_not_brand_specific"
                ),
                "evidence": [{"authority": "official", "fact": "synthetic"}],
            }
        ]
    }
    payload = build_reconciliation(eligibility, conduct, ses, relationships)
    rows = _by_id(payload)

    assert rows["brand"]["complaints_12m"] == 1367
    assert rows["brand"]["pressure_comparability"]["state"] == (
        "consumer_subject_single_carrier_exposure_not_brand_specific"
    )
    assert rows["carrier"]["complaints_12m"] == 379
    assert rows["carrier"]["pressure_comparability"]["state"] == (
        "shared_exposure_with_external_consumer_subject"
    )
    assert rows["carrier"]["pressure_comparability"]["pressure_eligible_candidate"] is False


def test_portfolio_transfer_blocks_both_sides_until_temporal_reconciliation() -> None:
    source = _entity("old", "000005", "55555555000155", insurance=True)
    target = _entity("new", "000006", "66666666000146", insurance=True)
    eligibility, conduct, ses = _payloads(
        [source, target],
        {"old": 2589, "new": 0},
        {"old": 0.0, "new": 8_000_000.0},
    )
    relationships = {
        "relationships": [
            {
                "relationship_id": "transfer",
                "relationship_type": "insurance_portfolio_transfer",
                "subject_cnpj": source["cnpj"],
                "target_cnpjs": [target["cnpj"]],
                "effective_from": "2026-01-01",
                "pressure_policy": "temporal_reconciliation_required",
                "reconciliation_state": "portfolio_transfer_requires_temporal_reconciliation",
                "evidence": [{"authority": "official", "fact": "synthetic"}],
            }
        ]
    }
    payload = build_reconciliation(eligibility, conduct, ses, relationships)
    rows = _by_id(payload)

    assert rows["old"]["complaints_12m"] == 2589
    assert rows["old"]["pressure_comparability"]["pressure_eligible_candidate"] is False
    assert rows["new"]["complaints_12m"] == 0
    assert rows["new"]["pressure_comparability"]["state"] == (
        "portfolio_transfer_counterparty_requires_temporal_reconciliation"
    )


def test_multi_carrier_subject_does_not_allocate_generic_complaints() -> None:
    subject = _entity("generic", "000007", "77777777000137", insurance=False)
    carrier_a = _entity("carrier-a", "000008", "88888888000128", insurance=True)
    carrier_b = _entity(
        "carrier-b", "000009", "99999999000119", insurance=True, pension=True
    )
    eligibility, conduct, ses = _payloads(
        [subject, carrier_a, carrier_b],
        {"generic": 68, "carrier-a": 3305, "carrier-b": 5014},
        {"generic": 0.0, "carrier-a": 10_000_000.0, "carrier-b": 4_000_000.0},
    )
    relationships = {
        "relationships": [
            {
                "relationship_id": "split",
                "relationship_type": "insurance_portfolio_split",
                "subject_cnpj": subject["cnpj"],
                "target_cnpjs": [carrier_a["cnpj"], carrier_b["cnpj"]],
                "pressure_policy": "product_split_required",
                "reconciliation_state": "multi_carrier_subject_requires_product_split",
                "evidence": [{"authority": "official", "fact": "synthetic"}],
            }
        ]
    }
    payload = build_reconciliation(eligibility, conduct, ses, relationships)
    rows = _by_id(payload)

    assert rows["generic"]["complaints_12m"] == 68
    assert rows["carrier-a"]["complaints_12m"] == 3305
    assert rows["carrier-b"]["complaints_12m"] == 5014
    assert rows["generic"]["pressure_comparability"]["state"] == (
        "multi_carrier_subject_requires_product_split"
    )
    assert rows["carrier-a"]["pressure_comparability"]["state"] == (
        "shared_consumer_subject_requires_product_split"
    )


def test_runoff_remains_searchable_without_current_premium_pressure() -> None:
    entity = _entity("runoff", "000010", "10101010000101", insurance=False)
    eligibility, conduct, ses = _payloads(
        [entity], {"runoff": 19}, {"runoff": 0.0}
    )
    relationships = {
        "relationships": [
            {
                "relationship_id": "runoff",
                "relationship_type": "runoff",
                "subject_cnpj": entity["cnpj"],
                "target_cnpjs": [],
                "pressure_policy": "current_premium_pressure_not_applicable",
                "reconciliation_state": "runoff_pressure_not_applicable",
                "evidence": [{"authority": "official", "fact": "synthetic"}],
            }
        ]
    }
    payload = build_reconciliation(eligibility, conduct, ses, relationships)
    row = _by_id(payload)["runoff"]

    assert row["conduct_evidence_state"] == "observed"
    assert row["pressure_comparability"]["state"] == "runoff_pressure_not_applicable"
    assert row["widget_coverage_state"] == "conduct_observed_pressure_unavailable"


def test_negative_direct_premium_is_review_state_not_pressure() -> None:
    entity = _entity("negative", "000011", "12121212000192", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"negative": 32}, {"negative": -50.0}
    )
    payload = build_reconciliation(eligibility, conduct, ses, {"relationships": []})
    row = _by_id(payload)["negative"]

    assert row["pressure_comparability"]["state"] == (
        "negative_direct_premium_requires_accounting_review"
    )
    assert row["pressure_comparability"]["pressure_eligible_candidate"] is False
    assert "negative_insurance_premium_direct" in row["universe_audit_flags"]


def test_zero_complaints_is_not_labeled_favorable() -> None:
    entity = _entity("quiet", "000012", "13131313000183", insurance=True)
    eligibility, conduct, ses = _payloads(
        [entity], {"quiet": 0}, {"quiet": 1000.0}
    )
    payload = build_reconciliation(eligibility, conduct, ses, {"relationships": []})
    row = _by_id(payload)["quiet"]

    assert row["conduct_evidence_state"] == "no_observed_complaints"
    assert row["widget_coverage_state"] == "no_observed_complaints_pressure_candidate"
    assert "favorable" not in str(row).lower()
