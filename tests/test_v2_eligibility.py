import pytest

from api.v2.eligibility import (
    EligibilityInvariantError,
    apply_eligibility,
    derive_eligibility,
    eligibility_summary,
    validate_eligibility,
)


def _entity(**overrides):
    entity = {
        "entity_id": "fip:001234",
        "fip_code": "001234",
        "cnpj": "12345678000195",
        "legal_name": "SEGURADORA TESTE S.A.",
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "legal_lifecycle": {"cadastral_status": "active"},
        "relationships": [],
        "query_context": {
            "entity_state": "current_ordinary_insurer",
            "filter_bucket": "insurers",
        },
    }
    entity.update(overrides)
    return entity


def test_current_ordinary_insurer_passes_only_regulatory_gate():
    state = derive_eligibility(_entity())

    assert state["regulatory_universe_eligible"] is True
    assert state["regulatory_universe_id"] == "ordinary_current_insurers"
    assert state["assessment_state"] == "pending_evidence"
    assert state["assessment_eligible"] is False
    assert state["ranking_state"] == "pending_assessment"
    assert state["ranking_eligible"] is False
    assert state["comparison_cohort"] is None
    assert "receita_legal_entity_active" in state["reason_codes"]
    assert "financial_evidence_gate" in state["pending_requirements"]


def test_missing_receita_crosscheck_does_not_revoke_susep_license():
    entity = _entity(legal_lifecycle=None)
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is True
    assert "receita_lifecycle_crosscheck_unavailable" in state["reason_codes"]


def test_non_active_receita_status_blocks_regulatory_universe():
    entity = _entity(legal_lifecycle={"cadastral_status": "suspended"})
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is False
    assert "legal_entity_not_active_in_receita" in state["reason_codes"]


def test_sandbox_never_enters_ordinary_insurer_universe():
    entity = _entity(
        entity_type="sandbox_participant",
        regulatory_regime="sandbox",
        regulatory_status="sandbox_authorized",
        query_context={"filter_bucket": "sandbox"},
    )
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is False
    assert state["assessment_eligible"] is False
    assert state["ranking_eligible"] is False
    assert "sandbox_experimental_regime" in state["reason_codes"]


def test_special_regime_insurer_is_not_eligible():
    entity = _entity(
        regulatory_regime="special",
        regulatory_status="liquidation",
        query_context={"filter_bucket": "special_regime"},
    )
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is False
    assert "special_regulatory_regime" in state["reason_codes"]
    assert "not_currently_licensed_as_insurer" in state["reason_codes"]


def test_historical_incorporated_insurer_is_not_eligible():
    entity = _entity(
        regulatory_status="unknown",
        legal_lifecycle={"cadastral_status": "closed"},
        relationships=[
            {
                "relationship_type": "incorporated_into",
                "target_entity_id": "fip:009999",
            }
        ],
        query_context={"filter_bucket": "historical"},
    )
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is False
    assert "historical_legal_entity" in state["reason_codes"]


@pytest.mark.parametrize(
    ("entity_type", "reason"),
    [
        ("open_pension_entity", "different_market_open_pension"),
        ("capitalization_company", "different_market_capitalization"),
        ("local_reinsurer", "different_market_reinsurance"),
    ],
)
def test_other_markets_remain_searchable_but_outside_ranking(entity_type, reason):
    entity = _entity(
        entity_type=entity_type,
        regulatory_status="active_licensed",
        query_context={"filter_bucket": "other"},
    )
    state = derive_eligibility(entity)

    assert state["regulatory_universe_eligible"] is False
    assert reason in state["reason_codes"]


def test_validator_rejects_manual_non_insurer_eligibility_override():
    entity = _entity(entity_type="capitalization_company")
    entity["eligibility"] = {
        "regulatory_universe_eligible": True,
        "assessment_eligible": False,
        "ranking_eligible": False,
    }

    with pytest.raises(EligibilityInvariantError, match="non-insurer"):
        validate_eligibility([entity])


def test_summary_keeps_regulatory_assessment_and_ranking_counts_separate():
    entities = apply_eligibility(
        [
            _entity(),
            _entity(
                entity_id="cnpj:43095690000112",
                entity_type="sandbox_participant",
                regulatory_regime="sandbox",
                regulatory_status="sandbox_authorized",
                query_context={"filter_bucket": "sandbox"},
            ),
        ]
    )
    summary = eligibility_summary(entities)

    assert summary["regulatory_universe_eligible_count"] == 1
    assert summary["assessment_eligible_count"] == 0
    assert summary["ranking_eligible_count"] == 0
    assert summary["eligible_legal_crosscheck_counts"] == {"active": 1}
