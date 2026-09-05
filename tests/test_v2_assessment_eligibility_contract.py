from copy import deepcopy

import pytest

from api.v2.build_assessment_eligibility_contract import (
    OPERATIONAL_FRESHNESS_POLICY,
    build_assessment_eligibility_contract,
)


def _regulatory(entity_ids: list[str]) -> dict:
    entities = []
    for index, entity_id in enumerate(entity_ids):
        entities.append(
            {
                "entity_id": entity_id,
                "fip_code": f"{index + 1:06d}",
                "legal_name": f"SEGURADORA {index + 1}",
                "eligibility": {
                    "regulatory_universe_eligible": True,
                },
            }
        )
    return {
        "artifact": "v2_entity_eligibility_inventory",
        "generated_at": "2026-08-26T00:00:00Z",
        "meta": {
            "eligibility_version": "2.0-draft-regulatory-gate-1",
        },
        "entities": entities,
    }


def _semantic_row(
    entity_id: str,
    public_class: str,
    confidence: str = "historico_estabelecido",
    *,
    complete: bool = True,
) -> dict:
    if complete:
        state_by_class = {
            "favorable_reading": "no_current_core_adverse_signal",
            "attention": "conduct_pressure_only",
            "prudential_warning": "capital_shortfall_without_conduct_pressure",
        }
        title_by_class = {
            "favorable_reading": "Leitura central favorável",
            "attention": "Atenção à Conduta",
            "prudential_warning": "Alerta prudencial de capital",
        }
        return {
            "entity_id": entity_id,
            "assessment_completeness": "joint_core_complete",
            "semantic_public_assessment_supported": True,
            "matrix_state": state_by_class[public_class],
            "public_assessment": {
                "public_class": public_class,
                "title": title_by_class[public_class],
            },
            "qualifiers": {
                "financial_confidence": confidence,
            },
            "evidence_readiness": "avaliacao_conjunta_central_completa",
        }

    return {
        "entity_id": entity_id,
        "assessment_completeness": "joint_core_incomplete",
        "semantic_public_assessment_supported": False,
        "matrix_state": None,
        "public_assessment": {
            "public_class": "evidence_incomplete",
            "title": "Avaliação conjunta incompleta",
        },
        "qualifiers": {
            "financial_confidence": confidence,
        },
        "evidence_readiness": "conduta_nao_comparavel_com_seguranca",
    }


def _semantic(rows: list[dict]) -> dict:
    complete_count = sum(
        bool(row["semantic_public_assessment_supported"]) for row in rows
    )
    return {
        "artifact": "v2_cross_pillar_assessment_semantic_contract",
        "generated_at": "2026-08-26T00:00:00Z",
        "version": "test-semantic-1",
        "status": "cross_pillar_assessment_semantic_contract_closed",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "population": {
            "regulatory_universe": len(rows),
            "semantic_public_assessment_supported": complete_count,
            "joint_core_incomplete": len(rows) - complete_count,
        },
        "entities": rows,
    }


def test_gate_is_outcome_independent_and_ranking_stays_closed():
    ids = ["fip:000001", "fip:000002", "fip:000003", "fip:000004"]
    semantic = _semantic(
        [
            _semantic_row(ids[0], "favorable_reading"),
            _semantic_row(ids[1], "attention"),
            _semantic_row(ids[2], "prudential_warning"),
            _semantic_row(ids[3], "attention", complete=False),
        ]
    )

    payload = build_assessment_eligibility_contract(_regulatory(ids), semantic)

    assert payload["population"] == {
        "regulatory_universe": 4,
        "semantic_public_assessment_supported": 3,
        "assessment_eligible": 3,
        "assessment_not_eligible": 1,
        "ranking_eligible": 0,
    }
    assert payload["diagnostics"]["eligible_public_class_counts"] == {
        "attention": 1,
        "favorable_reading": 1,
        "prudential_warning": 1,
    }
    assert all(not row["ranking_eligible"] for row in payload["entities"])
    assert payload["gate_contract"]["performance_result_used_for_eligibility"] is False
    assert payload["source_contracts"]["operational_freshness_policy"] == (
        OPERATIONAL_FRESHNESS_POLICY
    )
    assert OPERATIONAL_FRESHNESS_POLICY == (
        "single_generation_workspace_cross_run_latest_successful_restore_forbidden"
    )


def test_limited_history_does_not_block_complete_assessment():
    ids = ["fip:000001"]
    semantic = _semantic(
        [
            _semantic_row(
                ids[0],
                "attention",
                confidence="historico_limitado",
            )
        ]
    )

    payload = build_assessment_eligibility_contract(_regulatory(ids), semantic)
    row = payload["entities"][0]

    assert row["assessment_eligible"] is True
    assert row["evidence_confidence"]["limited_history_blocks_assessment"] is False
    assert row["evidence_confidence"]["limited_history_is_performance_penalty"] is False


def test_incomplete_evidence_cannot_become_neutral_or_eligible():
    ids = ["fip:000001"]
    semantic = _semantic(
        [_semantic_row(ids[0], "favorable_reading", complete=False)]
    )

    payload = build_assessment_eligibility_contract(_regulatory(ids), semantic)
    row = payload["entities"][0]

    assert row["assessment_eligible"] is False
    assert row["assessment_state"] == "not_eligible_joint_evidence_incomplete"
    assert "missingness_not_treated_as_neutral" in row["assessment_reason_codes"]
    assert row["ranking_state"] == "blocked_by_assessment_ineligibility"


def test_insufficient_core_confidence_blocks_without_becoming_performance():
    ids = ["fip:000001"]
    semantic = _semantic(
        [
            _semantic_row(
                ids[0],
                "prudential_warning",
                confidence="evidencia_central_insuficiente",
            )
        ]
    )

    payload = build_assessment_eligibility_contract(_regulatory(ids), semantic)
    row = payload["entities"][0]

    assert row["assessment_eligible"] is False
    assert row["assessment_state"] == "not_eligible_core_evidence_confidence"
    assert row["performance_used_to_decide_eligibility"] is False


def test_population_mismatch_fails_closed():
    ids = ["fip:000001", "fip:000002"]
    semantic = _semantic([_semantic_row(ids[0], "favorable_reading")])

    with pytest.raises(ValueError, match="regulatory universe count mismatch"):
        build_assessment_eligibility_contract(_regulatory(ids), semantic)


def test_input_payload_is_not_mutated():
    ids = ["fip:000001"]
    regulatory = _regulatory(ids)
    semantic = _semantic([_semantic_row(ids[0], "favorable_reading")])
    original_regulatory = deepcopy(regulatory)
    original_semantic = deepcopy(semantic)

    build_assessment_eligibility_contract(regulatory, semantic)

    assert regulatory == original_regulatory
    assert semantic == original_semantic
