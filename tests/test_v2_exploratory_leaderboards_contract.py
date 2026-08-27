from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from api.v2.build_exploratory_leaderboards_contract import (
    build_exploratory_leaderboards_contract,
    write_public_outputs,
)


def _payloads():
    assessment = {
        "artifact": "v2_assessment_eligibility_contract",
        "status": "assessment_eligibility_contract_closed",
        "population": {
            "regulatory_universe": 3,
            "assessment_eligible": 2,
            "assessment_not_eligible": 1,
            "ranking_eligible": 0,
        },
        "entities": [
            {
                "entity_id": "fip:1",
                "fip_code": "1",
                "legal_name": "Alpha Seguros",
                "assessment_eligible": True,
                "assessment_state": "eligible_complete_joint_assessment",
                "ranking_eligible": False,
                "comparison_cohort": None,
            },
            {
                "entity_id": "fip:2",
                "fip_code": "2",
                "legal_name": "Beta Seguros",
                "assessment_eligible": True,
                "assessment_state": "eligible_complete_joint_assessment",
                "ranking_eligible": False,
                "comparison_cohort": None,
            },
            {
                "entity_id": "fip:3",
                "fip_code": "3",
                "legal_name": "Gamma Seguros",
                "assessment_eligible": False,
                "assessment_state": "not_eligible_joint_evidence_incomplete",
                "ranking_eligible": False,
                "comparison_cohort": None,
            },
        ],
    }

    semantic = {
        "artifact": "v2_cross_pillar_assessment_semantic_contract",
        "status": "cross_pillar_assessment_semantic_contract_closed",
        "entities": [
            {
                "entity_id": "fip:1",
                "fip_code": "1",
                "legal_name": "Alpha Seguros",
                "assessment_completeness": "joint_core_complete",
                "matrix_state": "no_current_core_adverse_signal",
                "evidence_readiness": "avaliacao_conjunta_central_completa",
                "public_assessment": {
                    "public_class": "favorable_reading",
                    "title": "Leitura central favorável",
                    "summary": "Sinais centrais favoráveis.",
                    "why_it_matters": "Há leitura conjunta.",
                    "mandatory_limit": "Não é garantia.",
                },
            },
            {
                "entity_id": "fip:2",
                "fip_code": "2",
                "legal_name": "Beta Seguros",
                "assessment_completeness": "joint_core_complete",
                "matrix_state": "conduct_pressure_only",
                "evidence_readiness": "avaliacao_conjunta_central_completa",
                "public_assessment": {
                    "public_class": "attention",
                    "title": "Atenção à Conduta",
                    "summary": "Pressão de Conduta.",
                    "why_it_matters": "Há sinal adverso.",
                    "mandatory_limit": "Não vale para todo cliente.",
                },
            },
            {
                "entity_id": "fip:3",
                "fip_code": "3",
                "legal_name": "Gamma Seguros",
                "assessment_completeness": "joint_core_incomplete",
                "matrix_state": None,
                "evidence_readiness": "conduta_nao_comparavel_com_seguranca",
                "public_assessment": {
                    "public_class": "evidence_incomplete",
                    "title": "Avaliação conjunta incompleta",
                    "summary": "Falta comparabilidade.",
                    "why_it_matters": "Não é possível fechar a leitura.",
                    "mandatory_limit": "Não é negativo nem neutro.",
                },
            },
        ],
    }

    financial = {
        "artifact": "v2_financial_methodology_closure",
        "status": "financial_methodology_closed_for_signal_design",
        "entities": [
            {
                "entity_id": "fip:1",
                "fip_code": "1",
                "legal_name": "Alpha Seguros",
                "reference_period": 202605,
                "core_financial_signal": "core_indicators_without_current_shortfall",
                "capital": {
                    "state": "capital_meets_or_exceeds_cmr",
                    "pla_cmr_ratio": 2.0,
                },
                "liquidity": {
                    "state": "ilt_at_or_above_arithmetic_parity",
                    "value": 1.5,
                },
                "operating_context": {"signal": "balanced_persistent"},
                "evidence_confidence": "established_core_history",
                "public_interpretation": {"headline": "OK", "detail": "OK"},
            },
            {
                "entity_id": "fip:2",
                "fip_code": "2",
                "legal_name": "Beta Seguros",
                "reference_period": 202605,
                "core_financial_signal": "core_indicators_without_current_shortfall",
                "capital": {
                    "state": "capital_meets_or_exceeds_cmr",
                    "pla_cmr_ratio": 1.5,
                },
                "liquidity": {
                    "state": "ilt_at_or_above_arithmetic_parity",
                    "value": 1.2,
                },
                "operating_context": {"signal": "balanced_persistent"},
                "evidence_confidence": "established_core_history",
                "public_interpretation": {"headline": "OK", "detail": "OK"},
            },
            {
                "entity_id": "fip:3",
                "fip_code": "3",
                "legal_name": "Gamma Seguros",
                "reference_period": 202605,
                "core_financial_signal": "core_financial_signal_unavailable",
                "capital": {
                    "state": "capital_signal_unavailable",
                    "pla_cmr_ratio": None,
                },
                "liquidity": {"state": "ilt_signal_unavailable", "value": None},
                "operating_context": {"signal": "indeterminate"},
                "evidence_confidence": "insufficient_core_evidence",
                "public_interpretation": {
                    "headline": "Sem dado",
                    "detail": "Sem dado",
                },
            },
        ],
    }

    candidate_common = {
        "comparability_state": "direct_one_to_one_candidate",
        "direct_pressure": {
            "temporal_coverage": {"comparable_months": 12},
            "persistence": {"state": "not_distinguishable_from_expected"},
            "trend": {"state": "no_clear_change"},
        },
    }
    conduct = {
        "artifact": "v2_conduct_methodology_closure",
        "status": "conduct_methodology_closed_for_signal_design",
        "candidate_entities": [
            {
                **candidate_common,
                "entity_id": "fip:1",
                "fip_code": "1",
                "legal_name": "Alpha Seguros",
                "display_name": "Alpha",
                "pressure_conclusion": {
                    "state": "below_expected_with_sufficient_evidence",
                    "human_summary": "Abaixo do esperado.",
                },
                "direct_pressure": {
                    **candidate_common["direct_pressure"],
                    "annual": {
                        "observed_complaints": 5,
                        "expected_complaints": 10.0,
                        "ratio": 0.5,
                    },
                },
            },
            {
                **candidate_common,
                "entity_id": "fip:2",
                "fip_code": "2",
                "legal_name": "Beta Seguros",
                "display_name": "Beta",
                "pressure_conclusion": {
                    "state": "above_expected_with_sufficient_evidence",
                    "human_summary": "Acima do esperado.",
                },
                "direct_pressure": {
                    **candidate_common["direct_pressure"],
                    "annual": {
                        "observed_complaints": 20,
                        "expected_complaints": 10.0,
                        "ratio": 2.0,
                    },
                    "persistence": {"state": "persistent_above_expected"},
                    "trend": {"state": "improving_pressure"},
                },
            },
        ],
        "non_comparable_entities": [
            {
                "entity_id": "fip:3",
                "fip_code": "3",
                "legal_name": "Gamma Seguros",
                "display_name": "Gamma",
                "pressure_conclusion": {
                    "state": "pressure_unavailable_not_comparable",
                    "comparability_reason": "hybrid_insurance_and_pension",
                    "reason_code": "hybrid",
                    "human_summary": "Não comparável.",
                },
            }
        ],
    }

    reconciliation = {
        "artifact": "v2_conduct_coverage_reconciliation",
        "status": "experimental_audit",
        "entities": [
            {
                "entity_id": "fip:1",
                "legal_name": "Alpha Seguros",
                "display_name": "Alpha",
                "complaints_12m": 5,
                "insurance_exposure_12m": {
                    "insurance_premium_direct": 100.0,
                    "insurance_premium_earned_diagnostic": 90.0,
                },
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "reason_code": None,
                },
            },
            {
                "entity_id": "fip:2",
                "legal_name": "Beta Seguros",
                "display_name": "Beta",
                "complaints_12m": 20,
                "insurance_exposure_12m": {
                    "insurance_premium_direct": 100.0,
                    "insurance_premium_earned_diagnostic": 95.0,
                },
                "pressure_comparability": {
                    "state": "direct_one_to_one_candidate",
                    "reason_code": None,
                },
            },
            {
                "entity_id": "fip:3",
                "legal_name": "Gamma Seguros",
                "display_name": "Gamma",
                "complaints_12m": 30,
                "insurance_exposure_12m": {
                    "insurance_premium_direct": 0.0,
                    "insurance_premium_earned_diagnostic": 0.0,
                },
                "pressure_comparability": {
                    "state": "hybrid_insurance_and_pension",
                    "reason_code": "hybrid",
                },
            },
        ],
    }

    ranking_preflight = {
        "artifact": "v2_ranking_eligibility_preflight",
        "status": "ranking_eligibility_preflight_closed_gate_remains_blocked",
        "population": {"ranking_eligible": 0},
        "closure_decision": {
            "ranking_eligibility_gate_opened": False,
            "semantic_comparison_of_assessment_eligible_subset_supported": True,
        },
    }

    return (
        assessment,
        semantic,
        financial,
        conduct,
        reconciliation,
        ranking_preflight,
    )


def _build():
    return build_exploratory_leaderboards_contract(*_payloads())


def test_primary_product_is_semantic_comparator_and_general_ranking_stays_closed():
    payload = _build()
    assert payload["status"] == "exploratory_leaderboards_contract_closed"
    assert payload["population"] == {
        "regulatory_universe": 3,
        "assessment_eligible": 2,
        "assessment_not_eligible": 1,
        "ranking_eligible": 0,
    }
    assert payload["publication_policy"]["semantic_assessment_is_primary_product"] is True
    assert payload["publication_policy"]["general_composite_ranking_supported"] is False
    assert payload["closure_decision"]["general_ranking_gate_opened"] is False


def test_numeric_leaderboards_order_only_declared_metric_and_preserve_ties():
    payload = _build()
    boards = {board["id"]: board for board in payload["leaderboards"]}
    assert set(boards) == {
        "largest_by_direct_premium",
        "highest_pla_cmr_ratio",
        "highest_ilt",
        "lowest_conduct_pressure_ratio",
        "highest_conduct_pressure_ratio",
    }
    premium = boards["largest_by_direct_premium"]
    assert [entry["leaderboard_rank"] for entry in premium["entries"]] == [1, 1]
    assert premium["tie_policy"].startswith("competition_rank")
    assert all(board["is_general_ranking"] is False for board in boards.values())


def test_conduct_leaderboards_require_directionally_conclusive_evidence():
    payload = _build()
    boards = {board["id"]: board for board in payload["leaderboards"]}
    low = boards["lowest_conduct_pressure_ratio"]["entries"]
    high = boards["highest_conduct_pressure_ratio"]["entries"]
    assert [row["entity_id"] for row in low] == ["fip:1"]
    assert [row["entity_id"] for row in high] == ["fip:2"]
    assert low[0]["conduct_pressure_state"] == "below_expected_with_sufficient_evidence"
    assert high[0]["conduct_pressure_state"] == "above_expected_with_sufficient_evidence"


def test_semantic_collections_are_unordered_and_marketing_labels_stay_blocked():
    payload = _build()
    groups = {group["id"]: group for group in payload["collections"]}
    assert all(group["ordered"] is False for group in groups.values())
    assert groups["favorable_joint_assessment"]["entity_count"] == 1
    assert groups["conduct_improving_but_still_adverse"]["entity_count"] == 1
    concepts = payload["concept_registry"]
    assert concepts["financeiro_mais_em_dia"]["classification"] == (
        "public_semantic_collection"
    )
    assert concepts["mais_popular"]["classification"] == "not_supported"
    assert concepts["emergente_promissora"]["classification"] == "not_supported"
    assert concepts["consagrada_exemplar"]["classification"] == "not_supported"
    assert concepts["ranking_geral"]["classification"] == "not_supported"


def test_incomplete_evidence_remains_visible_without_bottom_rank():
    payload = _build()
    gamma = next(
        row for row in payload["explorer_entities"] if row["entity_id"] == "fip:3"
    )
    assert gamma["assessment"]["eligible"] is False
    assert gamma["conduct"]["pressure_ratio"] is None
    assert gamma["market_context"]["complaints_12m"] == 30
    assert gamma["explore_memberships"]["leaderboards"] == []
    assert payload["publication_policy"]["missing_data_may_receive_bottom_position"] is False


def test_public_writer_emits_explorer_index_boards_and_collections(tmp_path: Path):
    payload = _build()
    written = write_public_outputs(payload, tmp_path)
    assert len(written) == 12
    explorer = json.loads(
        (tmp_path / "insurer_explorer.json").read_text(encoding="utf-8")
    )
    index = json.loads((tmp_path / "explore_index.json").read_text(encoding="utf-8"))
    assert len(explorer["entities"]) == 3
    assert len(index["leaderboards"]) == 5
    assert len(index["collections"]) == 5
    assert (tmp_path / "leaderboards" / "highest_pla_cmr_ratio.json").exists()
    assert (tmp_path / "collections" / "favorable_joint_assessment.json").exists()


def test_preflight_cannot_silently_open_general_ranking():
    payloads = list(_payloads())
    ranking = copy.deepcopy(payloads[-1])
    ranking["closure_decision"]["ranking_eligibility_gate_opened"] = True
    payloads[-1] = ranking
    with pytest.raises(ValueError, match="general ranking gate must remain closed"):
        build_exploratory_leaderboards_contract(*payloads)
