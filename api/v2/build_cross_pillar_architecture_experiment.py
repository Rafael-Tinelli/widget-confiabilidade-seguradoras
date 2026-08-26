from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE1_PATH = Path("data/derived/v2/cross_pillar_calibration_diagnostic.json")
COVERAGE_PATH = Path("data/derived/v2/cross_pillar_coverage_audit.json")
OUTPUT_PATH = Path("data/derived/v2/cross_pillar_architecture_experiment.json")

VERSION = "2.0-draft-cross-pillar-architecture-stage-2-1"

STATE_BY_SIGNATURE = {
    "F0|C0": "no_current_core_adverse_signal",
    "F0|C1": "conduct_pressure_only",
    "F1|C0": "liquidity_pressure_only",
    "F1|C1": "liquidity_and_conduct_pressure",
    "F2|C0": "capital_shortfall_without_conduct_pressure",
    "F2|C1": "capital_shortfall_and_conduct_pressure",
}

PUBLIC_LANGUAGE = {
    "no_current_core_adverse_signal": (
        "Nos indicadores centrais avaliados, não observamos insuficiência de capital, "
        "pressão de liquidez pelo ILT nem pressão de reclamações acima do esperado. "
        "Isso não é garantia de solvência, qualidade ou superioridade."
    ),
    "conduct_pressure_only": (
        "Os indicadores financeiros centrais não mostram insuficiência atual, mas a "
        "pressão de reclamações está acima do esperado para o tamanho da operação."
    ),
    "liquidity_pressure_only": (
        "O requisito de capital está atendido, mas o ILT está abaixo da paridade "
        "aritmética. A Conduta não mostra pressão de reclamações acima do esperado."
    ),
    "liquidity_and_conduct_pressure": (
        "O requisito de capital está atendido, mas há pressão de liquidez pelo ILT e "
        "pressão de reclamações acima do esperado."
    ),
    "capital_shortfall_without_conduct_pressure": (
        "O patrimônio ajustado está abaixo do capital mínimo requerido na competência "
        "de referência. A Conduta não mostra pressão de reclamações acima do esperado."
    ),
    "capital_shortfall_and_conduct_pressure": (
        "O patrimônio ajustado está abaixo do capital mínimo requerido na competência "
        "de referência e a pressão de reclamações está acima do esperado."
    ),
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _coordinate(row: dict[str, Any]) -> tuple[int, int] | None:
    value = row.get("core_coordinate")
    if not isinstance(value, list) or len(value) != 2:
        return None
    if not all(isinstance(item, int) for item in value):
        return None
    return int(value[0]), int(value[1])


def _matrix_state(row: dict[str, Any]) -> str | None:
    signature = str(row.get("core_signature") or "")
    return STATE_BY_SIGNATURE.get(signature)


def _persistence_qualifier(row: dict[str, Any]) -> str | None:
    persistence = str((row.get("conduct") or {}).get("persistence") or "")
    if persistence == "persistent_above_expected":
        return "conduct_adverse_persistent"
    if persistence == "episodic_or_sparse_above_expected":
        return "conduct_adverse_episodic_or_sparse"
    return None


def _trend_qualifier(row: dict[str, Any]) -> str | None:
    trend = str((row.get("conduct") or {}).get("trend") or "")
    if trend == "deteriorating_pressure":
        return "conduct_pressure_deteriorating"
    if trend == "improving_pressure":
        return "conduct_pressure_improving_but_current_level_remains_adverse"
    if trend == "no_clear_change":
        return "conduct_pressure_no_clear_change"
    return None


def _operating_qualifier(row: dict[str, Any]) -> str | None:
    context = str((row.get("financial") or {}).get("operating_context") or "")
    if context == "persistent_pressure":
        return "operating_context_persistent_pressure"
    if context == "recent_pressure":
        return "operating_context_recent_pressure"
    if context == "improved":
        return "operating_context_improved"
    if context == "balanced_persistent":
        return "operating_context_balanced_persistent"
    return None


def _matrix_flags(row: dict[str, Any]) -> list[str]:
    signature = row.get("adverse_signature") or {}
    flags: list[str] = []
    if bool(signature.get("capital_shortfall")):
        flags.append("material_capital_shortfall")
    if bool(signature.get("liquidity_pressure")):
        flags.append("liquidity_pressure")
    if bool(signature.get("conduct_above_expected")):
        flags.append("conduct_pressure_above_expected")
    return flags


def _scenario_rank(coord: tuple[int, int], scenario: str) -> int:
    f_level, c_level = coord
    if scenario == "financial_lexicographic":
        return f_level * 2 + c_level
    if scenario == "conduct_lexicographic":
        return c_level * 3 + f_level
    if scenario == "capital_gate":
        mapping = {
            (0, 0): 0,
            (0, 1): 1,
            (1, 0): 1,
            (1, 1): 2,
            (2, 0): 3,
            (2, 1): 4,
        }
        return mapping[coord]
    if scenario == "pareto_layer_as_order":
        return f_level + c_level
    raise KeyError(scenario)


def _pair_relation(left: int, right: int) -> int:
    if left < right:
        return -1
    if left > right:
        return 1
    return 0


def _tradeoff_breakdown(rows: list[dict[str, Any]]) -> dict[str, Any]:
    coordinate_counts = Counter(
        tuple(_coordinate(row))
        for row in rows
        if _coordinate(row) is not None
    )
    tradeoff_coordinate_pairs = [
        ((0, 1), (1, 0)),
        ((0, 1), (2, 0)),
        ((1, 1), (2, 0)),
    ]
    details = []
    total = 0
    for left, right in tradeoff_coordinate_pairs:
        count = coordinate_counts[left] * coordinate_counts[right]
        total += count
        scenario_relations = {}
        for scenario in (
            "financial_lexicographic",
            "conduct_lexicographic",
            "capital_gate",
            "pareto_layer_as_order",
        ):
            relation = _pair_relation(
                _scenario_rank(left, scenario),
                _scenario_rank(right, scenario),
            )
            scenario_relations[scenario] = (
                "left_preferred" if relation < 0 else "right_preferred" if relation > 0 else "tied"
            )
        details.append(
            {
                "left_coordinate": list(left),
                "right_coordinate": list(right),
                "entity_pair_count": count,
                "relations_if_totalized": scenario_relations,
            }
        )
    return {
        "tradeoff_entity_pairs": total,
        "coordinate_pair_details": details,
        "important": (
            "These pairs are incomparable under direct two-axis dominance. Any total order "
            "must resolve them through an additional normative priority or tie rule."
        ),
    }


def _scenario_comparison(rows: list[dict[str, Any]]) -> dict[str, Any]:
    joint = [row for row in rows if _coordinate(row) is not None]
    scenarios = (
        "financial_lexicographic",
        "conduct_lexicographic",
        "capital_gate",
        "pareto_layer_as_order",
    )
    result: dict[str, Any] = {}
    for scenario in scenarios:
        bucket_counts = Counter(
            _scenario_rank(_coordinate(row), scenario)
            for row in joint
            if _coordinate(row) is not None
        )
        result[scenario] = {
            "bucket_counts": {
                str(bucket): count for bucket, count in sorted(bucket_counts.items())
            },
            "total_order_between_distinct_coordinates": scenario
            in {"financial_lexicographic", "conduct_lexicographic"},
        }

    disagreements = Counter()
    ids = list(range(len(joint)))
    for i, left_index in enumerate(ids):
        left = _coordinate(joint[left_index])
        if left is None:
            continue
        for right_index in ids[i + 1 :]:
            right = _coordinate(joint[right_index])
            if right is None or left == right:
                continue
            f_rel = _pair_relation(
                _scenario_rank(left, "financial_lexicographic"),
                _scenario_rank(right, "financial_lexicographic"),
            )
            c_rel = _pair_relation(
                _scenario_rank(left, "conduct_lexicographic"),
                _scenario_rank(right, "conduct_lexicographic"),
            )
            if f_rel != c_rel:
                disagreements["financial_vs_conduct_lexicographic"] += 1
    result["pairwise_scenario_disagreement"] = dict(disagreements)
    return result


def build_architecture_experiment(
    stage1: dict[str, Any],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    if stage1.get("status") != "cross_pillar_calibration_stage_1_diagnostic":
        raise ValueError("unexpected Stage 1 status")
    if coverage.get("status") != "cross_pillar_market_coverage_audit":
        raise ValueError("unexpected coverage audit status")
    if stage1.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("Stage 1 scoring must remain forbidden")
    if coverage.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("coverage scoring must remain forbidden")

    rows = stage1.get("entities") or []
    if len(rows) != 157:
        raise ValueError("Stage 2 requires 157 entities")

    matrix_counts = Counter()
    qualifier_counts = Counter()
    output_rows = []
    for row in rows:
        state = _matrix_state(row)
        persistence = _persistence_qualifier(row)
        trend = _trend_qualifier(row)
        operating = _operating_qualifier(row)
        if state is not None:
            matrix_counts[state] += 1
        else:
            matrix_counts["evidence_incomplete_for_joint_assessment"] += 1
        for qualifier in (persistence, trend, operating):
            if qualifier:
                qualifier_counts[qualifier] += 1
        output_rows.append(
            {
                "entity_id": row.get("entity_id"),
                "legal_name": row.get("legal_name"),
                "joint_evidence_readiness": row.get("joint_evidence_readiness"),
                "matrix_state": state,
                "material_flags": _matrix_flags(row) if state is not None else [],
                "qualifiers": {
                    "conduct_persistence": persistence,
                    "conduct_trend": trend,
                    "operating_context": operating,
                },
                "public_language_candidate": PUBLIC_LANGUAGE.get(state),
                "formal_assessment_gate_effect": "none_experimental",
                "formal_ranking_gate_effect": "none_experimental",
            }
        )

    conduct_adverse = [
        row
        for row in output_rows
        if "conduct_pressure_above_expected" in row["material_flags"]
    ]
    adverse_persistence = Counter(
        row["qualifiers"]["conduct_persistence"] or "missing"
        for row in conduct_adverse
    )
    adverse_trend = Counter(
        row["qualifiers"]["conduct_trend"] or "missing"
        for row in conduct_adverse
    )

    joint_coverage = (coverage.get("coverage") or {}).get("joint_core_conclusive") or {}
    incomplete_coverage = (coverage.get("coverage") or {}).get("joint_core_incomplete") or {}

    tradeoffs = _tradeoff_breakdown(rows)
    scenario_comparison = _scenario_comparison(rows)

    return {
        "artifact": "v2_cross_pillar_architecture_experiment",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "cross_pillar_architecture_stage_2_experiment",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "Qual arquitetura traduz melhor os sinais conjuntos sem inventar compensacao, "
                "premiar ausencia de problema ou esconder falta de evidencia?"
            ),
            "rule": (
                "se dois estados exigem uma prioridade normativa nova para serem ordenados, "
                "o experimento deve expor essa escolha em vez de disfarca-la como matematica"
            ),
        },
        "matrix_architecture": {
            "status": "leading_candidate_not_yet_formal_assessment_contract",
            "state_counts": dict(sorted(matrix_counts.items())),
            "properties": {
                "noncompensatory": True,
                "preserves_exact_adverse_domain": True,
                "missingness_separate_from_performance": True,
                "requires_cross_pillar_weights": False,
                "pretends_total_order": False,
                "capital_shortfall_is_material_flag": True,
                "capital_shortfall_is_automatically_worse_than_every_noncapital_combination": False,
            },
            "reason": (
                "The six-state matrix is fully implied by the closed pillar contracts and does "
                "not need a hidden exchange rate between financial and Conduct adversity."
            ),
        },
        "normative_tradeoffs": tradeoffs,
        "scenario_comparison": scenario_comparison,
        "adverse_qualifier_diagnostic": {
            "conduct_above_expected_entities": len(conduct_adverse),
            "persistence_counts": dict(sorted(adverse_persistence.items())),
            "trend_counts": dict(sorted(adverse_trend.items())),
            "persistence_can_qualify_adverse_state_without_positive_bonus": True,
            "trend_can_qualify_adverse_state_without_erasing_current_adverse_level": True,
            "important": (
                "An improving trend does not erase a current above-expected pressure conclusion; "
                "a persistent signal may be described as more entrenched without converting the "
                "opposite state into a bonus."
            ),
        },
        "coverage_constraint": {
            "joint_conclusive_entities": int(joint_coverage.get("entity_count") or 0),
            "joint_conclusive_positive_premium_share": joint_coverage.get(
                "positive_premium_share"
            ),
            "joint_conclusive_complaint_share": joint_coverage.get("complaint_share"),
            "joint_incomplete_entities": int(incomplete_coverage.get("entity_count") or 0),
            "full_market_ranking_supported": False,
        },
        "architecture_decision": {
            "continuous_weighted_score_selected": False,
            "lexicographic_total_order_selected": False,
            "pareto_front_number_selected_as_public_tier": False,
            "capital_gate_total_order_selected": False,
            "leading_public_assessment_candidate": (
                "noncompensatory_state_matrix_with_adverse_qualifiers"
            ),
            "formal_assessment_contract_closed": False,
            "reason_not_closed_yet": (
                "The matrix itself is evidence-faithful, but public assessment semantics, "
                "capital materiality wording and coverage thresholds still require a closure test."
            ),
        },
        "next_test": {
            "name": "cross_pillar_assessment_contract_preflight",
            "questions": [
                "Can the six matrix states be explained to consumers without good/bad overclaiming?",
                "Should capital shortfall be a mandatory material warning without imposing a total rank?",
                "Can persistence and trend qualify only adverse Conduct states consistently?",
                "What minimum evidence and market coverage are required for a formal complete assessment?",
                "Should ranking remain blocked even if complete assessment becomes available for a subset?",
            ],
        },
        "entities": output_rows,
    }


def main() -> None:
    stage1 = json.loads(STAGE1_PATH.read_text(encoding="utf-8"))
    coverage = json.loads(COVERAGE_PATH.read_text(encoding="utf-8"))
    payload = build_architecture_experiment(stage1, coverage)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "version": payload["version"],
                "status": payload["status"],
                "matrix_architecture": payload["matrix_architecture"],
                "normative_tradeoffs": payload["normative_tradeoffs"],
                "scenario_comparison": payload["scenario_comparison"],
                "adverse_qualifier_diagnostic": payload["adverse_qualifier_diagnostic"],
                "coverage_constraint": payload["coverage_constraint"],
                "architecture_decision": payload["architecture_decision"],
                "next_test": payload["next_test"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
