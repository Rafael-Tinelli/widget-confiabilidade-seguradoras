from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FINANCIAL_PATH = Path("data/derived/v2/financial_methodology_closure.json")
CONDUCT_PATH = Path("data/derived/v2/conduct_methodology_closure.json")
OUTPUT_PATH = Path("data/derived/v2/cross_pillar_calibration_diagnostic.json")

VERSION = "2.0-draft-cross-pillar-calibration-stage-1"

FINANCIAL_LEVEL = {
    "core_indicators_without_current_shortfall": 0,
    "capital_requirement_met_with_liquidity_pressure": 1,
    "capital_requirement_shortfall_observed": 2,
}

CONDUCT_LEVEL = {
    "below_expected_with_sufficient_evidence": 0,
    "not_distinguishable_from_expected": 0,
    "above_expected_with_sufficient_evidence": 1,
}


class CrossPillarCalibrationError(RuntimeError):
    """Raised when the two closed pillar contracts cannot be joined safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _financial_entities(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("entities") or []
    return {
        str(row.get("entity_id") or ""): row
        for row in rows
        if str(row.get("entity_id") or "")
    }


def _conduct_entities(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for source, rows in (
        ("candidate", payload.get("candidate_entities") or []),
        ("non_comparable", payload.get("non_comparable_entities") or []),
    ):
        for row in rows:
            entity_id = str(row.get("entity_id") or "")
            if not entity_id:
                continue
            if entity_id in result:
                raise CrossPillarCalibrationError(
                    f"duplicate Conduct entity in closure: {entity_id}"
                )
            result[entity_id] = {**row, "_closure_population": source}
    return result


def _financial_level(state: str) -> int | None:
    return FINANCIAL_LEVEL.get(state)


def _conduct_level(state: str) -> int | None:
    # The closed Conduct contract explicitly says that below expected is not
    # proof of better service. Therefore below-expected and indistinguishable
    # are tied at the same non-adverse coordinate in this stage-1 diagnostic.
    return CONDUCT_LEVEL.get(state)


def _conduct_class(state: str) -> str:
    if state == "above_expected_with_sufficient_evidence":
        return "adverse_pressure_observed"
    if state == "below_expected_with_sufficient_evidence":
        return "lower_pressure_observed_not_quality_proof"
    if state == "not_distinguishable_from_expected":
        return "no_clear_pressure_difference"
    if state == "pressure_inconclusive_denominator_sensitivity":
        return "inconclusive_denominator_sensitivity"
    if state == "pressure_unavailable_insufficient_temporal_coverage":
        return "inconclusive_temporal_coverage"
    if state == "pressure_unavailable_not_comparable":
        return "unavailable_not_comparable"
    return "inconclusive_or_unavailable"


def _joint_readiness(financial_state: str, conduct_state: str) -> str:
    if _financial_level(financial_state) is None:
        return "financial_core_incomplete"
    if _conduct_level(conduct_state) is not None:
        return "joint_core_conclusive"
    if conduct_state == "pressure_unavailable_not_comparable":
        return "conduct_not_comparable"
    if conduct_state == "pressure_unavailable_insufficient_temporal_coverage":
        return "conduct_insufficient_temporal_coverage"
    if conduct_state == "pressure_inconclusive_denominator_sensitivity":
        return "conduct_denominator_sensitive"
    return "conduct_inconclusive_or_unavailable"


def _dominates(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return all(a <= b for a, b in zip(left, right, strict=True)) and any(
        a < b for a, b in zip(left, right, strict=True)
    )


def _pareto_fronts(coordinates: dict[str, tuple[int, int]]) -> dict[str, int]:
    remaining = set(coordinates)
    fronts: dict[str, int] = {}
    front_number = 1
    while remaining:
        current_front = []
        for entity_id in sorted(remaining):
            point = coordinates[entity_id]
            dominated = any(
                _dominates(coordinates[other], point)
                for other in remaining
                if other != entity_id
            )
            if not dominated:
                current_front.append(entity_id)
        if not current_front:
            raise CrossPillarCalibrationError("Pareto front construction stalled")
        for entity_id in current_front:
            fronts[entity_id] = front_number
        remaining.difference_update(current_front)
        front_number += 1
    return fronts


def _pairwise_orderability(
    coordinates: dict[str, tuple[int, int]],
) -> dict[str, Any]:
    ids = sorted(coordinates)
    tied = 0
    comparable = 0
    incomparable = 0
    for index, left_id in enumerate(ids):
        left = coordinates[left_id]
        for right_id in ids[index + 1 :]:
            right = coordinates[right_id]
            if left == right:
                tied += 1
            elif _dominates(left, right) or _dominates(right, left):
                comparable += 1
            else:
                incomparable += 1
    total = tied + comparable + incomparable
    return {
        "entity_count": len(ids),
        "pair_count": total,
        "strictly_comparable_pairs": comparable,
        "tied_pairs": tied,
        "incomparable_pairs": incomparable,
        "strictly_comparable_share": comparable / total if total else None,
        "tied_share": tied / total if total else None,
        "incomparable_share": incomparable / total if total else None,
    }


def _group_size_summary(values: list[str]) -> dict[str, Any]:
    counts = Counter(values)
    sizes = sorted(counts.values(), reverse=True)
    return {
        "unique_groups": len(counts),
        "largest_group": sizes[0] if sizes else 0,
        "groups_with_more_than_one_entity": sum(size > 1 for size in sizes),
        "entities_in_tied_groups": sum(size for size in sizes if size > 1),
        "counts": dict(sorted(counts.items())),
    }


def build_cross_pillar_diagnostic(
    financial: dict[str, Any],
    conduct: dict[str, Any],
) -> dict[str, Any]:
    if financial.get("status") != "financial_methodology_closed_for_signal_design":
        raise CrossPillarCalibrationError("Financial signal contract is not closed")
    if conduct.get("status") != "conduct_methodology_closed_for_signal_design":
        raise CrossPillarCalibrationError("Conduct signal contract is not closed")
    if financial.get("scoring") != "forbidden_in_this_artifact":
        raise CrossPillarCalibrationError("Financial closure must forbid scoring")
    if conduct.get("scoring") != "forbidden_in_this_artifact":
        raise CrossPillarCalibrationError("Conduct closure must forbid scoring")

    financial_by_id = _financial_entities(financial)
    conduct_by_id = _conduct_entities(conduct)
    if len(financial_by_id) < 100 or len(conduct_by_id) < 100:
        raise CrossPillarCalibrationError(
            "cross-pillar calibration received an unexpectedly small insurer universe"
        )
    if set(financial_by_id) != set(conduct_by_id):
        missing_financial = sorted(set(conduct_by_id) - set(financial_by_id))
        missing_conduct = sorted(set(financial_by_id) - set(conduct_by_id))
        raise CrossPillarCalibrationError(
            "pillar populations differ: "
            f"missing_financial={missing_financial[:5]} "
            f"missing_conduct={missing_conduct[:5]}"
        )
    universe_size = len(financial_by_id)

    rows: list[dict[str, Any]] = []
    matrix: dict[str, Counter[str]] = defaultdict(Counter)
    readiness_counts: Counter[str] = Counter()
    adverse_pattern_counts: Counter[str] = Counter()
    core_signatures: list[str] = []
    coordinates: dict[str, tuple[int, int]] = {}

    for entity_id in sorted(financial_by_id):
        financial_row = financial_by_id[entity_id]
        conduct_row = conduct_by_id[entity_id]

        financial_state = str(
            financial_row.get("core_financial_signal") or "financial_state_missing"
        )
        pressure = conduct_row.get("pressure_conclusion") or {}
        conduct_state = str(pressure.get("state") or "conduct_state_missing")
        readiness = _joint_readiness(financial_state, conduct_state)
        readiness_counts[readiness] += 1
        matrix[financial_state][conduct_state] += 1

        persistence = (
            ((conduct_row.get("direct_pressure") or {}).get("persistence") or {}).get(
                "state"
            )
            if conduct_row.get("_closure_population") == "candidate"
            else None
        )
        trend = (
            ((conduct_row.get("direct_pressure") or {}).get("trend") or {}).get("state")
            if conduct_row.get("_closure_population") == "candidate"
            else None
        )

        capital_shortfall = financial_state == "capital_requirement_shortfall_observed"
        liquidity_pressure = (
            financial_state == "capital_requirement_met_with_liquidity_pressure"
        )
        conduct_above = conduct_state == "above_expected_with_sufficient_evidence"
        conduct_persistent_above = persistence == "persistent_above_expected"
        conduct_deteriorating = trend == "deteriorating_pressure"
        operating_signal = str(
            ((financial_row.get("operating_context") or {}).get("signal"))
            or "indeterminate"
        )
        operating_pressure = operating_signal in {
            "recent_pressure",
            "persistent_pressure",
        }

        if readiness == "joint_core_conclusive":
            coordinate = (
                int(_financial_level(financial_state)),
                int(_conduct_level(conduct_state)),
            )
            coordinates[entity_id] = coordinate
            core_signature = f"F{coordinate[0]}|C{coordinate[1]}"
            core_signatures.append(core_signature)

            financial_adverse = coordinate[0] > 0
            conduct_adverse = coordinate[1] > 0
            if financial_adverse and conduct_adverse:
                adverse_pattern = "both_core_pillars_adverse"
            elif financial_adverse:
                adverse_pattern = "financial_only_core_adverse"
            elif conduct_adverse:
                adverse_pattern = "conduct_only_core_adverse"
            else:
                adverse_pattern = "no_core_adverse_signal"
            adverse_pattern_counts[adverse_pattern] += 1
        else:
            coordinate = None
            core_signature = None
            adverse_pattern = "joint_core_incomplete"
            adverse_pattern_counts[adverse_pattern] += 1

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": financial_row.get("fip_code") or conduct_row.get("fip_code"),
                "legal_name": financial_row.get("legal_name") or conduct_row.get("legal_name"),
                "joint_evidence_readiness": readiness,
                "financial": {
                    "core_state": financial_state,
                    "ordinal_adverse_level_for_stage1_only": _financial_level(
                        financial_state
                    ),
                    "evidence_confidence": financial_row.get("evidence_confidence"),
                    "operating_context": operating_signal,
                },
                "conduct": {
                    "pressure_state": conduct_state,
                    "stage1_class": _conduct_class(conduct_state),
                    "ordinal_adverse_level_for_stage1_only": _conduct_level(conduct_state),
                    "persistence": persistence,
                    "trend": trend,
                    "below_expected_is_positive_merit": False,
                },
                "adverse_signature": {
                    "capital_shortfall": capital_shortfall,
                    "liquidity_pressure": liquidity_pressure,
                    "conduct_above_expected": conduct_above,
                    "conduct_persistent_above": conduct_persistent_above,
                    "conduct_deteriorating": conduct_deteriorating,
                    "operating_pressure_context_only": operating_pressure,
                },
                "core_coordinate": list(coordinate) if coordinate is not None else None,
                "core_signature": core_signature,
                "adverse_pattern": adverse_pattern,
            }
        )

    pareto = _pareto_fronts(coordinates)
    for row in rows:
        row["pareto_front_stage1"] = pareto.get(str(row["entity_id"]))

    front_counts = Counter(pareto.values())
    top_front_ids = sorted(
        entity_id for entity_id, front in pareto.items() if front == 1
    )
    top_front_conduct_states = Counter(
        str(
            (conduct_by_id[entity_id].get("pressure_conclusion") or {}).get("state")
            or "missing"
        )
        for entity_id in top_front_ids
    )

    matrix_serialized = {
        financial_state: dict(sorted(conduct_counts.items()))
        for financial_state, conduct_counts in sorted(matrix.items())
    }

    orderability = _pairwise_orderability(coordinates)
    signature_resolution = _group_size_summary(core_signatures)

    return {
        "artifact": "v2_cross_pillar_calibration_diagnostic",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "cross_pillar_calibration_stage_1_diagnostic",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "O que os dois pilares conseguem afirmar juntos sem permitir que um sinal forte esconda um problema material ou que ausencia de evidencia vire neutralidade?"
            ),
            "questions": [
                "Quantas seguradoras possuem conclusao central utilizavel nos dois pilares?",
                "Quais combinacoes de sinais adversos realmente aparecem?",
                "Os contratos fechados determinam uma ordem entre seguradoras sem pesos adicionais?",
                "Quanto da ordenacao seria empate ou incomparabilidade sem escolhas normativas novas?",
                "Quais casos ficam fora por evidencia insuficiente e nao por desempenho?",
            ],
            "stage_1_rule": "medir poder de ordenacao antes de escolher transformacoes ou pesos",
        },
        "closed_contract_preservation": {
            "financial_excess_magnitude_rewarded": False,
            "conduct_below_expected_rewarded_as_quality": False,
            "missing_pillar_treated_as_neutral": False,
            "confidence_used_as_performance": False,
            "operating_context_used_as_core_override": False,
        },
        "ordinal_diagnostic_contract": {
            "purpose": "orderability_diagnostic_only_not_score",
            "financial_levels": {
                "0": "core_indicators_without_current_shortfall",
                "1": "capital_requirement_met_with_liquidity_pressure",
                "2": "capital_requirement_shortfall_observed",
            },
            "conduct_levels": {
                "0": [
                    "below_expected_with_sufficient_evidence",
                    "not_distinguishable_from_expected",
                ],
                "1": "above_expected_with_sufficient_evidence",
            },
            "important": (
                "Conduct level 0 is merely absence of the adverse above-expected state. "
                "It does not say that below expected is better service than statistically indistinguishable pressure."
            ),
        },
        "population": {
            "regulatory_universe": universe_size,
            "joint_core_conclusive": len(coordinates),
            "joint_core_not_conclusive": universe_size - len(coordinates),
        },
        "diagnostics": {
            "joint_evidence_readiness_counts": dict(sorted(readiness_counts.items())),
            "financial_by_conduct_matrix": matrix_serialized,
            "adverse_pattern_counts": dict(sorted(adverse_pattern_counts.items())),
            "safe_core_signature_resolution": signature_resolution,
            "pareto_partial_order": {
                "front_counts": {
                    str(front): count for front, count in sorted(front_counts.items())
                },
                "top_front_size": len(top_front_ids),
                "top_front_share_of_joint_conclusive": (
                    len(top_front_ids) / len(coordinates) if coordinates else None
                ),
                "top_front_conduct_state_counts": dict(
                    sorted(top_front_conduct_states.items())
                ),
                "pairwise_orderability": orderability,
            },
        },
        "architecture_candidates": {
            "weighted_continuous_score": {
                "stage_1_status": "not_supported_by_closed_contracts_alone",
                "reason": (
                    "The closed pillars deliberately do not define cardinal merit for excess capital, extreme liquidity, or below-expected complaints. A weighted continuous score would require new normative mappings."
                ),
            },
            "noncompensatory_state_matrix": {
                "stage_1_status": "compatible_with_closed_contracts",
                "reason": "preserves material adverse states and missingness without hidden offsets",
            },
            "pareto_partial_order": {
                "stage_1_status": "compatible_with_closed_contracts",
                "reason": "tests how much order emerges from adverse-state dominance without weights",
            },
            "lexicographic_total_order": {
                "stage_1_status": "requires_new_normative_priority",
                "reason": "a total order requires declaring one pillar categorically prior to another in conflicting cases",
            },
            "hybrid_gate_then_score": {
                "stage_1_status": "possible_future_test_not_yet_selected",
                "reason": "could preserve noncompensatory gates, but any within-gate score still needs a defensible positive-signal calibration",
            },
        },
        "next_stage": {
            "decision_after_real_distribution": (
                "Use the joint matrix, Pareto fronts, ties and incomparability to decide whether stage 2 should test a state/tier architecture or a gate-then-score architecture."
            ),
            "score_or_weights_selected": False,
        },
        "entities": rows,
    }


def main() -> None:
    financial = json.loads(FINANCIAL_PATH.read_text(encoding="utf-8"))
    conduct = json.loads(CONDUCT_PATH.read_text(encoding="utf-8"))
    payload = build_cross_pillar_diagnostic(financial, conduct)
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
                "population": payload["population"],
                "diagnostics": payload["diagnostics"],
                "architecture_candidates": payload["architecture_candidates"],
                "next_stage": payload["next_stage"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
