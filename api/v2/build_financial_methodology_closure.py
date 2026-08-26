from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FINANCIAL_PATH = Path("data/derived/v2/entity_financial_evidence_inventory.json")
LIQUIDITY_PATH = Path("data/derived/v2/liquidity_experiment.json")
OPERATING_PATH = Path("data/derived/v2/operating_experiment.json")
OUTPUT_PATH = Path("data/derived/v2/financial_methodology_closure.json")

VERSION = "2.0-draft-financial-methodology-closure-1"
CAPITAL_REQUIREMENT_REFERENCE = 1.0
LIQUIDITY_PARITY_REFERENCE = 1.0


class FinancialMethodologyClosureError(RuntimeError):
    """Raised when the financial closure cannot be built safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _entity_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    entities = payload.get("entities") or []
    if isinstance(entities, dict):
        return {str(key): value for key, value in entities.items()}
    return {
        str(row.get("entity_id") or ""): row
        for row in entities
        if str(row.get("entity_id") or "")
    }


def _eligible_financial_entities(
    financial: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        str(row["entity_id"]): row
        for row in (financial.get("entities") or [])
        if (row.get("eligibility") or {}).get("regulatory_universe_eligible")
    }


def _capital_state(entity: dict[str, Any]) -> tuple[str, float | None]:
    capital = ((entity.get("financial_evidence") or {}).get("capital") or {})
    ratio = _finite(capital.get("pla_cmr_ratio"))
    if capital.get("pla_cmr_ratio_state") != "derivable" or ratio is None:
        return "capital_signal_unavailable", None
    if ratio < CAPITAL_REQUIREMENT_REFERENCE:
        return "capital_below_cmr", ratio
    return "capital_meets_or_exceeds_cmr", ratio


def _liquidity_state(entity: dict[str, Any]) -> tuple[str, float | None]:
    current = (((entity.get("metrics") or {}).get("ILT") or {}).get("current"))
    if not isinstance(current, dict) or current.get("state") != "derivable":
        return "ilt_signal_unavailable", None
    value = _finite(current.get("value"))
    if value is None:
        return "ilt_signal_unavailable", None
    if value < LIQUIDITY_PARITY_REFERENCE:
        return "ilt_below_arithmetic_parity", value
    return "ilt_at_or_above_arithmetic_parity", value


def _evidence_confidence(entity: dict[str, Any]) -> str:
    state = str((entity.get("financial_evidence") or {}).get("state") or "")
    if state == "complete_core_history":
        return "established_core_history"
    if state == "limited_core_history":
        return "limited_core_history"
    return "insufficient_core_evidence"


def _core_signal(capital_state: str, liquidity_state: str) -> str:
    if capital_state == "capital_signal_unavailable":
        return "core_financial_signal_unavailable"
    if capital_state == "capital_below_cmr":
        return "capital_requirement_shortfall_observed"
    if liquidity_state == "ilt_signal_unavailable":
        return "capital_requirement_met_liquidity_unavailable"
    if liquidity_state == "ilt_below_arithmetic_parity":
        return "capital_requirement_met_with_liquidity_pressure"
    return "core_indicators_without_current_shortfall"


def _public_language(core_signal: str) -> dict[str, str]:
    language = {
        "capital_requirement_shortfall_observed": {
            "headline": "Há insuficiência de capital observada na competência financeira de referência.",
            "detail": (
                "O Patrimônio Líquido Ajustado ficou abaixo do Capital Mínimo Requerido. "
                "Isso é um sinal prudencial material, mas não equivale sozinho a insolvência ou incapacidade de pagar sinistros."
            ),
        },
        "capital_requirement_met_with_liquidity_pressure": {
            "headline": "O requisito de capital foi atendido, mas a liquidez total merece cautela.",
            "detail": (
                "O PLA/CMR não mostra insuficiência de capital na competência de referência, "
                "enquanto o ILT ficou abaixo da paridade aritmética de 1,0. Esse ponto de liquidez "
                "é uma referência econômica do indicador, não um limite prudencial oficial da SUSEP."
            ),
        },
        "core_indicators_without_current_shortfall": {
            "headline": "Os indicadores financeiros centrais não mostram insuficiência atual na competência de referência.",
            "detail": (
                "O PLA/CMR atende ao requisito observado e o ILT não está abaixo da paridade aritmética. "
                "Isso não significa garantia de solvência, selo de saúde financeira ou superioridade sobre outras seguradoras."
            ),
        },
        "capital_requirement_met_liquidity_unavailable": {
            "headline": "O requisito de capital foi atendido, mas a evidência de liquidez está indisponível.",
            "detail": (
                "Há evidência atual de capital, porém não há ILT utilizável na mesma competência. "
                "A ferramenta não completa o sinal financeiro por imputação."
            ),
        },
        "core_financial_signal_unavailable": {
            "headline": "Não há evidência suficiente para concluir o sinal financeiro central.",
            "detail": (
                "O PLA/CMR não está utilizável na competência de referência. Ausência de dado não é tratada como desempenho ruim nem como desempenho neutro."
            ),
        },
    }
    return language[core_signal]


def build_financial_methodology_closure(
    financial: dict[str, Any],
    liquidity: dict[str, Any],
    operating: dict[str, Any],
) -> dict[str, Any]:
    financial_entities = _eligible_financial_entities(financial)
    liquidity_entities = _entity_map(liquidity)
    operating_entities = _entity_map(operating)

    if len(financial_entities) != 157:
        raise FinancialMethodologyClosureError(
            f"expected 157 regulatory-universe insurers, got {len(financial_entities)}"
        )
    if set(financial_entities) != set(liquidity_entities):
        raise FinancialMethodologyClosureError(
            "financial and liquidity populations are not identical"
        )
    if set(financial_entities) != set(operating_entities):
        raise FinancialMethodologyClosureError(
            "financial and operating populations are not identical"
        )

    financial_period = int(
        ((financial.get("meta") or {}).get("financial_period_maturity") or {}).get(
            "selected_period"
        )
        or 0
    )
    liquidity_period = int((liquidity.get("summary") or {}).get("reference_period") or 0)
    operating_period = int((operating.get("summary") or {}).get("reference_period") or 0)
    if not financial_period or len({financial_period, liquidity_period, operating_period}) != 1:
        raise FinancialMethodologyClosureError(
            "financial, liquidity and operating reference periods are not aligned"
        )

    core_counts: Counter[str] = Counter()
    capital_counts: Counter[str] = Counter()
    liquidity_counts: Counter[str] = Counter()
    confidence_counts: Counter[str] = Counter()
    operating_counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []

    for entity_id in sorted(financial_entities):
        financial_entity = financial_entities[entity_id]
        liquidity_entity = liquidity_entities[entity_id]
        operating_entity = operating_entities[entity_id]

        capital_state, capital_ratio = _capital_state(financial_entity)
        liquidity_state, ilt = _liquidity_state(liquidity_entity)
        confidence = _evidence_confidence(financial_entity)
        core_signal = _core_signal(capital_state, liquidity_state)
        operating_state = operating_entity.get("operating_state") or {}
        operating_signal = str(operating_state.get("operating_signal") or "indeterminate")

        capital_counts[capital_state] += 1
        liquidity_counts[liquidity_state] += 1
        confidence_counts[confidence] += 1
        core_counts[core_signal] += 1
        operating_counts[operating_signal] += 1

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": financial_entity.get("fip_code"),
                "legal_name": financial_entity.get("legal_name"),
                "reference_period": financial_period,
                "core_financial_signal": core_signal,
                "capital": {
                    "state": capital_state,
                    "pla_cmr_ratio": capital_ratio,
                    "reference": CAPITAL_REQUIREMENT_REFERENCE,
                    "reference_meaning": "regulatory_capital_requirement_boundary",
                    "magnitude_rewarded": False,
                },
                "liquidity": {
                    "state": liquidity_state,
                    "metric": "ILT",
                    "value": ilt,
                    "parity_reference": LIQUIDITY_PARITY_REFERENCE,
                    "parity_is_regulatory_threshold": False,
                    "magnitude_rewarded": False,
                    "ilc_role": "supporting_diagnostic_only",
                },
                "operating_context": {
                    "signal": operating_signal,
                    "history_state": operating_state.get("history_state"),
                    "formula_state": operating_state.get("formula_state"),
                    "reference_metric": "ICA",
                    "supporting_metric": "IC",
                    "overrides_core_signal": False,
                },
                "evidence_confidence": confidence,
                "public_interpretation": _public_language(core_signal),
            }
        )

    return {
        "artifact": "v2_financial_methodology_closure",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "financial_methodology_closed_for_signal_design",
        "assessment_role": "financial_signal_contract_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": "A seguradora apresenta sinais financeiros compatíveis com sustentar seus compromissos?",
            "questions": [
                "O patrimônio ajustado cobre o capital mínimo requerido?",
                "A liquidez total mostra pressão na competência observada?",
                "A operação está equilibrada, melhorando ou sob pressão ao longo do tempo?",
                "Temos histórico suficiente para confiar na leitura longitudinal?",
            ],
            "principle": "o sinal deve descrever evidência observada sem transformar excesso de magnitude em mérito ilimitado",
        },
        "source_contract": {
            "reference_period": financial_period,
            "period_policy": "latest_common_mature_financial_period",
            "financial_evidence_artifact": str(FINANCIAL_PATH),
            "liquidity_artifact": str(LIQUIDITY_PATH),
            "operating_artifact": str(OPERATING_PATH),
        },
        "methodology_decisions": {
            "capital": {
                "primary_metric": "PLA_CMR",
                "reference": CAPITAL_REQUIREMENT_REFERENCE,
                "reference_role": "regulatory_requirement_boundary",
                "continuous_transform_selected": False,
                "positive_tiers_above_reference_selected": False,
                "reason": "PLA below CMR is materially interpretable; additional magnitude above the requirement is not converted into progressively larger merit without cross-pillar calibration.",
            },
            "liquidity": {
                "primary_metric": "ILT",
                "supporting_metric": "ILC",
                "parity_reference": LIQUIDITY_PARITY_REFERENCE,
                "parity_is_regulatory_threshold": False,
                "continuous_transform_selected": False,
                "positive_tiers_above_parity_selected": False,
                "reason": "ILT is the more stable primary liquidity diagnostic; 1.0 is arithmetic parity, not a SUSEP approval threshold, and extreme ratios must not receive linear rewards.",
            },
            "operating": {
                "reference_metric": "ICA",
                "supporting_metric": "IC",
                "role": "longitudinal_context_only",
                "overrides_capital_or_liquidity": False,
                "signals": [
                    "balanced_persistent",
                    "improved",
                    "recent_pressure",
                    "persistent_pressure",
                    "indeterminate",
                ],
            },
            "profitability": {
                "metric": "ILPL",
                "independent_component_selected": False,
                "role": "diagnostic_only",
                "reason": "prior closed experiment did not justify ILPL as an independent scoring axis",
            },
            "combination": {
                "weighted_average_selected": False,
                "numeric_internal_score_selected": False,
                "gate_logic_selected": True,
                "capital_shortfall_can_be_offset_by_liquidity": False,
                "liquidity_pressure_can_be_offset_by_excess_capital": False,
                "operating_context_can_override_core_signal": False,
            },
            "confidence": {
                "short_history_penalizes_performance": False,
                "complete_core_history": "established_core_history",
                "limited_core_history": "limited_core_history",
                "otherwise": "insufficient_core_evidence",
            },
        },
        "guardrails": [
            "no_missing_data_as_zero",
            "no_absolute_size_reward",
            "no_linear_reward_for_excess_capital",
            "no_linear_reward_for_extreme_liquidity",
            "no_percentile_as_prudential_threshold",
            "no_operating_context_override",
            "no_short_history_performance_penalty",
            "no_financial_score_before_cross_pillar_calibration",
        ],
        "population": {
            "regulatory_universe": len(rows),
            "reference_period": financial_period,
        },
        "diagnostics": {
            "core_financial_signal_counts": dict(sorted(core_counts.items())),
            "capital_state_counts": dict(sorted(capital_counts.items())),
            "liquidity_state_counts": dict(sorted(liquidity_counts.items())),
            "evidence_confidence_counts": dict(sorted(confidence_counts.items())),
            "operating_context_counts": dict(sorted(operating_counts.items())),
        },
        "closure": {
            "financial_architecture_complete": True,
            "financial_evidence_contract_complete": True,
            "financial_signal_contract_complete": True,
            "financial_numeric_score_defined": False,
            "internal_financial_weights_defined": False,
            "next_stage": "cross_pillar_score_calibration_with_conduct",
            "important": (
                "Fechar o sinal financeiro significa saber o que os indicadores permitem afirmar; "
                "nao significa transformar cada razao em pontos nem permitir que um eixo compense silenciosamente outro."
            ),
        },
        "entities": rows,
    }


def main() -> None:
    financial = json.loads(FINANCIAL_PATH.read_text(encoding="utf-8"))
    liquidity = json.loads(LIQUIDITY_PATH.read_text(encoding="utf-8"))
    operating = json.loads(OPERATING_PATH.read_text(encoding="utf-8"))
    payload = build_financial_methodology_closure(financial, liquidity, operating)
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
                "closure": payload["closure"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
