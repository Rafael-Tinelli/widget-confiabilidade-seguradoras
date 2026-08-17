from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

from api.sources.susep_financial_evidence import (
    LIQUIDITY_FORMULA_CMPIDS,
    OPERATING_FORMULA_CMPIDS,
)

FINANCIAL_EVIDENCE_VERSION = "2.0-draft-evidence-profile-1"
CORE_HISTORY_MONTHS = 12
DESCRIPTIVE_HISTORY_WINDOWS = (12, 24, 36)


class FinancialEvidenceInvariantError(ValueError):
    """Raised when financial evidence contradicts the regulatory universe."""


def month_window(reference_period: int | None, months: int) -> list[int]:
    if not reference_period or months <= 0:
        return []
    year = int(reference_period) // 100
    month = int(reference_period) % 100
    if month < 1 or month > 12:
        raise ValueError(f"invalid AAAAMM reference period: {reference_period}")
    output: list[int] = []
    for _ in range(months):
        output.append(year * 100 + month)
        month -= 1
        if month == 0:
            year -= 1
            month = 12
    output.reverse()
    return output


def _window_count(periods: set[int], window: list[int]) -> int:
    return sum(period in periods for period in window)


def _window_complete(periods: set[int], window: list[int]) -> bool:
    return bool(window) and all(period in periods for period in window)


def _formula_presence(
    balance_values: dict[int, dict[int, float]],
    formulas: dict[str, set[int]],
    reference_period: int | None,
) -> dict[str, Any]:
    latest_values = balance_values.get(reference_period or -1, {})
    latest_cmpids = set(latest_values)
    window = month_window(reference_period, CORE_HISTORY_MONTHS)
    output: dict[str, Any] = {}
    for formula, required in formulas.items():
        missing_latest = sorted(required - latest_cmpids)
        strict_complete_months = 0
        for period in window:
            present = set(balance_values.get(period, {}))
            if required.issubset(present):
                strict_complete_months += 1
        output[formula] = {
            "required_cmpids": sorted(required),
            "missing_cmpids_latest": missing_latest,
            "strict_components_complete_latest": not missing_latest,
            "strict_complete_months_last_12": strict_complete_months,
            "interpretation": (
                "Component presence only. Missing additive CMPIDs may represent zero-valued "
                "accounts rather than missing reporting, so strict presence is not yet a "
                "final formula-eligibility rule."
            ),
        }
    return output


def _capital_profile(
    evidence: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    history = evidence.get("capital_history") or {}
    periods = {int(period) for period in history}
    latest = history.get(reference_period) if reference_period else None
    windows = {
        months: month_window(reference_period, months)
        for months in DESCRIPTIVE_HISTORY_WINDOWS
    }
    latest_pla = (latest or {}).get("pla_adjusted")
    latest_cmr = (latest or {}).get("cmr")
    ratio = None
    ratio_state = "unavailable"
    if latest_pla is not None and latest_cmr is not None:
        if float(latest_cmr) > 0:
            ratio = float(latest_pla) / float(latest_cmr)
            ratio_state = "derivable"
        else:
            ratio_state = "non_positive_cmr_requires_investigation"

    latest_numeric = latest_pla is not None and latest_cmr is not None
    core_window = windows[CORE_HISTORY_MONTHS]
    if not latest or not latest_numeric:
        state = "insufficient"
    elif ratio_state == "non_positive_cmr_requires_investigation":
        state = "requires_investigation"
    elif _window_complete(periods, core_window):
        state = "complete_12m"
    else:
        state = "limited_history"

    return {
        "source_table": "Ses_pl_margem.csv",
        "reference_period": reference_period,
        "state": state,
        "first_period": min(periods) if periods else None,
        "last_period": max(periods) if periods else None,
        "observed_periods_total": len(periods),
        "history_windows": {
            str(months): {
                "expected_months": months,
                "observed_months": _window_count(periods, window),
                "complete": _window_complete(periods, window),
            }
            for months, window in windows.items()
        },
        "latest": deepcopy(latest),
        "pla_cmr_ratio": ratio,
        "pla_cmr_ratio_state": ratio_state,
        "duplicate_rows": int(evidence.get("duplicate_capital_rows") or 0),
    }


def _balance_profile(
    evidence: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    periods = {int(period) for period in (evidence.get("balance_periods") or set())}
    windows = {
        months: month_window(reference_period, months)
        for months in DESCRIPTIVE_HISTORY_WINDOWS
    }
    latest_present = bool(reference_period and reference_period in periods)
    core_window = windows[CORE_HISTORY_MONTHS]
    if not latest_present:
        state = "insufficient"
    elif _window_complete(periods, core_window):
        state = "complete_12m"
    else:
        state = "limited_history"

    balance_values = evidence.get("balance_values") or {}
    return {
        "source_table": "SES_Balanco.csv",
        "reference_period": reference_period,
        "state": state,
        "first_period": min(periods) if periods else None,
        "last_period": max(periods) if periods else None,
        "observed_periods_total": len(periods),
        "history_windows": {
            str(months): {
                "expected_months": months,
                "observed_months": _window_count(periods, window),
                "complete": _window_complete(periods, window),
            }
            for months, window in windows.items()
        },
        "liquidity_formula_component_presence": _formula_presence(
            balance_values,
            LIQUIDITY_FORMULA_CMPIDS,
            reference_period,
        ),
        "operating_formula_component_presence": _formula_presence(
            balance_values,
            OPERATING_FORMULA_CMPIDS,
            reference_period,
        ),
        "duplicate_candidate_cmpid_rows": int(
            evidence.get("duplicate_balance_cmpid_rows") or 0
        ),
    }


def _operations_profile(
    evidence: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    periods = {
        int(period) for period in (evidence.get("insurance_operation_periods") or set())
    }
    nonzero_periods = {
        int(period) for period in (evidence.get("nonzero_premium_periods") or set())
    }
    window = month_window(reference_period, CORE_HISTORY_MONTHS)
    if not periods:
        state = "not_observed"
    elif _window_complete(periods, window):
        state = "complete_12m"
    else:
        state = "limited_history"
    return {
        "source_table": "Ses_seguros.csv",
        "reference_period": reference_period,
        "state": state,
        "first_period": min(periods) if periods else None,
        "last_period": max(periods) if periods else None,
        "observed_periods_total": len(periods),
        "observed_months_last_12": _window_count(periods, window),
        "months_with_nonzero_premium_ganho_last_12": _window_count(
            nonzero_periods, window
        ),
        "complete_last_12": _window_complete(periods, window),
        "interpretation": (
            "Operational presence is descriptive evidence only. No absence or zero premium "
            "is converted into a negative score at this stage."
        ),
    }


def derive_financial_evidence_profile(
    entity: dict[str, Any],
    source_entity: dict[str, Any],
    reference_periods: dict[str, int | None],
) -> dict[str, Any]:
    regulatory_eligible = bool(
        (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    )
    if not regulatory_eligible:
        return {
            "version": FINANCIAL_EVIDENCE_VERSION,
            "state": "not_applicable",
            "core_financial_evidence_ready": False,
            "reason_codes": ["outside_regulatory_insurer_universe"],
        }

    capital = _capital_profile(source_entity, reference_periods.get("capital"))
    balance = _balance_profile(source_entity, reference_periods.get("balance"))
    operations = _operations_profile(
        source_entity, reference_periods.get("insurance_operations")
    )

    reasons: list[str] = []
    if capital["state"] == "insufficient":
        state = "insufficient_core_evidence"
        reasons.append("capital_evidence_insufficient")
    elif capital["state"] == "requires_investigation":
        state = "requires_investigation"
        reasons.append("capital_cmr_non_positive")
    elif balance["state"] == "insufficient":
        state = "insufficient_core_evidence"
        reasons.append("balance_evidence_insufficient")
    elif capital["state"] == "complete_12m" and balance["state"] == "complete_12m":
        state = "complete_core_history"
        reasons.append("capital_and_balance_12m_complete")
    else:
        state = "limited_core_history"
        if capital["state"] != "complete_12m":
            reasons.append("capital_history_under_12m")
        if balance["state"] != "complete_12m":
            reasons.append("balance_history_under_12m")

    if operations["state"] == "not_observed":
        reasons.append("insurance_operations_not_observed")
    elif operations["state"] == "limited_history":
        reasons.append("insurance_operations_history_under_12m")

    return {
        "version": FINANCIAL_EVIDENCE_VERSION,
        "state": state,
        "core_financial_evidence_ready": state == "complete_core_history",
        "assessment_eligible": False,
        "ranking_eligible": False,
        "capital": capital,
        "balance": balance,
        "operations": operations,
        "reason_codes": list(dict.fromkeys(reasons)),
        "methodology_note": (
            "This is an evidence-completeness profile, not a financial score. Twelve months "
            "is used only to identify a complete annual observation window; 24/36-month "
            "history is retained descriptively for later stability/confidence testing."
        ),
    }


def apply_financial_evidence(
    entities: list[dict[str, Any]],
    source_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    source_entities = source_payload.get("entities") or {}
    reference_periods = source_payload.get("reference_periods") or {}
    output: list[dict[str, Any]] = []
    for raw in entities:
        entity = deepcopy(raw)
        fip = str(entity.get("fip_code") or "").zfill(6)
        source_entity = source_entities.get(fip) or {}
        entity["financial_evidence"] = derive_financial_evidence_profile(
            entity,
            source_entity,
            reference_periods,
        )
        output.append(entity)
    return sorted(output, key=lambda item: item["entity_id"])


def validate_financial_evidence(entities: list[dict[str, Any]]) -> None:
    errors: list[str] = []
    for entity in entities:
        eligible = bool(
            (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
        )
        profile = entity.get("financial_evidence") or {}
        entity_id = entity.get("entity_id")
        if not eligible:
            if profile.get("state") != "not_applicable":
                errors.append(f"{entity_id}: non-eligible entity received financial profile")
            continue
        if profile.get("state") == "not_applicable":
            errors.append(f"{entity_id}: eligible insurer missing financial profile")
        if profile.get("assessment_eligible"):
            errors.append(f"{entity_id}: evidence profile cannot enable assessment")
        if profile.get("ranking_eligible"):
            errors.append(f"{entity_id}: evidence profile cannot enable ranking")
        capital = profile.get("capital") or {}
        if capital.get("pla_cmr_ratio_state") == "derivable":
            cmr = ((capital.get("latest") or {}).get("cmr"))
            if cmr is None or float(cmr) <= 0:
                errors.append(f"{entity_id}: derivable PLA/CMR ratio with invalid CMR")
    if errors:
        raise FinancialEvidenceInvariantError("; ".join(errors[:20]))


def financial_evidence_summary(entities: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [
        entity
        for entity in entities
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    states = Counter(
        (entity.get("financial_evidence") or {}).get("state", "missing")
        for entity in eligible
    )
    capital_states = Counter(
        ((entity.get("financial_evidence") or {}).get("capital") or {}).get(
            "state", "missing"
        )
        for entity in eligible
    )
    balance_states = Counter(
        ((entity.get("financial_evidence") or {}).get("balance") or {}).get(
            "state", "missing"
        )
        for entity in eligible
    )
    operation_states = Counter(
        ((entity.get("financial_evidence") or {}).get("operations") or {}).get(
            "state", "missing"
        )
        for entity in eligible
    )
    return {
        "financial_evidence_version": FINANCIAL_EVIDENCE_VERSION,
        "regulatory_eligible_count": len(eligible),
        "financial_evidence_state_counts": dict(sorted(states.items())),
        "capital_state_counts": dict(sorted(capital_states.items())),
        "balance_state_counts": dict(sorted(balance_states.items())),
        "operations_state_counts": dict(sorted(operation_states.items())),
        "core_financial_evidence_ready_count": sum(
            bool((entity.get("financial_evidence") or {}).get("core_financial_evidence_ready"))
            for entity in eligible
        ),
        "assessment_eligible_count": 0,
        "ranking_eligible_count": 0,
    }
