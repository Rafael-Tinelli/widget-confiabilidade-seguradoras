from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path
from typing import Any, Callable

from api.v2.financial_evidence import (
    CAPITAL_PLA_SOURCE_FIELD,
    FINANCIAL_EVIDENCE_VERSION,
)
from api.v2.financial_periods import MATURITY_POLICY_VERSION
from api.v2.liquidity_experiment import LIQUIDITY_EXPERIMENT_VERSION
from api.v2.operating_states import OPERATING_STATE_VERSION

FINANCIAL_EVIDENCE_PATH = Path(
    "data/derived/v2/entity_financial_evidence_inventory.json"
)
LIQUIDITY_PATH = Path("data/derived/v2/liquidity_experiment.json")
OPERATING_PATH = Path("data/derived/v2/operating_experiment.json")
FINANCIAL_CLOSURE_PATH = Path(
    "data/derived/v2/financial_methodology_closure.json"
)
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
CAPITAL_LEADERBOARD_PATH = Path(
    "data/derived/v2/public/leaderboards/highest_pla_cmr_ratio.json"
)
ILT_LEADERBOARD_PATH = Path(
    "data/derived/v2/public/leaderboards/highest_ilt.json"
)
PROFILES_DIR = Path("data/derived/v2/public/profiles")
OUTPUT_PATH = Path("data/derived/v2/financial_publication_audit.json")

REL_TOL = 1e-12
ABS_TOL = 1e-12


class FinancialPublicationAuditError(RuntimeError):
    """Raised when financial evidence changes across the publication chain."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _assert_close(actual: Any, expected: Any, label: str) -> None:
    actual_number = _finite(actual)
    expected_number = _finite(expected)
    if actual_number is None or expected_number is None:
        raise FinancialPublicationAuditError(
            f"{label}: expected finite values, "
            f"got actual={actual!r} expected={expected!r}"
        )
    if not math.isclose(
        actual_number,
        expected_number,
        rel_tol=REL_TOL,
        abs_tol=ABS_TOL,
    ):
        raise FinancialPublicationAuditError(
            f"{label}: {actual_number!r} != {expected_number!r}"
        )


def _row_map(rows: Any, label: str) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            raise FinancialPublicationAuditError(f"{label}: non-object row")
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            raise FinancialPublicationAuditError(f"{label}: row without entity_id")
        if entity_id in output:
            raise FinancialPublicationAuditError(
                f"{label}: duplicate entity_id {entity_id}"
            )
        output[entity_id] = row
    return output


def _eligible_financial_map(
    financial: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return _row_map(
        [
            row
            for row in financial.get("entities") or []
            if (row.get("eligibility") or {}).get("regulatory_universe_eligible")
        ],
        "financial_evidence",
    )


def _profile_map(profiles: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        identity = profile.get("identity") or {}
        entity_id = str(identity.get("entity_id") or "")
        if not entity_id:
            continue
        if entity_id in output:
            raise FinancialPublicationAuditError(
                f"profiles: duplicate identity.entity_id {entity_id}"
            )
        output[entity_id] = profile
    return output


def _competition_ranking(
    explorer_rows: list[dict[str, Any]],
    *,
    value_getter: Callable[[dict[str, Any]], Any],
    top_positions: int,
) -> list[tuple[str, float, int]]:
    prepared: list[tuple[str, str, float]] = []
    for row in explorer_rows:
        value = _finite(value_getter(row))
        if value is None:
            continue
        prepared.append(
            (
                str(row.get("entity_id") or ""),
                str(row.get("legal_name") or ""),
                value,
            )
        )
    prepared.sort(key=lambda row: (-row[2], row[1], row[0]))

    result: list[tuple[str, float, int]] = []
    previous_value: float | None = None
    previous_rank = 0
    for index, (entity_id, _legal_name, value) in enumerate(prepared, start=1):
        if previous_value is None or value != previous_value:
            rank = index
            previous_rank = rank
            previous_value = value
        else:
            rank = previous_rank
        if rank > top_positions:
            break
        result.append((entity_id, value, rank))
    return result


def _validate_leaderboard(
    leaderboard: dict[str, Any],
    *,
    leaderboard_id: str,
    metric: str,
    value_field: str,
    explorer_rows: list[dict[str, Any]],
    value_getter: Callable[[dict[str, Any]], Any],
) -> int:
    if leaderboard.get("id") != leaderboard_id:
        raise FinancialPublicationAuditError(
            f"unexpected leaderboard id: {leaderboard.get('id')!r}"
        )
    if leaderboard.get("metric") != metric:
        raise FinancialPublicationAuditError(
            f"{leaderboard_id}: unexpected metric {leaderboard.get('metric')!r}"
        )
    if leaderboard.get("direction") != "descending":
        raise FinancialPublicationAuditError(
            f"{leaderboard_id}: leaderboard must be descending"
        )
    if leaderboard.get("is_general_ranking") is not False:
        raise FinancialPublicationAuditError(
            f"{leaderboard_id}: must not become a general ranking"
        )
    top_positions = int(leaderboard.get("top_positions") or 0)
    if top_positions <= 0:
        raise FinancialPublicationAuditError(
            f"{leaderboard_id}: invalid top_positions"
        )
    expected = _competition_ranking(
        explorer_rows,
        value_getter=value_getter,
        top_positions=top_positions,
    )
    actual_entries = leaderboard.get("entries") or []
    if len(actual_entries) != len(expected):
        raise FinancialPublicationAuditError(
            f"{leaderboard_id}: entry count differs from explorer-derived ranking"
        )
    for actual, (expected_id, expected_value, expected_rank) in zip(
        actual_entries,
        expected,
        strict=True,
    ):
        if actual.get("entity_id") != expected_id:
            raise FinancialPublicationAuditError(
                f"{leaderboard_id}: order differs from explorer-derived ranking"
            )
        if int(actual.get("leaderboard_rank") or 0) != expected_rank:
            raise FinancialPublicationAuditError(
                f"{expected_id}: {leaderboard_id} rank mismatch"
            )
        _assert_close(
            actual.get(value_field),
            expected_value,
            f"{expected_id}: {leaderboard_id} value",
        )
    return len(actual_entries)


def _validate_common_contracts(
    financial: dict[str, Any],
    liquidity: dict[str, Any],
    operating: dict[str, Any],
    closure: dict[str, Any],
) -> int:
    meta = financial.get("meta") or {}
    if meta.get("financial_evidence_version") != FINANCIAL_EVIDENCE_VERSION:
        raise FinancialPublicationAuditError(
            "financial evidence version mismatch: "
            f"{meta.get('financial_evidence_version')!r} "
            f"!= {FINANCIAL_EVIDENCE_VERSION!r}"
        )
    maturity = meta.get("financial_period_maturity") or {}
    if maturity.get("policy_version") != MATURITY_POLICY_VERSION:
        raise FinancialPublicationAuditError(
            "financial maturity policy version mismatch: "
            f"{maturity.get('policy_version')!r} != {MATURITY_POLICY_VERSION!r}"
        )
    if maturity.get("capital_pla_source_field") != CAPITAL_PLA_SOURCE_FIELD:
        raise FinancialPublicationAuditError(
            "financial maturity capital numerator mismatch: "
            f"{maturity.get('capital_pla_source_field')!r} "
            f"!= {CAPITAL_PLA_SOURCE_FIELD!r}"
        )
    reference_period = int(maturity.get("selected_period") or 0)
    if not reference_period:
        raise FinancialPublicationAuditError(
            "financial maturity has no selected_period"
        )

    if liquidity.get("artifact") != "v2_liquidity_experiment":
        raise FinancialPublicationAuditError("unexpected liquidity artifact")
    liquidity_summary = liquidity.get("summary") or {}
    if int(liquidity_summary.get("reference_period") or 0) != reference_period:
        raise FinancialPublicationAuditError(
            "liquidity reference period differs from Financial Evidence"
        )
    if (
        (liquidity_summary.get("period_maturity") or {}).get("policy_version")
        != MATURITY_POLICY_VERSION
    ):
        raise FinancialPublicationAuditError(
            "liquidity maturity policy version differs from Financial Evidence"
        )

    if operating.get("artifact") != "v2_operating_experiment":
        raise FinancialPublicationAuditError("unexpected operating artifact")
    operating_summary = operating.get("summary") or {}
    if int(operating_summary.get("reference_period") or 0) != reference_period:
        raise FinancialPublicationAuditError(
            "operating reference period differs from Financial Evidence"
        )
    if (
        (operating.get("period_maturity") or {}).get("policy_version")
        != MATURITY_POLICY_VERSION
    ):
        raise FinancialPublicationAuditError(
            "operating maturity policy version differs from Financial Evidence"
        )

    if closure.get("status") != "financial_methodology_closed_for_signal_design":
        raise FinancialPublicationAuditError(
            "financial methodology closure is not closed"
        )
    closure_period = int(
        ((closure.get("source_contract") or {}).get("reference_period")) or 0
    )
    if closure_period != reference_period:
        raise FinancialPublicationAuditError(
            f"financial closure reference period mismatch: "
            f"{closure_period} != {reference_period}"
        )
    return reference_period


def audit_financial_publication_chain(
    financial: dict[str, Any],
    liquidity: dict[str, Any],
    operating: dict[str, Any],
    closure: dict[str, Any],
    explorer: dict[str, Any],
    capital_leaderboard: dict[str, Any],
    ilt_leaderboard: dict[str, Any],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    reference_period = _validate_common_contracts(
        financial,
        liquidity,
        operating,
        closure,
    )

    financial_by = _eligible_financial_map(financial)
    liquidity_by = _row_map(liquidity.get("entities"), "liquidity_experiment")
    operating_by = _row_map(operating.get("entities"), "operating_experiment")
    closure_by = _row_map(closure.get("entities"), "financial_closure")
    explorer_by = _row_map(explorer.get("entities"), "insurer_explorer")
    profiles_by = _profile_map(profiles)

    financial_ids = set(financial_by)
    if not financial_ids:
        raise FinancialPublicationAuditError(
            "financial regulatory population is empty"
        )
    for label, mapping in (
        ("liquidity experiment", liquidity_by),
        ("operating experiment", operating_by),
        ("financial closure", closure_by),
        ("insurer explorer", explorer_by),
    ):
        if set(mapping) != financial_ids:
            raise FinancialPublicationAuditError(
                f"{label} entity set differs from Financial Evidence"
            )
    missing_profiles = sorted(financial_ids - set(profiles_by))
    if missing_profiles:
        raise FinancialPublicationAuditError(
            f"public profiles missing financial entities: {missing_profiles[:10]}"
        )

    capital_derivable = 0
    capital_unavailable = 0
    capital_below_requirement = 0
    ilt_derivable = 0
    ilt_unavailable = 0
    ilt_below_parity = 0
    operating_signals: Counter[str] = Counter()

    for entity_id in sorted(financial_ids):
        financial_row = financial_by[entity_id]
        financial_capital = (
            (financial_row.get("financial_evidence") or {}).get("capital") or {}
        )
        liquidity_row = liquidity_by[entity_id]
        operating_row = operating_by[entity_id]
        closure_row = closure_by[entity_id]
        closure_capital = closure_row.get("capital") or {}
        closure_liquidity = closure_row.get("liquidity") or {}
        closure_operating = closure_row.get("operating_context") or {}
        explorer_row = explorer_by[entity_id]
        explorer_financial = explorer_row.get("financial") or {}
        explorer_capital = explorer_financial.get("capital") or {}
        explorer_liquidity = explorer_financial.get("liquidity") or {}
        explorer_operating = explorer_financial.get("operating_context") or {}
        profile = profiles_by[entity_id]
        profile_financial = ((profile.get("assessment") or {}).get("financial") or {})
        profile_capital = profile_financial.get("capital") or {}
        profile_capital_ratio = (
            (profile_capital.get("technical") or {}).get("ratio") or {}
        )
        profile_liquidity = profile_financial.get("liquidity") or {}
        profile_ilt_ratio = (
            (profile_liquidity.get("technical") or {}).get("ratio") or {}
        )
        profile_operating = profile_financial.get("operating_context") or {}

        financial_period = int(financial_capital.get("reference_period") or 0)
        if financial_period != reference_period:
            raise FinancialPublicationAuditError(
                f"{entity_id}: Financial Evidence period "
                f"{financial_period} != {reference_period}"
            )
        for label, period in (
            ("liquidity experiment", liquidity_row.get("reference_period")),
            ("operating experiment", operating_row.get("reference_period")),
            ("financial closure", closure_row.get("reference_period")),
            ("explorer", explorer_financial.get("reference_period")),
            ("profile", profile_financial.get("reference_period")),
        ):
            if int(period or 0) != reference_period:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: {label} period mismatch"
                )

        ratio_state = financial_capital.get("pla_cmr_ratio_state")
        stored_ratio = _finite(financial_capital.get("pla_cmr_ratio"))
        if ratio_state == "derivable":
            capital_derivable += 1
            if (
                financial_capital.get("pla_cmr_numerator_field")
                != CAPITAL_PLA_SOURCE_FIELD
            ):
                raise FinancialPublicationAuditError(
                    f"{entity_id}: PLA/CMR numerator is not "
                    f"{CAPITAL_PLA_SOURCE_FIELD}"
                )
            latest = financial_capital.get("latest") or {}
            new_pla = _finite(latest.get(CAPITAL_PLA_SOURCE_FIELD))
            cmr = _finite(latest.get("cmr"))
            if new_pla is None or cmr is None or cmr <= 0:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: derivable PLA/CMR lacks usable new_pla/CMR operands"
                )
            expected_ratio = new_pla / cmr
            _assert_close(
                stored_ratio,
                expected_ratio,
                f"{entity_id}: Financial Evidence new_pla/CMR",
            )
            expected_capital_state = (
                "capital_below_cmr"
                if expected_ratio < 1.0
                else "capital_meets_or_exceeds_cmr"
            )
            if expected_capital_state == "capital_below_cmr":
                capital_below_requirement += 1
            for label, capital in (
                ("Financial Closure", closure_capital),
                ("Explorer", explorer_capital),
            ):
                if capital.get("state") != expected_capital_state:
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: {label} capital state mismatch"
                    )
                _assert_close(
                    capital.get("pla_cmr_ratio"),
                    expected_ratio,
                    f"{entity_id}: {label} PLA/CMR",
                )
            if profile_capital.get("state") != expected_capital_state:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: profile capital state mismatch"
                )
            if profile_capital_ratio.get("availability") != "available":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: derivable profile capital ratio is not available"
                )
            _assert_close(
                profile_capital_ratio.get("value"),
                expected_ratio,
                f"{entity_id}: Profile PLA/CMR",
            )
        else:
            capital_unavailable += 1
            if stored_ratio is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: non-derivable Financial Evidence carries PLA/CMR value"
                )
            for label, capital in (
                ("Financial Closure", closure_capital),
                ("Explorer", explorer_capital),
            ):
                if capital.get("state") != "capital_signal_unavailable":
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: unavailable capital became conclusive in {label}"
                    )
                if capital.get("pla_cmr_ratio") is not None:
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: unavailable {label} carries PLA/CMR value"
                    )
            if profile_capital.get("state") != "capital_signal_unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable capital became conclusive in profile"
                )
            if profile_capital_ratio.get("value") is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile carries PLA/CMR value"
                )
            if profile_capital_ratio.get("availability") != "unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile capital ratio lacks semantics"
                )

        ilt_current = (
            ((liquidity_row.get("metrics") or {}).get("ILT") or {}).get("current")
            or {}
        )
        ilt_state = ilt_current.get("state")
        ilt_value = _finite(ilt_current.get("value"))
        if ilt_state == "derivable":
            ilt_derivable += 1
            numerator = _finite(ilt_current.get("numerator"))
            denominator = _finite(ilt_current.get("denominator"))
            if numerator is None or denominator is None or denominator <= 0:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: derivable ILT lacks usable numerator/denominator"
                )
            expected_ilt = numerator / denominator
            _assert_close(
                ilt_value,
                expected_ilt,
                f"{entity_id}: Liquidity Experiment ILT",
            )
            expected_liquidity_state = (
                "ilt_below_arithmetic_parity"
                if expected_ilt < 1.0
                else "ilt_at_or_above_arithmetic_parity"
            )
            if expected_liquidity_state == "ilt_below_arithmetic_parity":
                ilt_below_parity += 1
            for label, liquidity_payload in (
                ("Financial Closure", closure_liquidity),
                ("Explorer", explorer_liquidity),
            ):
                if liquidity_payload.get("state") != expected_liquidity_state:
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: {label} liquidity state mismatch"
                    )
                if liquidity_payload.get("metric") != "ILT":
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: {label} liquidity metric is not ILT"
                    )
                _assert_close(
                    liquidity_payload.get("value"),
                    expected_ilt,
                    f"{entity_id}: {label} ILT",
                )
                if liquidity_payload.get("parity_is_regulatory_threshold") is not False:
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: {label} mislabels ILT parity as regulatory"
                    )
            if profile_liquidity.get("state") != expected_liquidity_state:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: profile liquidity state mismatch"
                )
            if profile_ilt_ratio.get("availability") != "available":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: derivable profile ILT is not available"
                )
            _assert_close(
                profile_ilt_ratio.get("value"),
                expected_ilt,
                f"{entity_id}: Profile ILT",
            )
        else:
            ilt_unavailable += 1
            if ilt_value is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: non-derivable liquidity experiment carries ILT value"
                )
            for label, liquidity_payload in (
                ("Financial Closure", closure_liquidity),
                ("Explorer", explorer_liquidity),
            ):
                if liquidity_payload.get("state") != "ilt_signal_unavailable":
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: unavailable ILT became conclusive in {label}"
                    )
                if liquidity_payload.get("value") is not None:
                    raise FinancialPublicationAuditError(
                        f"{entity_id}: unavailable {label} carries ILT value"
                    )
            if profile_liquidity.get("state") != "ilt_signal_unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable ILT became conclusive in profile"
                )
            if profile_ilt_ratio.get("value") is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile carries ILT value"
                )
            if profile_ilt_ratio.get("availability") != "unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile ILT lacks unavailable semantics"
                )

        operating_state = operating_row.get("operating_state") or {}
        if operating_state.get("version") != OPERATING_STATE_VERSION:
            raise FinancialPublicationAuditError(
                f"{entity_id}: operating state version mismatch"
            )
        expected_operating = {
            "signal": operating_state.get("operating_signal"),
            "history_state": operating_state.get("history_state"),
            "formula_state": operating_state.get("formula_state"),
            "reference_metric": "ICA",
            "supporting_metric": "IC",
            "overrides_core_signal": False,
        }
        operating_signals[str(expected_operating["signal"] or "missing")] += 1
        if closure_operating != expected_operating:
            raise FinancialPublicationAuditError(
                f"{entity_id}: Financial Closure operating context drift"
            )
        if explorer_operating != expected_operating:
            raise FinancialPublicationAuditError(
                f"{entity_id}: Explorer operating context drift"
            )
        if profile_operating != expected_operating:
            raise FinancialPublicationAuditError(
                f"{entity_id}: Profile operating context drift"
            )

    explorer_rows = list(explorer_by.values())
    capital_leaderboard_entries = _validate_leaderboard(
        capital_leaderboard,
        leaderboard_id="highest_pla_cmr_ratio",
        metric="pla_cmr_ratio",
        value_field="pla_cmr_ratio",
        explorer_rows=explorer_rows,
        value_getter=lambda row: (
            ((row.get("financial") or {}).get("capital") or {}).get("pla_cmr_ratio")
        ),
    )
    ilt_leaderboard_entries = _validate_leaderboard(
        ilt_leaderboard,
        leaderboard_id="highest_ilt",
        metric="ilt",
        value_field="ilt",
        explorer_rows=explorer_rows,
        value_getter=lambda row: (
            ((row.get("financial") or {}).get("liquidity") or {}).get("value")
        ),
    )

    return {
        "artifact": "v2_financial_publication_audit",
        "status": "financial_publication_chain_verified",
        "reference_period": reference_period,
        "contracts": {
            "financial_evidence_version": FINANCIAL_EVIDENCE_VERSION,
            "financial_maturity_policy_version": MATURITY_POLICY_VERSION,
            "capital_pla_source_field": CAPITAL_PLA_SOURCE_FIELD,
            "liquidity_experiment_version": LIQUIDITY_EXPERIMENT_VERSION,
            "operating_state_version": OPERATING_STATE_VERSION,
        },
        "population": {
            "regulatory_entities": len(financial_ids),
            "capital_derivable": capital_derivable,
            "capital_unavailable": capital_unavailable,
            "capital_below_cmr": capital_below_requirement,
            "ilt_derivable": ilt_derivable,
            "ilt_unavailable": ilt_unavailable,
            "ilt_below_arithmetic_parity": ilt_below_parity,
            "operating_signal_counts": dict(sorted(operating_signals.items())),
            "capital_leaderboard_entries": capital_leaderboard_entries,
            "ilt_leaderboard_entries": ilt_leaderboard_entries,
        },
        "verified_boundaries": [
            "financial_evidence_ratio_equals_new_pla_divided_by_cmr",
            "financial_closure_preserves_capital_ratio_and_state",
            "insurer_explorer_preserves_capital_ratio_and_state",
            "public_profile_preserves_capital_ratio_and_state",
            "pla_cmr_leaderboard_is_derived_from_explorer_without_secondary_merit_tiebreaker",
            "liquidity_experiment_ilt_equals_numerator_divided_by_positive_denominator",
            "financial_closure_preserves_ilt_value_state_and_non_regulatory_parity_semantics",
            "insurer_explorer_preserves_ilt_value_and_state",
            "public_profile_preserves_ilt_value_and_state",
            "ilt_leaderboard_is_derived_from_explorer_without_secondary_merit_tiebreaker",
            "operating_experiment_state_is_preserved_through_closure_explorer_and_profile",
        ],
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
    }


def load_profiles(profiles_dir: Path) -> list[dict[str, Any]]:
    if not profiles_dir.is_dir():
        raise FinancialPublicationAuditError(
            f"public profiles directory is unavailable: {profiles_dir}"
        )
    profiles: list[dict[str, Any]] = []
    for path in sorted(profiles_dir.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            profiles.append(payload)
    if not profiles:
        raise FinancialPublicationAuditError(
            "public profiles directory is empty"
        )
    return profiles


def run_audit(
    *,
    financial_path: Path = FINANCIAL_EVIDENCE_PATH,
    liquidity_path: Path = LIQUIDITY_PATH,
    operating_path: Path = OPERATING_PATH,
    closure_path: Path = FINANCIAL_CLOSURE_PATH,
    explorer_path: Path = EXPLORER_PATH,
    capital_leaderboard_path: Path = CAPITAL_LEADERBOARD_PATH,
    ilt_leaderboard_path: Path = ILT_LEADERBOARD_PATH,
    profiles_dir: Path = PROFILES_DIR,
    output_path: Path = OUTPUT_PATH,
) -> dict[str, Any]:
    financial = json.loads(financial_path.read_text(encoding="utf-8"))
    liquidity = json.loads(liquidity_path.read_text(encoding="utf-8"))
    operating = json.loads(operating_path.read_text(encoding="utf-8"))
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    explorer = json.loads(explorer_path.read_text(encoding="utf-8"))
    capital_leaderboard = json.loads(
        capital_leaderboard_path.read_text(encoding="utf-8")
    )
    ilt_leaderboard = json.loads(
        ilt_leaderboard_path.read_text(encoding="utf-8")
    )
    profiles = load_profiles(profiles_dir)

    result = audit_financial_publication_chain(
        financial,
        liquidity,
        operating,
        closure,
        explorer,
        capital_leaderboard,
        ilt_leaderboard,
        profiles,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit capital, ILT and operating context from financial evidence "
            "through public JSON."
        )
    )
    parser.add_argument("--financial", type=Path, default=FINANCIAL_EVIDENCE_PATH)
    parser.add_argument("--liquidity", type=Path, default=LIQUIDITY_PATH)
    parser.add_argument("--operating", type=Path, default=OPERATING_PATH)
    parser.add_argument("--closure", type=Path, default=FINANCIAL_CLOSURE_PATH)
    parser.add_argument("--explorer", type=Path, default=EXPLORER_PATH)
    parser.add_argument(
        "--capital-leaderboard",
        type=Path,
        default=CAPITAL_LEADERBOARD_PATH,
    )
    parser.add_argument(
        "--ilt-leaderboard",
        type=Path,
        default=ILT_LEADERBOARD_PATH,
    )
    parser.add_argument("--profiles-dir", type=Path, default=PROFILES_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_audit(
        financial_path=args.financial,
        liquidity_path=args.liquidity,
        operating_path=args.operating,
        closure_path=args.closure,
        explorer_path=args.explorer,
        capital_leaderboard_path=args.capital_leaderboard,
        ilt_leaderboard_path=args.ilt_leaderboard,
        profiles_dir=args.profiles_dir,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
