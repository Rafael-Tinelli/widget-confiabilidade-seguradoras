from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

from api.v2.financial_evidence import (
    CAPITAL_PLA_SOURCE_FIELD,
    FINANCIAL_EVIDENCE_VERSION,
)
from api.v2.financial_periods import MATURITY_POLICY_VERSION

FINANCIAL_EVIDENCE_PATH = Path(
    "data/derived/v2/entity_financial_evidence_inventory.json"
)
FINANCIAL_CLOSURE_PATH = Path(
    "data/derived/v2/financial_methodology_closure.json"
)
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
LEADERBOARD_PATH = Path(
    "data/derived/v2/public/leaderboards/highest_pla_cmr_ratio.json"
)
PROFILES_DIR = Path("data/derived/v2/public/profiles")
OUTPUT_PATH = Path("data/derived/v2/financial_publication_audit.json")

REL_TOL = 1e-12
ABS_TOL = 1e-12


class FinancialPublicationAuditError(RuntimeError):
    """Raised when capital evidence changes across the publication chain."""


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
            f"{label}: expected finite values, got actual={actual!r} expected={expected!r}"
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
    top_positions: int,
) -> list[tuple[str, float, int]]:
    prepared: list[tuple[str, str, float]] = []
    for row in explorer_rows:
        ratio = _finite(((row.get("financial") or {}).get("capital") or {}).get(
            "pla_cmr_ratio"
        ))
        if ratio is None:
            continue
        prepared.append(
            (
                str(row.get("entity_id") or ""),
                str(row.get("legal_name") or ""),
                ratio,
            )
        )
    prepared.sort(key=lambda row: (-row[2], row[1], row[0]))

    result: list[tuple[str, float, int]] = []
    previous_value: float | None = None
    previous_rank = 0
    for index, (entity_id, _legal_name, ratio) in enumerate(prepared, start=1):
        if previous_value is None or ratio != previous_value:
            rank = index
            previous_rank = rank
            previous_value = ratio
        else:
            rank = previous_rank
        if rank > top_positions:
            break
        result.append((entity_id, ratio, rank))
    return result


def audit_financial_publication_chain(
    financial: dict[str, Any],
    closure: dict[str, Any],
    explorer: dict[str, Any],
    leaderboard: dict[str, Any],
    profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    meta = financial.get("meta") or {}
    if meta.get("financial_evidence_version") != FINANCIAL_EVIDENCE_VERSION:
        raise FinancialPublicationAuditError(
            "financial evidence version mismatch: "
            f"{meta.get('financial_evidence_version')!r} != {FINANCIAL_EVIDENCE_VERSION!r}"
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
            f"{maturity.get('capital_pla_source_field')!r} != {CAPITAL_PLA_SOURCE_FIELD!r}"
        )

    reference_period = int(maturity.get("selected_period") or 0)
    if not reference_period:
        raise FinancialPublicationAuditError("financial maturity has no selected_period")

    if closure.get("status") != "financial_methodology_closed_for_signal_design":
        raise FinancialPublicationAuditError("financial methodology closure is not closed")
    closure_period = int(
        ((closure.get("source_contract") or {}).get("reference_period")) or 0
    )
    if closure_period != reference_period:
        raise FinancialPublicationAuditError(
            f"financial closure reference period mismatch: {closure_period} != {reference_period}"
        )

    if leaderboard.get("id") != "highest_pla_cmr_ratio":
        raise FinancialPublicationAuditError("unexpected PLA/CMR leaderboard id")
    if leaderboard.get("metric") != "pla_cmr_ratio":
        raise FinancialPublicationAuditError("unexpected PLA/CMR leaderboard metric")
    if leaderboard.get("direction") != "descending":
        raise FinancialPublicationAuditError("PLA/CMR leaderboard must be descending")
    if leaderboard.get("is_general_ranking") is not False:
        raise FinancialPublicationAuditError(
            "PLA/CMR leaderboard must not become a general ranking"
        )

    financial_by = _eligible_financial_map(financial)
    closure_by = _row_map(closure.get("entities"), "financial_closure")
    explorer_by = _row_map(explorer.get("entities"), "insurer_explorer")
    profiles_by = _profile_map(profiles)

    financial_ids = set(financial_by)
    if not financial_ids:
        raise FinancialPublicationAuditError("financial regulatory population is empty")
    if set(closure_by) != financial_ids:
        raise FinancialPublicationAuditError(
            "financial closure entity set differs from Financial Evidence"
        )
    if set(explorer_by) != financial_ids:
        raise FinancialPublicationAuditError(
            "insurer explorer entity set differs from Financial Evidence"
        )
    missing_profiles = sorted(financial_ids - set(profiles_by))
    if missing_profiles:
        raise FinancialPublicationAuditError(
            f"public profiles missing financial entities: {missing_profiles[:10]}"
        )

    derivable = 0
    unavailable = 0
    below_requirement = 0

    for entity_id in sorted(financial_ids):
        financial_row = financial_by[entity_id]
        financial_capital = (
            (financial_row.get("financial_evidence") or {}).get("capital") or {}
        )
        closure_row = closure_by[entity_id]
        closure_capital = closure_row.get("capital") or {}
        explorer_row = explorer_by[entity_id]
        explorer_financial = explorer_row.get("financial") or {}
        explorer_capital = explorer_financial.get("capital") or {}
        profile = profiles_by[entity_id]
        profile_assessment = profile.get("assessment") or {}
        profile_financial = profile_assessment.get("financial") or {}
        profile_capital = profile_financial.get("capital") or {}
        profile_ratio = ((profile_capital.get("technical") or {}).get("ratio") or {})

        entity_period = int(financial_capital.get("reference_period") or 0)
        if entity_period != reference_period:
            raise FinancialPublicationAuditError(
                f"{entity_id}: Financial Evidence period {entity_period} != {reference_period}"
            )
        if int(closure_row.get("reference_period") or 0) != reference_period:
            raise FinancialPublicationAuditError(
                f"{entity_id}: financial closure period mismatch"
            )
        if int(explorer_financial.get("reference_period") or 0) != reference_period:
            raise FinancialPublicationAuditError(
                f"{entity_id}: explorer financial period mismatch"
            )
        if int(profile_financial.get("reference_period") or 0) != reference_period:
            raise FinancialPublicationAuditError(
                f"{entity_id}: profile financial period mismatch"
            )

        ratio_state = financial_capital.get("pla_cmr_ratio_state")
        stored_ratio = _finite(financial_capital.get("pla_cmr_ratio"))

        if ratio_state == "derivable":
            derivable += 1
            if financial_capital.get("pla_cmr_numerator_field") != CAPITAL_PLA_SOURCE_FIELD:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: PLA/CMR numerator is not {CAPITAL_PLA_SOURCE_FIELD}"
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

            expected_state = (
                "capital_below_cmr"
                if expected_ratio < 1.0
                else "capital_meets_or_exceeds_cmr"
            )
            if expected_state == "capital_below_cmr":
                below_requirement += 1
            if closure_capital.get("state") != expected_state:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: financial closure capital state mismatch"
                )
            _assert_close(
                closure_capital.get("pla_cmr_ratio"),
                expected_ratio,
                f"{entity_id}: Financial Closure PLA/CMR",
            )
            if explorer_capital.get("state") != expected_state:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: explorer capital state mismatch"
                )
            _assert_close(
                explorer_capital.get("pla_cmr_ratio"),
                expected_ratio,
                f"{entity_id}: Explorer PLA/CMR",
            )
            if profile_capital.get("state") != expected_state:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: profile capital state mismatch"
                )
            if profile_ratio.get("availability") != "available":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: derivable profile ratio is not available"
                )
            _assert_close(
                profile_ratio.get("value"),
                expected_ratio,
                f"{entity_id}: Profile PLA/CMR",
            )
        else:
            unavailable += 1
            if stored_ratio is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: non-derivable Financial Evidence carries PLA/CMR value"
                )
            if closure_capital.get("state") != "capital_signal_unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable capital became conclusive in Financial Closure"
                )
            if closure_capital.get("pla_cmr_ratio") is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable Financial Closure carries PLA/CMR value"
                )
            if explorer_capital.get("state") != "capital_signal_unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable capital became conclusive in explorer"
                )
            if explorer_capital.get("pla_cmr_ratio") is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable explorer capital carries PLA/CMR value"
                )
            if profile_capital.get("state") != "capital_signal_unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable capital became conclusive in profile"
                )
            if profile_ratio.get("value") is not None:
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile carries PLA/CMR value"
                )
            if profile_ratio.get("availability") != "unavailable":
                raise FinancialPublicationAuditError(
                    f"{entity_id}: unavailable profile ratio lacks unavailable semantics"
                )

    top_positions = int(leaderboard.get("top_positions") or 0)
    if top_positions <= 0:
        raise FinancialPublicationAuditError("PLA/CMR leaderboard has invalid top_positions")
    expected_leaderboard = _competition_ranking(
        list(explorer_by.values()),
        top_positions=top_positions,
    )
    actual_entries = leaderboard.get("entries") or []
    if len(actual_entries) != len(expected_leaderboard):
        raise FinancialPublicationAuditError(
            "PLA/CMR leaderboard entry count differs from explorer-derived ranking"
        )
    for actual, (expected_id, expected_ratio, expected_rank) in zip(
        actual_entries,
        expected_leaderboard,
        strict=True,
    ):
        if actual.get("entity_id") != expected_id:
            raise FinancialPublicationAuditError(
                "PLA/CMR leaderboard order differs from explorer-derived ranking"
            )
        if int(actual.get("leaderboard_rank") or 0) != expected_rank:
            raise FinancialPublicationAuditError(
                f"{expected_id}: PLA/CMR leaderboard rank mismatch"
            )
        _assert_close(
            actual.get("pla_cmr_ratio"),
            expected_ratio,
            f"{expected_id}: Leaderboard PLA/CMR",
        )

    return {
        "artifact": "v2_financial_publication_audit",
        "status": "financial_publication_chain_verified",
        "reference_period": reference_period,
        "contracts": {
            "financial_evidence_version": FINANCIAL_EVIDENCE_VERSION,
            "financial_maturity_policy_version": MATURITY_POLICY_VERSION,
            "capital_pla_source_field": CAPITAL_PLA_SOURCE_FIELD,
        },
        "population": {
            "regulatory_entities": len(financial_ids),
            "capital_derivable": derivable,
            "capital_unavailable": unavailable,
            "capital_below_cmr": below_requirement,
            "leaderboard_entries": len(actual_entries),
        },
        "verified_boundaries": [
            "financial_evidence_ratio_equals_new_pla_divided_by_cmr",
            "financial_closure_preserves_capital_ratio_and_state",
            "insurer_explorer_preserves_capital_ratio_and_state",
            "public_profile_preserves_capital_ratio_and_state",
            "pla_cmr_leaderboard_is_derived_from_explorer_without_secondary_merit_tiebreaker",
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
        raise FinancialPublicationAuditError("public profiles directory is empty")
    return profiles


def run_audit(
    *,
    financial_path: Path = FINANCIAL_EVIDENCE_PATH,
    closure_path: Path = FINANCIAL_CLOSURE_PATH,
    explorer_path: Path = EXPLORER_PATH,
    leaderboard_path: Path = LEADERBOARD_PATH,
    profiles_dir: Path = PROFILES_DIR,
    output_path: Path = OUTPUT_PATH,
) -> dict[str, Any]:
    financial = json.loads(financial_path.read_text(encoding="utf-8"))
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    explorer = json.loads(explorer_path.read_text(encoding="utf-8"))
    leaderboard = json.loads(leaderboard_path.read_text(encoding="utf-8"))
    profiles = load_profiles(profiles_dir)

    result = audit_financial_publication_chain(
        financial,
        closure,
        explorer,
        leaderboard,
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
            "Audit PLA/CMR from Financial Evidence through Financial Closure and public JSON."
        )
    )
    parser.add_argument("--financial", type=Path, default=FINANCIAL_EVIDENCE_PATH)
    parser.add_argument("--closure", type=Path, default=FINANCIAL_CLOSURE_PATH)
    parser.add_argument("--explorer", type=Path, default=EXPLORER_PATH)
    parser.add_argument("--leaderboard", type=Path, default=LEADERBOARD_PATH)
    parser.add_argument("--profiles-dir", type=Path, default=PROFILES_DIR)
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_audit(
        financial_path=args.financial,
        closure_path=args.closure,
        explorer_path=args.explorer,
        leaderboard_path=args.leaderboard,
        profiles_dir=args.profiles_dir,
        output_path=args.output,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
