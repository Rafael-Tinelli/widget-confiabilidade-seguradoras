from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.v2.build_public_search_profile_contract import write_public_outputs

CONTRACT_PATH = Path("data/derived/v2/public_search_profile_contract.json")
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
CONDUCT_PATH = Path("data/derived/v2/conduct_methodology_closure.json")
SANDBOX_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")
VERSION = "2.0-public-information-projection-1"


class PublicInformationProjectionError(RuntimeError):
    """Raised when approved internal context cannot be projected safely."""


def _normalize_months(values: list[Any], label: str) -> list[str]:
    months = [str(value or "").strip() for value in values]
    if not months or any(len(month) != 7 or month[4] != "-" for month in months):
        raise PublicInformationProjectionError(f"invalid {label} month window: {months}")
    if months != sorted(months) or len(months) != len(set(months)):
        raise PublicInformationProjectionError(f"unordered/duplicate {label} months: {months}")
    return months


def _conduct_months(conduct: dict[str, Any]) -> list[str]:
    windows: set[tuple[str, ...]] = set()
    for row in conduct.get("candidate_entities") or []:
        monthly = ((row.get("direct_pressure") or {}).get("monthly") or [])
        months = [str(item.get("month") or "") for item in monthly if item.get("month")]
        if months:
            windows.add(tuple(_normalize_months(months, "Conduct")))
    if not windows:
        raise PublicInformationProjectionError("Conduct closure has no monthly window")
    if len(windows) != 1:
        raise PublicInformationProjectionError(
            f"Conduct candidate windows disagree: {sorted(windows)}"
        )
    months = list(next(iter(windows)))
    if len(months) != 12:
        raise PublicInformationProjectionError(
            f"expected 12 preserved Conduct months, got {len(months)}"
        )
    return months


def _sandbox_months(sandbox: dict[str, Any]) -> list[str]:
    months = _normalize_months(list((sandbox.get("source") or {}).get("months") or []), "Sandbox")
    if len(months) != 12:
        raise PublicInformationProjectionError(
            f"expected 12 Sandbox months, got {len(months)}"
        )
    return months


def _reference_window(months: list[str]) -> dict[str, Any]:
    return {
        "start_month": months[0],
        "end_month": months[-1],
        "months": len(months),
        "semantics": "preserved_consumer_gov_window_not_inferred_by_frontend",
    }


def apply_public_information_projection(
    contract: dict[str, Any],
    explorer: dict[str, Any],
    conduct: dict[str, Any],
    sandbox: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if contract.get("artifact") != "v2_public_search_profile_contract":
        raise PublicInformationProjectionError("unexpected public profile contract")
    if explorer.get("artifact") != "v2_public_insurer_explorer":
        raise PublicInformationProjectionError("unexpected public explorer")
    if conduct.get("artifact") != "v2_conduct_methodology_closure":
        raise PublicInformationProjectionError("unexpected Conduct closure")
    if sandbox.get("artifact") != "v2_sandbox_brand_conduct_evidence":
        raise PublicInformationProjectionError("unexpected Sandbox Conduct artifact")

    conduct_months = _conduct_months(conduct)
    sandbox_months = _sandbox_months(sandbox)
    if conduct_months != sandbox_months:
        raise PublicInformationProjectionError(
            f"ordinary/Sandbox Conduct windows differ: {conduct_months} != {sandbox_months}"
        )
    window = _reference_window(conduct_months)

    projected_explorer = deepcopy(explorer)
    explorer_count = 0
    for entity in projected_explorer.get("entities") or []:
        public_conduct = entity.get("conduct")
        if not isinstance(public_conduct, dict):
            raise PublicInformationProjectionError(
                f"explorer entity lacks Conduct block: {entity.get('entity_id')}"
            )
        public_conduct["reference_window"] = deepcopy(window)
        explorer_count += 1

    projected_contract = deepcopy(contract)
    ordinary_count = 0
    sandbox_count = 0
    brand_sandbox_count = 0
    for profile in projected_contract.get("profiles") or []:
        assessment = profile.get("assessment") or {}
        assessment_conduct = assessment.get("conduct")
        if isinstance(assessment_conduct, dict):
            assessment_conduct["reference_window"] = deepcopy(window)
            ordinary_count += 1

        sandbox_conduct = profile.get("sandbox_conduct")
        if isinstance(sandbox_conduct, dict):
            sandbox_conduct["reference_window"] = deepcopy(window)
            sandbox_count += 1

        brand_context = profile.get("sandbox_conduct_context")
        if isinstance(brand_context, dict):
            brand_context["reference_window"] = deepcopy(window)
            brand_sandbox_count += 1

    projected_contract["public_information_projection"] = {
        "version": VERSION,
        "conduct_reference_window": deepcopy(window),
        "policy": {
            "frontend_may_infer_conduct_period": False,
            "ordinary_and_sandbox_windows_must_match": True,
            "projection_changes_methodology": False,
        },
        "counts": {
            "explorer_entities": explorer_count,
            "ordinary_profiles_with_conduct": ordinary_count,
            "sandbox_entity_contexts": sandbox_count,
            "sandbox_brand_contexts": brand_sandbox_count,
        },
    }
    return projected_contract, projected_explorer


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def project_from_files() -> tuple[dict[str, Any], dict[str, Any]]:
    contract = json.loads(CONTRACT_PATH.read_text(encoding="utf-8"))
    explorer = json.loads(EXPLORER_PATH.read_text(encoding="utf-8"))
    conduct = json.loads(CONDUCT_PATH.read_text(encoding="utf-8"))
    sandbox = json.loads(SANDBOX_PATH.read_text(encoding="utf-8"))
    projected_contract, projected_explorer = apply_public_information_projection(
        contract, explorer, conduct, sandbox
    )
    _write_json_atomic(CONTRACT_PATH, projected_contract)
    _write_json_atomic(EXPLORER_PATH, projected_explorer)
    write_public_outputs(projected_contract)
    return projected_contract, projected_explorer


def main() -> None:
    contract, explorer = project_from_files()
    print(
        json.dumps(
            {
                "artifact": contract["artifact"],
                "projection": contract["public_information_projection"],
                "explorer_entities": len(explorer.get("entities") or []),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
