from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CONTRACT_PATH = Path("data/derived/v2/public_search_profile_contract.json")
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
SANDBOX_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")
PUBLIC_DIR = Path("data/derived/v2/public")


class PublicProfileValidationError(RuntimeError):
    """Raised when the real public profile package violates the closed contract."""


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PublicProfileValidationError(message)


def _validate_population(
    contract: dict[str, Any],
    explorer: dict[str, Any],
    sandbox: dict[str, Any],
) -> None:
    population = contract.get("population") or {}
    lifecycle_entities = int(population.get("lifecycle_entities") or 0)
    brands = int(population.get("brands") or 0)
    profiles = int(population.get("profiles") or 0)
    search_entries = int(population.get("search_entries") or 0)
    ordinary = int(population.get("ordinary_current_insurer_profiles") or 0)
    assessed = int(
        population.get("ordinary_profiles_with_assessment_payload") or 0
    )
    sandbox_profiles = int(population.get("sandbox_entity_profiles") or 0)
    sandbox_with_conduct = int(
        population.get("sandbox_profiles_with_conduct_context") or 0
    )

    explorer_count = len(explorer.get("entities") or [])
    sandbox_count = len(sandbox.get("carriers") or [])

    _require(lifecycle_entities >= explorer_count, "search universe smaller than explorer")
    _require(profiles == lifecycle_entities + brands, "profile population mismatch")
    _require(search_entries == profiles, "every profile must have one search entry")
    _require(ordinary >= explorer_count, "ordinary search universe lost assessed insurers")
    _require(assessed == explorer_count, "assessment coverage differs from explorer")
    _require(sandbox_profiles == sandbox_count, "Sandbox profile population mismatch")
    _require(
        sandbox_with_conduct == sandbox_count,
        "not every Sandbox carrier received Conduct context",
    )


def _validate_policy(contract: dict[str, Any]) -> None:
    policy = contract.get("publication_policy") or {}
    required_false = {
        "frontend_may_use_fuzzy_search_to_decide_identity",
        "brand_inherits_entity_assessment",
        "group_membership_implies_succession_or_joint_venture",
        "missing_value_may_be_coerced_to_zero",
        "raw_zero_may_be_relabelled_as_missing",
        "zero_complaints_is_automatically_favorable",
        "sandbox_enters_ordinary_ranking",
        "php_may_recompute_methodology",
    }
    for key in required_false:
        _require(policy.get(key) is False, f"public safety policy changed: {key}")


def _validate_youse(profiles: dict[str, dict[str, Any]]) -> dict[str, Any]:
    youse = profiles.get("entity:fip:001121")
    _require(youse is not None, "Youse public profile missing")
    assessment = youse.get("assessment") or {}
    technical = ((assessment.get("conduct") or {}).get("technical") or {})

    observed = (technical.get("observed_complaints_12m") or {}).get("value")
    _require(observed == 1367, "Youse observed complaints changed unexpectedly")

    for metric in (
        "expected_complaints_12m",
        "observed_expected_ratio",
        "comparable_months",
    ):
        item = technical.get(metric) or {}
        _require(item.get("value") is None, f"Youse {metric} was coerced to a value")
        _require(
            item.get("availability") == "unavailable",
            f"Youse {metric} missingness semantics changed",
        )

    operation = (
        assessment.get("operation_context", {}).get("insurance_premium_direct_12m", {})
    )
    _require(operation.get("value") == 0.0, "Youse raw premium snapshot changed")
    _require(
        operation.get("public_use") == "do_not_render_as_operation_size",
        "Youse raw zero could be rendered as operation size",
    )

    relationships = (
        (youse.get("relationship_context") or {}).get("conduct_reconciliation") or []
    )
    relation = next(
        (
            row
            for row in relationships
            if row.get("relationship_type") == "consumer_subject_single_risk_carrier"
        ),
        None,
    )
    _require(relation is not None, "Youse -> Caixa relationship missing")
    names = {row.get("name") for row in relation.get("targets") or []}
    _require("CAIXA SEGURADORA S.A." in names, "Youse did not resolve Caixa")

    return {
        "complaints": observed,
        "expected": None,
        "operation_public_use": operation.get("public_use"),
    }


def _validate_loovi_lti(profiles: dict[str, dict[str, Any]]) -> int:
    loovi = profiles.get("brand:loovi")
    lti = profiles.get("entity:cnpj:47006254000180")
    _require(loovi is not None and lti is not None, "Loovi/LTI profiles missing")
    _require(
        (loovi.get("assessment") or {}).get("availability") == "not_applicable",
        "brand inherited entity assessment",
    )
    context = loovi.get("sandbox_conduct_context") or {}
    complaints = ((context.get("metrics") or {}).get("complaints") or {}).get("value")
    _require(complaints == 1329, "Loovi/LTI complaints not preserved")
    _require(
        context.get("risk_carrier_profile_id") == "entity:cnpj:47006254000180",
        "Loovi did not resolve to LTI",
    )
    lti_complaints = (
        (((lti.get("sandbox_conduct") or {}).get("metrics") or {}).get("complaints") or {})
        .get("value")
    )
    _require(lti_complaints == 1329, "LTI carrier complaints not preserved")
    return int(complaints)


def _validate_hdi(profiles: dict[str, dict[str, Any]]) -> str:
    hdi = profiles.get("entity:fip:006572")
    hdi_global = profiles.get("entity:fip:001571")
    _require(hdi is not None and hdi_global is not None, "HDI profiles missing")
    _require(
        hdi["identity"].get("cnpj") != hdi_global["identity"].get("cnpj"),
        "HDI and HDI Global identities collapsed",
    )
    group = (hdi.get("relationship_context") or {}).get("economic_group") or {}
    _require(group.get("group_name") == "TALANX AG", "HDI TALANX context missing")
    related = {row.get("profile_id") for row in group.get("related_entities") or []}
    _require("entity:fip:001571" in related, "HDI Global missing from HDI context")
    return str(group["group_name"])


def _walk_public(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        forbidden = {"score", "overall_score", "ranking_position", "winner"}
        overlap = forbidden & set(value)
        _require(not overlap, f"forbidden decision fields at {path}: {sorted(overlap)}")
        is_metric = {"value", "availability", "public_use", "meaning"} <= set(value)
        if is_metric:
            _require(
                value["availability"] != "unavailable" or value["value"] is None,
                f"unavailable metric carries value at {path}",
            )
        for key, child in value.items():
            _walk_public(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_public(child, f"{path}[{index}]")


def validate_real_public_search_profile_contract() -> dict[str, Any]:
    contract = _load(CONTRACT_PATH)
    explorer = _load(EXPLORER_PATH)
    sandbox = _load(SANDBOX_PATH)
    _require(
        contract.get("status") == "public_search_profile_contract_closed",
        "public search/profile contract did not close",
    )
    _validate_population(contract, explorer, sandbox)
    _validate_policy(contract)

    profiles = {
        row["profile_id"]: row for row in contract.get("profiles") or []
    }
    youse = _validate_youse(profiles)
    loovi_complaints = _validate_loovi_lti(profiles)
    hdi_group = _validate_hdi(profiles)
    _walk_public(contract.get("profiles") or [])

    profile_count = int((contract.get("population") or {}).get("profiles") or 0)
    _require((PUBLIC_DIR / "search_index.json").is_file(), "search index missing")
    _require((PUBLIC_DIR / "profile_manifest.json").is_file(), "manifest missing")
    _require(
        len(list((PUBLIC_DIR / "profiles").glob("*.json"))) == profile_count,
        "profile-file count differs from profile population",
    )

    ordinary = int(
        (contract.get("population") or {}).get("ordinary_current_insurer_profiles") or 0
    )
    explorer_count = len(explorer.get("entities") or [])
    return {
        "status": contract["status"],
        "population": contract["population"],
        "explorer_assessment_profiles": explorer_count,
        "new_ordinary_profiles_without_assessment": ordinary - explorer_count,
        "youse": youse,
        "loovi_lti_complaints": loovi_complaints,
        "hdi_group": hdi_group,
    }


def main() -> None:
    print(
        json.dumps(
            validate_real_public_search_profile_contract(),
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
