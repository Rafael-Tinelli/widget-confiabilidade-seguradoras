from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from api.v2.public_profile_regulatory_semantics import (
    SSPE_ASSESSMENT_REASON,
    SSPE_LABEL,
    SSPE_QUERY_STATE,
)

CONTRACT_PATH = Path("data/derived/v2/public_search_profile_contract.json")
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
SANDBOX_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")
PUBLIC_DIR = Path("data/derived/v2/public")


class PublicProfileValidationError(RuntimeError):
    """Raised when public profile outputs violate structural publication contracts."""


PUBLIC_COPY_FORBIDDEN = {
    "internal version label": re.compile(r"\bv2\b", re.IGNORECASE),
    "internal project wording": re.compile(r"\bprojeto\b", re.IGNORECASE),
    "implementation label": re.compile(r"\bwidget\b", re.IGNORECASE),
    "internal snapshot wording": re.compile(r"\bsnapshot\b", re.IGNORECASE),
}


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PublicProfileValidationError(message)


def _profile_maps(
    contract: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    profiles = list(contract.get("profiles") or [])
    by_profile_id = {
        str(row.get("profile_id") or ""): row for row in profiles
    }
    _require(
        "" not in by_profile_id and len(by_profile_id) == len(profiles),
        "public profile ids must be non-empty and unique",
    )

    entity_by_id: dict[str, dict[str, Any]] = {}
    for profile in profiles:
        if profile.get("profile_kind") != "entity":
            continue
        entity_id = str((profile.get("identity") or {}).get("entity_id") or "")
        _require(entity_id, f"entity profile without entity_id: {profile.get('profile_id')}")
        _require(entity_id not in entity_by_id, f"duplicate entity profile: {entity_id}")
        entity_by_id[entity_id] = profile
    return by_profile_id, entity_by_id


def _validate_population(
    contract: dict[str, Any],
    explorer: dict[str, Any],
    sandbox: dict[str, Any],
    entity_by_id: dict[str, dict[str, Any]],
) -> dict[str, int]:
    population = contract.get("population") or {}
    integer_fields = {
        key: population.get(key)
        for key in (
            "lifecycle_entities",
            "brands",
            "profiles",
            "search_entries",
            "ordinary_current_insurer_profiles",
            "ordinary_profiles_with_assessment_payload",
            "sandbox_entity_profiles",
            "sandbox_profiles_with_conduct_context",
            "special_purpose_insurer_profiles",
        )
    }
    for key, value in integer_fields.items():
        _require(isinstance(value, int) and value >= 0, f"invalid population field {key}: {value}")

    lifecycle_entities = int(integer_fields["lifecycle_entities"])
    brands = int(integer_fields["brands"])
    profiles = int(integer_fields["profiles"])
    search_entries = int(integer_fields["search_entries"])
    ordinary = int(integer_fields["ordinary_current_insurer_profiles"])
    assessed = int(integer_fields["ordinary_profiles_with_assessment_payload"])
    sandbox_profiles = int(integer_fields["sandbox_entity_profiles"])
    sandbox_with_conduct = int(integer_fields["sandbox_profiles_with_conduct_context"])
    sspe_profiles = int(integer_fields["special_purpose_insurer_profiles"])

    _require(lifecycle_entities == len(entity_by_id), "lifecycle entity count differs from entity profiles")
    _require(profiles == lifecycle_entities + brands, "profile population mismatch")
    _require(search_entries == profiles, "every profile must have one search entry")

    explorer_ids = {
        str(row.get("entity_id") or "") for row in explorer.get("entities") or []
    }
    _require("" not in explorer_ids, "explorer contains empty entity_id")
    ordinary_ids = {
        entity_id
        for entity_id, profile in entity_by_id.items()
        if (profile.get("regulatory") or {}).get("query_state")
        == "current_ordinary_insurer"
    }
    _require(
        ordinary_ids == explorer_ids,
        "ordinary public profile set differs from assessment explorer set",
    )
    _require(ordinary == len(ordinary_ids), "ordinary profile count mismatch")
    _require(assessed == len(explorer_ids), "assessment payload count differs from explorer")

    sspe_ids = {
        entity_id
        for entity_id, profile in entity_by_id.items()
        if (profile.get("regulatory") or {}).get("query_state") == SSPE_QUERY_STATE
    }
    _require(sspe_profiles == len(sspe_ids), "SSPE population count mismatch")
    _require(not (sspe_ids & ordinary_ids), "SSPE leaked into ordinary assessment universe")

    sandbox_carrier_ids = {
        str(row.get("entity_id") or "") for row in sandbox.get("carriers") or []
    }
    _require("" not in sandbox_carrier_ids, "Sandbox carrier without entity_id")
    sandbox_profile_ids = {
        entity_id
        for entity_id, profile in entity_by_id.items()
        if (profile.get("regulatory") or {}).get("regime") == "sandbox"
    }
    _require(
        sandbox_profile_ids == sandbox_carrier_ids,
        "Sandbox public profile set differs from Sandbox Conduct carrier set",
    )
    _require(sandbox_profiles == len(sandbox_profile_ids), "Sandbox profile population mismatch")
    _require(
        sandbox_with_conduct == len(sandbox_carrier_ids),
        "not every Sandbox carrier received Conduct context",
    )

    return {
        "explorer": len(explorer_ids),
        "ordinary": ordinary,
        "sspe": sspe_profiles,
        "sandbox": sandbox_profiles,
        "profiles": profiles,
    }


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
        "sspe_enters_ordinary_assessment",
        "sspe_enters_ordinary_ranking",
        "php_may_recompute_methodology",
    }
    for key in required_false:
        _require(policy.get(key) is False, f"public safety policy changed: {key}")
    _require(
        policy.get("search_is_broader_than_ordinary_assessment") is True,
        "search must remain broader than ordinary assessment",
    )


def _validate_profile_semantics(
    profiles: dict[str, dict[str, Any]],
) -> None:
    for profile_id, profile in profiles.items():
        kind = profile.get("profile_kind")
        assessment = profile.get("assessment") or {}
        if kind == "brand":
            _require(
                assessment.get("availability") == "not_applicable",
                f"brand inherited entity assessment: {profile_id}",
            )
            continue
        if kind != "entity":
            raise PublicProfileValidationError(f"unknown profile kind: {kind}")

        regulatory = profile.get("regulatory") or {}
        query_state = regulatory.get("query_state")
        if query_state == SSPE_QUERY_STATE:
            _require(regulatory.get("label") == SSPE_LABEL, f"SSPE label missing: {profile_id}")
            _require(
                (profile.get("public_summary") or {}).get("headline") == SSPE_LABEL,
                f"SSPE headline missing: {profile_id}",
            )
            _require(
                assessment.get("availability") == "not_applicable",
                f"SSPE received ordinary assessment: {profile_id}",
            )
            _require(
                assessment.get("reason") == SSPE_ASSESSMENT_REASON,
                f"SSPE assessment reason is ambiguous: {profile_id}",
            )
        elif query_state == "current_ordinary_insurer":
            _require(
                assessment.get("availability") in {"available", "incomplete"},
                f"ordinary insurer lost assessment payload: {profile_id}",
            )


def _validate_profile_references(
    profiles: dict[str, dict[str, Any]],
) -> None:
    valid_profile_ids = set(profiles)

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if (
                    (key == "profile_id" or key.endswith("_profile_id"))
                    and child is not None
                ):
                    _require(
                        isinstance(child, str) and child in valid_profile_ids,
                        f"dangling public profile reference at {path}.{key}: {child!r}",
                    )
                walk(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")

    for profile_id, profile in profiles.items():
        walk(profile, f"$.profiles[{profile_id}]")


def _validate_public_copy(
    profiles: dict[str, dict[str, Any]],
) -> None:
    """Keep implementation vocabulary out of copy shipped to end users."""

    def walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                walk(child, f"{path}.{key}")
        elif isinstance(value, list):
            for index, child in enumerate(value):
                walk(child, f"{path}[{index}]")
        elif isinstance(value, str):
            for label, pattern in PUBLIC_COPY_FORBIDDEN.items():
                _require(
                    pattern.search(value) is None,
                    f"{label} leaked into public profile copy at {path}",
                )

    for profile_id, profile in profiles.items():
        walk(profile, f"$.profiles[{profile_id}]")


def _validate_search_index(
    contract: dict[str, Any],
    profiles: dict[str, dict[str, Any]],
) -> None:
    entries = list(contract.get("search_index") or [])
    by_profile = {
        str(row.get("profile_id") or ""): row for row in entries
    }
    _require(
        "" not in by_profile and len(by_profile) == len(entries),
        "search profile ids must be non-empty and unique",
    )
    _require(set(by_profile) == set(profiles), "search index/profile manifest population differs")
    paths = [str(row.get("profile_path") or "") for row in entries]
    _require(all(paths) and len(set(paths)) == len(paths), "search profile paths must be unique")

    for profile_id, profile in profiles.items():
        regulatory = profile.get("regulatory") or {}
        if regulatory.get("query_state") != SSPE_QUERY_STATE:
            continue
        entry = by_profile[profile_id]
        _require(
            SSPE_LABEL in str(entry.get("disambiguation") or ""),
            f"SSPE search result lacks subtype disambiguation: {profile_id}",
        )
        _require(
            entry.get("filter_bucket") != "insurers",
            f"SSPE search result leaked into ordinary insurer filter: {profile_id}",
        )


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


def _validate_public_files(profile_count: int) -> None:
    _require((PUBLIC_DIR / "search_index.json").is_file(), "search index missing")
    _require((PUBLIC_DIR / "profile_manifest.json").is_file(), "manifest missing")
    profile_dir = PUBLIC_DIR / "profiles"
    _require(profile_dir.is_dir(), "profile directory missing")
    _require(
        len(list(profile_dir.glob("*.json"))) == profile_count,
        "profile-file count differs from profile population",
    )


def validate_real_public_search_profile_contract() -> dict[str, Any]:
    contract = _load(CONTRACT_PATH)
    explorer = _load(EXPLORER_PATH)
    sandbox = _load(SANDBOX_PATH)
    _require(
        contract.get("status") == "public_search_profile_contract_closed",
        "public search/profile contract did not close",
    )

    profiles, entity_by_id = _profile_maps(contract)
    counts = _validate_population(contract, explorer, sandbox, entity_by_id)
    _validate_policy(contract)
    _validate_profile_semantics(profiles)
    _validate_profile_references(profiles)
    _validate_public_copy(profiles)
    _validate_search_index(contract, profiles)
    _walk_public(contract.get("profiles") or [])
    _validate_public_files(counts["profiles"])

    return {
        "status": contract["status"],
        "version": contract.get("version"),
        "population": contract["population"],
        "structural_counts": counts,
        "general_ranking_fields_present": False,
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
