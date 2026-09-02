from __future__ import annotations

import json
import re
import unicodedata
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.v2.build_public_search_profile_contract import write_public_outputs

CONTRACT_PATH = Path("data/derived/v2/public_search_profile_contract.json")
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
CONDUCT_PATH = Path("data/derived/v2/conduct_methodology_closure.json")
SANDBOX_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")
RELATIONSHIPS_PATH = Path("data/reference/v2/verified_relationships.json")
VERSION = "2.0-public-information-projection-2"


class PublicInformationProjectionError(RuntimeError):
    """Raised when approved internal context cannot be projected safely."""


def _normalize_search(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


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
    months = _normalize_months(
        list((sandbox.get("source") or {}).get("months") or []), "Sandbox"
    )
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


def _verified_market_identities(
    relationship_registry: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    seen_cnpjs: dict[str, str] = {}
    for brand in relationship_registry.get("brands") or []:
        market_identity = brand.get("market_identity")
        if market_identity is None:
            continue
        if not isinstance(market_identity, dict):
            raise PublicInformationProjectionError(
                f"market_identity must be an object: {brand.get('brand_id')}"
            )
        brand_id = str(brand.get("brand_id") or "").strip()
        kind = str(market_identity.get("kind") or "").strip()
        public_label = str(market_identity.get("public_label") or "").strip()
        legal_name = str(market_identity.get("legal_name") or "").strip()
        public_note = str(market_identity.get("public_note") or "").strip()
        cnpj = re.sub(r"\D+", "", str(market_identity.get("cnpj") or ""))
        evidence = market_identity.get("evidence")
        if not brand_id or not kind or not public_label or not legal_name or not public_note:
            raise PublicInformationProjectionError(
                f"incomplete verified market identity: {brand_id or '<missing brand_id>'}"
            )
        if len(cnpj) != 14:
            raise PublicInformationProjectionError(
                f"invalid market identity CNPJ for {brand_id}: {cnpj!r}"
            )
        if not isinstance(evidence, dict) or not evidence.get("authority"):
            raise PublicInformationProjectionError(
                f"market identity requires source-backed evidence: {brand_id}"
            )
        other = seen_cnpjs.get(cnpj)
        if other and other != brand_id:
            raise PublicInformationProjectionError(
                f"market identity CNPJ collision: {cnpj} -> {other}, {brand_id}"
            )
        seen_cnpjs[cnpj] = brand_id
        output[brand_id] = {
            "kind": kind,
            "public_label": public_label,
            "legal_name": legal_name,
            "cnpj": cnpj,
            "public_note": public_note,
            "evidence": deepcopy(evidence),
            "assessment_inheritance": "forbidden",
        }
    return output


def _project_verified_market_identities(
    contract: dict[str, Any],
    relationship_registry: dict[str, Any],
) -> int:
    market_identities = _verified_market_identities(relationship_registry)
    if not market_identities:
        return 0

    profiles = {
        str(profile.get("profile_id") or ""): profile
        for profile in contract.get("profiles") or []
    }
    search_entries = {
        str(entry.get("profile_id") or ""): entry
        for entry in contract.get("search_index") or []
    }

    count = 0
    for brand_id, market_identity in market_identities.items():
        profile = profiles.get(brand_id)
        entry = search_entries.get(brand_id)
        if not profile or profile.get("profile_kind") != "brand":
            raise PublicInformationProjectionError(
                f"verified market identity has no public brand profile: {brand_id}"
            )
        if not entry:
            raise PublicInformationProjectionError(
                f"verified market identity has no public search entry: {brand_id}"
            )

        identity = profile.setdefault("identity", {})
        identity["market_identity"] = deepcopy(market_identity)

        relationships = profile.get("relationships") or []
        target_names = [
            str(relation.get("target_name") or "").strip()
            for relation in relationships
            if str(relation.get("target_name") or "").strip()
        ]
        targets = ", ".join(target_names) or "a entidade regulada documentada"
        name = str(identity.get("name") or brand_id)
        summary = profile.setdefault("public_summary", {})
        summary["headline"] = (
            f"{name}: {market_identity['public_label']} com relação verificada"
        )
        summary["quick_answer"] = (
            f"{market_identity['public_note']} A relação documentada aponta para "
            f"{targets}. Esta identidade de mercado não herda automaticamente a "
            "avaliação, os dados financeiros ou as reclamações da entidade relacionada."
        )

        entry["cnpj"] = market_identity["cnpj"]
        entry["entity_type"] = market_identity["kind"]
        entry["market_role_label"] = market_identity["public_label"]
        pieces = [
            market_identity["public_label"],
            f"CNPJ {market_identity['cnpj']}",
            f"relacionada a {targets}" if target_names else None,
        ]
        entry["disambiguation"] = " · ".join(piece for piece in pieces if piece)
        search_terms = [
            entry.get("search_text"),
            market_identity["legal_name"],
            market_identity["cnpj"],
            market_identity["public_label"],
        ]
        entry["search_text"] = _normalize_search(
            " ".join(str(term) for term in search_terms if term)
        )
        count += 1

    return count


def apply_public_information_projection(
    contract: dict[str, Any],
    explorer: dict[str, Any],
    conduct: dict[str, Any],
    sandbox: dict[str, Any],
    relationship_registry: dict[str, Any] | None = None,
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

    market_identity_count = 0
    if relationship_registry is not None:
        market_identity_count = _project_verified_market_identities(
            projected_contract, relationship_registry
        )

    projected_contract["public_information_projection"] = {
        "version": VERSION,
        "conduct_reference_window": deepcopy(window),
        "policy": {
            "frontend_may_infer_conduct_period": False,
            "ordinary_and_sandbox_windows_must_match": True,
            "projection_changes_methodology": False,
            "market_identity_may_inherit_related_entity_assessment": False,
        },
        "counts": {
            "explorer_entities": explorer_count,
            "ordinary_profiles_with_conduct": ordinary_count,
            "sandbox_entity_contexts": sandbox_count,
            "sandbox_brand_contexts": brand_sandbox_count,
            "verified_market_identity_profiles": market_identity_count,
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
    relationship_registry = json.loads(RELATIONSHIPS_PATH.read_text(encoding="utf-8"))
    projected_contract, projected_explorer = apply_public_information_projection(
        contract,
        explorer,
        conduct,
        sandbox,
        relationship_registry,
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
