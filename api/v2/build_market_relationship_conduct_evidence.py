from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from api.v2.build_consumer_gov_identity_experiment import OUTPUT_PATH as IDENTITY_PATH
from api.v2.consumer_gov_conduct_core import load_monthly_entries
from api.v2.market_relationship_conduct_core import (
    build_market_relationship_conduct_evidence as _build_market_relationship_conduct,
    utc_now,
)

LIFECYCLE_PATH = Path("data/derived/v2/entity_lifecycle_relationship_inventory.json")
OUTPUT_PATH = Path("data/derived/v2/market_relationship_conduct_evidence.json")
LEGACY_SANDBOX_OUTPUT_PATH = Path(
    "data/derived/v2/sandbox_brand_conduct_evidence.json"
)


def _load_entries_from_source(month: str) -> dict[str, Any]:
    _, entries, _ = load_monthly_entries(month)
    return entries


def build_market_relationship_conduct_evidence(
    lifecycle: dict[str, Any],
    identity: dict[str, Any],
    *,
    entry_loader=None,
) -> dict[str, Any]:
    return _build_market_relationship_conduct(
        lifecycle,
        identity,
        entry_loader=entry_loader or _load_entries_from_source,
    )


def build_legacy_sandbox_projection(payload: dict[str, Any]) -> dict[str, Any]:
    """Temporary compatibility view for consumers that still expect Sandbox fields."""
    source = payload.get("source") or {}
    sandbox_bearers = [
        row
        for row in payload.get("risk_bearers") or []
        if row.get("regulatory_regime") == "sandbox"
    ]
    bearer_by_id = {str(row["entity_id"]): row for row in sandbox_bearers}

    carriers = []
    for row in sandbox_bearers:
        summary = row.get("conduct_summary") or {}
        carriers.append(
            {
                "entity_id": row.get("entity_id"),
                "entity_type": "sandbox_participant",
                "cnpj": row.get("cnpj"),
                "legal_name": row.get("name"),
                "display_name": row.get("name"),
                "regulatory_regime": "sandbox",
                "regulatory_public_label": row.get("regulatory_class"),
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
                "consumer_gov_provider_labels_observed": summary.get("provider_labels")
                or [],
                "totals": {
                    key: summary.get(key)
                    for key in (
                        "complaints",
                        "responded",
                        "response_rate",
                        "finalized",
                        "finalized_rate",
                        "satisfaction_count",
                        "average_satisfaction",
                    )
                },
                "monthly": summary.get("monthly") or [],
                "film": summary.get("film") or {},
            }
        )

    brands = []
    for row in payload.get("brands") or []:
        all_bearers = list(row.get("risk_bearers") or [])
        sandbox_relations = [
            relation
            for relation in all_bearers
            if str(relation.get("entity_id") or "") in bearer_by_id
        ]
        # The old contract is singular. Never collapse a multi-carrier relationship
        # into a misleading one-carrier Sandbox card.
        if len(all_bearers) != 1 or len(sandbox_relations) != 1:
            continue
        relation = sandbox_relations[0]
        carrier = bearer_by_id[str(relation["entity_id"])]
        carrier_summary = carrier.get("conduct_summary") or {}
        brand_summary = row.get("market_identity_conduct_summary") or {}
        market = row.get("market_identity") or {}
        brands.append(
            {
                "brand_id": row.get("brand_id"),
                "name": row.get("name"),
                "aliases": row.get("aliases") or [],
                "representative_cnpj": market.get("cnpj"),
                "market_identity": market,
                "distribution_role": row.get("distribution_role"),
                "distribution_role_state": row.get("distribution_role_state"),
                "risk_carrier_entity_id": carrier.get("entity_id"),
                "risk_carrier_cnpj": carrier.get("cnpj"),
                "risk_carrier_name": carrier.get("name"),
                "risk_carrier_regulatory_class": carrier.get("regulatory_class"),
                "regulatory_scope": "sandbox",
                "product_scope": relation.get("product_scope"),
                "conduct_scope": "separate_distributor_subject_and_carrier_context",
                "conduct_context_policy": (
                    "separate_distributor_subject_from_carrier_context"
                ),
                "ordinary_ranking_effect": "none",
                "ordinary_market_baseline_effect": "none",
                "brand_subject_attribution_note": (
                    "Compatibility projection: complaints remain attached to the verified "
                    "market identity and are not transferred to the Sandbox risk bearer."
                ),
                "carrier_attribution_note": (
                    "Compatibility projection: carrier complaints remain with the "
                    "risk-bearing legal entity and are not complaints of the distributor."
                ),
                "brand_subject_provider_labels_observed": brand_summary.get(
                    "provider_labels"
                )
                or [],
                "brand_subject_conduct_summary": brand_summary,
                "carrier_conduct_summary": carrier_summary,
                "evidence": [relation.get("evidence") or {}],
            }
        )

    return {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "version": "2.0-compat-from-market-relationship-conduct-1",
        "generated_at": payload.get("generated_at") or utc_now(),
        "status": "compatibility_projection",
        "assessment_role": "legacy_sandbox_projection_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "ordinary_ranking_effect": "none",
        "source": source,
        "policy": {
            "authoritative_relationship_model": (
                "v2_market_relationship_conduct_evidence"
            ),
            "sandbox_is_regulatory_attribute_only": True,
            "sandbox_never_enters_ordinary_ranking": True,
            "distributor_complaints_remain_with_distributor": True,
            "cnpj_precedes_brand_label_for_subject_attribution": True,
            "fuzzy_brand_complaint_attribution": False,
        },
        "summary": {
            "sandbox_entities": len(carriers),
            "verified_brand_contexts": len(brands),
        },
        "carriers": carriers,
        "brands": brands,
    }


def build_from_files() -> tuple[dict[str, Any], dict[str, Any]]:
    lifecycle = json.loads(LIFECYCLE_PATH.read_text(encoding="utf-8"))
    identity = json.loads(IDENTITY_PATH.read_text(encoding="utf-8"))
    payload = build_market_relationship_conduct_evidence(lifecycle, identity)
    legacy = build_legacy_sandbox_projection(payload)
    return payload, legacy


def main() -> None:
    payload, legacy = build_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    LEGACY_SANDBOX_OUTPUT_PATH.write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "summary": payload["summary"],
                "legacy_sandbox_projection": legacy["summary"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")
    print(f"compatibility projection written to {LEGACY_SANDBOX_OUTPUT_PATH}")


if __name__ == "__main__":
    main()
