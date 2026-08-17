from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.v2.classification import (
    apply_licensed_classification,
    apply_sandbox_classification,
    apply_special_regime_classification,
    classification_summary,
)
from api.v2.identity import build_canonical_entities
from api.v2.sandbox_identity import materialize_unmatched_sandbox_identities

DEFAULT_OUTPUT = Path("data/derived/v2/entity_classification_inventory.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_classification_inventory(
    ses_companies: Any,
    licensed_records: list[dict[str, Any]],
    special_records: list[dict[str, Any]] | None = None,
    sandbox_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    base_entities = build_canonical_entities(ses_companies)
    classified = apply_licensed_classification(base_entities, licensed_records)
    classified = apply_special_regime_classification(classified, special_records or [])
    classified, unresolved_sandbox = apply_sandbox_classification(
        classified,
        sandbox_records or [],
    )
    classified, unresolved_sandbox = materialize_unmatched_sandbox_identities(
        classified,
        unresolved_sandbox,
    )

    summary = classification_summary(
        classified,
        licensed_records,
        special_records or [],
        sandbox_records or [],
        unresolved_sandbox,
    )
    sandbox_added = sum(
        (item.get("evidence") or {}).get("identity_origin") == "susep_sandbox"
        for item in classified
    )
    sandbox_resolved = sum(
        bool((item.get("evidence") or {}).get("sandbox"))
        for item in classified
    )
    summary["sandbox_records_resolved"] = sandbox_resolved
    summary["sandbox_matched_existing_by_exact_cnpj"] = sandbox_resolved - sandbox_added
    summary["entities_added_from_sandbox_source"] = sandbox_added
    summary.pop("sandbox_applied_by_exact_cnpj", None)

    return {
        "artifact": "v2_classification_inventory",
        "generated_at": _utc_now(),
        "status": "draft",
        "meta": {
            **summary,
            "classification_scope": (
                "official current licensed-entities, special-regime and Sandbox sources"
            ),
            "classification_note": (
                "SUSEP regulatory sources define scope, type and status. SES contributes "
                "financial/activity evidence when present. FIP is the preferred regulatory "
                "identity; Sandbox participants without a published FIP are retained by "
                "their official CNPJ. No fuzzy name matching is used."
            ),
        },
        "unresolved": {"sandbox": unresolved_sandbox},
        "entities": classified,
    }


def write_classification_inventory(
    payload: dict[str, Any],
    output: Path = DEFAULT_OUTPUT,
) -> Path:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(output.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(output)
    return output


def main() -> None:
    ses_out = extract_ses_master_and_financials()
    if not isinstance(ses_out, tuple) or len(ses_out) < 2:
        raise RuntimeError(f"Unexpected SES return: {type(ses_out)}")

    licensed = fetch_licensed_entities()
    special = fetch_special_regime_records()
    sandbox = fetch_sandbox_participants()
    payload = build_classification_inventory(ses_out[1], licensed, special, sandbox)
    path = write_classification_inventory(payload)
    meta = payload["meta"]
    print(
        "V2 classification inventory: "
        f"{meta['inventory_count']} entities; "
        f"{meta['by_regulatory_status'].get('active_licensed', 0)} active licensed; "
        f"{meta['special_regime_source_count']} special-regime records; "
        f"{meta['sandbox_records_resolved']}/{meta['sandbox_source_count']} Sandbox records resolved; "
        f"{meta['entities_added_from_sandbox_source']} Sandbox-only identities added; "
        f"{meta['sandbox_unresolved_count']} Sandbox unresolved; "
        f"written to {path}"
    )


if __name__ == "__main__":
    main()
