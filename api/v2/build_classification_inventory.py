from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.ses import extract_ses_master_and_financials
from api.sources.susep_licensed import fetch_licensed_entities
from api.sources.susep_sandbox import fetch_sandbox_participants
from api.sources.susep_special_regimes import fetch_special_regime_records
from api.utils.identifiers import normalize_cnpj_v2
from api.v2.classification import (
    apply_licensed_classification,
    apply_sandbox_classification,
    apply_special_regime_classification,
    classification_summary,
)
from api.v2.identity import build_canonical_entities
from api.v2.sandbox_identity import materialize_unmatched_sandbox_identities
from api.v2.sandbox_lifecycle_history import SandboxLifecycleHistoryError
from api.v2.sandbox_semantics import normalize_sandbox_entity_semantics
from api.v2.ses_activity_evidence import enrich_entities_with_ses_activity_evidence

DEFAULT_OUTPUT = Path("data/derived/v2/entity_classification_inventory.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _attach_sandbox_history(
    entities: list[dict[str, Any]],
    sandbox_history: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    if sandbox_history is None:
        return entities
    if sandbox_history.get("artifact") != "v2_sandbox_regulatory_history":
        raise SandboxLifecycleHistoryError("unexpected Sandbox lifecycle artifact")

    records = list(sandbox_history.get("records") or [])
    by_cnpj: dict[str, dict[str, Any]] = {}
    for raw in records:
        record = dict(raw)
        cnpj = normalize_cnpj_v2(record.get("cnpj"))
        if not cnpj:
            raise SandboxLifecycleHistoryError("Sandbox history record without CNPJ")
        by_cnpj[cnpj] = record

    output = [dict(item) for item in entities]
    represented: set[str] = set()
    for entity in output:
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if not cnpj or cnpj not in by_cnpj:
            continue
        evidence = dict(entity.get("evidence") or {})
        evidence["sandbox_history"] = by_cnpj[cnpj]
        entity["evidence"] = evidence
        represented.add(cnpj)

    for cnpj in sorted(set(by_cnpj) - represented):
        record = by_cnpj[cnpj]
        last_snapshot = dict(record.get("last_snapshot") or {})
        output.append(
            {
                "entity_id": f"cnpj:{cnpj}",
                "fip_code": None,
                "cnpj": cnpj,
                "legal_entity_id": f"cnpj:{cnpj}",
                "legal_name": (
                    record.get("legal_name")
                    or last_snapshot.get("legal_name")
                    or cnpj
                ),
                "entity_type": "sandbox_participant",
                "regulatory_regime": "sandbox",
                "regulatory_status": "sandbox_history_only",
                "activities": {
                    "insurance": False,
                    "pension": False,
                    "capitalization": False,
                    "reinsurance": False,
                },
                "evidence": {
                    "ses_present": False,
                    "identity_origin": "susep_sandbox_history",
                    "identity_cnpj_source": "susep_sandbox_history",
                    "sandbox_history": record,
                },
            }
        )

    return sorted(output, key=lambda item: item["entity_id"])


def _mark_unclassified_source_evidence(
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Record why an entity remains unknown without inventing an inactive status."""
    for item in entities:
        if item.get("regulatory_status") != "unknown":
            continue
        evidence = dict(item.get("evidence") or {})
        evidence["classification"] = {
            "result": "not_classified_by_current_regulatory_sources",
            "matched_current_licensed_source": False,
            "matched_special_regime_source": False,
            "matched_sandbox_source": False,
            "interpretation": (
                "Absence from the current regulatory sources checked does not establish "
                "that the entity is inactive, closed or incorporated."
            ),
        }
        item["evidence"] = evidence
    return entities


def build_classification_inventory(
    ses_companies: Any,
    licensed_records: list[dict[str, Any]],
    special_records: list[dict[str, Any]] | None = None,
    sandbox_records: list[dict[str, Any]] | None = None,
    *,
    sandbox_history: dict[str, Any] | None = None,
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
    classified = normalize_sandbox_entity_semantics(classified)
    classified = _attach_sandbox_history(classified, sandbox_history)
    classified = enrich_entities_with_ses_activity_evidence(classified)
    classified = _mark_unclassified_source_evidence(classified)

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
    summary["sandbox_history_records"] = len(
        (sandbox_history or {}).get("records") or []
    )
    summary["sandbox_history_only_entities"] = sum(
        (item.get("evidence") or {}).get("identity_origin") == "susep_sandbox_history"
        for item in classified
    )
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
            "ses_master_status_fields_available": False,
            "classification_note": (
                "SUSEP regulatory sources define scope, type and status. SES contributes "
                "financial/activity evidence when present. Current SES activity flags are "
                "derived from recent structured data-flow presence in BaseCompleta, never "
                "from company names or corporate groups. FIP is the preferred regulatory "
                "identity; Sandbox participants without a published FIP are retained by "
                "their official CNPJ and typed separately as sandbox_participant, never as "
                "ordinary insurers. Ses_cias.csv exposes entity/group identifiers and names, "
                "but no status or closure field; unmatched records therefore remain unknown "
                "rather than being labeled inactive. No fuzzy name matching is used."
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
